# Shale — Write Path

## 12. Write Path

### 12.1 Flow

```text
1. Writer      keeps allocations for segments due within allocation_horizon:
               SetService.Allocate {set, horizon}   (or ObjectService.Allocate
               {source, expected date_started} for a single segment)
2. CP          object_id, attempt_id, sink_id, node endpoint, access token
               (plus the next few ranked candidates, for fast reallocation)
3. Writer      live:     opens the upload when the segment starts and streams
                         bytes as they are recorded
               buffered: opens the upload once the segment is complete
               PUT <node>/objects/<key>  Upload-Offset: 0  Upload-Complete: ?1
               (Content-Length, or chunked + Shale-Size-Hint for live)
4. Node        admission: accept if the sink is below max_uploads,
               else 503 + Retry-After
5. Node        create the file at its final path, O_CREAT|O_EXCL|O_DIRECT
               fsetxattr(user.shale, record, state=open)
               fallocate(KEEP_SIZE, length or size hint)   (one contiguous extent)
6. Node        stage incoming bytes in a part buffer; each full part becomes
               a WRITE job at its offset
   ...         on disconnect: resume from the reported offset (§12.2)
7. Writer      ends the request body cleanly → the upload is complete
8. Worker      last part written, unused reservation released,
               xattr state=complete + final size, fsync   (one flush)
9. Node        add to the in-memory index; queue the ObjectStored event
10. Node       reply 201 to the Writer     ← commit point
11. Writer     release the segment
12. Consumer   ObjectStored → state COMMITTED in the index
```

**Pre-allocation.** Writers fetch allocations ahead of time. That takes the CP
round trip off the upload's critical path, and it lets ingest ride out a short
CP outage. It adds no durable Writer state:

- A Writer holds allocations for the segments of each camera that are due
  within `allocation_horizon` (negotiated, [§12.6](#126-upload-profile-negotiation);
  default 10 minutes ≈ 5 segments at 2 minutes),
  in RAM only. After a restart it simply asks again.
- `SetService.Allocate` returns allocations for every member of the set
  up to the horizon in one call. Placement is a pure function of
  `(set, epoch, ordinal)` and the expected `date_started` ([§11](03-placement.md#11-placement)), so an allocation
  computed early points to the same sink as one computed at upload time.
- An allocation is valid for `allocation_ttl` = horizon + maximum upload time
  (default 20 minutes), and so is its access token
  ([§33.2](10-security.md#332-access-tokens)). An unused allocation
  expires as `ABANDONED` and leaves no trace for readers ([§8](02-data-model.md#8-state-model)).
- If a segment's actual `date_started` falls into a different epoch than the
  expected one (e.g. the set restarted), the Writer discards the allocation
  and asks again.

**What the horizon costs.** An early allocation reflects cluster state at the
time it was issued, up to one horizon old:

| Change during the horizon | Effect |
|---|---|
| Device quarantined or sink CRITICAL | The PUT usually fails and the Writer moves to the next candidate in the allocation. The node does not know about CP-side quarantine, so a quarantined device that still works keeps receiving writes for up to one horizon |
| Node added | Keys that would move to the new node move up to one horizon late. Negligible |
| Set powers off | Up to one horizon of allocations per camera expire as `ABANDONED`. These are cheap rows |

**What the horizon buys.** Commit and allocation are separate failure domains:
Storage Nodes commit without the CP, and only index updates queue up ([§12.4](#124-commit-semantics)).
During a CP outage:

| Function | Behavior | Lasts |
|---|---|---|
| Writes | continue on allocations in hand | `allocation_horizon` |
| Commits | succeed on the node; `ObjectStored` events queue in node RAM (~200 B each) | hours |
| GC | no approvals, so sinks drift toward CRITICAL | ~40 h at 5% free and 5.6 MB/s per sink ([§26.3](08-sizing.md#263-worked-example-cctv)) |
| Reads | location lookup and URLs unavailable | **stop immediately** |

The horizon is therefore sized to cover a CP failover or redeploy, not a long
outage. Reads stop at once anyway, so long outages are handled by running the
CP highly available, not by a longer horizon.

### 12.2 Resumable part uploads

Producers may sit behind a LAN, a WAN, or a wireless link, and may stream
segments live. One upload protocol serves them all.

**Protocol.** Offset-based sequential append, in the style of the IETF
resumable-upload draft:

```text
PUT  /objects/<key>   Upload-Offset: o   Upload-Complete: ?1 | ?0
                      [Upload-Length: L]  [Shale-Size-Hint: H]
                      body = bytes from o  (Content-Length, or chunked)
       → 201                       upload complete and durable (commit)
       → 204  Upload-Offset: cur   request accepted, upload not complete yet
       → 409  Upload-Offset: cur   o does not match the node's offset
HEAD /objects/<key>
       → Upload-Offset: cur, Upload-Complete: ?0 | ?1, [Upload-Length]
```

- `Upload-Complete: ?1` means "the end of this request body is the end of the
  object". A body that ends cleanly completes the upload. A broken connection
  leaves it open.
- After a disconnect the Writer asks `HEAD` for the offset and sends the rest
  from there. Bytes the node already has are never sent again.
- The only upload state is the file itself: its xattr says `open` or
  `complete`, and its size is the offset. There is no part list and no
  separate "complete" call.

**Two ways to upload.**

| | Buffered | Live (default; chosen by negotiation, [§12.6](#126-upload-profile-negotiation)) |
|---|---|---|
| Starts | when the segment is complete | when the segment starts |
| Request | one request, `Content-Length` = L | one long request (≈ segment duration), chunked |
| Size at start | known (`Upload-Length`) | unknown; `Shale-Size-Hint` = bitrate × duration × 1.2, capped at the agreed maximum |
| Uplink | a burst per segment, smoothed by staggering | exactly the recording bitrate |
| Data reaches storage | after the segment closes | within one part fill time (≈ 16–32 s) |
| If the producer is destroyed | its buffered segments are gone | only bytes not yet written to a sink are gone ([§15](#15-partial-objects)) |

For live uploads, the node reserves `size_hint`. If the stream outgrows it,
the node reserves another extent; the object may then be in two pieces, which
is harmless. At completion the unused reservation beyond EOF is released.

**Writer retention.** The node reports an offset once the bytes are written to
the device, before any `fsync`. A Writer chooses how long it keeps the bytes:

| `retain` | Writer RAM | If the node loses unsynced data (power loss) |
|---|---|---|
| `committed` (default) | the whole segment until `201` | resume from the lower offset, or re-upload elsewhere |
| `written` | about one part: the segment is sent as part-sized (hence 4 KiB-aligned) requests with `Upload-Complete: ?0`; the node replies `204` only after that part is written, and the Writer then frees it | the segment is lost |

`written` is meant for RAM-constrained producers. It follows the
loss-tolerant principle, but the loss is real.

**Staging in parts.**

- Incoming bytes fill a RAM **part buffer** (`part_size`, default 16 MB). A
  full buffer becomes one WRITE job at its offset, and then the buffer is
  released. Buffers grow on demand, so a slowly filling live upload holds only
  what it has received so far.
- The extent was reserved up front with `fallocate(KEEP_SIZE)`. Parts of
  concurrent uploads therefore interleave in *time* on a device, but each object
  stays **contiguous on the device**, and later reads remain sequential.
- On disconnect, the node flushes the aligned prefix of the buffer and drops
  the unaligned remainder. The reported offset is then 4 KiB-aligned and the
  Writer resends at most a few KiB.
- The final part pads to alignment and the node truncates the file to its
  final length before the `fsync`.

**Cost on the device.** A live or slow camera writes a 16 MB part every ~30 s at
4 Mbps. About 10 such cameras per sink add ~0.3 part-writes per second, i.e.
~3 ms of seeking per second, **< 1% of device time**. Devices still see only large
sequential writes, just part-sized rather than object-sized:

| `part_size` | Write time | Efficiency at full device load (one ~10 ms seek per part) |
|---:|---:|---:|
| 8 MB | ~45 ms | ~80% |
| 16 MB | ~90 ms | ~90% |
| 32 MB | ~180 ms | ~95% |

At CCTV loads (devices at ~5% of bandwidth, [§26.3](08-sizing.md#263-worked-example-cctv)) the difference is invisible.
Bandwidth-bound deployments can raise `part_size` and pay for it in RAM ([§26.4](08-sizing.md#264-ram)).
On a LAN, buffered uploads arrive back to back and behave like the single
large write they replace.

**Admission and timeouts.**

| Limit | Default | On breach |
|---|---|---|
| `max_uploads` per sink | 64 | new upload → `503` + `Retry-After` |
| `part_buffer_pool` per node | 12 GiB | stop reading sockets; TCP flow control slows the senders |
| `idle_timeout` (negotiated) | 30 s without bytes | close the connection; the upload stays resumable |
| `abandon_timeout` (negotiated) | 5 min without bytes | abandoned: a live upload is **finalized** as an incomplete object ([§15](#15-partial-objects)); a buffered one is deleted |
| `allocation_ttl` | 20 min | URLs expire, so no upload can be resumed after this |

With live uploads every camera on a sink has an upload in flight, and two
briefly overlap at each segment boundary. A busy sink carries ~20 cameras
([placement decisions](placement-decisions.md) D8), so `max_uploads = 64` leaves room.

Unlike a minimum-rate rule, none of these penalize a slow but steady link.

**Node restarts.** Upload state is the file, so an upload survives a node
process restart. The startup scan leaves `open` files alone until they have
been idle for `abandon_timeout`, and then applies the abandon rule above.
After a power loss, unflushed tail data may be gone. The file size then
reflects only what persisted, so the Writer resumes from a lower offset.

**Links.** A producer's uplink must sustain its set's bitrate with headroom
(≈ 1.2×). Below that, live uploads fall behind real time and the Writer's
backlog grows until it drops the oldest segments ([§16](#16-writer-backpressure)). On slow or lossy
links the Writer:

- in buffered mode, uploads oldest first with 1–2 concurrent uploads
  (parallel streams only compete on a wireless link). In live mode, it runs
  one stream per camera, which together equal the set's bitrate,
- uses TLS, as every client does ([§33.5](10-security.md#335-tls)),
- should use a loss-tolerant TCP congestion control such as BBR.

**Staggered segment boundaries.** A set powers on as a unit, and a power
restore can start a whole site at once, so without staggering every camera
would close its segment at the same instant. Writers cut boundaries by
duration at the nearest keyframe, with a deterministic phase:

```text
phase(camera) = hash(set) mod D  +  ordinal × D / set_size     (D = segment duration)
```

The first segment after power-on is shortened to reach its phase. For
buffered uploads this spaces the set's uploads evenly, so the producer's uplink
sees a smooth stream instead of set-sized bursts. For live uploads the uplink
is smooth anyway. Staggering then spreads the per-object work (file creation,
final `fsync`, commit events, allocation use) and the part flushes of cameras
that started together.

### 12.3 Why there is no temp file and no rename

The classic `write temp → fsync → rename → fsync dir` pattern does two things:
it hides half-written files from readers, and it tells complete files apart
from partial ones after a crash. Shale gets both more cheaply:

- **Visibility**: readers only find objects through the index, which only
  learns about an object after the fsync. The node's in-memory index likewise
  exposes only completed writes.
- **Partial detection**: the xattr says `state=open` until the final
  `fsync`, which writes `state=complete` and the final size in the same
  journal flush. An `open` file is an upload in progress (resumable, [§12.2](#122-resumable-part-uploads)),
  or an abandoned one once idle for `abandon_timeout` ([§15](#15-partial-objects)). A `complete` file
  whose size disagrees with its xattr is damaged and is deleted.

This saves a rename and a directory fsync (extra journal writes, i.e. seeks) on
every object. A file cut off by a crash is simply a lost object, which the
design already accepts.

### 12.4 Commit semantics

**Commit** means: the object is durable in one specific sink, and so on one
physical device.

The Storage Node decides the commit, and the Writer learns of it from the PUT
response. The `ObjectStored` event updates the index:

```text
ObjectStored {
  object_id
  attempt_id
  sink_id
  object_key
  size
  incomplete                        (§15)
  date_started, date_ended, source_id   (for index rebuilds)
}
```

Events are pushed to the CP at least once, with no external queue
([§34.9](11-deployment.md#349-commit-event-delivery)). The node keeps
unpublished events in RAM only. If the node crashes after step 10 but before publishing, the event is
lost. On startup the node republishes `ObjectStored` for every file modified
within the last `event_replay_window` (e.g. 10 minutes), which covers that gap.
Anything still missed becomes an orphan, and orphans are reclaimed by GC ([§21.3](06-retention-gc.md#213-orphans)).

### 12.5 Idempotent uploads

- Resending bytes the node already has is harmless. An `Upload-Offset` below
  the node's offset is answered with `409` + the current offset, and the Writer
  continues from there.
- If the upload is already complete → `200` (the Writer may have missed the
  first `201`).
- A different `Upload-Length` for the same key, or bytes past the end of a
  complete upload → `409 Conflict`.

### 12.6 Upload profile negotiation

Cameras differ. A 1 Mbps camera fills 64 MB in 8.5 minutes, and a 16 Mbps
camera fills it in 32 seconds. A producer on a wireless link needs more
patience than one on a LAN. So the upload parameters that depend on the camera
or the link are **negotiated**: the producer proposes, and the CP answers with
the proposal clamped into cluster bounds.

**What is negotiated, and what is not.**

| Negotiated (per source or per set) | Not negotiated |
|---|---|
| target object size, from bitrate × segment duration (per source) | `part_size`, `max_uploads`, `part_buffer_pool`: the node's own resources |
| upload mode, `live` or `buffered` (per set) | `retain`, `resume_timeout`, retries: the Writer's own business |
| `idle_timeout`, `abandon_timeout` (per set, from the link) | watermarks, token lifetimes: cluster policy |
| `allocation_horizon` (per set, from the producer's buffering) | |

The bounds and defaults are in [§36.1](13-configuration.md#361-configuration-reference).
They live in an `UploadPolicy`, a global, versioned entity that operators
manage through the cluster API ([§35.5](12-api.md#355-cluster-api-custom-rpcs)).

**Protocol.**

```text
Producer  SetService.Negotiate {
            set,
            link:    {mode, idle_timeout, abandon_timeout, allocation_horizon},
            sources: [{source, bitrate, segment_duration}, ...]
          }
CP        clamps each value into the active UploadPolicy's bounds, stores the
          result on the set and its sources, and answers {
            profile_version,
            agreed:      the same fields, as they now apply,
            adjustments: [{field, proposed, agreed, reason}, ...]
          }
```

- The producer calls `Negotiate` when it starts and whenever a camera's
  settings change. A set that never negotiated uses the defaults.
- The producer **must use the agreed values**. When the CP adjusts object size,
  it keeps the camera's bitrate, which the producer cannot change, and changes
  the **segment duration** instead. Example: 1 Mbps × 2 min = 15 MB is below
  the 32 MB floor, so the agreed duration becomes ~4.3 min.
- The agreed values travel **in the access token** ([§33.2](10-security.md#332-access-tokens)):
  `max_length`, `mode`, `idle_timeout`, and `abandon_timeout`. A node enforces
  them per upload, within its own hard caps. It needs no knowledge of sets,
  and it cannot be asked for more than the CP agreed to.
- Every allocation carries the current `profile_version`. When an operator
  activates a new `UploadPolicy`, the CP re-clamps stored profiles, the version
  changes, and the producer negotiates again on its next allocation.

**Why bounds.** The lower size bound keeps the per-object fixed cost small on
HDDs ([§25](07-storage-node.md#25-object-size)). The upper bound caps node RAM,
the loss unit, and upload duration. Timeout bounds keep an abandoned upload
from holding a node's slot indefinitely, and keep a slow link from being cut
off too early.

## 13. Retry and Reallocation

### Same-target retry (resume)

Transient transport errors are retried against the same target: connect/TLS
timeouts, connection reset, `502`, `504`, and `503` (honoring `Retry-After` up
to a cap). An interrupted upload resumes from its offset ([§12.2](#122-resumable-part-uploads)).

The limit is based on progress, not attempt count. A slow link may reconnect
many times. The Writer keeps resuming as long as each attempt advances the
offset, and gives up on the target after `resume_timeout` (e.g. 2 minutes)
without progress, or when `allocation_ttl` runs out.

### Placement retry

After that, the Writer moves to the next candidate: either one returned with
the allocation or one from `ObjectService.Reallocate`. Each move creates a
new `attempt_id`.

```text
object O123
attempt A1 → sink S-0412 (node17) → FAILED
attempt A2 → sink S-0087 (node42) → STORED
```

### Immediate reallocation

These errors skip same-target retries: `ENOSPC`, device I/O error, sink or
device unavailable or quarantined, explicit reject.

### Limits

```text
same-target:  until resume_timeout without progress
placement:    2–3
```

When retries are exhausted, the object → **LOST**, and the Writer reports it
and drops the segment. One object never blocks the ingest pipeline.

Every failure the Writer reports feeds sink, device, and node health ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)).

## 14. Duplicates and Orphans

**One physical copy is best effort.** If attempt A1 is slow rather than dead,
it can commit after A2 did.

- The index has a unique constraint on `object_id`. The first
  `ObjectStored` wins, and later ones mark their attempt `DUPLICATE`.
- Exception: a **complete** attempt always beats an **incomplete** one ([§15](#15-partial-objects)).
  If an abandoned live upload was finalized first and the Writer later stored
  the full segment elsewhere, the full one replaces it, and the truncated one
  becomes the duplicate.
- A duplicate's file is left for GC, which reclaims it once expired
  ([§21.3](06-retention-gc.md#213-orphans)). The CP never calls a node to
  delete it.

**Orphans** are files in a sink that the index does not know: a lost commit event, a
duplicate nobody deleted, or data left over after an index loss. They waste
space but never break correctness. GC reclaims them after their `date_expired`
([§21.3](06-retention-gc.md#213-orphans)).

**Missing objects** are index entries whose file is gone. They are detected
lazily: when a read gets `404` from the node, the CP marks the object LOST.

## 15. Partial Objects

A segment can end early in two ways.

**The camera stops** (set powered off, camera fault). The Writer still
controls the upload. It ends the segment where the recording stopped and
completes the upload normally, with the real `date_ended` and size. If the
container is broken, the Writer drops the segment instead.

**The producer disappears mid-upload** (destroyed, powered off, link gone for
good). This case matters for CCTV: the last minutes before a producer is
destroyed are often the most important footage. With live upload, most of the
segment is already on a sink. When such an upload has been idle for
`abandon_timeout`, the node:

1. flushes and keeps what it has,
2. finalizes the file as a committed object flagged **`incomplete`**, with
   the size it actually has and no `date_ended`,
3. publishes `ObjectStored` with `incomplete: true`.

The node does not parse media. Writers that use live upload must use a
**streamable container** (MPEG-TS, fragmented MP4) so that any prefix is
playable up to its last complete unit. The media server works out the real
duration when it reads the object.

A buffered upload that is abandoned is deleted instead. Its Writer still holds
the whole segment and will have re-uploaded it elsewhere if it could.

## 16. Writer Backpressure

When no candidate accepts a write (all sinks at `max_uploads`, all targets failing), the
Writer buffers segments **in RAM** and retries with backoff. When its buffer is
full, it drops the oldest unstored segment and reports it LOST.

Writer parameters:

- upload mode: `live` or `buffered`
- `retain`: `committed` or `written` ([§12.2](#122-resumable-part-uploads))
- RAM buffer size (`committed`: ≥ 2 segments per camera it serves, plus
  headroom; `written`: ≈ 1 part per camera plus backlog)
- maximum age of a buffered segment
- retry backoff
- max concurrent uploads
