# Shale — Write Path

## 12. Write Path

### 12.1 Flow

```text
1. Producer    keeps allocations for segments due within allocation_horizon:
               SetService.Allocate {set, horizon}   (or ObjectService.Allocate
               {source, expected date_started} for a single segment)
2. CP          object_id and a ranked list of candidates, each with its own
               attempt_id, sink_id, node endpoints, and access token
3. Producer    live:     opens the upload when the segment starts and streams
                         bytes as they are recorded
               buffered: opens the upload once the segment is complete
               PUT <node>/objects/<key>  Upload-Offset: 0  Upload-Complete: ?1
               Shale-Date-Started: <t>
               (Content-Length, or chunked + Shale-Size-Hint for live)
4. Node        admission: accept if the sink is below max_uploads and the
               actor below uploads_per_actor, else 503 + Retry-After
5. Node        create the file at its final path, O_CREAT|O_EXCL|O_DIRECT
               fsetxattr(user.shale, record from the token, state=open)
               fallocate(KEEP_SIZE, length or size hint)   (one contiguous extent)
6. Node        stage incoming bytes in a part buffer; each full part becomes
               a WRITE job at its offset
   ...         on disconnect: resume from the reported offset (§12.2)
7. Producer    ends the request body cleanly, with Shale-Date-Ended → the
               upload is complete
8. Worker      last part written, unused reservation released,
               xattr state=complete + final size, fsync   (one flush)
9. Node        add to the in-memory index; queue the ObjectStored event
10. Node       reply 201 to the producer     ← commit point
11. Producer   release the segment
12. CP         ObjectStored → state COMMITTED in the index
```

**Pre-allocation.** Producers fetch allocations ahead of time. That takes the
CP round trip off the upload's critical path, and it lets ingest ride out a
short CP outage. It adds no durable producer state:

- A producer holds allocations for the segments of each camera that are due
  within `allocation_horizon` (negotiated, [§12.6](#126-upload-profile-negotiation);
  default 10 minutes ≈ 5 segments at 2 minutes), in RAM only. After a restart
  it simply asks again.
- `SetService.Allocate` returns allocations for every member of the set up to
  the horizon in one call. Placement is a pure function of
  `(set, epoch, ordinal)` and the expected `date_started`
  ([§11](03-placement.md#11-placement)), so an allocation computed early
  points to the same sink as one computed at upload time.
- Allocation is **idempotent per segment**: asking again for the same
  `(source, expected date_started)` answers the same `object_id`, with a fresh
  attempt only if the previous one has expired. A producer therefore never
  creates two objects for one segment, however often it restarts. A slot
  usually holds one segment; when the camera stops and comes back within
  the slot ([§15](#15-partial-objects)) the segment that follows begins
  after the stored one ended, and gets an object of its own in the same
  slot, so nothing the camera delivered is refused. A source may hold at
  most `max_open_attempts` (default 32) unfinished attempts.
- The CP accepts an expected `date_started` between `now − max_backlog_age`
  (default: the set's `retention.expire`) and `now + allocation_horizon +
  clock_tolerance` (5 minutes). A backlog that arrives hours late after a link
  outage is therefore ordinary ([§10](02-data-model.md#10-time-semantics));
  a time further in the future than the horizon allows is refused with an
  error, never clamped.
- An allocation is valid for `allocation_ttl`, and so is its access token
  ([§33.2](10-security.md#332-access-tokens)):

  ```text
  allocation_ttl = allocation_horizon
                 + the longest agreed segment duration in the set
                 + abandon_timeout
                 + 5 min
  ```

  With the defaults that is 22 minutes (10 + 2 + 5 + 5); at the bounds it is
  about 80. The TTL covers a live upload that starts at the end of the horizon
  and runs for its whole segment, and the node's abandon decision after it.
  An upload that still outlives it, for instance a buffered backlog queued
  behind other segments, gets a fresh token for the same attempt and target
  from `ObjectService.Renew`.
- An attempt without an `ObjectStored` at the end of its TTL becomes
  `ABANDONED`, and an object none of whose attempts stored anything is
  removed `abandon_grace` (1 hour) later. Both are provisional: a late
  `ObjectStored` still wins ([§14](#14-duplicates-and-orphans)).
- If a segment's actual `date_started` falls into a different epoch than the
  expected one (e.g. the set restarted), the producer discards the allocation
  and asks again.

**What the horizon costs.** An early allocation reflects cluster state at the
time it was issued, up to one horizon old:

| Change during the horizon | Effect |
|---|---|
| Device quarantined or sink CRITICAL | The CP tells the node at once ([§35.7](12-api.md#357-storage-node-control-api)), so the PUT is refused and the producer moves to the next candidate in its allocation |
| Node added | Keys that would move to the new node move up to one horizon late. Negligible |
| Set powers off | Up to one horizon of allocations per camera expire as `ABANDONED`. These rows are pruned ([§20.4](06-retention-gc.md#204-row-retention)) |

**What the horizon buys.** Commit and allocation are separate failure domains:
Storage Nodes commit without the CP, and only index updates queue up ([§12.4](#124-commit-semantics)).
During a CP outage:

| Function | Behavior | Lasts |
|---|---|---|
| Writes | continue on allocations in hand | `allocation_horizon` |
| Commits | succeed on the node; `ObjectStored` events queue in node RAM (~200 B each) | hours |
| GC | no approvals, so sinks drift from `low` toward `critical` | ~16 h from 5% to 3% free at 5.6 MB/s per sink ([§26.3](08-sizing.md#263-worked-example-cctv)); at CRITICAL the node refuses new uploads on that sink |
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
                      [Shale-Date-Started: t]      (first request)
                      [Shale-Date-Ended: t]        (the request that completes;
                                                    a trailer when chunked)
                      body = bytes from o  (Content-Length, or chunked)
       → 201                       upload complete and durable (commit)
       → 204  Upload-Offset: cur   request accepted; bytes below cur are written
       → 409  Upload-Offset: cur   o does not match the node's offset
       → 413                       the upload would exceed max_length (§12.5)
HEAD /objects/<key>
       → Upload-Offset: cur, Upload-Complete: ?0 | ?1, [Upload-Length],
         [Shale-Incomplete: ?1]    (finalized by the node, §15)
```

- `Upload-Complete: ?1` means "the end of this request body is the end of the
  object". A body that ends cleanly completes the upload. A broken connection
  leaves it open.
- After a disconnect the producer asks `HEAD` for the offset and sends the
  rest from there. Bytes the node already has are never sent again. A put
  token authorizes `HEAD` on its key ([§33.2](10-security.md#332-access-tokens)).
- **The end of every request is a flush point.** When a request with
  `Upload-Complete: ?0` ends, the node writes the 4 KiB-aligned prefix of what
  it holds and answers `204` with the offset now on the device. The unaligned
  tail stays in the buffer, so the producer must keep the bytes above the
  reported offset until a later response covers them.
- **One writer per key.** A new `PUT` on a key with a request still open ends
  the older request first, flushing its aligned prefix as on a disconnect.
  Two connections never append to one file at once.
- The only upload state is the file itself: its xattr says `open` or
  `complete`, and its size is the offset. There is no part list and no
  separate "complete" call.

**Two ways to upload.**

| | Buffered | Live (default; chosen by negotiation, [§12.6](#126-upload-profile-negotiation)) |
|---|---|---|
| Starts | when the segment is complete | when the segment starts |
| Request | one request, `Content-Length` = L | one long request (≈ segment duration), chunked |
| Size at start | known (`Upload-Length`) | unknown; `Shale-Size-Hint` = the source's expected rate × duration × 1.2 ([§12.6](#126-upload-profile-negotiation)), capped at `max_length` |
| Uplink | a burst per segment, smoothed by staggering | exactly the recording bitrate |
| Data reaches storage | after the segment closes | within one part fill time (≈ 16–32 s at 4–8 Mbps) |
| If the producer is destroyed | its buffered segments are gone | only bytes not yet written to a sink are gone ([§15](#15-partial-objects)) |

For live uploads, the node reserves `size_hint`. If the stream outgrows it,
the node reserves another extent; the object may then be in two pieces, which
is harmless. At completion the unused reservation beyond EOF is released.

**Producer retention.** The node reports an offset once the bytes are written
to the device, before any `fsync`. A producer chooses how long it keeps the
bytes:

| `retain` | Producer RAM | If the node loses unsynced data (power loss) |
|---|---|---|
| `committed` (default) | the whole segment until `201` | resume from the lower offset, or re-upload elsewhere |
| `written` | what is above the last reported offset: the segment is sent as a series of requests with `Upload-Complete: ?0`, and the producer frees everything below each `204`'s offset | the segment is lost |

`written` is meant for RAM-constrained producers. It follows the
loss-tolerant principle, but the loss is real. Its requests may be any
4 KiB-aligned size. Requests of about `part_size` (16 MB) or more keep the
device's writes large; smaller ones cost the node a small write each and are
otherwise safe.

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
  producer resends at most a few KiB.
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
| `uploads_per_actor` per node | 64 | new upload from that actor → `503` + `Retry-After` |
| `part_buffer_pool` per node | 12 GiB | stop reading sockets; TCP flow control slows the senders |
| `idle_timeout` (negotiated) | 30 s without bytes | close the connection; the upload stays resumable |
| `abandon_timeout` (negotiated, ≥ 2 × `idle_timeout`) | 5 min without bytes and no open request | abandoned: a live upload is **finalized** as an incomplete object ([§15](#15-partial-objects)); a buffered one is deleted and reported |
| `allocation_ttl` | 22 min | tokens expire; `ObjectService.Renew` before that, or a new attempt after |

Abandonment never fires while a request on the key is open: the idle timeout
closes an idle request first, and only then does the abandon clock count.
Negotiation keeps `abandon_timeout` at least twice `idle_timeout` so the two
cannot be inverted.

With live uploads every camera on a sink has an upload in flight, and two
briefly overlap at each segment boundary. A busy sink carries ~20 cameras
([placement decisions](placement-decisions.md) D8), so `max_uploads = 64` leaves room.

Unlike a minimum-rate rule, none of these penalize a slow but steady link.

**Node restarts.** Upload state is the file, so an upload survives a node
process restart. The xattr of an `open` file records the upload's `mode` and
`abandon_timeout` ([§23.1](07-storage-node.md#231-self-describing-objects)), so
the startup scan can apply the abandon rule without the token: it leaves
`open` files alone until they have been idle for their `abandon_timeout`, then
finalizes live ones and deletes buffered ones. After a power loss, unflushed
tail data may be gone. The file size then reflects only what persisted, so the
producer resumes from a lower offset.

**Links.** A producer's uplink must sustain the sum of its set's
`max_bitrate`s with headroom (≈ 1.2×). Below that, live uploads fall behind
real time and the producer's backlog grows until it drops the oldest segments
([§16](#16-producer-backpressure)). On slow or lossy links the producer:

- in buffered mode, uploads oldest first with 1–2 concurrent uploads
  (parallel streams only compete on a wireless link). In live mode, it runs
  one stream per camera, which together equal the set's bitrate,
- uses TLS, as every client does ([§33.5](10-security.md#335-tls)),
- should use a loss-tolerant TCP congestion control such as BBR.

**Staggered segment boundaries.** A set powers on as a unit, and a power
restore can start a whole site at once, so without staggering every camera
would close its segment at the same instant. Producers cut boundaries by
duration at the nearest keyframe, with a deterministic phase:

```text
phase(camera) = (hash(set) + ordinal × D / set_size) mod D     (D = segment duration)
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
  whose size disagrees with its xattr is damaged: the node deletes it and
  reports it (`ObjectMissing`, [§34.9](11-deployment.md#349-events-and-directives)).

This saves a rename and a directory fsync (extra journal writes, i.e. seeks) on
every object. A file cut off by a crash is simply a lost object, which the
design already accepts.

### 12.4 Commit semantics

**Commit** means: the object is durable in one specific sink, and so on one
physical device.

The Storage Node decides the commit, and the producer learns of it from the
PUT response. The `ObjectStored` event updates the index:

```text
ObjectStored {
  object_id
  attempt_id
  sink_id
  object_key
  size
  incomplete                            (§15)
  date_started, date_ended              (as declared, §10)
  date_committed                        (node clock)
  source_id                             (for diagnostics)
}
```

The IDs and dates the node writes into the object's xattr come from the
`record` in the put token ([§33.2](10-security.md#332-access-tokens)); the
node adds what only it knows: size, state, `date_ended`, and the
`incomplete` flag.

Events are pushed to the CP at least once, with no external queue
([§34.9](11-deployment.md#349-events-and-directives)). The node keeps
unpublished events in RAM only. If the node crashes after step 10 but before
publishing, the event is lost. On startup the node republishes `ObjectStored`
for every file modified within the last `event_replay_window` (10 minutes),
which covers a quick restart. A longer outage is covered by
**reconciliation**: when the node's heartbeats resume, the CP asks it for
every complete file newer than the last commit it knows on each sink
([§34.9](11-deployment.md#349-events-and-directives)). Anything still missed
becomes an orphan, and orphans are reclaimed by GC ([§21.3](06-retention-gc.md#213-orphans)).

### 12.5 Idempotent uploads

- Resending bytes the node already has is harmless. An `Upload-Offset` below
  the node's offset is answered with `409` + the current offset, and the
  producer continues from there.
- If the upload is already complete → `200` (the producer may have missed the
  first `201`). If the node completed it itself, the answer carries
  `Shale-Incomplete: ?1`, and the producer must not treat the segment as
  stored ([§15](#15-partial-objects)).
- A different `Upload-Length` for the same key, or bytes past the end of a
  complete upload → `409 Conflict`.
- Bytes that would take the upload past the token's `max_length` → `413`.
  The node then treats the upload as abandoned at once: a live one is
  finalized as incomplete with the bytes it has, a buffered one is deleted.
  The producer moves to the next candidate with a corrected profile, or
  reports the object.

- **A key that holds bytes the producer never sent** is an earlier
  incarnation's upload of the same slot: the producer restarted mid-slot
  and the CP answered the same object and attempts. The node cannot tell
  whose bytes they are, so the producer does: a `409` whose offset is
  past what it stated, or a `HEAD` offset past what it sent, ends the
  attempt with the reason "the node holds bytes of this key that are not
  ours", the CP keeps that sink eligible, and `Reallocate` hands out a
  fresh attempt with a fresh key. The old file is finalized or removed by
  the abandon rule and ends as a duplicate.


### 12.6 Upload profile negotiation

Cameras differ. A 1 Mbps camera fills 64 MB in 8.5 minutes, and a 16 Mbps
camera fills it in 32 seconds. A producer on a wireless link needs more
patience than one on a LAN. So the upload parameters that depend on the camera
or the link are **negotiated**: the producer proposes, and the CP answers with
the proposal clamped into cluster bounds.

**What is negotiated, and what is not.**

| Negotiated (per source or per set) | Not negotiated |
|---|---|
| `max_bitrate`, segment duration, and keyframe interval (per source) | `part_size`, `max_uploads`, `part_buffer_pool`: the node's own resources |
| upload mode, `live` or `buffered` (per set) | `retain`, `resume_timeout`, retries: the producer's own business |
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
            sources: [{source, max_bitrate, segment_duration, keyframe_interval}, ...]
          }
CP        clamps each value into the active UploadPolicy's bounds and the
          set's own caps, stores the result on the set and its sources, and
          answers {
            profile_version,
            agreed:      the same fields, as they now apply,
            adjustments: [{field, proposed, agreed, reason}, ...]
          }
```

- The producer calls `Negotiate` when it starts and whenever a camera's
  settings change. A set that never negotiated uses the defaults: a segment
  duration of 64 MB ÷ `max_bitrate`, live mode, and the policy's timeouts.
- The producer **must use the agreed values**. When the CP adjusts a segment,
  it keeps the camera's `max_bitrate`, which the producer cannot change, and
  changes the **segment duration** instead. Example: 1 Mbps × 2 min = 15 MB is
  below the 32 MB floor, so the agreed duration becomes ~4.3 min.
- The agreed values travel **in the access token** ([§33.2](10-security.md#332-access-tokens)):
  `max_length`, `mode`, `idle_timeout`, and `abandon_timeout`. A node enforces
  them per upload, within its own hard caps. It needs no knowledge of sets,
  and it cannot be asked for more than the CP agreed to.
- `max_length` is what a stream running at its ceiling can legitimately
  produce for one segment:

  ```text
  max_length = max_bitrate × (segment duration + keyframe interval + 2 s)
  ```

  The keyframe interval is the longest a boundary can wait past its phase,
  since segments cut only at keyframes, and 2 s is one encoder burst: the
  VBV buffer a capped-VBR encoder may release at once
  ([§38.3](15-producer.md#383-managed-capture)). For a 2-minute segment
  that is 3% of headroom; for the 32-second segment of a 16 Mbps camera,
  12%. A flat percentage would be too little for the short one.
- Every allocation carries the current `profile_version`. When an operator
  activates a new `UploadPolicy`, the CP re-clamps stored profiles, the version
  changes, and the producer negotiates again on its next allocation.

**Bitrate is a ceiling, not a rate.** Encoders either hold a constant bitrate
(CBR) or vary it with the picture (VBR). A quiet scene then costs a fraction of
a busy one, while picture quality stays even. Most CCTV scenes are quiet most
of the time, so VBR saves a lot of storage, but its rate is not a number
anyone can declare in advance: the bench measured a constant-quality encoder
at 8.6 Mbps on a noisy night scene ([producer bench](producer-bench.md)).
So a producer declares **`max_bitrate`**:

- the ceiling for **all streams in the segment together**: video, audio, and
  anything else the container carries;
- met by CBR, or by **VBR with a cap** (recommended). Uncapped constant-quality
  encoding has no ceiling to declare, and the node refuses bytes beyond
  `max_length`;
- used for everything that must hold in the worst case: `max_length`, the
  object size bounds, and the link check below;
- bounded twice: the `UploadPolicy` caps any single `max_bitrate` (default
  32 Mbps), and a tenant admin may cap a set's total (`max_bitrate_total`), so
  a mistaken or compromised producer cannot multiply its footprint and eat
  the retention of the tenant's other cameras;
- chosen by the producer, or for it: `max_bitrate: auto` starts from the
  camera's mode and is raised from observation
  ([§38.5](15-producer.md#385-choosing-the-ceiling)).

Segments are still **cut by duration**, at the keyframe nearest the staggered
phase ([§12.2](#122-resumable-part-uploads)), or earlier when a segment
approaches `max_length` ([§38.2](15-producer.md#382-cutting-segments)).
Durations keep segment boundaries deterministic and time ranges aligned. Under VBR, the object size varies
instead: a quiet segment is small, and a busy one approaches `max_length`. The
bounds apply to the ceiling, `max_bitrate × duration`, which should be at
least 32 MB and must be at most 512 MB. A small object from a quiet scene is
fine: the number of objects per hour is fixed by the duration, and a quiet
camera costs a sink little time.

Two rules bound the duration from above: the 512 MB ceiling and "a segment
lasts at most a quarter of the epoch" ([§25](07-storage-node.md#25-object-size)).
For a source under about 0.28 Mbps (audio only, a very low-resolution camera)
the 32 MB floor cannot be reached within a quarter epoch. **The epoch rule
wins**: the duration is capped at `epoch / 4` and the floor is a target, not
a bound. Such objects are small anyway, as VBR objects already are.

The **keyframe interval** is negotiated with the rest of the profile
(default 2 s, at most 4 s). Segments can only be cut at keyframes, so the
interval bounds how far a boundary drifts from its phase, and it is part of
`max_length` above.

**Observed rate.** What a source actually writes is learned from what it
commits. For each source the CP keeps an **observed rate**, the size of its
committed objects divided by their data time span
([§10](02-data-model.md#10-time-semantics)):

```text
recent       exponentially weighted average, half-life of one epoch
expected     max(recent, the rate in the same hour of the previous day)
```

The second term anticipates daily patterns: the noise that raises bitrates at
dusk shows up in yesterday's figures before it shows up in the last hour's.
`expected` is capped at `max_bitrate`. Until a source has committed anything,
`expected = max_bitrate`.

The observed rate feeds the estimates, never the limits:

| Uses the expected (observed) rate | Uses `max_bitrate` |
|---|---|
| `Shale-Size-Hint` for live uploads | `max_length` in the access token |
| capacity forecast ([§11.1](03-placement.md#111-capacity-forecast)) | object size bounds |
| "time until the cluster runs out" | link check: the uplink must carry the sum of its set's ceilings |
| the estimated end of an incomplete object ([§19](05-read-path.md#19-reader-semantics)) | |

The observed rate is state, but it is **derived**: it can be recomputed at any
time from the objects table, so it adds no new source of truth. It is cached on
the `Source` row.

**Why bounds.** The lower size bound keeps the per-object fixed cost small on
HDDs ([§25](07-storage-node.md#25-object-size)). The upper bound caps node RAM,
the loss unit, and upload duration. Timeout bounds keep an abandoned upload
from holding a node's slot indefinitely, and keep a slow link from being cut
off too early.

## 13. Retry and Reallocation

### Same-target retry (resume)

Transient transport errors are retried against the same target: connect/TLS
timeouts, connection reset, `502`, `504`, and `503` (honoring `Retry-After` up
to `retry_after_cap`, default 2 minutes). An interrupted upload resumes from
its offset ([§12.2](#122-resumable-part-uploads)).

The limit is based on progress, not attempt count. A slow link may reconnect
many times. The producer keeps resuming as long as each attempt advances the
offset, and gives up on the target after `resume_timeout` (e.g. 2 minutes)
without progress, or when the token cannot be renewed.

### Placement retry

After that, the producer moves to the next candidate: one returned with the
allocation, each of which carries its own `attempt_id` and token, or one from
`ObjectService.Reallocate`. Before it moves, it reports the failed attempt
(`ObjectService.ReportAttempt {attempt, failure_reason}`), which marks the
attempt `FAILED` and feeds health ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)).

```text
object O123
attempt A1 → sink S-0412 (node17) → FAILED (I/O error)
attempt A2 → sink S-0087 (node42) → STORED
```

### Immediate reallocation

These errors skip same-target retries: `ENOSPC`, device I/O error, sink or
device unavailable or quarantined, `413`, explicit reject.

### Limits

```text
same-target:  until resume_timeout without progress
placement:    2–3
```

When retries are exhausted, the object → **LOST**
(`ObjectService.ReportFailure`), and the producer drops the segment. One
object never blocks the ingest pipeline.

## 14. Duplicates and Orphans

**One physical copy is best effort.** If attempt A1 is slow rather than dead,
it can commit after A2 did.

- The index has a unique constraint on `object_id`. The first
  `ObjectStored` wins, and later ones for *other* attempts mark their attempt
  `DUPLICATE`. Redelivery of the same event is recognized by `attempt_id` and
  changes nothing.
- Exception: a **complete** attempt always beats an **incomplete** one ([§15](#15-partial-objects)).
  If an abandoned live upload was finalized first and the producer later
  stored the full segment elsewhere, the full one replaces it, and the
  truncated one becomes the duplicate.
- **A late event still counts.** An `ObjectStored` for an attempt already
  marked `ABANDONED` or `FAILED` moves it to `STORED` (or `DUPLICATE`), an
  object already `LOST` returns to `COMMITTED`, and an object whose row was
  removed is recreated from the event. The node's word about what is on its
  device is final; the CP's timeouts are guesses.
- A duplicate's file is left for GC, which reclaims it once its xattr dates
  say so ([§21.3](06-retention-gc.md#213-orphans)), or at once when the
  object is deleted early ([§20.3](06-retention-gc.md#203-rescheduling)).

**Orphans** are files in a sink that the index does not know: a lost commit
event, a duplicate nobody deleted, or data left over after an index loss.
They waste space but never break correctness. Reconciliation turns most of
them back into known objects ([§34.9](11-deployment.md#349-events-and-directives));
GC reclaims the rest after their xattr `date_expired`
([§21.3](06-retention-gc.md#213-orphans)).

**Missing objects** are index entries whose file is gone. They are detected
lazily: when a valid token names a key the node does not have, the node
answers `404` and pushes `ObjectMissing`. The CP marks the object `LOST`,
unless it was `DELETING`, in which case the deletion is simply confirmed
([§21.2](06-retention-gc.md#212-protocol)). The node reports its own
housekeeping the same way: a damaged file it removed ([§12.3](#123-why-there-is-no-temp-file-and-no-rename))
or an abandoned buffered upload it deleted ([§15](#15-partial-objects)).

## 15. Partial Objects

A segment can end early in two ways.

**The camera stops** (set powered off, camera fault). The producer still
controls the upload. It ends the segment where the recording stopped and
completes the upload normally, with the real `date_ended` and size. If the
container is broken, the producer drops the segment instead. When frames
return within the same slot, the next segment is a second object of that
slot ([§12.1](#121-flow)); the slot's phase boundary cuts it as usual.

**The producer disappears mid-upload** (destroyed, powered off, link gone for
good). This case matters for CCTV: the last minutes before a producer is
destroyed are often the most important footage. With live upload, most of the
segment is already on a sink. When such an upload has been idle for
`abandon_timeout`, the node:

1. flushes and keeps what it has,
2. finalizes the file as a committed object flagged **`incomplete`**, with
   the size it actually has and no `date_ended`,
3. publishes `ObjectStored` with `incomplete: true`.

From then on `HEAD` and a resumed `PUT` answer with `Shale-Incomplete: ?1`
([§12.5](#125-idempotent-uploads)). A producer that comes back and sees it
knows the node has closed the object without the tail. If it still holds the
segment (`retain: committed`), it uploads the whole segment to the next
candidate, and the complete copy replaces the incomplete one
([§14](#14-duplicates-and-orphans)).

The node does not parse media. Producers that use live upload must use a
**streamable container** (MPEG-TS, fragmented MP4) so that any prefix is
playable up to its last complete unit. The media server works out the real
duration when it reads the object; the index carries an estimate
([§19](05-read-path.md#19-reader-semantics)).

A buffered upload that is abandoned is deleted instead, and the node reports
it (`ObjectMissing`). Its producer still holds the whole segment and will
have re-uploaded it elsewhere if it could.

## 16. Producer Backpressure

When no candidate accepts a write (all sinks at `max_uploads`, all targets failing), the
producer buffers segments **in RAM** and retries with backoff. When its buffer
is full, it drops the oldest unstored segment and reports it LOST.

Producer parameters:

- upload mode: `live` or `buffered`
- `retain`: `committed` or `written` ([§12.2](#122-resumable-part-uploads))
- RAM buffer size (`committed`: ≥ 2 segments per camera it serves, plus
  headroom; `written`: what is above the last written offset per camera)
- maximum age of a buffered segment
- retry backoff
- max concurrent uploads
