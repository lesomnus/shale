# Shale — Read Path

## 17. Read Path

### 17.1 Flow

```text
1. Reader   LaminaService.Timeline {set, from, to, size, after}      (whole set, per member)
            LaminaService.Timeline {source, from, to, size, after}   (one camera)
            LaminaService.Get {ref}
2. CP       one page of laminae with states, gaps with reasons, read tokens
            (GET URLs), and a cursor for the next page
3. Reader   GET <node>/laminae/<key>   (Range supported), members in parallel
```

A time-range query returns explicit **gaps** for any part of the range that has
no available lamina ([§19](#19-reader-semantics)). What the bytes are is
the source's `content_type` ([§7](02-data-model.md#7-source-set-zone-epoch)):
the node serves them as `application/octet-stream`, since it knows no
sources, and the reader knows from the source what it is reading. This is the path for
recordings. A camera *now* is watched through the relay instead
([§39](16-relay.md#39-relay)). It is paged like every
`List` ([§35.1](12-api.md#351-conventions)): at most `timeline_page` laminae
(default 1,000) per answer, each with its own read token, so a query over a
month of a large set is many small answers rather than one of tens of
megabytes.

### 17.2 Read chunks

Lamina size and read scheduling unit are separate:

```text
lamina        32–512 MB   storage efficiency
read chunk    ~16 MB      latency / fairness in the Device Queue
```

A GET is served as a sequence of READ jobs, one per chunk. The file descriptor
stays open for the whole session (see [§18](#18-delete-during-read)).

### 17.3 Prefetch

For sequential playback the node may prefetch the next chunk, and the next
lamina of the same source, into the session's RAM buffer. This is optional and
workload-specific.

### 17.4 Read locality and parallelism

- **One camera**: all its laminae within an epoch live in one sink, so a
  single-camera playback reads from one device at a time. That is accepted:
  one HDD (~100+ MB/s) is far faster than one camera stream (~0.5 MB/s).
- **One set**: members live on distinct devices, and on distinct nodes where
  possible ([§11](03-placement.md#11-placement)), so a whole-set read runs in parallel. Exporting one hour of
  an 8-camera set (8 × 1.8 GB at 4 Mbps) takes ~15 s from 8 devices, against
  ~115 s if the set were packed on one device. Real-time playback is not
  device-bound either way. The difference shows up in exports and fast seeking.

Read sessions are limited per node (`max_read_sessions`, 64) and per caller
(`sessions_per_actor`, 16, from the token's `actor` claim,
[§33.2](10-security.md#332-access-tokens)), so one media server cannot take a
node's whole read capacity from the others.

## 18. Delete During Read

POSIX keeps an unlinked file readable through already-open descriptors:

```text
Reader:  open(lamina) → fd
GC:      unlink(lamina)
Reader:  read(fd) → still works
```

- New readers can no longer open the lamina.
- Existing sessions finish.
- The blocks are freed when the last fd closes.

So GC must measure real free space (`statfs()`, or the sink's own accounting,
which counts unlinked-but-open files, [§22.2](07-storage-node.md#222-sinks-and-devices)), not the sum of the sizes it
deleted. Under CRITICAL pressure the node may abort **stalled or long-idle**
read sessions that hold deleted files:

```text
NORMAL / RECLAIM   existing readers are protected
CRITICAL           abort stalled readers of deleted laminae;
                   limit new read admission; writes take priority
```

## 19. Reader Semantics

Lamina states exposed to readers:

```text
AVAILABLE     (possibly flagged incomplete: tail missing, date_ended estimated)
UNAVAILABLE   (sink/device/node down or quarantined for reads)
LOST          (Shale was given the data, or tried to take it, and lost it)
DELETED       (date_deleted has passed, §20.1, or GC has deleted or is
               deleting the lamina, §21.2)
NOT_FOUND     (unknown lamina ID)
```

A lamina in `DELETING` reads as `DELETED`: no token is issued for it and it
cannot be rescheduled, whatever its `date_deleted` says.

An **incomplete** lamina has no declared `date_ended`. The index carries an
**estimated** one, `date_started + size ÷ expected rate` of its source
([§12.6](04-write-path.md#126-upload-profile-negotiation)), marked as an
estimate, so time-range queries and the gap after it have a boundary. The
media server finds the real end when it reads the lamina.

Time-range queries return **gaps** with a reason:

```text
NOT_RECEIVED  no upload was ever attempted for this span:
              the camera or set was off, or the producer failed before upload
IN_PROGRESS   an attempt is open for this span right now: a live upload
              in progress, or a segment not yet committed
LOST / DELETED / UNAVAILABLE   as above, for spans covered by such laminae
```

For CCTV the difference between "the camera was not recording" and "the
storage lost it" matters, e.g. when footage is used as evidence. Shale derives
it without any extra reporting from the producer: a span is `LOST` only if an
attempt exists for it. A `NOT_RECEIVED` gap that covers every member of a set
at once almost always means the set was off. Spans older than any row the
index still holds are answered from policy
([§20.4](06-retention-gc.md#204-row-retention)).

A read that reaches a node and finds no file gets `404`; the node reports it
(`LaminaMissing`, [§34.9](11-deployment.md#349-events-and-directives)) and the
CP marks the lamina `LOST`, or confirms its deletion if it was `DELETING`
([§14](04-write-path.md#14-duplicates-and-orphans)).
