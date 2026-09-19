# Shale — Read Path

## 17. Read Path

### 17.1 Flow

```text
1. Reader   ObjectService.Timeline {set, from, to}      (whole set, per member)
            ObjectService.Timeline {source, from, to}   (one camera)
            ObjectService.Get {ref}
2. CP       objects with states, gaps with reasons, and read tokens (GET URLs)
3. Reader   GET <node>/objects/<key>   (Range supported), members in parallel
```

A time-range query returns explicit **gaps** for any part of the range that has
no available object ([§19](#19-reader-semantics)).

### 17.2 Read chunks

Object size and read scheduling unit are separate:

```text
object        32–512 MB   storage efficiency
read chunk    ~16 MB      latency / fairness in the Device Queue
```

A GET is served as a sequence of READ jobs, one per chunk. The file descriptor
stays open for the whole session (see [§18](#18-delete-during-read)).

### 17.3 Prefetch

For sequential playback the node may prefetch the next chunk, and the next
object of the same source, into the session's RAM buffer. This is optional and
workload-specific.

### 17.4 Read locality and parallelism

- **One camera**: all its objects within an epoch live in one sink, so a
  single-camera playback reads from one device at a time. That is accepted:
  one HDD (~100+ MB/s) is far faster than one camera stream (~0.5 MB/s).
- **One set**: members live on distinct devices, and on distinct nodes where
  possible ([§11](03-placement.md#11-placement)), so a whole-set read runs in parallel. Exporting one hour of
  an 8-camera set (8 × 1.8 GB at 4 Mbps) takes ~15 s from 8 devices, against
  ~115 s if the set were packed on one device. Real-time playback is not
  device-bound either way. The difference shows up in exports and fast seeking.

## 18. Delete During Read

POSIX keeps an unlinked file readable through already-open descriptors:

```text
Reader:  open(object) → fd
GC:      unlink(object)
Reader:  read(fd) → still works
```

- New readers can no longer open the object.
- Existing sessions finish.
- The blocks are freed when the last fd closes.

So GC must measure real free space (`statfs()`, or the sink's own accounting,
which counts unlinked-but-open files, [§22.2](07-storage-node.md#222-sinks-and-devices)), not the sum of the sizes it
deleted. Under CRITICAL pressure the node may abort **stalled or long-idle**
read sessions that hold deleted files:

```text
NORMAL / RECLAIM   existing readers are protected
CRITICAL           abort stalled readers of deleted objects;
                   limit new read admission; writes take priority
```

## 19. Reader Semantics

Object states exposed to readers:

```text
AVAILABLE     (possibly flagged incomplete: tail missing, date_ended unknown)
UNAVAILABLE   (sink/device/node down or quarantined for reads)
LOST          (Shale was given the data, or tried to take it, and lost it)
DELETED       (date_deleted has passed, §20.1)
NOT_FOUND     (unknown object ID)
```

Time-range queries return **gaps** with a reason:

```text
NOT_RECEIVED  no upload was ever attempted for this span:
              the camera or set was off, or the producer failed before upload
LOST / DELETED / UNAVAILABLE   as above, for spans covered by such objects
```

For CCTV the difference between "the camera was not recording" and "the
storage lost it" matters, e.g. when footage is used as evidence. Shale derives
it without any extra reporting from the producer: a span is `LOST` only if an
attempt exists for it. A `NOT_RECEIVED` gap that covers every member of a set
at once almost always means the set was off.
