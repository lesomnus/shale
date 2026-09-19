# Shale — Data Model

## 7. Source, Set, Zone, Epoch

A **Source** is what produces objects: one camera.

A **Set** is a group of Sources behind one producer. Its members:

- are **fate-shared**: they power on and off together, and a producer failure
  stops them all;
- are **read together**: a reader usually wants every member for the same
  time span;
- are registered and removed as a unit.

Each member has a stable, never-reused **ordinal** within its set (0, 1, 2, …),
assigned at registration. Placement uses it to spread members apart ([§11](03-placement.md#11-placement)), and
Writers use it to stagger segment boundaries ([§12.2](04-write-path.md#122-resumable-part-uploads)).

A **Zone** is an optional label for Sources whose fields of view overlap. A
zone may cross sets. It describes redundancy between angles, not fate-sharing.
Placement does not use it yet ([§34](11-open-decisions.md#34-open-decisions)). Shale needs no camera geometry beyond
these two labels: the only placement question is which Sources should not
share a failure domain.

An **Epoch** is a fixed time bucket (default 1 hour). The epoch of an object is
derived from its `start_time`. During one epoch, all objects of a Source go to
the same sink as long as that sink stays eligible. When the epoch changes, the
Source moves to another sink.

```text
camera-17
10:00–10:59 → sink S-0412
11:00–11:59 → sink S-0087
12:00–12:59 → sink S-0339
```

## 8. State Model

Object state and attempt state are separate machines.

### Object

```text
PENDING ──► COMMITTED ──► DELETING ──► DELETED
   │            │
   │            └──► LOST        (file found missing on read / device declared dead)
   ├──► LOST                     (Writer tried and retries were exhausted)
   └──► (discarded)              (only ABANDONED attempts: nothing was uploaded)
```

An object whose allocations were never used is removed. It reads as
`NOT_RECEIVED`, not `LOST`, because Shale was never given any data ([§19](05-read-path.md#19-reader-semantics)).

A committed object may carry the `incomplete` flag ([§15](04-write-path.md#15-partial-objects)). It is still
`COMMITTED`; the flag only says its tail is missing.

`UNAVAILABLE` is not stored. It is derived at read time from the health of the
object's sink, device, and node.

### Write attempt

```text
ALLOCATED ──► STORED
    │
    ├──► FAILED       (with failure_reason)
    ├──► ABANDONED    (never used; allocation TTL expired)
    └──► DUPLICATE    (stored, but another attempt won)
```

```text
write_attempts
  attempt_id
  object_id
  sink_id
  state
  failure_reason
  created_at
  finished_at
```

## 9. Identity

### Node

`node_id` is a UUID, independent of hostname and IP.

### Device

`device_id` comes from the hardware (WWN, or serial). For volumes Shale cannot
see through, it is the filesystem UUID. It is independent of `/dev/sdX`.

### Sink

`sink_id` is assigned when the sink is created and stored in its label
(`.shale-sink`), together with the device identity seen at that time. A sink
whose label and current device disagree is flagged rather than trusted (e.g.
a directory copied onto another device).

### Logical slot

The physical bay is separate:

```text
logical_slot = chassis-01/bay-17
old HDD:      device_id = WWN-A, sink_id = S123
replacement:  device_id = WWN-B, sink_id = S987
```

A replacement HDD gets a new sink and never reuses the old `sink_id`.

## 10. Time Semantics

- Stored times are UTC.
- `start_time` / `end_time` come from the Writer, i.e. media time. The epoch is
  derived from `start_time`, so a delayed retry still lands in its original
  epoch.
- `created_at` / `committed_at` come from CP / node clocks.
- Clocks are NTP-synchronized. The CP rejects or clamps `start_time` values
  that are too far from its own clock (tolerance: [§34](11-open-decisions.md#34-open-decisions)).
