# Shale — Data Model

## 7. Source, Set, Zone, Epoch

A **Tenant** owns sets, sources, objects, and the holders (producers, readers,
admins) that act on them. It is payday's tenant, and the wall around it is
always on ([§33.1](10-security.md#331-trust-model)). A single organization runs
a cluster with exactly one tenant, created by `shale cluster init`, and never
has to name it: slugs leave it out and every holder belongs to it. Adding
tenants later needs no change to code, schema, or clients. Storage
infrastructure (nodes, devices, sinks) is not owned by any tenant.

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

A **Site** groups sets within a tenant, e.g. a building or a branch. It is
payday's second permission axis (field 3): a holder can be limited to the
sites it is a member of, so a guard at one building cannot read another's
cameras ([§33.1](10-security.md#331-trust-model)). A set belongs to at most one
site, fixed when the set is added. Its sources, objects, and attempts carry the
same site. A tenant that does not use sites leaves the field empty and sees no
difference.

A **Zone** is an optional label, assigned by people, for Sources whose fields
of view overlap. A zone may cross sets. It describes redundancy between angles,
not fate-sharing. The v1 scheduler does not use it, and zone-aware placement is
not planned ([§11.2](03-placement.md#112-scheduler-interface)). The label is
stored so a future scheduler could. Shale needs no camera geometry beyond
these labels: the only placement question is which Sources should not share a
failure domain.

An **Epoch** is a fixed time bucket (default 1 hour). The epoch of an object is
derived from its `date_started`. During one epoch, all objects of a Source go to
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

`DELETED` is also reached without any state change: an object whose
`date_deleted` has passed is deleted, whatever its stored state says
([§20.1](06-retention-gc.md#201-date_deleted-is-an-expiry-not-an-event)).

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
  date_created
  date_finished
```

## 9. Identity

Every entity's ID is payday's UUIDv8: time-ordered, with a byte naming the
entity kind ([§35.1](12-api.md#351-conventions)). People name rows by slug,
e.g. `cam-03#source`, or `@acme/cam-03#source` when there are several tenants.

### Node

`node_id` is independent of hostname and IP.

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
- **System times** are stamped by Shale: `date_created` (allocation, CP clock),
  `date_committed` (commit, node clock), `date_finished` (attempt end).
- **Data times** are `date_started` and `date_ended`: the time span the
  object's data covers, **declared by the producer**. Shale does not interpret
  them. It indexes them for time-range queries and derives the epoch from
  `date_started`. A producer that declares nothing gets the write times
  instead (first byte and commit).
- The two differ whenever an upload is not live. A buffered segment arrives a
  segment-length late, and a backlog after a link outage can arrive hours
  late. A pre-allocated object's `date_created` even precedes its data. Data
  times keep time-range queries and epochs correct in all these cases, with no
  knowledge of what the data is.
- Clocks are NTP-synchronized. The CP rejects or clamps `date_started` values
  that are too far from its own clock (tolerance: [§36.1](13-configuration.md#361-configuration-reference)).
