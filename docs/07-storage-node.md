# Shale — Storage Node

## 22. Storage Node Internals

### 22.1 HDD layout

Each HDD is an independent filesystem.

```text
Storage Node
├─ OS            → SSD/NVMe (OS, binaries, logs only)
├─ HDD 01        → XFS
├─ HDD 02        → XFS
├─ HDD 03        → XFS
└─ ...
```

- No hardware RAID, no mdraid, no multi-HDD ZFS pool
- One XFS (or ext4) filesystem per HDD, **with its journal on the same HDD**.
  An external journal on a shared SSD would save one seek per object, but it
  would turn one SSD failure into the loss of every filesystem on the node,
  which breaks the per-device failure isolation.
- HBA in IT/JBOD mode
- Never use `/dev/sdX` as identity. A device is identified by its WWN/serial,
  and the sink on it by the label written at its root ([§9](02-data-model.md#9-identity)).

### 22.2 Sinks and devices

A **Sink** is where Shale stores objects: a directory registered on a node,
with its own label, capacity, pressure state, and GC. Placement chooses sinks,
and an object's location is `(sink_id, object_key)`.

A **Device** is the physical block device under a sink. The node detects it at
registration (`st_dev` → block device → WWN/serial; for a volume Shale cannot
see through, such as a RAID set, NAS mount, or cloud volume, the filesystem
UUID). The **device**, not the sink, is:

- the **I/O unit**: one Device Queue per device, shared by all its sinks ([§24](#24-device-queue-and-scheduling));
- the **failure domain**: placement spreads sets across devices ([§11](03-placement.md#11-placement)), and
  health and quarantine are tracked per device ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)).

**In production each HDD is one device holding exactly one sink, mounted
whole.** The documents still keep the terms apart. *Sink* is where objects
live (placement, location, capacity, GC). *Device* is what fails and what
serializes I/O. *HDD* is used only for hardware and media behavior.

Other layouts are supported for low-budget deployments and testing:

| Layout | Typical use | Notes |
|---|---|---|
| One sink per whole HDD | production | capacity and free space from `statfs` |
| Directory sink on a shared filesystem | single-HDD nodes, existing RAID/NAS/cloud volumes | `capacity` required |
| Several sinks on one device | tests only | no spread benefit; node warns in its heartbeat |
| tmpfs or container directory | CI, development | no `O_DIRECT`; buffered fallback |

A volume with redundancy underneath (RAID, NAS) counts as a single device.
Shale neither sees nor relies on that redundancy.

```yaml
sinks:
  - path: /mnt/hdd01           # whole-HDD mount: device and capacity detected
  - path: /srv/shale/a
    capacity: 500GiB           # required when the filesystem is shared
```

**Capacity on a shared filesystem.** A sink with a declared `capacity`
accounts for its own usage: the files in the sink, the extents reserved for
uploads in progress, and unlinked files still held open by readers ([§18](05-read-path.md#18-delete-during-read)),
which the node knows because it owns those descriptors. Its pressure state
([§21](06-retention-gc.md#21-lazy-gc)) is the worse of two measures: headroom within `capacity`, and the
filesystem's real free space. Other tenants can fill a shared filesystem, so
the second one remains a hard floor.

**Capability probe.** At registration the node tests the sink's filesystem:

| Feature | Required | Without it |
|---|---|---|
| user xattrs | **yes** | registration fails, because self-describing metadata and upload state live there ([§23.1](#231-self-describing-objects), [§12.2](04-write-path.md#122-resumable-part-uploads)) |
| `O_DIRECT` | no | buffered writes, `fsync` at commit; page-cache effects return ([§22.4](#224-bypass-the-page-cache)) |
| `fallocate` | no | no extent reservation; objects may fragment |

Probe results are reported in heartbeats and shown by `shale sink list`.

**Development mode.** `shaled --dev <dir>` runs the Control Plane and one
Storage Node in one process, with a single directory sink. The multi-sink
warning is suppressed.

### 22.3 No SSD in the data path

**Object data is never written to SSD/NVMe**, not even temporarily. There is
no persistent spool. Reasons:

- A persistent spool writes every byte twice and would wear out SSDs quickly
  (a node ingesting 5 GB/s writes ~430 TB/day).
- A spool's main job was to protect data that the Writer had already been
  told was stored. Under the commit contract in [§12](04-write-path.md#12-write-path), the Writer keeps the
  segment until the data is durable on HDD, so nothing needs protecting.

The only buffer is a bounded pool of RAM **part buffers** ([§12.2](04-write-path.md#122-resumable-part-uploads)).

### 22.4 Bypass the page cache

This is about the page cache, not swap. Swap should be disabled or unused.

With buffered writes, data first becomes dirty pages and the kernel flushes it
in the background. With many devices writing at once, the global dirty limit is
shared: one slow device can throttle writers to every other device, and a large
`dirty_ratio` just produces larger `fsync` stalls. More RAM does not solve this.

Shale already stages each part in its own RAM buffer, so it writes parts with
**`O_DIRECT`** at aligned offsets. Reads also use `O_DIRECT`
with Shale's own chunking and prefetch ([§17](05-read-path.md#17-read-path)). Memory use is then explicit, and
each device sees exactly the I/O its Device Queue issued.

### 22.5 Sequential I/O on HDD

What matters is not whether the OS "knows" the I/O is sequential, but:

- large contiguous extents are allocated (`fallocate` the full size up front),
- I/O from different objects is not interleaved on a device (one Device Queue),
- `fsync` is rare (once per object),
- the filesystem does not fragment.

Rough time scales: kernel scheduling µs to tens of µs, HDD seek a few ms, one
7200rpm revolution ≈ 8.3 ms.

## 23. Object Model and Storage Format

An **Object** is the unit of storage and is immutable.

### 23.1 Self-describing objects

Each object file carries its own metadata in an **inline extended attribute**
(`user.shale`), so a sink alone is enough to rebuild its part of the index.

- The xattr is set before the data is written, and it is persisted by the
  **same** `fsync` that makes the data durable, in the same journal
  transaction. It adds no extra seek.
- Keep the record compact (≤ ~200 bytes, versioned binary encoding) so it
  fits inline in a 512-byte XFS inode (the `mkfs.xfs` default).
- The file content stays the raw segment, so a mounted sink can be inspected
  with ordinary tools (e.g. `ffprobe`).

Record fields:

```text
format_version
object_id
attempt_id
source_id
set_id            (optional)
start_time        (optional)
end_time          (optional)
state             (open | complete)
size_bytes        (final size, set when the upload completes; see §12.3)
size_hint         (live uploads: expected size used for the reservation)
incomplete        (true if finalized from an abandoned live upload, §15)
expires_at
must_delete_by    (optional)
checksum          (optional, §30)
placement_version
```

Each sink carries a label file at its root (`.shale-sink`) with `sink_id`,
`cluster_id`, the device identity seen at creation, and creation time.

### 23.2 Paths

```text
/<mount>/objects/<yyyy>/<mm>/<dd>/<hh>/<object_id>.<attempt_id>
```

- Hourly directories (from `start_time`, or from the commit time if absent)
  keep directories small and make time-ordered scans cheap.
- The attempt suffix means two attempts of one object never collide.

### 23.3 Index metadata (Control Plane)

```text
object_id
source_id
set_id
start_time
end_time
sink_id
object_key
size_bytes
state
incomplete
expires_at
must_delete_by
hold
placement_version
created_at
committed_at
```

`source_id`, `set_id`, `start_time`, and `end_time` are optional for
non-time-series workloads.

An object's location is **`(sink_id, object_key)`**. The node is *not* part of
the location: it comes from the `sinks` table (`sink_id → node_id, device_id`),
so a device can be moved to another node without rewriting object rows ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)).

URLs are never stored. They are generated on demand.

## 24. Device Queue and Scheduling

Each physical device has exactly one **Device Queue** and one worker, shared by
all sinks on that device ([§22.2](#222-sinks-and-devices)), so I/O from different jobs is never
interleaved on the device.

Jobs are bounded:

| Class | Job | Size bound |
|---|---|---|
| WRITE | one part of an upload (the last one also does the `fsync`) | `part_size` (e.g. 16 MB ≈ 90 ms) |
| READ | one read chunk | `read_chunk` (e.g. 16 MB ≈ 90 ms) |
| MAINT | unlink batch, startup scan step | small, time-bounded |

### 24.1 Starvation-free scheduling

Writers push continuously, so a strict priority would starve someone. Instead
the worker uses **deficit round robin (DRR) by bytes** across classes:

- Each class has a weight (default `WRITE : READ : MAINT = 1 : 1 : 0.1`, with a
  minimum quantum so MAINT always progresses).
- **Work-conserving**: an idle class's share goes to the others. A device with no
  readers writes at full speed.
- **Bounded wait**: a backlogged class is served at least once per round, so a
  READ waits at most about one WRITE part (~0.1 s) plus one MAINT step.
- Within READ, sessions are served round-robin, so one long export cannot
  monopolize a device.
- Queues are bounded. The WRITE backlog is bounded by the part buffer pool
  ([§12.2](04-write-path.md#122-resumable-part-uploads)). The READ backlog is capped per device. Excess work is refused
  (`503` + `Retry-After` for new requests) or throttled through TCP flow
  control (for uploads in progress) rather than queued.

Weights are configurable per node. Bandwidth-bound deployments may favor
WRITE, and deployments that need interactive playback may favor READ.

## 25. Object Size

At ~200 MB/s sequential write:

| Object size | Pure write time |
|---:|---:|
| 1 MB | ~5 ms |
| 16 MB | ~80 ms |
| 32 MB | ~160 ms |
| 64 MB | ~320 ms |
| 128 MB | ~640 ms |
| 500 MB | ~2.5 s |

Against a per-object fixed cost of a few seeks (one `fsync` journal write plus
moving between jobs, ~10–30 ms):

```text
< 16 MB     fixed cost is significant
32 MB       practical lower bound
64–128 MB   sweet spot (default target)
> 256 MB    little extra HDD benefit; larger loss unit, longer uploads over
            slow links, more Writer RAM
```

For CCTV the segment size also sets the **loss granularity** and the
**upload duration** of a live upload: at 4 Mbps, 64 MB ≈ 2 minutes of video.
With live upload, what a destroyed producer takes with it is bounded by the
part buffer and the link, not by the segment size ([§12.2](04-write-path.md#122-resumable-part-uploads)).

Each Storage Node advertises `max_object_size`. Writers cut segments no larger
than that.
