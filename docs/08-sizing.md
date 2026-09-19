# Shale — Sizing and Hardware

## 26. Sizing and Hardware Ceilings

A node's sustainable ingest `W` is capped by four independent limits. The
smallest one binds, and it tells the operator **which hardware to add**.

| Ceiling | Formula | If this binds, add… |
|---|---|---|
| Network | `W ≤ NIC_eff` | faster / more NICs |
| HDD bandwidth | `W + R_hdd ≤ N_hdd × B_eff` | more HDDs (per node or more nodes) |
| Capacity × retention | `W ≤ N_hdd × C_hdd × fill / T_retention` | more or larger HDDs |
| RAM | `part buffers + index ≤ RAM budget` ([§26.4](#264-ram)) | RAM |

Where:

- `NIC_eff` ≈ 90% of line rate. Ingress (writes) and egress (reads) are
  separate on a full-duplex link.
- `B_eff` is the per-HDD steady-state budget, ≈ 60–75% of the HDD's average
  sequential throughput (inner tracks, seeks between jobs, reads, GC,
  maintenance). With a 7200rpm HDD averaging ~180 MB/s, use **`B_eff ≈ 125 MB/s`**.
- `fill` is the steady-state fill ratio GC maintains (≈ 0.9, [§21](06-retention-gc.md#21-lazy-gc)).
- `R_hdd` is read traffic that hits the HDDs.

### 26.1 Network ceiling: HDDs needed to saturate a NIC

| NIC | `NIC_eff` | HDDs to saturate at `B_eff = 125 MB/s` |
|---|---:|---:|
| 10 GbE | ~1.1 GB/s | ~9 |
| 25 GbE | ~2.8 GB/s | ~22 |
| 2 × 25 GbE | ~5.6 GB/s | ~45 |
| 100 GbE | ~11 GB/s | ~88 |

A 48-HDD node on a single 25 GbE link is NIC-bound **if** the workload is
bandwidth-bound. For CCTV it usually is not ([§26.3](#263-worked-example-cctv)).

### 26.2 HDDs per node

HDDs per node is still a free choice. It sets the node's capacity, its
bandwidth ceiling, and the size of the blast radius when a whole node is down
([§28](09-operations.md#28-failure-semantics)). Typical: 24–60 HDDs per chassis/JBOD.

### 26.3 Worked example: CCTV

48 × 16 TB HDD, `fill = 0.9` → ~691 TB usable, 30-day retention.

```text
capacity ceiling = 691 TB / 30 d        ≈ 267 MB/s  (≈ 2.1 Gbps)
HDD ceiling      = 48 × 125 MB/s        ≈ 6.0 GB/s
NIC ceiling      = 25 GbE               ≈ 2.8 GB/s
```

The node is **capacity-bound**: ~267 MB/s ≈ 530 cameras at 4 Mbps. Each HDD
sees only ~5.6 MB/s of writes, and 10 GbE would be enough.

The retention at which the NIC starts to bind is `usable capacity / NIC_eff`:
691 TB / 2.8 GB/s ≈ **2.9 days** for 25 GbE. Shorter retention → buy NIC
bandwidth. Longer retention → buy HDDs.

### 26.4 RAM

Object data is buffered only in RAM ([§22.3](07-storage-node.md#223-no-ssd-in-the-data-path)), one **part** at a time ([§12.2](04-write-path.md#122-resumable-part-uploads)), so
RAM is sized explicitly:

```text
part buffers = uploads in flight × part_size × fill   (capped by part_buffer_pool)
read buffers = max_read_sessions × read_chunk × 2
index        = objects on the node × ~150 B
```

With **live uploads** ([§12.2](04-write-path.md#122-resumable-part-uploads)) every camera has an upload in flight at all
times, so uploads in flight ≈ cameras served by the node. Buffered uploads
over slow links behave almost the same. Part buffers grow on demand from a
slab pool, so on average they are about half full (`fill ≈ 0.5`) and the
worst case is `fill = 1`. For the CCTV node of [§26.3](#263-worked-example-cctv) (~530 cameras):

```text
part buffers, 16 MB parts: 530 × 16 MB × 0.5 ≈ 4.2 GiB   (worst case 8.5 GiB)
part buffers,  8 MB parts: 530 ×  8 MB × 0.5 ≈ 2.1 GiB   (worst case 4.2 GiB)
read buffers:              64 × 16 MB × 2     = 2 GiB
index:                     48 × 250,000 × 150 B ≈ 1.8 GiB
```

**32 GiB** is comfortable for such a node. Because I/O bypasses the page cache
([§22.4](07-storage-node.md#224-bypass-the-page-cache)), extra RAM does not improve throughput.

For comparison, buffering whole objects would need 530 × 64 MB ≈ 33 GiB, and
live upload would be impossible. That is why uploads are staged in parts.

**The index is rebuilt at every start** by walking each sink's directories
and reading inodes (~12 M on this node), which takes minutes per sink, in
parallel across HDDs, as MAINT work. Nothing waits for it that does not need
it: uploads create new files and reads open files by path, so both are served
from the first second; GC proposals, the daily sweep, and `ObjectMissing`
reports for a sink start once that sink's scan is complete.
