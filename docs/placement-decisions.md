# Shale Placement — Decision Report

This report records how Shale places new objects and why. The design summary
is in [§11](03-placement.md#11-placement).

## Terminology

Shale places objects into **sinks** (storage directories), and each sink sits
on a physical **device** ([§22.2](07-storage-node.md#222-sinks-and-devices)). In production every HDD is one device
holding one sink, so in the scenario numbers below sinks, devices, and HDDs
coincide (480 of each). The terms still mean different things, and the report
keeps them apart:

- rankings choose **sinks**, weighted by sink capacity;
- set spread and failure analysis are per **device**, because that is what
  fails and what serializes I/O;
- **HDD** appears only for hardware and media behavior.

## 1. Scenario

The decisions are made for the CCTV archive described in [§1](01-overview.md#1-what-shale-is-for).

| Parameter | Assumed value |
|---|---|
| Cameras | 5,000 (grows over time; cameras are added and removed as sets) |
| Bitrate | 4 Mbps (0.5 MB/s) per camera, constant |
| Segment | 64 MB ≈ 2 minutes of video |
| Retention | 30 days, lazy |
| Cluster | 10 nodes × 48 × 16 TB HDD = 480 HDDs, one sink each |
| Sets | 4–16 cameras behind one producer; a set powers on/off as a unit |
| Readers | A few media servers; the typical query is "all cameras of set S from t1 to t2", less often a single camera |

Derived load:

```text
cluster ingest      5,000 × 0.5 MB/s   = 2.5 GB/s   (~39 segments/s)
per node            250 MB/s           (capacity-bound, §26.3)
per device          ~5.2 MB/s          (~4% of B_eff)
cameras per sink    ~10 at any moment
```

Three facts drive most of the decisions. **Devices are nearly idle on bandwidth**:
placement does not need to balance load tightly. **Sinks are always nearly full**:
placement must not assume free space is a meaningful signal. **Sets are read
together**: a set's members must not share a device, or a whole-set export runs
at one device's speed.

## 2. Requirements

1. **R1 Stable mapping.** Adding or removing cameras, sinks, or nodes moves
   only a proportional share of placements.
2. **R2 Tunable loss shape.** The operator chooses between "many cameras lose
   short gaps" and "few cameras lose long spans".
3. **R3 Set spread.** Members of a set must be readable in parallel, and one
   failure should not take a whole set's moment away.
4. **R4 No hot spots on new hardware.** No migration means new HDDs join
   empty. They must not attract a disproportionate share of writes.
5. **R5 Deterministic fallback.** Retries and ineligible sinks need a
   well-defined next choice.
6. **R6 Stateless CP replicas.** Any CP replica computes the same answer
   without coordinating.
7. **R7 Cheap.** ~40 allocations/s cluster-wide.

## 3. Decisions

### D1. Weighted rendezvous hashing (HRW)

For a key `k` and each target `d` with weight `w_d`:

```text
score(k, d) = -w_d / ln(u(k, d))      u = hash(k, d) mapped to (0, 1)
ranking(k)  = targets sorted by score, descending
```

The first eligible target wins, and the rest of the ranking is the fallback order.

**Why:**

- R1: adding or removing a target only moves keys whose top choice was that
  target, in proportion to its weight.
- R5: HRW produces a full ranking, not just a winner. Reallocation is "next in
  ranking", which is deterministic and never returns to the failed target.
- R6: it is a pure function of `(key, target set, weights)`.
- R7: O(targets) per allocation. 480 hashes × 40/s is negligible, and so is
  5,000 sinks.
- Weights are native and exact. No virtual nodes are needed.

**Rejected:**

| Alternative | Reason |
|---|---|
| `hash mod N` | Violates R1: every change reshuffles everything |
| Consistent-hash ring | Needs virtual nodes for balance and weighting; the fallback order (walk the ring) clusters on neighbors |
| CRUSH | Solves replica placement across a hierarchy; with one copy, its complexity buys nothing HRW doesn't |
| Stateful assignment table | Works, but needs coordinated writes between CP replicas (violates R6) and adds a table to maintain. Since the index already records actual locations, determinism is enough |
| Least-loaded / random | No stickiness, so there is no loss-shape control (R2) |

### D2. The Control Plane picks the sink; no Placement Groups

The original design mapped key → Placement Group → node → local Disk Group →
HDD, with the last step chosen by the node.

**Decision:** the CP ranks node → device → sink directly (see D5) and returns
one `sink_id`. There are no Placement Groups and no Disk Groups.

**Why:**

- The failure domain that matters most is the device. Loss shaping (R2) only
  works if the *sink*, and so its device, is sticky for a `(source, epoch)`. A node-local choice
  would need to replicate the same logic anyway.
- Quarantine is decided centrally from producer reports and heartbeats
  ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)), so the CP already has per-device and per-sink eligibility.
- Placement Groups exist in systems like Ceph to bound per-object metadata and
  to move data in batches. Shale records every object's location and never
  moves data, so PGs would be an extra layer with no function.

The node can still refuse a write (`503`, device error), and the normal retry
path handles that.

### D3. Placement key = `(set, epoch, placement_version)` + ordinal

```text
epoch = floor(date_started / epoch_duration)
```

- `date_started` is the **data time** the producer declares
  ([§10](02-data-model.md#10-time-semantics)), not the allocation or write time. A
  segment retried minutes later, or buffered by a producer during an outage,
  still lands with its epoch-mates.
- `placement_version` is in the key, so a policy change rotates placement
  cleanly rather than half-applying.
- The key is per set, and the member's ordinal selects its position in the
  ranking (D5). Every source belongs to a set, a lone camera being a set of
  one ([§7](02-data-model.md#7-source-set-zone-epoch)).
- Producers pre-allocate up to `allocation_horizon` ahead ([§12.1](04-write-path.md#121-flow)), so
  the key uses the segment's **expected** `date_started`. Segment boundaries are
  deterministic (staggered phases), so for live uploads, which start with the
  segment, the expected time is the actual one. Because placement is
  deterministic, an early allocation lands on the same sink as a late one.
  Only eligibility changes within the horizon are missed. If the actual start
  falls in another epoch, the producer asks again.
- Epoch boundaries are aligned for all cameras. Staggering them is unnecessary
  because load is spread across devices either way. Segment boundaries, which
  do matter for bursts, are staggered separately ([§12.2](04-write-path.md#122-resumable-part-uploads)).

### D4. Loss shape is controlled by `epoch`

**Model.** Over retention `T`, a camera occupies `T / E` `(camera, epoch)`
slots, each on an effectively random device out of `D`. When one device dies:

```text
expected loss per camera       = (T/E) / D epochs  = T / D   (independent of E)
P(camera affected)             = 1 − (1 − 1/D)^(T/E)
```

With `T = 30 d`, `D = 480`:

| `epoch` | Cameras affected | Loss per affected camera | Shape |
|---|---:|---|---|
| 5 min | ~100% | ~18 gaps × 5 min (~1.5 h) | scattered short gaps |
| **1 h** | **~78%** | **1–2 whole hours** | **a few hour-long gaps** |
| 1 d | ~6% | ~1 whole day | rare day-long gaps |
| ∞ (sticky) | ~0.2% | entire history | a few cameras wiped |

The **expected total loss is the same** for every setting. `epoch` only
decides how that loss is distributed. Hence it is a policy parameter,
configurable per set:

```yaml
placement:
  epoch: 1h
  set_spread: spread
```

**Default `1h`, why:**

- A gap under ~5 minutes interrupts almost any incident review, so 5-minute
  epochs mean nearly every camera has broken playback somewhere.
- Hour-long gaps are easy to reason about ("11:00–12:00 is missing on camera
  17") and match how operators search footage.
- A 1-hour epoch holds ~30 segments (~2 GB) per camera per sink, so playback
  of an hour reads one device sequentially ([§17.4](05-read-path.md#174-read-locality-and-parallelism)).
- Sticky (∞) placement makes a single device failure erase whole cameras, which
  is the worst case for evidence retrieval.

**When to choose otherwise:**

- **Longer (1d, sticky):** when a partial recording of a source is worthless,
  e.g. continuity-critical analytics, or when per-source export speed matters
  more than loss spread.
- **Shorter (5–15 min):** when any hour-long gap is unacceptable and many
  small gaps can be tolerated (e.g. traffic counting sampled from footage).

### D5. Set spread: distinct nodes first, distinct devices always

A set is **read together** and **fails together at the source** (it powers
on and off as a unit). The two properties point the same way:

- **Reads**: a whole-set export is as fast as its slowest device. For an 8-camera
  set at 4 Mbps, one hour is 8 × 1.8 GB. Packed on one device that takes
  ~115 s; spread over 8 devices it takes ~15 s.
- **Losses**: spread, one device failure costs a set at most one member's epoch.
  The other angles of that moment survive.
- **Writes**: when a set powers on, it adds one camera to each of N devices
  rather than N cameras to one device.

Selection for camera `s` with ordinal `i` in set `g`, epoch `e`. Rankings are
computed over **all** nodes and devices, eligible or not; eligibility only
decides which entries are skipped (D7):

```text
1. node ranking    = HRW over all nodes,                 key (g, e, version),
                     weight = sum of the node's sink capacity (clamped, §27)
   node            = node_ranking[i mod N]               (N = #nodes),
                     or the next eligible node after that position
2. device ranking  = HRW over the node's devices,        key (g, e, version),
                     weight = sum of the device's sink capacity
   device          = device_ranking[(i div N) mod Nd]    (Nd = #devices on the node),
                     or the next eligible device after that position
3. sink            = top eligible of HRW over the device's sinks, key (s, e, version),
                     weight = sink capacity
```

In production each device has one sink, so step 3 is trivial.

- Members `0 … N−1` land on **distinct nodes**. Beyond that, members wrap onto
  nodes already used, and step 2 gives them **different positions** in that
  node's device ranking. As a result members always sit on **distinct
  devices**, up to the total number of eligible devices.
- Because the ranking is over all nodes, a node that becomes ineligible
  changes nothing for the members whose positions are elsewhere: only the
  member at its position steps forward to the next eligible node, where it
  may sit next to a sibling for the rest of the epoch, which is soft. Ranking
  only eligible nodes would instead shift every member behind the failed
  one, moving most of a set mid-epoch.
- Spreading over devices rather than sinks keeps the guarantee honest when a
  device carries several sinks: two sinks on one device fail together and share
  one Device Queue.
- Both rankings are keyed by the set, not the camera, so members take
  consecutive positions of the *same* ranking. Independent per-camera hashes
  would collide by chance (with 8 cameras on 10 nodes, ~98% of epochs would
  put at least two members on one node).
- Ordinals are stable and never reused. Removing a camera leaves a hole and
  shifts nobody (R1). A hole can let two members share a node once `N` is
  small, which is acceptable because the spread is soft.
- Fallback for member `i` walks its node's device ranking, then the next node
  positions. It may land next to a sibling, which is soft again.
- Across sets, rankings are independent, so load stays uniform.

**Why nodes before devices:** a node outage (power, NIC, OS, maintenance) is far
more frequent than a device failure and takes all its devices at once. A node's
NIC is also a shared read path.

Options per set:

| `set_spread` | Placement | Use when |
|---|---|---|
| `spread` (default) | as above | members are useful on their own (CCTV) |
| `pack` | whole set on `device_ranking[0]` of `node_ranking[0]` | a partial set is worthless; accepts one-device reads |
| `none` | each camera keyed by `(s, e)` alone | no set structure |

**Zones** (overlapping views that may cross sets) are not used, and
zone-aware placement is not planned. A camera that moves away from its zone-
mates can collide with its own set-mates, which then have to move, and the
chain runs through every set the zones connect. Solving it needs a per-epoch
assignment stored for each such group, which gives up the stateless hash. Set
spread already covers the common case, because overlapping cameras usually
belong to the same set. If zones ever matter, a zone-aware scheduler can
replace this one without migration ([§11.2](03-placement.md#112-scheduler-interface)).

### D6. Weight = raw capacity, not free space

With no migration, new hardware joins empty.

- **Free-space weighting** sends nearly all writes to a new sink until it
  fills. That creates a write hot spot and **concentrates the most recent
  footage on a batch of brand-new HDDs**, which have elevated infant
  mortality. That is correlated loss of exactly the data most likely to be
  needed.
- **Capacity weighting** gives a new sink a share proportional to its size.
  It fills over one retention period, like every other sink. Old sinks sit at
  the GC watermark and keep deleting expired data, so nobody runs out.

Consequence: during the first retention period a new sink holds less data than
its peers. That wastes nothing and needs no correction.

Mixed HDD sizes are handled by the weight itself. A 24 TB sink gets 1.5× the
writes of a 16 TB sink and stays proportionally full. The weight is the
capacity the node reports, clamped by `max_sink_capacity`
([§27](09-operations.md#27-node--device--sink-health-and-quarantine)), so a
misreported or malicious capacity cannot capture the cluster's writes.

An optional `new_device_ramp` (weight × 0.25 for the first 30 days when on)
can further limit infant-mortality exposure. It is off by default.

### D7. Eligibility and fallback

Ranking is computed over **all** targets. Eligibility decides which entries
are **skipped**:

| Skipped when | Source |
|---|---|
| device unhealthy, quarantined, or retired | heartbeat + failure score ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) |
| sink CRITICAL, retired, or failed the capability probe | heartbeat ([§22.2](07-storage-node.md#222-sinks-and-devices), [§21](06-retention-gc.md#21-lazy-gc)) |
| sink forecast to run out this epoch | capacity forecast ([§11.1](03-placement.md#111-capacity-forecast)), decided once per sink and epoch |
| node down | heartbeat freshness ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) |
| weight reduced (suspect / probation) | failure score; changes the ranking, so it moves a proportional share of keys, like a capacity change |

Deliberately **not** grounds for skipping:

- **RECLAIM pressure.** It is the steady state of a full cluster. Excluding it
  would exclude everything.
- **Momentary queue fullness (`503`).** The producer waits `Retry-After` on
  the same target, which keeps `(source, epoch)` sticky. Only persistent
  failure moves to the next candidate.

The allocation response includes the top few candidates, each with its own
attempt and token, so a producer can reallocate without another CP round
trip. It still reports the failed attempt so the CP can update health.

When eligibility changes mid-epoch (a device is quarantined, a node goes
down), the keys whose position was on that target move to the next eligible
entry of their ranking for the rest of the epoch. Nothing else moves, because
the ranking itself does not depend on eligibility.

### D8. Load balance is sufficient without load-aware placement

With ~5,000 cameras over 480 sinks, a sink carries ~10.4 cameras per epoch
(Poisson-like, σ ≈ 3.2). A busy sink might carry ~20 cameras ≈ 10 MB/s, which
is **~8% of `B_eff`**. Hash-based placement is therefore balanced enough.

This changes if per-device write load approaches `B_eff`, which happens with
short retention or very high bitrates ([§26.3](08-sizing.md#263-worked-example-cctv)). Then the per-sink upload
limits provide backpressure and persistent `503`s spill writes to the next
candidate. That is a "bounded load" behavior that emerges from D7 without
a separate mechanism. If spill becomes common, it is a sizing signal (add
HDDs or nodes), not a placement problem.

Per-sink capacity variance has the same root: busier sinks cycle through GC
faster, so **effective retention varies slightly per sink**. [§20](06-retention-gc.md#20-retention)
documents that `date_expired` is a lower bound only while capacity allows.

### D9. Versioning

- `placement_version` is stored with each object for diagnostics only.
- Past locations are **never recomputed**. The index (`sink_id`, `object_key`)
  is authoritative.
- A new version applies to allocations after its activation. Epochs in
  progress switch at once. That moves only future segments, which is harmless.

## 4. Summary

| # | Decision | One-line reason |
|---|---|---|
| D1 | Weighted HRW | Stable, weighted, deterministic, and gives a ranked fallback |
| D2 | CP picks the sink (node → device → sink); no PG / Disk Group | The device is the failure unit that matters; PGs add nothing without migration |
| D3 | Key `(set, epoch, version)` + ordinal, with media-time epochs | Retries, delayed uploads, and pre-allocation stay with their epoch |
| D4 | `epoch` = loss-shape knob, default 1h | Same expected loss; hour-long gaps are the most usable shape for CCTV |
| D5 | Set spread by ordinal over a ranking of all nodes, then devices | Sets are read together; parallel export, partial-loss survival, and a node failure moves only its own members |
| D6 | Weight = capacity, clamped | No hot spot, no concentration of new data on new HDDs, no capture by a misreported sink |
| D7 | Rank everything, skip the ineligible; `503` retried in place | Keeps stickiness; RECLAIM is the steady state; nothing else moves |
| D8 | No load-aware placement | Devices run at ~4–8% of bandwidth; upload limits handle the rest |
| D9 | Version in key, never recompute | The index is the truth |
