# Shale — Placement

## 11. Placement

Summary. The full rationale is in the [placement decision report](placement-decisions.md).

- **Weighted rendezvous (HRW) hashing**, keyed by
  `(set, epoch, placement_version)` and indexed by the member's ordinal (or
  keyed by `(source, …)` for Sources without a set). The resulting ranking is
  also the retry order.
- **Weight = sink capacity** (raw HDD size in production), not free space. A
  new, empty sink therefore does not attract a burst of writes.
- **Eligibility filters** are applied after ranking: sink and device health,
  quarantine ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)), CRITICAL pressure ([§21](06-retention-gc.md#21-lazy-gc)), and the
  capacity forecast below. A momentarily busy sink (`503`) is not
  filtered out: the Writer waits for `Retry-After`, so the Source stays on its
  sink. Only a persistent `503` moves the attempt to the next candidate.
- **Set spread**: within an epoch, members of a set land on **distinct nodes**
  while there are enough nodes, and always on **distinct devices** up to the
  number of eligible devices. A whole-set read therefore runs in parallel
  across devices, and one device failure costs a set at most one member's epoch.
- The Control Plane chooses the **sink** (node → device → sink). There are no
  Placement Groups and no node-local Disk Groups.

Placement is tunable per set:

```yaml
placement_policy:
  epoch: 1h              # how long a source sticks to one sink
  set_spread: spread     # spread | pack | none
```

- `spread` (default): distinct nodes, then distinct devices, as above.
- `pack`: the whole set shares one sink per epoch. Losses are all-or-nothing
  per set, and set reads are limited to one device. Use it only when a partial
  set is worthless.
- `none`: each member is placed independently.

| `epoch` | When one device dies (30-day retention, 480 devices) |
|---|---|
| 5m | almost every camera loses ~18 scattered 5-minute gaps (~1.5 h total) |
| 1h (default) | ~78% of cameras lose 1–2 whole hours |
| 1d | ~6% of cameras lose ~1 day each |
| ∞ (sticky) | ~0.2% of cameras lose their entire history |

A longer epoch concentrates losses in fewer sources and longer gaps. A shorter
epoch spreads them as many short gaps. The expected total amount lost is the
same for every setting.

### 11.1 Capacity forecast

The CP knows, fairly exactly, what each sink is about to receive. Every source
placed on a sink has a negotiated bitrate
([§12.6](04-write-path.md#126-upload-profile-negotiation)), and placement is
deterministic for the epoch. So for each sink and the coming epoch:

```text
incoming    = Σ bitrate of the sources placed on the sink × epoch length
reclaimable = free space + bytes on the sink whose date_expired falls
              before the end of the epoch                    (index query)
```

A sink with `reclaimable < incoming × forecast_margin` (default 1.5) is
**ineligible for that epoch**: it would reach CRITICAL before GC could make
room. Its keys fall to their next candidates, and nothing else moves.

This is a filter, not a weight. Weights stay at raw capacity, so placement
stays stable. In steady state all sinks look alike and the filter rarely
fires. It catches the exceptions: a sink full of long-retention data, a
rescheduled incident pinning a lot of bytes, or a burst of new high-bitrate
cameras. Summed over the cluster, the same numbers give **the time until the
cluster runs out**, which is reported to operators
([§31](09-operations.md#31-observability)).

### 11.2 Scheduler interface

Placement is a replaceable **scheduler** behind one interface:

```text
input:   source, set, ordinal, zone, epoch, and the eligible nodes,
         devices, and sinks with their weights and forecasts
output:  a ranked list of candidate sinks (first = target, rest = retries)
```

The weighted-HRW scheduler above is the v1 implementation.
`PlacementPolicy` names the scheduler and its parameters, and
`placement_version` identifies the policy an object was placed under.

Swapping schedulers is safe at any time. Past object locations are never
recomputed: the index is the source of truth, and the scheduler is only used
to place new objects. A new scheduler therefore changes where future objects
go, and needs no migration.

This is where zone-aware placement would go, if it is ever needed
([§7](02-data-model.md#7-source-set-zone-epoch)). Keeping cameras with
overlapping views on different devices *across* sets conflicts with set
spread, and resolving that needs a per-epoch assignment stored for each group
of sets linked by zones, instead of a stateless hash. It is not planned.
