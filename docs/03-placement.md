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
  quarantine ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)), and CRITICAL pressure ([§21](06-retention-gc.md#21-lazy-gc)). A momentarily busy sink (`503`) is not
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

Past object locations are never recomputed. The index is the source of truth,
and the placement function is only used to place new objects.
