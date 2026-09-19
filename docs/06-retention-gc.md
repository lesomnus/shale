# Shale — Retention and GC

## 20. Retention

Two separate deadlines, plus holds:

| Field | Meaning | Enforced by |
|---|---|---|
| `expires_at` | After this, **may** be deleted when space is needed | lazy GC ([§21](#21-lazy-gc)) |
| `must_delete_by` | Must be deleted by this time (e.g. privacy law on CCTV retention) | a periodic sweep, independent of pressure |
| `hold` | Must not be deleted (e.g. an incident under investigation) | CP refuses to approve deletion |

- Space permitting, expired objects are kept and stay readable.
- Whether `hold` overrides `must_delete_by` is a legal/policy question ([§36.2](13-configuration.md#362-open-decisions)).
- Because GC is lazy and per-sink load varies, **actual retention differs
  slightly per sink**. `expires_at` is a lower bound only while capacity allows.

## 21. Lazy GC

### 21.1 Watermarks

Watermarks apply per sink. Free space is the sink's own measure ([§22.2](07-storage-node.md#222-sinks-and-devices)): real
filesystem free space for a whole-HDD sink, and the worse of quota headroom
and filesystem free space for a sink with a declared `capacity`.

In steady state a CCTV cluster is always nearly full, so being "full" is the
normal operating point, not an alarm.

```text
NORMAL     free ≥ low_watermark          nothing to do
RECLAIM    free < low_watermark          GC until free ≥ target_free
CRITICAL   free < critical_watermark     GC could not free enough
           (no approvable candidates)    → sink excluded from placement
```

Example: `critical = 3%`, `low = 5%`, `target_free = 8%`. Tune from operation.

Sinks in RECLAIM **stay eligible** for placement. Only CRITICAL sinks are
excluded.

### 21.2 Protocol

The node proposes and the CP approves. The node knows what is in the sink; the
CP knows holds and policy.

```text
1. Node   free < low_watermark on sink S
2. Node   selects candidates from its in-memory index:
          expires_at <= now, oldest first, until the target is met
3. Node   SinkService.ProposeGc {sink, candidates}
4. CP     approves unless the object is held;
          marks approved objects DELETING
5. Node   unlink (MAINT jobs), re-measures free space (§22.2)
6. Node   SinkService.ReportDeleted → CP marks them DELETED
```

The `must_delete_by` sweep uses the same proposal path, independent of space.

### 21.3 Orphans

A candidate the CP does not know (an orphan) is approved once its xattr
`expires_at` has passed. Orphans are reclaimed exactly when normal objects
would be, with no separate scan.

If the CP is unreachable, nothing is deleted. The sink may then reach CRITICAL
and stop taking writes. That costs availability, which is acceptable, and it
never deletes a held object.
