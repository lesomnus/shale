# Shale — Retention and GC

## 20. Retention

Every object carries two dates:

| Field | Meaning | Enforced by |
|---|---|---|
| `date_expired` | from then on, the object **may** be deleted when space is needed | lazy GC ([§21](#21-lazy-gc)) |
| `date_deleted` | from then on, the object **is** deleted | read path at once; a sweep removes the bytes |

The CP keeps `date_expired ≤ date_deleted` at all times.

### 20.1 `date_deleted` is an expiry, not an event

`date_deleted` works like the expiry of an X.509 certificate. Nothing has to
happen at that moment for it to take effect. Whoever reads the object compares
it with the current time:

- `date_deleted > now`: the object is valid.
- `date_deleted ≤ now`: the object **is deleted**, whether or not its bytes are
  still on a sink. It reads as `DELETED`
  ([§19](05-read-path.md#19-reader-semantics)), and the CP issues no more read
  tokens for it.

Removing the bytes is housekeeping. A periodic sweep proposes objects past
their `date_deleted` through the normal GC path ([§21.2](#212-protocol)),
independent of space pressure. A read token issued before the deadline stays
valid until it expires (`read_token_ttl`, at most 1 hour). That lag is
accepted.

### 20.2 Initial dates

When an object is allocated, both dates come from its set's retention
settings, counted from `date_started`
([§10](02-data-model.md#10-time-semantics)):

```text
date_expired = date_started + retention.expire     (e.g. 30 days)
date_deleted = date_started + retention.delete     (e.g. 90 days, or never)
```

A set without `retention.delete` keeps objects until lazy GC reclaims them.
The initial dates are also written into the object's xattr
([§23.1](07-storage-node.md#231-self-describing-objects)).

### 20.3 Rescheduling

There is no separate hold flag. Keeping or removing an object early is done by
**changing its dates**:

| Intent | Change |
|---|---|
| Preserve until T (e.g. an incident under investigation) | move `date_expired` and `date_deleted` to at least T |
| Delete now (e.g. a privacy request) | set `date_deleted` to now |
| Return to normal retention | set the dates back |

- `ObjectService.Reschedule` does this for one object, or in bulk for a set or
  a source over a time range, since an incident usually spans several cameras
  and several segments ([§35.4](12-api.md#354-tenant-api-custom-rpcs)).
- A reason is required. payday's audit trail records who changed which dates,
  from what, to what, and why, so the original dates are never lost and every
  decision to keep or delete has an author.
- Which rule wins, a preservation request or a deletion deadline, is therefore
  not a question the storage answers. It is decided by whoever edits the
  dates, and the audit trail shows it.

Because GC is lazy and per-sink load varies, **actual retention differs
slightly per sink**. `date_expired` is a lower bound only while capacity
allows.

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
excluded. Placement also avoids sinks that are *forecast* to run out
([§11](03-placement.md#11-placement)).

### 21.2 Protocol

The node proposes and the CP approves. The node knows what is in the sink; the
CP knows the current dates, tenant shares, and policy.

```text
1. Node   free < low_watermark on sink S
2. Node   selects candidates from its in-memory index:
          date_expired <= now, oldest first,
          about gc_proposal_factor (3×) the bytes it needs
3. Node   SinkService.ProposeGc {sink, candidates}
4. CP     drops candidates whose current date_expired is in the future
          (rescheduled), and returns their current dates;
          orders the rest by tenant fair share (§21.4), then by date_expired;
          approves from the top until the target is met;
          marks approved objects DELETING
5. Node   updates its index and the xattrs of rescheduled objects;
          unlinks the approved ones (MAINT jobs); re-measures free space (§22.2)
6. Node   SinkService.ReportDeleted → CP marks them DELETED
```

The `date_deleted` sweep ([§20.1](#201-date_deleted-is-an-expiry-not-an-event))
uses the same path, independent of space and of fair share.

### 21.3 Orphans

A candidate the CP does not know (an orphan) is approved once its xattr
`date_expired` has passed. Orphans are reclaimed exactly when normal objects
would be, with no separate scan.

If the CP is unreachable, nothing is deleted. The sink may then reach CRITICAL
and stop taking writes. That costs availability, which is acceptable, and it
never deletes an object whose dates were extended.

### 21.4 Tenant fair share

Sinks are shared by every tenant, so one tenant writing more would otherwise
shorten everyone's retention. Each tenant has a `capacity_share`: a fraction
of cluster capacity (default: equal shares). The CP tracks each tenant's
stored bytes from commit and deletion events.

When GC approves candidates, **expired objects of tenants over their share go
first**, then the rest by `date_expired`. Recording is never refused. A tenant
that stores more than its share only loses its own expired objects sooner.

Fair share only orders what has already expired. It cannot free space held by
unexpired objects. That case is caught earlier by the capacity forecast
([§11](03-placement.md#11-placement), [§31](09-operations.md#31-observability)),
which warns operators before any sink runs out. In a single-tenant cluster
fair share has no effect.
