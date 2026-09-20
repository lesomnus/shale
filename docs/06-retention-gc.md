# Shale — Retention and GC

## 20. Retention

Every object carries two dates:

| Field | Meaning | Enforced by |
|---|---|---|
| `date_expired` | from then on, the object **may** be deleted when space is needed | lazy GC ([§21](#21-lazy-gc)) |
| `date_deleted` | from then on, the object **is** deleted | the read path at once; the node's daily sweep removes the bytes ([§20.1](#201-date_deleted-is-an-expiry-not-an-event)) |

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

Removing the bytes is housekeeping, and the **node** does it: once a day
(`sweep_interval`) each node walks its in-memory index, collects every file
whose xattr `date_deleted` has passed, and proposes them through the normal
GC path ([§21.2](#212-protocol)), independent of space pressure. Because the
sweep reads the xattr, it also catches duplicate files and orphans, whose
dates the index never held. A read token issued before the deadline stays
valid until it expires (`read_token_ttl`, at most 1 hour). That lag, and up
to one `sweep_interval` before the bytes are gone, are accepted; the
guideline that matters for Korean CCTV allows five days
([§20.2](#202-initial-dates)).

### 20.2 Initial dates

When an object is allocated, both dates come from its set's retention
settings, counted from `date_started`
([§10](02-data-model.md#10-time-semantics)):

```text
date_expired = date_started + retention.expire     (default 30 days)
date_deleted = date_started + retention.delete     (default: = retention.expire)
```

The default makes retention **hard**: an object is deleted 30 days after it
was recorded, whether or not space was needed, and the sweep removes the
bytes within a day. That is the right default for the primary target. CCTV
recordings are personal data almost everywhere, and Korea's standard
guideline under the Personal Information Protection Act sets a retention
period of at most 30 days where no shorter one can be justified, with
destruction within five days after it. A workload that may keep data as long
as space allows sets `retention.delete: none`, and then objects live until
lazy GC reclaims them.

The initial dates are also written into the object's xattr
([§23.1](07-storage-node.md#231-self-describing-objects)).

### 20.3 Rescheduling

There is no separate hold flag. Keeping or removing an object early is done by
**changing its dates**:

| Intent | Change |
|---|---|
| Preserve until T (e.g. an incident under investigation) | move `date_expired` and `date_deleted` to at least T |
| Delete now (e.g. a privacy request) | set both dates to now |
| Return to normal retention | set the dates back |

- `ObjectService.Reschedule` does this for one object, or in bulk for a set or
  a source over a time range, since an incident usually spans several cameras
  and several segments ([§35.4](12-api.md#354-tenant-api-custom-rpcs)). It is
  refused for an object already `DELETING` or `DELETED`; in bulk such objects
  are left alone.
- A bulk reschedule works in pages of `reschedule_page` (1,000) objects, each
  its own transaction, and stops short of the call's deadline: a set's day is
  tens of thousands of objects. The answer says how many were changed and how
  many `remaining` in the range the call did not reach; the caller sends the
  same request again while that is not zero, and the CLI does so by itself.
  An object the request already changed is not selected again, so the calls
  add up to exactly one change per object.
- A reason is required. payday's audit trail records who changed which dates,
  from what, to what, and why, so the original dates are never lost and every
  decision to keep or delete has an author.
- Which rule wins, a preservation request or a deletion deadline, is therefore
  not a question the storage answers. It is decided by whoever edits the
  dates, and the audit trail shows it.
- **The node hears about it at once.** A reschedule marks the object
  `dates_synced = false`, and the CP calls the node holding it to rewrite the
  xattr (`NodeControl.SetDates`, [§35.7](12-api.md#357-storage-node-control-api)).
  A **delete now** additionally calls `NodeControl.Delete` for every file the
  CP knows for the object, the winning attempt's and any duplicate's, so the
  bytes go within seconds. If the node is unreachable, the CP keeps trying and
  re-sends when the node returns ([§34.9](11-deployment.md#349-events-and-directives)).

The DB is the truth for dates; the xattr holds the latest values the node was
told. Dates changed while a node was unreachable, and then lost with the DB,
are lost. §29 says what to back up.

Because GC is lazy and per-sink load varies, **actual retention differs
slightly per sink**. `date_expired` is a lower bound only while capacity
allows.

### 20.4 Row retention

Rows are kept only as long as readers can use them:

| Row | Pruned |
|---|---|
| `DELETED` or `LOST` object | `row_retention` (default 30 days) after its `date_deleted`, or after it was deleted or lost, whichever is later |
| attempt in a terminal state other than `STORED` | with its object, or 7 days after it ended |
| object with no stored attempt | `abandon_grace` (1 hour) after its last attempt expired |

A `Timeline` over a span older than any row answers from policy instead:
`DELETED` for spans past the set's `retention.delete` (or `retention.expire`
when there is no delete date), `NOT_RECEIVED` otherwise
([§19](05-read-path.md#19-reader-semantics)). The pruning sweeps run on the CP
leader ([§34.9](11-deployment.md#349-events-and-directives)).

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
           (no approvable candidates)    → the node refuses new uploads on the sink
                                           (503, then ENOSPC-class reallocation, §13);
                                           the CP excludes the sink from placement
```

Example: `critical = 3%`, `low = 5%`, `target_free = 8%`. Tune from operation.

Sinks in RECLAIM **stay eligible** for placement. Only CRITICAL sinks are
excluded. Placement also avoids sinks that are *forecast* to run out
([§11](03-placement.md#11-placement)).

### 21.2 Protocol

The node proposes and the CP approves. The node knows what is in the sink; the
CP knows the current dates, tenant shares, and policy. A candidate is a
**file**, named by `object_key`, with the `object_id`, `attempt_id`, dates,
and size from its xattr.

```text
1. Node   free < low_watermark on sink S       (or: the daily sweep, §20.1)
2. Node   selects candidates from its in-memory index:
          pressure:  xattr date_expired <= now, oldest first,
                     about gc_proposal_factor (3×) the bytes it needs
          sweep:     xattr date_deleted <= now
          in pages of at most gc_page (5,000) candidates
3. Node   SinkService.ProposeGc {sink, candidates}
4. CP     matches each candidate to what it knows:
          - the object's current location (sink S, this key): drops it if
            the current date_expired (pressure) or date_deleted (sweep) is
            still in the future, and returns the current dates; otherwise
            it is approvable
          - a duplicate's file or an orphan: approvable when the xattr date
            the node proposed on has passed
          orders approvable candidates by tenant fair share (§21.4), then
          by date; approves from the top until the target is met (all of
          them for a sweep); marks the approved objects DELETING
5. Node   rewrites the xattrs of candidates that came back with new dates;
          unlinks the approved files (MAINT jobs); re-measures free space (§22.2);
          queues an ObjectDeleted event per file
6. CP     ObjectDeleted → object DELETED, or a duplicate's or orphan's
          file forgotten
```

Deletions travel through the same outbox as commits
([§34.9](11-deployment.md#349-events-and-directives)), so a node crash
between the unlink and the event loses nothing for long: reconciliation asks
the node which `DELETING` keys are gone and confirms them. An object never
stays `DELETING` indefinitely, and a tenant's stored bytes ([§21.4](#214-tenant-fair-share))
are decremented when the deletion is confirmed.

A candidate is only ever matched by **location**, `(sink_id, object_key)`.
A duplicate's file on another sink never marks the live object `DELETING`.

### 21.3 Orphans

A candidate the CP does not know (an orphan) is approved once the xattr date
it was proposed on has passed: `date_expired` under pressure, `date_deleted`
in the sweep. Orphans are reclaimed exactly when normal objects would be, with
no separate scan. Most orphans never get that far: reconciliation
([§34.9](11-deployment.md#349-events-and-directives)) turns a file whose
commit event was lost back into a known object.

If the CP is unreachable, nothing is deleted. The sink may then reach CRITICAL
and stop taking writes. That costs availability, which is acceptable, and it
never deletes an object whose dates were extended.

### 21.4 Tenant fair share

Sinks are shared by every tenant, so one tenant writing more would otherwise
shorten everyone's retention. Each tenant has a `capacity_share`: a fraction
of cluster capacity (default: equal shares). The CP tracks each tenant's
stored bytes from commit and confirmed deletion events.

When GC approves candidates, **expired objects of tenants over their share go
first**, then the rest by `date_expired`. Recording is never refused. A tenant
that stores more than its share only loses its own expired objects sooner.

Fair share only orders what has already expired. It cannot free space held by
unexpired objects. That case is caught earlier by the capacity forecast
([§11](03-placement.md#11-placement), [§31](09-operations.md#31-observability)),
which warns operators before any sink runs out. Within a tenant, a set's
`max_bitrate_total` ([§12.6](04-write-path.md#126-upload-profile-negotiation))
keeps one set from eating the others' retention. In a single-tenant cluster
fair share has no effect.
