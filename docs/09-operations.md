# Shale — Operations and Failure Handling

## 27. Node / Device / Sink Health and Quarantine

Nodes send heartbeats every `heartbeat_interval` (5 s). A node whose
heartbeats are missing for `node_down_after` (30 s) is **down**: its sinks are
skipped by placement and its objects read as UNAVAILABLE. Relays heartbeat
on the same schedule; a relay that is down has its producers reassigned
([§39.2](16-relay.md#392-assignment)).

```text
node_id, certificate serial, CA bundle hash, key IDs held
per device: device_id, health, error counters, SMART summary,
            queue depth per class, write/read latency
per sink:   sink_id, device_id, total/free bytes (§22.2), pressure state,
            uploads in flight, capability probe results, warnings
            (e.g. several sinks on one device)
```

The CP believes a node only about what is attached to it: reports about a
device or sink attached to another node are rejected, and a reported sink
capacity above `max_sink_capacity` (64 TB) is clamped for placement and
flagged, so neither a typo nor a compromised node can capture the cluster's
writes ([§11](03-placement.md#11-placement), [§33.7](10-security.md#337-what-a-compromise-costs)).

Health is tracked where failures actually happen:

- **Device**: I/O errors, latency, failed WRITE jobs, and SMART. A device's
  state applies to every sink on it.
- **Sink**: pressure and capability. These are filesystem- and quota-level
  conditions.
- **Node**: reachability and failures reported by producers.

### Local exclusion

On a device I/O error, the node stops accepting new writes to every sink on
that device at once and reports it.

### SMART

The node reads each device's SMART data periodically. SMART is the best early
warning of a failing HDD, well before I/O errors appear. It needs access to the
device and `CAP_SYS_RAWIO` ([§34.8](11-deployment.md#348-containers)).

| Signal | Effect |
|---|---|
| overall health assessment fails | quarantine at once |
| reallocated sectors increase | score +5 per increase |
| pending or offline-uncorrectable sectors appear | score +10 |
| interface CRC errors increase | logged against the cable/slot, not the device |

### Quarantine (Control Plane)

Each device and node has a **failure score**, an exponentially decaying sum of
weighted events. The defaults are starting values, to be tuned from real
failure data:

```text
events       I/O error +10, failed WRITE job +5, timeout +2,
             producer-reported failure +1 (at most +5 per producer per node
             per day, so one misbehaving host cannot quarantine a node),
             SMART as above
decay        half-life 24 h

score < 10                 healthy
10 ≤ score < 30            SUSPECT: placement weight × 0.5
score ≥ 30                 QUARANTINED: no new writes, reads still served
score < 5 after cool-down  eligible to return
cool-down                  24 h
probation                  7 days at weight × 0.5, then full weight
```

- Separate enter and exit thresholds (30 and 5) prevent flapping.
- A failure during probation re-quarantines at once.
- The moment a device is quarantined or released, the CP tells its node
  (`NodeControl.SetSinkState`, [§35.7](12-api.md#357-storage-node-control-api)),
  so a quarantined device refuses uploads at once even from producers holding
  allocations to it.

### Operator view and actions

Quarantine is automatic, but what happens to a quarantined device is an
operator's call. Every device records why it is where it is:

```text
Device.quarantine = { state, date_quarantined, reason,
                      score history, recent errors, SMART summary,
                      objects and bytes on it }
```

| Command | Effect |
|---|---|
| `shale device quarantined` | devices awaiting a decision; `watch` for alerts |
| `shale device get <device>` | the record above: why, since when, and what is at stake |
| `shale device release <device>` | back on probation (e.g. after reseating a cable) |
| `shale device retire <device>` | reads only, never again for writes; objects stay readable until they expire |
| `shale device declare-dead <device>` | its objects → LOST; the device is forgotten |
| `shale device locate <device> [--off]` | light the bay LED, so the right HDD is pulled |

Every one of these reaches the node through its control API. A replaced HDD
joins as a new device with a new sink ([§9](02-data-model.md#9-identity)). An
operator can also retire a single sink.

Consoles watch `Device` to show the quarantine queue live, and `Node`,
`Producer`, and `Reader` to show hosts waiting to be adopted
([§33.4](10-security.md#334-joining-and-adoption)).

## 28. Failure Semantics

### 28.1 No draining, no migration

```text
no replica · no EC · no RAID · no draining · no migration · no rebuild
```

Objects on a device or node under maintenance are temporarily UNAVAILABLE. This
availability loss is deliberate. Placement ([§11](03-placement.md#11-placement)) shapes how that loss is
distributed across sources and time.

### 28.2 Device failure

```text
device failure
→ only the objects on that device's sinks are LOST
→ every other device keeps working
→ no rebuild, no restoration
```

### 28.3 Node failure and device re-homing

A node failure makes all of its sinks' objects UNAVAILABLE, not LOST: the sinks
are independent, self-describing filesystems.

**The same machine comes back**, after a reinstall or with a new OS disk: it
joins with its hardware identity, an operator adopts it again, and it
continues as the same `node_id` with the same sinks
([§33.4](10-security.md#334-joining-and-adoption)). Nothing else changes.

**The machine cannot be repaired**: move the HDDs into another chassis.

```text
1. attach the HDDs to node B
2. node B reads each sink label and reports the sinks in its heartbeat
3. the CP sees sinks attached to node A:
   - A still heartbeats → B's claim is refused and B does not serve them
   - A is down → the sinks are "pending adoption"
4. shale sink adopt <sink> '{"node":{"id":"B"}}'  (or automatically once A has been down
   for sink_auto_adopt_after, 10 minutes)
5. the CP updates sinks.node_id, and reconciles each sink with B (§34.9)
6. objects are readable again; no object rows change
```

A sink its node stops reporting, the disk pulled or the directory gone, is
pending adoption from that heartbeat on: placement skips it, and the CP
stops handing out tokens for it, so producers are not sent to a node that
answers "this node does not serve that sink". The node's next report of
it, or an adoption by another node, attaches it again.

This recovers data without violating "no migration", because no bytes are
copied. The adoption step is what keeps a sink from being served by two nodes
at once, which the label check cannot prevent on a shared filesystem, where
both nodes see the same filesystem UUID
([§9](02-data-model.md#9-identity)). A node that was only partitioned from
the CP, and kept committing on tokens its producers held, still gets those
commits indexed when it returns: an event is applied when its attempt was
allocated to that sink on that node, even after the sink moved
([§34.9](11-deployment.md#349-events-and-directives)).

Shale isolates and accepts failures rather than repairing them.

## 29. Metadata Index

The index is a **cache of truth that lives in the sinks**. Losing it is an
availability problem, not data loss.

- Run the DB with ordinary replication/backups. This is cheap and avoids
  rebuilds, but it is not critical.
- **Rebuild** path: `shale index rebuild [<sink>]` makes the CP
  reconcile every sink from the beginning (`NodeControl.Reconcile` with
  `since = 0`, [§34.9](11-deployment.md#349-events-and-directives)): each
  node walks the sink (directory walk + `getxattr`) and streams the records.
  Because the xattrs are inline in the inodes, the scan reads inodes only,
  never object data.
- The xattr carries the object's current dates as last told to the node
  ([§20.3](06-retention-gc.md#203-rescheduling)), so rescheduled objects come
  back with their rescheduled dates, except for changes made while their node
  was unreachable.
- What exists only in the DB: the audit trail, people, sites, policies, and
  the host rows with their certificate serials. After a DB loss every host
  therefore has to be adopted again. Back the DB up.

Scale check: 10 PB at 64 MB ≈ 160 M objects. At 267 MB/s per node, each node
commits ~4 objects/s. This is well within a single PostgreSQL instance, and
SQLite handles a single machine ([§34.2](11-deployment.md#342-external-dependencies)).
Suggested indexes: `(sink_id, date_expired)` for GC approval,
`(source_id, date_started)` and `(set_id, date_started)` for range queries.
Rows of deleted and lost objects are pruned
([§20.4](06-retention-gc.md#204-row-retention)), so the table holds about
one retention period of objects plus 30 days.

## 30. Integrity

- **Bit rot is accepted.** No scrub, no read verification by default. A
  damaged segment usually decodes with artifacts, which is acceptable for CCTV.
- The size check is mandatory (it detects partial files). A checksum, enabled
  per set (e.g. CRC32C computed while receiving) is optional. When enabled, it
  is stored in the xattr and checked only on demand. A node that restarts
  during an upload recomputes the running checksum from the file (a MAINT
  job) before it continues.

Authentication, tokens, adoption, and TLS are covered in
[§33](10-security.md#33-authentication-and-authorization).

## 31. Observability

```text
ingest
  write latency (allocate → 201), commit latency, allocation latency
  uploads in flight per sink, part buffer pool usage, 503 rate
  resumes per upload, idle timeouts, per-actor limit hits
  live lag (recorded time − time covered by bytes on a sink), per producer
  abandoned uploads: finalized incomplete / deleted
  upload duration by producer (spots slow or degrading links)
  same-target / placement retry counts, lost objects
  duplicate attempts, orphans reclaimed, objects recovered by reconciliation

device
  queue depth and wait time per class (WRITE/READ/MAINT)
  I/O errors, SMART attributes, failure score, quarantine transitions

sink
  free bytes, free ratio, pressure state
  GC proposed / approved / reclaimed bytes; sweep deletions
  forecast: incoming vs. reclaimable for the coming epoch (§11.1)

capacity
  time until the cluster runs out, from the same forecast
  stored bytes and share per tenant (§21.4)

read
  read latency, open sessions, stalled sessions, aborted sessions

node
  NIC ingress/egress utilization, RAM buffer usage, index size
  startup scan progress per sink

producer (from its heartbeat, §38.6)
  per source: input up/down, frame rate, measured rate vs. ceiling,
  keyframe interval, capture restarts, early cuts
  host CPU, temperature, uplink usage
  live helpers running (audio transcodes for the relay, §38.7), live bytes dropped

relay (§39)
  attached producers, active sources, viewers, egress
  sessions started / refused (limits, bad tokens), start-to-first-frame time
  CPU

control plane
  hosts pending adoption, certificates due for renewal
  producers per relay, reassignments
  directives pending per node, reconciliation lag per sink
  objects in DELETING, rows pruned
  watch streams, broker reconnects

ceilings (§26)
  current ingest vs. NIC / HDD / capacity ceilings per node
```

Structured logs and distributed tracing are also useful.

## 32. CLI / Processes

The binary is also the CLI. payday generates `get`, `ls`, `watch`, `add`,
`patch`, and `erase` for each entity where the schema declares them, and rows
are named by ID or slug ([§35.1](12-api.md#351-conventions)). The CLI signs
in as a person (`shale login`) and keeps a session.

```bash
# processes (§34.1)
shale init [--tenant t] [--admin a] [--operator o]   # CA, signing key, first tenant, first operator and admin
shale serve control|cluster|storage|producer|reader|relay|all [--dev <dir>]
shale login [--cluster] [--password p] @tenant/alias # sign in as a person; keeps a session
shale holder set-password <holder>

# Every entity has the generated verbs the schema declares: get|ls|watch|add|
# patch|erase. Flags come before arguments. A custom verb takes a REF where
# its request names a row and the rest of the request as protojson; every
# field shows with `-o protojson`. A state is a verb of its own (`pending`,
# `quarantined`), since an enum is not something `ls` filters by.

# hosts (§33.4)
shale node ls|pending|get|erase|watch
shale node adopt <node>
shale node resolve <node> '{"from":"<ip>"}'                 # the endpoint that caller would get (§34.10)
shale relay ls|pending|get|patch|erase|watch                 # patch: labels
shale relay adopt <relay>
shale relay assign <relay> '{"producer":{"id":"<producer>"}}' # move a producer (§39.2)
shale producer ls|pending|get|erase|watch                    # tenant API
shale producer adopt <producer> '{"set":{"id":"<set>"}}'
shale producer scan|probe                                    # what this host can see (§38.4)
shale reader ls|pending|get|erase|watch                      # tenant API
shale reader adopt <reader> '{"sites":[{"id":"<site>"}]}'

# cluster setup (cluster API)
shale tenant add|ls|erase|patch          # patch: capacity_share
shale signing-key ls|watch|rotate
shale placement-policy add|ls|activate
shale address-policy add|ls|activate     # how nodes are named to clients (§34.10)
shale upload-policy add|ls|activate      # bounds for negotiated upload profiles
# `ca rotate`, a CA rollover started early (§33.5), comes with the rollover itself (#60)

# cluster state and operations (cluster API)
shale device ls|quarantined|get|watch
shale device quarantine|release|retire|declare-dead|locate <device>
shale sink ls|pending|get|watch
shale sink adopt <sink> '{"node":{"id":"<node>"}}'  # re-home a sink (§28.3)
shale sink retire|reconcile|gc <sink>
shale gc run <sink>
shale index rebuild [<sink>]

# tenant work (tenant API)
shale set add|ls|get|patch|erase|watch   # patch: retention, placement, max_bitrate_total
shale set negotiate|allocate|live <set>
shale source add|ls|get|patch|erase|live
shale site add|ls|erase
shale site-member add|ls|erase           # which people and readers may see which site
shale holder add|ls|patch|erase          # people
shale object get|ls|watch
shale object reschedule '{"set":{"id":"<set>"},"from":"<t>","to":"<t>","expired":"<date>","deleted":"<date>","reason":"<text>"}'
shale object timeline '{"set":{"id":"<set>"},"from":"<t>","to":"<t>","size":n,"after":"<cursor>"}'
shale live [--for d] <set>               # a WebRTC viewer per source, for a look (§39.4)
```
