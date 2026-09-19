# Shale — Operations and Failure Handling

## 27. Node / Device / Sink Health and Quarantine

Nodes send heartbeats:

```text
node_id
per device: device_id, health, error counters, SMART summary,
            queue depth per class, write/read latency
per sink:   sink_id, device_id, total/free bytes (§22.2), pressure state,
            uploads in flight, capability probe results, warnings
            (e.g. several sinks on one device)
```

Health is tracked where failures actually happen:

- **Device**: I/O errors, latency, failed WRITE jobs, and SMART. A device's
  state applies to every sink on it.
- **Sink**: pressure and capability. These are filesystem- and quota-level
  conditions.
- **Node**: reachability and failures reported by Writers.

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
             Writer-reported failure +1, SMART as above
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
| `shale device ls --quarantined` | devices awaiting a decision; `watch` for alerts |
| `shale device get <device>` | the record above: why, since when, and what is at stake |
| `shale device release <device>` | back on probation (e.g. after reseating a cable) |
| `shale device retire <device>` | reads only, never again for writes; objects stay readable until they expire |
| `shale device declare-dead <device>` | its objects → LOST; the device is forgotten |
| `shale device locate <device> [--off]` | light the bay LED, so the right HDD is pulled |

A replaced HDD joins as a new device with a new sink
([§9](02-data-model.md#9-identity)). An operator can also retire a single sink.

Consoles watch `Device` to show the quarantine queue live.

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
are independent, self-describing filesystems. If the node cannot be repaired,
**move the HDDs into another chassis**:

```text
1. attach the HDDs to node B
2. node B reads each sink label, scans xattrs, registers the sinks
3. CP updates sinks.node_id
4. objects are readable again; no object rows change
```

This recovers data without violating "no migration", because no bytes are
copied.

Shale isolates and accepts failures rather than repairing them.

## 29. Metadata Index

The index is a **cache of truth that lives in the sinks**. Losing it is an
availability problem, not data loss.

- Run the DB with ordinary replication/backups. This is cheap and avoids
  rebuilds, but it is not critical.
- **Rebuild** path: every node scans its sinks (directory walk + `getxattr`)
  and streams records to the CP. Because the xattrs are inline in the inodes,
  the scan reads inodes only, never object data.
- Rescheduled dates, the audit trail, holders, sites, and policies exist only
  in the DB. The xattrs keep only the initial dates. These are the things that
  are actually lost with the DB, so back them up.

Scale check: 10 PB at 64 MB ≈ 160 M objects. At 267 MB/s per node, each node
commits ~4 objects/s. This is well within a single PostgreSQL instance, and
SQLite handles a single machine ([§34.2](11-deployment.md#342-external-dependencies)).
Suggested index: `(sink_id, date_expired)` for GC approval,
`(source_id, date_started)` for range queries.

## 30. Integrity

- **Bit rot is accepted.** No scrub, no read verification by default. A
  damaged segment usually decodes with artifacts, which is acceptable for CCTV.
- The size check is mandatory (it detects partial files). A checksum, enabled
  per set (e.g.
  CRC32C computed while receiving) is optional. When enabled, it is stored in
  the xattr and checked only on demand.

Authentication, tokens, enrollment, and TLS are covered in
[§33](10-security.md#33-authentication-and-authorization).

## 31. Observability

```text
ingest
  write latency (allocate → 201), commit latency, allocation latency
  uploads in flight per sink, part buffer pool usage, 503 rate
  resumes per upload, idle timeouts
  live lag (recorded time − time covered by bytes on a sink), per producer
  abandoned uploads: finalized incomplete / deleted
  upload duration by producer (spots slow or degrading links)
  same-target / placement retry counts, lost objects
  duplicate attempts, orphans reclaimed

device
  queue depth and wait time per class (WRITE/READ/MAINT)
  I/O errors, SMART attributes, failure score, quarantine transitions

sink
  free bytes, free ratio, pressure state
  GC proposed / approved / reclaimed bytes
  forecast: incoming vs. reclaimable for the coming epoch (§11.1)

capacity
  time until the cluster runs out, from the same forecast
  stored bytes and share per tenant (§21.4)

read
  read latency, open sessions, stalled sessions, aborted sessions

node
  NIC ingress/egress utilization, RAM buffer usage

ceilings (§26)
  current ingest vs. NIC / HDD / capacity ceilings per node
```

Structured logs and distributed tracing are also useful.

## 32. CLI / Processes

The binary is also the CLI. payday generates `get`, `ls`, `watch`, `add`,
`patch`, and `erase` for each entity where the schema declares them, and rows
are named by ID or slug ([§35.1](12-api.md#351-conventions)).

```bash
# processes (§34.1)
shale control                            # tenant API
shale cluster                            # cluster API
shale storage --join <cp> --token <t> --ca-hash <h>
shale all [--dev <dir>]                  # everything in one process

# cluster setup (cluster API)
shale cluster init                       # CA, signing key, first tenant, admin credentials
shale tenant add|ls|erase|patch          # patch: capacity_share
shale join-token add --ttl 1h
shale signing-key ls|watch|rotate
shale placement-policy add|ls|activate
shale address-policy add|ls|activate     # how nodes are named to clients
shale node resolve <node> [--from <ip>]  # the endpoint a caller would get
shale upload-policy add|ls|activate      # bounds for negotiated upload profiles

# cluster state and operations (cluster API)
shale node ls|get|watch
shale device ls [--quarantined]|get|watch
shale device quarantine|release|retire|declare-dead|locate <device>
shale sink ls|watch|retire
shale gc run <sink>

# tenant work (tenant API)
shale set add|ls|get|patch|erase|watch
shale source add|ls|get|patch|erase
shale enrollment-token add --role producer|reader [--set <set>] --ttl 24h
shale holder ls|erase                    # erasing a holder revokes its credential
shale site add|ls|erase
shale site-member add|ls|erase           # which holders may see which site
shale object get|ls|watch
shale object reschedule --set <set> --from <t> --to <t> \
      [--expired <date>] [--deleted <date>] --reason <text>
shale object timeline --set <set> --from <t> --to <t>
```
