# Shale — Operations and Failure Handling

## 27. Node / Device / Sink Health and Quarantine

Nodes send heartbeats:

```text
node_id
per device: device_id, health, error counters, queue depth per class,
            write/read latency
per sink:   sink_id, device_id, total/free bytes (§22.2), pressure state,
            uploads in flight, capability probe results, warnings
            (e.g. several sinks on one device)
```

Health is tracked where failures actually happen:

- **Device**: I/O errors, latency, and failed WRITE jobs. A device's state
  applies to every sink on it.
- **Sink**: pressure and capability. These are filesystem- and quota-level
  conditions.
- **Node**: reachability and failures reported by Writers.

### Local exclusion

On a device I/O error, the node stops accepting new writes to every sink on
that device at once and reports it.

### Quarantine (Control Plane)

Each device and node has a **failure score**: an exponentially decaying count
of I/O errors, failed WRITE jobs, timeouts, and failures reported by Writers.

```text
score < suspect                 healthy
suspect ≤ score < quarantine    weight reduced
score ≥ quarantine              QUARANTINED: no new writes, reads still served
```

- Hysteresis (separate enter/exit thresholds) prevents flapping.
- After a cool-down, a quarantined device returns on **probation** with a
  reduced weight and returns to full weight if it stays clean.
- An operator can pin a device or a single sink as `retired` (reads only) or
  `dead` (its objects → LOST).

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
- Holds, `must_delete_by` overrides, and policy exist only in the DB. These
  are the things that are actually lost with the DB, so back them up.

Scale check: 10 PB at 64 MB ≈ 160 M objects. At 267 MB/s per node, each node
commits ~4 objects/s. This is well within a single PostgreSQL instance, and
SQLite handles a single machine ([§34.2](11-deployment.md#342-external-dependencies)).
Suggested index: `(sink_id, expires_at)` for GC approval,
`(source_id, start_time)` for range queries.

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
  I/O errors, failure score, quarantine transitions

sink
  free bytes, free ratio, pressure state
  GC proposed / approved / reclaimed bytes

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
shale tenant add|ls|erase
shale join-token add --ttl 1h
shale signing-key ls|watch|rotate
shale placement-policy add|ls|activate
shale upload-policy add|ls|activate      # bounds for negotiated upload profiles

# cluster state and operations (cluster API)
shale node ls|get|watch
shale device ls|watch|quarantine|release|retire|declare-dead
shale sink ls|watch|retire
shale gc run <sink>

# tenant work (tenant API)
shale set add|ls|get|patch|erase|watch
shale source add|ls|get|patch|erase
shale enrollment-token add --role producer|reader [--set <set>] --ttl 24h
shale holder ls|erase                    # erasing a holder revokes its credential
shale object get|ls|watch|hold|release
shale object timeline --set <set> --from <t> --to <t>
```
