# Shale — Operations and Failure Handling

## 27. Node / Device / Sink Health and Quarantine

Nodes send heartbeats every `heartbeat_interval` (5 s). A node whose
heartbeats are missing for `node_down_after` (30 s) is **down**: its sinks are
skipped by placement and its laminae read as UNAVAILABLE. Relays are held
to a shorter schedule of their own, `relay.heartbeat_interval` (2 s) and
`control.relay_down_after` (6 s), since a viewer waits on a relay that
went and nothing is lost when one is wrongly held down: a relay that is
down has its producers reassigned ([§39.2](16-relay.md#392-assignment)).

```text
node_id, certificate serial, CA bundle hash, key IDs held
per device: device_id, health, error counters, SMART summary,
            queue depth per class, write/read latency
per sink:   sink_id, device_id, total/free bytes (§22.2), pressure state,
            uploads in flight, capability probe results, warnings
            (e.g. several sinks on one device)
```

The CP believes a node only about what is attached to it: a report about a
device or sink another live node holds changes nothing, its SMART and its
pressure included, and a reported sink capacity above `max_sink_capacity`
(64 TB) is stored clamped and flagged, so neither a typo nor a compromised
node can capture the cluster's writes or empty another node's sink out of
placement ([§11](03-placement.md#11-placement), [§33.7](10-security.md#337-what-a-compromise-costs)).

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
                      laminae and bytes on it }
```

| Command | Effect |
|---|---|
| `shale device quarantined` | devices awaiting a decision; `watch` for alerts |
| `shale device get <device>` | the record above: why, since when, and what is at stake |
| `shale device release <device>` | back on probation (e.g. after reseating a cable) |
| `shale device retire <device>` | reads only, never again for writes; laminae stay readable until they expire |
| `shale device declare-dead <device>` | its laminae → LOST; the device is forgotten |
| `shale device locate <device> ['{"off": true}']` | light the bay LED, so the right HDD is pulled; `off` puts it out |

Every one of these reaches the node through its control API. A replaced HDD
joins as a new device with a new sink ([§9](02-data-model.md#9-identity)). An
operator can also retire a single sink.

The console shows the quarantine queue and the hosts waiting to be adopted
([§33.4](10-security.md#334-joining-and-adoption), [§40](17-console.md#40-console)):
the rows it drew are watched, and the lists are read again for what
arrives.

## 28. Failure Semantics

### 28.1 No draining, no migration

```text
no replica · no EC · no RAID · no draining · no migration · no rebuild
```

Laminae on a device or node under maintenance are temporarily UNAVAILABLE. This
availability loss is deliberate. Placement ([§11](03-placement.md#11-placement)) shapes how that loss is
distributed across sources and time.

### 28.2 Device failure

```text
device failure
→ only the laminae on that device's sinks are LOST
→ every other device keeps working
→ no rebuild, no restoration
```

### 28.3 Node failure and device re-homing

A node failure makes all of its sinks' laminae UNAVAILABLE, not LOST: the sinks
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
6. laminae are readable again; no lamina rows change
```

**The host is gone for good**: `shale node erase` says so, and is not undone
([§33.4](10-security.md#334-joining-and-adoption), *Erasing a host*). Its
sinks' laminae are UNAVAILABLE from then until the sinks are adopted, not
LOST, exactly as when A is down above. They cannot be moved first: an
adoption is refused while the sink's node still heartbeats ("erase or stop
that node first"), so erasing or stopping A is the first step, and B adopts
the sinks after it.

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

### 28.4 Changing the disk under a node

A sink is a directory, and a disk is exchanged by adding the new sink and
taking the old one away; nothing is moved
([§28.1](#281-no-draining-no-migration)). The order is what keeps the
recording going, and each step is one an operator can stop after:

```text
1. mount the new disk and list it beside the old one; restart the node
   → both sinks are registered, and placement writes to both
2. shale sink retire <old>
   → the node takes no more writes there; its laminae stay readable
3. wait as long as the recordings are worth keeping (their retention, or
   not at all)
4. shale device declare-dead <old device>   # only when the data goes
   → its laminae become LOST, and the timeline says so (§19)
5. take the sink out of the node's configuration; restart the node
   → the row keeps its `RETIRED` attachment, and nothing asks for it again
6. the disk is now Shale's business no longer: unmount, wipe, return
```

Steps 4 and 5 are the ones with a choice in them. Skipping 4 keeps the rows
of laminae nobody can read, which a reader sees as `UNAVAILABLE` rather than
as a gap — right while a disk is away for repair, wrong for a disk that is
not coming back. Doing 5 before 4 is allowed and answers the same way.

A node refuses to start when a sink it is told to open is not there, so the
configuration and the mounts are changed together. This is deliberate: a
node that quietly came up without one of its disks would look healthy while
its laminae read as gaps.

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
  never lamina data.
- The xattr carries the lamina's current dates as last told to the node
  ([§20.3](06-retention-gc.md#203-rescheduling)), so rescheduled laminae come
  back with their rescheduled dates, except for changes made while their node
  was unreachable.
- What exists only in the DB: the audit trail, people, sites, policies, and
  the host rows with their certificate serials. After a DB loss every host
  therefore has to be adopted again. Back the DB up.

Scale check: 10 PB at 64 MB ≈ 160 M laminae. At 267 MB/s per node, each node
commits ~4 laminae/s. This is well within a single PostgreSQL instance, and
SQLite handles a single machine ([§34.2](11-deployment.md#342-external-dependencies)).
Suggested indexes: `(sink_id, date_expired)` for GC approval,
`(source_id, date_started)` and `(set_id, date_started)` for range queries.
Rows of deleted and lost laminae are pruned
([§20.4](06-retention-gc.md#204-row-retention)), so the table holds about
one retention period of laminae plus 30 days.

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
  producer segments by outcome: stored, lost, cut short under `retain: written` (§12.2),
    handed off to the satellite under `retain: accepted` (§41.9)
  upload duration by producer (spots slow or degrading links)
  same-target / placement retry counts, lost laminae
  duplicate attempts, orphans reclaimed, laminae recovered by reconciliation

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

satellite (§41)
  in-flight pool and its budget, requests passing through
  producers assigned, bytes forwarded, requests refused
  retained pool and its budget, segments held, bytes accepted,
    segments delivered, given up, passed through (§41.9)
  CPU

control plane
  hosts pending adoption, certificates due for renewal
  producers per relay, reassignments
  directives pending per node, reconciliation lag per sink
  laminae in DELETING, rows pruned
  watch streams, broker reconnects

ceilings (§26)
  current ingest vs. NIC / HDD / capacity ceilings per node
```

Every line above is an instrument every process exports over OTLP, to
wherever `otel:` in its configuration says
([§36.1](13-configuration.md#361-configuration-reference)); there is no
scrape endpoint. The names, by the process that measures them:

| §31 line | Instrument |
|---|---|
| write, commit, allocation latency | `shale.cp.write_latency_ms`, `shale.cp.commit_latency_ms`, `shale.cp.allocation_latency_ms` |
| uploads in flight, part buffer pool, 503 rate | `shale.node.uploads_in_flight`, `shale.node.part_buffer_used_bytes`, `shale.node.refused{reason}` |
| data plane requests by HTTP version ([§35.6](12-api.md#356-storage-node-http-data-plane)) | `shale.node.requests{proto=h1,h2,h3}` |
| resumes, idle timeouts, per-actor limit hits | `shale.node.upload_resumes`, `shale.node.uploads_abandoned{outcome,why}`, `shale.node.refused{reason=upload_limit,read_limit}` |
| live lag | `shale.node.oldest_upload_age_ms{sink}` |
| abandoned uploads | `shale.node.uploads_abandoned{outcome=finalized_incomplete,deleted}` |
| producer segments by outcome, cuts | `shale.producer.segments{outcome=stored,lost,cut,skipped,handed_off}`, `shale.producer.early_cuts{source}` |
| dark scenes (§38.10) | `shale.producer.dark{source}` (1 while skipping), `shale.cp.laminae_skipped{reason}` |
| upload duration by producer | `shale.producer.upload_duration_ms` |
| retry counts, lost laminae | `shale.producer.retries{kind=same_target,placement}`, `shale.cp.laminae_lost{by=producer,satellite}` (a node's missing file is counted without `by`) |
| duplicates, orphans, recovered | `shale.cp.duplicates`, `shale.node.gc_approved{reason}`, `shale.cp.recovered{sink}` |
| device queue depth and wait per class | `shale.node.device_queue_depth{device,class}`, `shale.node.device_wait_ms{class}` |
| I/O errors, failure score, quarantines | `shale.cp.device_io_errors{device}`, `shale.cp.device_failure_score{device}`, `shale.cp.quarantines{kind}` |
| sink free, pressure | `shale.node.sink_free_bytes{sink}`, `shale.node.sink_pressure{sink}` |
| GC proposed, approved, reclaimed; sweep | `shale.node.gc_proposed{reason}`, `shale.node.gc_approved{reason}`, `shale.node.gc_reclaimed_bytes`, `shale.cp.gc_approved_bytes{sink,reason}`; the sweep is `reason=sweep` |
| forecast, time until the cluster runs out | `shale.cp.capacity_runway_s` |
| stored bytes per tenant | `shale.cp.stored_bytes{tenant}` |
| read latency, sessions, aborted | `shale.node.read_latency_ms{sink}`, `shale.node.read_sessions`, `shale.node.reads_aborted{sink}` |
| where one read's time goes: the device's wait and read, the rest writing to the client; bytes read from the device beside bytes sent ([§17.2](05-read-path.md#172-read-chunks)) | `shale.node.read_wait_ms{sink}`, `read_device_ms{sink}`, `read_send_ms{sink}`, `read_device_bytes{sink}`, `read_sent_bytes{sink}` |
| a browser's MP4 export as the page reports it ([§40](17-console.md#40-console)): all of it, the index, the media requests' first bytes, bodies and the gaps between them, joining, and the media rate | `shale.node.export_total_ms`, `export_index_ms`, `export_first_byte_ms`, `export_body_ms`, `export_gap_ms`, `export_assemble_ms`, `export_rate_mbps`, `export_bytes` |
| NIC utilization, RAM buffer, index size, scan | `shale.node.nic_rx_bps{interface}`, `shale.node.nic_tx_bps{interface}`, `shale.node.part_buffer_used_bytes`, `shale.node.index_laminae{sink}`, `shale.node.scan_done{sink}` |
| ingest per node | `shale.node.ingest_bps` |
| producer per source | `shale.producer.input_up`, `frame_rate`, `rate_bps`, `keyframe_interval_ms`, `capture_restarts`, `early_cuts`, all `{source}` |
| producer host, uplink, live helpers | `shale.producer.cpu`, `temperature`, `uplink_bps`, `live_transcodes`, `live_dropped` |
| the path a producer's uploads take, and its satellite failing ([§41.5](18-satellite.md#415-from-the-producer)) | `shale.producer.upload_requests{path=direct,satellite}`, `shale.producer.satellite_fallbacks` |
| how long live waits on the producer: queued until the relay stream takes it ([§39.3](16-relay.md#393-from-the-producer)) | `shale.producer.live_send_wait_ms` |
| frames the recording lost: gaps over one and three quarters frames, and wall time a capture's rebased timestamps hid ([§38.6](15-producer.md#386-health-and-heartbeats)) | `shale.producer.frame_gaps{source}`, `shale.producer.frames_missed{source}` |
| captures that delivered nothing for 5 s or more, and the timestamp jumps ffmpeg rebased | `shale.producer.capture_stalls{source}`, `shale.producer.capture_stalled_seconds{source}`, `shale.producer.timestamp_discontinuities{source}` |
| the Go runtime, every process: collections and their CPU, assists, heap, goroutines, scheduling | `shale.runtime.gc.cycles`, `gc.cpu`, `gc.assist`, `heap.live`, `heap.goal`, `memory`, `goroutines`, `sched.latency_max` |
| relay | `shale.relay.attached_producers`, `active_sources`, `viewers`, `egress_bytes`, `sessions{outcome}`, `first_frame_ms`, `catch_up_ms`, `nacked_packets`, `cpu` |
| satellite ([§41.4](18-satellite.md#414-memory)) | `shale.satellite.in_flight_bytes`, `in_flight_budget`, `uploads`, `producers`, `forwarded_bytes`, `refused`, `cpu` |
| a satellite's live fan-out: the sources it feeds relays, the bytes sent, the fragments dropped for a relay behind; the relay's handoffs between a satellite and a producer's tee ([§41.8](18-satellite.md#418-live-fan-out)) | `shale.satellite.live_sources`, `live_bytes`, `live_dropped`; `shale.relay.feeder_handoffs` |
| `retain: accepted` ([§41.9](18-satellite.md#419-retain-accepted)) | `shale.satellite.retained_bytes`, `retained_budget`, `held`, `accepted_bytes`, `delivered`, `given_up`, `passed`; `shale.producer.handed_off_bytes{source}`, `shale.producer.accept_passed{reason}` |
| what pacing adds to live's delay, and its clock's moves ([§39.4](16-relay.md#394-viewers)) | `shale.relay.pace_delay_ms`, `shale.relay.pace_resets{why=late,ahead}`, `shale.relay.pace_recovered_ms` |
| live's way from the producer: its queue (t2 − t1), the uplink (t3 − t2), and the payloads whose clocks disagree ([§39.3](16-relay.md#393-from-the-producer)) | `shale.relay.producer_queue_ms{producer}`, `shale.relay.uplink_ms{producer}`, `shale.relay.uplink_skewed{producer}` |
| live's way to the screen: held in the relay (t4 − t3), and what timed viewers report, the network (t5 − t4), the browser (t6 − t5) and all of it (t6 − t1) ([§39.4](16-relay.md#394-viewers)) | `shale.relay.hold_ms{path=paced,catch_up}`, `shale.relay.viewer_network_ms`, `viewer_playout_ms`, `viewer_total_ms` |
| hosts pending, certificates due | `shale.cp.hosts_pending{kind}`, `shale.cp.certs_due{kind}` |
| producers per relay, reassignments | `shale.cp.producers_per_relay{relay}`, `shale.cp.relay_reassignments` |
| directives pending, reconciliation lag | `shale.cp.laminae_dates_unsynced`, `shale.cp.reconcile_lag_s{sink}`, `shale.cp.directive_errors` |
| the `dns` resolver's records: updates by what and how, and the age of the last whole read of the zone ([§34.10](11-deployment.md#3410-node-addresses)) | `shale.cp.dns_updates{op=create,replace,delete; result=ok,error,conflict}`, `shale.cp.dns_sync_lag_s` |
| laminae in DELETING, rows pruned | `shale.cp.laminae_deleting`, `shale.cp.rows_pruned{kind}` |
| watch streams, broker reconnects | payday's, when it exports them; none here yet |

Logs are structured (`slog`), and a presigned URL never reaches one whole:
what is logged is the key, and where a URL is logged its `token` is
replaced (`token.RedactURL`). Tracing rides on the same `otel:` block.

## 32. CLI / Processes

The binary is also the CLI. payday generates `get`, `ls`, `watch`, `add`,
`patch`, and `erase` for each entity where the schema declares them, and rows
are named by ID or slug ([§35.1](12-api.md#351-conventions)). The CLI signs
in as a person (`shale login`) and keeps a session.

A script calls the tenant API with a person's key for Shale instead
([§33.1](10-security.md#331-trust-model)): made by its person on the
console's *Tokens* page, or minted at roster for the app Shale with the
methods the script calls -- on roster's account page, or `roster me
issue-key --app shale` -- and handed to the CLI as `client.token`,
in the file or as `SHALE_CLIENT_TOKEN`, which is sent as `Authorization:
Bearer` on every call. There is no `--token` flag: a flag is in the shell's
history and the process list, and the variable is not. The cluster API does
not take a key.

```bash
export SHALE_CLIENT_TOKEN=rt_…             # from a secret store, not the command line
shale --addr https://shale.example:7400 source ls
shale --addr https://shale.example:7400 lamina timeline '{"source":{"id":"<source>"},"from":"<t>","to":"<t>"}'
```

A key is held to its own methods and to what its person may do, both: a
method it does not name is `PERMISSION_DENIED`, *this credential is not for
that*; a key ended at roster, of somebody suspended there, or for another
app is `UNAUTHENTICATED`, within `auth.operators.ttl` (30 s) of being
ended. A `watch` opened with a key is cut at the key's end, and within the
same 30 s of the key being ended at roster; a key that watches names the
entity's `Get` beside its `Watch` (`/shale.SetService/Watch` and
`/shale.SetService/Get`), since each row a stream sends is read as a `Get`.

**A key that leaked** is ended by its person on the *Tokens* page
(`HolderService/EndKey`), or at roster: on the account page's *revoke*, or
by the tenant's administrators and Shale's (an operator of roster's who
holds `app-administrator` for Shale ends anybody's key for Shale there,
`ApiKeyService/Erase`); suspending the person ends every key of theirs.
It lives on **for `auth.operators.ttl`** (30 s) at most after that: a call
made with it is served while what roster said of it is believed, and a
`watch` opened with it is cut when it runs out, since a key with a stream
open is asked about again then; where Shale follows roster's sync stream, a
suspension lands at once. What it signed before it was ended -- a view
token, a read token, an export ticket -- runs out on its own clock, minutes
at most ([§33.7](10-security.md#337-what-a-compromise-costs)). The
trail at roster says who ended it; Shale's says nothing of a key, since
none of its rows changed.

**A script's token narrowed to some cameras** carries one of its person's
lists of cameras, a `reach` ([§33.1](10-security.md#331-trust-model), *A
key narrowed to some cameras*). The person makes the list -- sites and
cameras they see -- and the token with it on the *Tokens* page, *which
cameras*: *a list of cameras*; or makes the list with `shale reach add`
signed in, and asks for the key with its identifier as the scope
(`HolderService/BeginKey`'s `scope`). Such a token may call only the reads
and views of cameras -- `source`, `set`, `lamina` and `attempt` reads and
watches, `live`, `timeline` and `export` -- and
answers only what the list reaches of what its person sees; anything else
is `PERMISSION_DENIED`, *this key carries a list of cameras*, whatever the
token was minted with.

```bash
shale reach add '{"name":"gate cameras","cameras":{"sites":["<site>"],"sources":["<source>"]}}'
shale reach ls
shale reach patch <reach> '{"cameras":{"sources":["<source>"]},"date_updated_force":true}'
shale reach erase <reach>
```

**Changing its list** changes what every token that carries it reaches,
from its next call (within `auth.operators.ttl` where several control
planes serve the tenant); the token itself is not made again. **Erasing
the list** leaves its tokens reaching nothing: a list answers empty, and a
call that names a camera is `FAILED_PRECONDITION`, *this key's list of
cameras is gone*. **Ending the token** is as for any key, above; its list
stays, for other tokens to carry.

```bash
# processes (§34.1)
shale init [--tenant t] [--admin a] [--operator o]   # CA, signing key, first tenant, first operator and admin
shale serve control|cluster|storage|producer|reader|relay|all [--dev <dir>]
shale serve producer --demo 3              # three cameras ffmpeg draws for itself (§38.1)
shale login [--cluster] [--password p] @tenant/alias # sign in as a person; keeps a session
shale holder issue-password <holder>       # a fresh password, shown once; roster in this process only
shale identity migrate                     # once, after upgrading a deployment that predates roster (§33.1); another roster: §32.1

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
shale producer push [--to ADDR] [--open] <alias> [FILE]      # write a stream to a pushed source (§38.9)
shale reader ls|pending|get|erase|watch                      # tenant API
shale reader adopt <reader> '{"sites":[{"id":"<site>"}]}'    # none: the whole tenant; again, new key: its sites (§36.4)

# cluster setup (cluster API)
shale tenant ls|erase|patch              # patch: capacity_share; tenants come from roster (§33.1)
shale signing-key ls|watch|rotate
shale placement-policy add|ls|activate
shale address-policy add|ls|get|patch|activate  # how nodes are named to clients (§34.10)
shale upload-policy add|ls|activate      # bounds for negotiated upload profiles
shale ca ls|get|watch                    # the CAs, once a rollover stored them (§33.5)
shale ca status                          # the phase, the bundle and its hash, the live hosts a rollover waits for
shale ca rotate                          # begin a CA rollover now rather than a year before the CA expires

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
shale reach add|ls|get|patch|erase        # a person's own lists of cameras, which a token may carry (§33.1)
shale site add|ls|get|patch|erase       # visibility: who sees it, its members, the company's people or everybody (§33.1)
shale site-member add|ls|erase           # which people and readers a site names
shale holder add|ls|patch|erase          # people: `add` makes them at roster first (§33.1)
shale lamina get|ls|watch
shale lamina reschedule <lamina> --expired <t> --deleted <t> --reason <text>
shale lamina reschedule --set <set>|--source <source> --from <t> --to <t> --expired <t> --reason <text>
shale lamina reschedule ... --delete-now --reason <text>   # both dates to now (§20.3)
                                         # <t>: RFC 3339, a date, `now`, or a duration from now (--from=-24h)
shale lamina timeline '{"set":{"id":"<set>"},"from":"<t>","to":"<t>","size":n,"after":"<cursor>"}'
shale live [--for d] <set>               # a WebRTC viewer per source, for a look (§39.4)
```

### 32.1 Moving to another roster

Shale keeps a tenant and a person by roster's identifier: `Tenant.id` is
roster's tenant, `Holder.id` roster's holder and the `sub` of every token,
and every row of a tenant hangs off its identifier
([§33.1](10-security.md#331-trust-model)). Pointing a deployment at
another roster -- a new instance, or one whose database was made again --
keeps all of it only where that roster names the same tenant and the same
people by the same identifiers. Both are written by roster's operator,
before anybody signs in:

```sh
shale tenant ls                                  # each tenant's id, as this deployment has it (cluster API)
roster --as <operator> tenant add @acme '{"id": "<that id>"}'
shale holder ls                                  # each person's id, in their tenant
roster --as <operator> holder add @acme/bob '{"id": "<that id>"}'
```

and then Shale is installed in the tenant as on any roster
([§33.1](10-security.md#331-trust-model)).

- **The tenant, by its identifier.** A sign-in for a tenant this
  deployment has under another identifier than roster gives it is refused,
  saying so and what to do, and logged once, at ERROR, with both
  identifiers; nothing is made, and nothing is moved onto the new one. The
  browser's and the command line's sign-in pages say it; the password form
  answers 401, as it answers any sign-in that failed, and the reason is in
  the log. roster's declared rows (the file
  `roster serve` applies) cannot say a tenant's identifier, so a tenant
  declared there is made with a new one unless it was added as above first.
- **People, by theirs.** A person made with their old identifier is who
  they were: their sites (`all_sites`, their site memberships) and the
  delegations apps started for them are theirs. A person roster names by
  another identifier than the row of their alias has is refused at sign-in
  the same way, saying so and what to do, and logged once, at ERROR, with
  both identifiers: roster's operator makes them again with the identifier
  `shale holder ls` prints, or an administrator erases the row here
  (`shale holder erase`). Erased, the person arrives as a new holder at
  their next sign-in, and what was the old row's is not theirs: their site
  memberships, an `all_sites` an administrator gave them, and a delegation
  an app started for them. An administrator gives it again -- every site
  with `shale holder patch` and `all_sites`, some with `shale site-member
  add` -- where the new row does not have it already: a new person sees
  the sites a site's kind gives everybody, and nothing else until given
  it; with `auth.operators`, `all_sites` is not read, and every site is
  whom roster grants all of Shale. What they may
  do at Shale is what roster grants the new holder
  ([§33.1](10-security.md#331-trust-model)).
- `shale identity migrate` writes a deployment's tenants and people into
  the roster in this process with their identifiers; it does not reach a
  roster elsewhere.

### 32.2 Upgrading past roster's v0.2.0 freeze, with roster in this process

roster keeps nothing for an earlier roster until it is released, and the
roster this build links reads no database an earlier one made. A
deployment whose roster runs in the control plane's process (no
`auth.roster.addr`) cannot carry its `roster.db` across: this build
refuses to start on it, with roster's own words about its columns and
then Shale's, naming this section. Shale's own database is read as it
was. What it takes:

```sh
mv /var/lib/shale/control/roster.db /var/lib/shale/control/roster.db.before   # and its -wal and -shm
shale identity migrate      # every tenant and person of Shale's, under their identifiers; a password each, printed once
shale serve …
```

`identity migrate` makes again, from Shale's own rows, each tenant (its
identifier, alias and name) and each person (identifier, alias, name),
binds whoever sees every site to the role that administers the tenant at
roster, and issues every person a new password. Nothing else of the old
file comes across:

- **the passwords**: every person signs in with the one printed;
- **what roster bound** besides that one role: the operators' role
  (`auth.operators`) and any role or binding a tenant administrator wrote
  there, which are written again;
- **people roster had and Shale had not met** (nobody who never signed in
  has a row here), who are made again at roster;
- whatever else that roster kept of anybody: addresses, second factors,
  keys, and its own trail and access record.

Shale's holder in each tenant, its role and the binding are written again
on their own the first time Shale asks roster anything there. Where
`auth.roster.db` names a database of its own, it is that database which is
moved aside or emptied.

### 32.3 Upgrading to sites that say who sees them

A site now says who sees it: its members, the company's people, or
everybody signed in ([§7](02-data-model.md#7-source-set-zone-epoch)). The
database gains the column on its own when the control plane starts, and
every site that exists reads as `MEMBERS`, so the kinds show nobody more
than before.

**One thing changes what somebody sees, where `auth.operators` is set.**
Until now every person's row was made with `all_sites` on there, and it
was read, so everybody saw every site and every camera in no site. A
person's `all_sites` is not read there any more, and a row is no longer
made with it: who sees every site is whoever roster grants all of Shale.
**A person who is not granted all of Shale stops seeing every site** and
sees the sites the kinds and memberships give them -- with no site said
`EVERYBODY` or `COMPANY` and no membership, no camera at all -- and a set
in no site is seen by whoever is granted all of Shale alone
([§33.1](10-security.md#331-trust-model)). No row is rewritten. Without
`auth.operators` nothing changes: `all_sites` is read as before.

What to do, so that nobody loses a camera they should keep:

- **Grant all of Shale** (`/shale.*/*`) at roster, before the upgrade, to
  whoever should go on seeing every site and every camera in no site.
- **Add the people a `MEMBERS` site names**, before or after: a
  membership counts whatever the site's kind.
- **Say each site's kind** once the upgrade is in: `EVERYBODY` for one
  everybody signed in should see, `COMPANY` for the company's people, and
  `MEMBERS`, which every site starts as, for one only the people it names
  see (who is of the company: [§7](02-data-model.md#7-source-set-zone-epoch)).
- **For `COMPANY`, at a roster elsewhere**, roster's operator adds
  `/roster.EmailService/List` and `/roster.MailDomainService/List` to what
  Shale may call in the tenant (`roster app install --allow`, beside the
  methods named there already, [§33.1](10-security.md#331-trust-model)),
  and the tenant names its mail domains at roster (a `MailDomain`, by
  name, with the connection it routes to). Until both are so, nobody is of
  the company, a `COMPANY` site is seen by the people it names, and the
  log says which methods are missing. Where roster runs inside Shale,
  everybody of the tenant is of the company and nothing is added.
- **Cameras in no site** are not shown to anybody else by any kind. A
  deployment that added its cameras before it used sites has every set in
  no site, so after the upgrade: **make the sites** (`site add`), **say
  each one's kind**, add the people a `MEMBERS` one names, and **place the
  sets** in them (`set place`, [§32.4](#324-placing-a-set-in-a-site)),
  each with everything it ever recorded. A set whose cameras Shale manages
  moves with its camera network, and its producer is restarted after it
  ([§32.4](#324-placing-a-set-in-a-site)).

```sh
shale set ls                     # a set with no site is seen by whoever is granted all of Shale alone
shale site add '{"tenant":{"alias":"acme"},"alias":"hall","visibility":"SITE_VISIBILITY_EVERYBODY"}'
shale site-member add '{"tenant":{"alias":"acme"},"site":{"id":"<site>"},"holder":{"id":"<person>"}}'
shale site patch <site> '{"visibility":"SITE_VISIBILITY_EVERYBODY","date_updated_force":true}'
shale set place --dry-run @acme/yard @acme/hall   # what would move
shale set place @acme/yard @acme/hall
```

**Everybody holds the basics, at roster too, where `auth.operators` is
set and its reads are everybody's** (the default,
[§33.1](10-security.md#331-trust-model)). Two things change on the
upgrade:

- **Somebody who read hosts with no role no longer does.** What every
  person calls with no role is now the basics -- the reads and views of
  cameras and sites -- and the hosts' reads are not among them:
  `Get`, `List` and `Watch` of `ProducerService` and `ReaderService`,
  which the console's *Hosts* page reads, and *Cameras* for whether a
  set's producer is up. They are refused `PERMISSION_DENIED`, and the two
  pages say what it takes. Give whoever looks after the hosts a role of
  Shale's naming them, bound to them or to a group of Shale's they are in:

  ```sh
  roster --as <operator> role add @acme/shale-hosts \
    '{"app": {"slug": {"alias": "shale", "tenant": {"alias": "acme"}}}, "methods": ["/shale.ProducerService/Get", "/shale.ProducerService/List", "/shale.ProducerService/Watch", "/shale.ReaderService/Get", "/shale.ReaderService/List", "/shale.ReaderService/Watch"]}'
  roster --as <operator> binding add \
    '{"role": {"slug": {"alias": "shale-hosts", "tenant": {"alias": "acme"}}}, "holder": {"slug": {"alias": "olive", "tenant": {"alias": "acme"}}}}'
  ```

  Whoever is granted all of Shale reads them as before. A producer's and a
  reader's own calls are unchanged.
- **Shale writes at roster**, as its own holder, in each tenant it serves:
  the role `shale-basics` (Shale's, naming the eighteen methods of the
  basics), the group `shale-people` (Shale's), the role bound to the
  group in no site -- each made once, at start or when a tenant is first
  served, and remembered on Shale's row of the tenant (`shale.basics`) --
  and a place in `shale-people` for each person it sees, from their next
  call. Each says in its `desc` that Shale made it. Nothing is asked of
  roster's operator: it is what `roster app install --serves '/shale.*/*'`
  lets an app write already. A roster that refuses (Shale installed
  without `--serves`) is one line in the log, and a token then offers only
  what roles an operator binds by hand.
- **After that the three are the operator's.** Shale does not write the
  role's methods again: `roster role widen` and `roster role narrow`
  change them, and where they differ from the build's list Shale says so
  once at start, naming the methods each way and the commands that would
  bring the role to the list, and changes nothing. A later upgrade that
  adds a method to the basics is said the same way and is the operator's
  to widen. What an operator removes -- the role, the group, the binding --
  is not made again, and said once at start. **Taking the basics away from
  the group** without changing `reads` is removing the binding of
  `shale-basics` to `shale-people` at roster (a token then offers only
  roles bound by hand, and the gate still lets everybody call the basics);
  **from everybody**, `auth.operators.reads: granted` as well, with which
  Shale writes none of it, and says once at start what an earlier
  `everybody` left there.

```sh
roster --as <operator> holder reaches @acme/vic '{"app": {"slug": {"alias": "shale", "tenant": {"alias": "acme"}}}}'   # the basics, once vic has called Shale
```

### 32.4 Placing a set in a site

`shale set place <set> <site>` (`SetService/Place`) puts a set that exists
into a site, or moves it from one to another, with every row that carries
its site because of it: its sources, its laminae, their attempts, its
producer ([§7](02-data-model.md#7-source-set-zone-epoch)). It prints where
the set was and is, and how many rows of each kind moved; `--dry-run`
counts them and moves nothing. Only whoever sees every site may (where
roster says what people may change, whoever it grants all of Shale; else
whoever's row has `all_sites`); for a set already in the site nothing is
written; no site, another tenant's and an erased one are refused. A set
whose cameras Shale manages moves with its camera network and everything
of it, or not at all: what is refused, and why, is in
[§7](02-data-model.md#7-source-set-zone-epoch), and `--dry-run` prints it.

```text
$ shale set place @acme/yard @acme/hall
yard: in no site, now in hall
moved 4 source(s), 2161 lamina(s), 2290 attempt(s), 1 producer(s)
```

**What it locks.** One transaction, five statements, the set's row first:

| rows | statement | index |
|---|---|---|
| the set | `UPDATE set SET site_id, date_updated WHERE id = ? AND site_id` is what it was | the key |
| its sources | `UPDATE source SET site_id WHERE set_id = ?` | `source_member` (`set_id`, `ordinal`) |
| its laminae | `UPDATE lamina SET site_id WHERE set_id = ?` | `lamina_set_time` (`set_id`, `date_started`) |
| their attempts | `UPDATE attempt SET site_id WHERE lamina_id IN (SELECT id FROM lamina WHERE set_id = ?)` | `attempt_lamina` (`lamina_id`, `date_created`), the laminae by `lamina_set_time` |
| its producer | `UPDATE producer SET site_id, date_updated WHERE tenant_id = ? AND set_id = ?` | `producer_page` (`tenant_id`, …) for the tenant: `producer` has no index on `set_id`, so this reads the tenant's producers, one row per host |

For a set whose cameras Shale manages, after the set's row and before the
rest: its networks' rows held (`FOR UPDATE`), the plan read again under
the hold, then one statement a kind, each `WHERE tenant_id = ? AND` the tie
of [§7](02-data-model.md#7-source-set-zone-epoch), on each kind's
`*_page` index for the tenant (these are rows per camera, not per
recording): networks by id, devices by `network_id`, credentials by their
device's network, credential versions by their credential's, assignments
by id, bindings and jobs by `assignment_id`, policies by `network_id`,
presets and the worker by id. Only `site_id` is written: no version
moves, so no camera operation under way is refused for a row changed
under it. The plan reads the tenant's camera jobs, to find the presets
they name. Every camera operation on an assignment holds the set's row and
then the network's (then, as it needs them, the assignment's, the device's
and its Sources'), adding a device or a policy holds the network's, and
a credential's store holds its device's: each waits for a placing, then
reads the site it gave.

A dry run is the same filters counted, a read that writes nothing. On
PostgreSQL the move holds the set's row and every row it rewrites until it
commits: an allocation, a source added, a producer adopted and a node's
report for **that set** wait for it, then write the site the set is in;
other sets go on. On SQLite every write of the control plane waits for
it, as for any write (each transaction takes the database at its
`BEGIN`).

**How long it takes.** As long as rewriting that many rows: no index is
on `site_id`, so none is rewritten, and the cost is the rows themselves.
Measured on SQLite, on a small machine (4 CPUs), two attempts a lamina:

| laminae | attempts | dry run | placing |
|---|---|---|---|
| 200,000 | 400,000 | 2 s | 8 s |
| 1,000,000 | 2,000,000 | 28 s | 62 s |

PostgreSQL was not measured. A set that has recorded for months is
millions of laminae. On SQLite that is every write of the control plane
waiting a minute or more, and a write that waits longer than the busy
timeout (a minute unless the DSN says `busy_timeout`) is refused
`database is locked`: producers ask for their allocations again and hold
what they record as their upload mode holds it
([§12.2](04-write-path.md#122-resumable-part-uploads)), nodes push their
events again. Place such a set when that is acceptable, with a longer
`busy_timeout` for it, or run the control plane on PostgreSQL, where only
that set's writes wait. On SQLite's default journal a dry run, a long
read, holds other writes' commits for as long as it reads.

**What is seen meanwhile.** Until it commits, nothing has moved: a viewer
sees the set where it was, its producer goes on uploading what it has
been allocated, and its next allocation waits. Once it commits, a person's
sight follows within the ten seconds Shale keeps a person's sites; a
producer's at once (its sites are read at each call). A key's list that
named one of the set's sources still reaches it; one that named the site
it left no longer does ([§7](02-data-model.md#7-source-set-zone-epoch)).
The tokens of the relay and the nodes carry the source, not the site, and
are unchanged. A producer's certificate names its row and nothing of a
site, and its relay and satellite are kept while they are up; when one is
assigned anew it is by the new site's selectors
([§39.2](16-relay.md#392-assignment), [§41.2](18-satellite.md#412-assignment)).
What a sink holds beside a lamina's bytes (its record,
[§23.1](07-storage-node.md#231-self-describing-laminae)) says the site the
set had when it was written: it is not rewritten, and a lamina rebuilt
from it takes the set's site of the moment instead.
**A set whose cameras Shale manages: restart its producer.** A producer
reads the site its set is in once, when it starts, and its cameras'
credentials are asked for in that site; Shale delivers one in the site the
assignment is in. After the move, the producer is refused its cameras'
next credential (a credential's lease lasts at most five minutes) until it
is restarted, and a camera that needs one does not record meanwhile.
Restart it right after the move. Its certificate names its row and nothing
of a site, and is unchanged. The camera worker's certificate names its row
alone too; it reads its site and its assignments at each call and goes on
without a restart.
A credential's sealed payload is bound to its tenant, device, binding,
credential, purpose and version, not to a site.

The trail has one line, the set's, with who placed it and the site it was
in and is in; the counts are in the answer and the log.

