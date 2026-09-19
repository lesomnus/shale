# Shale — Configuration

## 36. Configuration and Open Decisions

### 36.1 Configuration reference

Every tunable in the design, with its default. Defaults are decided values: a
deployment runs well on them, and changes them only for a reason.

**Scope** says who sets a value:

- **cluster**: the operator, through the cluster API; stored in the CP DB, so
  every CP process agrees.
- **node**: the Storage Node's own configuration.
- **set**: a tenant admin, per set.
- **negotiated**: the producer proposes, and the CP clamps the proposal into
  cluster bounds ([§12.6](04-write-path.md#126-upload-profile-negotiation)).
- **writer**: the producer's own configuration; nobody else sees it.

| Area | Parameter | Default | Bounds | Scope | Described in |
|---|---|---|---|---|---|
| Placement | `epoch` | 1 h | — | set | [§11](03-placement.md#11-placement) |
| Placement | `set_spread` | `spread` | `spread`, `pack`, `none` | set | [§11](03-placement.md#11-placement) |
| Placement | `forecast_margin` | 1.5 | — | cluster | [§11.1](03-placement.md#111-capacity-forecast) |
| Placement | scheduler | weighted HRW | — | cluster (`PlacementPolicy`) | [§11.2](03-placement.md#112-scheduler-interface) |
| Addresses | resolver | `advertised` | `advertised`, `template`, `dns` | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Retention | `retention.expire` | 30 days | — | set | [§20.2](06-retention-gc.md#202-initial-dates) |
| Retention | `retention.delete` | none (never) | ≥ `retention.expire` | set | [§20.2](06-retention-gc.md#202-initial-dates) |
| Segments | target object size | 64 MB | 32–512 MB; segment ≤ `epoch` / 4 | negotiated, per source | [§25](07-storage-node.md#25-object-size) |
| Uploads | upload mode | `live` | `live`, `buffered` | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `idle_timeout` | 30 s | 10 s – 5 min | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `abandon_timeout` | 5 min | 1–30 min | negotiated, per set | [§15](04-write-path.md#15-partial-objects) |
| Uploads | `allocation_horizon` | 10 min | 1–30 min | negotiated, per set | [§12.1](04-write-path.md#121-flow) |
| Uploads | `allocation_ttl` | horizon + 10 min | derived | — | [§12.1](04-write-path.md#121-flow) |
| Uploads | `retain` | `committed` | `committed`, `written` | writer | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `resume_timeout` | 2 min | — | writer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | placement retries | 3 | — | writer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | checksum | off (CRC32C when on) | — | set | [§30](09-operations.md#30-integrity) |
| Node | `part_size` | 16 MB | — | node | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Node | `max_uploads` per sink | 64 | — | node | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Node | `part_buffer_pool` | 12 GiB | — | node | [§26.4](08-sizing.md#264-ram) |
| Node | `read_chunk` | 16 MB | — | node | [§17.2](05-read-path.md#172-read-chunks) |
| Node | scheduler weights WRITE : READ : MAINT | 1 : 1 : 0.1 | — | node | [§24.1](07-storage-node.md#241-starvation-free-scheduling) |
| Node | `event_replay_window` | 10 min | — | node | [§12.4](04-write-path.md#124-commit-semantics) |
| GC | watermarks critical / low / target | 3% / 5% / 8% | — | cluster | [§21.1](06-retention-gc.md#211-watermarks) |
| GC | `gc_proposal_factor` | 3× the bytes needed | — | node | [§21.2](06-retention-gc.md#212-protocol) |
| GC | `capacity_share` | equal shares | — | cluster, per tenant | [§21.4](06-retention-gc.md#214-tenant-fair-share) |
| Health | failure score events | I/O error +10, failed WRITE +5, timeout +2, Writer report +1 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | score half-life | 24 h | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | suspect / quarantine / exit thresholds | 10 / 30 / 5 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | cool-down / probation | 24 h / 7 days at weight × 0.5 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Time | clock tolerance for `date_started` | 5 min | — | cluster | [§10](02-data-model.md#10-time-semantics) |
| Security | `read_token_ttl` | 1 h | — | cluster | [§33.2](10-security.md#332-access-tokens) |
| Security | CA certificate lifetime | 10 years | — | cluster | [§33.5](10-security.md#335-tls) |
| Security | node certificate lifetime | 90 days, auto-renewed | — | cluster | [§33.4](10-security.md#334-enrollment) |

Not configurable on purpose: the token signature algorithm (Ed25519), the
entity domain bytes ([§35.3](12-api.md#353-entities)), and the rule that
duplicates are left to GC ([§14](04-write-path.md#14-duplicates-and-orphans)).

### 36.2 Open decisions

None at the moment. Every question raised during the design has either been
decided, and is described in the section it belongs to, or deliberately left
out:

- **Zone-aware placement** is not planned. The scheduler interface leaves room
  for it ([§11.2](03-placement.md#112-scheduler-interface)).
- **Serving live video** is not Shale's job
  ([§1](01-overview.md#1-what-shale-is-for)).

### 36.3 Rejected alternatives

**Volume files.** Packing many objects into large pre-allocated volume files
(Haystack style) would remove per-object filesystem metadata entirely. It pays
off for small objects, and the 32 MB lower bound
([§25](07-storage-node.md#25-object-size)) means Shale has none. It would also
complicate rescheduling (deletion per volume), resumable uploads, incomplete
objects, and the unlink/fd semantics of
[§18](05-read-path.md#18-delete-during-read). Shale stores one file per object.

**S3-style multipart.** Independent parts, a part list, and a completion call.
The offset-based resumable upload of
[§12.2](04-write-path.md#122-resumable-part-uploads) gives the same RAM savings
and restart safety, and its only state is a file size.

**A hold flag.** A separate "must not delete" flag raised the question of
which wins, the hold or the deletion deadline. Rescheduling the dates
([§20.3](06-retention-gc.md#203-rescheduling)) removes the question.
