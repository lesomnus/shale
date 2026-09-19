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
| Segments | target object size | 64 MB | 32–128 MB (`max_object_size`) | negotiated, per source | [§25](07-storage-node.md#25-object-size) |
| Uploads | upload mode | `live` | `live`, `buffered` | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `idle_timeout` | 30 s | 10 s – 5 min | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `abandon_timeout` | 5 min | 1–30 min | negotiated, per set | [§15](04-write-path.md#15-partial-objects) |
| Uploads | `allocation_horizon` | 10 min | 1–30 min | negotiated, per set | [§12.1](04-write-path.md#121-flow) |
| Uploads | `allocation_ttl` | horizon + 10 min | derived | — | [§12.1](04-write-path.md#121-flow) |
| Uploads | `retain` | `until_commit` | `until_commit`, `until_written` | writer | [§12.2](04-write-path.md#122-resumable-part-uploads) |
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
| Time | clock tolerance for `start_time` | 5 min | — | cluster | [§10](02-data-model.md#10-time-semantics) |
| Security | `read_token_ttl` | 1 h | — | cluster | [§33.2](10-security.md#332-access-tokens) |
| Security | CA certificate lifetime | 10 years | — | cluster | [§33.5](10-security.md#335-tls) |
| Security | node certificate lifetime | 90 days, auto-renewed | — | cluster | [§33.4](10-security.md#334-enrollment) |

Not configurable on purpose: the token signature algorithm (Ed25519), the
entity domain bytes ([§35.3](12-api.md#353-entities)), and the rule that
duplicates are left to GC ([§14](04-write-path.md#14-duplicates-and-orphans)).

### 36.2 Open decisions

What is still undecided or deliberately left out of v1.

| Topic | Question | Status |
|---|---|---|
| Hold vs. `must_delete_by` | which wins when both apply ([§20](06-retention-gc.md#20-retention)) | needs policy / legal input |
| Quarantine | score decay, thresholds, cool-down ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) | to be set from real failure data |
| Zone spread | use `zone` labels in placement ([§7](02-data-model.md#7-source-set-zone-epoch)) | not in v1; soft anti-affinity later if needed |
| Second axis | payday field 3, e.g. `Site`, to narrow readers within a tenant ([§35.3](12-api.md#353-entities)) | not in v1 |
| Tenant capacity | quotas or retention caps per tenant on shared sinks | not in v1; without them one tenant can shorten another's retention |
| Sink layout | file-per-object vs. append-only volume files | file-per-object (see note) |

**Note on volume files.** Packing many objects into large pre-allocated volume
files (Haystack style) would remove per-object filesystem metadata entirely.
Deletion would then happen per volume, which fits epoch-grouped, similarly
expiring data. Holds would pin whole volumes, and the unlink/fd semantics of
[§18](05-read-path.md#18-delete-during-read) would change. It is not needed at 64–128 MB objects; revisit if objects
shrink.

**Note on multipart.** S3-style multipart (independent parts, a part list,
and a completion call) was considered and dropped. The offset-based resumable
upload of [§12.2](04-write-path.md#122-resumable-part-uploads) gives the same RAM savings and restart safety, and its only
state is a file size.
