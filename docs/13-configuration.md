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
- **producer**: the producer's own configuration; nobody else sees it.

| Area | Parameter | Default | Bounds | Scope | Described in |
|---|---|---|---|---|---|
| Placement | `epoch` | 1 h | — | set | [§11](03-placement.md#11-placement) |
| Placement | `set_spread` | `spread` | `spread`, `pack`, `none` | set | [§11](03-placement.md#11-placement) |
| Placement | `forecast_margin` | 1.5 | — | cluster | [§11.1](03-placement.md#111-capacity-forecast) |
| Placement | scheduler | weighted HRW | — | cluster (`PlacementPolicy`) | [§11.2](03-placement.md#112-scheduler-interface) |
| Placement | `max_sink_capacity` | 64 TB | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Placement | `new_device_ramp` | off (weight × 0.25 for 30 days when on) | — | cluster | [D6](placement-decisions.md#d6-weight--raw-capacity-not-free-space) |
| Addresses | resolver | `advertised` | `advertised`, `template`, `dns` | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `trusted_proxies` | none | — | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Retention | `retention.expire` | 30 days | — | set | [§20.2](06-retention-gc.md#202-initial-dates) |
| Retention | `retention.delete` | = `retention.expire` | ≥ `retention.expire`, or `none` | set | [§20.2](06-retention-gc.md#202-initial-dates) |
| Retention | `sweep_interval` | 1 day | — | node | [§20.1](06-retention-gc.md#201-date_deleted-is-an-expiry-not-an-event) |
| Retention | `row_retention` | 30 days | — | cluster | [§20.4](06-retention-gc.md#204-row-retention) |
| Retention | `abandon_grace` | 1 h | — | cluster | [§20.4](06-retention-gc.md#204-row-retention) |
| Segments | `max_bitrate` | declared by the producer | ≤ the policy's `max_bitrate` | negotiated, per source | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Segments | `max_bitrate` cap | 32 Mbps | — | cluster (`UploadPolicy`) | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Segments | `max_bitrate_total` | none | — | set | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Segments | segment duration | 64 MB ÷ `max_bitrate` | `max_bitrate` × duration ≤ 512 MB, ≥ 32 MB as a target; duration ≤ `epoch` / 4 | negotiated, per source | [§12.6](04-write-path.md#126-upload-profile-negotiation), [§25](07-storage-node.md#25-lamina-size) |
| Segments | `keyframe_interval` | 2 s | 0.5–4 s | negotiated, per source | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Segments | `max_length` | `max_bitrate` × (duration + `keyframe_interval` + 2 s) | derived | — | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Segments | observed rate half-life | one epoch | — | cluster | [§12.6](04-write-path.md#126-upload-profile-negotiation) |
| Uploads | upload mode | `live` | `live`, `buffered` | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `idle_timeout` | 30 s | 10 s – 5 min | negotiated, per set | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `abandon_timeout` | 5 min | 1–30 min, and ≥ 2 × `idle_timeout` | negotiated, per set | [§15](04-write-path.md#15-partial-laminae) |
| Uploads | `allocation_horizon` | 10 min | 1–30 min | negotiated, per set | [§12.1](04-write-path.md#121-flow) |
| Uploads | `allocation_ttl` | horizon + longest segment + `abandon_timeout` + 5 min (22 min at the defaults) | derived | — | [§12.1](04-write-path.md#121-flow) |
| Uploads | `max_open_attempts` per source | 32 | — | cluster | [§12.1](04-write-path.md#121-flow) |
| Uploads | `clock_tolerance` (future `date_started`) | 5 min beyond the horizon | — | cluster | [§10](02-data-model.md#10-time-semantics) |
| Uploads | `max_backlog_age` (past `date_started`) | = `retention.expire` | — | set | [§10](02-data-model.md#10-time-semantics) |
| Segments | `max_bitrate: auto` starting table | 1080p30 → 4 Mbps and the rest of the table in [§38.5](15-producer.md#385-choosing-the-ceiling) | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | pass-through ceiling guess | peak 1-s rate of the first 60 s × 1.5 | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | early cut | at the next keyframe once a segment holds `max_bitrate` × duration ahead of its phase | — | producer | [§38.2](15-producer.md#382-cutting-segments) |
| Segments | ceiling raise after an early cut | +25% | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | starvation | a second is at the cap when its trailing keyframe interval holds ≥ 95% of what the ceiling allows; an episode is 3–60 s at the cap; starved = ≥ 50 episodes in a day with < 10% of its seconds at the cap | — | cluster | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | `auto_raise` | off (suggest only) | on: +25%, at most once a day; never lowers | set | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Uploads | `retain` | `committed` | `committed`, `written` | producer | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `resume_timeout` | 2 min | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | `retry_after_cap` | 2 min | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | placement retries | 3 | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | checksum | off (CRC32C when on) | — | set | [§30](09-operations.md#30-integrity) |
| Node | `part_size` | 16 MB | — | node | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Node | `max_uploads` per sink | 64 | — | node | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Node | `uploads_per_actor` | 64 | — | node | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Node | `part_buffer_pool` | 12 GiB | — | node | [§26.4](08-sizing.md#264-ram) |
| Node | `read_chunk` | 16 MB | — | node | [§17.2](05-read-path.md#172-read-chunks) |
| Node | `max_read_sessions` | 64 | — | node | [§26.4](08-sizing.md#264-ram) |
| Node | `sessions_per_actor` | 16 | — | node | [§17.4](05-read-path.md#174-read-locality-and-parallelism) |
| Node | `read_backlog` per device | 64 chunks | — | node | [§24.1](07-storage-node.md#241-starvation-free-scheduling) |
| Node | scheduler weights WRITE : READ : MAINT | 1 : 1 : 0.1; `maint_quantum` 1 MB | — | node | [§24.1](07-storage-node.md#241-starvation-free-scheduling) |
| Node | `event_replay_window` | 10 min | — | node | [§12.4](04-write-path.md#124-commit-semantics) |
| Node | `heartbeat_interval` | 5 s | — | node | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Producer | `producer_heartbeat_interval` | 30 s | — | producer | [§38.6](15-producer.md#386-health-and-heartbeats) |
| Producer | `producer_down_after` | 90 s (three heartbeats) | — | cluster | [§38.6](15-producer.md#386-health-and-heartbeats) |
| Producer | `uplink` | none | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Producer | `push` | off | `unix:/path`, `tcp://host:port`: the listener pushed sources are written to | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | `push_idle` | 30 s | — | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | a source's `kind` | `ts` | `ts`, `raw`; pushed sources only | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | encoder target | (ceiling − audio) ÷ 1.05; capped VBR with `bufsize` = 2 s at the ceiling | — | producer | [§38.3](15-producer.md#383-managed-capture) |
| Producer | start-up check | 10 s after a capture process starts | — | producer | [§38.3](15-producer.md#383-managed-capture) |
| Node | `gc_page` | 5,000 candidates | — | node | [§21.2](06-retention-gc.md#212-protocol) |
| Node | `token_skew` | 1 min | — | node | [§33.2](10-security.md#332-access-tokens) |
| GC | watermarks critical / low / target | 3% / 5% / 8% | — | cluster | [§21.1](06-retention-gc.md#211-watermarks) |
| GC | `gc_interval` | 1 min | — | node | [§21.2](06-retention-gc.md#212-protocol) |
| GC | `gc_proposal_factor` | 3× the bytes needed | — | node | [§21.2](06-retention-gc.md#212-protocol) |
| GC | `capacity_share` | equal shares | — | cluster, per tenant | [§21.4](06-retention-gc.md#214-tenant-fair-share) |
| Health | failure score events | I/O error +10, failed WRITE +5, timeout +2, producer report +1 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | producer report cap | +5 per producer per node per day | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | score half-life | 24 h | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | suspect / quarantine / exit thresholds | 10 / 30 / 5 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | cool-down / probation | 24 h / 7 days at weight × 0.5 | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | `node_down_after` | 30 s | — | cluster | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | `sink_auto_adopt_after` | 10 min | — | cluster | [§28.3](09-operations.md#283-node-failure-and-device-re-homing) |
| Health | `reconcile_interval` | 24 h | — | cluster | [§34.9](11-deployment.md#349-events-and-directives) |
| Health | `control.directives_every` | 5 s | — | control | [§34.9](11-deployment.md#349-events-and-directives) |
| Limits | `server.limit` / `cluster.limit` | none | rate, burst per tenant | control | [§35.1](12-api.md#351-conventions) |
| Limits | `control.actor_limit` | none | rate, burst per actor | control | [§35.1](12-api.md#351-conventions) |
| Hosts | `join_pending_ttl` | 24 h | — | cluster | [§33.4](10-security.md#334-joining-and-adoption) |
| Hosts | `readopt` | `manual` | `manual`, `auto` | cluster | [§33.4](10-security.md#334-joining-and-adoption) |
| Hosts | host certificate lifetime | 90 days, renewed at 60 | — | cluster | [§33.5](10-security.md#335-tls) |
| Hosts | CP certificate lifetime | 1 year, renewed by the CP | — | cluster | [§33.5](10-security.md#335-tls) |
| Hosts | CA lifetime | 10 years, rollover starts 1 year before | — | cluster | [§33.5](10-security.md#335-tls) |
| Security | `read_token_ttl` | 1 h | — | cluster | [§33.2](10-security.md#332-access-tokens) |
| People | `auth.roster.addr` | none: roster in this process | an address | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.roster.keys` | none | tenant alias → `env:NAME`, `file:PATH` | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.roster.ca_file`, `auth.roster.insecure` | the system pool, TLS | — | control | [§33.1](10-security.md#331-trust-model) |
| Trail | `audit.profile` | none: forever | `pipa`, `pipa-sensitive`, `pci`, `hipaa`, `sox`, `gdpr`, `forever` | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.retain`, `audit.destroy` | the profile's | how long a row stays in the table, and in the archive | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.archive`, `audit.discard` | none | a directory; a window with neither is refused | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.every` | 24h | — | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.by.<kind>` | — | `profile`, `retain`, `destroy`, `discard` for one kind: `lamina`, `set`, `holder`, …, `d19` for `site` | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| People | `auth.roster.db` | SQLite beside the control plane's state | a database every control plane shares | control | [§34.7](11-deployment.md#347-single-machine), [§34.5](11-deployment.md#345-kubernetes) |
| Live | `view_token_ttl` | 1 h | — | cluster | [§39.4](16-relay.md#394-viewers) |
| Live | `publish_token_ttl` | 24 h | — | cluster | [§39.3](16-relay.md#393-from-the-producer) |
| Live | `relay_idle_stop` | 10 s after the last viewer leaves | — | relay | [§39.3](16-relay.md#393-from-the-producer) |
| Live | `max_viewers` per relay | 500 | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `viewers_per_actor` | 16 | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `ice` (STUN / TURN servers) | none (host candidates only) | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `relay_selector` | none (any relay) | labels | site | [§39.2](16-relay.md#392-assignment) |
| Security | `rpc_rate` | 20 calls/s per actor, burst 100; 2,000/s per tenant | — | cluster | [§35.1](12-api.md#351-conventions) |
| Security | `timeline_page` | 1,000 laminae | — | cluster | [§17.1](05-read-path.md#171-flow) |

Not configurable on purpose: the token signature algorithm (Ed25519), the
entity domain bytes ([§35.3](12-api.md#353-entities)), the order in which a
host's hardware identity is read ([§33.4](10-security.md#334-joining-and-adoption)),
and the rule that duplicates are left to GC ([§14](04-write-path.md#14-duplicates-and-orphans)).

**Metrics.** Every process measures the instruments of
[§31](09-operations.md#31-observability) it owns (`shale.node.*`,
`shale.cp.*`, `shale.producer.*`, `shale.relay.*`) through OpenTelemetry,
and exports them wherever `otel:` says; nothing is exported until it does.
An OTLP collector:

```yaml
otel:
  exporters:
    otlp:
      endpoint: collector.example.com:4317
  providers:
    meter:
      processors: [resource/shale]
      exporters: [otlp]
```

### 36.2 Open decisions

None at the moment. Every question raised during the design has either been
decided, and is described in the section it belongs to, or deliberately left
out:

- **Zone-aware placement** is not planned. The scheduler interface leaves room
  for it ([§11.2](03-placement.md#112-scheduler-interface)).
- **Live video through storage.** Live viewing is the Relay's job and never
  touches a Storage Node ([§39](16-relay.md#39-relay)). Not in the relay's
  first version: relay-to-relay cascades, sub-streams, multi-track sessions,
  and playing recordings through the relay ([§39.5](16-relay.md#395-capacity)).
- **Encoding inside Shale's binary.** The producer supervises capture
  processes and never touches a frame ([§38.3](15-producer.md#383-managed-capture)).
  The producer's live helper, an ffmpeg it runs while a camera is watched,
  transcodes audio to Opus and nothing else ([§38.7](15-producer.md#387-live-output)).

Ideas recorded, not planned:

- **Disk discovery.** A node finding the unused disks of labeled machines
  and formatting them into sinks after an operator's claim, as Rook does.
  Sinks are listed explicitly until someone needs this
  ([§22.2](07-storage-node.md#222-sinks-and-devices)).

### 36.3 Rejected alternatives

**Volume files.** Packing many laminae into large pre-allocated volume files
(Haystack style) would remove per-lamina filesystem metadata entirely. It pays
off for small laminae, and the 32 MB lower bound
([§25](07-storage-node.md#25-lamina-size)) means Shale has none. It would also
complicate rescheduling (deletion per volume), resumable uploads, incomplete
laminae, and the unlink/fd semantics of
[§18](05-read-path.md#18-delete-during-read). Shale stores one file per lamina.

**S3-style multipart.** Independent parts, a part list, and a completion call.
The offset-based resumable upload of
[§12.2](04-write-path.md#122-resumable-part-uploads) gives the same RAM savings
and restart safety, and its only state is a file size.

**A hold flag.** A separate "must not delete" flag raised the question of
which wins, the hold or the deletion deadline. Rescheduling the dates
([§20.3](06-retention-gc.md#203-rescheduling)) removes the question.

**Join tokens and enrollment tokens.** A one-time token per host is one more
secret for an operator to mint, copy to the machine, and keep off a shared
screen. Adoption ([§33.4](10-security.md#334-joining-and-adoption)) needs
nothing on the host but the Control Plane's address, and the operator's
decision moves to a console where the host's identity is in view.

**Bearer credentials for hosts.** Long-lived tokens are easy to copy and
have no lifecycle. Host certificates are issued, renewed, and refused by the
Control Plane without an operator touching a key.

**A pull-only control channel.** Carrying CP directives in heartbeat
responses would keep the Control Plane from ever dialing a node, but every
CP-initiated action would then wait for the next heartbeat and need its own
acknowledgement bookkeeping. Nodes are directly reachable by design, so the
Control Plane calls them ([§35.7](12-api.md#357-storage-node-control-api)).

**WebRTC on the producer.** Publishing from the producer over WebRTC (WHIP)
or serving viewers from it would put ICE, DTLS, and RTP on a small machine
and let viewers load its uplink. The producer sends plain MPEG-TS over one
stream to its relay, only while someone watches, and WebRTC exists only
between the relay and the viewer ([§39.3](16-relay.md#393-from-the-producer)).

**An external live server.** A ready-made media server could serve WebRTC,
but it cannot tell the producer to start and stop sending, and it would
need its own gateway to Shale's tokens and sites. The relay reuses both
and adds one thing, on-demand ingest ([§39](16-relay.md#39-relay)).
