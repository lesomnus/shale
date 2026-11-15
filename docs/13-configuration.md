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
- **reader**: the reader agent's own configuration, likewise.

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
| Addresses | `template` | none | `template`, `dns`: a host name with `{alias}` or `{id}`; for `dns`, ending in `dns_zone` | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `dns_server`, `dns_zone` | none | `dns`: `host` or `host:port` (53), reached over TCP; the zone the names are in | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `dns_tsig_name`, `dns_tsig_algorithm`, `dns_tsig_secret` | none; `hmac-sha256` | `dns`: the TSIG key; `hmac-sha224`, `-sha384`, `-sha512`, `-sha1`; the secret is the policy's own field, base64, sealed, never answered with | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `dns_ttl` | 60 s | — | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `dns_networks` | none: a host's first IPv4 and IPv6 address, neither loopback nor link-local | CIDRs: the addresses inside them; the data plane's IP when it listens on one, either way | cluster (`AddressPolicy`) | [§34.10](11-deployment.md#3410-node-addresses) |
| Addresses | `control.dns_sync_every` | 10 min | how often the leader reads the `dns` zone whole and repairs it; changes are followed every `control.directives_every` | control | [§34.10](11-deployment.md#3410-node-addresses) |
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
| Uploads | `allocation_ttl` | horizon + longest segment + `abandon_timeout` + 5 min (22 min at the defaults) | derived; `control.allocation_ttl` replaces it | control | [§12.1](04-write-path.md#121-flow) |
| Uploads | `max_open_attempts` per source | 32 | — | cluster | [§12.1](04-write-path.md#121-flow) |
| Uploads | `clock_tolerance` (future `date_started`) | 5 min beyond the horizon | — | cluster | [§10](02-data-model.md#10-time-semantics) |
| Uploads | `max_backlog_age` (past `date_started`) | = `retention.expire` | — | set | [§10](02-data-model.md#10-time-semantics) |
| Segments | `max_bitrate: auto` starting table | 1080p30 → 4 Mbps and the rest of the table in [§38.5](15-producer.md#385-choosing-the-ceiling) | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | pass-through ceiling guess | peak 1-s rate of the first 60 s × 1.5 | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | early cut | at the next keyframe once a segment holds `max_bitrate` × duration ahead of its phase | — | producer | [§38.2](15-producer.md#382-cutting-segments) |
| Segments | ceiling raise after an early cut | +25% | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | starvation | a second is at the cap when its trailing keyframe interval holds ≥ 95% of what the ceiling allows; an episode is 3–60 s at the cap; starved = ≥ 50 episodes in a day with < 10% of its seconds at the cap | — | cluster | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Segments | `auto_raise` | off (suggest only) | on: +25% to a starved source the producer encodes with a variable rate (not CBR, not a copied stream, not a camera a `CameraBinding` manages), when its last raise was at least 24 h ago, and only inside the upload policy's cap, the set's `max_bitrate_total` and the producer's `uplink` (else held there, with a warning); agreed at every negotiation while on; never lowers | set | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Uploads | `retain` | `committed` | `committed`, `written` | producer | [§12.2](04-write-path.md#122-resumable-part-uploads) |
| Uploads | `resume_timeout` | 2 min | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | `retry_after_cap` | 2 min | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | placement retries | 3 | — | producer | [§13](04-write-path.md#13-retry-and-reallocation) |
| Uploads | `http3` | `off`: TCP, as before HTTP/3 | `auto`: HTTP/3 to a node whose answers offer it; `on`: to every node from the first request. Either falls back to TCP when the handshake does not complete in 5 s, and keeps that node on TCP for 5 min, doubling to 1 h | producer | [§35.6](12-api.md#356-storage-node-http-data-plane) |
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
| Node | `storage.cert_file`, `storage.key_file` | none (the host certificate) | both or neither; the data plane only, read again when the files change | node | [§33.5](10-security.md#335-tls), [§40.5](17-console.md#405-playback) |
| Node | `storage.advertise` | the bound data address, through the resolver | host:port; a name is handed out as it is | node | [§34.10](11-deployment.md#3410-node-addresses) |
| Node | `storage.http3` | `on` | `on`, `off`: the data plane over HTTP/3 as well, on UDP at the data port, offered with `Alt-Svc`; a UDP port the node cannot have leaves it on TCP alone; none in development mode | node | [§35.6](12-api.md#356-storage-node-http-data-plane) |
| Producer | `producer_heartbeat_interval` | 30 s | — | producer | [§38.6](15-producer.md#386-health-and-heartbeats) |
| Producer | `producer_down_after` | 90 s (three heartbeats) | — | cluster | [§38.6](15-producer.md#386-health-and-heartbeats) |
| Producer | `uplink` | none | — | producer | [§38.5](15-producer.md#385-choosing-the-ceiling) |
| Producer | `push` | off | `unix:/path`, `tcp://host:port`: the listener pushed sources are written to | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | `push_idle` | 30 s | — | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | `audio` | absent: each camera's own audio | `{codec: none}` only | producer | [§38.3](15-producer.md#383-managed-capture) |
| Producer | `demo` | 0 | how many sources of a picture and a tone ffmpeg draws for itself, beside `sources`; `--demo` on `serve producer` | producer | [§38.1](15-producer.md#381-inputs) |
| Producer | a source's `kind` | `ts` | `ts`, `raw`; pushed sources only | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | a source's `controls` | none | V4L2 control names and values; set before every capture start | producer | [§38.3](15-producer.md#383-managed-capture) |
| Producer | a source's `idle.dark_after` | none (absent: every segment stored); 10 min when `idle:` is given | how long the scene stays dark before its segments are skipped; sources the producer encodes only | producer | [§38.10](15-producer.md#3810-dark-scenes) |
| Producer | a source's `idle.threshold` | 0.10 | luma, 0..1, at or below which a pixel is dark; a frame is dark at 98 % dark pixels | producer | [§38.10](15-producer.md#3810-dark-scenes) |
| Producer | a source's `content_type` | `video/mp4`; `application/octet-stream` for `raw` | proposed at negotiation; a person's value on the source wins | producer | [§38.9](15-producer.md#389-pushed-sources-and-raw-frames) |
| Producer | encoder target | (ceiling − audio per track) ÷ 1.05; capped VBR with `bufsize` = 2 s at the ceiling | — | producer | [§38.3](15-producer.md#383-managed-capture) |
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
| Health | `control.node_down_after` | 30 s | how long a Storage Node may go unheard before it is down; relays have `control.relay_down_after` | control | [§27](09-operations.md#27-node--device--sink-health-and-quarantine) |
| Health | `control.sink_auto_adopt_after` | 10 min | how long a sink whose node is down waits before the node reporting it takes it over | control | [§28.3](09-operations.md#283-node-failure-and-device-re-homing) |
| Health | `reconcile_interval` | 24 h | — | cluster | [§34.9](11-deployment.md#349-events-and-directives) |
| Health | `control.directives_every` | 5 s | — | control | [§34.9](11-deployment.md#349-events-and-directives) |
| Listeners | `server.http.addr` / `cluster.http.addr` | the API's port plus two (7402, 7403) | — | control | [§40.4](17-console.md#404-serving-it) |
| Listeners | `server.http.allow_web` / `cluster.http.allow_web` | off; on in development mode | on: the listener answers Connect and gRPC-Web, for the console | control | [§40.1](17-console.md#401-two-surfaces-two-sign-ins) |
| Listeners | `server.http.origins` / `cluster.http.origins` | none: the listener's own origin; loopback at the console's port and at vite's in development mode | the origins a page may call from; the console's, on the cluster listener | control | [§40.4](17-console.md#404-serving-it) |
| Limits | `server.limit` / `cluster.limit` | none | rate, burst per tenant | control | [§35.1](12-api.md#351-conventions) |
| Limits | `control.actor_limit` | none | rate, burst per actor | control | [§35.1](12-api.md#351-conventions) |
| Exports | `export.enabled` | false | opt-in MP4 attachment route on the tenant HTTP listener; media passes through the exporting CP | control | [MP4 downloads](mp4-export.md) |
| Exports | `export.public_url` | `/exports/recording.mp4` | external endpoint URL, or path relative to the tenant HTTP origin | control | [MP4 downloads](mp4-export.md) |
| Exports | `export.max_concurrent` / `export.per_actor` | 2 / 1 | active jobs per CP process, including planning; no wait queue | control | [MP4 downloads](mp4-export.md) |
| Exports | `export.max_duration` | 30 min | planning and streaming execution deadline; ticket admission TTL is 5 min or less | control | [MP4 downloads](mp4-export.md) |
| Hosts | `join_pending_ttl` | 24 h | — | cluster | [§33.4](10-security.md#334-joining-and-adoption) |
| Hosts | `readopt` | `manual` | `manual`, `auto` | cluster | [§33.4](10-security.md#334-joining-and-adoption) |
| Hosts | host certificate lifetime | 90 days, renewed at 60 | — | cluster | [§33.5](10-security.md#335-tls) |
| Hosts | CP certificate lifetime | 1 year, renewed by the CP at two thirds, in memory, and when the CA that issues changes | — | cluster | [§33.5](10-security.md#335-tls) |
| Hosts | CA lifetime | 10 years | — | cluster | [§33.5](10-security.md#335-tls) |
| Hosts | `control.ca_rollover_before` | 1 year | how long before the active CA expires the leader begins a rollover; `shale ca rotate` begins one at any time | control | [§33.5](10-security.md#335-tls) |
| Hosts | `control.ca_settle` | 1 min | the least time each step of a CA rollover stands before the next; every CP process reads the CA set again at half of it | control | [§33.5](10-security.md#335-tls) |
| Hosts | `reader.heartbeat_interval` | 30 s | — | reader | [§36.4](#364-running-a-reader) |
| Hosts | `reader.cp_ca` | none: the CA pinned on first contact | `system` (the system's roots) or a PEM file: a CA trusted for the CP's certificate beside the pinned bundle, in place of the pin | reader | [§36.4](#364-running-a-reader), [§33.5](10-security.md#335-tls) |
| Hosts | `reader.addr` | none | refused: the localhost listener is not built | reader | [§33.4](10-security.md#334-joining-and-adoption) |
| Security | `read_token_ttl` | 1 h | — | cluster | [§33.2](10-security.md#332-access-tokens) |
| People | `auth.roster.addr` | none: roster in this process | an address | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.roster.key` | none | `env:NAME`, `file:PATH`, or the key: an `rk_` serves the tenants that nominated it, an `rt_` its own | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.roster.keys` | none | tenant alias → `env:NAME`, `file:PATH`: a tenant key (`rt_`) each, not with `key` | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.roster.ca_file`, `auth.roster.insecure` | the system pool, TLS | — | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.operators.tenant` | none: operators are the people of `control.cluster_tenant`, and every person administers their tenant | a tenant this deployment serves; set, what a person may change is what roster grants them, and the cluster API serves that tenant's people | control | [§33.1](10-security.md#331-trust-model), [§35.2](12-api.md#352-two-api-surfaces) |
| People | `auth.operators.team`, `auth.operators.site` | — | refused at start: operators are a grant at roster, not a team | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.oidc.issuer` | none: no single sign-on | the OIDC issuer, e.g. `https://sso.hday.dev` | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.oidc.client_id`, `auth.oidc.client_secret` | none | the browser's confidential client at the issuer; the secret as `env:NAME`, `file:PATH` or itself, sent as `client_secret_basic` | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.oidc.device_client_id` | none: no single sign-on from the CLI | the CLI's public client, with the device grant | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.oidc.tenant_origin`, `auth.oidc.cluster_origin` | the request's own origin (development) | where a browser reaches each HTTP listener: the callback is `<origin>/sso/callback`, a logout comes back to `<origin>/` | control | [§33.1](10-security.md#331-trust-model), [§40.1](17-console.md#401-two-surfaces-two-sign-ins) |
| People | `auth.oidc.scopes`, `auth.oidc.ca_file` | `openid profile email`; the system pool | — | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.sso_only` | off | on: a password is refused and the console draws only the issuer's button; requires `auth.oidc.issuer` | control | [§33.1](10-security.md#331-trust-model) |
| People | `client.web`, `client.cluster_web` | the API's host two ports up | the CLI's sign-in endpoints, when an Ingress is in front: e.g. `https://shale.hday.dev`; verified against the system's roots and the CP's CA | CLI | [§33.1](10-security.md#331-trust-model) |
| People | `auth.operators.ttl` | 30 s | how long roster's answer about a person is kept -- what it grants them, and whether a session of theirs is still good, which is asked whether or not `auth.operators` is set; a failure is kept 5 s and is no | control | [§33.1](10-security.md#331-trust-model) |
| People | `auth.operators.reads` | `everybody` | `granted`: what the console's pages read is as far as roster grants it, like any other call; a grant of `/shale.DelegationService/*` lets somebody view through an app and read nothing here themselves | control | [§33.1](10-security.md#331-trust-model), [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| People | `auth.delegation.clients` | none: no app views on anybody's behalf | an app's OAuth client ID → its name at roster (its holder's alias in each tenant); requires `auth.oidc.issuer` and `auth.operators` | control | [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| People | `auth.delegation.audience` | none | what the issuer calls Shale's API, e.g. `urn:hday:api:shale`: a person's access token is believed only for it | control | [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| People | `auth.delegation.max_ttl` | 12 h | the longest one delegation lives, however it is renewed | control | [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| People | `auth.delegation.persons` | `reads`: a person is viewed for as far as `auth.operators.reads` lets them read (with `granted`, their grant of the read or of `/shale.DelegationService/*`) | `tenant`: any person of the tenant in good standing, granted or not; the app decides who sees what, within what roster grants the app, the handle, and the person's sites. The console is unchanged | control | [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| People | `auth.delegation.token_ttl` | 5 min | the longest a view or read token issued on a person's behalf lives: how long a live view or a signed URL outlives the delegation | control | [§33.8](10-security.md#338-viewing-on-a-persons-behalf) |
| Trail | `audit.profile` | none: forever | `pipa`, `pipa-sensitive`, `pci`, `hipaa`, `sox`, `gdpr`, `forever` | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.retain`, `audit.destroy` | the profile's | how long a row stays in the table, and in the archive | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.archive`, `audit.discard` | none | a directory; a window with neither is refused | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.checkpoints.key` | none; required with `audit.archive` | what signs every checkpoint and the archive's account: a file, or the PEM document itself (`SHALE_AUDIT_CHECKPOINTS_KEY`); `openssl genpkey -algorithm ed25519` makes one | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.checkpoints.dir`, `audit.checkpoints.witnesses` | none: kept in the archive, and heard nowhere else | a directory of their own; where each checkpoint is heard outside the database and the archive, `log:` or `store:<dir>` | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.every` | 24h | — | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.content` | `patch`: what the write set | `fields`: which fields, and nothing they were set to; `value`: the row as the write left it. A field declared personal or secret is on the trail as a value at no level | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| Trail | `audit.by.<kind>` | — | `profile`, `retain`, `destroy`, `discard`, `content` for one kind: `lamina`, `set`, `holder`, …, `d19` for `site` | control | [§26.5](08-sizing.md#265-the-control-planes-database) |
| People | `auth.roster.db` | SQLite beside the control plane's state | a database every control plane shares | control | [§34.7](11-deployment.md#347-single-machine), [§34.5](11-deployment.md#345-kubernetes) |
| Live | `view_token_ttl` | 1 h | — | cluster | [§39.4](16-relay.md#394-viewers) |
| Live | `publish_token_ttl` | 24 h | — | cluster | [§39.3](16-relay.md#393-from-the-producer) |
| Live | `relay_idle_stop` | 10 s after the last viewer leaves | — | relay | [§39.3](16-relay.md#393-from-the-producer) |
| Live | `max_viewers` per relay | 500 | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `viewers_per_actor` | 16 | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `relay.rewind_budget` | 1 GiB | what the recent windows of every source hold together; about one lamina per source | relay | [§39.5](16-relay.md#395-capacity) |
| Live | `ice` (STUN / TURN servers) | none (host candidates only) | — | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `relay.whep_cert_file`, `relay.whep_key_file` | none (the host certificate) | both or neither; read again when the files change | relay | [§39.4](16-relay.md#394-viewers), [§33.5](10-security.md#335-tls) |
| Live | `relay.whep_advertise` | `relay.advertise` and the bound port | a host, or host:port | relay | [§39.4](16-relay.md#394-viewers) |
| Live | `relay_selector` | none (any relay) | labels | site | [§39.2](16-relay.md#392-assignment) |
| Live | `relay.heartbeat_interval` | 2 s | a third of `control.relay_down_after` or less | relay | [§39.2](16-relay.md#392-assignment) |
| Live | `control.relay_down_after` | 6 s (three relay heartbeats) | how long a relay may go unheard before it is down: its producers are reassigned and `Live` stops naming it; at least three `relay.heartbeat_interval`s | control | [§39.2](16-relay.md#392-assignment), [§39.6](16-relay.md#396-failures) |
| Security | `rpc_rate` | 20 calls/s per actor, burst 100; 2,000/s per tenant | — | cluster | [§35.1](12-api.md#351-conventions) |
| Security | `timeline_page` | 1,000 laminae | — | cluster | [§17.1](05-read-path.md#171-flow) |

Not configurable on purpose: the token signature algorithm (Ed25519), the
entity domain bytes ([§35.3](12-api.md#353-entities)), the order in which a
host's hardware identity is read ([§33.4](10-security.md#334-joining-and-adoption)),
and the rule that duplicates are left to GC ([§14](04-write-path.md#14-duplicates-and-orphans)).

**The `dns` resolver** ([§34.10](11-deployment.md#3410-node-addresses))
needs a zone on an authoritative server that takes RFC 2136 updates over
TCP, and a TSIG key the server accepts for updates of A, AAAA and TXT
records in it, for signed queries, and for a transfer of the zone (AXFR,
which the whole read every `control.dns_sync_every` is). A zone of its own
for the nodes, here `nodes.example.com`, keeps everything else out of
Shale's reach. Make the key on the server's side:

```sh
tsig-keygen -a hmac-sha256 shale-update     # BIND: prints the key block below
keymgr -t shale-update hmac-sha256          # Knot: prints the key in its own form
```

With BIND, the key and what it may do, in `named.conf`:

```text
key "shale-update" {
    algorithm hmac-sha256;
    secret "<base64 secret>";
};

zone "nodes.example.com" {
    type primary;
    file "/var/lib/bind/nodes.example.com.zone";
    update-policy {
        grant shale-update zonesub A AAAA TXT;
    };
    allow-transfer { key shale-update; };
};
```

With Knot DNS, in `knot.conf`:

```yaml
key:
  - id: shale-update
    algorithm: hmac-sha256
    secret: <base64 secret>

acl:
  - id: shale-update
    key: shale-update
    action: [update, transfer]
    update-type: [A, AAAA, TXT]

zone:
  - domain: nodes.example.com
    acl: shale-update
```

Then the policy, with the secret in a file rather than on a command line
(`-` reads the request from standard input), and its activation, which
asks the server for the zone's SOA with the key and is refused when that
fails:

```sh
cat > dns-policy.json <<'EOF'
{
  "version": 2,
  "params": {
    "resolver": "dns",
    "template": "{alias}.nodes.example.com",
    "dns_server": "192.0.2.53",
    "dns_zone": "nodes.example.com",
    "dns_tsig_name": "shale-update",
    "dns_ttl": 60
  },
  "dns_tsig_secret": "<base64 secret>"
}
EOF
shale address-policy add @dns - < dns-policy.json && rm dns-policy.json
shale address-policy activate @dns
shale address-policy get @dns -o protojson   # dns_sync: what the leader last did
```

Every name Shale writes carries a TXT record
`heritage=shale,shale/owner=<cluster tenant ID>`, and Shale changes no name
without it. Nodes and relays renew their certificates for the template's
names in their next heartbeats. To rotate the key, add it on the server,
`shale address-policy patch @dns` with the new `dns_tsig_secret`, then
remove the old one.

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

A host with no collector to reach writes its metrics to a file instead, as
JSON lines, once every `OTEL_METRIC_EXPORT_INTERVAL` milliseconds (60 s by
default); on a tmpfs, nothing reaches the disk:

```yaml
otel:
  exporters:
    debug:
      output_paths: [/tmp/shale-metrics.jsonl]
  providers:
    meter:
      exporters: [debug]
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
  The Opus that live needs is a second audio track the producer's capture
  ffmpeg writes, or its live helper's output for a stream it did not
  encode ([§38.7](15-producer.md#387-live-output)); either transcodes audio
  and nothing else.

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
and let viewers load its uplink. The producer sends its fragments over one
stream to its relay, at all times or only while someone watches
([§39.2](16-relay.md#392-assignment)), and WebRTC exists only between the
relay and the viewer ([§39.3](16-relay.md#393-from-the-producer)).

**An external live server.** A ready-made media server could serve WebRTC,
but it cannot tell the producer to start and stop sending, and it would
need its own gateway to Shale's tokens and sites. The relay reuses both
and adds one thing, on-demand ingest ([§39](16-relay.md#39-relay)).

### 36.4 Running a Reader

A program that shows Shale's video to people of its own -- a viewer that
lists cameras, asks for live and recorded video, and sends its browsers
straight to the relay and the nodes -- reads as a **Reader**: a host of the
tenant, adopted once, whose certificate `shale serve reader` keeps current
beside it ([§33.4](10-security.md#334-joining-and-adoption)). It needs no
person's session, no signing key and no KEK, and it only reads.

```yaml
# /etc/shale/shale.yaml on the machine beside the viewer
cp: cp.example.com:7400   # the tenant API's gRPC listener (server.addr)
tenant: acme              # with more than one tenant
reader:
  # cp_ca: system         # the CP's certificate is from a public CA
```

1. **Start it.** `shale serve reader` makes a key under
   `/var/lib/shale/reader`, joins, and logs `waiting for adoption` with the
   reader's alias, its hardware identity, its key fingerprint, and the
   command that adopts it.
2. **Adopt it** once the fingerprint matches the one `shale reader pending`
   or the console shows: `shale reader adopt @acme/<alias>
   '{"sites":[{"id":"<site>"}]}'`, or with no sites for the whole tenant.
3. **Read with its files**: `host.key` (mode 0600), `host.crt`, and
   `ca.crt`, the shale CA's bundle, in the same directory. The program dials
   the tenant API's gRPC listener with the key and certificate as its client
   certificate and the bundle as its roots, and calls what a reader may:
   `Get`, `List` and `Live` of sets and sources, `Get`, `List`, `Timeline`
   and `Export` of laminae, `Get` and `List` of sites, and
   `ReaderService.Get`. The answers carry the view and read tokens its
   browsers take to the relay and the nodes
   ([§17.1](05-read-path.md#171-flow), [§39.4](16-relay.md#394-viewers)).

**The endpoint.** `cp` is the tenant API's native gRPC listener
(`server.addr`, 7400 by default), reached by TLS passthrough when there is a
load balancer in front ([§34.5](11-deployment.md#345-kubernetes)): the
certificate is the credential, and it is presented in the TLS handshake. The
HTTP listener beside it (`server.http.addr`, 7402) is the browsers' -- the
console, signing in, Connect and gRPC-Web -- and usually sits behind an
Ingress that ends TLS, which a client certificate does not get through. A
host neither joins nor calls there.

**Trusting the CP.** On first contact the agent pins the CA the CP
presents, or checks it against `ca_hash`, and trusts that bundle alone from
then on. A CP whose `control.cert_file` comes from a public or corporate CA
is named instead, explicitly: `reader.cp_ca: system` trusts the system's
roots, a path trusts the CAs in that PEM file. The CP is then verified by
its name like any server, nothing is pinned, and the bundle the join hands
over is trusted beside it ([§33.5](10-security.md#335-tls)). The program
reading with the files trusts the same.

**Renewal.** The agent heartbeats every `reader.heartbeat_interval` and
renews at two thirds of the certificate's life, 60 of its 90 days, or when
the CP asks. A renewal keeps the key and replaces the files whole, so they
are never half-written, and from that moment the CP refuses the old serial,
on connections already open as well. The program loads the pair for each
new connection, or when `host.crt` changes, and once a call is refused
`UNAUTHENTICATED` it loads the files again and retries.

**What it reads.** Its tenant, within the sites it was adopted for, or all
of it when it was adopted with none. A set in no site, with its cameras and
their recordings, is in none of a reader's sites: only a reader of the
whole tenant reads it. Every write is refused.

**When the certificate runs out.** A reader that was off for longer than
its certificate lives cannot renew it: the CP's TLS refuses an expired
certificate. The agent then makes a new key and joins again without the
expired one; it is pending under its old alias as "known host, new key",
and `shale reader adopt @acme/<alias>` takes it back with the sites it had
(naming sites replaces them). Until then the program reads nothing. A
reader that was erased is refused at once and joins again as a new reader,
for an operator to adopt; a machine that lost its state directory joins as
"known host, new key" too ([§34.3](11-deployment.md#343-state-on-disk)).

**Not built.** The localhost listener of §33.4 -- the tenant API without
TLS, for a media server that manages no keys -- is not: `reader.addr` is
refused, and a program reads with the files.
