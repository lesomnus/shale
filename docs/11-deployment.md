# Shale — Deployment

## 34. Deployment

### 34.1 One binary, one command per role

```text
shale init                        create the cluster: CA, signing key, first tenant,
                                  first operator and tenant admin
shale serve control               Control Plane, tenant API     (producers, readers, tenant admins)
shale serve cluster               Control Plane, cluster API    (Storage Nodes, cluster operators)
shale serve storage  --cp <url>   Storage Node
shale serve producer --cp <url>   Producer: records the cameras of one set and uploads them
shale serve reader   --cp <url>   Reader agent beside a media server (§33.4)
shale serve relay    --cp <url>   Relay: live streams to viewers over WebRTC (§39)
shale serve all [--dev <dir>]     control, cluster, one Storage Node, and one Relay in one process
```

- `control` and `cluster` are **separate entry points**, not one server with a
  flag. The cluster API spans tenants, and no configuration of `shale serve
  control` can mount it, so a configuration mistake cannot expose it
  ([§35.2](12-api.md#352-two-api-surfaces)). Both are stateless and share the
  metadata DB.
- `shale serve all` serves both APIs on **separate listeners**. The cluster
  listener binds to localhost by default.
- A host needs one setting, the Control Plane's address. Everything else about
  its identity is done by joining and adoption
  ([§33.4](10-security.md#334-joining-and-adoption)).
- The binary knows nothing about Kubernetes. Configuration comes from flags,
  then environment variables (`SHALE_*`), then a file
  (`/etc/shale/shale.yaml`), in that order of precedence.
- The same binary is also the CLI. payday generates `get`, `ls`, `watch`,
  `add`, `patch`, and `erase` for every entity that declares them
  ([§32](09-operations.md#32-cli--processes)).
- Kubernetes manifests, systemd units, and container images only wrap this
  binary. Every deployment runs the same code paths, including authentication
  ([§33](10-security.md#33-authentication-and-authorization)).

### 34.2 External dependencies

| Dependency | Needed | Notes |
|---|---|---|
| Metadata DB | yes | **PostgreSQL** whenever more than one CP process runs; **SQLite** for `shale serve all` on a single machine |
| Watch broker | yes, built in | Watch streams must see writes from every CP process. With PostgreSQL the broker runs on the DB itself (LISTEN/NOTIFY), so there is no extra component. `shale serve all` uses an in-memory broker |
| Message queue | **no** | events are pushed from nodes to the cluster API, and directives from the CP to nodes ([§34.9](#349-events-and-directives)) |
| PKI | no | built-in CA ([§33.5](10-security.md#335-tls)); external certificates optional |
| Clock sync | yes | NTP on every machine ([§10](02-data-model.md#10-time-semantics)) |

`control` and `cluster` are always at least two processes, except in
`shale serve all`. That is why a shared broker is required rather than
optional.

**PostgreSQL requirements.**

- The connection used for `PushEvents` runs with `synchronous_commit = on`
  (the default). With asynchronous replication a failover can lose an
  acknowledged batch of commit events, and the node has already dropped it
  from its outbox. Periodic reconciliation ([§34.9](#349-events-and-directives))
  would repair that within a day, but the events path should not rely on it.
- `LISTEN` is not available on a hot standby and `NOTIFY` is not written to
  the WAL. After a failover the broker reconnects and every open `Watch`
  stream **re-sends its snapshot**, so a client never misses a change
  silently.
- Connection poolers in transaction mode (pgbouncer's default) do not carry
  `LISTEN`. Use session mode, or connect the broker directly.
- One CP replica at a time runs the background jobs, under a PostgreSQL
  advisory lock ([§34.9](#349-events-and-directives)).

### 34.3 State on disk

Everything except object data lives on the OS disk. None of it is object
data, so the "no SSD in the data path" rule
([§22.3](07-storage-node.md#223-no-ssd-in-the-data-path)) is not affected.

| Role | Path | Contents |
|---|---|---|
| CP | `/var/lib/shale/control` | KEK, CA key and certificate (or mounted Secrets), SQLite DB for `shale serve all` |
| Storage Node | `/var/lib/shale/storage` | host key and certificate, CA bundle, cached key set |
| Storage Node | each sink path | object data, sink label |
| Producer | `/var/lib/shale/producer` | host key and certificate, CA bundle. Segments are held in RAM only ([§16](04-write-path.md#16-producer-backpressure)) |
| Reader | `/var/lib/shale/reader` | host key and certificate, CA bundle |
| Relay | `/var/lib/shale/relay` | host key and certificate, CA bundle, cached key set. No stream state: everything it carries is in RAM ([§39](16-relay.md#39-relay)) |

A host that loses its state directory has lost its key. It joins again on its
next start and is recognized by its hardware identity
([§33.4](10-security.md#334-joining-and-adoption)).

### 34.4 Environments

| | Kubernetes | Ubuntu (systemd) | Single machine | Docker |
|---|---|---|---|---|
| Tenant API | Deployment, ≥ 2 replicas, public Service/Ingress | `shale-control.service` | `shale serve all` | container |
| Cluster API | Deployment, ≥ 2 replicas, **internal** Service only | `shale-cluster.service`, internal interface | `shale serve all` (localhost) | container, internal network |
| Storage Node | DaemonSet on nodes labeled for storage | `shale-storage.service` | same process | container per host |
| Producer | — (an edge host) | `shale-producer.service` on the gateway | `shale-producer.service` | container on the gateway |
| Reader | `shale-reader` sidecar or host service beside the media server | `shale-reader.service` | same host | sidecar container |
| Relay | Deployment with hostNetwork or a LoadBalancer with UDP, one per site | `shale-relay.service` | same process | container, `--network host` |
| DB | PostgreSQL | PostgreSQL | SQLite | PostgreSQL or SQLite |
| Sinks | hostPath | mount points | mount points or directories | bind mounts |
| Node network | **hostNetwork** | host | host | `--network host` |
| Secrets | Kubernetes Secrets | files, mode 0600 | files | mounted files |

### 34.5 Kubernetes

- **Tenant API**: a Deployment behind a Service that may be exposed through an
  Ingress or LoadBalancer. It speaks gRPC, so the Ingress must support HTTP/2,
  and it must pass client certificates through (TLS passthrough, or
  termination that forwards the verified client certificate), because
  producers and readers authenticate with them.
- **Cluster API**: a separate Deployment with a ClusterIP Service, or an
  internal LoadBalancer when Storage Nodes run outside the cluster. It is never
  exposed publicly.
- Both share the PostgreSQL DB, the KEK, and the CA through Secrets.
- **Storage Nodes**: a DaemonSet restricted by a node selector to machines with
  storage HDDs.
  - **hostNetwork.** Producers connect to the one node that placement chose,
    so every Storage Node needs a stable, directly reachable address. The data
    path should also avoid the overlay network and kube-proxy. HTTP/3 needs
    UDP on the same port. The CP reaches the node's control API on the same
    host address ([§35.7](12-api.md#357-storage-node-control-api)).
  - How clients reach the node, by host IP or by a DNS name, is set by the
    address resolver ([§34.10](#3410-node-addresses)). Behind an Ingress the
    tenant API does not see the caller's address, so use `template` or `dns`
    there, or configure `trusted_proxies` with the PROXY protocol.
  - Sinks are hostPath mounts of the HDD mount points, listed in the node's
    configuration. Finding and formatting unused disks on labeled nodes is
    an idea, not a plan ([§36.2](13-configuration.md#362-open-decisions)).
  - The pod needs the devices behind its sinks and `CAP_SYS_RAWIO`, for SMART
    and bay LEDs ([§34.8](#348-containers)). A privileged pod also works.
  - Each new machine joins on its first start and appears in
    `shale node ls --pending`; an operator adopts it once. The node's state
    directory is a hostPath, so it keeps its key and identity across pod
    restarts, and its hardware identity across reinstalls
    ([§33.4](10-security.md#334-joining-and-adoption)).
- **Relays** need to be reachable by producers and viewers directly, with
  UDP for ICE: hostNetwork on a labeled node, or a Service of type
  LoadBalancer that carries UDP. Viewers outside the cluster network need
  a public address or a TURN server (`ice`,
  [§36.1](13-configuration.md#361-configuration-reference)). A relay is
  stateless, so replicas are simply more relays for the CP to assign.
- **Upgrades** roll one Storage Node at a time. While a node is down, its
  objects are UNAVAILABLE and placement skips it through missed heartbeats.
  No data moves, so no disruption budget beyond "one at a time" is needed.
  A relay restart drops its sessions for a few seconds and nothing else.

### 34.6 Ubuntu with systemd

- Packages install `/usr/bin/shale` and six units: `shale-control.service`,
  `shale-cluster.service`, `shale-storage.service`, `shale-producer.service`,
  `shale-reader.service`, and `shale-relay.service`. Enable the ones a
  machine plays.
- Configuration lives in `/etc/shale/shale.yaml` and state in `/var/lib/shale`.
- Bind the cluster API to an internal interface only.
- Services run as a dedicated `shale` user that owns the sink directories.
- Host settings: swap off ([§22.4](07-storage-node.md#224-bypass-the-page-cache)),
  NTP on, and a raised `LimitNOFILE` for many concurrent uploads.

### 34.7 Single machine

`shale serve all` runs the tenant API, the cluster API, one Storage Node, and
one Relay in one process, with SQLite and an in-memory watch broker.

- `shale init` creates one tenant. A single organization never has to think
  about tenants: slugs omit them, and every producer, reader, and person
  belongs to that one tenant.
- Sinks can be whole HDDs or directories
  ([§22.2](07-storage-node.md#222-sinks-and-devices)).
- The local node is adopted automatically. Producers and readers still join
  and are adopted, and still use CP-signed tokens, so moving to a multi-node
  cluster later changes no client.
- With one node, set spread falls back to spreading across devices
  ([§11](03-placement.md#11-placement)).
- `shale serve all --dev <dir>` is development mode: one directory sink,
  plaintext allowed, and the multi-sink warning suppressed.

### 34.8 Containers

The image contains the static binary only (distroless). Requirements:

- **Sinks must be bind mounts or hostPath volumes**, never the container's own
  overlay filesystem. Overlay filesystems do not provide `O_DIRECT` or
  `fallocate` reliably ([§22.2](07-storage-node.md#222-sinks-and-devices)).
- `--network host` (or hostNetwork), for the reasons in
  [§34.5](#345-kubernetes).
- **Device access.** Device identity comes from `st_dev` →
  `/sys/dev/block/MAJ:MIN` → WWN/serial, which the default read-only `/sys`
  provides. SMART ([§27](09-operations.md#27-node--device--sink-health-and-quarantine))
  and bay LEDs (`shale device locate`) need more: the block devices behind the
  sinks and the enclosure's `/dev/sg*` devices, plus **`CAP_SYS_RAWIO`**. Grant
  exactly that (`--device … --cap-add SYS_RAWIO`, or the Kubernetes
  equivalent), or run the container privileged. Without it, the node still
  stores and serves data, but health scoring loses SMART and `locate` does
  not work.
- **Hardware identity** comes from `/sys/class/dmi/id/product_uuid` or
  `/sys/firmware/devicetree/base/serial-number`, which the default `/sys`
  provides.
- The state directory is a volume, so the node keeps its key when the
  container is replaced.

### 34.9 Events and directives

Nothing between the CP and a node needs an external queue. Both directions
are direct, idempotent, and repeat until acknowledged.

**Events: node → CP.** Nodes push what happened on their sinks:

| Event | When | Deduplicated by |
|---|---|---|
| `ObjectStored` | an upload committed, or was finalized incomplete ([§12.4](04-write-path.md#124-commit-semantics)) | `attempt_id` |
| `ObjectDeleted` | a file the CP approved or ordered deleted is gone ([§21.2](06-retention-gc.md#212-protocol)) | `(sink_id, object_key)` |
| `ObjectMissing` | a valid token named a key the node does not have, or the node removed a damaged or abandoned file itself ([§14](04-write-path.md#14-duplicates-and-orphans)) | `(sink_id, object_key, reason)` |

- Each node keeps an outbox of unsent events in RAM and sends them in batches
  with `NodeService.PushEvents`.
- On failure it retries with backoff. The outbox holds hours of events at
  ~200 B each ([§12.1](04-write-path.md#121-flow)).
- The CP applies events idempotently, so redelivery is harmless and the result
  is at-least-once. An event about a sink that is no longer attached to the
  sending node is still applied when the attempt it names was allocated to
  that sink on that node ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)).
- Events lost in a node crash are covered by reconciliation below.

**Directives: CP → node.** The CP calls the node's control API
([§35.7](12-api.md#357-storage-node-control-api)). A directive is never a
message that can be lost, because each one is **derived from state the CP
holds**:

| State on the CP | Directive it implies | Cleared by |
|---|---|---|
| object `DELETING` on sink S | `Delete` | `ObjectDeleted`, or `Delete` answering `absent` |
| object with `dates_synced = false` | `SetDates` | the node's acknowledgement |
| device `QUARANTINED` or retired; sink retired | `SetSinkState(accept_writes: false)` | acknowledgement |
| device released | `SetSinkState(accept_writes: true)` | acknowledgement |
| node certificate outdated by an `AddressPolicy` change | `InstallCertificate` | the heartbeat reporting the new serial |
| operator asked for `locate` or `gc run` | `Locate`, `Gc` | acknowledgement (not retried) |

The CP sends a directive when the state changes, retries with backoff while
the node is unreachable, and re-sends everything still implied for a node when
its heartbeats resume. A node that was down therefore catches up within
seconds of coming back, and nothing the CP decided while it was down is lost.
The leader compares the state with what every live node was told every
`directives_every` (5 s); it dials a node's control API on the address the
node's heartbeats come from, with the port the node reports, and checks the
node's certificate by the ID it names rather than by that address.

**Reconciliation.** `NodeControl.Reconcile {sink, since, deleting}` makes the
node stream the records of every complete file on the sink newer than
`since`, and say which of the `deleting` keys are already absent. The CP runs
it, with `since` = the newest `date_committed` it knows for that sink minus a
margin:

- when a node is adopted or re-joins, for each of its sinks,
- when a sink is adopted by another node ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)),
- when a node's heartbeats resume after it was down,
- every `reconcile_interval` (default 24 hours), for every sink,
- with `since = 0` for a full index rebuild ([§29](09-operations.md#29-metadata-index)).

Commits whose events were lost become known this way instead of staying
orphans, and objects stuck in `DELETING` because a node died between the
unlink and its event are confirmed gone.

**One leader.** Rotation waits, policy activation, directive delivery,
reconciliation, DNS maintenance, and the row-retention sweeps are multi-step
jobs. Exactly one CP replica runs them, elected by a PostgreSQL advisory
lock, so two replicas never race on the same job.

### 34.10 Node addresses

Producers and Readers connect directly to the node that placement chose.
**How that node is named to a client is a replaceable policy**, kept apart
from the node itself.

```text
Node       reports facts:     node_id, alias, interfaces and IPs it listens on
Resolver   decides policy:    (node, caller, protocol) → endpoint(s)
Token      stays address-free: aud = node_id, never a host name (§33.2)
```

The CP runs the active resolver whenever it hands out an endpoint: in
allocations, reallocations, read tokens, and the relay endpoints of
`Live` and of a producer's assignment ([§39](16-relay.md#39-relay)).
Nothing else in the system depends on how hosts are addressed. Placement,
tokens, and the index all speak host IDs. The CP itself dials a node's
control API on the cluster-facing IP the node reported, and never through
the resolver.

**Resolvers.** The resolver and its parameters form an `AddressPolicy`, a
global, versioned entity that operators activate through the cluster API
([§35.5](12-api.md#355-cluster-api-custom-rpcs)).

| Resolver | Endpoint handed out | Use |
|---|---|---|
| `advertised` (default) | one of the IPs the node reports, chosen by rules on the caller's network (e.g. internal CIDRs get the internal interface, others the external one; IPv4 or IPv6) | no DNS at all |
| `template` | a name built from the node, e.g. `{alias}.nodes.example.com` | DNS records managed outside Shale |
| `dns` | the same template names, and the CP **maintains the records itself** in a DNS server it is given (RFC 2136 dynamic update, or a provider plugin) | DNS that follows the cluster: records appear when a node joins, change when its IPs change, and disappear when it leaves |

Further resolvers (per-site or per-region names, a load-aware name, a
service-mesh address) implement the same interface: given a node, the caller
(its network, tenant, and site), and the protocol (HTTP/1.1, HTTP/2, or HTTP/3),
return one or more endpoints with scheme, host, and port.

**The caller's network** is the source address of the call, unless the
`AddressPolicy` lists `trusted_proxies`: calls from those addresses carry the
real client address in the PROXY protocol header, and the resolver uses that.
Behind an Ingress or LoadBalancer without it, every caller looks internal, so
`advertised` would hand remote producers unreachable addresses. Such
deployments use `template` or `dns`, where the IP is chosen by DNS on the
client's side (split-horizon DNS gives internal and external clients
different answers), and the CP never needs the caller's network at all.

**TLS follows the resolver.** A client verifies the node certificate against
whatever host it was given, so the node certificate must cover every name and
IP the active resolver can hand out:

- The CP computes the SANs from the resolver: the IPs for `advertised`, the
  generated names for `template` and `dns`.
- When the `AddressPolicy` changes, the CP issues new certificates for the
  affected nodes' existing keys and installs them through the control API
  ([§33.5](10-security.md#335-tls)). The CP activates the new policy only
  after every live node has one, the same way it rotates signing keys
  ([§33.3](10-security.md#333-signing-keys-and-rotation)).
- With external certificates, a wildcard such as `*.nodes.example.com` covers
  a template in one certificate.

**Stale endpoints.** An allocation can be up to one `allocation_horizon` old.
If a node's address changed in the meantime:

- Names from `template` or `dns` keep working, because DNS carries the change
  and its TTL bounds the staleness.
- An IP from `advertised` may no longer answer. The upload then fails like any
  unreachable target, and the producer moves to the next candidate
  ([§13](04-write-path.md#13-retry-and-reallocation)).

Changing a node's addresses never touches objects. Their location is
`(sink_id, object_key)` ([§23.3](07-storage-node.md#233-index-metadata-control-plane)).
