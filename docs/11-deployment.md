# Shale — Deployment

## 34. Deployment

### 34.1 One binary, four roles

```text
shale control     Control Plane, tenant API     (producers, readers, tenant admins)
shale cluster     Control Plane, cluster API    (Storage Nodes, cluster operators)
shale storage     Storage Node
shale all         all of the above in one process (single machine, development)
```

- `control` and `cluster` are **separate entry points**, not one server with a
  flag. The cluster API spans tenants, and no configuration of `shale control`
  can mount it, so a configuration mistake cannot expose it
  ([§35.2](12-api.md#352-two-api-surfaces)). Both are stateless and share the
  metadata DB.
- `shale all` serves both APIs on **separate listeners**. The cluster listener
  binds to localhost by default.
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
| Metadata DB | yes | **PostgreSQL** whenever more than one CP process runs; **SQLite** for `shale all` on a single machine |
| Watch broker | yes, built in | Watch streams must see writes from every CP process. With PostgreSQL the broker runs on the DB itself (LISTEN/NOTIFY), so there is no extra component. `shale all` uses an in-memory broker |
| Message queue | **no** | commit events are pushed from nodes to the cluster API ([§34.9](#349-commit-event-delivery)) |
| PKI | no | built-in CA ([§33.5](10-security.md#335-tls)); external certificates optional |
| Clock sync | yes | NTP on every machine ([§10](02-data-model.md#10-time-semantics)) |

`control` and `cluster` are always at least two processes, except in
`shale all`. That is why a shared broker is required rather than optional.

### 34.3 State on disk

Everything except object data lives on the OS disk. None of it is object
data, so the "no SSD in the data path" rule
([§22.3](07-storage-node.md#223-no-ssd-in-the-data-path)) is not affected.

| Role | Path | Contents |
|---|---|---|
| CP | `/var/lib/shale/control` | signing keys, CA key and certificate (or mounted Secrets), SQLite DB for `shale all` |
| Storage Node | `/var/lib/shale/storage` | `node_id`, node key and certificate, CA bundle, cached key set |
| Storage Node | each sink path | object data, sink label |

### 34.4 Environments

| | Kubernetes | Ubuntu (systemd) | Single machine | Docker |
|---|---|---|---|---|
| Tenant API | Deployment, ≥ 2 replicas, public Service/Ingress | `shale-control.service` | `shale all` | container |
| Cluster API | Deployment, ≥ 2 replicas, **internal** Service only | `shale-cluster.service`, internal interface | `shale all` (localhost) | container, internal network |
| Storage Node | DaemonSet on nodes labeled for storage | `shale-storage.service` | same process | container per host |
| DB | PostgreSQL | PostgreSQL | SQLite | PostgreSQL or SQLite |
| Sinks | hostPath | mount points | mount points or directories | bind mounts |
| Node network | **hostNetwork** | host | host | `--network host` |
| Secrets | Kubernetes Secrets | files, mode 0600 | files | mounted files |

### 34.5 Kubernetes

- **Tenant API**: a Deployment behind a Service that may be exposed through an
  Ingress or LoadBalancer. It speaks gRPC, so the Ingress must support HTTP/2.
- **Cluster API**: a separate Deployment with a ClusterIP Service, or an
  internal LoadBalancer when Storage Nodes run outside the cluster. It is never
  exposed publicly.
- Both share the PostgreSQL DB, the signing key, and the CA through Secrets.
- **Storage Nodes**: a DaemonSet restricted by a node selector to machines with
  storage HDDs.
  - **hostNetwork.** Writers connect to the one node that placement chose, so
    every Storage Node needs a stable, directly reachable address. The data
    path should also avoid the overlay network and kube-proxy. HTTP/3 needs
    UDP on the same port.
  - How clients reach the node, by host IP or by a DNS name, is set by the
    address resolver ([§34.10](#3410-node-addresses)).
  - Sinks are hostPath mounts of the HDD mount points.
  - The pod needs the devices behind its sinks and `CAP_SYS_RAWIO`, for SMART
    and bay LEDs ([§34.8](#348-containers)). A privileged pod also works.
  - The join token comes from a Secret. After joining, the node's state
    directory is a hostPath too, so it keeps its identity across pod restarts.
- **Upgrades** roll one Storage Node at a time. While a node is down, its
  objects are UNAVAILABLE and placement skips it through missed heartbeats.
  No data moves, so no disruption budget beyond "one at a time" is needed.

### 34.6 Ubuntu with systemd

- Packages install `/usr/bin/shale` and three units: `shale-control.service`,
  `shale-cluster.service`, and `shale-storage.service`. Enable the ones a
  machine plays.
- Configuration lives in `/etc/shale/shale.yaml` and state in `/var/lib/shale`.
- Bind the cluster API to an internal interface only.
- Services run as a dedicated `shale` user that owns the sink directories.
- Host settings: swap off ([§22.4](07-storage-node.md#224-bypass-the-page-cache)),
  NTP on, and a raised `LimitNOFILE` for many concurrent uploads.

### 34.7 Single machine

`shale all` runs the tenant API, the cluster API, and one Storage Node in one
process, with SQLite and an in-memory watch broker.

- `shale cluster init` creates one tenant. A single organization never has to
  think about tenants: slugs omit them, and every holder belongs to that one
  tenant.
- Sinks can be whole HDDs or directories
  ([§22.2](07-storage-node.md#222-sinks-and-devices)).
- The node joins implicitly. Producers and readers still enroll and still use
  CP-signed tokens, so moving to a multi-node cluster later changes no client.
- With one node, set spread falls back to spreading across devices
  ([§11](03-placement.md#11-placement)).
- `shale all --dev <dir>` is development mode: one directory sink, plaintext
  allowed, and the multi-sink warning suppressed.

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
- The state directory is a volume, so the node keeps its identity when the
  container is replaced.

### 34.9 Commit event delivery

The CP needs no external queue. Nodes push `ObjectStored` and deletion events
directly:

- Each node keeps an outbox of unsent events in RAM and sends them in batches
  with `NodeService.PushEvents` on the cluster API.
- On failure it retries with backoff. The outbox holds hours of events at
  ~200 B each ([§12.1](04-write-path.md#121-flow)).
- The CP applies events idempotently
  ([§14](04-write-path.md#14-duplicates-and-orphans)), so redelivery is
  harmless and the result is at-least-once.
- Events lost in a node crash are covered by the startup replay window
  ([§12.4](04-write-path.md#124-commit-semantics)) and, beyond that, by orphan
  reclamation ([§21.3](06-retention-gc.md#213-orphans)).

### 34.10 Node addresses

Writers and Readers connect directly to the node that placement chose. **How
that node is named to a client is a replaceable policy**, kept apart from
the node itself.

```text
Node       reports facts:     node_id, alias, interfaces and IPs it listens on
Resolver   decides policy:    (node, caller, protocol) → endpoint(s)
Token      stays address-free: aud = node_id, never a host name (§33.2)
```

The CP runs the active resolver whenever it hands out an endpoint: in
allocations, reallocations, and read tokens. Nothing else in the system
depends on how nodes are addressed. Placement, tokens, and the index all
speak `node_id`.

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

**TLS follows the resolver.** A client verifies the node certificate against
whatever host it was given, so the node certificate must cover every name and
IP the active resolver can hand out:

- The CP computes the SANs from the resolver: the IPs for `advertised`, the
  generated names for `template` and `dns`.
- When the `AddressPolicy` changes, affected nodes receive new certificates
  through their normal renewal ([§33.4](10-security.md#334-enrollment)). The
  CP activates the new policy only after they have them, the same way it
  rotates signing keys ([§33.3](10-security.md#333-signing-keys-and-rotation)).
- With external certificates, a wildcard such as `*.nodes.example.com` covers
  a template in one certificate.

**Stale endpoints.** An allocation can be up to one `allocation_horizon` old.
If a node's address changed in the meantime:

- Names from `template` or `dns` keep working, because DNS carries the change
  and its TTL bounds the staleness.
- An IP from `advertised` may no longer answer. The upload then fails like any
  unreachable target, and `ObjectService.Reallocate` returns fresh endpoints
  ([§13](04-write-path.md#13-retry-and-reallocation)).

Changing a node's addresses never touches objects. Their location is
`(sink_id, object_key)` ([§23.3](07-storage-node.md#233-index-metadata-control-plane)).
