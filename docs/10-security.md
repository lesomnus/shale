# Shale — Security

## 33. Authentication and Authorization

### 33.1 Trust model

| Principal | Kind | Authenticates to | With | May |
|---|---|---|---|---|
| **Producer** | Holder of a tenant | tenant API | bearer credential | register and allocate for its tenant's sets |
| **Reader** | Holder of a tenant | tenant API | bearer credential | query and read its tenant's objects |
| **Tenant admin** | Holder of a tenant | tenant API | bearer credential | manage its tenant's sites, sets, sources, holders, enrollment tokens, and retention dates |
| **Storage Node** | cluster infrastructure | cluster API | node certificate (mTLS) | report its own devices and sinks; push events; propose GC |
| **Cluster operator** | cluster infrastructure | cluster API | admin credential | manage tenants, nodes, devices, sinks, keys, policies |
| **Control Plane** | — | everyone | TLS certificate, signing key | placement, authorization, metadata |

- **The tenant wall.** Every tenant-owned entity is behind payday's wall: a
  holder sees and changes only its own tenant's rows. A single-organization
  deployment is a cluster with one tenant, so the wall is always on, and
  multi-tenancy needs no code change
  ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Sites.** Within a tenant, a holder can be further limited to the sites it
  is a member of (payday's second axis, field 3). Tenant admins see every
  site. Readers and producers see only their own sites, and a producer's
  credential is issued *within* the site of the set it was enrolled for, so it
  is useless anywhere else ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Two surfaces.** Anything that must span tenants (Storage Nodes, cluster
  operators) uses the cluster API, which is a separate entry point on the
  internal network. The tenant API contains no path that sees more than one
  tenant ([§35.2](12-api.md#352-two-api-surfaces)).
- **Storage Nodes know nothing about tenants, holders, or credentials.** They
  trust one thing: a valid CP signature on each request.
- Enrolled devices are trusted to behave. Networks are not: a producer may sit
  behind a WAN or wireless link, so TLS is on everywhere by default
  ([§33.5](#335-tls)).

### 33.2 Access tokens

Every request a Storage Node accepts carries an **access token** signed by
the CP. The node follows a single rule: no valid CP signature, no service.
The CP itself never calls a node.

The signature is **Ed25519** over a compact set of claims:

```text
kid          signing key ID
exp          expiry
aud          node_id              (a token for node A is useless on node B)
op           put | get | head
sink_id
object_key
attempt_id   (put)
max_length   (put: upper bound on the upload's size, from the agreed profile)
mode, idle_timeout, abandon_timeout
             (put: the agreed upload profile, §12.6; enforced within node caps)
holder       holder ID and tenant ID of the caller the token was issued to
```

- Carried in `Authorization: Shale <token>`, or in a `token=` query parameter
  for clients that can only pass a URL (e.g. a media server handing a GET URL
  to a player). The URL form is what the design calls a presigned URL.
- **Verification** on the node is local and needs no call to the CP:
  signature, known `kid`, `exp` (with a small clock-skew allowance), `aud`
  equals its own node ID, `op` matches the method, `sink_id`/`object_key`
  match the path, and the upload does not exceed `max_length`. Ed25519
  verification costs ~50 µs, which is negligible at hundreds of requests per
  second.
- **Issued by the tenant API**, inside the wall: `Allocate` issues put tokens
  and `Timeline` issues get tokens, and only for the caller's own tenant's
  objects. The wall therefore extends to the data plane without the node
  knowing tenants exist.
- **Lifetime**: put tokens live as long as their allocation
  (`allocation_ttl`, [§12.1](04-write-path.md#121-flow)). Get tokens live
  `read_token_ttl` (default 1 hour). A reader asks again for longer sessions.
- **Replay** gains nothing. A put token names one key and one attempt, uploads
  are idempotent ([§12.5](04-write-path.md#125-idempotent-uploads)), and a
  complete object cannot be overwritten.
- **Revocation** stops the CP from issuing new tokens. Tokens already issued
  stay valid until they expire, so revocation takes effect within
  `allocation_ttl` (20 minutes) at most. This lag is accepted.

`holder` is not used for authorization on the node. It goes into node logs and
metrics, so a misbehaving producer can be traced.

### 33.3 Signing keys and rotation

- Signing keys are `SigningKey` rows, global to the cluster. Only their public
  halves are readable through the API.
- Nodes receive the key set when they join, then **watch** `SigningKey` on the
  cluster API, so a new key reaches them within seconds. They **cache the key
  set in their state directory**, so a node that restarts while the CP is down
  still verifies tokens.
- Each heartbeat reports the key IDs the node holds.
- All CP replicas share the private signing key, stored as a file or a
  Kubernetes Secret ([§34](11-deployment.md#34-deployment)).
- Rotation (`SigningKeyService.Rotate`):
  1. add the new key,
  2. wait until every live node reports holding it,
  3. start signing with the new key,
  4. remove the old key after the longest token lifetime has passed.

### 33.4 Enrollment

**The cluster** is initialized once:

```text
shale cluster init
  → CA, CP certificate, signing key
  → the first tenant (the organization, in a single-organization deployment)
  → a cluster admin credential and a tenant admin credential for that tenant
```

**Storage Nodes** join the way `kubeadm join` works:

```text
operator:  shale join-token add --ttl 1h
node:      shale storage --join https://cp-cluster.internal:7401 \
                         --token <join-token> --ca-hash sha256:<fingerprint>
```

1. The node connects to the cluster API and checks the CP certificate's CA
   against `--ca-hash`, which rules out a man in the middle on first contact.
2. It generates its own key pair and calls `NodeService.Join` with a CSR, its
   interfaces and IPs, and its sinks, authenticated by the one-time join
   token.
3. The CP assigns a `node_id`, issues a **node certificate** from its built-in
   CA ([§33.5](#335-tls)), and returns the CA bundle and the key set.

The node certificate serves two purposes. It is the node's TLS server
certificate, so producers and readers can trust the node, and it is the node's
mTLS client certificate for the cluster API. The node renews it automatically
(`NodeService.RenewCertificate`) before it expires.

**Producers and readers** are holders, enrolled by their tenant's admin:

```text
tenant admin:  shale enrollment-token add --role producer --set <set> --ttl 24h
device:        HolderService.Enroll {enrollment token}
               → holder ID, credential (long-lived bearer token), CA bundle
```

- `Enroll` is the one tenant API call made without a credential. The
  enrollment token decides the tenant and role, so a device cannot choose
  them.
- The credential is **scoped by role and site**: a producer may register and
  allocate only for the set its enrollment named, within that set's site. A
  reader may query and read within the sites its enrollment named, or the
  whole tenant if it named none.
- The CP stores only a hash of each credential. Erasing the holder revokes it.
- Remote devices get bearer tokens rather than client certificates because
  tokens are easier to provision and replace.

### 33.5 TLS

- The CP runs a **built-in CA** by default. `shale cluster init` creates it. No
  external PKI is needed on Kubernetes, plain Linux, or a single machine.
- Node certificates carry every name and IP the active address resolver can
  hand out for that node. Writers and Readers dial nodes directly, so the
  certificate must match whatever endpoint they were given
  ([§34.10](11-deployment.md#3410-node-addresses)).
- **External certificates** (cert-manager, ACME, a corporate CA) can replace
  the built-in CA's output: configure certificate and key paths, and the node
  and CP load those instead.
- TLS is on everywhere, the data-center LAN included. With AES-NI it costs
  about one core per several GB/s, which is small next to HDD-bound ingest.
- **Plaintext only in development mode** (`shale all --dev`).

### 33.6 Channels

| From → To | Protocol | Authentication |
|---|---|---|
| Producer / Reader / tenant admin → tenant API | gRPC over TLS | bearer credential |
| Producer / Reader → Storage Node | HTTP/1.1, HTTP/2, or HTTP/3 over TLS | CP-signed access token |
| Storage Node → cluster API | gRPC over mTLS | node certificate |
| Cluster operator → cluster API | gRPC over TLS | admin credential |
| CP → Storage Node | none; the CP never calls a node | — |
| Node → Node | none; nodes never talk to each other | — |

### 33.7 What a compromise costs

| Leaked | Attacker can | Response |
|---|---|---|
| One access token | one operation on one object until it expires | none needed |
| A producer credential | allocate and write into that producer's sets | erase the holder; effective within `allocation_ttl` |
| A reader credential | read its tenant's objects | erase the holder |
| A tenant admin credential | anything inside **that tenant only**; the wall holds | erase the holder |
| A node's key | act as that node: serve or drop its own sinks' data, report its own devices and sinks (the CP rejects reports and events for sinks not attached to that node) | revoke the node certificate, re-join |
| A cluster admin credential | manage the cluster and read across tenants, **from the internal network** | rotate; the cluster API is not reachable from outside |
| The CP signing key | read and write any object on any node | rotate the key at once ([§33.3](#333-signing-keys-and-rotation)) |
| The CA key | impersonate nodes or the CP to clients | re-initialize the CA and re-enroll everything; protect it accordingly |
