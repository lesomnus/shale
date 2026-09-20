# Shale — Security

## 33. Authentication and Authorization

### 33.1 Trust model

Two kinds of principal exist. **People** sign in and are payday Holders.
**Hosts** are machines the operator owns; each is a row of its own, identified
by its hardware and authenticated by a certificate the Control Plane issues
when an operator adopts it ([§33.4](#334-joining-and-adoption)). Nothing else
holds a credential.

| Principal | Kind | Authenticates to | With | May |
|---|---|---|---|---|
| **Producer** | host, tenant entity | tenant API | host certificate (mTLS) | negotiate and allocate for its set; upload with the access tokens it is given |
| **Reader** | host, tenant entity | tenant API | host certificate (mTLS) | query and read objects of its sites |
| **Tenant admin** | person (Holder) | tenant API | payday sign-in | manage its tenant's sites, sets, sources, producers, readers, and retention dates |
| **Storage Node** | host, global entity | cluster API | host certificate (mTLS) | report its own devices and sinks; push events; propose GC |
| **Relay** | host, global entity | cluster API | host certificate (mTLS) | report its load; serve producers and viewers that carry tokens ([§39](16-relay.md#39-relay)) |
| **Cluster operator** | person (Holder) | cluster API | payday sign-in | adopt nodes; manage tenants, sinks, keys, policies |
| **Control Plane** | — | everyone | CP certificate, signing key | placement, authorization, metadata; calls nodes ([§35.7](12-api.md#357-storage-node-control-api)) |

- **The tenant wall.** Every tenant-owned entity is behind payday's wall: a
  caller sees and changes only its own tenant's rows. A single-organization
  deployment is a cluster with one tenant, so the wall is always on, and
  multi-tenancy needs no code change
  ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Sites.** Within a tenant, a person or a reader can be limited to the
  sites it is a member of (payday's second axis, field 3). Tenant admins see
  every site. A producer is issued *within* the site of the set it was adopted
  for, so it is useless anywhere else
  ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Two surfaces.** Anything that must span tenants (Storage Nodes, cluster
  operators) uses the cluster API, which is a separate entry point on the
  internal network. The tenant API contains no path that sees more than one
  tenant ([§35.2](12-api.md#352-two-api-surfaces)).
- **People sign in** with payday's session login (alias and password), or
  through an external identity provider over OIDC. `shale init` creates the
  first cluster operator and the first tenant admin and prints their one-time
  credentials. The `shale` CLI signs in the same way (`shale login`) and keeps
  a session; automation uses a Holder API token. Cluster operators are Holders
  whose tenant the cluster API's policy lets see every tenant.
- **Storage Nodes and Relays know nothing about tenants or people.** On
  their data planes they trust one thing: a valid CP signature on each
  request or stream. On its control API a node trusts one peer: the Control
  Plane.
- **Viewers** of live video are people in a browser or Reader hosts. Both
  get a view token from the tenant API and present it to a relay; the relay
  cannot tell them apart and need not ([§39.4](16-relay.md#394-viewers)).
- Adopted hosts are trusted to behave. Networks are not: a producer may sit
  behind a WAN or wireless link, so TLS is on everywhere by default
  ([§33.5](#335-tls)).

### 33.2 Access tokens

Every data-plane request a Storage Node accepts, and every stream or
session a Relay accepts, carries an **access token** signed by the CP. The
host follows a single rule: no valid CP signature, no service.

The signature is **Ed25519** over a compact set of claims:

```text
kid          signing key ID
exp          expiry
aud          node_id or relay_id  (a token for host A is useless on host B)
op           put | get            (either also allows HEAD on the key)
             publish | view       (relay: §39)
sink_id
object_key
attempt_id   (put)
record       (put: the object's initial xattr record, §23.1; the node writes
              it as given and fills in what only it knows)
max_length   (put: upper bound on the upload's size, from the agreed profile)
mode, idle_timeout, abandon_timeout
             (put: the agreed upload profile, §12.6; enforced within node caps)
sources      (publish: the source IDs this producer may feed)
source       (view: the one source this viewer may watch)
actor        the Producer, Reader, or person the token was issued to, and
             its tenant
```

- Carried in `Authorization: Shale <token>`, or in a `token=` query parameter
  for clients that can only pass a URL (e.g. a media server handing a GET URL
  to a player). The URL form is what the design calls a presigned URL. Nodes
  and proxies must not log query strings.
- **Verification** on the node is local and needs no call to the CP:
  signature, known `kid`, `exp` (with `token_skew`, default 1 minute), `aud`
  equals its own node ID, `op` matches the method (`HEAD` is allowed by
  either), `sink_id`/`object_key` match the path, and the upload does not
  exceed `max_length`. Ed25519 verification costs ~50 µs, which is negligible
  at hundreds of requests per second.
- **Issued by the tenant API**, inside the wall: `Allocate` issues put
  tokens, `Timeline` issues get tokens, `Live` issues view tokens, and
  `Negotiate`, the producer heartbeat, and `ProducerService.Relay` issue
  publish tokens, all only for the caller's own tenant's sources and
  objects. The wall therefore extends to the data plane without a node or a
  relay knowing tenants exist.
- **Lifetime**: put tokens live as long as their allocation
  (`allocation_ttl`, [§12.1](04-write-path.md#121-flow)), and
  `ObjectService.Renew` issues a fresh one for an attempt still in progress.
  Get tokens live `read_token_ttl` (default 1 hour) and view tokens
  `view_token_ttl` (1 hour); a session already open outlives its token. A
  publish token lives `publish_token_ttl` (24 hours) and is checked when the
  producer attaches ([§39.3](16-relay.md#393-from-the-producer)).
- **Replay** gains nothing. A put token names one key and one attempt, uploads
  are idempotent ([§12.5](04-write-path.md#125-idempotent-uploads)), and a
  complete object cannot be overwritten.
- **Revocation** stops the CP from issuing new tokens. Tokens already issued
  stay valid until they expire: at most `allocation_ttl` for uploads (22
  minutes at the defaults, about 80 at the bounds) and `read_token_ttl` for
  reads. This lag is accepted.

`actor` is used by the node for two things only: **per-actor limits**
(`uploads_per_actor`, `sessions_per_actor`,
[§36.1](13-configuration.md#361-configuration-reference)), so one misbehaving
host cannot take a node's whole capacity, and logs and metrics, so it can be
traced. It grants nothing.

### 33.3 Signing keys and rotation

- Signing keys are `SigningKey` rows, global to the cluster. The private half
  is stored **in the DB, wrapped by a key-encryption key** (`KEK`) that lives
  in the CP's state directory or a Kubernetes Secret. Every CP replica reads
  the same row, so a rotation is one transaction and never leaves a replica
  signing with a key the others do not know. Only public halves are readable
  through the API.
- Nodes receive the key set when they are adopted, then **watch**
  `SigningKey` on the cluster API, so a new key reaches them within seconds.
  They **cache the key set in their state directory**, so a node that
  restarts while the CP is down still verifies tokens.
- Each heartbeat reports the key IDs the node holds.
- Rotation (`SigningKeyService.Rotate`, run by the CP leader,
  [§34.9](11-deployment.md#349-events-and-directives)):
  1. add the new key,
  2. wait until every live node reports holding it,
  3. start signing with the new key,
  4. remove the old key after the longest token lifetime has passed
     (`allocation_ttl` at its bound, or `read_token_ttl`, whichever is longer).

### 33.4 Joining and adoption

The operator does three things over a cluster's life: create it, point each
host at it, and adopt hosts. Keys and certificates are the Control Plane's
business throughout.

**The cluster** is initialized once:

```text
shale init
  → CA, CP certificate, signing key, KEK
  → the first tenant (the organization, in a single-organization deployment)
  → the first cluster operator and the first tenant admin, with one-time
    sign-in credentials
```

**A host joins on its first run.** Every host runs the Shale binary and is
given only the Control Plane's address:

```text
shale serve storage  --cp https://cp-cluster.internal:7401
shale serve producer --cp https://cp.example.com:7400 [--tenant acme]
shale serve reader   --cp https://cp.example.com:7400 [--tenant acme]
```

1. The host reads its **hardware identity**: the DMI product UUID
   (`/sys/class/dmi/id/product_uuid`), or on boards without DMI such as a
   Raspberry Pi the device-tree serial number
   (`/sys/firmware/devicetree/base/serial-number`). Both survive a reinstall.
   Only when neither exists does it fall back to `/etc/machine-id`, which does
   not, and it says so in its join request.
2. It generates a key pair in its state directory and connects to the CP. On
   first contact it **pins the CA it sees**; `--ca-hash sha256:<fingerprint>`
   makes that a check instead of a pin, for operators who want to rule out a
   man in the middle on an untrusted network.
3. It calls `Join` with its role, hardware identity, hostname, and a CSR, and
   for a producer or reader the tenant it is meant for (implied in a
   single-organization cluster). A Storage Node adds its interfaces and its
   sinks. `Join` needs no credential and is rate-limited per source address.
4. The CP records a **pending host**: a `Node`, `Producer`, or `Reader` row in
   state `pending`. If a row with the same hardware identity already exists,
   the request is attached to that row instead, shown as "known host, new
   key". The host repeats `Join` every 10 seconds until it is answered with a
   certificate. A pending request nobody adopts expires after
   `join_pending_ttl` (24 hours); the host keeps asking, so it reappears.
5. An operator **adopts** it, from the console or the CLI, after checking that
   it is theirs. The console shows the hostname, the hardware identity, the
   key fingerprint the host also prints in its log, and, for a re-join, the
   row it will continue:

   ```text
   shale node ls --pending
   shale node adopt <node>
   shale producer adopt <producer> --set <set>          # site follows the set
   shale reader adopt <reader> [--site <site>]...       # none: the whole tenant
   ```

6. The CP issues the **host certificate** from its built-in CA
   ([§33.5](#335-tls)) and the next `Join` answers with it, the CA bundle,
   and, for a node, its `node_id` and the key set. The host is running.

**Renewal is automatic.** A host renews at two thirds of its certificate's
lifetime (`RenewCertificate`, over mTLS with the current certificate), and may
send a new key. If a certificate has already expired, for instance a chassis
that sat in storage for a year, the host goes back to step 3 with the same
hardware identity, appears as "known host, new key", and one `adopt` puts it
back under its old name, `node_id`, and sinks. A cluster that trusts its
hardware identities can set `readopt: auto` and skip that click; the default
is `manual`, because a hardware identity is a claim the host makes, not one
the CP can verify.

**Erasing a host** is soft (`shale node erase`, `shale producer erase`,
`shale reader erase`). The CP resolves every mTLS peer to its row and compares
the certificate serial the row records, so an erased host, or a host whose
certificate was replaced, is refused on its next call without a revocation
list. `shale node erase` is refused while sinks are attached to the node,
unless `--detach` is given, which leaves their objects UNAVAILABLE until the
sinks are adopted elsewhere ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)).

A **Reader** host is the machine that runs a media server. `shale serve
reader` is an agent beside it: it holds the reader's certificate, serves the
tenant API on localhost without TLS for the media server software, and writes
the CA bundle to a file the media server uses to trust Storage Nodes when it
fetches objects directly. The media server itself manages no keys.

### 33.5 TLS

- The CP runs a **built-in CA** by default. `shale init` creates it. No
  external PKI is needed on Kubernetes, plain Linux, or a single machine.
- **Host certificates** name their row: a URI SAN carrying the entity ID,
  whose domain byte says whether it is a node, a producer, or a reader
  ([§35.1](12-api.md#351-conventions)). A node certificate also carries every
  name and IP the active address resolver can hand out for that node, because
  producers and readers dial nodes directly and verify what they were given
  ([§34.10](11-deployment.md#3410-node-addresses)). When the `AddressPolicy`
  changes, the CP issues new node certificates for the nodes' existing keys
  and installs them (`NodeControl.InstallCertificate`), and activates the new
  policy only after every live node has one.
- Lifetimes: host certificates 90 days, renewed at 60; the CP certificate one
  year, renewed by the CP itself; the CA 10 years.
- **CA rollover is automatic.** One year before the CA expires, the CP creates
  the next CA and starts handing out a bundle with both. Every renewal from
  then on is issued by the new CA. When every live host reports the new bundle
  (heartbeats and renewals carry its hash) and no certificate from the old CA
  is still valid, the old CA is dropped. `shale ca rotate` starts the same
  procedure early. A **compromised** CA cannot be rolled over this way, since
  nothing it signs can be trusted: the operator re-initializes the CA and
  every host is adopted again ([§33.7](#337-what-a-compromise-costs)).
- **External certificates** (cert-manager, ACME, a corporate CA) can replace
  the built-in CA's output for the CP and for nodes: configure certificate and
  key paths, and they are loaded instead. Host adoption still issues the
  client certificates producers and readers use.
- TLS is on everywhere, the data-center LAN included. With AES-NI it costs
  about one core per several GB/s, which is small next to HDD-bound ingest.
- **Plaintext only in development mode** (`shale serve all --dev`) and on the
  reader agent's localhost listener.

### 33.6 Channels

| From → To | Protocol | Authentication |
|---|---|---|
| Producer / Reader → tenant API | gRPC over mTLS | host certificate |
| Person / CLI → tenant or cluster API | gRPC over TLS | payday session or API token |
| Producer / Reader → Storage Node | HTTP/1.1, HTTP/2, or HTTP/3 over TLS | CP-signed access token |
| Storage Node → cluster API | gRPC over mTLS | node certificate |
| Control Plane → Storage Node | gRPC over mTLS (control API, [§35.7](12-api.md#357-storage-node-control-api)) | CP certificate |
| Producer → Relay | gRPC over TLS (ingest, [§35.8](12-api.md#358-relay-ingest-and-whep)) | CP-signed publish token |
| Viewer → Relay | HTTPS (WHEP) and WebRTC (DTLS-SRTP) | CP-signed view token |
| Relay → cluster API | gRPC over mTLS | relay certificate |
| Node → Node, Relay → Relay, Relay → Node | none | — |

### 33.7 What a compromise costs

| Leaked | Attacker can | Response |
|---|---|---|
| One access token | one operation on one object until it expires; a view token, one camera for an hour | none needed |
| A publish token | feed false video for that producer's cameras to viewers, for up to a day | erase the producer; the relay refuses it at its next attach |
| A relay's key | serve any stream it carries to anyone, and feed viewers anything; it holds no token for any Storage Node, so recordings are out of reach | erase the relay and adopt the machine again; its producers are reassigned |
| A producer's key | negotiate and allocate for that producer's set, and write objects into it up to the set's ceilings ([§12.6](04-write-path.md#126-upload-profile-negotiation)) | erase the producer; mTLS refuses it at once, tokens in flight expire within `allocation_ttl` |
| A reader's key | read the objects of its sites | erase the reader; effective at once for new tokens, within `read_token_ttl` for issued ones |
| A person's session | anything that person may do, inside **their tenant only**; the wall holds | end the session, reset the password |
| A node's key | act as that node: serve or drop its own sinks' data, report its own devices and sinks (the CP rejects reports for sinks and devices not attached to that node, and clamps reported capacity, [§27](09-operations.md#27-node--device--sink-health-and-quarantine)) | erase the node and adopt the machine again under a new key |
| A hardware identity | request adoption as a known host | nothing until an operator adopts it; with `readopt: auto`, impersonate that host, which is why the default is `manual` |
| A cluster operator's session | manage the cluster and read across tenants, **from the internal network** | end the session; the cluster API is not reachable from outside |
| The CP signing key | read and write any object on any node | rotate the key at once ([§33.3](#333-signing-keys-and-rotation)) |
| The CA key | impersonate nodes or the CP to clients | re-initialize the CA and adopt every host again; protect it accordingly |
