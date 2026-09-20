# Shale — API

## 35. API

The Control Plane speaks **resource-oriented gRPC**, built with
[payday](https://github.com/lesomnus/payday). Storage Nodes speak **HTTP**
(HTTP/1.1, HTTP/2, and HTTP/3 over QUIC) to move object bytes, and serve a
small **control API** (gRPC) that only the Control Plane calls
([§35.7](#357-storage-node-control-api)). Relays take streams from
producers over gRPC and serve viewers over WHEP
([§35.8](#358-relay-ingest-and-whep)).

### 35.1 Conventions

Shale follows payday's contract. Each entity is declared once in a schema
proto, and its service, messages, database schema, server, CLI verbs, and
TypeScript client are generated from that declaration.

| Element | Meaning |
|---|---|
| `<E>Service` | one service per entity, with only the verbs the schema declares: `Add`, `Get`, `Patch`, `Erase`, `List`, `Watch` |
| `<E>Ref` | names a row: `oneof { bytes id; <E>RefBySlug slug }` |
| `<E>Select` | the fields a `Get` answers with |
| `List` | `filters` (refs) + `size` + `after`; the answer carries `next`. The cursor names the last row read, so rows added meanwhile are neither skipped nor repeated |
| `Watch` | the first message is a snapshot; after that each message is the **current state** of a changed row, never a delta. An absent value means the row left the caller's view |
| `Patch` | optimistic locking on `date_updated`; skipping the check requires `date_updated_force` |
| `Erase` | answers `erased: bool`; "already gone", "never existed", and "not yours" are one answer |
| Custom RPCs | operations that mean something are their own RPCs, added to the entity's service in an overlay (`*_svc.ext.proto`), not expressed as a `Patch` |
| Errors | gRPC status; invalid input carries field paths in `google.rpc.BadRequest` |
| Batch | `payday.BatchService` runs several writes in one transaction (e.g. a set and its sources) |

**Identifiers** are payday's UUIDv8: time-ordered, with one **domain byte**
naming the entity kind, so a reference of the wrong kind is refused at the
edge. An `object_id` is therefore also a creation timestamp.

**Slugs** are the names people write: `@TENANT/ALIAS#DOMAIN`. In a
single-organization deployment the tenant is implied by the caller, so
`cam-03#source` is enough ([§7](02-data-model.md#7-source-set-zone-epoch)).

**Rate limits.** Every RPC is counted per caller: per actor for hosts and
people, and per tenant on top of that (`rpc_rate`,
[§36.1](13-configuration.md#361-configuration-reference)). A caller over its
rate is answered `RESOURCE_EXHAUSTED` with a retry delay.

**General writes.** payday closes `Patch` and `Apply` by default, since
they can write anything the schema has. Shale serves `Patch` for the rows
[§32](09-operations.md#32-cli--processes) says people and operators edit
(a set's retention and placement, a source, a site, a person, a tenant's
share, a relay's labels, the policies) and keeps it closed for what the
system writes (objects, attempts) and for hosts, devices, sinks, and keys,
whose states move only through their own verbs. `Apply` stays closed.

### 35.2 Two API surfaces

The Control Plane serves two gRPC surfaces from **separate entry points**,
following payday's advice that a path able to see every tenant must not exist
in the public server at all
([§34.1](11-deployment.md#341-one-binary-one-command-per-role)).

| Surface | Entry point | Callers | Tenant wall | Exposure |
|---|---|---|---|---|
| **Tenant API** | `shale serve control` | producers, readers, tenant admins | **on**: a caller sees only its own tenant | may face the internet |
| **Cluster API** | `shale serve cluster` | Storage Nodes, cluster operators | spans all tenants; writes global entities | **internal network only** |

No flag mounts cluster services into `shale serve control`. A single-machine
`shale serve all` serves both, on separate listeners, with the cluster
listener on localhost by default.

A host joins through the surface it will use afterwards: a Storage Node
through the cluster API, a producer or a reader through the tenant API
([§33.4](10-security.md#334-joining-and-adoption)). `Join` is the one call on
each surface that is made without a credential.

### 35.3 Entities

| Entity | Tenancy | Domain | Generated verbs | Surface |
|---|---|---|---|---|
| `Tenant` (payday) | is the tenant | 1 | Add, Get, List, Erase | cluster |
| `Holder` (payday) | tenant | 2 | Add, Get, Patch, List, Erase | tenant |
| `Set` | tenant | 7 | Add, Get, Patch, Erase, List, Watch | tenant |
| `Source` | tenant | 8 | Add, Get, Patch, Erase, List, Watch | tenant |
| `Object` | tenant | 9 | Get, List, Watch | tenant (all tenants on cluster) |
| `Attempt` | tenant | 10 | Get, List | tenant |
| `Site` | tenant | 19 | Add, Get, Patch, Erase, List, Watch | tenant |
| `SiteMember` | tenant | 20 | Add, List, Erase | tenant |
| `Producer` | tenant | 22 | Get, Patch, List, Erase, Watch | tenant |
| `Reader` | tenant | 23 | Get, Patch, List, Erase, Watch | tenant |
| `Node` | **global** | 12 | Get, Patch, List, Erase, Watch | cluster |
| `Relay` | **global** | 24 | Get, Patch, List, Erase, Watch | cluster |
| `Device` | **global** | 13 | Get, List, Watch | cluster |
| `Sink` | **global** | 14 | Get, List, Watch | cluster |
| `SigningKey` | **global** | 15 | List, Watch | cluster |
| `PlacementPolicy` | **global** | 16 | Add, Get, List | cluster |
| `UploadPolicy` | **global** | 18 | Add, Get, List | cluster |
| `AddressPolicy` | **global** | 21 | Add, Get, List | cluster |

Domains 11 and 17 were enrollment tokens and join tokens. They are retired:
hosts are adopted instead ([§33.4](10-security.md#334-joining-and-adoption),
[§36.3](13-configuration.md#363-rejected-alternatives)). A retired domain is
never reused.

- **Tenant-owned** is payday's default. Every entity outside the wall says
  `global: {}` explicitly, and a search for it lists exactly the eight above.
  Storage is physically shared, so the infrastructure that holds it belongs to
  the cluster, not to any tenant.
- **Holders are people**: tenant admins, cluster operators, and console users.
  They sign in ([§33.1](10-security.md#331-trust-model)). payday's audit trail
  names a Holder or a host as the actor of every write.
- **Hosts are entities of their own.** A `Producer` and a `Reader` belong to a
  tenant; a `Node` and a `Relay` are global. A host row is created by the
  host's own `Join` and made usable by an operator's `Adopt`; there is no
  generated `Add`. Each carries its hardware identity and its certificate
  ([§33.4](10-security.md#334-joining-and-adoption)).
- **Field 3 is `site`**, payday's second permission axis
  ([§7](02-data-model.md#7-source-set-zone-epoch)). `Set` declares it,
  nullable and immutable. `Source`, `Object`, `Attempt`, and `Producer` carry
  a copy, because payday narrows each row by its own field 3. A reader may see
  several sites, so its sites are `SiteMember` rows, like a person's. Tenant
  admins see all sites.
- **Erasure.** A `Set` or `Source` is soft-erased (`date_erased`): its cameras
  stop being allocated for, and their recorded objects stay readable until
  retention removes them. A host is soft-erased too, and an erased host's
  certificate is refused from then on ([§33.4](10-security.md#334-joining-and-adoption)).
  An `Object` is never erased through the API. GC moves it to `DELETED`
  ([§21](06-retention-gc.md#21-lazy-gc)), and the row is pruned later
  ([§20.4](06-retention-gc.md#204-row-retention)).
- **Generated `Patch` stays closed on `Object`, `Attempt`, and the global
  entities**, and on the state and identity fields of hosts. State there
  changes only through the custom RPCs below, each of which means one thing.
  `Patch` on a host changes its alias, name, and labels.

Key fields, beyond payday's `id`, `tenant`, `alias`, and `date_*`:

```text
Tenant           capacity_share (overlay on payday's Tenant, §21.4)
Site             name, description, relay_selector (labels, §39.2)
SiteMember       site, and one of holder | reader
Set              site, epoch, set_spread, retention (expire, delete), checksum,
                 max_bitrate_total (optional cap, §12.6), auto_raise (§38.5),
                 agreed link profile (mode, timeouts, horizon), profile_version
Source           set, ordinal (assigned by Add, immutable), zone,
                 agreed segment profile (max_bitrate, segment duration,
                 keyframe interval), observed rate (recent, expected;
                 derived, §12.6), seconds at the cap and episodes (§38.5)
Producer         set, site (copied from the set), hardware_id, hostname,
                 state (pending | adopted | erased), certificate serial,
                 date_adopted, date_seen, relay (assigned, §39.2)
Reader           hardware_id, hostname, state, certificate serial,
                 date_adopted, date_seen; sites through SiteMember
Object           source, set, sink, object_key, date_started, date_ended, size,
                 site, state, incomplete, date_expired, date_deleted,
                 dates_synced (§20.3), placement_version, date_committed
Attempt          object, sink, node, state, failure_reason
Node             alias, hardware_id, hostname, state, reported interfaces and
                 IPs, certificate serial, last heartbeat, known key IDs,
                 CA bundle hash
Relay            alias, labels, hardware_id, hostname, state, reported
                 interfaces and IPs, certificate serial, last heartbeat,
                 attached producers, active sources, viewers, egress (§39)
Device           node, hardware ID, slot, health, SMART summary, failure score,
                 quarantine (state, date, reason, history)
Sink             node, device, path, capacity, free, pressure, capabilities,
                 attachment (attached | pending adoption, §28.3)
PlacementPolicy  version, parameters, active
AddressPolicy    version, resolver, resolver parameters, network rules,
                 trusted proxies, active
UploadPolicy     version, bounds and defaults of every negotiated value, active
```

### 35.4 Tenant API: custom RPCs

```proto
service SetService {
  // Proposed upload profile in, agreed profile and adjustments out (§12.6).
  // The answer also carries the producer's relay assignment (§39.2).
  rpc Negotiate(SetNegotiateRequest) returns (SetNegotiateResponse);
  // Allocations for every member of the set, up to a horizon (§12.1).
  rpc Allocate(SetAllocateRequest) returns (SetAllocateResponse);
  // Live viewing: per member, the relay's endpoints, a view token, and the
  // WHEP URL (§39.4).
  rpc Live(SetLiveRequest) returns (SetLiveResponse);
}

service SourceService {
  // The same for one source.
  rpc Live(SourceLiveRequest) returns (SourceLiveResponse);
}

service ObjectService {
  // One allocation for one segment of one source. Idempotent per
  // (source, expected date_started): asking twice answers the same object.
  rpc Allocate(ObjectAllocateRequest) returns (Allocation);
  // The next candidate after a failed attempt (§13).
  rpc Reallocate(ObjectReallocateRequest) returns (Allocation);
  // A fresh token for an attempt still in progress on the same target (§12.1).
  rpc Renew(ObjectRenewRequest) returns (Allocation);
  // One attempt failed, with a reason; feeds health (§13, §27). The object
  // stays PENDING.
  rpc ReportAttempt(ObjectReportAttemptRequest) returns (Attempt);
  // The producer gives up on an object; it becomes LOST (§13).
  rpc ReportFailure(ObjectReportFailureRequest) returns (Object);
  // Changes date_expired and/or date_deleted, for one object or in bulk by
  // set or source and a time range; a reason is required and audited (§20.3).
  rpc Reschedule(ObjectRescheduleRequest) returns (ObjectRescheduleResponse);
  // Objects and gaps over a time range, with read tokens; paged (§17, §19).
  rpc Timeline(ObjectTimelineRequest) returns (ObjectTimelineResponse);
}

service ProducerService {
  // Called without a credential by a host on its first run, and polled until
  // an admin adopts it: hardware identity, hostname, and CSR in; certificate
  // and CA bundle out once adopted (§33.4).
  rpc Join(ProducerJoinRequest) returns (ProducerJoinResponse);
  // A tenant admin accepts a pending producer and assigns its set.
  rpc Adopt(ProducerAdoptRequest) returns (Producer);
  // Called over mTLS before the certificate expires (§33.5).
  rpc RenewCertificate(ProducerRenewCertificateRequest) returns (ProducerRenewCertificateResponse);
  // Input state per source and host load, every producer_heartbeat_interval
  // (§38.6). The answer carries the current relay assignment (§39.2).
  rpc Heartbeat(ProducerHeartbeatRequest) returns (ProducerHeartbeatResponse);
  // The current relay assignment and a fresh publish token, on demand:
  // called the moment the producer's relay stream breaks (§39.2).
  rpc Relay(ProducerRelayRequest) returns (ProducerRelayResponse);
}

service ReaderService {
  rpc Join(ReaderJoinRequest) returns (ReaderJoinResponse);                  // as above
  rpc Adopt(ReaderAdoptRequest) returns (Reader);                            // with its sites
  rpc RenewCertificate(ReaderRenewCertificateRequest) returns (ReaderRenewCertificateResponse);
}
```

- `Allocation` carries `object_id`, the target sink, and the **ranked
  candidates**. Each candidate has its own `attempt_id`, the node's
  **endpoints** as the active address resolver gives them
  ([§34.10](11-deployment.md#3410-node-addresses)), and its own **access
  token**, so a producer can move to the next candidate without a round trip
  ([§13](04-write-path.md#13-retry-and-reallocation)).
- `ObjectTimelineRequest` names a set or a source and a time range, with
  `size` (at most `timeline_page`, default 1,000 objects) and `after`.
  `ObjectTimelineResponse` lists, per source, the objects of the page with
  their states and read tokens, the gaps with their reasons
  (`NOT_RECEIVED`, `IN_PROGRESS`, `LOST`, `DELETED`, `UNAVAILABLE`), and
  `next`.
- `ObjectService.Watch` filtered by a set is how a console shows segments
  arriving. Watch requires filters, so no caller watches the whole table.
- `Holder` has no custom RPCs. People sign in through payday
  ([§33.1](10-security.md#331-trust-model)).

### 35.5 Cluster API: custom RPCs

```proto
service NodeService {
  // Hardware identity, hostname, interfaces, sinks, and CSR in; polled until
  // adopted; node_id, certificate, CA bundle, and key set out (§33.4).
  rpc Join(NodeJoinRequest) returns (NodeJoinResponse);
  // An operator accepts a pending node.
  rpc Adopt(NodeAdoptRequest) returns (Node);
  rpc RenewCertificate(NodeRenewCertificateRequest) returns (NodeRenewCertificateResponse);
  // Health, capacity, and pressure of the node, its devices, and its sinks (§27).
  rpc Heartbeat(NodeHeartbeatRequest) returns (NodeHeartbeatResponse);
  // ObjectStored, ObjectDeleted, and ObjectMissing events, batched; applied
  // idempotently (§34.9).
  rpc PushEvents(NodePushEventsRequest) returns (NodePushEventsResponse);
  // What the active address resolver would hand out, for a given caller (§34.10).
  rpc Resolve(NodeResolveRequest) returns (NodeResolveResponse);
}

service SinkService {
  rpc ProposeGc(SinkProposeGcRequest) returns (SinkProposeGcResponse);   // §21.2
  rpc Adopt(SinkAdoptRequest) returns (Sink);                            // attach a moved sink (§28.3)
  rpc Retire(SinkRetireRequest) returns (Sink);
}

service RelayService {
  rpc Join(RelayJoinRequest) returns (RelayJoinResponse);                // as NodeService.Join (§33.4)
  rpc Adopt(RelayAdoptRequest) returns (Relay);
  rpc RenewCertificate(RelayRenewCertificateRequest) returns (RelayRenewCertificateResponse);
  // Attached producers, active sources, viewers, egress, load (§39.2).
  rpc Heartbeat(RelayHeartbeatRequest) returns (RelayHeartbeatResponse);
  // An operator moves a producer to a relay; the assignment stays sticky.
  rpc Assign(RelayAssignRequest) returns (Relay);
}

service DeviceService {
  rpc Quarantine(DeviceQuarantineRequest) returns (Device);               // §27
  rpc Release(DeviceReleaseRequest) returns (Device);
  rpc Retire(DeviceRetireRequest) returns (Device);
  rpc DeclareDead(DeviceDeclareDeadRequest) returns (Device);            // objects → LOST
  rpc Locate(DeviceLocateRequest) returns (Device);                      // bay LED on/off
}

service SigningKeyService {
  rpc Rotate(SigningKeyRotateRequest) returns (SigningKey);               // §33.3
}

service PlacementPolicyService {
  rpc Activate(PlacementPolicyActivateRequest) returns (PlacementPolicy); // §11
}

service UploadPolicyService {
  rpc Activate(UploadPolicyActivateRequest) returns (UploadPolicy);       // §12.6
}

service AddressPolicyService {
  rpc Activate(AddressPolicyActivateRequest) returns (AddressPolicy);     // §34.10
}
```

- Nodes **poll** `SigningKey` every 30 s; a rotation waits for every live
  node to report the new key before it signs
  ([§33.3](10-security.md#333-signing-keys-and-rotation)).
- Consoles watch `Node`, `Device`, `Sink`, `Producer`, and `Reader` for live
  state, including hosts waiting to be adopted.
- Operator actions with an effect on a node (`Quarantine`, `Retire`,
  `Locate`, GC on demand) are carried out by the CP through the node's
  control API ([§35.7](#357-storage-node-control-api)).

### 35.6 Storage Node: HTTP data plane

```text
PUT    /objects/{object_key}   Upload-Offset, Upload-Complete, Upload-Length
                               or Shale-Size-Hint, Shale-Date-Started,
                               Shale-Date-Ended; resumable, may be chunked
GET    /objects/{object_key}   Range supported
HEAD   /objects/{object_key}   upload offset and completeness, or object metadata
```

| Response | Meaning |
|---|---|
| `201` | upload complete and durable (commit) |
| `204 Upload-Offset` | request accepted; every byte below the offset is written to the device |
| `200` | the upload was already complete; `Shale-Incomplete: ?1` if the node finalized it from an abandoned live upload ([§15](04-write-path.md#15-partial-objects)) |
| `409 Upload-Offset` | the request's offset does not match the node's; resume from the offset given |
| `413` | the upload would exceed the token's `max_length`; the node finalizes what it has ([§12.5](04-write-path.md#125-idempotent-uploads)) |
| `503 Retry-After` | the sink or the caller is at its upload limit |

- Served over HTTP/1.1, HTTP/2, and **HTTP/3 (QUIC)**, with the same paths,
  headers, and semantics. HTTP/3 helps producers on lossy or wireless links:
  one lost packet does not stall other streams, and a connection survives a
  change of network.
- Every request carries a CP-signed access token, in
  `Authorization: Shale <token>` or a `token=` query parameter
  ([§33.2](10-security.md#332-access-tokens)). A put token also authorizes
  `HEAD` on its key, and so does a get token. Nodes and any proxy in front of
  them must not log the query string.
- There is no `DELETE`. Nodes delete what the CP approves or orders
  ([§21.2](06-retention-gc.md#212-protocol),
  [§35.7](#357-storage-node-control-api)), and their own housekeeping
  (abandoned buffered uploads, damaged files), which they report
  ([§34.9](11-deployment.md#349-events-and-directives)).
- Nodes also serve health and metrics endpoints, and nothing else over HTTP.

A `HEAD` on a complete object also answers `Shale-Checksum: crc32c=<hex>`
when the set asked for checksums ([§30](09-operations.md#30-integrity)).

### 35.7 Storage Node: control API

The Control Plane calls nodes. Each node serves one gRPC service on its
cluster-facing listener, over mTLS, and accepts only a peer whose certificate
chains to the cluster CA and names the Control Plane
([§33.5](10-security.md#335-tls)).

```proto
service NodeControl {
  // Unlink these objects now. Answers per key: deleted | absent.
  rpc Delete(NodeDeleteRequest) returns (NodeDeleteResponse);
  // Rewrite date_expired and date_deleted in these objects' xattrs (§20.3).
  rpc SetDates(NodeSetDatesRequest) returns (NodeSetDatesResponse);
  // Stop or resume accepting uploads on a sink: quarantine, retire, release (§27).
  rpc SetSinkState(NodeSetSinkStateRequest) returns (NodeSetSinkStateResponse);
  // Light or clear a bay LED (§27).
  rpc Locate(NodeLocateRequest) returns (NodeLocateResponse);
  // Run a GC round on a sink now (`shale gc run`).
  rpc Gc(NodeGcRequest) returns (NodeGcResponse);
  // Records of every complete file on a sink newer than `since`, streamed,
  // and which of the given DELETING keys are absent (§34.9, §29).
  rpc Reconcile(NodeReconcileRequest) returns (stream NodeReconcileResponse);
  // A new certificate chain for the node's current key (§33.5).
  rpc InstallCertificate(NodeInstallCertificateRequest) returns (NodeInstallCertificateResponse);
}
```

Every call is idempotent, and every directive is **derived from state the
CP already holds**, so nothing is lost when a node is unreachable: the CP
re-sends until the node acknowledges
([§34.9](11-deployment.md#349-events-and-directives)).

### 35.8 Relay: ingest and WHEP

A relay serves two things and nothing else ([§39](16-relay.md#39-relay)).

**Ingest**, one bidirectional gRPC stream per producer, dialed by the
producer:

```proto
service RelayIngest {
  rpc Attach(stream AttachRequest) returns (stream AttachResponse);
}
// AttachRequest:  Hello {publish_token} once, then Data {source, bytes}
//                 for every source the relay has started
// AttachResponse: Start {source} | Stop {source}
```

The publish token names the relay and the producer's sources
([§33.2](10-security.md#332-access-tokens)); the relay starts and stops
sources as viewers come and go ([§39.3](16-relay.md#393-from-the-producer)).

**WHEP**, over HTTPS, for viewers:

```text
POST   /whep/{source_id}       Content-Type: application/sdp, body: offer
                               Authorization: Shale <view token>
       → 201  Location: /whep/{session}   body: SDP answer
DELETE /whep/{session}         end the session
```

ICE servers are announced in the answer's candidates and, when configured,
as `Link` headers per the WHEP draft. There are no other endpoints besides
health and metrics.
