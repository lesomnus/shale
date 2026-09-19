# Shale — API

## 35. API

The Control Plane speaks **resource-oriented gRPC**, built with
[payday](https://github.com/lesomnus/payday). Storage Nodes speak **HTTP**
(HTTP/1.1, HTTP/2, and HTTP/3 over QUIC), and only to move object bytes.

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

### 35.2 Two API surfaces

The Control Plane serves two gRPC surfaces from **separate entry points**,
following payday's advice that a path able to see every tenant must not exist
in the public server at all
([§34.1](11-deployment.md#341-one-binary-four-roles)).

| Surface | Entry point | Callers | Tenant wall | Exposure |
|---|---|---|---|---|
| **Tenant API** | `shale control` | producers, readers, tenant admins | **on**: a caller sees only its own tenant | may face the internet |
| **Cluster API** | `shale cluster` | Storage Nodes, cluster operators | spans all tenants; writes global entities | **internal network only** |

No flag mounts cluster services into `shale control`. A single-machine
`shale all` serves both, on separate listeners, with the cluster listener on
localhost by default.

### 35.3 Entities

| Entity | Tenancy | Domain | Generated verbs | Surface |
|---|---|---|---|---|
| `Tenant` (payday) | is the tenant | 1 | Add, Get, List, Erase | cluster |
| `Holder` (payday) | tenant | 2 | Add, Get, Patch, List, Erase | tenant |
| `Set` | tenant | 7 | Add, Get, Patch, Erase, List, Watch | tenant |
| `Source` | tenant | 8 | Add, Get, Patch, Erase, List, Watch | tenant |
| `Object` | tenant | 9 | Get, List, Watch | tenant (all tenants on cluster) |
| `Attempt` | tenant | 10 | Get, List | tenant |
| `EnrollmentToken` | tenant | 11 | Add, List, Erase | tenant |
| `Site` | tenant | 19 | Add, Get, Patch, Erase, List, Watch | tenant |
| `SiteMember` | tenant | 20 | Add, List, Erase | tenant |
| `Node` | **global** | 12 | Get, List, Watch | cluster |
| `Device` | **global** | 13 | Get, List, Watch | cluster |
| `Sink` | **global** | 14 | Get, List, Watch | cluster |
| `SigningKey` | **global** | 15 | List, Watch | cluster |
| `PlacementPolicy` | **global** | 16 | Add, Get, List | cluster |
| `JoinToken` | **global** | 17 | Add, List, Erase | cluster |
| `UploadPolicy` | **global** | 18 | Add, Get, List | cluster |
| `AddressPolicy` | **global** | 21 | Add, Get, List | cluster |

- **Tenant-owned** is payday's default. Every entity outside the wall says
  `global: {}` explicitly, and a search for it lists exactly the eight above.
  Storage is physically shared, so the infrastructure that holds it belongs to
  the cluster, not to any tenant.
- **Holders** are the tenant's actors: producers, readers, and tenant admins,
  distinguished by role. Storage Nodes are **not** holders. They authenticate
  to the cluster API by certificate
  ([§33.1](10-security.md#331-trust-model)).
- **Field 3 is `site`**, payday's second permission axis
  ([§7](02-data-model.md#7-source-set-zone-epoch)). `Set` declares it,
  nullable and immutable. `Source`, `Object`, `Attempt`, and `EnrollmentToken`
  carry a copy, because payday narrows each row by its own field 3.
  `SiteMember` links holders to sites and answers "which sites may this caller
  see". Tenant admins see all sites.
- **Erasure.** A `Set` or `Source` is soft-erased (`date_erased`): its cameras
  stop being allocated for, and their recorded objects stay readable until
  retention removes them. An `Object` is never erased through the API. GC
  moves it to `DELETED` ([§21](06-retention-gc.md#21-lazy-gc)), and the row
  stays so readers can report the reason for a gap.
- **Generated `Patch` stays closed on `Object`, `Attempt`, and the global
  entities.** State there changes only through the custom RPCs below, each of
  which means one thing.

Key fields, beyond payday's `id`, `tenant`, `alias`, and `date_*`:

```text
Tenant           capacity_share (overlay on payday's Tenant, §21.4)
Site             name, description
SiteMember       holder, site
Set              site, epoch, set_spread, retention (expire, delete), checksum,
                 agreed link profile (mode, timeouts, horizon), profile_version
Source           set, ordinal (assigned by Add, immutable), zone,
                 agreed segment profile (bitrate, segment duration, object size)
Object           source, set, sink, object_key, date_started, date_ended, size,
                 site, state, incomplete, date_expired, date_deleted,
                 placement_version
Attempt          object, sink, state, failure_reason
Node             alias, reported interfaces and IPs, state, last heartbeat,
                 known key IDs
Device           node, hardware ID, slot, health, SMART summary, failure score,
                 quarantine (state, date, reason, history)
Sink             node, device, path, capacity, free, pressure, capabilities
PlacementPolicy  version, parameters, active
AddressPolicy    version, resolver, resolver parameters, network rules, active
UploadPolicy     version, bounds and defaults of every negotiated value, active
```

### 35.4 Tenant API: custom RPCs

```proto
service SetService {
  // Proposed upload profile in, agreed profile and adjustments out (§12.6).
  rpc Negotiate(SetNegotiateRequest) returns (SetNegotiateResponse);
  // Allocations for every member of the set, up to a horizon (§12.1).
  rpc Allocate(SetAllocateRequest) returns (SetAllocateResponse);
}

service ObjectService {
  // One allocation for one segment of one source.
  rpc Allocate(ObjectAllocateRequest) returns (Allocation);
  // The next candidate after a failed attempt (§13).
  rpc Reallocate(ObjectReallocateRequest) returns (Allocation);
  // A Writer gives up on an object; it becomes LOST (§13).
  rpc ReportFailure(ObjectReportFailureRequest) returns (Object);
  // Changes date_expired and/or date_deleted, for one object or in bulk by
  // set or source and a time range; a reason is required and audited (§20.3).
  rpc Reschedule(ObjectRescheduleRequest) returns (ObjectRescheduleResponse);
  // Objects and gaps over a time range, with read tokens (§17, §19).
  rpc Timeline(ObjectTimelineRequest) returns (ObjectTimelineResponse);
}

service HolderService {
  // Called without a credential: exchanges an enrollment token for one (§33.4).
  rpc Enroll(HolderEnrollRequest) returns (HolderEnrollResponse);
}
```

- `Allocation` carries `object_id`, `attempt_id`, the target sink, the node's
  **endpoints** as the active address resolver gives them
  ([§34.10](11-deployment.md#3410-node-addresses)), the **access token**, and
  the next few ranked candidates.
- `ObjectTimelineRequest` names a set or a source and a time range.
  `ObjectTimelineResponse` lists, per source, the objects with their states
  and read tokens, and the gaps with their reasons
  (`NOT_RECEIVED`, `LOST`, `DELETED`, `UNAVAILABLE`).
- `ObjectService.Watch` filtered by a set is how a console shows segments
  arriving. Watch requires filters, so no caller watches the whole table.

### 35.5 Cluster API: custom RPCs

```proto
service NodeService {
  // Join token + CSR → node_id, node certificate, CA bundle, key set (§33.4).
  rpc Join(NodeJoinRequest) returns (NodeJoinResponse);
  rpc RenewCertificate(NodeRenewCertificateRequest) returns (NodeRenewCertificateResponse);
  // Health, capacity, and pressure of the node, its devices, and its sinks (§27).
  rpc Heartbeat(NodeHeartbeatRequest) returns (NodeHeartbeatResponse);
  // ObjectStored and deletion events, batched; applied idempotently (§34.9).
  rpc PushEvents(NodePushEventsRequest) returns (NodePushEventsResponse);
  // What the active address resolver would hand out, for a given caller (§34.10).
  rpc Resolve(NodeResolveRequest) returns (NodeResolveResponse);
}

service SinkService {
  rpc ProposeGc(SinkProposeGcRequest) returns (SinkProposeGcResponse);   // §21.2
  rpc ReportDeleted(SinkReportDeletedRequest) returns (SinkReportDeletedResponse);
  rpc Retire(SinkRetireRequest) returns (Sink);
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

- Nodes **watch** `SigningKey` instead of polling for keys
  ([§33.3](10-security.md#333-signing-keys-and-rotation)).
- Consoles watch `Node`, `Device`, and `Sink` for live cluster state.
- The CP never calls a node. Every cluster RPC is initiated by a node or an
  operator.

### 35.6 Storage Node: HTTP data plane

```text
PUT    /objects/{object_key}   Upload-Offset, Upload-Complete, Upload-Length
                               or Shale-Size-Hint; resumable, may be chunked
GET    /objects/{object_key}   Range supported
HEAD   /objects/{object_key}   upload offset and completeness, or object metadata
```

- Served over HTTP/1.1, HTTP/2, and **HTTP/3 (QUIC)**, with the same paths,
  headers, and semantics. HTTP/3 helps producers on lossy or wireless links:
  one lost packet does not stall other streams, and a connection survives a
  change of network.
- Every request carries a CP-signed access token, in
  `Authorization: Shale <token>` or a `token=` query parameter
  ([§33.2](10-security.md#332-access-tokens)).
- There is no `DELETE`. Nodes delete only what the CP approves through
  `SinkService.ProposeGc`.
- Nodes also serve health and metrics endpoints, and nothing else.
