# Shale — Glossary

## 37. Glossary

| Term | Meaning |
|---|---|
| **Shale** | The storage system |
| **Control Plane (CP)** | Placement, metadata index, URLs, retention, health |
| **Storage Node** | Server that stores and serves objects |
| **Writer** | Client that produces and uploads objects; owns a segment until commit |
| **Reader** | Client that reads objects (typically a media server) |
| **Source** | A producer of objects (a camera) |
| **Set** | Sources behind one producer; powered on/off and read together |
| **Zone** | Optional label for Sources with overlapping fields of view |
| **Ordinal** | Stable, never-reused index of a Source within its set |
| **Stagger** | Deterministic per-camera phase of segment boundaries |
| **Pre-allocation** | Fetching allocations for segments due within `allocation_horizon` |
| **Allocation Horizon** | How far ahead a Writer holds allocations; bounds write availability during a CP outage |
| **Object** | Immutable unit of storage (a video segment) |
| **Object ID** | Global identity of a logical object |
| **Attempt ID** | Identity of one write attempt of an object |
| **Object Key** | Path of an object within its sink |
| **Location** | `(sink_id, object_key)` |
| **Allocation** | Result of placement: target sink and node for an attempt |
| **Placement** | Choosing a sink for a new object |
| **Placement Version** | Version of the placement policy |
| **Epoch** | Time bucket during which a Source sticks to one sink |
| **Candidate Ranking** | HRW-ordered list of sinks for a key; also the retry order |
| **Eligibility** | Whether a sink may receive new writes now |
| **Sink** | A directory where a node stores objects; unit of placement and location |
| **Sink ID** | Immutable sink identity stored in the sink label |
| **Sink Label** | `.shale-sink` file at a sink's root |
| **Device** | Physical block device under one or more sinks; I/O unit and failure domain |
| **Device ID** | Hardware identity of a device (WWN/serial, or filesystem UUID) |
| **HDD** | Hard disk drive; in production, one device holding one sink |
| **Logical Slot** | Chassis/bay position |
| **Node ID** | Immutable node identity |
| **Part Buffer** | RAM buffer staging one part of an upload before it is written |
| **Upload Offset** | Bytes of an upload already written to the sink; where a resumed upload continues |
| **Live Upload** | Streaming a segment to its sink while it is being recorded |
| **Buffered Upload** | Uploading a segment after it is complete |
| **Retain Policy** | How long a Writer keeps uploaded bytes: until commit, or until written |
| **Incomplete Object** | A committed object finalized from an abandoned live upload; its tail is missing |
| **Abandon Timeout** | Idle time after which an open upload is finalized (live) or deleted (buffered) |
| **Device Queue** | The single serialized job queue of a device, shared by its sinks |
| **Job** | Bounded WRITE / READ / MAINT operation |
| **DRR** | Deficit round robin; the starvation-free Device Queue scheduler |
| **Read Chunk** | Unit of a READ job |
| **Commit** | Object durable in one sink; signalled to the Writer by the final `201` |
| **Commit Event** | `ObjectStored`, updates the index |
| **Duplicate** | A second stored attempt of an already committed object |
| **Orphan** | A file in a sink unknown to the index |
| **Lost Object** | An object that could not be stored, or whose file is gone |
| **expires_at** | Time after which the object may be deleted when space is needed |
| **must_delete_by** | Time by which the object must be deleted |
| **Hold** | Flag that forbids deletion |
| **Lazy GC** | Deleting expired objects only under space pressure |
| **GC Proposal** | Node's list of deletion candidates for CP approval |
| **Watermark** | Free-space threshold between pressure states |
| **Reader Abort** | Terminating a stalled read session to free space |
| **Gap** | A time span with no available object, with a reason |
| **NOT_RECEIVED** | Gap reason: no upload was ever attempted for the span |
| **Unavailable** | Object may exist but cannot be reached now |
| **Quarantine** | Device/node excluded from new writes due to failures |
| **Failure Score** | Decaying failure count driving quarantine |
| **Re-homing** | Moving a device (HDD) to another node without copying data |
| **Failure Domain** | Unit that fails together (device, node, chassis) |
| **Blast Radius** | Set of sources and time spans affected by a failure |
| **Access Token** | CP-signed (Ed25519) grant for one operation on one object at one node; a presigned URL carries it |
| **Key Set** | The CP's public signing keys (`SigningKey` rows), watched and cached by nodes |
| **Principal** | An authenticated identity: a holder, a Storage Node, a cluster operator, or the CP |
| **Tenant** | Owner of sets, sources, objects, and holders; one per organization, and a single-organization cluster has exactly one |
| **Holder** | An actor of a tenant: producer, reader, or tenant admin (payday) |
| **Wall** | payday's tenant boundary: a caller sees and changes only its own tenant's rows |
| **Tenant API** | gRPC surface for holders, behind the wall (`shale control`) |
| **Cluster API** | internal gRPC surface for Storage Nodes and cluster operators, spanning tenants (`shale cluster`) |
| **Global Entity** | An entity outside the wall, owned by the cluster: Node, Device, Sink, SigningKey, PlacementPolicy, UploadPolicy, JoinToken |
| **Upload Profile** | The upload parameters agreed for a set and its sources: object size, mode, timeouts, horizon |
| **Upload Policy** | Cluster-wide bounds and defaults for negotiated upload profiles |
| **Negotiation** | A producer proposing its upload profile and the CP clamping it into bounds |
| **Slug** | Human-written name of a row, `@TENANT/ALIAS#DOMAIN`; the tenant is implied in a single-organization cluster |
| **Domain** | The byte in a UUIDv8 identifier naming the entity kind |
| **Watch** | Streaming RPC: a snapshot, then the current state of each changed row |
| **Join Token** | One-time token with which a Storage Node joins the cluster |
| **Enrollment Token** | One-time token a producer or reader exchanges for a credential |
| **Credential** | Long-lived bearer token of a holder or cluster operator; stored hashed by the CP |
| **Node Certificate** | CA-issued certificate a node uses as TLS server and as mTLS client to the cluster API |
| **Built-in CA** | Certificate authority run by the CP; replaceable by external certificates |
