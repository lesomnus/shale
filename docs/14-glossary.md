# Shale — Glossary

## 37. Glossary

| Term | Meaning |
|---|---|
| **Shale** | The storage system |
| **Control Plane (CP)** | Placement, metadata index, tokens, retention, health, adoption, relay assignment; calls nodes through their control API |
| **Storage Node** | Host that stores and serves objects |
| **Relay** | Host that takes a producer's live streams on demand and fans them out to viewers over WebRTC; stateless, never touches storage |
| **Viewer** | Whoever watches live through a relay: a person in a browser, or a Reader host such as a monitoring wall |
| **Relay Assignment** | The relay a producer sends live streams to, chosen by the CP by site labels and load, sticky until the relay is down |
| **Publish Token** | Access token with `op = publish`: lets a producer attach to one relay and feed its sources |
| **View Token** | Access token with `op = view`: lets a viewer watch one source on one relay for an hour |
| **WHEP** | WebRTC HTTP egress protocol: the viewer posts an SDP offer to the relay and gets the answer |
| **GOP Cache** | The relay's copy of the current group of pictures per source, so a joining viewer starts at once |
| **Producer** | Host that receives a set's streams, cuts them into segments, and uploads them; owns a segment until commit |
| **Capture Process** | A process the producer runs per managed source, whose standard output is the source's MPEG-TS stream; ffmpeg by default |
| **Managed Capture** | The producer spawning, configuring, and supervising capture processes from its own configuration |
| **Early Cut** | Closing a segment at the next keyframe once it has produced `max_bitrate` × duration bytes ahead of its phase; the sign of a ceiling set too low |
| **Episode** | 3 to 60 consecutive seconds with a source at its ceiling; counted by the producer to tell starvation from noise |
| **Reader** | Host that reads objects, typically a media server; `shale serve reader` is the agent that holds its certificate |
| **Host** | A machine running Shale as a Storage Node, a Relay, a Producer, or a Reader; adopted by an operator, authenticated by a certificate |
| **Hardware Identity** | DMI product UUID or device-tree serial number; how a host finds its row again after a reinstall |
| **Adoption** | An operator accepting a pending host (or a moved sink), after which the CP issues its certificate |
| **Pending Host** | A host that has joined and is waiting to be adopted |
| **Holder** | A person: tenant admin, cluster operator, or console user (payday) |
| **Actor** | Whoever a request or token is from: a Holder or a host |
| **Source** | A producer of objects (a camera, or one stream of data) |
| **Set** | Sources behind one producer; powered on/off and read together; every Source is in exactly one |
| **Zone** | Optional label for Sources with overlapping fields of view |
| **Ordinal** | Stable, never-reused index of a Source within its set |
| **Stagger** | Deterministic per-camera phase of segment boundaries |
| **Pre-allocation** | Fetching allocations for segments due within `allocation_horizon` |
| **Allocation Horizon** | How far ahead a producer holds allocations; bounds write availability during a CP outage |
| **Object** | Immutable unit of storage (a video segment) |
| **Object ID** | Global identity of a logical object |
| **Attempt ID** | Identity of one write attempt of an object |
| **Object Key** | Path of an object within its sink |
| **Location** | `(sink_id, object_key)` |
| **Allocation** | Result of placement: an object and its ranked candidates, each with an attempt and a token |
| **Placement** | Choosing a sink for a new object |
| **Placement Version** | Version of the placement policy |
| **Epoch** | Time bucket during which a Source sticks to one sink |
| **Candidate Ranking** | HRW-ordered list over all sinks for a key; also the retry order; ineligible entries are skipped |
| **Eligibility** | Whether a sink may receive new writes now |
| **Sink** | A directory where a node stores objects; unit of placement and location |
| **Sink ID** | Immutable sink identity stored in the sink label |
| **Sink Label** | `.shale-sink` file at a sink's root |
| **Device** | Physical block device under one or more sinks; I/O unit and failure domain. Never a machine |
| **Device ID** | Hardware identity of a device (WWN/serial, or filesystem UUID) |
| **HDD** | Hard disk drive; in production, one device holding one sink |
| **Logical Slot** | Chassis/bay position |
| **Node ID** | Immutable node identity, assigned at adoption |
| **Part Buffer** | RAM buffer staging one part of an upload before it is written |
| **Upload Offset** | Bytes of an upload already written to the sink; where a resumed upload continues |
| **Flush Point** | The end of any upload request: the node writes what it holds and reports the offset |
| **Live Upload** | Streaming a segment to its sink while it is being recorded |
| **Buffered Upload** | Uploading a segment after it is complete |
| **Retain Policy** | How long a producer keeps uploaded bytes: until commit, or until written |
| **Incomplete Object** | A committed object finalized from an abandoned live upload; its tail is missing and its end is estimated |
| **Abandon Timeout** | Idle time, with no request open, after which an open upload is finalized (live) or deleted (buffered) |
| **Device Queue** | The single serialized job queue of a device, shared by its sinks |
| **Job** | Bounded WRITE / READ / MAINT operation |
| **DRR** | Deficit round robin; the starvation-free Device Queue scheduler |
| **Read Chunk** | Unit of a READ job |
| **Commit** | Object durable in one sink; signalled to the producer by the final `201` |
| **Event** | What a node reports to the CP: `ObjectStored`, `ObjectDeleted`, `ObjectMissing` |
| **Directive** | What the CP orders a node to do through its control API; derived from state and re-sent until acknowledged |
| **Node Control API** | The gRPC service a node serves for the CP: delete, set dates, sink state, locate, GC, reconcile, install certificate |
| **Reconciliation** | The CP asking a node for every complete file newer than the last commit it knows on a sink |
| **Duplicate** | A second stored attempt of an already committed object |
| **Orphan** | A file in a sink unknown to the index |
| **Lost Object** | An object that could not be stored, or whose file is gone |
| **date_expired** | Time from which the object may be deleted when space is needed |
| **date_deleted** | Time from which the object is deleted; read like a certificate expiry |
| **Sweep** | The node's daily proposal of every file whose xattr `date_deleted` has passed |
| **Reschedule** | Changing an object's dates, to preserve it longer or delete it sooner; audited; pushed to the node at once |
| **Row Retention** | How long rows of deleted, lost, and never-used objects are kept for readers |
| **Data Time** | `date_started`/`date_ended`: the span an object's data covers, declared by the producer |
| **Site** | A group of sets within a tenant (e.g. a building); payday's second permission axis |
| **Site Member** | A person's or a reader's membership in a site, deciding which sites it may see |
| **Capacity Forecast** | Expected incoming vs. reclaimable bytes per sink for the coming epoch |
| **Fair Share** | GC ordering that reclaims expired objects of over-share tenants first |
| **Scheduler** | The replaceable placement implementation behind one interface |
| **Address Resolver** | The replaceable policy that turns a node into the endpoint a client dials: IP, template name, or CP-managed DNS |
| **Endpoint** | Scheme, host, and port a client uses to reach a node, as a resolver hands it out |
| **Lazy GC** | Deleting expired objects only under space pressure |
| **GC Proposal** | Node's list of deletion candidates for CP approval, named by location |
| **Watermark** | Free-space threshold between pressure states |
| **Reader Abort** | Terminating a stalled read session to free space |
| **Gap** | A time span with no available object, with a reason |
| **NOT_RECEIVED** | Gap reason: no upload was ever attempted for the span |
| **IN_PROGRESS** | Gap reason: an attempt is open for the span right now |
| **Unavailable** | Object may exist but cannot be reached now |
| **Quarantine** | Device/node excluded from new writes due to failures |
| **Failure Score** | Decaying failure count driving quarantine |
| **Re-homing** | Moving a device (HDD) to another node without copying data; the sink is adopted by the new node |
| **Failure Domain** | Unit that fails together (device, node, chassis) |
| **Blast Radius** | Set of sources and time spans affected by a failure |
| **Access Token** | CP-signed (Ed25519) grant for one operation at one host: put or get on one object at a node, publish or view on a relay; carries the actor; a presigned URL carries it |
| **Key Set** | The CP's public signing keys (`SigningKey` rows), watched and cached by nodes |
| **KEK** | Key-encryption key that wraps the private signing keys stored in the DB |
| **Principal** | An authenticated identity: a person, a host, or the CP |
| **Tenant** | Owner of sets, sources, objects, people, producers, and readers; one per organization, and a single-organization cluster has exactly one |
| **Wall** | payday's tenant boundary: a caller sees and changes only its own tenant's rows |
| **Tenant API** | gRPC surface for producers, readers, and tenant admins, behind the wall (`shale serve control`) |
| **Cluster API** | internal gRPC surface for Storage Nodes and cluster operators, spanning tenants (`shale serve cluster`) |
| **Global Entity** | An entity outside the wall, owned by the cluster: Node, Relay, Device, Sink, SigningKey, PlacementPolicy, UploadPolicy, AddressPolicy |
| **Upload Profile** | The upload parameters agreed for a set and its sources: `max_bitrate` and segment duration per source; mode, timeouts, horizon per set |
| **Upload Policy** | Cluster-wide bounds and defaults for negotiated upload profiles |
| **Max Bitrate** | The declared ceiling of all streams in a source's segments; the basis for every limit |
| **Observed Rate** | What a source actually writes, learned from its commits; the basis for every estimate |
| **Capped VBR** | Variable-bitrate encoding with a ceiling: quality stays even, the rate varies up to `max_bitrate` |
| **Negotiation** | A producer proposing its upload profile and the CP clamping it into bounds |
| **Slug** | Human-written name of a row, `@TENANT/ALIAS#DOMAIN`; the tenant is implied in a single-organization cluster |
| **Domain** | The byte in a UUIDv8 identifier naming the entity kind |
| **Watch** | Streaming RPC: a snapshot, then the current state of each changed row |
| **Host Certificate** | CA-issued certificate a host uses for mTLS; a node's is also its TLS server certificate |
| **Built-in CA** | Certificate authority run by the CP; rolled over automatically; replaceable by external certificates |
