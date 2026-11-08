# Central camera authority

The tenant CP API stores camera inventory, immutable logical Source bindings,
assignments and resumable job/publication state. This is the authority layer of
[#193](https://github.com/lesomnus/shale/issues/193), implemented by
[#194](https://github.com/lesomnus/shale/issues/194). It does not itself discover
cameras, initialize them, change a Producer's capture processes, store passwords,
or implement a DHCP server.

The existing local provisioner and manually configured USB/RTSP Producers keep
working. Adding CP inventory or publishing a desired specification creates
control state, not running capture. CP-backed credentials/presets, adopted
Workers, SourceProvider consumers, dynamic capture and combined edge execution
are separate follow-ups #195–#202.

## Authority and scope

Every CameraNetwork, CameraDevice, CameraAssignment, CameraBinding and CameraJob
has immutable tenant/site scope at the usual Payday fields 2 and 3. A network's
site must match its assigned Set. A binding derives scope from its assignment;
its Source remains in that same immutable Set. Device IPs and observed MAC/model
values never grant ownership, select credentials, or move a logical Source.

Camera services are mounted on the tenant API only. They are not ordinary viewer
reads. With roster enabled, operators need grants for the relevant custom
methods or services. Existing Producer/Reader allowlists and ordinary viewer
permissions are unchanged. With roster operator authorization disabled, the
existing tenant-person administration policy is retained; this feature does not
invent a new implicit administrator identity.

Generated Get/List/Watch methods provide scoped observation. Generated
Add/Patch/Apply/Erase are refused even for operators: they could bypass Source
reservation, ownership, job fences and immutable-input checks. The custom
operations below are the only external write path. They call the normal
transactional/recorded/scoped layers; direct Ent reads are limited by already
resolved tenant/site-scoped parent IDs.

`worker_slot` is an administrator-selected routing label. It is not a machine
identity or a permission to claim work. `worker_id` identifies an explicitly
adopted CameraWorker in the exact tenant/site. Arbitrary UUIDs and unapproved
machines are rejected. Job Claim/Report/Publish allow an operator or the current
assigned Worker; the lease owner is the authenticated actor, never a
request-supplied ID. See [the outbound Worker contract](camera-worker.md).

## Models and operations

| Entity | Custom operations | Meaning |
| --- | --- | --- |
| CameraNetwork | Configure | Explicit canonical CIDRs, seed addresses, multicast intent and local interface hint |
| CameraDevice | Claim, Observe | Stable inventory key in one network, separate authenticated/untrusted identity evidence and mutable endpoints |
| CameraAssignment | Assign, Snapshot, Handover | One network, one immutable Set, its approved Producer, execution slot and aggregate catalog revision |
| CameraBinding | Bind, SetCollect | Stable logical role, claimed device and permanent Source identity; desired versus observed/applied state |
| CameraJob | Create, Claim, Report, Publish | Pinned inputs, durable idempotency, physical-device writer lease, verified results and publication |

Creation of an assignment requires an existing adopted Producer for the target
Set. The empty-Producer runtime bootstrap is #199; this API does not require or
automatically create a synthetic source.

Claiming the same `(network, device_key)` returns the same inventory row. The
key is an explicit inventory claim, not the current IP; DHCP updates use Observe.
Claim is not proof that advertised serial/MAC values are globally unique.

Bind reserves the logical key and Source centrally. It generates a stable alias
when none is provided, reuses a matching existing provisioning binding, or imports
an explicitly referenced manual Source. An existing alias in another Set or
with a different binding is a conflict, not permission to adopt that camera.
Bind preserves Source ID/history and adds the logical/physical binding labels.
Use the existing provisioning binding/device keys when importing such a Source;
a conflicting established label cannot be silently replaced.

Configure/Observe/assignment updates and SetCollect use the read row's
`date_updated` as `expected_version`. A stale or absent precondition is Aborted. Custom write responses are read back
inside the transaction so their version timestamps use the database's persisted
precision, including PostgreSQL microseconds.
Network edits cannot exclude existing device endpoints or published capture
inputs. Snapshot also validates published inputs against the current network;
a failed validation or CP read is an error, never a successful empty catalog.

## Durable jobs and fencing

Create binds a tenant-scoped `request_key` to canonical immutable inputs. An
identical retry returns the same job; reuse for another binding/input conflicts.
Policy/preset and credential fields are versioned references, not mutable names
or secret material. This layer validates their shape and purpose; it does not
claim a reference has been resolved or a central secret has been prepared. #196
must make the intended initial administrator credential durably recoverable
before an execution worker is allowed to change a camera password.

Claim requires an operation key, expected job version and a 1–300 second lease.
It records the actual actor, current assignment generation, deadline and a
physical-device-wide monotonic lease generation. A different job or executor
cannot claim an actively leased camera. Repeating the same last operation key
and payload returns the prior result without issuing a new lease or publication;
a different payload or owner conflicts. Replayed Claims additionally revalidate
current assignment/device generation, ownership and expiry: an old Claim is not
a way to obtain a revoked or expired execution grant. Older writes with stale
versions are rejected and must resynchronize rather than being replayed as new writes.

Report and Publish check the actor, unexpired lease, physical-device owner and
both generations in the same transaction. Moving an execution slot or handing
over the Producer increments assignment generation and fences old reports.
After expiry/replacement, a new Claim can resume the same pinned job; applying or
verifying progress is retained. A camera call that timed out may have succeeded:
reinspect with the prepared exact credential before another write. CP leases do
not fence firmware or terminate an already in-flight HTTP operation.

Terminal jobs and idempotency records cannot be erased through generated CRUD.
A physical-device slot may be claimed by a different job only after the previous
job is explicitly settled/terminal. Expiry alone does not prove a camera write
stopped: resume and reconcile the same pinned job first. SetCollect(false) cancels outstanding jobs for that binding so an old publication
cannot silently reenable collection. Stopping collection never erases the Source
or its recordings.

## Publication, snapshots and readiness

Publish requires a verifying job, bounded independently decoded media evidence,
effective settings matching the proposed capture profile, and the pinned
streaming credential reference. It validates a credential-free RTSP endpoint in
the assigned network and reserves the binding/Source IDs, alias and labels.
Source metadata labels are synchronized in the same transaction, preserving
unrelated labels, so normal Producer registration can validate the specification.
Explicit nonempty names/zones are copied; profile negotiation remains the
Producer/Set negotiation path. Managed specifications exclude commands, vendor clients and plaintext secrets.
Unknown nested wire fields, userinfo and authentication query parameters are
rejected instead of being persisted as an opaque JSON side channel. URI queries
are limited to noncredential stream selectors.

The transaction updates binding desired state, job deployment-ready state and
one aggregate assignment `catalog_revision`. The monotonically increasing
counter covers every binding/spec/membership publication; it is not the maximum
of several row timestamps. Nested Source creation joins the same transaction, including when an external
Payday Batch has rebound the server. A later Binding/job failure rolls back the
Source and its recorded changes as well; no orphan Source is committed.
The acknowledgement is returned only after commit.
Combined execution may then update its memory catalog. It must rebuild from CP
if a crash happens before or after that notification; memory is not the only
copy. This publication API does not send a localhost callback or restart a
Producer.

Snapshot returns one consistent revision and all collecting Source specifications.
Assignment Watch notifications tell consumers to re-fetch it; consumers must
resnapshot after reconnect/drop rather than assume notifications are a durable
complete event log. A successful empty snapshot is distinct from failure.

`DEPLOYMENT_READY` is not `ACTIVE`. Active reporting requires all of:

- The exact currently published revision acknowledged as applied.
- Independent decoded media and advancing decoded live-frame observations after
  publication; a Live URL or a packet counter alone is insufficient evidence.
- A fresh assigned Producer heartbeat showing that Source ingesting.
- A new complete committed lamina whose start and commit are after publication.

For #194, applied/live observations are typed acknowledgements from an authorized
operator. They are not independently obtained Producer-runtime acknowledgements.
#198/#199 must correlate the actual Producer-applied binding/catalog revision
(for example through SourceReport/status) rather than treat an already-running
imported stream as proof of a new specification. #202 exercises actual WHEP/WebRTC
frame progression and independently playable committed output. Zero GOP interval
means unknown; #194 does not invent a random-access cadence or close #190.

## Approved Producer replacement

Handover requires the assignment version/generation and a separately joined,
pending, unassigned replacement Producer from the same tenant. It atomically
soft-erases the old Producer, adopts the replacement into the existing Set and
advances assignment/catalog generation. The replacement gets its own key and
certificate; no old private key is copied. Existing Source IDs, immutable
Tenant/Site/Set and archives remain untouched.

Revocation stops subsequent CP operations by the old identity and old job
reports. Already-issued Storage/Relay capabilities and existing media sessions
retain their own expiry/drain semantics. This operation does not claim instant
data-plane termination. Coordinated handover must account for that bounded
remaining authority before claiming exclusive media production.

## Adoption of existing installations

An operator can create a camera network, claim inventory, assign the existing
adopted Producer and explicitly bind its existing Sources. This does not rewrite
local capture YAML, copy a private key, rotate a camera password, import plaintext
credential URLs, restart recording or expose DHCP on Wi-Fi. Preserve local
configuration until the CP-backed runtime is installed and verified.

Central credential import/encryption and preset migration belong to #196/#197;
local file-provider data is not automatically mirrored into inventory or capture
specifications. The following edge runtime/deployment work controls service
startup, applied-state caches, finite RAM buffering and isolated-interface DHCP.
A durable video spool and general offline cold-start recording are not part of
this authority layer.
