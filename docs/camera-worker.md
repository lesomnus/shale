# Outbound camera Worker

`shale serve camera-worker` is a separate machine role. It discovers its approved
work through the tenant CP API, keeps its own private key and client-only mTLS
certificate, and opens no incoming listener. It can run independently of a
Producer. The CLI installs the CP-backed discovery and execution adapters; the
[managed edge](managed-edge.md) can also compose it with a separately approved
Producer in one process.

```yaml
state: /var/lib/shale
cp: https://cp.example:7400
tenant: company
ca_hash: sha256:<control-plane-CA-fingerprint>
camera_worker:
  heartbeat_interval: 10s
  retry_interval: 1s
  lease_seconds: 30
```

The role stores its identity beneath `state/camera-worker`. Do not reuse a
Producer's state directory or certificate. No human session, API bearer token or
local plaintext credential provider is silently substituted. The normal host CA
pinning behavior applies; configuring `ca_hash` verifies first contact.

Run the Worker, compare its printed hardware/key fingerprint with the pending CP
row, then approve it using `shale camera-worker adopt @<alias>` (or the
`CameraWorkerService.Adopt` RPC). Adopt can select a site. That approved site is
fixed for this identity; a site-less Worker can serve only site-less assignments.
Use a replacement identity for another site. A new key needs explicit operator
adoption. RenewCertificate verifies a signed CSR for the already-approved key,
replaces its serial and rejects the old certificate even on an open connection.
After an offline certificate expires, the explicit public bootstrap path omits
that expired client certificate only for Join. A signed CSR for the same approved
key can recover a fresh certificate; a valid approved certificate is returned
without rotating its serial. The approved certificate is stored separately from
a pending new-key Join, so an anonymous new request cannot block key recovery.
Revoked/erased identities receive no automatic readoption. Ordinary machine RPCs
retain normal mTLS checks and reject expired or replaced certificates.
An Unauthenticated session result also enters that public same-key recovery
path, so a lost renewal response can recover the CP's already-issued certificate.
Generated Add/Patch/Erase are closed. Operators revoke with
`CameraWorkerService.Revoke` and its expected version.

Assign the adopted Worker ID through `CameraAssignmentService.Assign`, together
with the intended network, Producer and routing label. The CP verifies adoption
and exact tenant/site equality. `worker_slot` remains an operator label, not an
identity. Changing the Worker increments the assignment generation. Producer
handover clears the old Worker ID; an operator must explicitly assign the new
execution location.

## Authority and polling

`CameraWorkerService.Poll` is a bounded outbound long poll (at most 15 seconds).
Its opaque SHA-256 revision covers the full consistent typed assignment/job view;
after a timeout it returns that current view again. Responses are limited to 100
assignments and 500 unfinished jobs. It exposes only assignments whose current
`worker_id` matches the authenticated Worker. An unassigned Worker receives an
empty view. No generic Source, Set, Lamina, inventory, or credential read API is
opened. Producer permissions are unchanged.

A Worker can call its heartbeat, renewal, Poll, CheckJob, ObserveJob,
assignment-scoped policy Observe, job Claim/Report/Publish, and exact job-scoped
preset and credential resolution. Generic configuration, inventory registration,
assignment, binding, handover and revocation remain operator operations. Each
execution call checks the current adopted identity/certificate serial and exact
assignment. Job grants additionally check the assignment generation, lease owner,
lease generation/expiry and physical-device fence. Replayed machine write
acknowledgments must still match that fence. Poll checks identity again while
waiting and before returning a view.

The grouped database wall cannot represent exactly `site = NULL` with its
all-sites flag disabled. Only these explicit Worker execution methods use a
private transaction with the normal write recorders. That boundary checks the
complete assignment/network/Set/Producer and job/binding/device scope graph
before a response or mutation. Operators keep their normal wall, and a Worker
never receives an all-sites flag or access to generic private-store methods.

`CheckJob` returns the complete typed pinned Work envelope. `ObserveJob` adds only
that leased job's assigned Producer status, complete committed recording evidence
since publication, and the bound Source live capability when available. That view
token expires no later than the current job lease or normal viewer-token TTL.
An unavailable relay produces no live capability; it does not make a job ACTIVE.
Actual decoded frames, fresh input evidence, recording and applied-revision checks
remain required by the CP's ACTIVE transition. The managed edge wires current
Producer applied acknowledgments and the native H264/WebRTC decoder observer.

## Execution and recovery seam

`internal/cameraworker.Executor` receives `Execution`, which retains the complete
CP Work checkpoint. `Snapshot` is detached state, not an execution grant. Before
each camera operation an executor uses `Step`, which calls CheckJob and bounds
the device call by the current lease. Runtime session cancellation also cancels a
Step even when a caller supplies another context. `Checkpoint` persists typed job
progress before a later device write; `Publish` and `Observe` are job scoped.
Stable operation keys must be retained when retrying uncertain responses.

The runtime renews the lease, polls assignments and heartbeats concurrently.
Identity loss, CP failure, lease/fence rejection or assignment replacement cancels
ongoing execution. It runs one local executor at a time. A reconnect/restart
resumes the same unfinished CP job and pinned inputs; it never creates a new job
or replaces an unsettled physical-camera intent. An executor which yields WAITING
is retried after a reconnect/restart, rather than continuously reclaiming its job.
Each fresh job list is visited after the last attempted job, including across
session reconnects. A repeatedly failing camera therefore leaves other assigned
cameras a turn; Claim, current CheckJob and physical-device fences still precede
every execution. The cursor retains only one job ID and forgets removed work.

The bare `cameraworker.New` library with no injected executor reports WAITING with
`central_credentials_pending`. It performs no camera initialization, password
change, local FileProvider fallback or Source publication. The central
[credential provider](camera-credentials.md) is available through
`Execution.Credentials()`. The managed CLI composition injects the policy/preset,
scoped drivers, independent media verifier and CP publisher/observer adapters.
Other embeddings select their executor explicitly. The seam retains the complete
typed job and recoverable progress without a lossy conversion to a local Run.

A CP lease cannot recall a camera firmware write already sent over the LAN. After
an uncertain mutation, a later executor must reconcile this same pinned job and
recoverable credentials before another write. Previously issued data-plane
capabilities last until their bounded expiry; already-started streams are not
instantly revoked. This role does not claim immediate data-plane revocation.

## Optional outbound discovery

The Worker library enables LAN discovery only when its composition root injects
`Config.Discoverer` or `Config.InitialObservations`. It uses the adopted Worker's
existing outbound CP connection; discovery adds no listener, credential provider,
policy file or Producer authority.
The interval defaults to 30 seconds (one second to one hour), and the hard scan
timeout defaults to two seconds (one millisecond to 30 seconds). The discoverer's
observation window is 90% of that timeout, leaving time to complete before the
hard context deadline. Each discovery Poll and policy Observe RPC has its own
ten-second deadline.

Each LAN call follows a fresh nonblocking Poll for that assignment. The current
network supplies its interface hint, multicast choice, explicit seeds and allowed
prefixes. Removed or replaced assignments cannot use a historical snapshot to
start another scan. Seeds and returned endpoints must be credential-free IP
literals within those prefixes. A scan returning more than 256 observations is
rejected. Observe carries the generation used for the scan, and the CP rechecks
current assignment and identity authority before accepting it.

Only a nonzero UUID EPR (`urn:uuid:...`) or a canonical unicast six-byte MAC within
the assigned network supplies a rediscovery hint. Inventory IDs, CandidateID,
endpoint URLs and IP addresses never supply that key; address-only seeds are
skipped. If one scan associates the same hint with different candidate IDs or
transport hosts, that whole key is skipped with a fixed ambiguity warning.
Duplicate packets for one candidate and host can merge endpoints. These hints
never authenticate a physical identity: discovery authentication flags are
cleared, and the CP preserves previously authenticated inventory evidence. After
a DHCP address change, leased inspection must confirm the authenticated physical
identity; an address hint never grants inventory ownership or credential access.

Discovery runs independently of heartbeat, job polling and lease renewal. Errors
produce fixed messages without device text and retry at the next interval; they
do not cancel an executing job. Session cancellation and certificate renewal
cancel and join discovery cleanup before closing the old CP connection. Injected
discoverers must honor context cancellation.

### Initial observations before ONVIF activation

Some factory cameras do not answer WS-Discovery until their first password is
set. The portable `camera.InitialObservationProvider` seam can supply an initial
candidate without making that candidate an authenticated device. The managed
Worker composes this provider with its regular discoverer. All observations
still pass the current assignment generation, endpoint prefixes, central
enrollment policy and credential preparation checks.

On Linux, opt in to the read-only neighbor-cache provider with:

```yaml
camera_worker:
  initial_neighbors: true
```

Configure the camera network in CP with an exact `interface_hint` and explicit
seed endpoint IPs. The provider runs the ordinary unprivileged iproute2 command
`ip -j neighbor show dev <interface>`; it sends no probes, reads no DHCP lease
files and needs no additional capabilities. It considers only those seed hosts,
at most 32 seeds and 1024 cache entries. Missing, failed, incomplete, multicast
or conflicting mappings provide no candidate. Another normal network operation
must populate a missing kernel cache entry; an IP seed alone remains insufficient.
Other embeddings may inject a bounded DHCP or vendor observation implementation
through the same seam.

An initial canonical MAC observation reuses an explicitly registered
`mac/<MAC>` inventory key in that same network. A later EPR at the same endpoint
can be recorded as discovery evidence while retaining the inventory, binding
and Source. A known EPR and MAC that identify different rows are rejected rather
than merged. Existing manual logical bindings are reused only for the exact
same assignment and device; conflicting bindings require operator reconciliation.
CP checks one locked snapshot of at most 256 existing rows in that network for
legacy MAC formatting and endpoint-host collisions. A larger inventory requires
explicit operator enrollment or smaller camera networks. An unresolved EPR at an
already owned IP host is rejected; an address alone never merges records or
creates another physical writer.

Discovery never replaces authenticated physical identity. It also cannot change
the transport endpoints of an authenticated device or active job. Such a move
requires an explicit operator reconciliation followed by fresh leased inspection;
a fresh MAC/EPR alone cannot redirect that device's credentials. Ordinary PKI or
separately approved per-device TLS trust remains independent of these hints.

## Explicit HTTP compatibility

`shale camera worker --host-config worker.yaml` remains the standalone HTTP worker
from the earlier provisioning host. Its explicit bearer clients, local scopes,
file providers and operator CP connection are compatibility configuration, not the
outbound machine role. See [the provisioner guide](camera-provisioner.md). Do not
use its local state as a replacement for the CP's central credential/checkpoint
contracts in the new Worker mode.
