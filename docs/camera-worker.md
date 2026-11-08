# Outbound camera Worker

`shale serve camera-worker` is a separate machine role. It discovers its approved
work through the tenant CP API, keeps its own private key and client-only mTLS
certificate, and opens no incoming listener. It can run independently of a
Producer. The combined host and CP-backed camera execution providers are separate
steps in the camera provisioning plan.

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

A Worker can call only its heartbeat, renewal, Poll, CheckJob, ObserveJob and the
job Claim/Report/Publish execution verbs. Configuration, inventory registration,
assignment, binding, handover and revocation remain operator operations. Each
execution call checks the current adopted identity/certificate serial and exact
assignment. Job grants additionally check the assignment generation, lease owner,
lease generation/expiry and physical-device fence. Replayed machine write
acknowledgments must still match that fence. Poll checks identity again while
waiting and before returning a view.

`CheckJob` returns the complete typed pinned Work envelope. `ObserveJob` adds only
that leased job's assigned Producer status, complete committed recording evidence
since publication, and the bound Source live capability when available. That view
token expires no later than the current job lease or normal viewer-token TTL.
An unavailable relay produces no live capability; it does not make a job ACTIVE.
Actual decoded frames, fresh input evidence, recording and applied-revision checks
remain required by the CP's ACTIVE transition. Runtime applied acknowledgments and
the complete WebRTC end-to-end verifier are later integration layers.

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

The default executor deliberately reports WAITING with
`central_credentials_pending`. It performs no camera initialization, password
change, local FileProvider fallback or Source publication. Durable CP credential
delivery (#196) and the full policy/preset checkpoint contract (#197) must be
injected before production device adaptation can run. The seam keeps the complete
typed job so those layers can add immutable inputs and typed progress without a
lossy conversion to the old local Run model.

A CP lease cannot recall a camera firmware write already sent over the LAN. After
an uncertain mutation, a later executor must reconcile this same pinned job and
recoverable credentials before another write. Previously issued data-plane
capabilities last until their bounded expiry; already-started streams are not
instantly revoked. This role does not claim immediate data-plane revocation.

## Explicit HTTP compatibility

`shale camera worker --host-config worker.yaml` remains the standalone HTTP worker
from the earlier provisioning host. Its explicit bearer clients, local scopes,
file providers and operator CP connection are compatibility configuration, not the
outbound machine role. See [the provisioner guide](camera-provisioner.md). Do not
use its local state as a replacement for the CP's central credential/checkpoint
contracts in the new Worker mode.
