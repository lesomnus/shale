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
assignment-scoped policy Observe and ObserveClock, job Claim/Report/Publish, and
exact job-scoped preset and credential resolution. Generic configuration, inventory registration,
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
When a pinned device refuses its pin, the driver also reads the key it presents,
in its own leased step and without a credential, and the job stops with that key
for an operator ([approving a changed key](camera-tls-identity.md#approving-a-changed-key)).
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
cleared, and the CP preserves previously authenticated inventory evidence. A
camera seen at other endpoints after a DHCP address change becomes a
[transport candidate](#transport-candidates), staged beside its authorized
endpoints. An address hint never grants inventory ownership or credential
access.

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

Configure the camera network in CP with an exact `interface_hint` and its
current CIDRs. The provider runs the ordinary unprivileged iproute2 command
`ip -j neighbor show dev <interface>`; it sends no probes, reads no DHCP lease
files and needs no additional capabilities. It considers only two kinds of
host, at most 32 seeds, 256 discovered hosts and 1024 cache entries:

- the network's explicit seed endpoint IPs, for a factory camera that does not
  answer WS-Discovery yet;
- hosts inside the current CIDRs that answered this round's WS-Discovery
  without a MAC. A static-address camera has no DHCP lease, and some firmware
  answers with an EPR that is not a `urn:uuid:` (a bare `uuid:`, for
  example); without the cache entry such a camera has no durable identity and
  every scan skips it.

Missing, failed, incomplete, multicast or conflicting mappings provide no
candidate. Another normal network operation must populate a missing kernel
cache entry; an IP seed or a WS-Discovery answer alone remains insufficient.
Other embeddings may inject another bounded observation implementation through
the same seam; one that implements `discovery.DiscoveredObservationProvider`
also receives the regular discoverer's observations.

### DHCP lease hints for newly connected cameras

A local DHCP server already knows the addresses it has assigned, including new
factory cameras that do not yet answer ONVIF. Opt in to reading its dnsmasq lease
file rather than enumerating the network or requiring each new IP as a seed:

```yaml
camera_worker:
  initial_neighbors: true
  dhcp_leases:
    - interface: eth0
      file: /var/lib/shale-camera-dhcp/leases
```

Each configured file belongs to one exact interface and must be readable by the
Worker. Interfaces without a configured file do not query another interface's
leases. The local administrator controls the file path; DHCP hostname and client
ID fields cannot supply a URL, path, credential, model or inventory identity.
This option does not run or configure a DHCP server.

The [camera-LAN recipe](../deploy/edge/README.md) supplies a static non-login DHCP
account and a dedicated `shale-camera-discovery` group: DHCP state 0750, lease
file 0640, and a supplementary-group drop-in only for the selected non-root
`shale@edge` or `shale@camera-worker` instance. The older DynamicUser recipe's
private 0700 ancestor is not readable through a group change alone; use its
explicit lease-preserving migration. Do not make Worker root or add DHCP to the
Shale machine-credential group to bypass this boundary.

The assigned CP network must still name its `interface_hint`, current CIDRs and
at least one HTTP(S) seed management endpoint. DHCP candidates reuse only the
scheme, port and path of those seed endpoints at active lease hosts within those
CIDRs. For example, `http://192.168.250.205:2020/onvif/device_service` supplies a
management template for a new `192.168.250.42` lease; it does not authorize hosts
outside the current assigned prefixes. Media-only RTSP seeds are not templates.
No packet is sent by this provider and no whole-prefix or neighbor-cache scan is
performed. The ordinary WS-Discovery and optional neighbor-cache sources remain
independently composed with these hints.

Only usable IPv4 unicast/MAC pairs are considered. Expired, malformed,
out-of-prefix, network/broadcast or conflicting mappings do not yield a candidate.
Dnsmasq expiry `0` means an explicitly infinite lease. Reading is bounded to one
MiB, 4096 lines and 1024 usable lease rows, with at most 128 candidate hosts per
network. A missing file before the first lease is an empty source; an unreadable
or oversized source fails that discovery attempt without canceling an executing
camera job. Linux opens refuse symlinks and cannot wait for a replacement FIFO.

These are unauthenticated observations, including leases for non-camera devices.
They do not grant activation, approve a certificate or let a camera reclaim an
existing inventory row. The current automatic-enrollment policy, capacity,
device trust and leased execution checks decide whether a discovered candidate
can become a camera. A later DHCP move of an already authenticated camera is a
[transport candidate](#transport-candidates), never an endpoint change.

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
the transport endpoints of a recording camera: a fresh MAC/EPR alone cannot
redirect that device's credentials. Ordinary PKI or separately approved
per-device TLS trust remains independent of these hints.

### Transport candidates

A camera's endpoints are guarded once it has an authenticated identity, a job
ever held it, or its binding has a published capture input. When the assigned
Worker's scan places a guarded camera, by its EPR or MAC, at other endpoints,
`CameraPolicy.Observe` does not move it. It stages a transport candidate in
`CameraDevice.transport` and answers `transport_<code>`:

- **Separate.** The candidate holds the observed endpoints, the hints that
  matched, and the assignment generation and endpoint revision it departs
  from. The device's endpoints, identity, observation revision and discovery ID
  stay as they were; no binding, Source, job or credential is touched.
- **Bounded and expiring.** At most four per device, newest kept; each lives
  two minutes. A scan repeating a live candidate's endpoints returns that same
  candidate, so a fixed outcome stays until it expires. A later scan after
  expiry starts a new one: stale evidence never approves a later observation.
- **One at a time.** Two live candidates with different endpoints are both
  `ambiguous`.

A candidate the CP can check is `pending` with code `verification_required`.
Its grant goes to the assigned Worker in `CameraPolicyObserveResponse.transport_grant`.
The grant binds the exact candidate, Device, Network and its version, the
assignment and its generation, the device's endpoint revision, its latest job
and that job's policy revision, and the device's approved key: the key a job
under that revision pins for this device now
([camera TLS identity](camera-tls-identity.md#verifying-a-moved-camera)). Its
endpoint is `https://<candidate host>:<the pinned bootstrap web port>`.

The Worker polls its assignment again and checks the grant against it: the same
generation, network version and endpoint revision, the one host its scan saw,
within the assigned prefixes, inside the grant's lifetime. Only then does it
perform one credential-free TLS handshake with that origin, bounded by the
grant's expiry: no HTTP request, credential, cookie or caller TLS configuration.
It sends the presented key to `CameraPolicy.VerifyTransport` with the exact
stored grant. An unreachable candidate is not reported; it stays pending and
expires.

`VerifyTransport` rechecks, in one transaction under the network, assignment
and device locks: the grant's expiry, the presented key against the grant's,
the assignment generation, the network version (trust changes with it), the
device's endpoint revision, its latest job and unfinished jobs, the key a job
would pin today, the other live candidates and every other device's hosts. Any
difference settles the candidate `operator_required` with a fixed code. A
candidate that passes is `accepted`, and the same transaction:

1. moves the device's endpoints to the candidate's and advances its endpoint
   revision, keeping its authenticated identity and discovery ID;
2. enrolls a recovery job for the same binding, so the same Source, with the
   request key `transport/<candidate>`. It keeps the latest job's policy and
   preset revisions, bootstrap, exact management and streaming credential
   versions and the verified key; it has no initial credential and no
   activation. A job that would differ is refused and nothing is written.

The recovery job reinspects the camera, authenticated, before anything else.
While a device's endpoints are those of an accepted candidate, both the Worker
(before the engine continues) and the CP (before it accepts a checkpoint with
configuration, firmware or publication state) require that inspection to report
the recorded serial and one recorded MAC. Only then does the job configure,
verify media and publish the binding's new input to the Producer.

A replay of a decided grant, after a lost response or a race, returns its
outcome and recovery job without another write; the same grant with another key
is refused. Only the assigned Worker may call it. The next scan finds the camera
at its endpoints and reports the recovery as `reconciliation_pending`.

An operator moves any other camera: `CameraDevice.Observe` with the device's
current version and the candidate's endpoints (omit `identity` to keep the
authenticated one), also `shale camera-device observe DEVICE '{...}'`. A changed
endpoint set clears every staged candidate. The console's Cameras page lists the
cameras seen at another address, each candidate's state and why, and that command
for the ones an operator must decide.

These cases never get a grant, or settle the candidate, and stay with an
operator:

| Code | Why |
| --- | --- |
| `insecure_http` | The device's web transport is HTTP: no key to verify |
| `trust_missing` | No per-device key: ordinary PKI trusts a CA, and a certificate from the same CA is not this camera |
| `tls_verification_skip` | The job's or the active policy skips TLS verification |
| `trust_mismatch` | The active policy approves another key than the device's last job pinned |
| `identity_missing` | No authenticated serial and MAC to reinspect |
| `recovery_unavailable` | No published binding whose last job pinned a policy revision, preset and credentials a recovery can reproduce |
| `policy_manual` | The network's active policy is not automatic: discovery starts no job there |
| `mutation_active` | A job for the device is unfinished |
| `evidence_conflict` | The EPR or MAC contradicts the device's known ones |
| `host_owned` | Another device owns the candidate's host, in any spelling, port or path |
| `ambiguous` | Several live candidates, or several hosts in one |
| `key_mismatch` | The candidate presented another key; nothing else reached it |
| `expired` | The evidence or its grant outlived two minutes |
| `assignment_changed`, `trust_changed`, `endpoints_changed`, `job_changed` | What the grant was bound to changed before the CP checked it |

A Worker older than this ignores the grant; its candidates expire and stay on
the operator path, and its recovery jobs, if a newer Worker created one, are
still checked by the CP. A CP older than this refuses the moved observation, as
it always did, and sends no grant.

### Camera clocks

A camera LAN without a time service leaves each camera's clock where it was
last set. On an edge with six TP-Link C420s, four were 138 to 147 days behind
and two within about 13 seconds, every one in `NTP` mode with TZ `GMT0GMT` and
no NTP server to reach. Shale writes a camera's clock only when a preset's
`time_sync` asks ([camera time sync](#camera-time-sync)), and then through
ONVIF `SetNTP` and `SetSystemDateAndTime` alone. Inspection, activation and
configuration call neither, nor the TP-Link login helper that writes the
clock, and the pinned C420 path signs WS-Security with the camera's own public
time instead. The Worker observes each clock either way, so operators see the
drift, and a time sync that converges late, as it changes.

After a scan's `CameraPolicy.Observe`, the Worker reads the clock of each
camera that scan placed at its authorized endpoints: the first HTTP(S)
endpoint the scan saw that is also one of the device's. A camera seen only
somewhere else, a [transport candidate](#transport-candidates), is not read.
The reading is ONVIF `GetSystemDateAndTime` with no WS-Security, HTTP
authentication or cookie, following no redirect, through a transport that
dials only hosts in the assigned prefixes, within five seconds. Each camera is
read at most once per `ClockInterval`, five minutes by default (one second to
one hour), and a failed reading waits as long; with the 30-second scan a
reading is at most about five and a half minutes old. `shale serve
camera-worker` and `shale serve edge` install the reader; the bare
`cameraworker.New` reads no clock unless its composition root injects
`Config.Clocks`. Clocks are read only beside discovery, which these commands
always run.

The Worker sends `CameraPolicy.ObserveClock` the assignment and generation of
the scan, the device's ID, the endpoint it read and the reading, which
becomes `CameraDevice.clock`:

| Field | Meaning |
| --- | --- |
| `offset_ms` | The camera's UTC minus the Worker's clock at the round trip's midpoint. The camera answers in whole seconds, so the middle of its second is taken. |
| `rtt_ms` | The round trip, connection included, rounded up. The offset is right to within half of it and half a second. |
| `mode` | The camera's `DateTimeType`: `Manual` or `NTP`; empty when it said neither. |
| `tz` | The POSIX TZ string the camera reported; empty when it reported none. |
| `observed_at` | The Worker's clock at the midpoint. |

The CP records it only from the assigned Worker, under the current assignment
generation, for a device of that network whose endpoints still include the
one read, and within bounds: `observed_at` within five minutes of the CP's
clock, since a Worker further off measures every camera against a wrong
clock; a camera clock between 1970 and 9999 at that time; `rtt_ms` from zero
to thirty seconds; one of the three modes; and a TZ of at most 64 printable
ASCII characters without a space. Anything else is refused and nothing is
recorded. The Worker records a mode or TZ outside these as unknown rather
than dropping the reading. The reading replaces the last one and changes
nothing else on the device.

The console's Cameras page shows each recording camera's offset in the unit
that reads best (`−147 d`, `+13 s`), its mode, its TZ and the reading's age,
as a warning when the offset is over two seconds or the mode or TZ is
unknown. `shale camera-device get` prints the whole reading.

A CP older than this answers `Unimplemented`; the Worker records nothing and
logs nothing for it. A Worker older than this sends no reading, and the
device has no `clock`.

### Camera time sync

A job whose preset sets `initialization.time_sync`
([camera policy](camera-policy.md#camera-time-sync)) points the camera at an
NTP server and sets its time zone, once its video settings read back. The
Worker resolves the setting's `ntp` itself, for each camera, at plan time and
again at apply time:

- `edge` is the Worker's own address toward the camera: the local address of
  a UDP socket connected to the camera's endpoint host on port 123, which the
  kernel chooses by route and sends nothing for. On an edge that is the Pi's
  camera-LAN address, where chrony serves the cameras
  ([time for the camera LAN](../deploy/edge/README.md#time-for-the-camera-lan)).
- An IPv4 literal is itself. No name is looked up.

The result must be an IPv4 address inside the assigned network's CIDRs, the
same prefixes that bound every camera address the Worker dials. Otherwise the
camera is neither asked about its time nor written to, and the job records a
`time_sync_refused` warning. So a network whose CIDRs name the cameras but not
the edge's own address refuses `edge`; list the camera subnet itself.

The step, each device operation under a fresh lease as everywhere else:

1. Plan, read-only: resolve the server, then read `GetNTP` (with the job's
   credential) and the public `GetSystemDateAndTime`. The plan names the
   resolved server and what the camera reported, and lists which of the two
   writes differ. A camera already pointed at that server alone, keeping time
   by NTP in that zone, needs none.
2. Journal that plan in the job's checkpoint (`time_sync_planned`) before the
   first write. The CP refuses a plan whose server is outside the network.
3. Apply: resolve again (a different result is a conflict), read again, and
   write only what differs: `SetNTP(FromDHCP=false, NTPManual=[IPv4 server])`,
   then `SetSystemDateAndTime(DateTimeType=NTP, DaylightSavings=false,
   TimeZone{TZ})`, then read both back. A write can move the camera's clock, so
   the cached device time that signs WS-Security is dropped after each one.
4. Read back with a fresh plan, then read the camera's clock credential-free,
   as above, every five seconds for up to 90, until it is within two seconds
   of the Worker's.

The outcome is recorded once (`progress.time_sync.settled`), with the event
`time_sync_verified`, `time_sync_unconverged`, `time_sync_unsupported` or
`time_sync_refused` and, for all but the first, a warning; a retried job does
not repeat it. None of them fails the enrollment. Only the TP-Link C420 on its
qualified build is driven; every other camera is `unsupported` before any call.

## Explicit HTTP compatibility

`shale camera worker --host-config worker.yaml` remains the standalone HTTP worker
from the earlier provisioning host. Its explicit bearer clients, local scopes,
file providers and operator CP connection are compatibility configuration, not the
outbound machine role. See [the provisioner guide](camera-provisioner.md). Do not
use its local state as a replacement for the CP's central credential/checkpoint
contracts in the new Worker mode.
