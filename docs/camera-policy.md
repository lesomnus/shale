# Central camera policy and pinned execution

An optional `CameraEnrollmentPolicy.bootstrap` pins an explicit compatibility
profile identifier, `http` or `https` web scheme, and web port (1–65535). All
three values are required when present; the CP validates syntax without
guessing firmware, choosing defaults, or consulting an installed driver. The
profile contains no address, URL, credential, or activation grant. A Worker may
report an explicitly selected profile as unsupported if it is not installed.

Enrollment copies this selection into immutable `CameraJobInputs.bootstrap`.
An operator may also supply it on a direct job; a policy-derived job must match
the exact policy value, including absence. The portable request and typed
checkpoint retain the selection through retries, transport recovery and
assignment replacement. Bootstrap jobs require a typed checkpoint before
publication. Absent pins preserve legacy behavior, while activation continues
to require `allow_activation`, scoped credentials and a current execution lease.
The installed web transport derives its host only from freshly authorized camera
addresses; the pin supplies the scheme and port only.

This document describes the central API and execution contracts introduced in
#197 and used by the managed edge.

## Authority and revisions

CP stores immutable preset and enrollment-policy revisions. A revision belongs
to one tenant and optional site. A policy names one camera network and a preset
revision in the same scope. Editing either body creates a new revision; it does
not rewrite a revision referenced by an existing job.

The camera network selects its active policy using its existing version/CAS
boundary. Activating a different policy affects subsequent enrollment decisions.
Reapplying settings to an existing binding requires a new job with a new intent
key; activation does not reset cameras, rotate passwords or restart capture.

Generic generated mutations must not bypass immutable-revision validation.
Operator access follows the existing tenant/site wall. Machine access is custom
and job/assignment scoped; an approved Worker cannot list a tenant's rulesets,
edit policy, assign itself or impersonate a Producer.

## Explicit device TLS identity

An enrollment revision may contain `device_tls_identities`, each approving one
exact CameraDevice ID and public-key SPKI digest for an explicit HTTPS bootstrap
profile. Enrollment copies only that device's approval into the immutable job;
normal PKI remains the default. This does not grant activation, ownership or
credential access based on discovery hints.

The [device TLS contract](camera-tls-identity.md) defines operator approval,
per-device scope, checkpoint recovery, transport verification and key withdrawal.
A job that meets a key other than its pin stops with that key, and
`CameraPolicy.ApproveTLS` approves it in one request: a new revision of the
active policy with that device's key, activated by network CAS, and a successor
job pinned to it ([approving a changed key](camera-tls-identity.md#approving-a-changed-key)).

A policy may instead set `tls_verification: "skip"`, off by default. Its jobs
then check no web key and identify a camera by its recorded MAC and serial
only; administrator credentials go to an endpoint whose key is unverified. The
revision shows the setting, and only a revision that says so makes a job skip
([skipping TLS verification](camera-tls-identity.md#skipping-tls-verification)).

## Automatic first factory TLS trust

An isolated private camera network can explicitly opt in to
`CameraEnrollmentPolicy.trust_new_factory_tls`. Inventory remains unbound until
the assigned Worker completes a credential-free known factory challenge and the
CP commits the first exact SPKI pin together with the enrollment job. Existing
accounts, bindings and changed keys cannot use this path. See the
[first-enrollment trust contract](camera-first-enrollment-trust.md) for the
required policy, expiry, ownership and recovery boundaries.

## Preset representation

Reuse `camera/preset.Provider` for rule validation, precedence, conflict checks,
canonical fingerprints and composition. The CP wire representation must preserve
all of its semantics:

- Initialization frequency, smart codec and time sync, profile token, codec,
  resolution, fractional FPS, bitrate and GOP are typed fields. A capture profile's keyframe interval alone
  cannot represent a device's requested GOP length.
- Preserve fields and required fields retain their separate meanings.
- An absent fallback override inherits alternatives; an explicitly empty override
  clears them. Encode that distinction with a presence-bearing wrapper.
- Rule order is not precedence. Equivalent content has the same fingerprint;
  equally specific conflicting patches remain an error.
- DHCP leases, MAC addresses, serials and discovery scopes are matching inputs.
  They are not machine identity or a secret-delivery grant.

The CP-backed `DevicePresetProvider` resolves against the job's pinned immutable
ruleset, never the network's latest policy. It returns a detached resolved
snapshot, including matched rule IDs, requested settings, ordered alternatives,
required fields and the full immutable reference. It does not expose unrelated
rules or a tenant-wide ruleset to a Worker.

## Enrollment decisions

Automatic enrollment is explicitly enabled per network policy. The default is
observation without camera writes. A policy bounds eligible observations,
assignment, device/binding count and whether first-time activation is permitted.
Missing or unsupported policy leaves visible waiting state; CP errors never
become an implicit local-policy match.

An observation is accepted only from the currently approved Worker assigned to
that network, with current assignment generation and bounded sanitized endpoints
inside its configured prefixes. Discovery identifiers are candidate deduplication
hints. CP assigns inventory and logical binding identities; a current IP never
becomes the authoritative identity. Previously authenticated physical identity
must not be silently replaced by a matching unauthenticated hint.

The accepted decision records the exact policy and preset revisions. Durable
credential preparation precedes job creation and every password-changing camera
call. The credential store owns idempotent preparation; policy code requests a
bundle through an injected preparer rather than generating or keeping passwords.
The same enrollment intent reuses the stored bundle and job. A missing preparer
or unavailable durable store leaves the decision waiting without camera writes.

New observations do not authorize factory reset, guessing an existing password,
taking over an initialized camera or disclosing an existing credential based on
MAC alone. Existing camera credentials require the normal exact assignment,
device, purpose and version authorization.

## Durable execution and recovery

The reusable provisioning engine remains independent of the CP transport. Its
central adapter persists the resolved snapshot before configuration writes and
preserves the selected candidate, initialization/apply intent, authenticated
identity, exact credential references and structured warnings across restarts.
Typed checkpoints must preserve these facts rather than projecting an entire
workflow onto a status string.

Every checkpoint and device step is bound to the current adopted Worker, job
lease, physical-device writer and assignment generation. A lost checkpoint
response stops subsequent writes until the same intent is reconciled. A changed
policy cannot replace the snapshot halfway through a job. An uncertain write
must be independently read back before an allowed fallback is considered.

Warnings retain requested, selected and independently observed settings. Required
fields, authentication failures, identity changes and uncertain writes do not
authorize downgrade. A preset change creates a new job against the existing
binding and Source; it does not erase prior recordings.

Endpoint changes are transport observations, not new physical identities. The
central adapter must reconcile a DHCP address change against the same authorized
device and pinned job without discarding the previous authenticated identity or
creating a second writer. A recording camera's move is staged as a
[transport candidate](camera-worker.md#transport-candidates) and verified by its
approved key before anything else. Arbitrary replacement is a separate operator
action.

## Provider selection and qualification

CP-managed mode selects the central policy/preset provider by default. Explicit
file-managed mode retains the same public provider contract and fallback tests;
neither path silently falls back to the other when CP is unavailable.

Qualification covers immutable edits, tenant/site/role denials, nil-versus-empty
fallbacks, matching precedence, pinned restart/candidate recovery, required-field
rejection, an opt-in automatic decision and stable Source identity. Final edge
qualification separately proves actual decoded live media and newly committed
recordings after Producer application; policy resolution is not readiness.

## Implemented surfaces

Operators use `CameraPreset.Create`, `CameraPolicy.Create`, and
`CameraPolicy.Activate` to publish immutable revisions and CAS the network's
active pointer. `CameraPolicy.Enroll` explicitly creates another intent for an
existing binding. `CameraPolicy.ApproveTLS` writes and activates the revision
approving a key a job stopped on, under the same validation and CAS, and replaces
that job. Generic mutations of either revision entity remain closed.

The assigned Worker sends `CameraPolicy.Observe` with its assignment generation.
CP first commits the sanitized inventory observation, then prepares credentials
and creates the binding/Source/job in one recorded transaction. A preparation
failure rolls back that transaction and records a fixed enrollment outcome on
the visible device. Outcomes include `policy_missing`, `policy_manual`,
`policy_not_matched`, `credential_preparer_unavailable`, `credentials_unavailable`,
`capacity_exceeded`, `reconciliation_pending`, and `enrolled`. A recording camera
seen at other endpoints enrolls nothing: the outcome is `transport_<code>` of its
[transport candidate](camera-worker.md#transport-candidates), and
`CameraPolicy.VerifyTransport` takes the assigned Worker's credential-free check
of it. `CameraPolicy.ObserveClock` takes the same Worker's credential-free
reading of a device's own clock at its authorized endpoint and records it on
the device ([camera clocks](camera-worker.md#camera-clocks)). Only a preset's
[`time_sync`](#camera-time-sync) sets a camera's clock.

An optional `Deps.PrepareCameraEnrollment` callback receives the current recorded
server and transaction driver. Its return value contains purpose-bound durable
references only. The default CP Build connects its credential store through this
callback; the policy layer retains only references. The policy's
optional activation username defaults to `admin` and affects newly prepared
activation material only. Existing/imported accounts retain their usernames.

`CameraPreset.ResolveForJob` returns only the currently leased job's resolved
snapshot. The first reported snapshot is validated against the immutable rules
and atomically pinned on that job. `internal/cameraworker.JobStore` stores the
complete typed portable workflow, bounded to 32 KiB, through ordinary leased
reports. Poll snapshots fail explicitly above two MiB instead of truncating
assignments or presenting an empty catalog. An exact unchanged historical
checkpoint can be carried in a status-only report after replacement; edits
must use today's full delivery scope and retain every pinned material version.

The `Provisioner`, `JobPresets`, `LeasedDevices`, and `JobCommitter` adapters
connect the portable engine without adding a CP dependency to `camera` or
`provision`. Every device operation obtains a fresh grant and lease deadline;
only its endpoint transport hints follow current observations. Lost checkpoint
responses cancel execution before another device write. A committed publication
can be recovered without replaying it with newly measured probe timestamps.
Authentication and readiness observation remain injected by the edge runtime.

Before calling a publisher, the run journals the first complete deployment
profile and operation ID. Later samples can vary within the verified limits;
both the central committer and the standalone file publisher reuse the first
payload and receipt. A sample above 1.3 times the pinned bitrate cannot enter
publication again. Older unresolved publication checkpoints without either pin
require explicit reconciliation before any further device operation; completed
and canceled legacy runs retain their existing behavior.

A media probe rechecks its RTSP address against the fresh job network prefixes
immediately before the LAN callback. A pending media prefix may be removed even
while the management endpoint remains permitted, so the earlier checkpoint is
insufficient authorization. Unchanged historical checkpoints still validate the
same capture declaration: changing only outer summaries cannot publish a
different profile or bypass preset resolution and candidate selection.

Checkpoint validation preserves activation-attempt flags, candidate order,
resolved intent, authenticated identity and structured fallback warnings. Outer
verification must match the saved independently decoded frame count, duration,
observation time and measured bitrate. Configured GOP values cannot substitute
for observed cadence. Legacy jobs without typed state retain their existing
explicit compatibility path; a job with policy/preset/direct configuration
cannot publish by omitting the typed checkpoint.

A repeated discovery reuses an unfinished job without regenerating secrets.
When a completed binding's address changes, transport reconciliation retains
the old policy and credential versions, the same binding and Source, and
disables first-time activation. For a camera with a per-device key, a verified
[transport candidate](camera-worker.md#transport-candidates) creates that job
itself; otherwise it follows the operator's `CameraDevice.Observe`. A terminal failure is not an implicit retry:
an operator creates a new enrollment intent. An active-policy edit alone never
reapplies settings to an existing camera.

## Publishing a declarative preset

Keep model defaults in the strict portable `camera/preset` YAML or JSON format
shown in [the provisioner guide](camera-provisioner.md). The same declaration
can now be published to CP without running a standalone provisioner or putting
policy files on a managed Pi:

```sh
shale --config operator.yaml camera-preset publish --file camera-presets.yaml --dry-run
shale --config operator.yaml camera-preset publish --file camera-presets.yaml --name "Office cameras"
```

`--dry-run` validates and prints the typed `CameraPreset.Create` request locally.
It does not connect, read the CLI session, or load CP authentication/TLS files.
The normal command uses the same tenant connector, configured CP trust and
operator authentication as other CLI operations. JSON output prints UUIDs in the
usual CLI form; `-o protojson` selects protobuf's exact JSON representation.

The document `id` becomes `preset_key` and its source `revision` becomes the
immutable CP revision. The portable provider's internal fingerprint suffix is
not appended to that revision. An equivalent rule ordering produces the same
request; absent fallback inheritance and explicit `fallbacks: []` remain
different. Publishing unchanged content at the same revision reuses the stored
revision. Changed content must use a new revision and otherwise receives a
conflict from CP.

Every rule must name the same canonical tenant UUID. `--tenant UUID` explicitly
checks that target; otherwise it is inferred only from that one consistent
tenant in the document. `--site UUID` publishes a site-specific row and requires
every rule to name that exact site. With no `--site`, the row is a tenant
template and may retain site-specific rule selectors. Scope flags do not rewrite
rules or confer access. Dry-run checks format and target consistency; the normal
CP API still authorizes all referenced inventory and the caller's current scope.

This command publishes only a preset revision. It does not create or replace a
network policy, enable automatic enrollment, approve device trust, rotate
credentials, or reapply an existing camera. The following normal RPC commands
are available for those separate, explicit policy operations:

```sh
shale --config operator.yaml camera-policy create - -o json < camera-policy.json
shale --config operator.yaml camera-network get NETWORK_UUID '{"select":{"all":true}}' -o json
shale --config operator.yaml camera-policy activate POLICY_UUID '{"expectedNetworkVersion":"2026-11-09T12:00:00Z"}' -o json
shale --config operator.yaml camera-policy enroll - -o json < enrollment-intent.json
shale --config operator.yaml camera-policy approve-tls JOB_UUID '{"spkiSha256":"sha256:<key>","expectedNetworkVersion":"2026-11-09T12:00:00Z"}' -o json
```

Replace the example IDs and timestamp with the intended network/policy and the
network's exact current `dateUpdated`. The create request names the network,
published preset, policy key/revision, and the reviewed enrollment declaration.
Keep existing grants, bootstrap choice and device trust approvals unless their
changes are explicitly intended. Activation CAS changes only the network's
active pointer. An enrollment intent separately names `binding`, `policy` and a
new stable `requestKey`; it preserves the logical Source and does not silently
rewrite an existing job's snapshot.

`camera-preset create` also accepts the typed JSON request directly. These RPC
commands use inline JSON or a literal `-` for stdin; they do not interpret
`@file` or a filename as request input. A file publication is one operator
operation, not a continuously running controller. Do not add `preset_file` to
the managed edge's `shale.yaml`: that older setting belongs only to the
standalone provisioner. Device initialization settings such as 60Hz still need
an independently qualified adapter for the camera's actual firmware.

A rule's `firmware` requirement ([provisioner guide](camera-provisioner.md#firmware-requirement))
is published the same way. It takes effect only in enrollment jobs created from a
policy naming that preset; publishing it upgrades nothing by itself, and it never
upgrades a device that is not on one of the listed `from` builds.

The same preset publisher accepts `initialization.smart_codec: "off"` or
`"on"`. Omission preserves the current mode; this field does not implicitly
change power-line frequency, bitrate, FPS or GOP. List
`initialization.smart_codec` in `required` when preserving the current mode is
not an acceptable fallback. Use a newly published immutable preset and a new
enrollment intent to change an existing job's settings. CP checkpoints bind the
explicit mode to a bounded native before snapshot and reject unsolicited or
modified snapshots. The selected mode is read back before video configuration
and checked again afterward; an applied mode does not by itself prove a GOP.

`initialization.time_sync` is published the same way; see
[camera time sync](#camera-time-sync).

## Camera time sync

A camera LAN usually reaches no time server but its edge, and a camera with a
static address never learns one from DHCP. A preset can point such cameras at
the edge's own NTP service ([time for the camera LAN](../deploy/edge/README.md#time-for-the-camera-lan))
and set their time zone:

```yaml
settings:
  initialization:
    time_sync: {ntp: edge, timezone: "KST-9"}
```

`ntp` is `edge` or an IPv4 literal, and nothing else. `edge` is the Worker's
own address on the camera network: the source address it reaches that camera
from ([camera time sync](camera-worker.md#camera-time-sync)). A literal is
written as given. A hostname, an IPv6 address, a port, a zone, a non-canonical
spelling (`192.0.2.01`) and the unspecified, broadcast and multicast addresses
are refused when the preset is published. Whichever form, the server written
must lie inside the camera network's CIDRs, so a preset cannot point cameras
at an arbitrary server:

- `CameraPolicy.Create` refuses a policy whose preset names a literal outside
  its network's CIDRs, in any rule or alternative that can apply on that
  network (no `scope.network_id`, or that network's).
- The Worker resolves the setting for each camera at plan and again at apply
  time, and writes nothing to a camera when the result is outside its
  network's CIDRs. `edge` resolves only there.
- Every checkpoint that journals a time-sync plan names the resolved server,
  and the CP refuses one outside the job's network CIDRs, or a literal written
  as anything but itself.

`timezone` is a POSIX TZ string as ONVIF's `TimeZone/TZ` carries it, at most
64 bytes: `std offset [dst [offset] [,start[/time],end[/time]]]`, such as
`KST-9`, `UTC0`, `CST6CDT,M3.2.0,M11.1.0` or `<+0330>-3:30`. An Olson name
such as `Asia/Seoul` is refused. Both fields are set, or neither: an absent
or empty `time_sync` leaves the camera's clock as it is, which is the default
and what every existing preset does.

The setting travels like `smart_codec`: in rules and alternatives, in
`CameraJobInputs.configuration` for a direct job, in the resolved snapshot a
job pins and in its typed checkpoint. A more specific rule replaces the whole
setting; `preserve: [initialization.time_sync]` in an alternative leaves the
clock alone, and `required` may list it. It is not a media prerequisite: the
job applies it on its own after the video settings read back
([provisioner](camera-provisioner.md#time-sync)), through ONVIF `SetNTP` and
`SetSystemDateAndTime`, never through a vendor web UI.

The device's answer never fails the enrollment. Each of these leaves the job
`ACTIVE` with a durable warning on `initialization.time_sync`:

| Code | When |
| --- | --- |
| `unsupported` | The adapter is not qualified for the camera's model and firmware, or the camera declined a time operation (an ONVIF fault such as `ActionNotSupported`) or does not keep what it took. `applied` is `unchanged` when nothing was written, or what the camera reports now. |
| `time_sync_refused` | The server resolved outside the camera network. Nothing was written. |
| `time_sync_unconverged` | The settings read back, but the camera's clock was not within two seconds of the Worker's after 90 seconds. `applied` names the server, the zone and the last offset. |

A clock that converges later is seen in `CameraDevice.clock`, which the Worker
reads every five minutes ([camera clocks](camera-worker.md#camera-clocks)).
Authentication, transport and lease failures fail the step as anywhere else.

Only the TP-Link EasyCam C420 HW1.0 on firmware 1.2.2 Build260624 is
qualified ([PROTOCOL.md](../camera/vendors/tplink/PROTOCOL.md#time-sync)). The
generic ONVIF path has no device initializer, so any other camera receives an
`unsupported` warning and no write. The standalone provisioner has no camera
network to resolve against and reports time sync as unsupported too.

A Worker older than this field cannot apply it and cannot claim it applied:
it decodes the pinned snapshot without the field, and the CP refuses the
first checkpoint carrying that snapshot (`preset_resolved`), before any
initialization or video write. Such a job makes no progress until its Worker
is upgraded. A Worker that knows the field, given a preset without it, writes
and journals nothing new.
