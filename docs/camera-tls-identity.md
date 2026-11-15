# Device-specific factory TLS approval

A factory camera may expose HTTPS with a certificate that normal PKI cannot
verify. An operator can approve the observed public-key identity of one exact CP
camera through an immutable `CameraPolicy` revision. This approval does not grant
activation, credential access, device ownership, or access to another camera.
Without an approval the existing strict PKI behavior is unchanged, unless the
policy explicitly [skips TLS verification](#skipping-tls-verification).

`CameraEnrollmentPolicy.device_tls_identities` contains at most 128 entries:

```json
{
  "device_id": "<canonical CameraDevice UUID>",
  "spki_sha256": "sha256:<64 lowercase hexadecimal digits>"
}
```

The digest is SHA256 of the certificate's DER SubjectPublicKeyInfo, rather than
its whole certificate. The operator must establish the relationship between the
physical camera, its CP inventory ID and the key it approves. Discovery hints,
manufacturer/model matches, an IP address or an unauthenticated observation do
not approve that relationship automatically.

For this compatibility path the policy must also name an explicit HTTPS
`bootstrap` profile, scheme and web port. A key cannot be attached to an implicit
or HTTP bootstrap destination. For the pinned factory-web HTTPS endpoint, explicit
SPKI approval replaces
certificate-chain, hostname and certificate-validity checks. The TLS handshake
still proves possession of the approved private key. The independent normal
ONVIF client retains its existing PKI behavior. This approval is not a tenant-wide
CA or permission to connect to every device: the runtime also restricts it to the
current Device's hosts, network CIDRs and explicit HTTPS web port, without
following redirects. See [the driver deployment guide](camera-provisioner.md).

Only the existing operator `CameraPolicy.Create` boundary approves identities.
Every named Device must exist in that policy's exact tenant, site (including
NULL), and network. Duplicate Device IDs, malformed IDs and noncanonical digests
are rejected. A new key needs a new policy revision, with the one exception
[after a firmware upgrade](#after-a-firmware-upgrade) that Shale performed itself;
an existing revision cannot be overwritten. A job that stops on a changed key
lets one operator request write that revision
([approving a changed key](#approving-a-changed-key)). Activation permission and any policy activation CAS remain
independent requirements. Changing or activating a policy does not withdraw the
pin from an already created immutable Job. To withdraw existing execution, cancel
the affected Job or revoke/replace its Worker assignment; do not rely on changing
the active policy pointer alone.

Enrollment copies only the binding Device's matching identity into immutable
`CameraJobInputs.device_tls_identity`. The array and other cameras' approvals are
not delivered to that job. Direct Job creation may include a TLS identity only
when it names the exact approved policy revision, actual binding Device and
matching network/scope. A selected policy approval cannot be changed or omitted.
A Device without a selected approval receives no exception and uses normal PKI.
A DHCP address change can preserve the same Device and approved key. The runtime
must still obtain that Device's current permitted endpoints; an old IP address
never grants trust to another physical unit.

The portable checkpoint preserves this identity through
`CameraProvisionRequest.device_tls_identity`. CP checks exact equality with the
immutable Job input before decoding the checkpoint or taking its historical
status shortcut. A historical payload cannot add, drop, change or borrow a key
approval. Current job leases, assignment generations and credential-purpose
checks still authorize device operations separately.

## After a firmware upgrade

Upgrading a C420 from 1.2.0 to 1.2.2 replaces the key its web interface
presents; it kept the IP address, MAC, serial, administrator account and
streaming configuration on every camera upgraded so far. When an enrollment job
performs a [preset-required upgrade](camera-provisioner.md#firmware-requirement),
it accepts the device's new key only if all of these hold right after that upgrade:

- the job's pinned key was in force when it started the upgrade, and the
  control plane authorized that upgrade from the job's own preset;
- an authenticated inspection reports the target build, with the same non-empty
  MAC and serial as before the upgrade and as the device's recorded identity;
- the job's own managed administrator credential logs in over a connection that
  presents the new key.

The worker reads the key with a credential-free handshake, then logs in on a
second connection pinned to it. The checkpoint `firmware_upgraded` carries the
key in `CameraProvisionFirmware.tls_identity`. The control plane validates it in
the same transaction that records it on the device as `firmware_tls`: the key
it replaces, the new key, the job and the build. The job's immutable input pin is
not rewritten; its later steps pin the accepted key from that checkpoint.

A later job for the device pins the recorded key while the approval it replaced
is still the device's approval. A new policy revision that approves another key
for the device takes precedence; the recorded key then no longer applies. Without
any approval for the device there is no pin, and a recorded key never becomes a
first trust.

Outside that window nothing changed: a key that differs from the job's pin is
refused, including after a firmware update or factory reset Shale did not perform.
Such a job stops, as the next section describes.

## Approving a changed key

A job whose device presents a key other than the one it pins (a manual
firmware update, a factory reset) is refused in the TLS handshake, before any
request or credential is sent. The worker then reads the presented key with a
credential-free handshake on the device's current authorized endpoint and web
port, and the job stops: stage `WAITING`, code `tls_key_changed`, and the key
in its checkpoint as `CameraProvisionState.changed_tls_identity`. The control
plane accepts that field only for the job's own device and only when it differs
from the key the job pins. It is an observation, never a pin: the job keeps its
immutable input, a later job for the device pins the policy's key as before, and
a job created naming the observed key is refused. If the device later answers
over its pinned key again, the next attempt clears the observation. The console
lists stopped camera jobs with both keys.

One operator request approves the observed key:

```sh
shale --config operator.yaml camera-policy approve-tls JOB_UUID '{"spkiSha256":"sha256:<the key the job recorded>","expectedNetworkVersion":"2026-11-09T12:00:00Z"}'
```

`CameraPolicy.ApproveTLS` names the stopped job, the exact key it recorded and
the network's current `dateUpdated`. The device is the job's own; no request
field names another one. In one recorded transaction the control plane:

1. checks that the job is `WAITING` with that exact key for its own device,
   and that the network version is current (the same CAS as `Activate`);
2. writes a revision of the network's active policy (the job's own revision when
   none is active) that differs only in this device's `device_tls_identities`
   entry, with the same `policy_key` and name and the revision
   `tls-<digest of base revision, device and key>`; it passes the same
   validation as `CameraPolicy.Create`;
3. activates that revision;
4. cancels the stopped job, whose pin can never be met again;
5. enrolls a successor job for the same binding under the new revision. It keeps
   the stopped job's exact credential versions, may activate the device only if
   both the stopped job and the new revision allowed that, and pins the approved
   key.

Any other key, another job, a stale network version or a job that has not
stopped on a changed key is refused, and nothing is written. Once it succeeds the
same request is refused: the stopped job is settled. If the device presents yet
another key afterwards, the successor stops on it and needs its own approval.
Workers cannot call `ApproveTLS`.

## Skipping TLS verification

A camera network policy may set `tls_verification` to `"skip"`
(`"tlsVerification": "skip"` in the JSON request). It is off unless a policy
revision says so. The default, empty, verifies the camera's HTTPS web key: by
its pin, or by ordinary PKI where no key is approved.

- **Allowed values.** Only `skip` is accepted besides empty. It needs an explicit
  HTTPS bootstrap. It cannot be combined with `device_tls_identities` or
  `trust_new_factory_tls`, whose keys it would not check.
- **Visible.** The setting is part of the immutable revision, so
  `camera-policy get` shows it.
- **Copied exactly.** Each job copies it into `CameraJobInputs.tls_verification`
  as its pinned revision says, and its checkpoint keeps it as it keeps the
  bootstrap selection. A job without a policy, or under a policy that verifies,
  cannot set it; a job under a skipping policy cannot drop it.

A skipping job pins no key. The worker's web client accepts whatever key the
camera presents. It is still bound to the device's current authorized hosts and
HTTPS web port, and it follows no redirects. The device is identified by its
recorded MAC and serial only:

- **Worker.** Before any credentialed web call (initialization, firmware upgrade)
  the driver requires an authenticated inspection that reports the serial and
  one of the MACs of the device's authenticated identity at the control plane.
  A device with no such recorded identity is refused.
- **Control plane.** It refuses any checkpoint whose authenticated inspection
  reports another serial or MAC.

A device's first authenticated inspection records that identity, as for any job.

**What it gives up.** The managed administrator credential is sent to an
endpoint whose key is not verified. For a factory camera, so is the initial
administrator password during activation, before the device can be identified
at all. Anything on the camera LAN that answers at the device's address, and
reports its MAC and serial, can receive them. Use it only on an isolated camera
network, and prefer pinned keys, with the approval above, where keys can be
maintained.

A skipping policy approves no key. `ApproveTLS` is therefore refused while such a
policy is the network's active one; enroll the binding under it instead. A
firmware upgrade under it accepts no new key either: the device's MAC and serial
alone confirm it afterwards.

The policy, job and checkpoint fields extend existing typed JSON payloads. They
add no credential plaintext or new table; `CameraDevice.firmware_tls` is a new
nullable JSON column added by the schema migration. Old records with
absent identities retain their existing behavior. Deploy the matching CP and
edge runtime before creating a policy using this field.

Validation covers policy syntax/scope, detached per-device selection, direct Job
substitution and checkpoint retention in `server/core/camera_tls_identity_test.go`.
`TestCameraPolicyDeviceTLSIdentityAuthorityAndJobPins` exercises the public CP
with actual TLS, Roster operator grants and independently adopted Worker and
Producer identities. `TestCameraPolicyApproveChangedTLSKey` drives a job through
a real Worker to a changed-key stop and covers the approval's refusals and
result. Camera transport verification has its own tests.
