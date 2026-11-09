# Device-specific factory TLS approval

A factory camera may expose HTTPS with a certificate that normal PKI cannot
verify. An operator can approve the observed public-key identity of one exact CP
camera through an immutable `CameraPolicy` revision. This approval does not grant
activation, credential access, device ownership, or access to another camera.
Without an approval the existing strict PKI behavior is unchanged.

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
are rejected. A new key needs a new policy revision; an existing revision cannot
be overwritten. Activation permission and any policy activation CAS remain
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

The policy, job and checkpoint fields extend existing typed JSON payloads. They
add no credential plaintext, new table or database column. Old records with
absent identities retain their existing behavior. Deploy the matching CP and
edge runtime before creating a policy using this field.

Validation covers policy syntax/scope, detached per-device selection, direct Job
substitution and checkpoint retention in `server/core/camera_tls_identity_test.go`.
`TestCameraPolicyDeviceTLSIdentityAuthorityAndJobPins` exercises the public CP
with actual TLS, Roster operator grants and independently adopted Worker and
Producer identities. Camera transport verification has its own tests.
