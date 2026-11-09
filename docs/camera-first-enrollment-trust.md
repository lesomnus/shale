# Automatic first trust for factory cameras

Normal camera connections use ordinary certificate verification. A policy can
explicitly set `trust_new_factory_tls: true` for a separately configured private
camera LAN. This is an opt-in first-enrollment policy, not a general certificate
error fallback. It requires automatic enrollment, activation permission, a
positive device limit, an explicit interface and private CIDRs, and a supported
C420 bootstrap profile using HTTPS port 443.

The CP records discovery inventory first. Before creating a binding, Source,
job or credential, it returns `factory_tls_required` and a two-minute plan for
one current Device, endpoint, policy revision and Worker assignment generation.
An unrelated DHCP client can remain in inventory without using a camera slot.

The assigned Worker performs a credential-free TLS handshake, records the leaf
SPKI SHA256 and pins that exact key for the known anonymous factory challenge.
Only the supported `initial password unset` response permits first trust. The
probe sends no password, authorization header or cookie. It does not log in,
change the clock, initialize an account or configure media. A key change between
the handshake and challenge fails before HTTP is sent.

`CameraPolicy.TrustFactoryTLS` rechecks the adopted Worker's current assignment,
network version, policy, observation revision, endpoint and plan expiry. It also
checks the current network inventory for another owner of that canonical IP,
including aliases using another port, path or IPv4-mapped spelling. Only
then does one CP transaction store the accepted Device pin and prepare the
encrypted central credentials, stable binding/Source and immutable job. Any
failure rolls all of those enrollment writes back; the preceding inventory
observation remains visible. Camera activation and credential delivery happen
after that transaction commits. A lost response can be replayed with the exact
stored plan and key, without generating another account or job.

An authenticated identity, any prior binding or any existing credential record
prevents first-use trust, including erased or revoked history. Initialized
cameras and unknown factory protocols require operator action. An accepted key
is retained in later jobs. Discovery cannot rotate it, trust a replacement, or
approve a changed key. Existing per-device operator SPKI approvals and normal
PKI policies continue to work. The existing operator approval requirement for
moving an enrolled device to a new address is unchanged.

The anonymous factory response and LAN key observation do not authenticate a
manufacturer. Enabling this policy delegates first-use physical-device trust to
the assigned Worker on that isolated network. Use operator-approved exact SPKI
identities when the network cannot provide that enrollment boundary. Pin
verification replaces factory web certificate chain, hostname and validity
checks while the TLS handshake still proves possession of the accepted key.
Normal ONVIF transport verification remains separate.

To stop a pending plan, disable or replace the policy or revoke/reassign the
Worker; stale plans are rejected. Policy activation does not rewrite an already
created immutable job. Cancel affected jobs or revoke their assignment when
withdrawing an existing job's authority. Rotate a camera key through an explicit
operator-approved policy revision, never automatic first trust.
