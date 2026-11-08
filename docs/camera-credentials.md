# Central camera credentials

The CP durably owns camera accounts. `CameraCredential` identifies a tenant,
physical device, optional logical binding and purpose; immutable versions contain
the encrypted account. A separately approved replacement machine recovers the
same version through its new assignment without copying the old machine key.

The built-in backend uses AES-256-GCM with fresh nonces and HKDF-separated keys
from the existing CP KEK. Authenticated context fixes tenant/device, optional
binding, purpose, credential ID and version. Current machines, network and
assignment generations are delivery checks rather than encryption identity.
Back up the CP database **and its original KEK/state** together. Losing the KEK
makes old versions unrecoverable; retries never regenerate installed passwords.
This version adds no KEK rotation protocol and leaves signing-key envelopes intact.

`initial_admin` is device-wide. Management may also be device-wide; streaming can
be binding-specific. Purposes constrain delivery. Sharing one camera administrator
account under several purposes does not create camera-side account isolation.

## Prepare, import and recover

Authorized tenant/site operators call `CameraCredentialService.Prepare`, `Import`,
`Revoke` and explicit administrative `Resolve`. Generated mutation methods are
closed. Prepare commits the password/version before returning a reference. A
stable subject/intent replays the exact original result after lost responses or
CP restarts. Changed intent inputs conflict. Another intent against an installed
account creates a `REUSED` version with the same material rather than rotating
it; old exact pins remain resolvable. A requested username applies only to a new
account; existing/imported usernames remain byte-exact.

Import uses the explicit binary codec in `camera/credential/material`, preserving
non-UTF-8 username/password bytes. Replacement requires an exact
`expected_current`; creation cannot overwrite a concurrent version. Same-intent
retries survive an advanced current pointer. Equality uses keyed tags, never a
persisted unkeyed username/password hash. Wrong KEKs, damaged ciphertext and
backend outages fail closed with fixed errors.

Internal `cameracredential.PrepareBundle` uses the caller's **current** recorded
transaction, after assignment graph authorization. Preparation and Job creation
must commit together before camera I/O. It preserves purpose-specific imported
accounts and shares a sufficiently privileged installed account only for a
missing purpose. It generates a new initial account only with activation
permission. Initialized cameras can use management/streaming imports without an
initial-admin reference; those with no usable account remain waiting. An error
rolls back the enclosing transaction. The default CP Build wires this helper to
the enrollment request's recorded Server and current Driver, using the CP KEK
and the existing initial-password generator. It does not impersonate an operator
or call an outward RPC inside a job. An embedding may explicitly replace or
disable this callback; a missing or failed preparer leaves enrollment waiting.

Resolve explicitly transfers plaintext over the authenticated CP connection.
Use its callback-based lease promptly and close it. Never log or persist raw
transfer protobufs. Ordinary CRUD and custom preparation metadata omit material,
ciphertext and keyed tags. Audit records contain actor/action/version metadata;
machine reads are audited explicitly. Administrative recovery can reveal original
account bytes. Version revocation is one-way and rejects future delivery.

## Machine delivery and defaults

Workers resolve only exact purpose references pinned in their current job. Every
request checks the adopted certificate, assignment/site, job lease, assignment
generation and physical-device fence. Initial-admin delivery also requires job
activation permission. Workers have no generic credential Get/List/Watch access.
`Execution.Credentials()` supplies a job-bound provider that checks the current
execution before every resolve rather than trusting a snapshot.

Producers resolve only streaming accounts for their own currently collecting
Source, binding and desired credential/version/revision. Management/initial
requests, unpublished sources, foreign Sets and replaced identities are rejected.
The client binds a provider to each exact Source before capture. Leases default
to 30 seconds, maximum 300, bounded by the job/machine grant. Existing camera RTSP
connections do not close automatically when the lease expires or CP access ends.

With a CP catalog, credentials default to central machine delivery:

```yaml
producer:
  catalog:
    provider: cp
  credentials:
    provider: cp
```

The factory opens only after authentication and Set adoption, using the renewable
machine connection. No person session or directory is consulted on CP errors.
Standalone capture can explicitly select a private `credentials.directory` and
provider name for `FileProvider`; `cp` plus a directory is rejected. Existing
explicit local configurations remain supported. The file deployment publisher
accepts an injected preflight resolver. Its legacy fallback requires an explicit
absolute private directory; injected failures never select another provider.
The caller must provide the resolver that capture can actually use.

Worker execution still requires the explicitly configured managed edge adapters
and policy. Default CP credential preparation alone enables no camera writes.

## Optional streaming cache

Caching is **off by default**. Opt-in `cp.StreamingCache` requires a private local
directory and this Producer's private key material. A separately HKDF-derived key
AEAD-encrypts entries; no CP KEK is copied to the edge. Replacement keys require
fresh online grants. The edge composition supplies the key and actual capture
observer together. No CLI switch treats a successful Resolve as successful media.

Only an online streaming grant with CP `cache_until`, exact Source/revision/ref
and assignment generation enters pending memory. Persistence requires matching
actual-media proof from the current capture: initialization, an IDR, advancing
video and the exact credential lease used. CP status, publication receipts and
old capture epochs are insufficient. The managed edge connects the Source
application observer when its bounded streaming cache is explicitly enabled.

Fallback is limited to transport unavailability and the authenticated original
deadline: at most five minutes and 64 entries. Worker/initial/management accounts
are never cached. Online denials, invalid leases and stop/supersede remove entries.
Reconnect first tries the CP. File timestamps, process restarts and CP outages
cannot extend expiry. Authenticated expired files are pruned on write;
damaged/unreadable files cause bounded refusal and may be removed from this
dedicated directory by an operator.

The cache does not replace current Source catalog, machine authentication or a
fresh application ticket, so it promises no unconditional offline cold restart.
Active RTSP sessions can continue independently of credential-delivery expiry.
Applications must fence stale proofs before `RecordApplied` and connect
stopped/superseded events to `InvalidateSource`.

## Explicit standalone migration

An operator resolves the old exact FileProvider ref, encodes its account with
`material.New/MarshalBinary`, then Imports into the chosen CP subject with the
complete `legacy` ref. The mapping retains original tenant, site, Set, Producer,
network, provider, ID, version, purpose and recoverability. Mapping one legacy
identity into another CP target conflicts; original scope is not rewritten to a
new assignment. Import does not rotate the camera or delete its old file.

After commit, explicitly repin the desired job/Source to the CP reference and
verify central capture before retiring the old deployment. Keep old versions and
backups until old pins are retired. Credential/device revocation rejects future
resolves without rewriting historical Source/recording identity or recalling
already-issued data-plane tokens and established camera sessions.
