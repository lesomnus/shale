# Source catalogs

The public `source` package is the reference-only boundary between desired
capture configuration and a Producer. It imports no Worker, ONVIF driver or CLI.
`Provider` offers a full `Snapshot` and a blocking `WaitForChange` hint. The
consumer owns cancellation and periodic resynchronization; a provider does not
require a background goroutine or an unbounded notification queue.

## Identity and revisions

A managed snapshot names an assignment, its generation and its aggregate
catalog revision. Its tenant, optional site, Set, Producer and camera-network
scope are explicit. Managed identifiers must be valid PDIDs of their expected
entity domains, not merely syntactically valid UUIDs. Each specification
has an explicit binding and Source ID, alias and revision, a sanitized RTSP
endpoint, a capture profile and an optional streaming credential reference.
Aliases, Source IDs and binding IDs cannot repeat within a snapshot.

Assignment generations fence changes of ownership. Catalog revisions never
decrease and increase whenever capture membership or content changes. A source's
own revision can be older than the aggregate revision: publishing another camera
does not change this camera's capture generation. Fractional FPS is preserved;
an unknown keyframe interval remains zero.

Runtime adapters must not cast or round this fractional FPS into the legacy
integer `SourceConfig.Fps`. Managed copy capture can preserve source cadence
without a legacy FPS override; its exact profile remains available to negotiation
and future acknowledgement. The initial Producer adapter uses `Encoder: copy`
and leaves `Fps` unset, retaining the exact profile in its accepted catalog.
Its negotiated ceiling includes the configured video ceiling plus the existing
default budget for archive audio and the live Opus track.

`Memory.Publish` validates and atomically accepts a deep copy. An exact duplicate
is idempotent. An older generation or catalog revision returns `ErrStale`; a
different assignment/scope or different content at the same catalog revision
returns `ErrConflict`. A generation can increase without changing capture intent.
Moving to another authorized assignment requires reconstructing the provider,
not silently resetting its revision or tenant boundary.

Snapshots preserve source order and return caller-owned slices, maps and
credential-reference pointers. Consumers must not mutate an input concurrently
with publication; after publication returns, the caller retains its own copy.

## Static compatibility

`NewStatic` exposes the already normalized file/manual source list at local
revision one. It preserves input order and aliases and does not invent a Source
ID for a legacy entry which has not passed normal Producer registration.

```go
provider, err := source.NewStatic([]source.Spec{
    {Alias: "entrance", Input: "rtsp://192.0.2.10/stream"},
    {Alias: "usb-camera", Input: "v4l2:/dev/video0"},
})
```

The existing file/CLI compatibility adapter owns local capture commands, USB
controls, encoder options, ONVIF settings and its AuthProvider privately. Those
settings are not fields of the shareable `Spec`. An empty static input may refer
to a locally configured command in that private adapter; it does not authorize
a remotely supplied command. Existing `max_bitrate: onvif` handling stays on that
legacy path.

A legacy RTSP URI containing user information must be split at the private
compatibility/import boundary into a clean URI and an AuthProvider reference.
Catalog construction and serialization reject user information, credential
query parameters, malformed query encoding and URI fragments. They do not
silently strip credentials and then publish an input with changed behavior.
Errors do not repeat the rejected endpoint.

The Producer's file adapter preserves both RTSP user information and credential
query parameters in a private, process-local read lease. Its public static
catalog contains only the clean URI and an opaque reference. The lease binds to
that exact clean endpoint and sanitizes subprocess errors, including encoded
credential values. Such references are not recoverable activation credentials:
restarting the Producer rereads the private local configuration. An existing
explicit, versioned credential reference is retained with its exact scope and
uses the configured AuthProvider. Combining a referenced credential and embedded
URI credentials is rejected as ambiguous.

For a local URI which previously supplied user information for ONVIF profile
inference, the private adapter also preserves that original read-only management
intent. It binds the lease to the exact source scope, clean RTSP input and
original management endpoint. This nonrecoverable package-private capability
does not make a streaming reference usable for generic management or device
writes. An explicit management reference continues to use the normal provider.

`CredentialRef` contains only provider, stable ID, immutable version and purpose.
It is not a grant or proof of recoverability. Credential consumers still check
the current assignment, exact runtime scope and authorized immutable version
through a separate AuthProvider. Existing exact-scope references require explicit
compatibility mapping when imported; the catalog does not loosen their checks.

## Waiting, errors and resynchronization

`WaitForChange(ctx, after)` returns a newer revision hint or an error. It does
not deliver deltas or acknowledge that a revision has been applied. Slow
consumers may skip several hints and recover everything from the next full read.
A static provider waits for cancellation when its revision has not changed.

Consumers should use bounded wait contexts and periodically read a full snapshot
even if no hint arrives. They retain their last successfully read/applied state
on transport errors, `ErrUnavailable` or canceled reads. A successful empty
snapshot is distinct and can intentionally stop collection under the runtime's
reconciliation policy. A zero-value uninitialized provider is unavailable, not
an implicitly empty catalog. An explicitly constructed empty managed snapshot
at catalog revision zero is valid.

## Durable publication and runtime boundary

`Memory` is a notification/read optimization, not the durable authority. A
CP-managed publisher must first commit its desired full snapshot to CP, then
publish the committed revision in memory. If the process stops between those
steps, a full CP snapshot restores it. A late local notification cannot roll
back a newer accepted revision. Speculative job results must not become locally
consumable before the central commit.

CP adapters use an adopted Producer's current assignment, not an operator login
or a tenant-wide inventory grant. Initial loading occurs after machine
authentication and Set assignment. Dynamic source add/change/stop, zero-source
idle operation and applied acknowledgements are the separate #199 lifecycle
boundary. A runtime acknowledgement must correlate assignment generation,
aggregate catalog revision and each binding/Source specification revision after
registration, negotiation and actual capture application. Reading a snapshot
alone is not applied state or proof of live/committed media readiness.

## CP-backed initial loading

The CLI continues to use `producer.sources` by default. A CP-managed Producer
selects its catalog explicitly:

```yaml
producer:
  catalog:
    provider: cp
    # Optional: pin a specific assignment ID rather than its unique Set.
    # assignment: <CameraAssignment PDID>
    poll_interval: 1s
  credentials:
    directory: /var/lib/shale/credentials
    provider: local
```

Do not combine `catalog.provider: cp` with local `sources` or `demo`. `file` and
`static` explicitly select the existing local configuration path. Credential
configuration is independent of catalog selection: a thin reference never
creates or authorizes a secret resolver. A reference to a central credential
backend requires that separately configured AuthProvider; selecting `cp` alone
does not implement credential distribution.

Embedded callers can supply `producer.Config.SourceProvider`, or a mutually
exclusive `SourceProviderFactory`. The factory receives `SourceSession` only
after the host agent has authenticated and obtained its actual Producer, tenant,
site and Set. Its connection follows machine-certificate renewal. `source/cp`
uses this session for the single Producer-scoped `CameraAssignment.Snapshot`
RPC, validates owner and scope, and does not request generic assignment inventory
or Watch access. An omitted assignment ID uses the Set reference and fails if
more than one matching assignment exists.

`source/cp.Provider` uses deadline-bounded full reads for change polling and
periodic resynchronization. A failed read leaves its accepted memory untouched
and returns an error. The initial runtime verifies the snapshot scope, looks up
each managed Source by its exact CP ID, and rejects identity, alias, Set, tenant
or configured-label mismatches. It never creates a replacement ID for a managed
binding. Legacy aliases without IDs still use normal registration.

This implementation consumes one initial snapshot. A successfully empty catalog
returns `producer.ErrInitialCatalogEmpty` with a clear runtime boundary; it does
not synthesize a camera or interpret a failed CP read as empty. Dynamic updates,
empty idle operation and actual-applied acknowledgements remain #199 work.

## Worker publication adapter

`provision/source.Publisher` accepts a narrow durable `Committer`. A publication
receipt contains the stable Source binding and the committed assignment/catalog
revision. The adapter validates that receipt before notifying a separately
injected Producer-authenticated `Provider` and `Memory`. The Worker's commit
capability grants no full-catalog read authority. Standalone workers can omit
both Provider and Memory and use durable commit-only publication.

The adapter refreshes a complete authorized catalog, never a snapshot invented
from one camera's result. A failed or stale refresh after a successful commit
returns the durable binding and a fixed, secret-free refresh classification;
it does not retry device mutation. `Refresh` or normal resynchronization repairs
the missed notification. A commit receipt is not evidence of live, ingest or
committed recording readiness: those are delegated to the separate Observer.
