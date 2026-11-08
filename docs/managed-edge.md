# Managed outbound edge roles

`shale serve edge` runs an outbound camera Worker and Producer in one process.
They keep separate machine keys, certificates, approval and state directories:
`<state>/camera-worker` and `<state>/producer`. Approve each machine for its own
CP role and assignment. The process does not borrow a human session or grant the
Worker the Producer's catalog or credential permissions.

```yaml
cp: https://shale.example.internal:7400
tenant: company
ca_hash: sha256:<deployment-ca-fingerprint>
state: /var/lib/shale

edge:
  roles: [worker, producer]
  retry_interval: 1s

camera_worker:
  discovery_interval: 30s
  discovery_timeout: 2s
  # Optional tool paths and an additional camera TLS CA; CP trust uses ca_hash.
  # ffmpeg: /usr/bin/ffmpeg
  # ffprobe: /usr/bin/ffprobe
  # camera_ca_file: /etc/shale/camera-ca.pem

producer:
  buffer: 512MiB
  uplink: 20Mbps
  drain_timeout: 2m
```

Omitting `edge.roles` enables both roles. `[worker]` or `[producer]` enables one;
an explicit empty list, duplicate role, unknown role or different capitalization
is rejected. `shale serve camera-worker` is the Worker-only managed entry point.
The existing `shale serve producer` keeps its standalone static/file/manual
configuration behavior. `shale camera worker --host-config ...` and the other
local camera host-config commands remain separate compatibility paths. `serve
all` still runs the CP APIs, Storage Node and Relay; it does not add edge roles.

For an enabled managed Producer, the catalog and credential provider default to
CP. Local/demo sources, a file/static/custom catalog, local credential directory,
custom credential provider, local Set alias or pushed inputs are rejected. A CP
assignment selector is preserved. The Producer's bounded one-second
resynchronization still runs; a provider's `poll_interval` does not lengthen that
check or promise an exact request frequency. Inactive role sections are ignored so one shared file can still configure independent roles.
Camera policy, presets, bootstrap intent and credential material come from the
authorized CP workflow, never additional edge policy or password files.

The supervisor validates both selected configurations and constructs both hosts
before starting either role's CP or LAN loop. Constructors may create/read the
private machine state and keys. State directories must differ even after
resolving existing symlinks and the closest existing parent of an absent path;
an unresolved link or a regular file is rejected. Do not reuse one role's state
directory for the other role.

An unexpected role return starts only that role's fresh instance after the
bounded retry interval. Its sibling keeps running. A canceled attempt context is
not reused; process cancellation reaches both roles and waits for their cleanup.
Retry diagnostics contain only a fixed role and code. The process does not echo
transport errors, endpoints, full configuration or credential material.

This supervision isolates recoverable role failures. The roles share a process,
memory and operating-system fate: a panic, process crash or OOM can stop both and
lose Producer data that was only in RAM. Use an external service manager to
restart the host. Run the roles as separate processes when independent process
failure is required; their separate machine identities remain the same model.

Streaming credential caching is off by default. Explicitly opt in only when the
Producer role is enabled:

```yaml
edge:
  roles: [worker, producer]
  streaming_cache:
    max_age: 60s
    max_entries: 32
```

`max_age` is mandatory when that section exists and must be 1–300 seconds.
`max_entries` defaults to 32 and is at most 64. The cache holds only narrowly
authorized streaming credentials with an actual matching local applied proof;
it does not cache bootstrap/management access or replace the current catalog,
assignment authorization or a new BeginApply ticket. It is not offline cold
startup or permission to activate a new capture while CP is unavailable.
