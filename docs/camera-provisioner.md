# Running the camera provisioner

The public `camera` and `provision` packages can be embedded in another Go
application. `shale camera` uses the same workflow locally or submits a durable
job to the standalone Payday HTTP host. A LAN worker is the same host with only
local assignments. Recording remains camera → Producer → relay/storage while
the provisioner is stopped.

This first version targets an **existing adopted Producer**. Applying a capture
manifest gracefully restarts that Producer, affecting all its feeds. Plans do
not change the camera; applying requires an explicit request and the request's
`allow_producer_restart` policy. Factory reset, firmware updates, cross-Set moves,
idle Producers, hot reload and a camera-management UI are outside this release.

## Local installation

Build `go build -o shale ./cmd/shale` with the repository's Go version. Install
`ffmpeg` and `ffprobe` on the worker. The service account needs network access to
the cameras, the CP and the assigned relay, read access to its configured secret
store, and permission to replace the Producer configuration and gracefully
restart its service. Configure those paths/commands as administrator-owned
configuration; jobs cannot supply them.

Keep host configuration and bearer/CP token files private (`0600`) and state and
credential directories private (`0700`). Use separate randomly generated service
tokens of at least 32 bytes, for example `openssl rand -hex 32 > operator.token`
with a private umask. Tokens and credential material are not command-line flags.
Remote worker/API connections require verified HTTPS; explicit loopback HTTP is
available for development. Use camera IP literals in configured CIDRs: the host
rejects DNS names and out-of-scope control/RTSP endpoints before resolving secrets.

Example `provisioner.yaml` (replace the example IDs with the existing assignment):

```yaml
state_dir: /var/lib/shale/provisioner
listen: 127.0.0.1:7415
allow_loopback_http: true
networks:
  - id: floor4
    cidrs: [10.1.2.0/24]
    interface: eth0
    multicast: true
providers:
  - name: cameras
    directory: /var/lib/shale/camera-secrets
targets:
  - scope: &assignment
      tenant_id: 00000000-0000-0000-0000-000000000001
      set_id: 00000000-0000-0000-0000-000000000002
      producer_id: 00000000-0000-0000-0000-000000000003
      network_id: floor4
    provider: cameras
    config_path: /etc/shale/producer.yaml
    restart: [systemctl, restart, shale-producer.service]
    cp:
      url: https://shale.example:7400
      ca_file: /etc/shale/ca.pem
      session_file: /etc/shale/provisioner-cp.session
clients:
  - token_file: /etc/shale/provisioner-operator.token
    scopes: [*assignment]
    networks: [floor4]
```

The CP identity must be authorized to inspect the assigned Producer/Set, the
**whole tenant alias namespace**, live readiness and committed recording. It
does not impersonate the Producer. The Producer creates/reuses its Source
through its normal adopted identity. Partial or forbidden alias enumeration is
an error, not evidence that an alias is free.

Sign the provisioner identity in to the **tenant** API with `shale --addr
https://shale.example:7400 login @tenant/provisioner`. The CLI stores the complete
`__Host-shale_tenant=…` cookie pair in its private user configuration directory
(`$XDG_CONFIG_HOME/shale/session-https___shale.example_7400`, or
`~/.config/shale/session-https___shale.example_7400`). Securely copy that file to
the example `session_file`, owned by the provisioner service account with mode
`0600`, or point `session_file` directly to the CLI's saved file. Keep the complete
pair, including its name, with at most one final newline. A development CP uses
`shale_tenant` instead. A cluster session, bare cookie value, bearer token, cookie
jar, or `Set-Cookie` header is not a tenant session file.

The provisioner rereads the file for each CP call, so a renewed login at the same
path takes effect without restarting it. The CP verifies the sealed session,
expiry and the person's current standing; an invalid or expired session cannot
complete provisioning. Sign in again when it expires (currently 24 hours) and
resume the existing job. This session is a provisioner control credential; it is
separate from camera passwords and the standalone host/worker bearer tokens.

Choose exactly one CP authentication method: `session_file`, `token_file` for an
endpoint that explicitly supports that bearer credential, or the matching
`cert_file`/`key_file` pair for an appropriately authorized mTLS identity. Current
Shale person sign-in uses the session file; putting its contents in `token_file`
does not authenticate it. Combining methods is refused. Worker HTTP connections
accept their configured bearer token and reject `session_file`. Plain HTTP is
available only with `allow_loopback_http: true` and a loopback CP URL.

The assigned Producer must also be able to resolve the exact streaming versions
written into the manifest. Its normal Shale configuration needs:

```yaml
producer:
  credentials:
    provider: cameras
    directory: /var/lib/shale/camera-secrets
  # Preserve the existing Producer identity, CP settings and sources.
```

Sharing the worker's private credential directory locally is the simplest path.
For a separate runtime store, preload the same immutable references/versions.
Configuring the provisioner's provider alone does not configure the Producer.
The deployer checks that its generated streaming reference can be resolved by
the configured Producer resolver before replacing the manifest.

The existing manifest must also contain a nonempty `producer.sources` sequence.
For an isolated lab, start and adopt a Producer with an explicit synthetic source
entry, such as `input: demo:testsrc2`. The `producer.demo` count alone creates
runtime sources without populating that editable manifest sequence.

Start the host with:

```sh
shale camera serve --host-config /etc/shale/provisioner.yaml
```

For a remote listener, replace the loopback address and supply `cert_file` and
`key_file`; keep `allow_loopback_http: false`. `network.ca_file` can add a camera
CA. `cp.ca_file` also trusts the CP-assigned relay during bounded live checking.

## Claim, prepare credentials, plan and apply

Discovery never allocates a physical identity or a Source. Inspect its candidates,
explicitly assign a physical `device.id` and a stable logical `binding_id`, then
keep those values across address changes/retries. Do not turn an IP/name/serial
hash into a universal identity. Conflicting authenticated hardware/serial
evidence requires an explicit replacement workflow.

The request below contains no passwords. `id` is a stable job key using letters,
digits, `_` or `-`; a new desired revision uses a new job ID and the same binding.

```json
{
  "id": "front-door-v1",
  "binding_id": "front-door",
  "scope": {
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "set_id": "00000000-0000-0000-0000-000000000002",
    "producer_id": "00000000-0000-0000-0000-000000000003",
    "network_id": "floor4"
  },
  "device": {
    "id": "camera-inventory-001",
    "endpoints": [{"url": "http://10.1.2.43:2020/onvif/device_service", "network_scope": "floor4"}]
  },
  "desired": {"profile_token": "main", "codec": "h264", "width": 1280, "height": 720, "fps": 30, "revision": "1"},
  "provider": "cameras",
  "allow_activation": false,
  "allow_producer_restart": true
}
```

Use actual profile tokens/capabilities from the camera. Unsupported settings do
not silently downgrade quality. Prepare management and streaming credentials
from private JSON files with `username`/`password` fields or `--material-file -`
for stdin. Use a streaming account with the permissions the camera requires;
the provisioner does not silently create users or enable extra services.

```sh
shale camera credential --host-config /etc/shale/provisioner.yaml --request request.json --purpose management --intent front-door-management-v1 --material-file /run/private/management.json > management-ref.json
shale camera credential --host-config /etc/shale/provisioner.yaml --request request.json --purpose streaming --intent front-door-streaming-v1 --material-file /run/private/streaming.json > streaming-ref.json
```

Copy only the resulting references into `management_ref` and `streaming_ref` in
the request. The command prints references only. Reusing an intent with different
secret material is refused; use a new explicit version/intent for future
rotation. Releasing a lease never deletes a password already installed on a
camera. Remove temporary material files when no longer needed using the secret
store's normal operational policy; do not commit them or include them in jobs.

Run directly without an HTTP listener:

```sh
shale camera discover --host-config /etc/shale/provisioner.yaml --network floor4
shale camera plan --host-config /etc/shale/provisioner.yaml --request request.json
shale camera apply --host-config /etc/shale/provisioner.yaml --request request.json
```

Or submit to the service with `--server https://provisioner.example --token-file
/etc/shale/provisioner-operator.token` and optional `--ca-file`. For an explicit
loopback development server, add `--allow-loopback-http`:

```sh
shale camera plan --server https://provisioner.example --token-file /etc/shale/provisioner-operator.token --request request.json --wait
shale camera status --server https://provisioner.example --token-file /etc/shale/provisioner-operator.token --id front-door-v1
shale camera resume --server https://provisioner.example --token-file /etc/shale/provisioner-operator.token --id front-door-v1 --apply --wait
```

Submitting an identical job is idempotent. Approve a previously submitted plan
using `resume --apply`; resubmitting the same job ID with a different mode/request
is a conflict. To submit a fresh apply job directly, use `camera apply`.
Replaying a completed job returns its saved result; it does not reapply old
settings over a newer revision. Use a new job ID for an explicit reconciliation
or current health qualification.

`Active` requires read-back, independent bounded RTSP decoding, successful
deployment, normal Source registration, live video and a new complete committed
recording. A row in the CP alone does not complete enrollment.

## First-time TP-Link activation

The first compatibility profile is the observed EasyCam C420 HW1.0 firmware
`1.2.2 Build260624`. The pinned adapter uses the verified first-password protocol
and delegates normal media operations to ONVIF. Add to the corresponding target:

```yaml
vendor:
  name: tplink
  web_endpoint: https://10.1.2.43
  compatibility_profile: easycam-c420-hw1-1.2.2-260624
```

Trust the camera's certificate through the network's `ca_file`. The compatibility
override only applies to that camera address; other cameras on the Producer use
the standard driver. Unknown activation mechanisms require manual activation.
For a genuinely fresh supported test device, prepare `--purpose initial_admin`
using the intended new password, add its reference as `initial_ref`, and set
`allow_activation: true`. Omitting `--intent` uses the engine's stable activation
intent. The credential is stored durably **before** any device write. Prepare a
separate streaming-purpose reference to the account used for actual RTSP.

Already initialized cameras skip activation. Reads do not call the web login
helper that changes the device clock. The C420 Media1 H.264 claim remains
unverified until Media2/read-back and the actual stream agree. Do not reset an
installed camera to create an activation test fixture.

## LAN workers

Run `shale camera worker --host-config worker.yaml` on a machine with camera-LAN
access and the assigned Producer's local configuration/secret store. It needs
verified HTTPS and a separate worker token. Configure a worker client entry with
the exact authorized scopes/networks and `worker: true`.

On the controller, replace that target's local paths/CP/restart settings with:

```yaml
worker:
  url: https://floor4-worker.example:7415
  ca_file: /etc/shale/workers-ca.pem
  token_file: /etc/shale/floor4-worker.token
```

Set the same `worker` connection under the controller's `networks` entry to route
discovery to the LAN worker. The worker uses its own configured interface/seeds,
authorization policy and local FileProvider. Callback objects, leases and secret
material are never serialized to it. A controller cannot assume multicast
crosses VLANs. Ordinary media never passes through the controller.

One controller owns a durable queue, and all its local targets share physical
identity reservations/locks. A worker also keeps durable runs and device locks.
Reconnect/restart replays the same request; there is no expiring camera-firmware
fencing token. Worker replacement must retain its state and stop the old worker
before another writer is assigned. Independent controllers/worker replicas with
unshared stores are not an automatic failover or a distributed lock service.

## API and recovery

All `/v1/` calls use `Authorization: Bearer …`; `/health` contains no inventory.
Bearer clients see only their exact configured assignments/networks.

| Request | Result |
| --- | --- |
| `POST /v1/discover` with `id`, `network_id`, optional address seeds | Durable discovery job |
| `POST /v1/runs` with `request`, `apply` | Durable inspect/plan/apply job |
| `GET /v1/runs`, `GET /v1/runs/{id}` | Visible jobs and normalized checkpoints |
| `POST /v1/runs/{id}/resume` with optional `apply` | Resume the same request |
| `POST /v1/runs/{id}/cancel` | Cancel pending work or interrupt its operation |
| `POST /v1/execute`, `/v1/checkpoint`, `/v1/discovery-execute` | Authenticated worker-only operations |

`credentials_required` and `input_required` preserve successful steps and exact
references. Restore the named credential version/provider access and resume.
`media_not_ready` retains deployment/binding history for readiness retry.
`unsupported`, `forbidden` and `conflict` require an explicit policy/device
decision, not password guessing, resetting, or silently adopting another Source.
Restart resumes jobs interrupted while running. A canceled operation must settle
before resume can start a new writer. Cancellation cannot undo a write already
accepted by the camera; the next apply re-inspects before deciding what to do.
If a camera write has an uncertain result, resume that original run before
submitting a different desired revision: its durable mutation reservation blocks
another run from overwriting a change whose completion is still unknown. Do not
clear that reservation to force a new writer; recover access/state and reconcile
the original intent first.

The service stores normalized state and references, not camera pictures. Keep
state backups private and maintain the separate recoverable secret store. Use
synthetic media and vendor-backed protocol fixtures for automated tests; actual
hardware runs are opt-in and must not reset production devices.
