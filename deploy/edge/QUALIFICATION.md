# Managed edge qualification

This suite exercises the default CP-managed camera pipeline with generated
ONVIF/vendor/RTSP cameras. The coordinator, CP/Node/Relay, edge applications and
cameras use distinct processes in a disposable DHCP topology. It does not open
workplace cameras or reuse their recordings, credentials or state.

## Run the complete isolated matrix

From a Linux source checkout with Go and Docker on a dedicated test runner:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOMAXPROCS=2 GOFLAGS=-p=2 \
  go test -c -tags grpcnotrace -o /tmp/shale-edge-qualification.test ./internal/e2e
python3 deploy/edge/qualification_test.py \
  --test-binary /tmp/shale-edge-qualification.test --case all --timeout 1000
```

The runner verifies that the binary contains the three coordinator groups and
three application sidecars, builds the fixture image with real FFmpeg/ffprobe,
and runs it with `--network=none`, two CPUs, 4 GiB and a 512-process limit. The
image build may fetch distribution packages. Runtime tests have no external
network or host filesystem/socket mounts. Privilege belongs to the outer
coordinator for creating/deleting its own namespaces; each application prefix
runs as UID/GID 65534 with no capabilities or supplementary groups and with
`no_new_privs`. See the [topology contract](README.md#two-camera-qualification-topology).

`--case all` includes every group below. A successful process exit is insufficient:
the runner also requires each exact `SHQA_RESULT_V1` completion witness. A group
emits its witness only after its assertions and cleanup checks pass.

| Selector | Coordinator | Required outcome |
| --- | --- | --- |
| `combined` | `TestEdgeIsolatedCentralWorkflow/combined` | One edge process with independently approved Worker and Producer identities |
| `independent` | `TestEdgeIsolatedCentralWorkflow/independent` | Separate Worker/Producer processes consuming the same central policy and catalog |
| `recovery` | `TestEdgeIsolatedRecovery` | Six rollback, response-loss and crash checkpoints retain pinned inventory and usable credentials |
| `operations` | `TestEdgeIsolatedOperations` | Two cameras, DHCP movement, selective stop/capacity, uplink/CP recovery and approved replacement |

Use an individual selector while debugging. The full suite is required for
assembled qualification. The coordinator timeout is bounded by the runner;
the Go test process also has an 18-minute limit. Temporary namespaces, DHCP
processes and test state belong to the disposable container. The runner removes
its owned container and image on success, failure, timeout and handled termination.

## Evidence and acceptance boundaries

The contract is [Shale #202](https://github.com/lesomnus/shale/issues/202).
The following map separates assembled transport/media evidence from the focused
authority and compatibility tests. A declaration here does not assert that an
untested revision passed: use the CI result for the exact recorded source SHA.

| Acceptance | Assembled or focused evidence |
| --- | --- |
| Adopted zero-source host, actual Ethernet discovery and immutable CP preset | Both baseline modes begin idle, discover the DHCP camera, import/prepare central references, activate once and change the real encoded profile from 160×90 to 320×180 |
| Real live viewing and a new playable recording | Current Producer application/epoch, native WHEP epoch confirmation, complete advancing H264 access units, independent FFmpeg decoding and a new complete COMMITTED lamina; native MP4 export is independently demuxed and decoded |
| Duplicate observations, DHCP change and retained Source/history/pins | Operations reconcile the same binding and Source at a new leased address, preserve exact management/streaming credential and policy/preset references and keep the other camera active; recovery verifies immutable initialization inputs and inventory counts |
| Credential/configuration/publication uncertainty and process crashes | Recovery covers job-insert rollback, committed Observe response loss, activation response loss, accepted configuration, committed publication response loss and first capture/upload; original/prepared credential versions remain resolvable and activation/configuration do not repeat |
| Restarts, missing hints and CP/Wi-Fi outages | Baselines stop/start edge roles and require stable machine keys/Source with a new capture epoch and new media; recovery/operations exercise resnapshot and loss/restart paths. CP snapshot failure is not an empty catalog. The CP provider uses bounded resnapshot, so notification loss cannot be treated as proof of deletion |
| Selective add/change/stop, capacity and finite buffering | Operations keep an unrelated camera's capture epoch and new media while another changes/stops, and explicitly reject inventory capacity. Strict media admission and live-peer continuity also have focused Producer/Relay tests. The focused RAM test described below separately proves queue loss/recovery |
| Identity, scope, stale jobs and credential-cache policy | Actual TLS focused Worker/Credential tests cover old/revoked actors and exact grants; cache tests separately cover authenticated expiry, applied proof, tamper/revocation/stop purge and replacement scope. The assembled media fixtures use explicit development transport and do not claim all assembled paths use production TLS |
| Provider/command compatibility and media ceilings | Existing file/static/manual Producer configuration and real RTSP recording tests remain in normal/race CI; independent commands retain their own tests. `TestCameraDerivedCeilingsRecord` and Producer lifecycle/admission tests cover real media inference/ceiling behavior |

The six recovery subcases are `job_insert_rollback`,
`observe_committed_response_loss`, `activation_response_loss`,
`accepted_configuration`, `publication_committed_response_loss` and
`first_capture_upload`. The post-success RPC gate is a fixture transport fault;
it never grants the Worker broad catalog access or supplies a successful media
observation. Failed API calls alone do not establish rollback: tests compare
durable inventory, exact versions and actual device/media state.

`first_capture_upload` holds successful Set/Lamina allocation responses while
the real Producer captures and reports current-epoch input progress. The test
kills that Producer before its first upload capability is delivered, then
requires a new epoch, a newly committed complete recording and independently
decoded live/MP4 media. The hold is persistent across concurrent allocation
calls and caller cancellation. Actual partially transmitted/incomplete storage
is separately covered by `TestBacklogAfterOutageWritten`; this checkpoint does
not claim that an HTTP PUT had already started.

Transport recovery intentionally omits a new initial/activation credential grant.
Management and streaming pins remain exact, and the original initial account's
version remains centrally recoverable. Retaining usable initial material does
not grant a transport-recovery job permission to initialize the camera again.

The C420 process fixture uses a known synthetic imported account and vendor wire
shape plus authenticated ONVIF. RSA plaintext fidelity is covered separately by
`camera/vendors/tplink` tests `TestFirstPasswordProtocolAndIdempotentResume` and
`TestInterruptionAfterPasswordWriteReinspects`; assembled activation counters
do not replace those checks or qualify real firmware.

`TestSourceCatalogAuthorityAndRecovery` and
`TestPollingRecoversLostLocalNotifications` separately exercise committed
publication without a local hint and full-snapshot recovery. They distinguish a
CP error from a successful empty catalog without granting Worker-wide reads.

Focused authority evidence includes
`TestCameraWorkerAssignedJobsAndReplacementFences`,
`TestCameraCredentialWorkerProducerAndPurposeGrants`, and
`TestCameraWorkerRuntimeCancelsARealAssignedDeviceStep`. An approved replacement
must not overlap an old physical writer. These TLS/lease checks remain separate
from the development-transport topology's reachability and media proof.

Streaming credential caching is opt-in and cannot authorize offline cold startup
or a new BeginApply operation. Focused cache evidence includes
`TestStreamingCacheRequiresMediaAndPreservesAuthenticatedExpiry`,
`TestStreamingCacheDenialTamperAndStopPurge`,
`TestStreamingCachePrunesAuthenticatedExpiryAtCapacity`,
`TestCacheObserverBindsOnlineLeaseToActualSourceProofAndPurgesStop`, and
`TestEdgeCacheIsExplicitAndBounded`. Do not equate an assembled short outage with
proof of every cache expiry/revocation branch.

The focused `TestEdgeRAMBudgetLimitsUnstoredMediaAndRecovers` uses a small
volatile queue, actual RTSP and live decoding, injected upload failure, explicit
Lost/LostBytes counters, held-byte bounds and recovery to new playable media.
Its bound concerns tracked media queues and draining segments, not total process
RSS. A deliberately interrupted capture can leave an incomplete tail; readiness
must still use a newly complete recording from the current epoch rather than
counting that tail as successful media. The normal-suite
`TestBacklogAfterOutageWritten` supplies the positive committed-incomplete
storage classification check. A normal graceful stop does not imply that every
tail is incomplete.
Queue loss can occur before a lamina was allocated centrally; Lost/LostBytes
accounting does not imply that every dropped segment has a CP LOST row.

Configured GOP is not proof of a camera's actual random-access cadence.
[Shale #190](https://github.com/lesomnus/shale/issues/190) remains a separate gap.
Decoder-output timestamps prove decoding progress and do not measure sensor
capture time or camera-to-viewer latency. File/manual USB configuration tests do
not qualify physical USB hardware.

## CI and retained artifacts

The `edge-qualification` workflow compiles a fresh CGO-free Linux amd64 binary
from the explicitly checked-out source SHA and runs `--case all`. Its 32-minute
job budget includes compilation and the image build. Normal/race CI continues
to cover each feature child's focused contracts; this job adds assembled
process/network/media evidence.

The seven-day evidence artifact contains only `status.json` and `summary.txt`:
the source SHA, binary hash, exit code, four group witnesses, cleanup result and
an allowlisted test summary. Raw diagnostic logs, JSON request/status bodies,
configuration, test keys and media are excluded. The workflow removes its raw
temporary log and compiled binary after recording the result. Do not upload a
container filesystem or private state directory when diagnosing a failure.

## Optional physical PoE/Pi qualification

Physical qualification is a separate, explicit lab operation. Select the lab Pi,
camera serials, isolated PoE switch and test tenant/network first. Use the
[camera LAN recipe](README.md) and [managed edge setup](../../docs/managed-edge.md)
with separate Worker/Producer approvals. Keep camera DHCP off the corporate
uplink. Choose the cameras and authorize any recording, reconfiguration or
factory reset explicitly; stopped lab producers remain stopped until selected.

Verify actual DHCP acquisition/reboot, measured Wi-Fi throughput, power/USB
conditions, ONVIF identity, codec/profile, advancing live video, playable stored
media and recovery on that equipment. Synthetic namespace tests simulate a
Wi-Fi link and cannot establish radio, PoE power or hardware reliability. Use
test credentials and generated scenes where possible; workplace footage and
production secrets never belong in qualification artifacts. Restore the selected
lab services/network explicitly when the physical check ends.
