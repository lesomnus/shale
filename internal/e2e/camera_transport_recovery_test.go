package e2e_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/edge"
	"github.com/lesomnus/shale/internal/mediaprobe"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// A recording camera pinned by its per-device key moves to an unowned
// address. The installed edge Worker verifies it there with a credential-free
// handshake, the CP moves it and enrolls a recovery job in one transaction,
// and the job reinspects it and republishes the same Source: video continues
// and the recording before the move stays playable. Only discovery's UDP
// destination is injected; drivers, credentials, publisher and Producer are
// the installed defaults.
func TestCameraTransportRecoveryKeepsRecording(t *testing.T) {
	_, _ = exportPlaybackTools(t)
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set := sourceCatalogSet(t, admin, "transport-edge")
	before := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}
	after := before
	after.Width, after.Height, after.Bitrate = 320, 180, 1_000_000
	const username, password = "fixture-manager", "fixture-password:/@local-only"
	const epr, mac, serial = "urn:uuid:3f1d2c4b-6a5e-4d7c-8b9a-0c1d2e3f4a5b", "02:00:00:00:33:01", "transport-recording-unit"
	media := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{ListenAddress: "127.0.0.2:0", Profile: before, Username: username, Password: password})
	onvif := cameratest.ServeONVIFWithOptions(t, media.Profile(), cameratest.ONVIFOptions{ListenAddress: "127.0.0.2:0", Media: media, Modes: []cameramodel.MediaProfile{before, after}, Identity: cameratest.CameraIdentity{Serial: serial, MAC: mac, DiscoveryID: epr}})
	onvif.RequireCredentials(username, password)
	key := cameratest.NewWebKey(t)
	web, err := url.Parse(onvif.ServeWebTLS(t, transportWebPort(t), key))
	require.NoError(t, err)
	port, err := strconv.Atoi(web.Port())
	require.NoError(t, err)
	camera := &cameratest.Camera{ONVIF: onvif, Media: media}
	target := onvif.ServeProbe(t, "127.0.0.1:0")
	state := t.TempDir()
	logs := &lifecycleLog{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	config := edge.Config{
		Producer:      &producer.Config{StateDir: filepath.Join(state, "producer"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: 4 * time.Second, HeartbeatInterval: 200 * time.Millisecond, AllocationHorizon: time.Minute, Buffer: 64 << 20, DrainTimeout: 5 * time.Second, Log: log},
		Worker:        &cameraworker.Config{StateDir: filepath.Join(state, "worker"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, HardwareID: "transport-edge-worker", HeartbeatInterval: 200 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 300, DiscoveryInterval: time.Second, DiscoveryTimeout: 400 * time.Millisecond, Discoverer: discovery.Discoverer{Target: target}, Log: log},
		RetryInterval: 100 * time.Millisecond, Log: log,
	}
	require.NoError(t, edge.Validate(config))
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- edge.Run(runCtx, config) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("edge outlived cancellation")
		}
		if t.Failed() {
			t.Logf("edge diagnostics: %s", logs.text())
		}
	})

	host := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	workers := api.NewCameraWorkerServiceClient(admin)
	var worker *api.CameraWorker
	require.Eventually(t, func() bool {
		rows, err := workers.List(ctx, api.CameraWorkerListRequest_builder{}.Build())
		if err == nil {
			for _, w := range rows.GetItems() {
				if w.GetState() == api.HostState_HOST_STATE_PENDING {
					worker = w
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)
	worker, err = workers.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build(), Alias: "transport-edge-worker"}.Build())
	require.NoError(t, err)
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "transport-edge-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo", Multicast: true}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "transport-edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	// The operator registers the camera and approves its key for it.
	devices := api.NewCameraDeviceServiceClient(admin)
	digest := sha256.Sum256([]byte(epr))
	device, err := devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), DeviceKey: "discovery/" + hex.EncodeToString(digest[:]), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: onvif.CurrentURL()}.Build()}}.Build())
	require.NoError(t, err)
	rule := api.CameraPresetRule_builder{Id: "fixture-main", Scope: api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, set.GetTenant().GetId())}.Build(), Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264", Width: int32(after.Width), Height: int32(after.Height), Fps: after.FPS, BitrateBps: after.Bitrate, GopFrames: int32(after.GOP)}.Build(), Preserve: []string{cameramodel.PresetFieldProfile}}.Build(), Required: []string{cameramodel.PresetFieldCodec, cameramodel.PresetFieldResolution}}.Build()
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("transport-main", "1", rule))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	request := cameraPolicyCreateRequest(network, preset, "transport-enrollment", "1")
	request.GetEnrollment().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420Known, WebScheme: "https", WebPort: int32(port)}.Build())
	request.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, device.GetId()), SpkiSha256: key.SPKI}.Build()})
	policy, err := policies.Create(ctx, request)
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
		return err == nil && v.GetEnrollment().GetCode() == "credentials_unavailable"
	}, 20*time.Second, 100*time.Millisecond, "discovery reaches the credential boundary")
	material := credentialPayload(t, username, password)
	defer clear(material)
	_, err = api.NewCameraCredentialServiceClient(admin).Import(ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), Purpose: "management", IntentKey: "installed-fixture-account", Access: "configure", Material: material}.Build())
	require.NoError(t, err)

	jobs := api.NewCameraJobServiceClient(admin)
	active := func(want int) []*api.CameraJob {
		t.Helper()
		var out []*api.CameraJob
		require.Eventually(t, func() bool {
			rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
			if err != nil || len(rows.GetItems()) != want {
				return false
			}
			for _, j := range rows.GetItems() {
				if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
					return false
				}
			}
			out = rows.GetItems()
			return true
		}, 100*time.Second, 200*time.Millisecond, "the Worker configures, qualifies, publishes and decodes live media")
		return out
	}
	first := active(1)[0]
	require.Equal(t, key.SPKI, first.GetInputs().GetDeviceTlsIdentity().GetSpkiSha256(), "the job pins the camera's key")
	binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: first.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	oldEpoch := binding.GetAppliedApplication().GetCaptureEpoch()
	history := lifecycleCommitted(t, admin, binding)
	recorded, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, recorded.GetIdentity().GetAuthenticated())
	require.Equal(t, serial, recorded.GetIdentity().GetSerial())
	attempts := len(onvif.AuthAttempts())

	// DHCP gives the camera another address. Its ports and key stay.
	moved, err := camera.RebindAddress("127.0.0.3")
	require.NoError(t, err)
	var accepted *api.CameraTransportCandidate
	require.Eventually(t, func() bool {
		v, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		for _, c := range v.GetTransport().GetCandidates() {
			if c.GetState() == "accepted" {
				accepted = c
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "the Worker verifies the moved camera's key")
	require.Equal(t, key.SPKI, accepted.GetPresentedSpkiSha256())
	require.Equal(t, moved.ONVIF, accepted.GetEndpoints()[0].GetUri())
	for _, a := range onvif.AuthAttempts()[attempts:] {
		require.Equal(t, "127.0.0.3", a.Host)
		require.True(t, a.At.After(accepted.GetDateDecided().AsTime()), "no credential reached the new address before its key was verified")
	}

	// The recovery job reuses every pin, reinspects and republishes.
	rows := active(2)
	var recovery *api.CameraJob
	for _, j := range rows {
		if j.GetRequestKey() == "transport/"+accepted.GetId() {
			recovery = j
		}
	}
	require.NotNil(t, recovery)
	require.Equal(t, accepted.GetJobId(), recovery.GetId())
	require.Equal(t, first.GetBinding().GetId(), recovery.GetBinding().GetId())
	in, last := recovery.GetInputs(), first.GetInputs()
	for _, pair := range [][2]proto.Message{{last.GetPolicy(), in.GetPolicy()}, {last.GetPreset(), in.GetPreset()}, {last.GetBootstrap(), in.GetBootstrap()}, {last.GetDeviceTlsIdentity(), in.GetDeviceTlsIdentity()}, {last.GetManagementCredential(), in.GetManagementCredential()}, {last.GetStreamingCredential(), in.GetStreamingCredential()}} {
		require.True(t, proto.Equal(pair[0], pair[1]), "the recovery keeps the last job's exact versions")
	}
	require.Nil(t, in.GetInitialCredential())
	require.False(t, in.GetAllowActivation())
	inspected := recovery.GetObserved().GetProgress().GetInspection().GetDevice()
	require.Equal(t, serial, inspected.GetSerial(), "the recovery reinspected the recorded serial")
	require.Equal(t, mac, inspected.GetMac(), "and MAC")
	_, activations := onvif.Writes()
	require.Zero(t, activations, "no activation")
	current, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, moved.ONVIF, current.GetEndpoints()[0].GetUri())
	require.True(t, proto.Equal(recorded.GetIdentity(), current.GetIdentity()), "the authenticated identity is unchanged")

	// The same binding and Source record at the new address: new live video
	// advances, a new recording commits, and the old one still plays.
	applied := lifecycleApplied(t, admin, binding, oldEpoch)
	require.Equal(t, binding.GetId(), applied.GetId())
	require.Equal(t, binding.GetSource().GetId(), applied.GetSource().GetId())
	input, err := url.Parse(applied.GetDesired().GetInput())
	require.NoError(t, err)
	require.Equal(t, "127.0.0.3", input.Hostname())
	require.Greater(t, applied.GetDesired().GetRevision(), binding.GetDesired().GetRevision())
	newer := lifecycleCommitted(t, admin, applied, history)
	require.NotEqual(t, oldEpoch, newer.GetCaptureEpoch())
	live := lifecycleLive(t, admin, applied)
	decoded, err := mediaprobe.Probe(ctx, mediaprobe.Options{}, mediaprobe.Endpoint{URL: live.GetWhepUrl(), Token: live.GetViewToken(), ExpectedEpoch: applied.GetAppliedApplication().GetCaptureEpoch()})
	require.NoError(t, err)
	require.Greater(t, decoded.DecodedFrames, 1)
	require.Equal(t, after.Width, decoded.Width)
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 1, "no second Source")
	for _, lamina := range []*api.Lamina{history, newer} {
		download, err := api.NewLaminaServiceClient(admin).Export(ctx, api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build(), From: lamina.GetDateStarted(), To: lamina.GetDateEnded()}.Build())
		require.NoError(t, err)
		playback := exportDownload(t, exportHTTPBase(t, c, cmd.SurfaceTenant), download)
		stream, _ := videoExportPackets(t, playback.Media)
		require.Equal(t, after.Width, stream.Width, "the recording before and after the move plays")
	}
}
