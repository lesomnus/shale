package e2e_test

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/edge"
	"github.com/lesomnus/shale/internal/mediaprobe"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// This loopback development cluster exercises the production composition, not
// TLS qualification (covered by the CameraWorker/credential contract tests).
// Only discovery's UDP destination is injected. Camera drivers, credentials,
// publisher, Producer catalog and native WHEP decoder are the installed defaults.
func TestCameraWorkerManagedEdgeEnrollsAndDecodes(t *testing.T) {
	_, _ = exportPlaybackTools(t)
	for _, mode := range []string{"combined", "independent"} {
		t.Run(mode, func(t *testing.T) { managedEdgeEnrollsAndDecodes(t, mode == "combined") })
	}
}

func managedEdgeEnrollsAndDecodes(t *testing.T, combined bool) {
	t.Helper()
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set := sourceCatalogSet(t, admin, "managed-edge")
	before := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}
	after := before
	after.Width, after.Height, after.Bitrate = 320, 180, 1_000_000
	// ServeCamera's lower-level fixtures allow both transports to require the
	// same installed account, including the actual Producer's RTSP input.
	const username, password = "fixture-manager", "fixture-password:/@local-only"
	media := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{Profile: before, Username: username, Password: password})
	onvif := cameratest.ServeONVIFWithOptions(t, media.Profile(), cameratest.ONVIFOptions{Media: media, Modes: []cameramodel.MediaProfile{before, after}, Identity: cameratest.CameraIdentity{Manufacturer: "Synthetic", Model: "ManagedEdge", Serial: "managed-edge-camera", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-000000000200"}})
	onvif.RequireCredentials(username, password)
	target := onvif.ServeProbe(t, "127.0.0.1:0")
	state := t.TempDir()
	logs := &lifecycleLog{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	config := edge.Config{
		Producer:      &producer.Config{StateDir: filepath.Join(state, "producer"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: 4 * time.Second, HeartbeatInterval: 200 * time.Millisecond, AllocationHorizon: time.Minute, Buffer: 64 << 20, DrainTimeout: 5 * time.Second, Log: log},
		Worker:        &cameraworker.Config{StateDir: filepath.Join(state, "worker"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, HardwareID: "managed-edge-worker", HeartbeatInterval: 200 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 300, DiscoveryInterval: time.Second, DiscoveryTimeout: 400 * time.Millisecond, Discoverer: discovery.Discoverer{Target: target}, Log: log},
		RetryInterval: 100 * time.Millisecond, Log: log,
	}
	require.NoError(t, edge.Validate(config))
	launch := func() func() {
		runCtx, stop := context.WithCancel(ctx)
		configs := []edge.Config{config}
		if !combined {
			p, w := config, config
			p.Worker, w.Producer = nil, nil
			configs = []edge.Config{p, w}
		}
		var completed []chan error
		for _, cfg := range configs {
			done := make(chan error, 1)
			completed = append(completed, done)
			go func() { done <- edge.Run(runCtx, cfg) }()
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				stop()
				for _, done := range completed {
					select {
					case err := <-done:
						require.NoError(t, err)
					case <-time.After(15 * time.Second):
						t.Error("managed edge roles outlived cancellation")
					}
				}
			})
		}
	}
	stop := launch()
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("managed edge diagnostics: %s", logs.text())
		}
	})

	// Each role obtains separate operator approval and persists its own key.
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
	}, 30*time.Second, 100*time.Millisecond, "Worker joins independently of Producer")
	worker, err := workers.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build(), Alias: "managed-edge-worker"}.Build())
	require.NoError(t, err)
	require.NotEqual(t, host.GetId(), worker.GetId())
	require.Eventually(t, func() bool {
		v, err := api.NewProducerServiceClient(admin).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: host.GetId()}.Build()}.Build())
		return err == nil && v.GetStatus().GetDateReported() != nil && len(v.GetStatus().GetSources()) == 0
	}, 20*time.Second, 100*time.Millisecond, "managed Producer runs and heartbeats with an empty catalog")
	require.Equal(t, 0, media.Sessions(), "empty catalog cannot open an RTSP input")

	networks := api.NewCameraNetworkServiceClient(admin)
	network, err := networks.Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "managed-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo", Multicast: true}.Build())
	require.NoError(t, err)
	assignment, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "managed-edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	rule := api.CameraPresetRule_builder{Id: "fixture-main", Scope: api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, set.GetTenant().GetId())}.Build(), Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264", Width: int32(after.Width), Height: int32(after.Height), Fps: after.FPS, BitrateBps: after.Bitrate, GopFrames: int32(after.GOP)}.Build(), Preserve: []string{cameramodel.PresetFieldProfile}}.Build(), Required: []string{cameramodel.PresetFieldCodec, cameramodel.PresetFieldResolution}}.Build()
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("managed-main", "1", rule))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	policy, err := policies.Create(ctx, cameraPolicyCreateRequest(network, preset, "managed-enrollment", "1"))
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.NoError(t, err)

	// Discovery retains the pending enrollment intent; importing an
	// existing account enables the next scan without granting device I/O here.
	devices := api.NewCameraDeviceServiceClient(admin)
	var device *api.CameraDevice
	require.Eventually(t, func() bool {
		rows, err := devices.List(ctx, api.CameraDeviceListRequest_builder{Filters: []*api.CameraDeviceFilter{api.CameraDeviceFilter_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build()}.Build()}}.Build())
		if err == nil && len(rows.GetItems()) == 1 {
			device = rows.GetItems()[0]
			return device.GetEnrollment().GetCode() == "credentials_unavailable"
		}
		return false
	}, 20*time.Second, 100*time.Millisecond, "real UDP discovery reaches the CP default credential preparation boundary")
	require.Equal(t, 0, media.Sessions())
	material := credentialPayload(t, username, password)
	defer clear(material)
	imported, err := api.NewCameraCredentialServiceClient(admin).Import(ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), Purpose: "management", IntentKey: "installed-fixture-account", Access: "configure", Material: material}.Build())
	require.NoError(t, err)

	jobs := api.NewCameraJobServiceClient(admin)
	var active *api.CameraJob
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		call, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rows, err := jobs.List(call, api.CameraJobListRequest_builder{}.Build())
		if err == nil {
			for _, j := range rows.GetItems() {
				t.Logf("job stage=%v code=%s progress=%s", j.GetStage(), j.GetObserved().GetCode(), j.GetObserved().GetProgress().GetStage())
			}
		}
	})
	require.Eventually(t, func() bool {
		rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
		if err != nil || len(rows.GetItems()) != 1 {
			return false
		}
		job := rows.GetItems()[0]
		if job.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
			return false
		}
		active = job
		return true
	}, 100*time.Second, 200*time.Millisecond, "default Worker must configure, qualify, publish and independently decode live media")
	require.Equal(t, assignment.GetId(), active.GetAssignment().GetId())
	require.Equal(t, preset.GetRevision(), active.GetInputs().GetPreset().GetRevision())
	require.Nil(t, active.GetInputs().GetInitialCredential(), "an installed camera does not need a new initial account")
	require.Equal(t, imported.GetCredential().GetId(), active.GetInputs().GetManagementCredential().GetId())
	require.False(t, active.GetInputs().GetAllowActivation())
	verified := active.GetObserved().GetVerified()
	require.True(t, verified.GetDecoded(), "the installed verifier must actually decode RTSP")
	require.Greater(t, verified.GetDecodedFrames(), int64(1))
	require.Equal(t, int32(after.Width), verified.GetProfile().GetWidth())
	require.Equal(t, int32(after.Height), verified.GetProfile().GetHeight())
	require.Equal(t, after.Width, media.Profile().Width)
	require.Equal(t, after.Height, media.Profile().Height)
	writes, activations := onvif.Writes()
	require.Positive(t, writes, "preset must perform actual ONVIF configuration")
	require.Zero(t, activations, "an imported account must not activate or rotate a camera password")

	binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: active.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, binding.GetCollect())
	require.Equal(t, active.GetPublishedRevision(), binding.GetDesired().GetRevision())
	app := binding.GetAppliedApplication()
	require.NotEmpty(t, app.GetCaptureEpoch())
	require.Equal(t, active.GetPublishedRevision(), app.GetTarget().GetSpecRevision())
	require.Equal(t, active.GetObserved().GetLiveCaptureEpoch(), app.GetCaptureEpoch())
	require.Greater(t, active.GetObserved().GetLiveDecodedFrames(), int64(1), "default native observer must receive and decode fresh WHEP AUs")
	require.True(t, active.GetObserved().GetLiveLastFrameAt().AsTime().After(active.GetObserved().GetLiveFirstFrameAt().AsTime()))
	committed := lifecycleCommitted(t, admin, binding)
	require.Equal(t, app.GetCaptureEpoch(), committed.GetCaptureEpoch())
	// The independently downloaded MP4 must contain the NEW real camera
	// profile. This oracle does not trust CP metadata or Shale's MP4 parser.
	download, err := api.NewLaminaServiceClient(admin).Export(ctx, api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build(), From: committed.GetDateStarted(), To: committed.GetDateEnded()}.Build())
	require.NoError(t, err)
	playback := exportDownload(t, exportHTTPBase(t, c, cmd.SurfaceTenant), download)
	stream, _ := videoExportPackets(t, playback.Media)
	require.Equal(t, after.Width, stream.Width)
	require.Equal(t, after.Height, stream.Height)

	// Repeated discovery preserves the same job, Source and exact credential
	// versions. No test-side publisher or synthetic readiness can create ACTIVE.
	require.Eventually(t, func() bool {
		v, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
		return err == nil && v.GetEnrollment().GetCode() == "already_enrolled"
	}, 5*time.Second, 100*time.Millisecond)
	rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rows.GetItems(), 1)
	require.True(t, proto.Equal(active.GetInputs(), rows.GetItems()[0].GetInputs()))
	require.True(t, bytes.Equal(active.GetBinding().GetId(), rows.GetItems()[0].GetBinding().GetId()))

	// A normal same-state restart recovers both independently approved keys,
	// catalog and pinned references. It starts NEW capture media; ACTIVE's old
	// checkpoint is never used as evidence that the restarted live stream works.
	producerKey, err := (pki.Store{Dir: config.Producer.StateDir}).Key()
	require.NoError(t, err)
	workerKey, err := (pki.Store{Dir: config.Worker.StateDir}).Key()
	require.NoError(t, err)
	require.NotEqual(t, producerKey.Fingerprint(), workerKey.Fingerprint())
	oldEpoch := app.GetCaptureEpoch()
	stop()
	require.Zero(t, media.Sessions(), "cancellation joins and closes RTSP subprocesses")
	restartedAt := time.Now()
	stop = launch()
	current := lifecycleApplied(t, admin, binding, oldEpoch)
	require.Equal(t, binding.GetId(), current.GetId())
	require.Equal(t, binding.GetSource().GetId(), current.GetSource().GetId())
	require.Equal(t, binding.GetDesired().GetRevision(), current.GetDesired().GetRevision())
	newCommitted := lifecycleCommitted(t, admin, current, committed)
	require.NotEqual(t, oldEpoch, newCommitted.GetCaptureEpoch())
	live := lifecycleLive(t, admin, current)
	decoded, err := mediaprobe.Probe(ctx, mediaprobe.Options{}, mediaprobe.Endpoint{URL: live.GetWhepUrl(), Token: live.GetViewToken(), ExpectedEpoch: current.GetAppliedApplication().GetCaptureEpoch()})
	require.NoError(t, err)
	require.Greater(t, decoded.DecodedFrames, 1)
	require.Equal(t, after.Width, decoded.Width)
	require.Equal(t, after.Height, decoded.Height)
	require.Eventually(t, func() bool {
		v, err := workers.Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build()}.Build())
		return err == nil && v.GetDateSeen().AsTime().After(restartedAt)
	}, 10*time.Second, 100*time.Millisecond, "the same Worker recovers its approved machine identity")
	for _, state := range []struct{ path, fingerprint string }{{config.Producer.StateDir, producerKey.Fingerprint()}, {config.Worker.StateDir, workerKey.Fingerprint()}} {
		key, err := (pki.Store{Dir: state.path}).Key()
		require.NoError(t, err)
		require.Equal(t, state.fingerprint, key.Fingerprint())
	}
	rows, err = jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rows.GetItems(), 1)
	require.Equal(t, active.GetId(), rows.GetItems()[0].GetId())
	require.True(t, proto.Equal(active.GetInputs(), rows.GetItems()[0].GetInputs()), "restart preserves exact immutable credential/preset versions")
	newWrites, newActivations := onvif.Writes()
	require.Equal(t, writes, newWrites, "completed enrollment does not repeat configuration after restart")
	require.Equal(t, activations, newActivations)
	stop()
	require.Zero(t, media.Sessions())
}
