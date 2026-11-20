package e2e_test

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/edge"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/stretchr/testify/require"
)

// timeSyncCalls are the camera's ONVIF time operations, read or written.
func timeSyncCalls(c *cameratest.ONVIFCamera) []string {
	var out []string
	for _, call := range c.Calls() {
		if call == "GetNTP" || call == "SetNTP" || call == "SetSystemDateAndTime" {
			out = append(out, call)
		}
	}
	return out
}

// An edge with three C420s on 1.2.2, all about 147 days behind and keeping
// time by NTP from a server they cannot reach. The preset asks one of them
// to take its time from the edge, one whose firmware declines the ONVIF time
// operations, and nothing of the third. The installed managed edge enrolls
// all three: the first is pointed at the worker's own address on the camera
// network and its clock, as the worker reads it, comes right; the second is
// ACTIVE with an unsupported warning; the third is never asked about time.
func TestCameraWorkerTimeSyncOnAManagedEdge(t *testing.T) {
	_, _ = exportPlaybackTools(t)
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set := sourceCatalogSet(t, admin, "time-sync-edge")

	before := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}
	after := before
	after.Width, after.Height, after.Bitrate = 320, 180, 1_000_000
	const username, password = "fixture-manager", "fixture-password:/@local-only"
	// The bootstrap pins one web port for the network, so every camera
	// serves it, each on its own loopback host.
	port := transportWebPort(t)
	type fixture struct {
		serial string
		onvif  *cameratest.ONVIFCamera
		media  *cameratest.RTSPCamera
	}
	cameras := map[string]*fixture{}
	for i, serial := range []string{"time-edge", "time-unsupported", "time-untouched"} {
		// A camera's stream is on its own host, as ONVIF requires.
		host := "127.0.0." + strconv.Itoa(i+2)
		media := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{ListenAddress: host + ":0", Profile: before, Username: username, Password: password})
		onvif := cameratest.ServeONVIFWithOptions(t, media.Profile(), cameratest.ONVIFOptions{ListenAddress: net.JoinHostPort(host, strconv.Itoa(port)), Media: media, Modes: []cameramodel.MediaProfile{before, after}, Identity: cameratest.CameraIdentity{Serial: serial, MAC: "02:00:00:00:44:0" + strconv.Itoa(i+1), DiscoveryID: "urn:uuid:7d1c2b3a-0000-4000-8000-00000000440" + strconv.Itoa(i+1)}})
		onvif.RequireCredentials(username, password)
		onvif.SetClock(-147*day, cameramodel.ClockModeNTP, "GMT0GMT")
		onvif.SetNTPConfig(false, "pool.ntp.example")
		cameras[serial] = &fixture{serial: serial, onvif: onvif, media: media}
	}
	cameras["time-unsupported"].onvif.SetTimeSync(cameratest.TimeSyncBehavior{Unsupported: true})
	discoverer := &clockDiscoverer{}
	for _, f := range cameras {
		discoverer.cameras = append(discoverer.cameras, f.onvif)
	}

	state := t.TempDir()
	logs := &lifecycleLog{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	config := edge.Config{
		Producer:      &producer.Config{StateDir: filepath.Join(state, "producer"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: 4 * time.Second, HeartbeatInterval: 200 * time.Millisecond, AllocationHorizon: time.Minute, Buffer: 64 << 20, DrainTimeout: 5 * time.Second, Log: log},
		Worker:        &cameraworker.Config{StateDir: filepath.Join(state, "worker"), Cp: "http://" + c.running.TenantAddr, Tenant: "acme", Dev: true, HardwareID: "time-sync-worker", HeartbeatInterval: 200 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 300, DiscoveryInterval: time.Second, DiscoveryTimeout: 400 * time.Millisecond, Discoverer: discoverer, ClockInterval: time.Second, Log: log},
		RetryInterval: 100 * time.Millisecond, Log: log,
	}
	require.NoError(t, edge.Validate(config))
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- edge.Run(runCtx, config) }()
	var once sync.Once
	halt := func() {
		once.Do(func() {
			stop()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Error("managed edge outlived cancellation")
			}
		})
	}
	t.Cleanup(func() {
		halt()
		if t.Failed() {
			t.Logf("managed edge diagnostics: %s", logs.text())
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
	worker, err := workers.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build(), Alias: "time-sync-worker"}.Build())
	require.NoError(t, err)

	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "time-sync-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo"}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "time-sync", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	tenant := api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, set.GetTenant().GetId())}.Build()
	edgeKST := api.CameraPresetTimeSync_builder{Ntp: cameramodel.TimeSyncEdge, Timezone: "KST-9"}.Build()
	rules := []*api.CameraPresetRule{
		api.CameraPresetRule_builder{Id: "video", Scope: tenant, Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264", Width: int32(after.Width), Height: int32(after.Height), Fps: after.FPS, BitrateBps: after.Bitrate, GopFrames: int32(after.GOP)}.Build(), Preserve: []string{cameramodel.PresetFieldProfile}}.Build(), Required: []string{cameramodel.PresetFieldCodec, cameramodel.PresetFieldResolution}}.Build(),
	}
	for _, serial := range []string{"time-edge", "time-unsupported"} {
		rules = append(rules, api.CameraPresetRule_builder{Id: serial, Scope: tenant, Match: api.CameraPresetMatch_builder{Serial: serial}.Build(), Settings: api.CameraPresetSettings_builder{TimeSync: edgeKST, Video: api.CameraPresetVideo_builder{}.Build()}.Build()}.Build())
	}
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("time-sync", "1", rules...))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	request := cameraPolicyCreateRequest(network, preset, "time-sync-enrollment", "1")
	request.GetEnrollment().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420V122, WebScheme: "http", WebPort: int32(port)}.Build())
	policy, err := policies.Create(ctx, request)
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.NoError(t, err)

	devices := api.NewCameraDeviceServiceClient(admin)
	deviceOf := func(f *fixture) *api.CameraDevice {
		rows, err := devices.List(ctx, api.CameraDeviceListRequest_builder{Filters: []*api.CameraDeviceFilter{api.CameraDeviceFilter_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build()}.Build()}}.Build())
		require.NoError(t, err)
		for _, d := range rows.GetItems() {
			if slices.ContainsFunc(d.GetEndpoints(), func(ep *api.CameraObservedEndpoint) bool { return ep.GetUri() == f.onvif.CurrentURL() }) {
				return d
			}
		}
		return nil
	}
	for _, f := range cameras {
		var device *api.CameraDevice
		require.Eventually(t, func() bool {
			device = deviceOf(f)
			return device.GetEnrollment().GetCode() == "credentials_unavailable"
		}, 20*time.Second, 100*time.Millisecond, "%s reaches credential preparation", f.serial)
		material := credentialPayload(t, username, password)
		_, err := api.NewCameraCredentialServiceClient(admin).Import(ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), Purpose: "management", IntentKey: "installed-" + f.serial, Access: "configure", Material: material}.Build())
		clear(material)
		require.NoError(t, err)
	}

	jobs := api.NewCameraJobServiceClient(admin)
	jobOf := map[string]*api.CameraJob{}
	t.Cleanup(func() {
		if t.Failed() {
			rows, err := jobs.List(context.Background(), api.CameraJobListRequest_builder{}.Build())
			if err == nil {
				for _, j := range rows.GetItems() {
					var events []string
					for _, e := range j.GetObserved().GetProgress().GetEvents().GetItems() {
						events = append(events, e.GetCode())
					}
					t.Logf("job stage=%v code=%s progress=%s events=%v warnings=%v", j.GetStage(), j.GetObserved().GetCode(), j.GetObserved().GetProgress().GetStage(), events, j.GetObserved().GetWarnings())
				}
			}
		}
	})
	require.Eventually(t, func() bool {
		rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
		if err != nil || len(rows.GetItems()) != len(cameras) {
			return false
		}
		for _, j := range rows.GetItems() {
			if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
				return false
			}
		}
		for _, f := range cameras {
			id := cameraPolicyID(t, deviceOf(f).GetId())
			for _, j := range rows.GetItems() {
				if j.GetObserved().GetProgress().GetRequest().GetDevice().GetId() == id {
					jobOf[f.serial] = j
				}
			}
		}
		return len(jobOf) == len(cameras)
	}, 150*time.Second, 200*time.Millisecond, "every camera enrolls; time sync never fails an enrollment")

	// "edge" is the worker's own address toward the camera.
	probe, err := net.Dial("udp4", "127.0.0.2:123")
	require.NoError(t, err)
	worker127 := probe.LocalAddr().(*net.UDPAddr).IP.String()
	probe.Close()
	synced := cameras["time-edge"]
	fromDHCP, servers, mode, tz := synced.onvif.TimeConfig()
	require.False(t, fromDHCP)
	require.Equal(t, []string{worker127}, servers, "SetNTP named the worker's address on the camera network")
	require.Equal(t, cameramodel.ClockModeNTP, mode)
	require.Equal(t, "KST-9", tz)
	writes := slices.DeleteFunc(timeSyncCalls(synced.onvif), func(call string) bool { return call == "GetNTP" })
	require.Equal(t, []string{"SetNTP", "SetSystemDateAndTime"}, writes, "NTP, then the zone, once")
	require.Equal(t, "GetNTP", timeSyncCalls(synced.onvif)[0], "read before written")
	progress := jobOf["time-edge"].GetObserved().GetProgress()
	require.True(t, progress.GetTimeSync().GetSettled())
	require.Equal(t, worker127, progress.GetTimeSync().GetPlan().GetTimeSync().GetServer(), "the job journals the resolved server")
	require.Empty(t, jobOf["time-edge"].GetObserved().GetWarnings())
	// The worker's own reading of the camera's clock shows it right now.
	require.Eventually(t, func() bool {
		clock := deviceOf(synced).GetClock()
		offset := time.Duration(clock.GetOffsetMs()) * time.Millisecond
		bound := cameramodel.TimeSyncTolerance + time.Duration(clock.GetRttMs())*time.Millisecond
		return clock != nil && offset > -bound && offset < bound && clock.GetTz() == "KST-9"
	}, 20*time.Second, 100*time.Millisecond, "CameraDevice.clock shows a small offset after the sync")

	declined := cameras["time-unsupported"]
	require.Equal(t, []string{"GetNTP"}, slices.Compact(timeSyncCalls(declined.onvif)), "a camera that declines time sync is written nothing")
	_, servers, _, tz = declined.onvif.TimeConfig()
	require.Equal(t, []string{"pool.ntp.example"}, servers)
	require.Equal(t, "GMT0GMT", tz)
	warnings := jobOf["time-unsupported"].GetObserved().GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, "unsupported", warnings[0].GetCode())
	require.Equal(t, cameramodel.PresetFieldTimeSync, warnings[0].GetField())
	require.Equal(t, "ntp=edge timezone=KST-9", warnings[0].GetRequested())
	require.Equal(t, "unchanged", warnings[0].GetApplied())
	require.True(t, jobOf["time-unsupported"].GetObserved().GetProgress().GetTimeSync().GetSettled())

	untouched := cameras["time-untouched"]
	require.Empty(t, timeSyncCalls(untouched.onvif), "a preset without time_sync asks the camera nothing about time")
	require.Nil(t, jobOf["time-untouched"].GetObserved().GetProgress().GetTimeSync())
	require.Empty(t, jobOf["time-untouched"].GetObserved().GetWarnings())
	require.Eventually(t, func() bool {
		clock := deviceOf(untouched).GetClock()
		return clock != nil && clock.GetOffsetMs() < int64(-146*day/time.Millisecond)
	}, 20*time.Second, 100*time.Millisecond, "its clock was left alone")
	halt()
}
