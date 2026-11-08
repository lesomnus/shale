package e2e_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/shale/api"
	camapi "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type qualificationControlStatus struct {
	Running   bool              `json:"running"`
	Endpoints map[string]string `json:"endpoints"`
}

func qualificationCall(t *testing.T, p *qualificationProcess, op string, fields map[string]any, out any) qualificationResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	response, err := p.request(ctx, op, fields)
	require.NoError(t, err, "isolated sidecar command %s", op)
	if out != nil {
		require.NoError(t, json.Unmarshal(response.Status, out))
	}
	if op == "stop" {
		require.NoError(t, p.finalize(ctx), "isolated sidecar finalization failed")
	}
	return response
}

func qualificationActor(t *testing.T, p *qualificationProcess) {
	t.Helper()
	data, err := osReadQualificationProc(p.command.Process.Pid)
	require.NoError(t, err)
	require.Equal(t, "65534", data["Uid"])
	require.Equal(t, "65534", data["Gid"])
	require.Equal(t, "1", data["NoNewPrivs"])
	for _, name := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		require.NotEmpty(t, data[name])
		require.Empty(t, strings.Trim(data[name], "0"), "application has %s", name)
	}
}

func TestEdgeIsolatedCentralWorkflow(t *testing.T) {
	_ = qualificationTopology(t)
	for _, combined := range []bool{true, false} {
		name := "independent"
		if combined {
			name = "combined"
		}
		t.Run(name, func(t *testing.T) { qualificationCentralWorkflow(t, combined) })
	}
}

func qualificationCentralWorkflow(t *testing.T, combined bool) {
	t.Helper()
	network := qualificationTopology(t)
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
	defer cancel()
	cpState := qualificationState(t, network.ApplicationUID, network.ApplicationGID)
	cp := startQualificationProcess(t, ctx, network.Roles["cp"], "TestEdgeControlProcess", "SHALE_EDGE_CONTROL_PROCESS")
	allProcesses := []*qualificationProcess{cp}
	var control qualificationControlStatus
	qualificationCall(t, cp, "start", map[string]any{"state_dir": cpState, "bind_ip": network.Roles["cp"].Address}, &control)
	require.True(t, control.Running)
	qualificationActor(t, cp)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	opts = append(opts, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	admin, err := grpc.NewClient(control.Endpoints["tenant_api"], opts...)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })
	qualificationSmallRecords(t, ctx, control.Endpoints["cluster_api"])
	set := sourceCatalogSet(t, admin, "isolated-edge")
	state := qualificationState(t, network.ApplicationUID, network.ApplicationGID)
	startEdge := func(roles []string) *qualificationProcess {
		p := startQualificationProcess(t, ctx, network.Roles["edge"], "TestEdgeRuntimeProcess", "SHALE_EDGE_RUNTIME_PROCESS")
		allProcesses = append(allProcesses, p)
		qualificationCall(t, p, "start", map[string]any{"config": map[string]any{"state": state, "cp": "http://" + control.Endpoints["tenant_api"], "tenant": "acme", "dev": true, "edge": map[string]any{"roles": roles, "retry_interval": "100ms"}, "producer": map[string]any{"buffer": "64MiB", "uplink": "20Mbps", "segment_duration": "4s", "heartbeat_interval": "200ms", "drain_timeout": "5s"}, "camera_worker": map[string]any{"heartbeat_interval": "200ms", "retry_interval": "100ms", "lease_seconds": 300, "discovery_interval": "1s", "discovery_timeout": "400ms"}}}, nil)
		qualificationActor(t, p)
		return p
	}
	var edge []*qualificationProcess
	if combined {
		edge = []*qualificationProcess{startEdge([]string{"worker", "producer"})}
	} else {
		edge = []*qualificationProcess{startEdge([]string{"producer"}), startEdge([]string{"worker"})}
	}
	producer := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	workers := api.NewCameraWorkerServiceClient(admin)
	var worker *api.CameraWorker
	require.Eventually(t, func() bool {
		rows, err := workers.List(ctx, api.CameraWorkerListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, candidate := range rows.GetItems() {
			if candidate.GetState() == api.HostState_HOST_STATE_PENDING {
				worker = candidate
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)
	worker, err = workers.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build(), Alias: "isolated-worker"}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, err := api.NewProducerServiceClient(admin).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: producer.GetId()}.Build()}.Build())
		return err == nil && current.GetStatus().GetDateReported() != nil && len(current.GetStatus().GetSources()) == 0
	}, 20*time.Second, 100*time.Millisecond)
	lan, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "isolated-camera-lan", Cidrs: []string{network.Roles["edge"].CameraCIDR}, InterfaceHint: network.Roles["edge"].CameraInterface, Multicast: true}.Build())
	require.NoError(t, err)
	assignment, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: lan.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "isolated-edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	const username, password = "admin", "CameraFixture42Pass"
	camera := startQualificationProcess(t, ctx, network.Roles["camera"], "TestEdgeCameraProcess", "SHALE_EDGE_CAMERA_PROCESS")
	allProcesses = append(allProcesses, camera)
	before := camapi.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128000, GOP: 10}
	after := before
	after.Width, after.Height, after.Bitrate = 320, 180, 1000000
	var cameraState edgeCameraStatus
	qualificationCall(t, camera, "start", map[string]any{"ip": network.Roles["camera"].Address, "interface": network.Roles["camera"].Interface, "http_port": 8080, "rtsp_port": 8554, "profile": before, "modes": []camapi.MediaProfile{before, after}, "username": username, "password": password, "active": false, "identity": cameratest.CameraIdentity{Serial: "isolated-camera-A", MAC: "02:00:00:00:00:0a", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-00000000020a"}}, &cameraState)
	qualificationActor(t, camera)
	require.False(t, cameraState.Active)
	var device *api.CameraDevice
	devices := api.NewCameraDeviceServiceClient(admin)
	require.Eventually(t, func() bool {
		rows, err := devices.List(ctx, api.CameraDeviceListRequest_builder{}.Build())
		if err == nil && len(rows.GetItems()) == 1 {
			device = rows.GetItems()[0]
			return device.GetEnrollment().GetCode() == "policy_missing"
		}
		return false
	}, 20*time.Second, 100*time.Millisecond, "real multicast discovery must cross the leased camera LAN")
	payload := credentialPayload(t, username, password)
	defer clear(payload)
	_, err = api.NewCameraCredentialServiceClient(admin).Import(ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), Purpose: "initial_admin", IntentKey: "isolated-initial-account", Access: "administrator", Material: payload}.Build())
	require.NoError(t, err)
	rule := api.CameraPresetRule_builder{Id: "isolated-main", Scope: api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, set.GetTenant().GetId())}.Build(), Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264", Width: 320, Height: 180, Fps: 10, BitrateBps: 1000000, GopFrames: 10}.Build(), Preserve: []string{camapi.PresetFieldProfile}}.Build(), Required: []string{camapi.PresetFieldCodec, camapi.PresetFieldResolution}}.Build()
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("isolated-main", "1", rule))
	require.NoError(t, err)
	request := cameraPolicyCreateRequest(lan, preset, "isolated-enrollment", "1")
	request.GetEnrollment().SetAllowActivation(true)
	request.GetEnrollment().SetInitialUsername(username)
	request.GetEnrollment().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420V122, WebScheme: "http", WebPort: 8080}.Build())
	policies := api.NewCameraPolicyServiceClient(admin)
	policy, err := policies.Create(ctx, request)
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: lan.GetDateUpdated()}.Build())
	require.NoError(t, err)
	jobs := api.NewCameraJobServiceClient(admin)
	var active *api.CameraJob
	var lastStage api.CameraJobStage
	var lastCode string
	var lastJob *api.CameraJob
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("isolated enrollment stage=%v code=%s", lastStage, lastCode)
		}
	})
	ready := assert.Eventually(t, func() bool {
		rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
		if err == nil && len(rows.GetItems()) == 1 {
			lastJob = rows.GetItems()[0]
			lastStage = rows.GetItems()[0].GetStage()
			lastCode = rows.GetItems()[0].GetObserved().GetCode()
		}
		if err == nil && len(rows.GetItems()) == 1 && rows.GetItems()[0].GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
			active = rows.GetItems()[0]
			return true
		}
		return false
	}, 100*time.Second, 200*time.Millisecond, "default isolated enrollment requires actual RTSP, current Producer and decoded WHEP")
	if !ready {
		qualificationReadiness(t, ctx, admin, producer, lastJob)
		t.FailNow()
	}
	require.Equal(t, assignment.GetId(), active.GetAssignment().GetId())
	require.Equal(t, policy.GetRevision(), active.GetInputs().GetPolicy().GetRevision())
	qualificationCall(t, camera, "status", nil, &cameraState)
	require.True(t, cameraState.Active)
	require.Equal(t, 1, cameraState.Activations)
	require.Positive(t, cameraState.ConfigurationWrites)
	require.Equal(t, after.Width, cameraState.Profile.Width)
	binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: active.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	initialEpoch := binding.GetAppliedApplication().GetCaptureEpoch()
	live := lifecycleLive(t, admin, binding)
	evidence, err := qualifyWHEP(ctx, live, edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, initialEpoch, evidence.CaptureEpoch)
	require.Equal(t, int32(after.Width), active.GetObserved().GetVerified().GetProfile().GetWidth())
	require.Greater(t, evidence.DecodedFrames, 1)
	committed := lifecycleCommitted(t, admin, binding)
	qualificationPlayable(t, ctx, admin, "http://"+control.Endpoints["tenant_http"], binding, committed, after.Width, after.Height)
	// Independent role processes reconstruct central state after their keys and
	// volatile capture memory are gone; the new epoch must contain new media.
	fingerprints := map[string]string{}
	for _, p := range edge {
		var stored edgeRuntimeStatus
		qualificationCall(t, p, "status", nil, &stored)
		for name, role := range stored.Roles {
			require.Equal(t, "adopted", role.IdentityState)
			require.NotEmpty(t, role.SPKISHA256)
			fingerprints[name] = role.SPKISHA256
		}
		qualificationCall(t, p, "stop", nil, nil)
	}
	restartedAt := time.Now()
	if combined {
		edge = []*qualificationProcess{startEdge([]string{"worker", "producer"})}
	} else {
		edge = []*qualificationProcess{startEdge([]string{"producer"}), startEdge([]string{"worker"})}
	}
	current := lifecycleApplied(t, admin, binding, initialEpoch)
	fresh := lifecycleCommitted(t, admin, current, committed)
	require.Equal(t, binding.GetSource().GetId(), current.GetSource().GetId())
	require.NotEqual(t, initialEpoch, fresh.GetCaptureEpoch())
	evidence, err = qualifyWHEP(ctx, lifecycleLive(t, admin, current), edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, current.GetAppliedApplication().GetCaptureEpoch(), evidence.CaptureEpoch)
	rows, err := jobs.List(ctx, api.CameraJobListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rows.GetItems(), 1)
	require.True(t, proto.Equal(active.GetInputs(), rows.GetItems()[0].GetInputs()))
	require.Eventually(t, func() bool {
		current, err := workers.Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: worker.GetId()}.Build()}.Build())
		return err == nil && current.GetDateSeen() != nil && current.GetDateSeen().AsTime().After(restartedAt)
	}, 15*time.Second, 100*time.Millisecond, "the same Worker must authenticate after restart too")
	for _, p := range edge {
		var stored edgeRuntimeStatus
		qualificationCall(t, p, "status", nil, &stored)
		for name, role := range stored.Roles {
			require.Equal(t, fingerprints[name], role.SPKISHA256, "role identity key changed across restart")
		}
	}
	qualificationCall(t, camera, "status", nil, &cameraState)
	require.Equal(t, 1, cameraState.Activations, "recovery cannot initialize a different password")
	for _, p := range edge {
		qualificationCall(t, p, "stop", nil, nil)
	}
	qualificationCall(t, camera, "stop", nil, nil)
	qualificationCall(t, cp, "stop", nil, nil)
	for _, p := range allProcesses {
		require.NoError(t, p.finalize(ctx))
		logs, overflow := p.logs.snapshot()
		require.False(t, overflow)
		for _, secret := range []string{password, url.QueryEscape(password), url.PathEscape(password), base64.StdEncoding.EncodeToString([]byte(username + ":" + password))} {
			if strings.Contains(logs, secret) {
				t.Error("public sidecar diagnostics exposed fixture credentials")
			}
		}
	}
	if !t.Failed() {
		mode := "independent"
		if combined {
			mode = "combined"
		}
		fmt.Println("SHQA_RESULT_V1 " + mode)
	}
}

func qualificationSmallRecords(t *testing.T, ctx context.Context, address string) {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	options = append(options, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	conn, err := grpc.NewClient(address, options...)
	require.NoError(t, err)
	defer conn.Close()
	policies := api.NewUploadPolicyServiceClient(conn)
	policy, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{Alias: "isolated-small", Version: 1, Bounds: api.UploadBounds_builder{MinLamina: 512 << 10, TargetLamina: 1 << 20, MaxLamina: 4 << 20}.Build()}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: policy.GetId()}.Build()}.Build())
	require.NoError(t, err)
}

func qualificationReadiness(t *testing.T, ctx context.Context, conn *grpc.ClientConn, producer *api.Producer, job *api.CameraJob) {
	t.Helper()
	if job == nil {
		return
	}
	binding, err := api.NewCameraBindingServiceClient(conn).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: job.GetBinding().GetId()}.Build()}.Build())
	if err != nil {
		t.Log("isolated binding diagnostics unavailable")
		return
	}
	app := binding.GetAppliedApplication()
	t.Logf("isolated readiness collect=%v starting=%v applied=%v current_epoch=%v live_frames=%d", binding.GetCollect(), binding.GetStartingApplication().GetCaptureEpoch() != "", app.GetCaptureEpoch() != "", app.GetCaptureEpoch() == binding.GetStartingApplication().GetCaptureEpoch(), job.GetObserved().GetLiveDecodedFrames())
	current, err := api.NewProducerServiceClient(conn).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: producer.GetId()}.Build()}.Build())
	if err == nil {
		for _, source := range current.GetStatus().GetSources() {
			t.Logf("isolated input up=%v fps=%g bitrate=%d restarts=%d has_error=%v epoch_matches=%v", source.GetInputUp(), source.GetFrameRate(), source.GetMeasuredBitrate(), source.GetCaptureRestarts(), source.GetError() != "", source.GetCaptureEpoch() == app.GetCaptureEpoch())
		}
	}
	records, err := api.NewLaminaServiceClient(conn).List(ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build()}.Build()}, Size: 100}.Build())
	if err == nil {
		complete := 0
		for _, record := range records.GetItems() {
			if record.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && !record.GetIncomplete() && record.GetCaptureEpoch() == app.GetCaptureEpoch() {
				complete++
			}
		}
		t.Logf("isolated recordings total=%d complete_current=%d", len(records.GetItems()), complete)
	}
}

func qualificationPlayable(t *testing.T, ctx context.Context, conn *grpc.ClientConn, base string, binding *api.CameraBinding, record *api.Lamina, width, height int) {
	t.Helper()
	response, err := api.NewLaminaServiceClient(conn).Export(ctx, api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build(), From: record.GetDateStarted(), To: record.GetDateEnded()}.Build())
	require.NoError(t, err)
	playback := exportDownload(t, base, response)
	stream, _ := videoExportPackets(t, playback.Media)
	require.Equal(t, width, stream.Width)
	require.Equal(t, height, stream.Height)
}
