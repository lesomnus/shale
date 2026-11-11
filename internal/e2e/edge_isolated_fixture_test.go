package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/shale/api"
	camapi "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// The factory deliberately stops before camera discovery or policy activation.
// Fault cases can prepare inventory/imports and arm gates without racing a
// prestarted enrollment. Edge roles use the installed providers and LAN drivers.
type isolatedFixture struct {
	T               *testing.T
	Ctx             context.Context
	Topology        isolatedTopology
	Control         *qualificationProcess
	ControlState    qualificationControlStatus
	Admin           *grpc.ClientConn // backend direct; never routed through a fault gate
	Proxy           *edgeRPCFaultProxy
	EdgeState       string
	Edges           []*qualificationProcess
	All             []*qualificationProcess
	EdgeRoles       map[string]*qualificationProcess
	Set             *api.Set
	Producer        *api.Producer
	Worker          *api.CameraWorker
	Network         *api.CameraNetwork
	Assignment      *api.CameraAssignment
	Camera          map[string]*qualificationProcess
	Device          map[string]*api.CameraDevice
	Policy          *api.CameraPolicy
	Preset          *api.CameraPreset
	combined        bool
	identities      map[string]cameratest.CameraIdentity
	stored          map[string]edgeRuntimeStoredRole
	secretNeedles   []string
	paused          *api.CameraPolicy
	initialUsername string
	allowActivation bool
}

type isolatedMedia struct {
	Job     *api.CameraJob
	Binding *api.CameraBinding
	Record  *api.Lamina
	Live    edgeMediaEvidence
}

func newIsolatedFixture(t *testing.T, combined bool) *isolatedFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	t.Cleanup(cancel)
	f := &isolatedFixture{T: t, Ctx: ctx, Topology: qualificationTopology(t), combined: combined, Camera: map[string]*qualificationProcess{}, Device: map[string]*api.CameraDevice{}, EdgeRoles: map[string]*qualificationProcess{}, identities: map[string]cameratest.CameraIdentity{}, stored: map[string]edgeRuntimeStoredRole{}}
	cpState := qualificationState(t, f.Topology.ApplicationUID, f.Topology.ApplicationGID)
	f.Control = startQualificationProcess(t, ctx, f.Topology.Roles["cp"], "TestEdgeControlProcess", "SHALE_EDGE_CONTROL_PROCESS")
	f.All = append(f.All, f.Control)
	qualificationCall(t, f.Control, "start", map[string]any{"state_dir": cpState, "bind_ip": f.Topology.Roles["cp"].Address}, &f.ControlState)
	require.True(t, f.ControlState.Running)
	qualificationActor(t, f.Control)
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	var err error
	f.Admin, err = grpc.NewClient(f.ControlState.Endpoints["tenant_api"], opts...)
	require.NoError(t, err)
	t.Cleanup(func() { f.Admin.Close() })
	qualificationSmallRecords(t, ctx, f.ControlState.Endpoints["cluster_api"])
	f.Proxy, err = newEdgeRPCFaultProxy(ctx, net.JoinHostPort(f.Topology.Roles["cp"].Address, "0"), f.ControlState.Endpoints["tenant_api"])
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Proxy.Close()) })
	f.Set = sourceCatalogSet(t, f.Admin, "isolated-fixture-edge")
	f.EdgeState = qualificationState(t, f.Topology.ApplicationUID, f.Topology.ApplicationGID)
	f.StartEdges()
	f.Producer = adoptProducer(t, ctx, api.NewProducerServiceClient(f.Admin), f.Set)
	workers := api.NewCameraWorkerServiceClient(f.Admin)
	require.Eventually(t, func() bool {
		rows, err := workers.List(ctx, api.CameraWorkerListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, w := range rows.GetItems() {
			if w.GetState() == api.HostState_HOST_STATE_PENDING {
				f.Worker = w
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "isolated Worker joins before any policy exists")
	f.Worker, err = workers.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: f.Worker.GetId()}.Build(), Alias: "isolated-fixture-worker"}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := api.NewProducerServiceClient(f.Admin).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Producer.GetId()}.Build()}.Build())
		return err == nil && v.GetStatus().GetDateReported() != nil && len(v.GetStatus().GetSources()) == 0
	}, 20*time.Second, 100*time.Millisecond, "adopted Producer heartbeats with an empty catalog")
	for _, role := range []string{"worker", "producer"} {
		f.RoleStatus(role)
	}
	f.Network, err = api.NewCameraNetworkServiceClient(f.Admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: f.Set.GetTenant().GetId()}.Build(), Alias: "isolated-fixture-lan", Cidrs: []string{f.Topology.Roles["edge"].CameraCIDR}, InterfaceHint: f.Topology.Roles["edge"].CameraInterface, Multicast: true}.Build())
	require.NoError(t, err)
	f.Assignment, err = api.NewCameraAssignmentServiceClient(f.Admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: f.Network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: f.Producer.GetId()}.Build(), WorkerSlot: "isolated-fixture-edge", WorkerId: f.Worker.GetId()}.Build())
	require.NoError(t, err)
	return f
}

func isolatedProcessDone(p *qualificationProcess) bool {
	if p == nil {
		return true
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (f *isolatedFixture) startRoles(roles ...string) *qualificationProcess {
	f.T.Helper()
	for _, role := range roles {
		require.True(f.T, isolatedProcessDone(f.EdgeRoles[role]), "an existing role incarnation must stop before replacement")
	}
	p := f.startRoleAt(f.EdgeState, "edge", roles)
	f.Edges = append(f.Edges, p)
	for _, role := range roles {
		f.EdgeRoles[role] = p
	}
	return p
}

func (f *isolatedFixture) startRoleAt(state, topologyRole string, roles []string) *qualificationProcess {
	f.T.Helper()
	require.NotEmpty(f.T, f.Topology.Roles[topologyRole].ExecPrefix)
	p := startQualificationProcess(f.T, f.Ctx, f.Topology.Roles[topologyRole], "TestEdgeRuntimeProcess", "SHALE_EDGE_RUNTIME_PROCESS")
	f.All = append(f.All, p)
	qualificationCall(f.T, p, "start", map[string]any{"config": map[string]any{"state": state, "cp": "http://" + f.Proxy.Address(), "tenant": "acme", "dev": true, "edge": map[string]any{"roles": roles, "retry_interval": "100ms"}, "producer": map[string]any{"buffer": "64MiB", "uplink": "20Mbps", "segment_duration": "4s", "heartbeat_interval": "200ms", "drain_timeout": "5s"}, "camera_worker": map[string]any{"heartbeat_interval": "200ms", "retry_interval": "100ms", "lease_seconds": 300, "discovery_interval": "1s", "discovery_timeout": "400ms"}}}, nil)
	qualificationActor(f.T, p)
	return p
}

// Replacement OS hardware identity and privileges belong to the topology's
// fixed exec_prefix. This only supplies a NEW private role state; adoption,
// assignment and key/identity assertions remain explicit CP actions by the case.
func (f *isolatedFixture) StartReplacement(roles []string) *qualificationProcess {
	f.T.Helper()
	return f.startRoleAt(qualificationState(f.T, f.Topology.ApplicationUID, f.Topology.ApplicationGID), "replacement", roles)
}
func (f *isolatedFixture) StartEdges() {
	if f.combined {
		f.startRoles("worker", "producer")
	} else {
		f.StartProducer()
		f.StartWorker()
	}
}
func (f *isolatedFixture) StartWorker() *qualificationProcess   { return f.startRoles("worker") }
func (f *isolatedFixture) StartProducer() *qualificationProcess { return f.startRoles("producer") }
func (f *isolatedFixture) retire(p *qualificationProcess) {
	for role, current := range f.EdgeRoles {
		if current == p {
			delete(f.EdgeRoles, role)
		}
	}
	var current []*qualificationProcess
	for _, entry := range f.Edges {
		if entry != p {
			current = append(current, entry)
		}
	}
	f.Edges = current
}
func (f *isolatedFixture) StopEdges() {
	f.T.Helper()
	for _, p := range append([]*qualificationProcess(nil), f.Edges...) {
		if !isolatedProcessDone(p) {
			qualificationCall(f.T, p, "stop", nil, nil)
		}
		f.retire(p)
	}
}
func (f *isolatedFixture) CrashRole(role string) {
	f.T.Helper()
	p := f.EdgeRoles[role]
	require.NotNil(f.T, p)
	f.RoleStatus(role)
	ctx, cancel := context.WithTimeout(f.T.Context(), 10*time.Second)
	defer cancel()
	require.NoError(f.T, p.Crash(ctx), "owned role crash did not join")
	f.retire(p)
}
func (f *isolatedFixture) RestartRole(role string) *qualificationProcess {
	f.T.Helper()
	if p := f.EdgeRoles[role]; p != nil {
		if !isolatedProcessDone(p) {
			f.RoleStatus(role)
			qualificationCall(f.T, p, "stop", nil, nil)
		}
		f.retire(p)
	}
	var p *qualificationProcess
	if f.combined {
		p = f.startRoles("worker", "producer")
	} else {
		p = f.startRoles(role)
	}
	f.RoleStatus(role)
	return p
}
func (f *isolatedFixture) RoleStatus(role string) edgeRuntimeStoredRole {
	f.T.Helper()
	p := f.EdgeRoles[role]
	if p == nil || isolatedProcessDone(p) {
		v, ok := f.stored[role]
		require.True(f.T, ok, "stored role identity unavailable")
		return v
	}
	var got edgeRuntimeStoredRole
	require.Eventually(f.T, func() bool {
		var state edgeRuntimeStatus
		qualificationCall(f.T, p, "status", nil, &state)
		v, ok := state.Roles[role]
		if !ok || v.IdentityState != "adopted" || v.SPKISHA256 == "" || v.ID == "" {
			return false
		}
		got = v
		return true
	}, 20*time.Second, 100*time.Millisecond, "role stored key/certificate must belong to its approved identity")
	if prior, ok := f.stored[role]; ok {
		require.Equal(f.T, prior.SPKISHA256, got.SPKISHA256, "role key changed across restart")
		require.Equal(f.T, prior.ID, got.ID, "role identity changed across restart")
	}
	f.stored[role] = got
	return got
}
func (f *isolatedFixture) WaitRoleHeartbeat(role string, after time.Time) {
	f.T.Helper()
	require.Eventually(f.T, func() bool {
		if role == "worker" {
			v, err := api.NewCameraWorkerServiceClient(f.Admin).Get(f.Ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: f.Worker.GetId()}.Build()}.Build())
			return err == nil && v.GetDateSeen() != nil && v.GetDateSeen().AsTime().After(after)
		}
		v, err := api.NewProducerServiceClient(f.Admin).Get(f.Ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Producer.GetId()}.Build()}.Build())
		return err == nil && v.GetStatus().GetDateReported() != nil && v.GetStatus().GetDateReported().AsTime().After(after)
	}, 20*time.Second, 100*time.Millisecond, "the original machine must authenticate after restart")
}

func (f *isolatedFixture) StartCamera(role string, active bool, user, password string, identity cameratest.CameraIdentity) edgeCameraStatus {
	f.T.Helper()
	require.NotEmpty(f.T, f.Topology.Roles[role].ExecPrefix)
	require.True(f.T, isolatedProcessDone(f.Camera[role]))
	if identity.DiscoveryID == "" {
		sum := sha256.Sum256([]byte(role))
		sum[6] = sum[6]&15 | 64
		sum[8] = sum[8]&63 | 128
		identity.DiscoveryID = fmt.Sprintf("urn:uuid:%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
	}
	if identity.Serial == "" {
		identity.Serial = "isolated-" + role
	}
	f.identities[role] = identity
	f.rememberSecret(user, password)
	p := startQualificationProcess(f.T, f.Ctx, f.Topology.Roles[role], "TestEdgeCameraProcess", "SHALE_EDGE_CAMERA_PROCESS")
	f.All = append(f.All, p)
	f.Camera[role] = p
	before := camapi.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128000, GOP: 10}
	after := before
	after.Width, after.Height, after.Bitrate = 320, 180, 1000000
	var state edgeCameraStatus
	qualificationCall(f.T, p, "start", map[string]any{"ip": f.Topology.Roles[role].Address, "interface": f.Topology.Roles[role].Interface, "http_port": 8080, "rtsp_port": 8554, "profile": before, "modes": []camapi.MediaProfile{before, after}, "username": user, "password": password, "active": active, "identity": identity}, &state)
	qualificationActor(f.T, p)
	return state
}
func (f *isolatedFixture) ObserveDevice(role string) *api.CameraDevice {
	f.T.Helper()
	identity, ok := f.identities[role]
	require.True(f.T, ok, "camera role was not started")
	var found *api.CameraDevice
	require.Eventually(f.T, func() bool {
		rows, err := api.NewCameraDeviceServiceClient(f.Admin).List(f.Ctx, api.CameraDeviceListRequest_builder{Filters: []*api.CameraDeviceFilter{api.CameraDeviceFilter_builder{Network: api.CameraNetworkRef_builder{Id: f.Network.GetId()}.Build()}.Build()}, Size: 100}.Build())
		if err != nil {
			return false
		}
		for _, d := range rows.GetItems() {
			if d.GetEnrollment().GetDiscoveryId() == identity.DiscoveryID {
				found = d
				return true
			}
		}
		return false
	}, 25*time.Second, 100*time.Millisecond, "multicast discovery must retain this exact physical inventory hint")
	if prior := f.Device[role]; prior != nil {
		require.Equal(f.T, prior.GetId(), found.GetId(), "rediscovery must preserve physical inventory")
	}
	f.Device[role] = found
	return found
}
func (f *isolatedFixture) Import(role, purpose, access, user, password string) *api.CameraCredentialReference {
	f.T.Helper()
	require.NotNil(f.T, f.Device[role])
	f.rememberSecret(user, password)
	material := credentialPayload(f.T, user, password)
	defer clear(material)
	request := api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: f.Device[role].GetId()}.Build(), Purpose: purpose, IntentKey: "isolated-import-" + role + "-" + purpose, Access: access, Material: material}.Build()
	if purpose == "streaming" {
		bindings, err := api.NewCameraBindingServiceClient(f.Admin).List(f.Ctx, api.CameraBindingListRequest_builder{Filters: []*api.CameraBindingFilter{api.CameraBindingFilter_builder{Assignment: api.CameraAssignmentRef_builder{Id: f.Assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: f.Device[role].GetId()}.Build()}.Build()}, Size: 100}.Build())
		require.NoError(f.T, err)
		require.Len(f.T, bindings.GetItems(), 1, "streaming import requires this camera's existing logical binding")
		request.SetBinding(api.CameraBindingRef_builder{Id: bindings.GetItems()[0].GetId()}.Build())
	}
	r, err := api.NewCameraCredentialServiceClient(f.Admin).Import(f.Ctx, request)
	require.NoError(f.T, err)
	return r.GetCredential()
}

func (f *isolatedFixture) ActivatePolicy(initialUsername string, allowActivation bool) {
	f.T.Helper()
	policies := api.NewCameraPolicyServiceClient(f.Admin)
	if f.Policy == nil {
		rule := api.CameraPresetRule_builder{Id: "isolated-fixture-main", Scope: api.CameraPresetScope_builder{TenantId: cameraPolicyID(f.T, f.Set.GetTenant().GetId())}.Build(), Settings: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264", Width: 320, Height: 180, Fps: 10, BitrateBps: 1000000, GopFrames: 10}.Build(), Preserve: []string{camapi.PresetFieldProfile}}.Build(), Required: []string{camapi.PresetFieldCodec, camapi.PresetFieldResolution}}.Build()
		var err error
		f.Preset, err = api.NewCameraPresetServiceClient(f.Admin).Create(f.Ctx, cameraPolicyPresetRequest("isolated-fixture-main", "1", rule))
		require.NoError(f.T, err)
		request := cameraPolicyCreateRequest(f.Network, f.Preset, "isolated-fixture-enrollment", "active-1")
		request.GetEnrollment().SetMaxDevices(2)
		request.GetEnrollment().SetAllowActivation(allowActivation)
		request.GetEnrollment().SetInitialUsername(initialUsername)
		request.GetEnrollment().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420V122, WebScheme: "http", WebPort: 8080}.Build())
		f.Policy, err = policies.Create(f.Ctx, request)
		require.NoError(f.T, err)
		f.initialUsername, f.allowActivation = initialUsername, allowActivation
	} else {
		require.Equal(f.T, f.initialUsername, initialUsername)
		require.Equal(f.T, f.allowActivation, allowActivation)
	}
	f.activate(f.Policy)
}
func (f *isolatedFixture) PauseEnrollment() {
	f.T.Helper()
	require.NotNil(f.T, f.Policy)
	if f.paused == nil {
		r := cameraPolicyCreateRequest(f.Network, f.Preset, "isolated-fixture-enrollment", "paused-1")
		r.GetEnrollment().SetAutomatic(false)
		r.GetEnrollment().SetMaxDevices(2)
		r.GetEnrollment().SetAllowActivation(f.allowActivation)
		r.GetEnrollment().SetInitialUsername(f.initialUsername)
		r.GetEnrollment().SetBootstrap(f.Policy.GetEnrollment().GetBootstrap())
		var err error
		f.paused, err = api.NewCameraPolicyServiceClient(f.Admin).Create(f.Ctx, r)
		require.NoError(f.T, err)
	}
	f.activate(f.paused)
}
func (f *isolatedFixture) activate(policy *api.CameraPolicy) {
	current, err := api.NewCameraNetworkServiceClient(f.Admin).Get(f.Ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: f.Network.GetId()}.Build()}.Build())
	require.NoError(f.T, err)
	if bytes.Equal(current.GetActivePolicyId(), policy.GetId()) {
		f.Network = current
		return
	}
	f.Network, err = api.NewCameraPolicyServiceClient(f.Admin).Activate(f.Ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: current.GetDateUpdated()}.Build())
	require.NoError(f.T, err)
}

func (f *isolatedFixture) WaitActive(role string) (*api.CameraJob, *api.CameraBinding) {
	f.T.Helper()
	require.NotNil(f.T, f.Device[role])
	var job *api.CameraJob
	var binding *api.CameraBinding
	require.Eventually(f.T, func() bool {
		rows, err := api.NewCameraBindingServiceClient(f.Admin).List(f.Ctx, api.CameraBindingListRequest_builder{Filters: []*api.CameraBindingFilter{api.CameraBindingFilter_builder{Assignment: api.CameraAssignmentRef_builder{Id: f.Assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: f.Device[role].GetId()}.Build()}.Build()}, Size: 100}.Build())
		if err != nil || len(rows.GetItems()) != 1 {
			return false
		}
		b := rows.GetItems()[0]
		jobs, err := api.NewCameraJobServiceClient(f.Admin).List(f.Ctx, api.CameraJobListRequest_builder{Filters: []*api.CameraJobFilter{api.CameraJobFilter_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build()}.Build()}, Size: 100, Reverse: true}.Build())
		if err != nil || len(jobs.GetItems()) == 0 {
			return false
		}
		j := jobs.GetItems()[0]
		app := b.GetAppliedApplication()
		starting := b.GetStartingApplication()
		target := app.GetTarget()
		if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE || !b.GetCollect() || j.GetPublishedRevision() < 1 || j.GetPublishedRevision() != b.GetDesired().GetRevision() || app.GetDateApplied() == nil || app.GetCaptureEpoch() == "" || app.GetCaptureEpoch() != starting.GetCaptureEpoch() || target.GetSpecRevision() != j.GetPublishedRevision() || !bytes.Equal(target.GetAssignmentId(), f.Assignment.GetId()) || target.GetAssignmentGeneration() != f.Assignment.GetGeneration() || !proto.Equal(target, starting.GetTarget()) {
			return false
		}
		job, binding = j, b
		return true
	}, 110*time.Second, 200*time.Millisecond, "this camera's latest job must qualify its exact deployed Source")
	return job, binding
}
func (f *isolatedFixture) VerifyMedia(role string, after ...*api.Lamina) isolatedMedia {
	f.T.Helper()
	job, binding := f.WaitActive(role)
	live := lifecycleLive(f.T, f.Admin, binding)
	evidence, err := qualifyWHEP(f.Ctx, live, edgeMediaOptions{})
	require.NoError(f.T, err)
	require.Equal(f.T, binding.GetAppliedApplication().GetCaptureEpoch(), evidence.CaptureEpoch)
	require.Greater(f.T, evidence.DecodedFrames, 1)
	require.Equal(f.T, int32(320), job.GetObserved().GetVerified().GetProfile().GetWidth())
	require.Equal(f.T, int32(180), job.GetObserved().GetVerified().GetProfile().GetHeight())
	record := lifecycleCommitted(f.T, f.Admin, binding, after...)
	qualificationPlayable(f.T, f.Ctx, f.Admin, "http://"+f.ControlState.Endpoints["tenant_http"], binding, record, 320, 180)
	return isolatedMedia{Job: job, Binding: binding, Record: record, Live: evidence}
}

func (f *isolatedFixture) rememberSecret(user, password string) {
	if password == "" {
		return
	}
	f.secretNeedles = append(f.secretNeedles, password, url.QueryEscape(password), url.PathEscape(password), base64.StdEncoding.EncodeToString([]byte(password)), base64.StdEncoding.EncodeToString([]byte(user+":"+password)))
}
func (f *isolatedFixture) FinalizeAndScan(passwords []string) {
	f.T.Helper()
	// Goexit or an expired operation context must still attempt ALL actors.
	// Use fixed nonfatal diagnostics so one broken incarnation cannot skip the
	// rest, and stop every replacement/old camera before the central services.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	stop := func(p *qualificationProcess) {
		if !isolatedProcessDone(p) {
			if _, err := p.request(ctx, "stop", nil); err != nil {
				f.T.Error("isolated actor stop failed")
			}
		}
		if err := p.finalize(ctx); err != nil {
			f.T.Error("isolated actor lifetime did not finish cleanly")
		}
	}
	seen := map[*qualificationProcess]bool{}
	for i := len(f.All) - 1; i >= 0; i-- {
		p := f.All[i]
		if p != f.Control && !seen[p] {
			seen[p] = true
			stop(p)
		}
	}
	if f.Control != nil {
		stop(f.Control)
	}
	f.Edges = nil
	f.EdgeRoles = map[string]*qualificationProcess{}
	for _, password := range passwords {
		f.rememberSecret("", password)
	}
	for _, p := range f.All {
		logs, overflow := p.logs.snapshot()
		if overflow {
			f.T.Error("isolated diagnostics exceeded bounded capture")
		}
		for _, needle := range f.secretNeedles {
			if needle != "" && strings.Contains(logs, needle) {
				f.T.Error("public isolated diagnostics exposed fixture credentials")
			}
		}
	}
	if err := f.Proxy.Close(); err != nil {
		f.T.Error("isolated RPC proxy did not close cleanly")
	}
}
