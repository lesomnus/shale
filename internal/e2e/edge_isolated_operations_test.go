package e2e_test

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const isolatedOperationsPassword = "CameraOperations42Pass"

func TestEdgeIsolatedOperations(t *testing.T) {
	_ = qualificationTopology(t)
	ran := 0
	for _, test := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"two_cameras_dhcp_stop_capacity", isolatedOperationsCameras},
		{"wifi_and_control_restart", isolatedOperationsConnectivity},
		{"approved_machine_replacement", isolatedOperationsReplacement},
	} {
		t.Run(test.name, func(t *testing.T) { test.run(t); ran++ })
	}
	if !t.Failed() && ran == 3 {
		fmt.Println("SHQA_RESULT_V1 operations")
	}
}

func isolatedOperationsCamera(t *testing.T, f *isolatedFixture, role, suffix string) {
	t.Helper()
	f.StartCamera(role, false, "admin", isolatedOperationsPassword, cameratest.CameraIdentity{
		Serial: "isolated-operations-" + suffix, MAC: "02:00:00:00:03:" + suffix,
		DiscoveryID: "urn:uuid:00000000-0000-4000-8000-0000000003" + suffix,
	})
	f.ObserveDevice(role)
	f.Import(role, "initial_admin", "administrator", "admin", isolatedOperationsPassword)
}

func isolatedOperationsPins(t *testing.T, before, after *api.CameraJob) {
	t.Helper()
	for _, pair := range [][2]*api.CameraCredentialReference{
		{before.GetInputs().GetManagementCredential(), after.GetInputs().GetManagementCredential()},
		{before.GetInputs().GetStreamingCredential(), after.GetInputs().GetStreamingCredential()},
	} {
		require.True(t, proto.Equal(pair[0], pair[1]), "transport recovery changed an exact credential pin")
	}
	if after.GetInputs().GetAllowActivation() {
		require.True(t, proto.Equal(before.GetInputs().GetInitialCredential(), after.GetInputs().GetInitialCredential()))
	} else {
		// Address recovery deliberately drops first-password authority while
		// retaining the management/streaming versions. Its old initial version
		// remains recoverable centrally; it is not granted to this new job.
		require.Nil(t, after.GetInputs().GetInitialCredential())
	}
	require.True(t, proto.Equal(before.GetInputs().GetPolicy(), after.GetInputs().GetPolicy()))
	require.True(t, proto.Equal(before.GetInputs().GetPreset(), after.GetInputs().GetPreset()))
}

func isolatedOperationsRecoverable(t *testing.T, f *isolatedFixture, job *api.CameraJob) {
	t.Helper()
	expected := credentialPayload(t, "admin", isolatedOperationsPassword)
	defer clear(expected)
	for _, ref := range []*api.CameraCredentialReference{job.GetInputs().GetInitialCredential(), job.GetInputs().GetManagementCredential(), job.GetInputs().GetStreamingCredential()} {
		require.NotNil(t, ref)
		lease, err := api.NewCameraCredentialServiceClient(f.Admin).Resolve(f.Ctx, api.CameraCredentialResolveRequest_builder{Credential: ref}.Build())
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, lease.GetMaterial()), "an original exact credential version became unusable")
		clear(lease.GetMaterial())
	}
}

func isolatedOperationsMediaAfter(t *testing.T, f *isolatedFixture, role string, after time.Time) isolatedMedia {
	t.Helper()
	job, binding := f.WaitActive(role)
	var record *api.Lamina
	require.Eventually(t, func() bool {
		rows, err := api.NewLaminaServiceClient(f.Admin).List(f.Ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build()}.Build()}, Size: 100}.Build())
		if err != nil || len(rows.GetItems()) >= 100 {
			return false
		}
		for _, row := range rows.GetItems() {
			if row.GetDateCommitted() != nil && row.GetDateCommitted().AsTime().After(after) && row.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && !row.GetIncomplete() && row.GetCaptureEpoch() == binding.GetAppliedApplication().GetCaptureEpoch() {
				record = row
				return true
			}
		}
		return false
	}, 35*time.Second, 100*time.Millisecond, "storage must commit actual media after connectivity is restored")
	evidence, err := qualifyWHEP(f.Ctx, lifecycleLive(t, f.Admin, binding), edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, binding.GetAppliedApplication().GetCaptureEpoch(), evidence.CaptureEpoch)
	require.Greater(t, evidence.DecodedFrames, 1)
	qualificationPlayable(t, f.Ctx, f.Admin, "http://"+f.ControlState.Endpoints["tenant_http"], binding, record, 320, 180)
	return isolatedMedia{Job: job, Binding: binding, Record: record, Live: evidence}
}

func isolatedOperationsCameras(t *testing.T) {
	f := newIsolatedFixture(t, true)
	defer f.FinalizeAndScan([]string{isolatedOperationsPassword})
	network := newIsolatedNetworkControl(t)
	initialNetwork := network.Call(t, "status", "", "")
	isolatedOperationsCamera(t, f, "camera", "01")
	f.ActivatePolicy("admin", true)
	a := f.VerifyMedia("camera")
	// Inventory and import the second physical account before allowing automatic
	// preparation. A generated initial password cannot describe this fixture.
	f.PauseEnrollment()
	isolatedOperationsCamera(t, f, "camera2", "02")
	f.ActivatePolicy("admin", true)
	b := f.VerifyMedia("camera2")
	aAfterAdd := f.VerifyMedia("camera", a.Record)
	require.Equal(t, a.Live.CaptureEpoch, aAfterAdd.Live.CaptureEpoch, "adding another camera restarted unrelated capture")
	require.Equal(t, a.Binding.GetSource().GetId(), aAfterAdd.Binding.GetSource().GetId())

	moved := network.Call(t, "move_camera", "camera2", "192.168.250.30")
	require.Equal(t, initialNetwork.DHCPPID, moved.DHCPPID)
	require.Equal(t, initialNetwork.Cameras["camera2"].ClientPID, moved.Cameras["camera2"].ClientPID)
	require.Equal(t, initialNetwork.Cameras["camera2"].MAC, moved.Cameras["camera2"].MAC)
	require.Equal(t, []string{"192.168.250.30"}, moved.Cameras["camera2"].Addresses)
	require.Equal(t, initialNetwork.Cameras["camera"].Addresses, moved.Cameras["camera"].Addresses)
	qualificationCall(t, f.Camera["camera2"], "rebind", map[string]any{"ip": "192.168.250.30", "interface": "eth0", "preserve_ports": true}, nil)
	role := f.Topology.Roles["camera2"]
	role.Address = "192.168.250.30"
	f.Topology.Roles["camera2"] = role
	bindings := api.NewCameraBindingServiceClient(f.Admin)
	require.Eventually(t, func() bool {
		current, err := bindings.Get(f.Ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.Binding.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		input, err := url.Parse(current.GetDesired().GetInput())
		return err == nil && input.Hostname() == "192.168.250.30" && current.GetDesired().GetRevision() > b.Binding.GetDesired().GetRevision()
	}, 80*time.Second, 100*time.Millisecond, "DHCP observation must durably reconcile the existing Source")
	bMoved := f.VerifyMedia("camera2", b.Record)
	require.Equal(t, b.Binding.GetId(), bMoved.Binding.GetId())
	require.Equal(t, b.Binding.GetSource().GetId(), bMoved.Binding.GetSource().GetId())
	isolatedOperationsPins(t, b.Job, bMoved.Job)
	isolatedOperationsRecoverable(t, f, b.Job)
	var cameraState edgeCameraStatus
	qualificationCall(t, f.Camera["camera2"], "status", nil, &cameraState)
	require.Equal(t, 1, cameraState.Activations)
	aAfterMove := f.VerifyMedia("camera", aAfterAdd.Record)
	require.Equal(t, a.Live.CaptureEpoch, aAfterMove.Live.CaptureEpoch)

	// Natural duplicate WS-Discovery observations must not create another intent.
	rows, err := api.NewCameraJobServiceClient(f.Admin).List(f.Ctx, api.CameraJobListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, rows.GetItems(), 3, "two initial intents and one address recovery are sufficient")
	current, err := bindings.Get(f.Ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: bMoved.Binding.GetId()}.Build()}.Build())
	require.NoError(t, err)
	stopAt := time.Now()
	_, err = bindings.SetCollect(f.Ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: current.GetId()}.Build(), ExpectedVersion: current.GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		producer, err := api.NewProducerServiceClient(f.Admin).Get(f.Ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Producer.GetId()}.Build()}.Build())
		if err != nil || producer.GetStatus().GetDateReported() == nil || !producer.GetStatus().GetDateReported().AsTime().After(stopAt) {
			return false
		}
		for _, source := range producer.GetStatus().GetSources() {
			if bytes.Equal(source.GetSourceId(), b.Binding.GetSource().GetId()) && source.GetInputUp() {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond, "stopping one binding must stop only its capture")
	require.Eventually(t, func() bool {
		var state edgeCameraStatus
		qualificationCall(t, f.Camera["camera2"], "status", nil, &state)
		return state.Sessions == 0
	}, 15*time.Second, 100*time.Millisecond, "stopped capture must close its actual RTSP session")
	aAfterStop := f.VerifyMedia("camera", aAfterMove.Record)
	require.Equal(t, a.Live.CaptureEpoch, aAfterStop.Live.CaptureEpoch)
	qualificationPlayable(t, f.Ctx, f.Admin, "http://"+f.ControlState.Endpoints["tenant_http"], bMoved.Binding, b.Record, 320, 180)

	// Inventory capacity counts retained device bindings, including stopped
	// cameras. A third identity cannot silently replace either logical Source.
	qualificationCall(t, f.Camera["camera2"], "stop", nil, nil)
	delete(f.Device, "camera2") // The replacement fixture below is a NEW physical device.
	f.StartCamera("camera2", false, "admin", isolatedOperationsPassword, cameratest.CameraIdentity{
		Serial: "isolated-operations-third", MAC: "02:00:00:00:03:03", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-000000000303",
	})
	third := f.ObserveDevice("camera2")
	require.Eventually(t, func() bool {
		device, err := api.NewCameraDeviceServiceClient(f.Admin).Get(f.Ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: third.GetId()}.Build()}.Build())
		return err == nil && device.GetEnrollment().GetCode() == "capacity_exceeded"
	}, 20*time.Second, 100*time.Millisecond)
	qualificationCall(t, f.Camera["camera2"], "status", nil, &cameraState)
	require.Zero(t, cameraState.Activations)
	require.Zero(t, cameraState.ConfigurationWrites)
	counts := isolatedRecoveryCounts(t, f)
	require.EqualValues(t, 2, counts.Binding)
	require.EqualValues(t, 2, counts.Source)
	require.EqualValues(t, 3, counts.Job)
	final := f.VerifyMedia("camera", aAfterStop.Record)
	require.Equal(t, a.Live.CaptureEpoch, final.Live.CaptureEpoch)
}

func isolatedOperationsConnectivity(t *testing.T) {
	f := newIsolatedFixture(t, false)
	defer f.FinalizeAndScan([]string{isolatedOperationsPassword})
	network := newIsolatedNetworkControl(t)
	isolatedOperationsCamera(t, f, "camera", "11")
	f.ActivatePolicy("admin", true)
	initial := f.VerifyMedia("camera")
	worker, producer := f.RoleStatus("worker"), f.RoleStatus("producer")
	f.PauseEnrollment()
	isolatedOperationsCamera(t, f, "camera2", "12")
	qualificationCall(t, f.EdgeRoles["worker"], "stop", nil, nil)
	f.ActivatePolicy("admin", true)
	before := network.Call(t, "status", "", "")
	down := network.Call(t, "uplink_down", "", "")
	require.False(t, down.UplinkUp)
	// A fresh Worker session cannot turn an offline local observation into a
	// new central credential/job grant. Existing Producer capture stays active.
	f.StartWorker()
	renewed := network.Call(t, "renew_camera", "camera", "")
	require.Equal(t, before.DHCPPID, renewed.DHCPPID)
	require.Equal(t, before.Cameras["camera"].ClientPID, renewed.Cameras["camera"].ClientPID)
	// Observe continuous real RTSP sessions while the uplink cannot reach CP,
	// Relay or storage. A control read failure must not delete accepted capture.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var state edgeCameraStatus
		qualificationCall(t, f.Camera["camera"], "status", nil, &state)
		require.Positive(t, state.Sessions, "CP unavailability stopped existing LAN capture")
		qualificationCall(t, f.Camera["camera2"], "status", nil, &state)
		require.False(t, state.Active)
		require.Zero(t, state.Activations, "CP outage authorized a new initialization")
		time.Sleep(200 * time.Millisecond)
	}
	upAt := time.Now()
	network.Call(t, "uplink_up", "", "")
	isolatedRecoverySameRole(t, f, "worker", worker, upAt)
	isolatedRecoverySameRole(t, f, "producer", producer, upAt)
	recovered := isolatedOperationsMediaAfter(t, f, "camera", upAt)
	second := f.VerifyMedia("camera2")
	require.Equal(t, initial.Live.CaptureEpoch, recovered.Live.CaptureEpoch)
	isolatedOperationsPins(t, initial.Job, recovered.Job)
	qualificationCall(t, f.Control, "stop_cp", nil, nil)
	for i := 0; i < 20; i++ {
		var state edgeCameraStatus
		qualificationCall(t, f.Camera["camera"], "status", nil, &state)
		require.Positive(t, state.Sessions)
		time.Sleep(200 * time.Millisecond)
	}
	dhcp := network.Call(t, "restart_dhcp", "", "")
	require.NotEqual(t, before.DHCPPID, dhcp.DHCPPID)
	require.Equal(t, before.Cameras["camera"].Addresses, dhcp.Cameras["camera"].Addresses)
	network.Call(t, "renew_camera", "camera", "")
	restarted := time.Now()
	qualificationCall(t, f.Control, "restart_cp", nil, &f.ControlState)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	afterCP := isolatedOperationsMediaAfter(t, f, "camera", restarted)
	afterSecond := isolatedOperationsMediaAfter(t, f, "camera2", restarted)
	require.Equal(t, initial.Binding.GetSource().GetId(), afterCP.Binding.GetSource().GetId())
	require.Equal(t, initial.Live.CaptureEpoch, afterCP.Live.CaptureEpoch)
	isolatedOperationsPins(t, initial.Job, afterCP.Job)
	require.Equal(t, second.Live.CaptureEpoch, afterSecond.Live.CaptureEpoch)
	isolatedOperationsPins(t, second.Job, afterSecond.Job)
}

func isolatedOperationsReplacement(t *testing.T) {
	f := newIsolatedFixture(t, false)
	defer f.FinalizeAndScan([]string{isolatedOperationsPassword})
	isolatedOperationsCamera(t, f, "camera", "21")
	f.ActivatePolicy("admin", true)
	initial := f.VerifyMedia("camera")
	oldWorker, oldProducer := f.RoleStatus("worker"), f.RoleStatus("producer")
	oldProducerID := bytes.Clone(f.Producer.GetId())
	replacement := f.StartReplacement([]string{"worker", "producer"})
	workers := api.NewCameraWorkerServiceClient(f.Admin)
	producers := api.NewProducerServiceClient(f.Admin)
	var newWorker *api.CameraWorker
	var newProducer *api.Producer
	require.Eventually(t, func() bool {
		w, werr := workers.List(f.Ctx, api.CameraWorkerListRequest_builder{}.Build())
		p, perr := producers.List(f.Ctx, api.ProducerListRequest_builder{}.Build())
		if werr != nil || perr != nil {
			return false
		}
		for _, row := range w.GetItems() {
			if row.GetState() == api.HostState_HOST_STATE_PENDING && !bytes.Equal(row.GetId(), f.Worker.GetId()) {
				newWorker = row
			}
		}
		for _, row := range p.GetItems() {
			if row.GetState() == api.HostState_HOST_STATE_PENDING && !bytes.Equal(row.GetId(), f.Producer.GetId()) {
				newProducer = row
			}
		}
		return newWorker != nil && newProducer != nil
	}, 30*time.Second, 100*time.Millisecond, "a replacement must join as new unapproved OS hardware and keys")
	var pending edgeRuntimeStatus
	qualificationCall(t, replacement, "status", nil, &pending)
	for role, prior := range map[string]edgeRuntimeStoredRole{"worker": oldWorker, "producer": oldProducer} {
		require.NotEmpty(t, pending.Roles[role].SPKISHA256)
		require.NotEqual(t, prior.SPKISHA256, pending.Roles[role].SPKISHA256, "replacement copied an old private key")
	}
	// An unapproved replacement has no assignment or device write authority.
	var cameraState edgeCameraStatus
	qualificationCall(t, f.Camera["camera"], "status", nil, &cameraState)
	require.Equal(t, 1, cameraState.Activations)
	require.Equal(t, 1, cameraState.ConfigurationWrites)
	// Join every old role before approval. There can be no outstanding physical
	// writer racing the replacement when the versioned assignment changes.
	f.StopEdges()
	approvedAt := time.Now()
	var err error
	newWorker, err = workers.Adopt(f.Ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: newWorker.GetId()}.Build(), Alias: "isolated-approved-replacement"}.Build())
	require.NoError(t, err)
	assignments := api.NewCameraAssignmentServiceClient(f.Admin)
	aref := api.CameraAssignmentRef_builder{Id: f.Assignment.GetId()}.Build()
	current, err := assignments.Get(f.Ctx, api.CameraAssignmentGetRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	f.Assignment, err = assignments.Handover(f.Ctx, api.CameraAssignmentHandoverRequest_builder{Ref: aref, ExpectedVersion: current.GetDateUpdated(), ExpectedGeneration: current.GetGeneration(), Replacement: api.ProducerRef_builder{Id: newProducer.GetId()}.Build()}.Build())
	require.NoError(t, err)
	f.Assignment, err = assignments.Assign(f.Ctx, api.CameraAssignmentAssignRequest_builder{Ref: aref, ExpectedVersion: f.Assignment.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: f.Network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: newProducer.GetId()}.Build(), WorkerSlot: "isolated-approved-replacement", WorkerId: newWorker.GetId()}.Build())
	require.NoError(t, err)
	f.Worker, f.Producer = newWorker, newProducer
	f.EdgeRoles["worker"], f.EdgeRoles["producer"] = replacement, replacement
	f.Edges = []*qualificationProcess{replacement}
	// This is an explicit approved hardware change, not a same-key restart.
	delete(f.stored, "worker")
	delete(f.stored, "producer")
	f.WaitRoleHeartbeat("worker", approvedAt)
	f.WaitRoleHeartbeat("producer", approvedAt)
	for role, prior := range map[string]edgeRuntimeStoredRole{"worker": oldWorker, "producer": oldProducer} {
		current := f.RoleStatus(role)
		require.NotEqual(t, prior.ID, current.ID)
		require.NotEqual(t, prior.SPKISHA256, current.SPKISHA256)
	}
	currentBinding := lifecycleApplied(t, f.Admin, initial.Binding, initial.Live.CaptureEpoch)
	require.Equal(t, initial.Binding.GetSource().GetId(), currentBinding.GetSource().GetId())
	require.Equal(t, f.Assignment.GetGeneration(), currentBinding.GetAppliedApplication().GetTarget().GetAssignmentGeneration())
	media := f.VerifyMedia("camera", initial.Record)
	require.NotEqual(t, initial.Live.CaptureEpoch, media.Live.CaptureEpoch)
	require.Equal(t, initial.Binding.GetId(), media.Binding.GetId())
	isolatedOperationsPins(t, initial.Job, media.Job)
	isolatedOperationsRecoverable(t, f, initial.Job)
	qualificationPlayable(t, f.Ctx, f.Admin, "http://"+f.ControlState.Endpoints["tenant_http"], media.Binding, initial.Record, 320, 180)
	qualificationCall(t, f.Camera["camera"], "status", nil, &cameraState)
	require.Equal(t, 1, cameraState.Activations)
	require.Equal(t, 1, cameraState.ConfigurationWrites)
	_, err = producers.Get(f.Ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: oldProducerID}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "handover must erase the superseded machine identity")
}
