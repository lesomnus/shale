package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCameraPolicyLeasedCheckpointPinsBeforeDeviceWrite(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	worker, workerConn, agent := cameraWorkerMachine(t, ctx, server, cp, admin, "checkpoint-worker", nil)
	presets := api.NewCameraPresetServiceClient(admin)
	request := cameraPolicyPresetRequest("checkpoint-preset", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, worker.GetTenant().GetId())}.Build()))
	preset, err := presets.Create(ctx, request)
	require.NoError(t, err)
	job := cameraWorkerJob(t, ctx, server, cp, admin, worker, "checkpoint", nil, api.CameraJobInputs_builder{Preset: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, preset.GetId()), Revision: preset.GetRevision()}.Build(), StreamingCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "checkpoint-stream", Version: "original", Purpose: "streaming"}.Build()}.Build())
	binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: job.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	devices := api.NewCameraDeviceServiceClient(admin)
	device, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: binding.GetDevice().GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = devices.Observe(ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), ExpectedVersion: device.GetDateUpdated(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10/onvif/device_service"}.Build()}}.Build())
	require.NoError(t, err)
	ready := make(chan *cameraworker.Execution, 1)
	failures := make(chan error, 1)
	runtime, err := cameraworker.New(cameraworker.Config{StateDir: agent.Store.Dir, Cp: cp, Tenant: "acme", HardwareID: "checkpoint-worker", CaHash: pki.Fingerprint(server.CA.Cert), HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 30, Log: agent.Log, Executor: cameraworker.ExecutorFunc(func(ctx context.Context, e *cameraworker.Execution) error {
		store := &cameraworker.JobStore{Execution: e}
		q, err := store.Request(ctx)
		if err != nil {
			failures <- err
			return err
		}
		now := time.Now().UTC()
		r := provision.Run{Request: q, Stage: provision.Pending, Version: 1, CreatedAt: now, UpdatedAt: now, InitialRef: q.InitialRef, ManagementRef: q.ManagementRef, StreamingRef: q.StreamingRef, Binding: provision.BindingState{BindingID: q.BindingID, Alias: q.Alias}}
		if err = store.Save(ctx, r); err != nil {
			failures <- err
			return err
		}
		if _, err = e.Checkpoint(ctx, "verify-phase-only", api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, e.Snapshot().GetJob().GetObserved()); err != nil {
			failures <- err
			return err
		}
		h := q.Device
		h.Manufacturer = "fixture"
		h.Model = "TestCam"
		h.Serial = "authenticated-unit"
		h.Evidence = []cameramodel.Evidence{{Source: "fixture", Authenticated: true}}
		snapshot, err := (cameraworker.JobPresets{Execution: e}).Resolve(ctx, q.Scope, h)
		if err != nil {
			failures <- err
			return err
		}
		r.Stage = provision.Inspecting
		r.Version++
		r.UpdatedAt = time.Now().UTC()
		r.Preset = &snapshot
		r.Inspection = &cameramodel.Inspection{Device: h, Activation: cameramodel.ActivationReady, Current: cameramodel.CurrentState{Firmware: "1"}}
		if err = store.Save(ctx, r); err != nil {
			failures <- err
			return err
		}
		ready <- e
		<-ctx.Done()
		return ctx.Err()
	})})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("checkpoint Worker did not stop")
		}
	})
	var execution *cameraworker.Execution
	select {
	case execution = <-ready:
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("checkpoint did not reach durable preset pin")
	}
	stored, err := api.NewCameraJobServiceClient(admin).Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotNil(t, stored.GetPinnedPreset())
	require.Equal(t, cameraPolicyID(t, preset.GetId()), stored.GetPinnedPreset().GetId())
	require.Equal(t, uint64(2), stored.GetObserved().GetProgress().GetVersion())
	device, err = devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, device.GetIdentity().GetAuthenticated())
	require.Equal(t, "authenticated-unit", device.GetIdentity().GetSerial())
	request.SetRevision("2")
	request.GetRules()[0].GetSettings().GetVideo().SetFps(15)
	newPreset, err := presets.Create(ctx, request)
	require.NoError(t, err)
	require.NotEqual(t, preset.GetId(), newPreset.GetId())
	store := &cameraworker.JobStore{Execution: execution}
	run, err := store.Load(ctx, cameraPolicyID(t, job.GetId()))
	require.NoError(t, err)
	resolved, err := (cameraworker.JobPresets{Execution: execution}).Resolve(ctx, run.Request.Scope, run.Inspection.Device)
	require.NoError(t, err)
	require.Equal(t, run.Preset.Ref, resolved.Ref, "a new ruleset does not alter an in-progress immutable job")
	require.Equal(t, 30000.0/1001, resolved.Requested.Video.FPS)
	claimed := execution.Snapshot().GetJob()
	_, err = api.NewCameraJobServiceClient(workerConn).Report(ctx, api.CameraJobReportRequest_builder{Ref: api.CameraJobRef_builder{Id: job.GetId()}.Build(), ExpectedVersion: claimed.GetDateUpdated(), OperationKey: "drop-checkpoint", LeaseGeneration: claimed.GetLeaseGeneration(), AssignmentGeneration: claimed.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	forged := proto.Clone(claimed.GetObserved()).(*api.CameraJobObservation)
	forged.GetProgress().SetVersion(3)
	forged.GetProgress().GetStreamingRef().SetVersion("rotated-without-new-job")
	_, err = api.NewCameraJobServiceClient(workerConn).Report(ctx, api.CameraJobReportRequest_builder{Ref: api.CameraJobRef_builder{Id: job.GetId()}.Build(), ExpectedVersion: claimed.GetDateUpdated(), OperationKey: "forge-credential", LeaseGeneration: claimed.GetLeaseGeneration(), AssignmentGeneration: claimed.GetAssignmentGeneration(), Stage: claimed.GetStage(), Observed: forged}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	forged = proto.Clone(claimed.GetObserved()).(*api.CameraJobObservation)
	forged.GetProgress().SetVersion(3)
	claimedProfile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 640, Height: 360, Fps: 15, MaxBitrateBps: 1_000_000}.Build()
	forged.SetEffective(claimedProfile)
	forged.SetVerified(api.CameraMediaEvidence_builder{Profile: claimedProfile, Decoded: true, DecodedFrames: 75, DurationSeconds: 5, ObservedAt: timestamppb.Now()}.Build())
	_, err = api.NewCameraJobServiceClient(workerConn).Publish(ctx, api.CameraJobPublishRequest_builder{Ref: api.CameraJobRef_builder{Id: job.GetId()}.Build(), ExpectedVersion: claimed.GetDateUpdated(), OperationKey: "unselected-intent-publish", LeaseGeneration: claimed.GetLeaseGeneration(), AssignmentGeneration: claimed.GetAssignmentGeneration(), Spec: api.CameraCaptureSpec_builder{Input: "rtsp://192.0.2.10/main", Profile: claimedProfile, Credential: claimed.GetInputs().GetStreamingCredential()}.Build(), Observed: forged}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "outer proof cannot publish a pinned ruleset that has not selected or configured its candidate")

	networks := api.NewCameraNetworkServiceClient(admin)
	network, err := networks.Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: execution.Snapshot().GetNetwork().GetId()}.Build()}.Build())
	require.NoError(t, err)
	configure := func(cidrs []string) {
		t.Helper()
		var e error
		network, e = networks.Configure(ctx, api.CameraNetworkConfigureRequest_builder{Ref: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Alias: network.GetAlias(), Name: network.GetName(), Cidrs: cidrs, InterfaceHint: network.GetInterfaceHint(), Multicast: network.GetMulticast(), ExpectedVersion: network.GetDateUpdated()}.Build())
		require.NoError(t, e)
	}
	configure([]string{"192.0.2.0/24", "198.51.100.0/24"})
	calls := 0
	leased := cameraworker.LeasedDevices{Execution: execution, Verifier: policyCheckpointProbe(func(context.Context, cameramodel.MediaRequest) (cameramodel.VerifiedState, error) {
		calls++
		return cameramodel.VerifiedState{}, nil
	})}
	probe := cameramodel.MediaRequest{Scope: run.Request.Scope, NetworkScope: run.Request.Scope.NetworkID, Device: run.Request.Device, Profile: cameramodel.MediaProfile{StreamURI: "rtsp://198.51.100.20/pending-stream"}}
	_, err = leased.Verify(ctx, probe)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	// The device's management endpoint remains permitted and no capture has
	// been published. Configure can remove only the pending media prefix.
	configure([]string{"192.0.2.0/24"})
	_, err = leased.Verify(ctx, probe)
	require.ErrorIs(t, err, cameramodel.ErrForbidden)
	require.Equal(t, 1, calls, "out-of-policy pending media must not reach a LAN callback")
}

type policyCheckpointProbe func(context.Context, cameramodel.MediaRequest) (cameramodel.VerifiedState, error)

func (f policyCheckpointProbe) Verify(ctx context.Context, r cameramodel.MediaRequest) (cameramodel.VerifiedState, error) {
	return f(ctx, r)
}
