package core

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCameraAuthorityInputBoundaries(t *testing.T) {
	cidrs := []string{"192.0.2.0/24"}
	for _, uri := range []string{"http://192.0.2.10/onvif/device_service", "rtsp://192.0.2.10:554/stream", "rtsp://192.0.2.10/cam?channel=1&subtype=0"} {
		require.NoError(t, cameraURI(uri, cidrs, false), uri)
	}
	const password = "private-camera-credential"
	for _, uri := range []string{
		"rtsp://u:" + password + "@192.0.2.10/stream",
		"rtsp://192.0.2.10/stream?password=" + password,
		"rtsp://192.0.2.10/stream?%70assword=" + password,
		"rtsp://192.0.2.10/stream?token=%zz",
		"rtsp://192.0.2.10/stream?opaque_auth=" + password,
		"rtsp://192.0.2.10/stream#" + password,
		"rtsp://camera.example/stream", "rtsp://198.51.100.10/stream", "file:///etc/passwd",
	} {
		err := cameraURI(uri, cidrs, false)
		require.Error(t, err)
		require.NotContains(t, err.Error(), password)
	}
	for _, bad := range [][]string{nil, {"0.0.0.0/0"}, {"192.0.2.10/24"}, {"224.0.0.0/4"}} {
		_, err := cameraNetworks(bad)
		require.Error(t, err)
	}
	require.Error(t, cameraURI("http://192.0.2.10/stream", cidrs, true))
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build()
	require.NoError(t, cameraProfile(profile, true), "zero GOP interval is unknown, not invented evidence")
	bad := proto.Clone(profile).(*api.CameraCaptureProfile)
	bad.SetFps(math.NaN())
	require.Error(t, cameraProfile(bad, true))
	ref := api.CameraCredentialReference_builder{Provider: "cp", Id: "camera-stream", Version: "1", Purpose: "streaming"}.Build()
	require.NoError(t, cameraCredential(ref, "streaming"))
	require.Error(t, cameraCredential(ref, "management"))
}
func TestCameraAuthorityRejectsUntypedSecrets(t *testing.T) {
	const password = "unknown-wire-password-must-not-persist"
	ref := api.CameraCredentialReference_builder{Provider: "cp", Id: "camera-stream", Version: "1", Purpose: "streaming"}.Build()
	unknown := protowire.AppendTag(nil, 99, protowire.BytesType)
	unknown = protowire.AppendString(unknown, password)
	ref.ProtoReflect().SetUnknown(unknown)
	req := api.CameraJobPublishRequest_builder{Spec: api.CameraCaptureSpec_builder{Credential: ref}.Build()}.Build()
	err := cameraMessage(req)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotContains(t, err.Error(), password)
	require.Error(t, cameraMessage(api.CameraNetworkConfigureRequest_builder{Name: string(make([]byte, 70<<10))}.Build()))
}
func TestCameraAuthorityLeaseFence(t *testing.T) {
	now := time.Now().UTC()
	owner := pdid.New(DomHolder)
	other := pdid.New(DomHolder)
	jobId := pdid.New(DomCameraJob)
	c := Core{d: &Deps{Now: func() time.Time { return now }}}
	j := api.CameraJob_builder{Id: jobId.Bytes(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, LeaseGeneration: 3, AssignmentGeneration: 7, LeaseOwner: owner.Bytes(), LeaseExpires: timestamppb.New(now.Add(time.Minute))}.Build()
	a := api.CameraAssignment_builder{Generation: 7}.Build()
	d := api.CameraDevice_builder{ActiveJobId: jobId.Bytes(), ClaimGeneration: 3}.Build()
	require.NoError(t, c.cameraLease(j, a, d, owner, 3, 7))
	require.Error(t, c.cameraLease(j, a, d, other, 3, 7))
	require.Error(t, c.cameraLease(j, a, d, owner, 2, 7))
	changed := proto.Clone(a).(*api.CameraAssignment)
	changed.SetGeneration(8)
	require.Equal(t, codes.Aborted, status.Code(c.cameraLease(j, changed, d, owner, 3, 7)))
	expired := proto.Clone(j).(*api.CameraJob)
	expired.SetLeaseExpires(timestamppb.New(now.Add(-time.Second)))
	require.Error(t, c.cameraLease(expired, a, d, owner, 3, 7))
	terminal := proto.Clone(j).(*api.CameraJob)
	terminal.SetStage(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED)
	require.Error(t, c.cameraLease(terminal, a, d, owner, 3, 7))
	require.False(t, cameraStageTransition(api.CameraJobStage_CAMERA_JOB_STAGE_PENDING, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE))
	require.False(t, cameraStageTransition(api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE))
	require.True(t, cameraStageTransition(api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE))
}
func TestCameraAuthorityGateIsOperatorOnly(t *testing.T) {
	tenant := pdid.New(DomTenant)
	admin := pdid.New(DomHolder)
	person := pdid.New(DomHolder)
	ops := &fakeOperators{}
	ops.grant(tenant, admin, "/shale.*/*")
	p := TenantPolicy{Operators: ops}
	for _, method := range []string{api.CameraNetworkService_Get_FullMethodName, api.CameraNetworkService_Configure_FullMethodName, api.CameraJobService_Claim_FullMethodName, api.CameraAssignmentService_Snapshot_FullMethodName} {
		require.NoError(t, p.May(context.Background(), gate.Call{Tenant: tenant, Actor: admin, Action: method}))
		require.Error(t, p.May(context.Background(), gate.Call{Tenant: tenant, Actor: person, Action: method}))
		require.Error(t, p.May(context.Background(), gate.Call{Tenant: tenant, Actor: pdid.New(DomProducer), Action: method}))
		require.Error(t, p.May(context.Background(), gate.Call{Tenant: tenant, Actor: pdid.New(DomReader), Action: method}))
	}
	for _, method := range []string{api.CameraJobService_Add_FullMethodName, api.CameraJobService_Patch_FullMethodName, api.CameraAssignmentService_Apply_FullMethodName, api.CameraDeviceService_Erase_FullMethodName} {
		require.Error(t, p.May(context.Background(), gate.Call{Tenant: tenant, Actor: admin, Action: method}))
	}
}

func TestCameraClaimReplayRequiresCurrentGrant(t *testing.T) {
	now := time.Now().UTC()
	owner := pdid.New(DomHolder)
	id := pdid.New(DomCameraJob)
	c := Core{d: &Deps{Now: func() time.Time { return now }}}
	j := api.CameraJob_builder{Id: id.Bytes(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING, LeaseGeneration: 3, AssignmentGeneration: 7, LeaseOwner: owner.Bytes(), LeaseExpires: timestamppb.New(now.Add(time.Minute)), LastOperationKey: "claim-1", LastOperationHash: []byte("hash")}.Build()
	a := api.CameraAssignment_builder{Generation: 7}.Build()
	d := api.CameraDevice_builder{ActiveJobId: id.Bytes(), ClaimGeneration: 3}.Build()
	replay, err := c.cameraClaimReplay(j, a, d, owner, "claim-1", []byte("hash"))
	require.NoError(t, err)
	require.True(t, replay)
	a.SetGeneration(8)
	_, err = c.cameraClaimReplay(j, a, d, owner, "claim-1", []byte("hash"))
	require.Error(t, err)
	a.SetGeneration(7)
	j.SetLeaseExpires(timestamppb.New(now.Add(-time.Second)))
	_, err = c.cameraClaimReplay(j, a, d, owner, "claim-1", []byte("hash"))
	require.Error(t, err)
	j.SetLeaseExpires(timestamppb.New(now.Add(time.Minute)))
	d.SetClaimGeneration(4)
	_, err = c.cameraClaimReplay(j, a, d, owner, "claim-1", []byte("hash"))
	require.Error(t, err)
}
func TestCameraUncertainExpiredJobMustSettleBeforeAnotherIntent(t *testing.T) {
	old := pdid.New(DomCameraJob)
	next := pdid.New(DomCameraJob)
	j := api.CameraJob_builder{Id: old.Bytes(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING, LeaseExpires: timestamppb.New(time.Now().Add(-time.Hour))}.Build()
	require.Error(t, cameraOtherJob(j, next.Bytes()), "expiry cannot fence an in-flight firmware write")
	require.NoError(t, cameraOtherJob(j, old.Bytes()), "the same immutable job may reconcile after expiry")
	j.SetStage(api.CameraJobStage_CAMERA_JOB_STAGE_FAILED)
	require.NoError(t, cameraOtherJob(j, next.Bytes()))
}
