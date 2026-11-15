package core

import (
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCameraProgressChangedTLSIsOnlyAnObservationOfTheJobsDevice(t *testing.T) {
	d := api.CameraDevice_builder{Id: pdid.New(DomCameraDevice).Bytes()}.Build()
	other := pdid.New(DomCameraDevice).Bytes()
	run := provision.Run{Request: provision.Request{DeviceTLSIdentity: cameratls.Decode(tlsIdentity(d.GetId(), "a"))}}
	require.NoError(t, cameraProgressChangedTLS(run, d), "no changed key")
	run.ChangedTLSIdentity = cameratls.Decode(tlsIdentity(d.GetId(), "b"))
	require.NoError(t, cameraProgressChangedTLS(run, d))

	// After a key accepted by the job's own upgrade, that key is the pin.
	rotated := run
	rotated.Firmware = &provision.FirmwareState{Complete: true, TLSIdentity: cameratls.Decode(tlsIdentity(d.GetId(), "b"))}
	require.Error(t, cameraProgressChangedTLS(rotated, d), "the accepted key is not a change")
	rotated.ChangedTLSIdentity = cameratls.Decode(tlsIdentity(d.GetId(), "a"))
	require.NoError(t, cameraProgressChangedTLS(rotated, d), "the replaced key is a change")

	for name, edit := range map[string]func(*provision.Run){
		"no pin":           func(r *provision.Run) { r.Request.DeviceTLSIdentity = nil },
		"same as pin":      func(r *provision.Run) { r.ChangedTLSIdentity = cameratls.Decode(tlsIdentity(d.GetId(), "a")) },
		"another device":   func(r *provision.Run) { r.ChangedTLSIdentity = cameratls.Decode(tlsIdentity(other, "b")) },
		"pin of another":   func(r *provision.Run) { r.Request.DeviceTLSIdentity = cameratls.Decode(tlsIdentity(other, "a")) },
		"malformed digest": func(r *provision.Run) { r.ChangedTLSIdentity.SPKISHA256 = "sha256:B" },
	} {
		r := run
		r.Request.DeviceTLSIdentity = cloneTLS(run.Request.DeviceTLSIdentity)
		r.ChangedTLSIdentity = cloneTLS(run.ChangedTLSIdentity)
		edit(&r)
		err := cameraProgressChangedTLS(r, d)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
}

func cloneTLS(v *camera.DeviceTLSIdentity) *camera.DeviceTLSIdentity {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func TestCameraStoppedTLSNeedsAJobStoppedOnAChangedKey(t *testing.T) {
	d := api.CameraDevice_builder{Id: pdid.New(DomCameraDevice).Bytes()}.Build()
	j := api.CameraJob_builder{Stage: api.CameraJobStage_CAMERA_JOB_STAGE_WAITING, Observed: api.CameraJobObservation_builder{Progress: api.CameraProvisionState_builder{}.Build()}.Build()}.Build()
	_, err := cameraStoppedTLS(j, d)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a job waiting for another reason")
	j.GetObserved().GetProgress().SetChangedTlsIdentity(tlsIdentity(d.GetId(), "b"))
	_, err = cameraStoppedTLS(j, d)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "an undecodable checkpoint")
	for _, stage := range []api.CameraJobStage{api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING, api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE} {
		v := proto.Clone(j).(*api.CameraJob)
		v.SetStage(stage)
		_, err = cameraStoppedTLS(v, d)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), stage.String())
	}
}

func TestCameraApprovalRevisionIsBoundToBaseDeviceAndKey(t *testing.T) {
	base := api.CameraPolicy_builder{Id: pdid.New(DomCameraPolicy).Bytes()}.Build()
	device := pdid.New(DomCameraDevice).Bytes()
	revision := cameraApprovalRevision(base, tlsIdentity(device, "b"))
	require.NoError(t, cameraToken("revision", revision))
	require.Equal(t, revision, cameraApprovalRevision(base, tlsIdentity(device, "b")), "a retried approval finds its own revision")
	for _, changed := range []string{
		cameraApprovalRevision(base, tlsIdentity(device, "c")),
		cameraApprovalRevision(base, tlsIdentity(pdid.New(DomCameraDevice).Bytes(), "b")),
		cameraApprovalRevision(api.CameraPolicy_builder{Id: pdid.New(DomCameraPolicy).Bytes()}.Build(), tlsIdentity(device, "b")),
	} {
		require.NotEqual(t, revision, changed)
	}
}
