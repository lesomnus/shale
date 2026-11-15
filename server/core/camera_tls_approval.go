package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// cameraProgressChangedTLS accepts the key a job stopped on only as an
// observation of the job's own device, in place of the key the job pins. It
// is never a pin by itself: only ApproveTLS makes it one.
func cameraProgressChangedTLS(run provision.Run, d *api.CameraDevice) error {
	v := run.ChangedTLSIdentity
	if v == nil {
		return nil
	}
	pin := run.PinnedTLSIdentity()
	if pin == nil || v.DeviceID != mustId(d.GetId()).String() || pin.DeviceID != v.DeviceID || pin.SPKISHA256 == v.SPKISHA256 || cameraTLSIdentity(cameratls.Encode(v)) != nil {
		return invalid("progress.changed_tls_identity", "a changed key is recorded only for this job's own pinned device")
	}
	return nil
}

// cameraStoppedTLS is the key job j stopped on, for its own device d.
func cameraStoppedTLS(j *api.CameraJob, d *api.CameraDevice) (*api.CameraDeviceTLSIdentity, error) {
	state := j.GetObserved().GetProgress()
	if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_WAITING || state.GetChangedTlsIdentity() == nil {
		return nil, failed("the camera job has not stopped on a changed TLS key")
	}
	run, err := cameraprogress.Decode(state)
	if err != nil || cameraProgressChangedTLS(run, d) != nil {
		return nil, failed("stored camera checkpoint is invalid")
	}
	return cameratls.Encode(run.ChangedTLSIdentity), nil
}

// cameraApprovalRevision names the revision approving one key for one device
// on top of one base revision, so a retried approval finds its own revision.
func cameraApprovalRevision(base *api.CameraPolicy, v *api.CameraDeviceTLSIdentity) string {
	h := sha256.New()
	h.Write(base.GetId())
	h.Write([]byte(v.GetDeviceId()))
	h.Write([]byte(v.GetSpkiSha256()))
	return "tls-" + hex.EncodeToString(h.Sum(nil))[:20]
}

// ApproveTLS is the operator's approval of the key a job stopped on
// (docs/camera-tls-identity.md, "Approving a changed key"). The request names the stopped job
// and the exact key it recorded; the device is that job's own. In one
// recorded transaction the CP writes a revision of the network's active
// policy that approves that key for that device only, activates it under the
// same network CAS as Activate, settles the stopped job, and enrolls its
// successor pinned to the new key.
func (s coreCameraPolicy) ApproveTLS(ctx context.Context, r *api.CameraPolicyApproveTLSRequest) (*api.CameraPolicyApproveTLSResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if !cameraSPKISHA256.MatchString(r.GetSpkiSha256()) {
		return nil, invalid("spki_sha256", "an explicit canonical SHA256 SPKI digest is required")
	}
	var out *api.CameraPolicyApproveTLSResponse
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		// Locks the network, the assignment and the device, in that order.
		j, a, b, d, err := c.cameraJobRows(ctx, r.GetRef())
		if err != nil {
			return err
		}
		observed, err := cameraStoppedTLS(j, d)
		if err != nil {
			return err
		}
		if observed.GetSpkiSha256() != r.GetSpkiSha256() {
			return failed("the camera job stopped on another key; approve exactly the key it recorded")
		}
		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedNetworkVersion(), n.GetDateUpdated()); err != nil {
			return err
		}
		// The active revision is extended, so its other settings stay as the
		// operator last activated them. A job's own older revision is the base
		// only when the network has none active.
		baseID := n.GetActivePolicyId()
		if len(baseID) == 0 {
			id, err := cameraPresetID(j.GetInputs().GetPolicy().GetId(), DomCameraPolicy)
			if err != nil {
				return failed("the camera job names no policy revision to extend")
			}
			baseID = id
		}
		base, err := nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: baseID}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(base.GetNetwork().GetId(), n.GetId()) {
			return denied("approve TLS", "policy belongs to another camera network")
		}
		if err := cameraSameScope(base.GetTenant().GetId(), base.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
			return err
		}
		if base.GetEnrollment().GetTlsVerification() == camera.TLSVerificationSkip {
			return failed("the network's active policy skips TLS verification and approves no key; enroll the binding under it instead")
		}
		p := base
		if !proto.Equal(cameraSelectedTLSIdentity(base, d.GetId()), observed) {
			enrollment := proto.Clone(base.GetEnrollment()).(*api.CameraEnrollmentPolicy)
			identities := make([]*api.CameraDeviceTLSIdentity, 0, len(enrollment.GetDeviceTlsIdentities())+1)
			for _, v := range enrollment.GetDeviceTlsIdentities() {
				if v.GetDeviceId() != observed.GetDeviceId() {
					identities = append(identities, v)
				}
			}
			enrollment.SetDeviceTlsIdentities(append(identities, proto.Clone(observed).(*api.CameraDeviceTLSIdentity)))
			preset, err := nx.CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: base.GetPreset().GetId()}.Build()}.Build())
			if err != nil {
				return err
			}
			p, err = c.cameraCreatePolicy(ctx, n, preset, base.GetPolicyKey(), cameraApprovalRevision(base, observed), base.GetName(), enrollment)
			if err != nil {
				return err
			}
		}
		n, err = c.cameraActivatePolicy(ctx, n, p)
		if err != nil {
			return err
		}
		// The stopped job's pin is immutable and can never be met again. It
		// is settled so its successor may hold the device.
		if _, err := nx.CameraJob().Patch(ctx, api.CameraJobPatchRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), Stage: z.Ptr(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED), LeaseExpires: timestamppb.New(c.d.now()), DateUpdated: j.GetDateUpdated()}.Build()); err != nil {
			return err
		}
		// The successor keeps the stopped job's exact credential versions and
		// may activate only where both it and the new revision allowed that.
		activation := j.GetInputs().GetAllowActivation() && p.GetEnrollment().GetAllowActivation()
		next, err := c.cameraEnrollJob(ctx, a, b, p, "tls/"+mustId(j.GetId()).String(), activation, j.GetInputs())
		if err != nil {
			return err
		}
		if !proto.Equal(next.GetInputs().GetDeviceTlsIdentity(), observed) {
			return failed("the approved key is not the new job's pin")
		}
		out = api.CameraPolicyApproveTLSResponse_builder{Policy: p, Network: n, Job: next}.Build()
		return nil
	})
	if errors.Is(err, errCameraEnrollmentCredentials) || errors.Is(err, errCameraEnrollmentUnconfigured) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return out, cameraConflict(err)
}
