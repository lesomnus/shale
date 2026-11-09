package core

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	errCameraEnrollmentUnconfigured = errors.New("central enrollment credential preparer is not configured")
	errCameraEnrollmentCredentials  = errors.New("central enrollment credentials are unavailable")
)

func cameraEnrollmentInputs(p *api.CameraPolicy, preset *api.CameraPreset, activation bool, device *api.CameraDevice) *api.CameraJobInputs {
	out := api.CameraJobInputs_builder{
		Policy:          api.CameraRevisionReference_builder{Id: mustId(p.GetId()).String(), Revision: p.GetRevision()}.Build(),
		Preset:          api.CameraRevisionReference_builder{Id: mustId(preset.GetId()).String(), Revision: preset.GetRevision()}.Build(),
		AllowActivation: activation,
	}.Build()
	if bootstrap := p.GetEnrollment().GetBootstrap(); bootstrap != nil {
		out.SetBootstrap(proto.Clone(bootstrap).(*api.CameraBootstrapProfile))
	}
	out.SetDeviceTlsIdentity(cameraDeviceTLSIdentity(p, device))
	return out
}

// cameraEnrollJob is called only after its caller has locked and authorized the
// current assignment. Credential preparation and job creation share the same
// recorded transaction; no camera operation can see its references before commit.
func (s Core) cameraEnrollJob(ctx context.Context, a *api.CameraAssignment, b *api.CameraBinding, p *api.CameraPolicy, key string, activation bool, reuse *api.CameraJobInputs) (*api.CameraJob, error) {
	if err := cameraToken("request_key", key); err != nil {
		return nil, err
	}
	if err := cameraSameScope(p.GetTenant().GetId(), p.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
		return nil, err
	}
	if !bytes.Equal(p.GetNetwork().GetId(), a.GetNetwork().GetId()) {
		return nil, denied("enrollment", "policy belongs to another camera network")
	}
	if activation && !p.GetEnrollment().GetAllowActivation() {
		return nil, denied("enrollment", "policy does not permit first-time activation")
	}
	preset, err := s.Next().CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: p.GetPreset().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := cameraPresetScope(preset, a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
		return nil, err
	}
	d, err := s.Next().CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: b.GetDevice().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	inputs := cameraEnrollmentInputs(p, preset, activation, d)
	old, err := s.ent(ctx).CameraJob.Query().Where(camerajob.TenantIdEQ(mustId(a.GetTenant().GetId()).Uuid()), camerajob.RequestKeyEQ(key), camerajob.DateErasedIsNil()).Only(ctx)
	if err == nil {
		// Idempotency is decided before touching a credential backend. Keep the
		// original exact versions even if a current credential has changed or
		// the backend is temporarily unavailable after a lost response.
		j, err := s.Next().CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: old.Id[:]}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(j.GetBinding().GetId(), b.GetId()) || !bytes.Equal(j.GetAssignment().GetId(), a.GetId()) {
			return nil, status.Error(codes.AlreadyExists, "enrollment intent belongs to another binding")
		}
		if reuse == nil {
			inputs.SetInitialCredential(j.GetInputs().GetInitialCredential())
			inputs.SetManagementCredential(j.GetInputs().GetManagementCredential())
			inputs.SetStreamingCredential(j.GetInputs().GetStreamingCredential())
		} else {
			inputs.SetInitialCredential(reuse.GetInitialCredential())
			inputs.SetManagementCredential(reuse.GetManagementCredential())
			inputs.SetStreamingCredential(reuse.GetStreamingCredential())
			if !activation {
				inputs.ClearInitialCredential()
			}
		}
		canonical := api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: key, Inputs: inputs}.Build()
		if !bytes.Equal(j.GetRequestHash(), cameraHash(canonical)) {
			return nil, status.Error(codes.AlreadyExists, "enrollment intent already has different pinned inputs")
		}
		return j, nil
	}
	if !ent.IsNotFound(err) {
		return nil, err
	}
	if !bytes.Equal(d.GetNetwork().GetId(), a.GetNetwork().GetId()) {
		return nil, denied("enrollment", "device belongs to another camera network")
	}
	if err := cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
		return nil, err
	}
	if reuse != nil {
		inputs.SetInitialCredential(reuse.GetInitialCredential())
		inputs.SetManagementCredential(reuse.GetManagementCredential())
		inputs.SetStreamingCredential(reuse.GetStreamingCredential())
		if !activation {
			inputs.ClearInitialCredential()
		}
	} else {
		if s.d.PrepareCameraEnrollment == nil {
			return nil, errCameraEnrollmentUnconfigured
		}
		username := p.GetEnrollment().GetInitialUsername()
		if username == "" {
			username = "admin"
		}
		bundle, err := s.d.PrepareCameraEnrollment(ctx, CameraEnrollmentPreparation{
			Server: s.Next(), Driver: s.cameraEnrollmentDriver(ctx),
			Assignment: proto.Clone(a).(*api.CameraAssignment), Device: proto.Clone(d).(*api.CameraDevice), Binding: proto.Clone(b).(*api.CameraBinding), Policy: proto.Clone(p).(*api.CameraPolicy),
			IntentKey:       "enrollment/" + hex.EncodeToString(cameraHash(api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: key, Inputs: inputs}.Build())),
			InitialUsername: username, AllowActivation: activation,
		})
		if err != nil {
			// A secret backend or device adapter can put credentials in an
			// error. The controller emits only this fixed waiting category.
			return nil, errCameraEnrollmentCredentials
		}
		inputs.SetInitialCredential(bundle.Initial)
		inputs.SetManagementCredential(bundle.Management)
		inputs.SetStreamingCredential(bundle.Streaming)
	}
	if inputs.GetManagementCredential() == nil || inputs.GetStreamingCredential() == nil || activation && inputs.GetInitialCredential() == nil {
		return nil, errCameraEnrollmentCredentials
	}
	provider := inputs.GetManagementCredential().GetProvider()
	for _, ref := range []*api.CameraCredentialReference{inputs.GetInitialCredential(), inputs.GetStreamingCredential()} {
		if ref != nil && ref.GetProvider() != provider {
			return nil, errCameraEnrollmentCredentials
		}
	}
	if err := cameraInputs(inputs); err != nil {
		return nil, err
	}
	return (coreCameraJob{s, s.Next().CameraJob()}).create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: key, Inputs: inputs}.Build())
}

func (s coreCameraPolicy) Enroll(ctx context.Context, r *api.CameraPolicyEnrollRequest) (*api.CameraJob, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraJob
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: r.GetBinding()}.Build())
		if err != nil {
			return err
		}
		a, err := c.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build())
		if err != nil {
			return err
		}
		p, err := nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: r.GetPolicy()}.Build())
		if err != nil {
			return err
		}
		out, err = c.cameraEnrollJob(ctx, a, b, p, r.GetRequestKey(), p.GetEnrollment().GetAllowActivation(), nil)
		return err
	})
	if errors.Is(err, errCameraEnrollmentUnconfigured) || errors.Is(err, errCameraEnrollmentCredentials) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return out, cameraConflict(err)
}
