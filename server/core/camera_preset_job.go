package core

import (
	"bytes"
	"context"
	"strings"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"google.golang.org/protobuf/proto"
)

// Resolve names immutable revisions and authorizes their entire scope before
// they become job inputs. The network's current active pointer is deliberately
// not consulted: an operator can explicitly reapply an older pinned revision.
func (s Core) cameraPinnedInputs(ctx context.Context, a *api.CameraAssignment, input *api.CameraJobInputs) (*api.CameraJobInputs, error) {
	if input == nil {
		return nil, invalid("inputs", "pinned job inputs are required")
	}
	out := proto.Clone(input).(*api.CameraJobInputs)
	if ref := out.GetPolicy(); ref != nil {
		id, err := cameraPresetID(ref.GetId(), DomCameraPolicy)
		if err != nil {
			return nil, err
		}
		p, err := s.Next().CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		if err := cameraSameScope(p.GetTenant().GetId(), p.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
			return nil, err
		}
		if !bytes.Equal(p.GetNetwork().GetId(), a.GetNetwork().GetId()) || p.GetRevision() != ref.GetRevision() {
			return nil, denied("policy", "job does not name this network's exact policy revision")
		}
		if out.GetAllowActivation() && !p.GetEnrollment().GetAllowActivation() {
			return nil, denied("allow_activation", "pinned policy does not permit first-time activation")
		}
		if !proto.Equal(out.GetBootstrap(), p.GetEnrollment().GetBootstrap()) {
			return nil, denied("bootstrap", "bootstrap does not match the pinned policy revision")
		}
		pinned, err := s.Next().CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: p.GetPreset().GetId()}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		want := api.CameraRevisionReference_builder{Id: mustId(pinned.GetId()).String(), Revision: pinned.GetRevision()}.Build()
		if out.GetPreset() != nil && !proto.Equal(out.GetPreset(), want) {
			return nil, denied("preset", "preset does not match the pinned policy revision")
		}
		out.SetPreset(want)
	}
	if ref := out.GetPreset(); ref != nil {
		id, err := cameraPresetID(ref.GetId(), DomCameraPreset)
		if err != nil {
			return nil, err
		}
		p, err := s.Next().CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		if err := cameraPresetScope(p, a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
			return nil, err
		}
		if p.GetRevision() != ref.GetRevision() {
			return nil, denied("preset", "job does not name the exact immutable preset revision")
		}
		if out.GetConfiguration() != nil || (out.GetDesired() != nil && proto.Size(out.GetDesired()) != 0) {
			return nil, invalid("inputs", "choose a pinned preset or direct configuration")
		}
	} else if out.GetConfiguration() != nil {
		if _, err := presetwire.DecodeSettings(out.GetConfiguration()); err != nil {
			return nil, invalid("configuration", "invalid direct device configuration")
		}
		if out.GetDesired() != nil && proto.Size(out.GetDesired()) != 0 {
			return nil, invalid("inputs", "choose direct device configuration or legacy capture intent")
		}
	}
	return out, nil
}

func cameraPresetEnrollmentScope(a *api.CameraAssignment) camera.EnrollmentScope {
	return camera.EnrollmentScope{TenantID: mustId(a.GetTenant().GetId()).String(), SiteID: cameraPresetOptionalID(a.GetSite().GetId()), SetID: mustId(a.GetSet().GetId()).String(), ProducerID: mustId(a.GetProducer().GetId()).String(), NetworkID: mustId(a.GetNetwork().GetId()).String()}
}

func cameraPresetIdentity(v *api.CameraIdentityEvidence) error {
	if len(v.GetMacs()) > 16 {
		return invalid("identity", "too many network interfaces")
	}
	for _, text := range []string{v.GetManufacturer(), v.GetModel(), v.GetHardware(), v.GetFirmware(), v.GetSerial()} {
		if !cameraPresetText(text) {
			return invalid("identity", "bounded identity evidence is required")
		}
	}
	for _, mac := range v.GetMacs() {
		if _, err := preset.NormalizeMAC(mac); err != nil {
			return invalid("identity", "an Ethernet MAC address is required")
		}
	}
	return nil
}

func cameraPresetDevice(d *api.CameraDevice, v *api.CameraIdentityEvidence) camera.DeviceHint {
	h := camera.DeviceHint{ID: camera.DeviceID(mustId(d.GetId()).String()), Manufacturer: v.GetManufacturer(), Model: v.GetModel(), Hardware: v.GetHardware(), Firmware: v.GetFirmware(), Serial: v.GetSerial()}
	if len(v.GetMacs()) > 0 {
		h.MAC = v.GetMacs()[0]
	}
	for _, ep := range d.GetEndpoints() {
		h.Endpoints = append(h.Endpoints, camera.Endpoint{URL: ep.GetUri(), NetworkScope: mustId(d.GetNetwork().GetId()).String()})
	}
	return h
}

func (s Core) cameraResolvePreset(ctx context.Context, j *api.CameraJob, a *api.CameraAssignment, d *api.CameraDevice, identity *api.CameraIdentityEvidence) (*api.CameraResolvedPreset, error) {
	if err := cameraPresetIdentity(identity); err != nil {
		return nil, err
	}
	if ref := j.GetInputs().GetPreset(); ref != nil {
		id, err := cameraPresetID(ref.GetId(), DomCameraPreset)
		if err != nil {
			return nil, err
		}
		p, err := s.Next().CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		if err := cameraPresetScope(p, a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
			return nil, err
		}
		if p.GetRevision() != ref.GetRevision() {
			return nil, failed("pinned preset revision is unavailable")
		}
		provider, err := presetwire.Provider(mustId(p.GetId()).String(), p.GetRevision(), p.GetRules())
		if err != nil {
			return nil, failed("stored preset rules are invalid")
		}
		resolved, err := provider.Resolve(ctx, cameraPresetEnrollmentScope(a), cameraPresetDevice(d, identity))
		if err != nil {
			return nil, failed("pinned camera preset could not be resolved")
		}
		return presetwire.EncodeSnapshot(resolved)
	}
	// Direct inputs are already immutable job state. Presenting them as one
	// candidate lets the same engine apply initialization prerequisites without
	// inventing a separate mutable preset or changing the public engine seam.
	var settings camera.PresetSettings
	if v := j.GetInputs().GetConfiguration(); v != nil {
		var err error
		settings, err = presetwire.DecodeSettings(v)
		if err != nil {
			return nil, failed("direct job configuration is invalid")
		}
	} else if p := j.GetInputs().GetDesired(); p != nil {
		settings.Video = camera.DesiredState{Codec: p.GetCodec(), Width: int(p.GetWidth()), Height: int(p.GetHeight()), FPS: p.GetFps(), Bitrate: p.GetMaxBitrateBps()}
	}
	settings = camera.MergePresetSettings(camera.PresetSettings{}, settings)
	resolved := camera.DevicePreset{Ref: camera.PresetRef{ID: "job/" + mustId(j.GetId()).String(), Revision: "direct-v1"}, Requested: settings}
	return presetwire.EncodeSnapshot(resolved)
}

func (s coreCameraPreset) ResolveForJob(ctx context.Context, r *api.CameraPresetResolveForJobRequest) (*api.CameraResolvedPreset, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	who, err := s.cameraExecutor(ctx)
	if err != nil {
		return nil, err
	}
	var out *api.CameraResolvedPreset
	err = s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		j, a, _, d, err := c.cameraJobRows(ctx, r.GetJob())
		if err != nil {
			return err
		}
		if err := c.cameraLease(j, a, d, who, r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
			return err
		}
		if v := j.GetPinnedPreset(); v != nil {
			if _, err := presetwire.DecodeSnapshot(v); err != nil {
				return failed("pinned job snapshot is invalid")
			}
			out = proto.Clone(v).(*api.CameraResolvedPreset)
			return nil
		}
		identity := r.GetIdentity()
		if identity == nil {
			identity = d.GetIdentity()
		}
		if prior := d.GetIdentity(); prior.GetAuthenticated() {
			if !identity.GetAuthenticated() {
				identity = prior
			} else if (prior.GetSerial() != "" && prior.GetSerial() != identity.GetSerial()) || (prior.GetHardware() != "" && prior.GetHardware() != identity.GetHardware()) {
				return failed("authenticated physical identity changed")
			}
		}
		out, err = c.cameraResolvePreset(ctx, j, a, d, identity)
		return err
	})
	if err != nil {
		return nil, err
	}
	// The job's row graph is current inside the transaction; reject a revoked
	// or superseded certificate again before returning its scoped settings.
	if who.Domain() == DomCameraWorker {
		if _, err := s.cameraWorkerActor(ctx); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func cameraPresetReferenceMatches(v *api.CameraResolvedPreset, j *api.CameraJob) bool {
	if p := j.GetInputs().GetPreset(); p != nil {
		return v.GetId() == p.GetId() && strings.HasPrefix(v.GetRevision(), p.GetRevision()+"~")
	}
	return v.GetId() == "job/"+mustId(j.GetId()).String() && v.GetRevision() == "direct-v1"
}
