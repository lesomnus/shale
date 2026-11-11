package cameraworker

import (
	"context"
	"errors"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
)

// JobPresets resolves only the immutable revision of the current leased job.
// It neither reads the network's latest policy nor downloads tenant templates.
type JobPresets struct {
	Execution *Execution
	Client    api.CameraPresetServiceClient
}

func cameraID(raw []byte) string {
	id, err := pdid.From(raw)
	if err != nil || id.IsZero() {
		return ""
	}
	return id.String()
}

func enrollmentScope(a *api.CameraAssignment) camera.EnrollmentScope {
	return camera.EnrollmentScope{TenantID: cameraID(a.GetTenant().GetId()), SiteID: cameraID(a.GetSite().GetId()), SetID: cameraID(a.GetSet().GetId()), ProducerID: cameraID(a.GetProducer().GetId()), NetworkID: cameraID(a.GetNetwork().GetId())}
}

func identityEvidence(h camera.DeviceHint) *api.CameraIdentityEvidence {
	v := api.CameraIdentityEvidence_builder{Manufacturer: h.Manufacturer, Model: h.Model, Hardware: h.Hardware, Firmware: h.Firmware, Serial: h.Serial}.Build()
	if h.MAC != "" {
		v.SetMacs([]string{h.MAC})
	}
	for _, e := range h.Evidence {
		if e.Authenticated {
			v.SetAuthenticated(true)
		}
	}
	return v
}

func deviceHint(w *api.CameraWorkerWork) camera.DeviceHint {
	d, a := w.GetDevice(), w.GetAssignment()
	i := d.GetIdentity()
	h := camera.DeviceHint{ID: camera.DeviceID(cameraID(d.GetId())), Manufacturer: i.GetManufacturer(), Model: i.GetModel(), Hardware: i.GetHardware(), Firmware: i.GetFirmware(), Serial: i.GetSerial()}
	if len(i.GetMacs()) > 0 {
		h.MAC = i.GetMacs()[0]
	}
	if i.GetAuthenticated() {
		h.Evidence = []camera.Evidence{{Source: "cp-inspection", Authenticated: true}}
	}
	for _, ep := range d.GetEndpoints() {
		h.Endpoints = append(h.Endpoints, camera.Endpoint{URL: ep.GetUri(), NetworkScope: cameraID(a.GetNetwork().GetId())})
	}
	return h
}

func (p JobPresets) Resolve(ctx context.Context, scope camera.EnrollmentScope, h camera.DeviceHint) (out camera.DevicePreset, err error) {
	if p.Execution == nil {
		return out, camera.ErrInvalid
	}
	client := p.Client
	if client == nil {
		client = p.Execution.presets
	}
	if client == nil {
		return out, camera.ErrInvalid
	}
	err = p.Execution.Step(ctx, func(ctx context.Context, work *api.CameraWorkerWork) error {
		if scope != enrollmentScope(work.GetAssignment()) || h.ID != camera.DeviceID(cameraID(work.GetDevice().GetId())) {
			return camera.ErrForbidden
		}
		j := work.GetJob()
		value, err := client.ResolveForJob(ctx, api.CameraPresetResolveForJobRequest_builder{Job: api.CameraJobRef_builder{Id: j.GetId()}.Build(), LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Identity: identityEvidence(h)}.Build())
		if err != nil {
			return errors.Join(camera.ErrNeedsUserInput, privateExecutionError{err})
		}
		out, err = presetwire.DecodeSnapshot(value)
		return err
	})
	return out, err
}

// Device/CP implementations can put credentials or raw vendor responses in
// errors. Retain classification for callers without exposing their text.
type privateExecutionError struct{ cause error }

func (privateExecutionError) Error() string   { return "camera execution operation failed" }
func (e privateExecutionError) Unwrap() error { return e.cause }
