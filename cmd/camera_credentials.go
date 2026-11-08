package cmd

import (
	"bytes"
	"context"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cc "github.com/lesomnus/shale/internal/cameracredential"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/lesomnus/shale/server/core"
	"github.com/protobuf-orm/ent/dialect"
)

func (s *Server) cameraCredentialFactory(recorded api.Server) cc.StoreFactory {
	return func(current dialect.Driver) (cc.Store, error) {
		if s == nil {
			return nil, cc.ErrUnavailable
		}
		backend, err := camerasecret.NewAESGCM(s.Kek)
		if err != nil {
			return nil, cc.ErrUnavailable
		}
		return cc.NewSQLFactory(recorded, backend, backend, cc.SQLOptions{})(current)
	}
}

// Enrollment uses the request's recorded stack and existing transaction, never
// the CP's pool-bound stack. Core authorizes and locks the complete graph before
// invoking this adapter; these comparisons also prevent accidental scope mixups
// when composing that authority with the stable credential subject.
func (s *Server) prepareCameraEnrollment(ctx context.Context, r core.CameraEnrollmentPreparation) (core.CameraEnrollmentCredentials, error) {
	var out core.CameraEnrollmentCredentials
	if err := ctx.Err(); err != nil {
		return out, err
	}
	a, d, b, p := r.Assignment, r.Device, r.Binding, r.Policy
	for _, v := range []struct {
		raw    []byte
		domain pdid.Domain
	}{
		{a.GetId(), core.DomCameraAssignment}, {a.GetTenant().GetId(), core.DomTenant}, {a.GetNetwork().GetId(), core.DomCameraNetwork}, {a.GetSet().GetId(), core.DomSet}, {a.GetProducer().GetId(), core.DomProducer},
		{d.GetId(), core.DomCameraDevice}, {b.GetId(), core.DomCameraBinding}, {b.GetSource().GetId(), core.DomSource}, {p.GetId(), core.DomCameraPolicy},
	} {
		id, err := pdid.From(v.raw)
		if err != nil || id.Domain() != v.domain {
			return out, cc.ErrInput
		}
	}
	for _, v := range []struct{ tenant, site []byte }{
		{a.GetTenant().GetId(), a.GetSite().GetId()},
		{d.GetTenant().GetId(), d.GetSite().GetId()},
		{b.GetTenant().GetId(), b.GetSite().GetId()},
		{p.GetTenant().GetId(), p.GetSite().GetId()},
	} {
		if !bytes.Equal(v.tenant, a.GetTenant().GetId()) || !bytes.Equal(v.site, a.GetSite().GetId()) {
			return out, cc.ErrInput
		}
		if len(v.site) > 0 {
			id, err := pdid.From(v.site)
			if err != nil || id.Domain() != core.DomSite {
				return out, cc.ErrInput
			}
		}
	}
	if !bytes.Equal(b.GetAssignment().GetId(), a.GetId()) || !bytes.Equal(b.GetDevice().GetId(), d.GetId()) || !bytes.Equal(d.GetNetwork().GetId(), a.GetNetwork().GetId()) || !bytes.Equal(p.GetNetwork().GetId(), a.GetNetwork().GetId()) || r.AllowActivation && !p.GetEnrollment().GetAllowActivation() {
		return out, cc.ErrInput
	}
	store, err := s.cameraCredentialFactory(r.Server)(r.Driver)
	if err != nil {
		return out, err
	}
	tenant, _ := pdid.From(a.GetTenant().GetId())
	device, _ := pdid.From(d.GetId())
	binding, _ := pdid.From(b.GetId())
	username := []byte(r.InitialUsername)
	defer clear(username)
	bundle, err := cc.PrepareBundle(ctx, store, cc.BundleRequest{TenantID: tenant, DeviceID: device, BindingID: binding, IntentKey: r.IntentKey, InitialUsername: username, AllowActivation: r.AllowActivation}, core.GenerateInitialMaterial(username))
	if err != nil {
		return out, err
	}
	ref := func(v cc.Reference) *api.CameraCredentialReference {
		if v.CredentialID.IsZero() {
			return nil
		}
		return api.CameraCredentialReference_builder{Provider: cc.Provider, Id: v.CredentialID.String(), Version: v.Version, Purpose: string(v.Subject.Purpose)}.Build()
	}
	return core.CameraEnrollmentCredentials{Initial: ref(bundle.InitialAdmin), Management: ref(bundle.Management), Streaming: ref(bundle.Streaming)}, nil
}
