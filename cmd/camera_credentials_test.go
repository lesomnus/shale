package cmd

import (
	"context"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cc "github.com/lesomnus/shale/internal/cameracredential"
	"github.com/lesomnus/shale/server/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func enrollmentCredentialGraph() core.CameraEnrollmentPreparation {
	id := func(domain pdid.Domain) []byte { return pdid.New(domain).Bytes() }
	tenant := api.Tenant_builder{Id: id(core.DomTenant)}.Build()
	network := api.CameraNetwork_builder{Id: id(core.DomCameraNetwork), Tenant: tenant}.Build()
	a := api.CameraAssignment_builder{Id: id(core.DomCameraAssignment), Tenant: tenant, Network: network, Producer: api.Producer_builder{Id: id(core.DomProducer)}.Build(), Set: api.Set_builder{Id: id(core.DomSet)}.Build()}.Build()
	d := api.CameraDevice_builder{Id: id(core.DomCameraDevice), Tenant: tenant, Network: network}.Build()
	b := api.CameraBinding_builder{Id: id(core.DomCameraBinding), Tenant: tenant, Assignment: a, Device: d, Source: api.Source_builder{Id: id(core.DomSource)}.Build()}.Build()
	p := api.CameraPolicy_builder{Id: id(core.DomCameraPolicy), Tenant: tenant, Network: network, Enrollment: api.CameraEnrollmentPolicy_builder{AllowActivation: true}.Build()}.Build()
	return core.CameraEnrollmentPreparation{Assignment: a, Device: d, Binding: b, Policy: p, IntentKey: "enrollment-unit", InitialUsername: "admin", AllowActivation: true}
}

func TestEnrollmentCredentialAdapterRejectsMixedGraphBeforeStore(t *testing.T) {
	s := &Server{}
	for name, change := range map[string]func(*core.CameraEnrollmentPreparation){
		"tenant": func(r *core.CameraEnrollmentPreparation) {
			r.Device.SetTenant(api.Tenant_builder{Id: pdid.New(core.DomTenant).Bytes()}.Build())
		},
		"NULL site": func(r *core.CameraEnrollmentPreparation) {
			r.Binding.SetSite(api.Site_builder{Id: pdid.New(core.DomSite).Bytes()}.Build())
		},
		"assignment": func(r *core.CameraEnrollmentPreparation) {
			r.Binding.SetAssignment(api.CameraAssignment_builder{Id: pdid.New(core.DomCameraAssignment).Bytes()}.Build())
		},
		"device": func(r *core.CameraEnrollmentPreparation) {
			r.Binding.SetDevice(api.CameraDevice_builder{Id: pdid.New(core.DomCameraDevice).Bytes()}.Build())
		},
		"device network": func(r *core.CameraEnrollmentPreparation) {
			r.Device.SetNetwork(api.CameraNetwork_builder{Id: pdid.New(core.DomCameraNetwork).Bytes()}.Build())
		},
		"policy network": func(r *core.CameraEnrollmentPreparation) {
			r.Policy.SetNetwork(api.CameraNetwork_builder{Id: pdid.New(core.DomCameraNetwork).Bytes()}.Build())
		},
		"policy domain": func(r *core.CameraEnrollmentPreparation) { r.Policy.SetId(pdid.New(core.DomCameraPreset).Bytes()) },
		"activation":    func(r *core.CameraEnrollmentPreparation) { r.Policy.GetEnrollment().SetAllowActivation(false) },
	} {
		t.Run(name, func(t *testing.T) {
			r := enrollmentCredentialGraph()
			// Clone separately: nested builder messages may share the fixture's
			// parents, while each test intentionally changes only one graph edge.
			r.Assignment = proto.Clone(r.Assignment).(*api.CameraAssignment)
			r.Device = proto.Clone(r.Device).(*api.CameraDevice)
			r.Binding = proto.Clone(r.Binding).(*api.CameraBinding)
			r.Policy = proto.Clone(r.Policy).(*api.CameraPolicy)
			change(&r)
			out, err := s.prepareCameraEnrollment(t.Context(), r)
			require.ErrorIs(t, err, cc.ErrInput)
			require.Equal(t, core.CameraEnrollmentCredentials{}, out)
		})
	}
	r := enrollmentCredentialGraph()
	_, err := s.prepareCameraEnrollment(t.Context(), r)
	require.ErrorIs(t, err, cc.ErrUnavailable, "a valid graph cannot escape to a pool when its current store is absent")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.prepareCameraEnrollment(ctx, r)
	require.ErrorIs(t, err, context.Canceled)
}
