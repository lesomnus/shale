package core

import (
	"context"
	"strings"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func tlsIdentity(device []byte, digit string) *api.CameraDeviceTLSIdentity {
	return api.CameraDeviceTLSIdentity_builder{DeviceId: mustId(device).String(), SpkiSha256: "sha256:" + strings.Repeat(digit, 64)}.Build()
}

type tlsPolicyServer struct {
	bootstrapPolicyServer
	devices map[string]*api.CameraDevice
}

func (s tlsPolicyServer) CameraDevice() api.CameraDeviceServiceServer {
	return tlsPolicyDevices{values: s.devices}
}

type tlsPolicyDevices struct {
	api.UnimplementedCameraDeviceServiceServer
	values map[string]*api.CameraDevice
}

func (s tlsPolicyDevices) Get(_ context.Context, q *api.CameraDeviceGetRequest) (*api.CameraDevice, error) {
	d := s.values[string(q.GetRef().GetId())]
	if d == nil {
		return nil, status.Error(codes.NotFound, "fixture absent")
	}
	return proto.Clone(d).(*api.CameraDevice), nil
}
func TestCameraTLSIdentityPolicySyntaxAndExactScope(t *testing.T) {
	f := newProgressFixture(t, false)
	pin := tlsIdentity(f.d.GetId(), "a")
	policy := api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile(), DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{pin}}.Build()
	c := New(tlsPolicyServer{devices: map[string]*api.CameraDevice{string(f.d.GetId()): f.d}}, &Deps{})
	require.NoError(t, c.cameraEnrollmentPolicy(t.Context(), f.network, policy))
	for _, test := range []struct {
		name string
		edit func(*api.CameraEnrollmentPolicy)
	}{
		{"duplicate", func(p *api.CameraEnrollmentPolicy) {
			p.SetDeviceTlsIdentities(append(p.GetDeviceTlsIdentities(), proto.Clone(pin).(*api.CameraDeviceTLSIdentity)))
		}},
		{"nil", func(p *api.CameraEnrollmentPolicy) { p.SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{nil}) }},
		{"missing-bootstrap", func(p *api.CameraEnrollmentPolicy) { p.ClearBootstrap() }},
		{"http-bootstrap", func(p *api.CameraEnrollmentPolicy) { p.GetBootstrap().SetWebScheme("http") }},
		{"uppercase-digest", func(p *api.CameraEnrollmentPolicy) {
			p.GetDeviceTlsIdentities()[0].SetSpkiSha256("sha256:" + strings.Repeat("A", 64))
		}},
		{"missing-prefix", func(p *api.CameraEnrollmentPolicy) {
			p.GetDeviceTlsIdentities()[0].SetSpkiSha256(strings.Repeat("a", 64))
		}},
		{"wrong-domain", func(p *api.CameraEnrollmentPolicy) {
			p.GetDeviceTlsIdentities()[0].SetDeviceId(pdid.New(DomCameraNetwork).String())
		}},
		{"noncanonical-id", func(p *api.CameraEnrollmentPolicy) {
			p.GetDeviceTlsIdentities()[0].SetDeviceId(strings.ToUpper(pin.GetDeviceId()))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := proto.Clone(policy).(*api.CameraEnrollmentPolicy)
			test.edit(v)
			require.Error(t, c.cameraEnrollmentPolicy(t.Context(), f.network, v))
		})
	}
	for _, scope := range []string{"tenant", "site", "network", "erased"} {
		t.Run(scope, func(t *testing.T) {
			d := proto.Clone(f.d).(*api.CameraDevice)
			switch scope {
			case "tenant":
				d.SetTenant(api.Tenant_builder{Id: pdid.New(DomTenant).Bytes()}.Build())
			case "site":
				d.SetSite(api.Site_builder{Id: pdid.New(DomSite).Bytes()}.Build())
			case "network":
				d.SetNetwork(api.CameraNetwork_builder{Id: pdid.New(DomCameraNetwork).Bytes()}.Build())
			case "erased":
				d.SetDateErased(timestamppb.Now())
			}
			c := New(tlsPolicyServer{devices: map[string]*api.CameraDevice{string(d.GetId()): d}}, &Deps{})
			require.Error(t, c.cameraEnrollmentPolicy(t.Context(), f.network, policy))
		})
	}
	require.NoError(t, (Core{}).cameraEnrollmentPolicy(t.Context(), nil, api.CameraEnrollmentPolicy_builder{}.Build()), "no pin leaves legacy policy unchanged")
}
func TestCameraTLSIdentityEnrollmentSelectionAndDirectJobMatch(t *testing.T) {
	f := newProgressFixture(t, false)
	preset := api.CameraPreset_builder{Id: pdid.New(DomCameraPreset).Bytes(), Tenant: f.a.GetTenant(), Revision: "preset-v1"}.Build()
	pin := tlsIdentity(f.d.GetId(), "a")
	other := tlsIdentity(pdid.New(DomCameraDevice).Bytes(), "b")
	policy := api.CameraPolicy_builder{Id: pdid.New(DomCameraPolicy).Bytes(), Tenant: f.a.GetTenant(), Network: f.a.GetNetwork(), Revision: "policy-v1", Preset: preset, Enrollment: api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile(), DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{pin, other}}.Build()}.Build()
	input := cameraEnrollmentInputs(policy, preset, false, f.d.GetId())
	require.True(t, proto.Equal(pin, input.GetDeviceTlsIdentity()))
	require.NotSame(t, pin, input.GetDeviceTlsIdentity())
	pin.SetSpkiSha256("sha256:" + strings.Repeat("c", 64))
	require.Equal(t, "sha256:"+strings.Repeat("a", 64), input.GetDeviceTlsIdentity().GetSpkiSha256())
	pin.SetSpkiSha256(input.GetDeviceTlsIdentity().GetSpkiSha256())
	c := New(bootstrapPolicyServer{progressServer: progressServer{preset: preset}, policy: policy}, &Deps{})
	out, err := c.cameraPinnedInputs(t.Context(), f.a, f.d, input)
	require.NoError(t, err)
	require.NotSame(t, out.GetDeviceTlsIdentity(), input.GetDeviceTlsIdentity())
	for _, edit := range []func(*api.CameraJobInputs){
		func(i *api.CameraJobInputs) { i.ClearDeviceTlsIdentity() },
		func(i *api.CameraJobInputs) { i.SetDeviceTlsIdentity(other) },
		func(i *api.CameraJobInputs) {
			i.GetDeviceTlsIdentity().SetSpkiSha256("sha256:" + strings.Repeat("d", 64))
		},
		func(i *api.CameraJobInputs) { i.ClearPolicy() },
	} {
		changed := proto.Clone(input).(*api.CameraJobInputs)
		edit(changed)
		_, err := c.cameraPinnedInputs(t.Context(), f.a, f.d, changed)
		require.Error(t, err)
	}
	changed := proto.Clone(f.a).(*api.CameraAssignment)
	changed.SetNetwork(api.CameraNetwork_builder{Id: pdid.New(DomCameraNetwork).Bytes()}.Build())
	_, err = c.cameraPinnedInputs(t.Context(), changed, f.d, input)
	require.Error(t, err, "assignment changes cannot borrow the old network's policy pin")
	policy.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{other})
	input = cameraEnrollmentInputs(policy, preset, false, f.d.GetId())
	require.Nil(t, input.GetDeviceTlsIdentity(), "a job must not receive another device's policy approval")
	_, err = c.cameraPinnedInputs(t.Context(), f.a, f.d, input)
	require.NoError(t, err)
	input.ClearPolicy()
	input.ClearPreset()
	input.ClearBootstrap()
	_, err = c.cameraPinnedInputs(t.Context(), f.a, f.d, input)
	require.NoError(t, err, "legacy direct inputs retain strict PKI")
}
func TestCameraTLSIdentityCheckpointRetainsPinBeforeHistoricalShortcut(t *testing.T) {
	f := newProgressFixture(t, false)
	pin := tlsIdentity(f.d.GetId(), "a")
	f.j.GetInputs().SetBootstrap(bootstrapProfile())
	f.j.GetInputs().SetDeviceTlsIdentity(pin)
	f.run.Request.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 8443}
	f.run.Request.DeviceTLSIdentity = cameratls.Decode(pin)
	_, err := f.check(t)
	require.NoError(t, err)
	// Historical status from an old Producer is permitted only with the same
	// immutable TLS approval, before the equal-checkpoint shortcut is considered.
	f.j.SetObserved(progressObservation(t, f.run))
	f.a.SetProducer(api.Producer_builder{Id: pdid.New(DomProducer).Bytes()}.Build())
	_, err = f.check(t)
	require.NoError(t, err)
	for _, edit := range []func(*api.CameraProvisionState){
		func(p *api.CameraProvisionState) { p.GetRequest().ClearDeviceTlsIdentity() },
		func(p *api.CameraProvisionState) {
			p.GetRequest().GetDeviceTlsIdentity().SetSpkiSha256("sha256:" + strings.Repeat("b", 64))
		},
		func(p *api.CameraProvisionState) {
			p.GetRequest().GetDeviceTlsIdentity().SetDeviceId(pdid.New(DomCameraDevice).String())
		},
	} {
		observed := progressObservation(t, f.run)
		edit(observed.GetProgress())
		f.j.SetObserved(proto.Clone(observed).(*api.CameraJobObservation))
		_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observed)
		require.Error(t, err, "equal historical payload cannot discard or substitute the job pin")
	}
	f.j.GetInputs().ClearDeviceTlsIdentity()
	observed := progressObservation(t, f.run)
	f.j.SetObserved(proto.Clone(observed).(*api.CameraJobObservation))
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observed)
	require.Error(t, err, "an unpinned job cannot add a TLS exception in a checkpoint")
}
