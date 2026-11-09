package core

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func factoryPolicy() *api.CameraEnrollmentPolicy {
	return api.CameraEnrollmentPolicy_builder{Automatic: true, AllowAny: true, AllowActivation: true, TrustNewFactoryTls: true, MaxDevices: 4, Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420Known, WebScheme: "https", WebPort: 443}.Build()}.Build()
}

func TestCameraFactoryTLSPolicyRequiresExplicitIsolatedActivation(t *testing.T) {
	n := api.CameraNetwork_builder{Cidrs: []string{"192.168.250.0/24"}, InterfaceHint: "camera0"}.Build()
	require.NoError(t, cameraFactoryTLSPolicy(nil, &api.CameraEnrollmentPolicy{}), "default-off policy keeps normal PKI")
	for _, profile := range []string{tplink.ProfileC420Known, tplink.ProfileC420V120, tplink.ProfileC420V122} {
		p := factoryPolicy()
		p.GetBootstrap().SetCompatibilityProfile(profile)
		require.NoError(t, cameraFactoryTLSPolicy(n, p))
	}
	for _, edit := range []func(*api.CameraNetwork, *api.CameraEnrollmentPolicy){
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.SetAutomatic(false) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.SetAllowActivation(false) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.SetMaxDevices(0) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.ClearBootstrap() },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.GetBootstrap().SetWebScheme("http") },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { p.GetBootstrap().SetWebPort(444) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) {
			p.GetBootstrap().SetCompatibilityProfile("unknown-factory")
		},
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { n.SetInterfaceHint("") },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { n.SetCidrs(nil) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { n.SetCidrs([]string{"192.0.2.0/24"}) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { n.SetCidrs([]string{"192.168.0.0/15"}) },
		func(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) { n.SetCidrs([]string{"0.0.0.0/0"}) },
	} {
		p, network := factoryPolicy(), proto.Clone(n).(*api.CameraNetwork)
		edit(network, p)
		require.Error(t, cameraFactoryTLSPolicy(network, p))
	}
}

func TestCameraFactoryTLSPinPersistsIntoFutureJobInputsWithoutRotation(t *testing.T) {
	f := newProgressFixture(t, false)
	pin := tlsIdentity(f.d.GetId(), "a")
	p := api.CameraPolicy_builder{Enrollment: factoryPolicy()}.Build()
	input := api.CameraJobInputs_builder{}.Build()
	require.Error(t, cameraExactTLSIdentity(p, f.a, f.d, input), "no job may bypass pre-bind trust")
	f.d.SetFactoryTls(api.CameraFactoryTLSState_builder{Plan: api.CameraFactoryTLSPlan_builder{DeviceId: f.d.GetId()}.Build(), Identity: pin, DateTrusted: timestamppb.Now()}.Build())
	input.SetDeviceTlsIdentity(cameraDeviceTLSIdentity(p, f.d))
	require.NoError(t, cameraExactTLSIdentity(p, f.a, f.d, input))
	require.NotSame(t, pin, input.GetDeviceTlsIdentity())
	input.GetDeviceTlsIdentity().SetSpkiSha256("sha256:" + strings.Repeat("b", 64))
	require.Error(t, cameraExactTLSIdentity(p, f.a, f.d, input))
	// Ordinary later policy revisions also retain the accepted key. Turning off
	// automatic first-use trust does not silently return this camera to no pin.
	p.GetEnrollment().SetTrustNewFactoryTls(false)
	require.True(t, proto.Equal(pin, cameraDeviceTLSIdentity(p, f.d)))
	f.d.GetFactoryTls().ClearDateTrusted()
	require.Nil(t, cameraDeviceTLSIdentity(p, f.d), "a pending public plan is never trust")
}
