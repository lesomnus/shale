package core

import (
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCameraTLSVerificationPolicySyntax(t *testing.T) {
	f := newProgressFixture(t, false)
	c := New(tlsPolicyServer{devices: map[string]*api.CameraDevice{string(f.d.GetId()): f.d}}, &Deps{})
	skip := api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile(), TlsVerification: camera.TLSVerificationSkip}.Build()
	require.NoError(t, c.cameraEnrollmentPolicy(t.Context(), f.network, skip))
	require.NoError(t, c.cameraEnrollmentPolicy(t.Context(), f.network, api.CameraEnrollmentPolicy_builder{}.Build()), "absent is the default: verify")
	for name, edit := range map[string]func(*api.CameraEnrollmentPolicy){
		"uppercase":      func(p *api.CameraEnrollmentPolicy) { p.SetTlsVerification("SKIP") },
		"verify":         func(p *api.CameraEnrollmentPolicy) { p.SetTlsVerification("verify") },
		"no bootstrap":   func(p *api.CameraEnrollmentPolicy) { p.ClearBootstrap() },
		"http bootstrap": func(p *api.CameraEnrollmentPolicy) { p.GetBootstrap().SetWebScheme("http") },
		"with a pin": func(p *api.CameraEnrollmentPolicy) {
			p.SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{tlsIdentity(f.d.GetId(), "a")})
		},
		"with factory trust": func(p *api.CameraEnrollmentPolicy) { p.SetTrustNewFactoryTls(true) },
	} {
		v := proto.Clone(skip).(*api.CameraEnrollmentPolicy)
		edit(v)
		require.Equal(t, codes.InvalidArgument, status.Code(c.cameraEnrollmentPolicy(t.Context(), f.network, v)), name)
	}
}

// Skipping verification is never a default and never a caller's choice: a
// job skips only when its pinned policy revision says so, and then pins no
// key, not even a device's trusted factory key.
func TestCameraTLSVerificationSkipIsOffUnlessThePolicySaysSo(t *testing.T) {
	f := newProgressFixture(t, false)
	preset := api.CameraPreset_builder{Id: pdid.New(DomCameraPreset).Bytes(), Tenant: f.a.GetTenant(), Revision: "preset-v1"}.Build()
	pin := tlsIdentity(f.d.GetId(), "a")
	f.d.SetFactoryTls(api.CameraFactoryTLSState_builder{Plan: api.CameraFactoryTLSPlan_builder{DeviceId: f.d.GetId()}.Build(), Identity: pin, DateTrusted: timestamppb.Now()}.Build())
	verifying := api.CameraPolicy_builder{Id: pdid.New(DomCameraPolicy).Bytes(), Tenant: f.a.GetTenant(), Network: f.a.GetNetwork(), Revision: "policy-v1", Preset: preset, Enrollment: api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile()}.Build()}.Build()
	skipping := proto.Clone(verifying).(*api.CameraPolicy)
	skipping.SetId(pdid.New(DomCameraPolicy).Bytes())
	skipping.SetRevision("policy-skip")
	skipping.GetEnrollment().SetTlsVerification(camera.TLSVerificationSkip)

	input := cameraEnrollmentInputs(verifying, preset, false, f.d)
	require.Empty(t, input.GetTlsVerification(), "a policy that does not say so verifies")
	require.True(t, proto.Equal(pin, input.GetDeviceTlsIdentity()))
	skipped := cameraEnrollmentInputs(skipping, preset, false, f.d)
	require.Equal(t, camera.TLSVerificationSkip, skipped.GetTlsVerification())
	require.Nil(t, skipped.GetDeviceTlsIdentity(), "skipping pins no key")
	require.NoError(t, cameraInputs(skipped))

	pinned := func(p *api.CameraPolicy, in *api.CameraJobInputs) error {
		_, err := New(bootstrapPolicyServer{progressServer: progressServer{preset: preset}, policy: p}, &Deps{}).cameraPinnedInputs(t.Context(), f.a, f.d, in)
		return err
	}
	require.NoError(t, pinned(verifying, input))
	require.NoError(t, pinned(skipping, skipped))

	added := proto.Clone(input).(*api.CameraJobInputs)
	added.SetTlsVerification(camera.TLSVerificationSkip)
	added.ClearDeviceTlsIdentity()
	require.Equal(t, codes.PermissionDenied, status.Code(pinned(verifying, added)), "a caller cannot add skip to a verifying policy's job")
	dropped := proto.Clone(skipped).(*api.CameraJobInputs)
	dropped.SetTlsVerification("")
	require.Equal(t, codes.PermissionDenied, status.Code(pinned(skipping, dropped)), "nor drop the policy's setting")
	direct := proto.Clone(skipped).(*api.CameraJobInputs)
	direct.ClearPolicy()
	direct.ClearPreset()
	require.Equal(t, codes.PermissionDenied, status.Code(pinned(skipping, direct)), "a job without a policy never skips")

	both := proto.Clone(skipped).(*api.CameraJobInputs)
	both.SetDeviceTlsIdentity(pin)
	require.Equal(t, codes.InvalidArgument, status.Code(cameraInputs(both)))
	unknown := proto.Clone(skipped).(*api.CameraJobInputs)
	unknown.SetTlsVerification("off")
	require.Equal(t, codes.InvalidArgument, status.Code(cameraInputs(unknown)))
	cleartext := proto.Clone(skipped).(*api.CameraJobInputs)
	cleartext.GetBootstrap().SetWebScheme("http")
	require.Equal(t, codes.InvalidArgument, status.Code(cameraInputs(cleartext)))
}

func TestCameraProgressSkipIdentityIsTheRecordedMACAndSerial(t *testing.T) {
	recorded := api.CameraIdentityEvidence_builder{Serial: "unit-a", Macs: []string{"a8:29:dc:c6:26:f3"}, Authenticated: true}.Build()
	d := api.CameraDevice_builder{Id: pdid.New(DomCameraDevice).Bytes(), Identity: recorded}.Build()
	skip := api.CameraJob_builder{Inputs: api.CameraJobInputs_builder{TlsVerification: camera.TLSVerificationSkip}.Build()}.Build()
	verify := api.CameraJob_builder{Inputs: api.CameraJobInputs_builder{}.Build()}.Build()
	run := func(serial, mac string) provision.Run {
		return provision.Run{Inspection: &camera.Inspection{Device: camera.DeviceHint{Serial: serial, MAC: mac, Evidence: []camera.Evidence{{Source: "fixture", Authenticated: true}}}}}
	}
	require.NoError(t, cameraProgressSkipIdentity(skip, run("unit-a", "A8-29-DC-C6-26-F3"), d))
	for name, r := range map[string]provision.Run{
		"another serial": run("unit-b", "a8:29:dc:c6:26:f3"),
		"another MAC":    run("unit-a", "a8:29:dc:c6:26:f4"),
		"no MAC":         run("unit-a", ""),
	} {
		require.Equal(t, codes.FailedPrecondition, status.Code(cameraProgressSkipIdentity(skip, r, d)), name)
		require.NoError(t, cameraProgressSkipIdentity(verify, r, d), "a verifying job is identified by its key")
	}
	unauthenticated := run("unit-b", "")
	unauthenticated.Inspection.Device.Evidence = nil
	require.NoError(t, cameraProgressSkipIdentity(skip, unauthenticated, d), "only authenticated inspections are compared")
	d.GetIdentity().SetAuthenticated(false)
	require.NoError(t, cameraProgressSkipIdentity(skip, run("unit-b", "a8:29:dc:c6:26:f4"), d), "a first authenticated inspection is recorded")
}
