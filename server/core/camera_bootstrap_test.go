package core

import (
	"context"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func bootstrapProfile() *api.CameraBootstrapProfile {
	return api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 8443}.Build()
}

func TestCameraBootstrapSyntaxAtPolicyAndDirectJobBoundaries(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *api.CameraBootstrapProfile
		valid bool
	}{
		{"absent", nil, true},
		{"explicit", bootstrapProfile(), true},
		{"empty", api.CameraBootstrapProfile_builder{}.Build(), false},
		{"missing-profile", api.CameraBootstrapProfile_builder{WebScheme: "https", WebPort: 443}.Build(), false},
		{"missing-scheme", api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-v1", WebPort: 443}.Build(), false},
		{"missing-port", api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-v1", WebScheme: "https"}.Build(), false},
		{"large-port", api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-v1", WebScheme: "http", WebPort: 65536}.Build(), false},
		{"url-profile", api.CameraBootstrapProfile_builder{CompatibilityProfile: "https://user@camera", WebScheme: "https", WebPort: 443}.Build(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			policyErr := (Core{}).cameraEnrollmentPolicy(t.Context(), nil, api.CameraEnrollmentPolicy_builder{Bootstrap: test.value}.Build())
			jobErr := cameraInputs(api.CameraJobInputs_builder{Bootstrap: test.value}.Build())
			if test.valid {
				require.NoError(t, policyErr)
				require.NoError(t, jobErr)
			} else {
				require.Error(t, policyErr)
				require.Error(t, jobErr)
			}
		})
	}
}

type bootstrapPolicyServer struct {
	progressServer
	policy *api.CameraPolicy
}

func (s bootstrapPolicyServer) CameraPolicy() api.CameraPolicyServiceServer {
	return bootstrapPolicy{value: s.policy}
}

type bootstrapPolicy struct {
	api.UnimplementedCameraPolicyServiceServer
	value *api.CameraPolicy
}

func (s bootstrapPolicy) Get(context.Context, *api.CameraPolicyGetRequest) (*api.CameraPolicy, error) {
	return proto.Clone(s.value).(*api.CameraPolicy), nil
}

func TestCameraEnrollmentBootstrapIsClonedAndExactPolicyIntent(t *testing.T) {
	f := newProgressFixture(t, false)
	preset := api.CameraPreset_builder{Id: pdid.New(DomCameraPreset).Bytes(), Tenant: f.a.GetTenant(), Revision: "preset-v1"}.Build()
	policy := api.CameraPolicy_builder{Id: pdid.New(DomCameraPolicy).Bytes(), Tenant: f.a.GetTenant(), Network: f.a.GetNetwork(), Revision: "policy-v1", Preset: preset, Enrollment: api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile()}.Build()}.Build()
	inputs := cameraEnrollmentInputs(policy, preset, false, f.d.GetId())
	require.True(t, proto.Equal(policy.GetEnrollment().GetBootstrap(), inputs.GetBootstrap()))
	require.NotSame(t, policy.GetEnrollment().GetBootstrap(), inputs.GetBootstrap())
	policy.GetEnrollment().GetBootstrap().SetWebPort(443)
	require.Equal(t, int32(8443), inputs.GetBootstrap().GetWebPort())
	policy.GetEnrollment().SetBootstrap(bootstrapProfile())
	c := New(bootstrapPolicyServer{progressServer: progressServer{preset: preset}, policy: policy}, &Deps{})
	out, err := c.cameraPinnedInputs(t.Context(), f.a, f.d, inputs)
	require.NoError(t, err)
	require.NotSame(t, inputs.GetBootstrap(), out.GetBootstrap())
	out.GetBootstrap().SetWebPort(80)
	require.Equal(t, int32(8443), inputs.GetBootstrap().GetWebPort())
	for _, edit := range []func(*api.CameraJobInputs){
		func(v *api.CameraJobInputs) { v.ClearBootstrap() },
		func(v *api.CameraJobInputs) { v.GetBootstrap().SetCompatibilityProfile("other") },
		func(v *api.CameraJobInputs) { v.GetBootstrap().SetWebScheme("http") },
		func(v *api.CameraJobInputs) { v.GetBootstrap().SetWebPort(443) },
	} {
		changed := proto.Clone(inputs).(*api.CameraJobInputs)
		edit(changed)
		_, err := c.cameraPinnedInputs(t.Context(), f.a, f.d, changed)
		require.Error(t, err)
	}
	policy.GetEnrollment().ClearBootstrap()
	_, err = c.cameraPinnedInputs(t.Context(), f.a, f.d, inputs)
	require.Error(t, err, "a legacy policy cannot acquire a caller's bootstrap pin")
	inputs.ClearBootstrap()
	_, err = c.cameraPinnedInputs(t.Context(), f.a, f.d, inputs)
	require.NoError(t, err)
}

func TestCameraProgressRetainsExactBootstrapBeforeHistoricalStatus(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "pinned"}[pinned], func(t *testing.T) {
			f := newProgressFixture(t, false)
			if pinned {
				f.j.GetInputs().SetBootstrap(bootstrapProfile())
				f.run.Request.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 8443}
			}
			_, err := f.check(t)
			require.NoError(t, err)
			if pinned {
				f.run.Request.Bootstrap.WebPort = 443
			} else {
				f.run.Request.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 8443}
			}
			_, err = f.check(t)
			require.Error(t, err)
			f.j.SetObserved(progressObservation(t, f.run))
			f.a.SetProducer(api.Producer_builder{Id: pdid.New(DomProducer).Bytes()}.Build())
			_, err = f.check(t)
			require.Error(t, err, "historical status cannot bypass the immutable bootstrap pin")
			if pinned {
				f.run.Request.Bootstrap = nil
				_, err = f.check(t)
				require.Error(t, err, "a checkpoint cannot discard the pin")
			}
		})
	}
}
