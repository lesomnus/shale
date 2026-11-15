package e2e_test

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Skipping TLS verification is a stored, visible setting of one policy
// revision, and the only way a job comes to skip it.
func TestCameraPolicyTLSVerificationSkipIsExplicitAndVisible(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	producer, _, _ := cameraWorkerProducer(t, ctx, server, cp, admin, "tls-skip-producer", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "tls-skip-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "tls-skip"}.Build())
	require.NoError(t, err)
	d, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "tls-skip-device"}.Build())
	require.NoError(t, err)
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("tls-skip-preset", "1", cameraPolicyRule("base", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, n.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	bootstrap := api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 443}.Build()
	policies := api.NewCameraPolicyServiceClient(admin)
	create := func(revision string, enrollment *api.CameraEnrollmentPolicy) (*api.CameraPolicy, error) {
		q := cameraPolicyCreateRequest(n, preset, "tls-skip-policy", revision)
		q.SetEnrollment(enrollment)
		return policies.Create(ctx, q)
	}

	verifying, err := create("verify", api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrap}.Build())
	require.NoError(t, err)
	require.Empty(t, verifying.GetEnrollment().GetTlsVerification(), "verification is the default")
	skipping, err := create("skip", api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrap, TlsVerification: cameramodel.TLSVerificationSkip}.Build())
	require.NoError(t, err)
	stored, err := policies.Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: skipping.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, cameramodel.TLSVerificationSkip, stored.GetEnrollment().GetTlsVerification(), "the policy shows that it skips verification")

	pin := api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, d.GetId()), SpkiSha256: "sha256:" + strings.Repeat("a", 64)}.Build()
	for name, enrollment := range map[string]*api.CameraEnrollmentPolicy{
		"unknown value": api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrap, TlsVerification: "off"}.Build(),
		"no bootstrap":  api.CameraEnrollmentPolicy_builder{TlsVerification: cameramodel.TLSVerificationSkip}.Build(),
		"with a pin":    api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrap, TlsVerification: cameramodel.TLSVerificationSkip, DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{pin}}.Build(),
	} {
		_, err := create("refused-"+strings.ReplaceAll(name, " ", "-"), enrollment)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}

	b, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: "tls-skip", SourceAlias: "tls-skip"}.Build())
	require.NoError(t, err)
	jobs := api.NewCameraJobServiceClient(admin)
	inputs := func(p *api.CameraPolicy, skip string) *api.CameraJobInputs {
		return api.CameraJobInputs_builder{Policy: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, p.GetId()), Revision: p.GetRevision()}.Build(), Bootstrap: proto.Clone(bootstrap).(*api.CameraBootstrapProfile), TlsVerification: skip}.Build()
	}
	job := func(key string, in *api.CameraJobInputs) (*api.CameraJob, error) {
		return jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: key, Inputs: in}.Build())
	}
	skipped, err := job("tls-skip-job", inputs(skipping, cameramodel.TLSVerificationSkip))
	require.NoError(t, err)
	require.Equal(t, cameramodel.TLSVerificationSkip, skipped.GetInputs().GetTlsVerification())
	require.Nil(t, skipped.GetInputs().GetDeviceTlsIdentity())

	_, err = job("tls-skip-added", inputs(verifying, cameramodel.TLSVerificationSkip))
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a caller cannot skip what the policy verifies")
	_, err = job("tls-skip-dropped", inputs(skipping, ""))
	require.Equal(t, codes.PermissionDenied, status.Code(err), "nor drop what the policy says")
	direct := inputs(skipping, cameramodel.TLSVerificationSkip)
	direct.ClearPolicy()
	_, err = job("tls-skip-direct", direct)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a job without a policy never skips")
}
