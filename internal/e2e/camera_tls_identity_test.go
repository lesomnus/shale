package e2e_test

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Real tenant TLS, machine certificates and Roster operator grants qualify the
// CP trust-approval boundary; no camera transport or credential material is used.
func TestCameraPolicyDeviceTLSIdentityAuthorityAndJobPins(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	producer, _, producerConn := cameraWorkerProducer(t, ctx, server, cp, admin, "tls-pin-producer", nil)
	worker, workerConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "tls-pin-worker", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "tls-pin-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "tls-pin", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	devices := api.NewCameraDeviceServiceClient(admin)
	claim := func(key string, network *api.CameraNetwork) *api.CameraDevice {
		t.Helper()
		v, e := devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), DeviceKey: key}.Build())
		require.NoError(t, e)
		return v
	}
	d1, d2 := claim("tls-pin-device-a", n), claim("tls-pin-device-b", n)
	pin := func(d *api.CameraDevice, digit string) *api.CameraDeviceTLSIdentity {
		return api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, d.GetId()), SpkiSha256: "sha256:" + strings.Repeat(digit, 64)}.Build()
	}
	firstPin, secondPin := pin(d1, "a"), pin(d2, "b")
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("tls-pin-preset", "1", cameraPolicyRule("base", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, n.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	request := cameraPolicyCreateRequest(n, preset, "tls-pin-policy", "1")
	request.SetEnrollment(api.CameraEnrollmentPolicy_builder{Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 443}.Build(), DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{firstPin, secondPin}}.Build())
	_, err = api.NewCameraPolicyServiceClient(producerConn).Create(ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "Producer cannot approve its own factory certificate")
	_, err = api.NewCameraPolicyServiceClient(workerConn).Create(ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "Worker discovery does not grant certificate trust approval")
	policies := api.NewCameraPolicyServiceClient(admin)
	approved, err := policies.Create(ctx, request)
	require.NoError(t, err)
	repeated, err := policies.Create(ctx, proto.Clone(request).(*api.CameraPolicyCreateRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(approved, repeated))
	changed := proto.Clone(request).(*api.CameraPolicyCreateRequest)
	changed.GetEnrollment().GetDeviceTlsIdentities()[0].SetSpkiSha256("sha256:" + strings.Repeat("c", 64))
	_, err = policies.Create(ctx, changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err), "an immutable approval revision cannot change key")
	duplicate := proto.Clone(request).(*api.CameraPolicyCreateRequest)
	duplicate.SetRevision("duplicate")
	duplicate.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{firstPin, firstPin})
	_, err = policies.Create(ctx, duplicate)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	otherNetwork := cameraPolicyNetwork(t, ctx, admin, "tls-pin-other-lan", nil)
	otherDevice := claim("tls-pin-foreign-network", otherNetwork)
	foreign := proto.Clone(request).(*api.CameraPolicyCreateRequest)
	foreign.SetRevision("foreign-network")
	foreign.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{pin(otherDevice, "c")})
	_, err = policies.Create(ctx, foreign)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "tenant operator cannot attach another network's Device pin")
	httpPolicy := proto.Clone(request).(*api.CameraPolicyCreateRequest)
	httpPolicy.SetRevision("http")
	httpPolicy.GetEnrollment().GetBootstrap().SetWebScheme("http")
	_, err = policies.Create(ctx, httpPolicy)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "pin approval must name an explicit HTTPS bootstrap destination")
	bindings := api.NewCameraBindingServiceClient(admin)
	bind := func(d *api.CameraDevice, alias string) *api.CameraBinding {
		t.Helper()
		v, e := bindings.Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: alias, SourceAlias: alias}.Build())
		require.NoError(t, e)
		return v
	}
	b1, b2 := bind(d1, "tls-pin-a"), bind(d2, "tls-pin-b")
	input := api.CameraJobInputs_builder{Policy: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, approved.GetId()), Revision: approved.GetRevision()}.Build(), Bootstrap: proto.Clone(approved.GetEnrollment().GetBootstrap()).(*api.CameraBootstrapProfile), DeviceTlsIdentity: proto.Clone(firstPin).(*api.CameraDeviceTLSIdentity)}.Build()
	jobs := api.NewCameraJobServiceClient(admin)
	create := api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b1.GetId()}.Build(), RequestKey: "tls-pin-job-a", Inputs: input}.Build()
	job, err := jobs.Create(ctx, create)
	require.NoError(t, err)
	require.True(t, proto.Equal(firstPin, job.GetInputs().GetDeviceTlsIdentity()))
	replay, err := jobs.Create(ctx, proto.Clone(create).(*api.CameraJobCreateRequest))
	require.NoError(t, err)
	require.Equal(t, job.GetId(), replay.GetId())
	require.True(t, proto.Equal(job.GetInputs(), replay.GetInputs()))
	for _, test := range []struct {
		name string
		edit func(*api.CameraJobCreateRequest)
	}{
		{"missing", func(q *api.CameraJobCreateRequest) { q.GetInputs().ClearDeviceTlsIdentity() }},
		{"wrong-device", func(q *api.CameraJobCreateRequest) { q.GetInputs().SetDeviceTlsIdentity(secondPin) }},
		{"wrong-binding", func(q *api.CameraJobCreateRequest) {
			q.SetBinding(api.CameraBindingRef_builder{Id: b2.GetId()}.Build())
		}},
		{"unapproved", func(q *api.CameraJobCreateRequest) { q.GetInputs().ClearPolicy() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := proto.Clone(create).(*api.CameraJobCreateRequest)
			q.SetRequestKey("tls-pin-denied-" + test.name)
			test.edit(q)
			_, e := jobs.Create(ctx, q)
			require.Equal(t, codes.PermissionDenied, status.Code(e))
		})
	}
	legacy := api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b2.GetId()}.Build(), RequestKey: "tls-pin-legacy-direct", Inputs: api.CameraJobInputs_builder{Desired: api.CameraCaptureProfile_builder{Codec: "h264", Width: 640, Height: 360, Fps: 15, MaxBitrateBps: 1_000_000}.Build()}.Build()}.Build()
	unpinned, err := jobs.Create(ctx, legacy)
	require.NoError(t, err)
	require.Nil(t, unpinned.GetInputs().GetDeviceTlsIdentity(), "ordinary direct jobs retain normal PKI")
}
