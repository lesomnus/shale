package e2e_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/server/core"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCameraPolicyScopedObservationAndAtomicEnrollment(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	server.Deps.PrepareCameraEnrollment = nil // exercise an explicitly unconfigured embedding
	worker, conn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "auto-worker", nil)
	_, other, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "auto-other", nil)
	producer, _, producerConn := cameraWorkerProducer(t, ctx, server, cp, admin, "auto-producer", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "auto-network", nil)
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	presets := api.NewCameraPresetServiceClient(admin)
	p, err := presets.Create(ctx, cameraPolicyPresetRequest("auto-preset", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, n.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	request := cameraPolicyCreateRequest(n, p, "auto-policy", "1")
	request.GetEnrollment().SetAllowActivation(true)
	request.GetEnrollment().SetInitialUsername("fixture-admin")
	request.GetEnrollment().SetMaxDevices(1)
	policy, err := policies.Create(ctx, request)
	require.NoError(t, err)
	n, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: n.GetDateUpdated()}.Build())
	require.NoError(t, err)
	observe := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), AssignmentGeneration: a.GetGeneration(), DiscoveryId: "urn:uuid:fixture-camera", Identity: api.CameraIdentityEvidence_builder{Manufacturer: "fixture", Model: "TestCam", Serial: "unauthenticated", Authenticated: true}.Build(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10/onvif/device_service"}.Build()}}.Build()
	client := api.NewCameraPolicyServiceClient(conn)
	_, err = api.NewCameraPolicyServiceClient(other).Observe(ctx, observe)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewCameraPolicyServiceClient(producerConn).Observe(ctx, observe)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	first, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "credential_preparer_unavailable", first.GetCode())
	require.Equal(t, first.GetCode(), first.GetDevice().GetEnrollment().GetCode())
	require.False(t, first.GetDevice().GetIdentity().GetAuthenticated(), "a discovery response cannot claim authenticated identity")
	require.Nil(t, first.GetBinding())
	require.Equal(t, 0, mustCameraBindingCount(t, ctx, server))
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	server.Deps.PrepareCameraEnrollment = func(ctx context.Context, q core.CameraEnrollmentPreparation) (core.CameraEnrollmentCredentials, error) {
		calls.Add(1)
		require.NotNil(t, q.Driver)
		require.Equal(t, "fixture-admin", q.InitialUsername)
		require.True(t, q.AllowActivation)
		// A recorded write through the injected transaction must roll back with
		// a failed secret preparation, including the new binding and Source.
		source, err := q.Server.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: q.Binding.GetSource().GetId()}.Build()}.Build())
		if err != nil {
			return core.CameraEnrollmentCredentials{}, err
		}
		_, err = q.Server.Source().Patch(ctx, api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: source.GetId()}.Build(), Name: z.Ptr("prepared fixture"), DateUpdated: source.GetDateUpdated()}.Build())
		if err != nil {
			return core.CameraEnrollmentCredentials{}, err
		}
		if fail.Load() {
			return core.CameraEnrollmentCredentials{}, errors.New("must-not-echo-fixture-secret")
		}
		ref := func(purpose string) *api.CameraCredentialReference {
			return api.CameraCredentialReference_builder{Provider: "fixture", Id: cameraPolicyID(t, q.Binding.GetId()) + "/" + purpose, Version: "1", Purpose: purpose}.Build()
		}
		return core.CameraEnrollmentCredentials{Initial: ref("initial_admin"), Management: ref("management"), Streaming: ref("streaming")}, nil
	}
	second, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "credentials_unavailable", second.GetCode())
	require.Equal(t, first.GetDevice().GetId(), second.GetDevice().GetId())
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, 0, mustCameraBindingCount(t, ctx, server))
	sources, err := server.Ent.Source.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, sources, "the failed enrollment cannot leave a partial Source")
	fail.Store(false)
	third, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "enrolled", third.GetCode())
	require.NotNil(t, third.GetBinding())
	require.NotNil(t, third.GetJob())
	require.Equal(t, policy.GetRevision(), third.GetJob().GetInputs().GetPolicy().GetRevision())
	require.Equal(t, p.GetRevision(), third.GetJob().GetInputs().GetPreset().GetRevision())
	require.True(t, third.GetJob().GetInputs().GetAllowActivation())
	require.Equal(t, int32(2), calls.Load())
	repeated, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "reconciliation_pending", repeated.GetCode())
	require.Equal(t, third.GetJob().GetId(), repeated.GetJob().GetId())
	require.Equal(t, third.GetBinding().GetSource().GetId(), repeated.GetBinding().GetSource().GetId())
	require.Equal(t, int32(2), calls.Load(), "repeated discovery does not regenerate credentials")
	unknown := proto.Clone(observe).(*api.CameraPolicyObserveRequest)
	unknown.SetDiscoveryId("urn:uuid:another-camera")
	unknown.GetEndpoints()[0].SetUri("http://192.0.2.11/onvif/device_service")
	limited, err := client.Observe(ctx, unknown)
	require.NoError(t, err)
	require.Equal(t, "capacity_exceeded", limited.GetCode())
	require.NotEqual(t, first.GetDevice().GetId(), limited.GetDevice().GetId())
	require.Nil(t, limited.GetBinding())
	require.Equal(t, int32(2), calls.Load())
	bad := proto.Clone(observe).(*api.CameraPolicyObserveRequest)
	bad.GetEndpoints()[0].SetUri("http://198.51.100.10/onvif/device_service")
	_, err = client.Observe(ctx, bad)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func mustCameraBindingCount(t *testing.T, ctx context.Context, server *cmd.Server) int {
	t.Helper()
	n, err := server.Ent.CameraBinding.Query().Count(ctx)
	require.NoError(t, err)
	return n
}
