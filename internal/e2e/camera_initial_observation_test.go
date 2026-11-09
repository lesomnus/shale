package e2e_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/server/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCameraInitialObservationPreservesManualInventoryBindingAndSource(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	worker, machine, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "initial-worker", nil)
	producer, _, _ := cameraWorkerProducer(t, ctx, server, cp, admin, "initial-producer", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "initial-lan", nil)
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	const mac = "02:00:00:00:00:01"
	const epr = "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248"
	eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10:2020/onvif/device_service", Provenance: "operator_l2_observation"}.Build()}
	devices := api.NewCameraDeviceServiceClient(admin)
	d, err := devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), DeviceKey: "mac/" + mac, Endpoints: eps}.Build())
	require.NoError(t, err)
	d, err = devices.Observe(ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), ExpectedVersion: d.GetDateUpdated(), Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build(), Endpoints: eps}.Build())
	require.NoError(t, err)
	b, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: "operator-selected-camera", SourceAlias: "initial-stream"}.Build())
	require.NoError(t, err)
	observe := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), AssignmentGeneration: a.GetGeneration(), DiscoveryId: "mac/" + mac, Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build(), Endpoints: eps}.Build()
	client := api.NewCameraPolicyServiceClient(machine)
	first, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "policy_missing", first.GetCode())
	require.Equal(t, d.GetId(), first.GetDevice().GetId(), "an initial observation reuses the operator's stable key")
	require.Nil(t, first.GetJob())
	var preparations atomic.Int32
	server.Deps.PrepareCameraEnrollment = func(_ context.Context, q core.CameraEnrollmentPreparation) (core.CameraEnrollmentCredentials, error) {
		preparations.Add(1)
		ref := func(purpose string) *api.CameraCredentialReference {
			return api.CameraCredentialReference_builder{Provider: "fixture", Id: cameraPolicyID(t, q.Binding.GetId()) + "/" + purpose, Version: "1", Purpose: purpose}.Build()
		}
		return core.CameraEnrollmentCredentials{Initial: ref("initial_admin"), Management: ref("management"), Streaming: ref("streaming")}, nil
	}
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("initial-preset", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, n.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	policyRequest := cameraPolicyCreateRequest(n, preset, "initial-policy", "1")
	policyRequest.GetEnrollment().SetAllowActivation(true)
	policyRequest.GetEnrollment().SetMaxDevices(1)
	policy, err := api.NewCameraPolicyServiceClient(admin).Create(ctx, policyRequest)
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(admin).Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: n.GetDateUpdated()}.Build())
	require.NoError(t, err)
	enrolled, err := client.Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "enrolled", enrolled.GetCode())
	require.Equal(t, d.GetId(), enrolled.GetDevice().GetId())
	require.Equal(t, b.GetId(), enrolled.GetBinding().GetId(), "automatic observation must reuse the existing manual logical binding")
	require.Equal(t, b.GetSource().GetId(), enrolled.GetBinding().GetSource().GetId())
	require.NotNil(t, enrolled.GetJob())
	late := proto.Clone(observe).(*api.CameraPolicyObserveRequest)
	late.SetDiscoveryId(epr)
	repeated, err := client.Observe(ctx, late)
	require.NoError(t, err)
	require.Equal(t, d.GetId(), repeated.GetDevice().GetId())
	require.Equal(t, enrolled.GetJob().GetId(), repeated.GetJob().GetId())
	require.Equal(t, b.GetId(), repeated.GetBinding().GetId())
	require.Equal(t, epr, repeated.GetDevice().GetEnrollment().GetDiscoveryId())
	late.GetIdentity().SetMacs(nil)
	repeated, err = client.Observe(ctx, late)
	require.NoError(t, err)
	require.Equal(t, d.GetId(), repeated.GetDevice().GetId(), "persisted EPR evidence resolves without a new MAC hint")
	require.Equal(t, []string{mac}, repeated.GetDevice().GetIdentity().GetMacs())
	require.Equal(t, int32(1), preparations.Load(), "rediscovery cannot prepare a second credential bundle")
	current, err := devices.Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
	require.NoError(t, err)
	current, err = devices.Observe(ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), ExpectedVersion: current.GetDateUpdated(), Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}, Manufacturer: "fixture", Model: "claimed-model", Serial: "authenticated-unit", Authenticated: true}.Build(), Endpoints: eps}.Build())
	require.NoError(t, err)
	late.GetIdentity().SetMacs([]string{mac})
	late.GetIdentity().SetSerial("forged-unit")
	repeated, err = client.Observe(ctx, late)
	require.NoError(t, err)
	require.True(t, repeated.GetDevice().GetIdentity().GetAuthenticated())
	require.Equal(t, "authenticated-unit", repeated.GetDevice().GetIdentity().GetSerial())
	moved := proto.Clone(late).(*api.CameraPolicyObserveRequest)
	moved.GetEndpoints()[0].SetUri("http://192.0.2.20:2020/onvif/device_service")
	_, err = client.Observe(ctx, moved)
	require.Equal(t, codes.Aborted, status.Code(err), "a MAC or EPR cannot redirect authenticated device material")
	_, err = client.Observe(ctx, observe)
	require.NoError(t, err)
	repeated, err = client.Observe(ctx, late)
	require.NoError(t, err)
	require.Equal(t, d.GetId(), repeated.GetDevice().GetId(), "an initial-only scan cannot erase the later EPR alias")
	// A conflicting EPR row is never merged into the MAC-selected camera.
	digest := sha256.Sum256([]byte(epr))
	_, err = devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), DeviceKey: "discovery/" + hex.EncodeToString(digest[:]), Endpoints: eps}.Build())
	require.NoError(t, err)
	_, err = client.Observe(ctx, late)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Equal(t, 1, mustCameraBindingCount(t, ctx, server))
	sources, err := server.Ent.Source.Query().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, sources)
	require.Equal(t, int32(1), preparations.Load())
	stale := proto.Clone(observe).(*api.CameraPolicyObserveRequest)
	stale.SetAssignmentGeneration(a.GetGeneration() + 1)
	_, err = client.Observe(ctx, stale)
	require.Equal(t, codes.Aborted, status.Code(err))
}
