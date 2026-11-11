package e2e_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Actual tenant TLS, adopted machine certificates and the default transaction-
// bound encrypted credential store; no camera transport or material retrieval.
func newFactoryTLSFixture(t *testing.T, scoped bool) edgeCredentialFixture {
	t.Helper()
	f := newEdgeCredentialFixture(t, true, scoped)
	var err error
	f.network, err = api.NewCameraNetworkServiceClient(f.admin).Configure(f.ctx, api.CameraNetworkConfigureRequest_builder{Ref: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), ExpectedVersion: f.network.GetDateUpdated(), Alias: f.network.GetAlias(), Cidrs: []string{"192.168.250.0/24"}, InterfaceHint: "camera0"}.Build())
	require.NoError(t, err)
	request := cameraPolicyCreateRequest(f.network, f.policy.GetPreset(), "factory-tls-policy", "1")
	request.GetEnrollment().SetAllowActivation(true)
	request.GetEnrollment().SetTrustNewFactoryTls(true)
	request.GetEnrollment().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: tplink.ProfileC420Known, WebScheme: "https", WebPort: 443}.Build())
	f.policy, err = api.NewCameraPolicyServiceClient(f.admin).Create(f.ctx, request)
	require.NoError(t, err)
	f.network, err = api.NewCameraPolicyServiceClient(f.admin).Activate(f.ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: f.policy.GetId()}.Build(), ExpectedNetworkVersion: f.network.GetDateUpdated()}.Build())
	require.NoError(t, err)
	f.request.SetEndpoints([]*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.168.250.10:2020/onvif/device_service"}.Build()})
	return f
}

func factoryTLSObserve(t *testing.T, f edgeCredentialFixture) *api.CameraPolicyObserveResponse {
	t.Helper()
	v, err := api.NewCameraPolicyServiceClient(f.workerConn).Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, "factory_tls_required", v.GetCode())
	require.NotNil(t, v.GetFactoryTlsPlan())
	require.Nil(t, v.GetBinding())
	require.Nil(t, v.GetJob())
	return v
}

func factoryTLSRequest(t *testing.T, v *api.CameraPolicyObserveResponse, digit string) *api.CameraPolicyTrustFactoryTLSRequest {
	t.Helper()
	return api.CameraPolicyTrustFactoryTLSRequest_builder{Plan: proto.Clone(v.GetFactoryTlsPlan()).(*api.CameraFactoryTLSPlan), Identity: api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, v.GetDevice().GetId()), SpkiSha256: "sha256:" + strings.Repeat(digit, 64)}.Build(), ActivationRequired: true}.Build()
}

func factoryTLSCounts(t *testing.T, f edgeCredentialFixture, bindings, jobs, credentials, versions int) {
	t.Helper()
	b, err := f.server.Ent.CameraBinding.Query().Where(camerabinding.DateErasedIsNil()).Count(f.ctx)
	require.NoError(t, err)
	j, err := f.server.Ent.CameraJob.Query().Where(camerajob.DateErasedIsNil()).Count(f.ctx)
	require.NoError(t, err)
	c, err := f.server.Ent.CameraCredential.Query().Where(cameracredential.DateErasedIsNil()).Count(f.ctx)
	require.NoError(t, err)
	v, err := f.server.Ent.CameraCredentialVersion.Query().Where(cameracredentialversion.DateErasedIsNil()).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, []int{bindings, jobs, credentials, versions}, []int{b, j, c, v})
}

func TestCameraPolicyFactoryTLSPrebindAndDurableReplay(t *testing.T) {
	f := newFactoryTLSFixture(t, false)
	first := factoryTLSObserve(t, f)
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	sources, err := f.server.Ent.Source.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Zero(t, sources, "a switch or initialized unknown account must not consume Source capacity")
	request := factoryTLSRequest(t, first, "a")
	client := api.NewCameraPolicyServiceClient(f.workerConn)
	_, err = client.TrustFactoryTLS(f.ctx, api.CameraPolicyTrustFactoryTLSRequest_builder{Plan: request.GetPlan(), Identity: request.GetIdentity()}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = api.NewCameraPolicyServiceClient(f.admin).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "normal operator authority does not impersonate the assigned LAN observer")
	accepted, err := client.TrustFactoryTLS(f.ctx, request)
	require.NoError(t, err)
	require.NotNil(t, accepted.GetJob())
	require.True(t, proto.Equal(request.GetIdentity(), accepted.GetJob().GetInputs().GetDeviceTlsIdentity()))
	require.True(t, proto.Equal(request.GetIdentity(), accepted.GetDevice().GetFactoryTls().GetIdentity()))
	factoryTLSCounts(t, f, 1, 1, 3, 3)
	// Exact retry is independent of a response reaching the Worker. No new
	// credential versions, binding, Source or immutable Job inputs are created.
	for range 2 {
		replay, err := client.TrustFactoryTLS(f.ctx, proto.Clone(request).(*api.CameraPolicyTrustFactoryTLSRequest))
		require.NoError(t, err)
		require.Equal(t, accepted.GetJob().GetId(), replay.GetJob().GetId())
		require.True(t, proto.Equal(accepted.GetJob().GetInputs(), replay.GetJob().GetInputs()))
	}
	again, err := client.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, accepted.GetJob().GetId(), again.GetJob().GetId())
	require.Nil(t, again.GetFactoryTlsPlan())
	_, err = client.TrustFactoryTLS(f.ctx, request)
	require.NoError(t, err, "an accepted replay survives an unchanged fresh observation")
	changed := proto.Clone(request).(*api.CameraPolicyTrustFactoryTLSRequest)
	changed.GetIdentity().SetSpkiSha256("sha256:" + strings.Repeat("b", 64))
	_, err = client.TrustFactoryTLS(f.ctx, changed)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	factoryTLSCounts(t, f, 1, 1, 3, 3)
	_, err = api.NewCameraDeviceServiceClient(f.admin).Patch(f.ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), FactoryTls: requestState(request), DateUpdated: again.GetDevice().GetDateUpdated()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "generic mutation cannot grant or rotate first-use trust")
}

func requestState(r *api.CameraPolicyTrustFactoryTLSRequest) *api.CameraFactoryTLSState {
	return api.CameraFactoryTLSState_builder{Plan: r.GetPlan(), Identity: r.GetIdentity(), DateTrusted: timestamppb.Now()}.Build()
}

func TestCameraPolicyFactoryTLSSnapshotFencesAndRacingKey(t *testing.T) {
	f := newFactoryTLSFixture(t, true)
	first := factoryTLSObserve(t, f)
	stale := factoryTLSRequest(t, first, "a")
	current := factoryTLSObserve(t, f)
	client := api.NewCameraPolicyServiceClient(f.workerConn)
	_, err := client.TrustFactoryTLS(f.ctx, stale)
	require.Equal(t, codes.Aborted, status.Code(err), "even an unchanged scan advances the first-use snapshot fence")
	request := factoryTLSRequest(t, current, "a")
	for _, edit := range []func(*api.CameraPolicyTrustFactoryTLSRequest){
		func(r *api.CameraPolicyTrustFactoryTLSRequest) {
			r.GetPlan().SetAssignmentGeneration(r.GetPlan().GetAssignmentGeneration() + 1)
		},
		func(r *api.CameraPolicyTrustFactoryTLSRequest) {
			r.GetPlan().SetObservationRevision(r.GetPlan().GetObservationRevision() + 1)
		},
		func(r *api.CameraPolicyTrustFactoryTLSRequest) { r.GetPlan().SetEndpoint("https://192.168.250.11:443") },
		func(r *api.CameraPolicyTrustFactoryTLSRequest) {
			r.GetPlan().SetDateExpires(timestamppb.New(time.Now().Add(-time.Minute)))
		},
		func(r *api.CameraPolicyTrustFactoryTLSRequest) { r.GetPlan().GetBootstrap().SetWebPort(444) },
		func(r *api.CameraPolicyTrustFactoryTLSRequest) { r.GetPlan().GetPolicy().SetRevision("unapproved") },
	} {
		changed := proto.Clone(request).(*api.CameraPolicyTrustFactoryTLSRequest)
		edit(changed)
		_, err := client.TrustFactoryTLS(f.ctx, changed)
		require.Error(t, err)
	}
	other, conn, _ := cameraWorkerMachine(t, f.ctx, f.server, f.cp, f.admin, "factory-other-worker", f.site)
	_ = other
	_, err = api.NewCameraPolicyServiceClient(conn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	var wg sync.WaitGroup
	errors := make([]error, 2)
	results := make([]*api.CameraPolicyObserveResponse, 2)
	for i := range 2 {
		candidate := factoryTLSRequest(t, current, []string{"a", "b"}[i])
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errors[i] = client.TrustFactoryTLS(f.ctx, candidate)
		}()
	}
	wg.Wait()
	successes := 0
	for i, err := range errors {
		if err == nil {
			successes++
			require.NotNil(t, results[i].GetJob())
		} else {
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		}
	}
	require.Equal(t, 1, successes)
	factoryTLSCounts(t, f, 1, 1, 3, 3)
}

func TestCameraPolicyFactoryTLSTrustAndCredentialsRollbackWithJob(t *testing.T) {
	f := newFactoryTLSFixture(t, false)
	first := factoryTLSObserve(t, f)
	request := factoryTLSRequest(t, first, "a")
	release := edgeCredentialJobFault(t, f)
	_, err := api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Error(t, err)
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	sources, err := f.server.Ent.Source.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Zero(t, sources)
	d, err := api.NewCameraDeviceServiceClient(f.admin).Get(f.ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), Select: api.CameraDeviceSelect_builder{All: z.Ptr(true)}.Build()}.Build())
	require.NoError(t, err)
	require.Nil(t, d.GetFactoryTls().GetIdentity(), "failed enrollment cannot commit a trusted key without its job")
	release()
	accepted, err := api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.NoError(t, err)
	require.NotNil(t, accepted.GetJob())
	factoryTLSCounts(t, f, 1, 1, 3, 3)
}

func TestCameraPolicyFactoryTLSRejectsPriorAccountsAndExpiredPlan(t *testing.T) {
	f := newFactoryTLSFixture(t, false)
	first := factoryTLSObserve(t, f)
	request := factoryTLSRequest(t, first, "a")
	// Advance the persisted fixture clock snapshot rather than changing the
	// request alone: this reaches the expiry check after exact stored equality.
	expired := proto.Clone(first.GetDevice().GetFactoryTls()).(*api.CameraFactoryTLSState)
	expired.GetPlan().SetDateIssued(timestamppb.New(time.Now().Add(-4 * time.Minute)))
	expired.GetPlan().SetDateExpires(timestamppb.New(time.Now().Add(-2 * time.Minute)))
	_, err := f.server.Ent.CameraDevice.Update().Where(cameradevice.DeviceKeyEQ(first.GetDevice().GetDeviceKey())).SetFactoryTls(expired).Save(f.ctx)
	require.NoError(t, err)
	request.SetPlan(expired.GetPlan())
	_, err = api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.Aborted, status.Code(err))
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	first = factoryTLSObserve(t, f)
	request = factoryTLSRequest(t, first, "a")
	payload := credentialPayload(t, "installed-manager", "existing-fixture-password")
	defer clear(payload)
	_, err = api.NewCameraCredentialServiceClient(f.admin).Import(f.ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), Purpose: "management", IntentKey: "prior-installed-account", Access: "configure", Material: payload}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "first-use trust never takes over an imported account")
	factoryTLSCounts(t, f, 0, 0, 1, 1)
}

func TestCameraPolicyFactoryTLSCurrentNetworkAndAssignmentFence(t *testing.T) {
	f := newFactoryTLSFixture(t, false)
	first := factoryTLSObserve(t, f)
	request := factoryTLSRequest(t, first, "a")
	var err error
	f.network, err = api.NewCameraNetworkServiceClient(f.admin).Configure(f.ctx, api.CameraNetworkConfigureRequest_builder{Ref: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), ExpectedVersion: f.network.GetDateUpdated(), Alias: f.network.GetAlias(), Name: "network edited while probing", Cidrs: f.network.GetCidrs(), InterfaceHint: f.network.GetInterfaceHint()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.Aborted, status.Code(err))
	first = factoryTLSObserve(t, f)
	request = factoryTLSRequest(t, first, "a")
	a, err := api.NewCameraAssignmentServiceClient(f.admin).Get(f.ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build()}.Build())
	require.NoError(t, err)
	f.assignment, err = api.NewCameraAssignmentServiceClient(f.admin).Assign(f.ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), ExpectedVersion: a.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: f.producer.GetId()}.Build(), WorkerSlot: "new-slot", WorkerId: f.worker.GetId()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.Aborted, status.Code(err))
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	f.request.SetAssignmentGeneration(f.assignment.GetGeneration())
	first = factoryTLSObserve(t, f)
	request = factoryTLSRequest(t, first, "a")
	accepted, err := api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, f.assignment.GetGeneration(), accepted.GetJob().GetAssignmentGeneration())
}

func TestCameraPolicyFactoryTLSRechecksCanonicalHostOwnership(t *testing.T) {
	f := newFactoryTLSFixture(t, false)
	first := factoryTLSObserve(t, f)
	request := factoryTLSRequest(t, first, "a")
	devices := api.NewCameraDeviceServiceClient(f.admin)
	other, err := devices.Claim(f.ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), DeviceKey: "other-host-owner"}.Build())
	require.NoError(t, err)
	// Another operator observation can claim this canonical address without
	// changing the first Device's observation or the Network's version. Port,
	// path and an IPv4-mapped spelling must not hide the collision.
	other, err = devices.Observe(f.ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: other.GetId()}.Build(), ExpectedVersion: other.GetDateUpdated(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://[::ffff:192.168.250.10]:80/other-service"}.Build()}}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.Equal(t, codes.Aborted, status.Code(err), "the current network inventory must have one exact owner of the probed host")
	factoryTLSCounts(t, f, 0, 0, 0, 0)
	d, err := devices.Get(f.ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), Select: api.CameraDeviceSelect_builder{All: z.Ptr(true)}.Build()}.Build())
	require.NoError(t, err)
	require.Nil(t, d.GetFactoryTls().GetIdentity())
	_, err = devices.Observe(f.ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: other.GetId()}.Build(), ExpectedVersion: other.GetDateUpdated(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.168.250.11:80/other-service"}.Build()}}.Build())
	require.NoError(t, err)
	accepted, err := api.NewCameraPolicyServiceClient(f.workerConn).TrustFactoryTLS(f.ctx, request)
	require.NoError(t, err, "explicitly resolving the collision permits the still-current original plan")
	require.NotNil(t, accepted.GetJob())
}
