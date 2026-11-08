package e2e_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"github.com/lesomnus/z"
	"github.com/protobuf-orm/ent/dialect"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// These are credential/transaction contracts over real TLS and SQL. No device,
// network camera, producer capture or independently qualified media is involved.
type edgeCredentialFixture struct {
	server            *cmd.Server
	cp                string
	ctx               context.Context
	admin, workerConn *grpc.ClientConn
	worker            *api.CameraWorker
	producer          *api.Producer
	network           *api.CameraNetwork
	assignment        *api.CameraAssignment
	policy            *api.CameraPolicy
	site              *api.SiteRef
	request           *api.CameraPolicyObserveRequest
}

func newEdgeCredentialFixture(t *testing.T, activation, scoped bool) edgeCredentialFixture {
	t.Helper()
	s, cp, admin, ctx := cameraPolicyControl(t)
	require.NotNil(t, s.Deps.PrepareCameraEnrollment, "Build must install the real CP preparer without test injection")
	f := edgeCredentialFixture{server: s, cp: cp, ctx: ctx, admin: admin}
	if scoped {
		v, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "credential-edge-site"}.Build())
		require.NoError(t, err)
		f.site = api.SiteRef_builder{Id: v.GetId()}.Build()
	}
	f.worker, f.workerConn, _ = cameraWorkerMachine(t, ctx, s, cp, admin, "default-credential-worker", f.site)
	f.producer, _, _ = cameraWorkerProducer(t, ctx, s, cp, admin, "default-credential-producer", f.site)
	f.network = cameraPolicyNetwork(t, ctx, admin, "default-credential-network", f.site)
	var err error
	f.assignment, err = api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: f.producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: f.worker.GetId()}.Build())
	require.NoError(t, err)
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("default-credential-preset", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, f.network.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	request := cameraPolicyCreateRequest(f.network, preset, "default-credential-policy", "1")
	request.GetEnrollment().SetAllowActivation(activation)
	request.GetEnrollment().SetInitialUsername("fresh-activation-admin")
	f.policy, err = api.NewCameraPolicyServiceClient(admin).Create(ctx, request)
	require.NoError(t, err)
	f.network, err = api.NewCameraPolicyServiceClient(admin).Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: f.policy.GetId()}.Build(), ExpectedNetworkVersion: f.network.GetDateUpdated()}.Build())
	require.NoError(t, err)
	f.request = api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build(), AssignmentGeneration: f.assignment.GetGeneration(), DiscoveryId: "urn:uuid:default-central-credentials", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10/onvif/device_service"}.Build()}}.Build()
	return f
}

func edgeCredentialResolve(t *testing.T, ctx context.Context, conn *grpc.ClientConn, ref *api.CameraCredentialReference, grant *api.CameraWorkerCheckJobRequest) []byte {
	t.Helper()
	lease, err := api.NewCameraCredentialServiceClient(conn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: ref, Job: grant}.Build())
	require.NoError(t, err)
	material := bytes.Clone(lease.GetMaterial())
	clear(lease.GetMaterial())
	return material
}

func TestCameraPolicyDefaultCredentialPreparationReplayAndWorkerRecovery(t *testing.T) {
	f := newEdgeCredentialFixture(t, true, false)
	observe := api.NewCameraPolicyServiceClient(f.workerConn)
	first, err := observe.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, "enrolled", first.GetCode())
	j := first.GetJob()
	require.NotNil(t, j)
	inputs := j.GetInputs()
	initial := edgeCredentialResolve(t, f.ctx, f.admin, inputs.GetInitialCredential(), nil)
	defer clear(initial)
	for purpose, ref := range map[string]*api.CameraCredentialReference{"initial_admin": inputs.GetInitialCredential(), "management": inputs.GetManagementCredential(), "streaming": inputs.GetStreamingCredential()} {
		require.NotNil(t, ref)
		require.Equal(t, "cp", ref.GetProvider())
		require.Equal(t, purpose, ref.GetPurpose())
		value := edgeCredentialResolve(t, f.ctx, f.admin, ref, nil)
		require.Equal(t, initial, value, "one installed account is shared through separate delivery purposes")
		clear(value)
	}
	count, err := f.server.Ent.CameraCredentialVersion.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	repeated, err := observe.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, j.GetId(), repeated.GetJob().GetId())
	require.True(t, proto.Equal(inputs, repeated.GetJob().GetInputs()))
	count, err = f.server.Ent.CameraCredentialVersion.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	// A new operator enrollment intent gets its own immutable ledger entries,
	// while existing job/source pins keep the same installed account material.
	newIntent := api.CameraPolicyEnrollRequest_builder{Binding: api.CameraBindingRef_builder{Id: first.GetBinding().GetId()}.Build(), Policy: api.CameraPolicyRef_builder{Id: f.policy.GetId()}.Build(), RequestKey: "explicit-second-enrollment"}.Build()
	second, err := api.NewCameraPolicyServiceClient(f.admin).Enroll(f.ctx, newIntent)
	require.NoError(t, err)
	again, err := api.NewCameraPolicyServiceClient(f.admin).Enroll(f.ctx, newIntent)
	require.NoError(t, err)
	require.Equal(t, second.GetId(), again.GetId())
	require.NotEqual(t, inputs.GetInitialCredential().GetVersion(), second.GetInputs().GetInitialCredential().GetVersion())
	value := edgeCredentialResolve(t, f.ctx, f.admin, second.GetInputs().GetStreamingCredential(), nil)
	require.Equal(t, initial, value)
	clear(value)
	count, err = f.server.Ent.CameraCredentialVersion.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 6, count)
	jref := api.CameraJobRef_builder{Id: j.GetId()}.Build()
	j, err = api.NewCameraJobServiceClient(f.workerConn).Claim(f.ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: j.GetDateUpdated(), OperationKey: "first-worker", LeaseSeconds: 60}.Build())
	require.NoError(t, err)
	oldGrant := api.CameraWorkerCheckJobRequest_builder{Ref: jref, LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration()}.Build()
	value = edgeCredentialResolve(t, f.ctx, f.workerConn, inputs.GetStreamingCredential(), oldGrant)
	require.Equal(t, initial, value)
	clear(value)
	replacement, conn, _ := cameraWorkerMachine(t, f.ctx, f.server, f.cp, f.admin, "default-credential-replacement", f.site)
	a, err := api.NewCameraAssignmentServiceClient(f.admin).Get(f.ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraAssignmentServiceClient(f.admin).Assign(f.ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), ExpectedVersion: a.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: f.producer.GetId()}.Build(), WorkerSlot: "replacement", WorkerId: replacement.GetId()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraCredentialServiceClient(f.workerConn).Resolve(f.ctx, api.CameraCredentialResolveRequest_builder{Credential: inputs.GetStreamingCredential(), Job: oldGrant}.Build())
	require.Error(t, err)
	j, err = api.NewCameraJobServiceClient(f.admin).Get(f.ctx, api.CameraJobGetRequest_builder{Ref: jref}.Build())
	require.NoError(t, err)
	j, err = api.NewCameraJobServiceClient(conn).Claim(f.ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: j.GetDateUpdated(), OperationKey: "replacement-worker", LeaseSeconds: 60}.Build())
	require.NoError(t, err)
	grant := api.CameraWorkerCheckJobRequest_builder{Ref: jref, LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration()}.Build()
	value = edgeCredentialResolve(t, f.ctx, conn, inputs.GetStreamingCredential(), grant)
	require.Equal(t, initial, value)
	clear(value)
}

func edgeCredentialJobFault(t *testing.T, f edgeCredentialFixture) func() {
	t.Helper()
	if f.server.Dialect == dialect.Postgres {
		_, err := f.server.Db.ExecContext(f.ctx, `CREATE FUNCTION edge_credential_reject_job() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'fixture enrollment job failure'; END; $$ LANGUAGE plpgsql`)
		require.NoError(t, err)
		_, err = f.server.Db.ExecContext(f.ctx, `CREATE TRIGGER edge_credential_reject_job BEFORE INSERT ON camerajob FOR EACH ROW EXECUTE FUNCTION edge_credential_reject_job()`)
		require.NoError(t, err)
		return func() {
			_, err := f.server.Db.ExecContext(f.ctx, `DROP TRIGGER IF EXISTS edge_credential_reject_job ON camerajob`)
			require.NoError(t, err)
			_, err = f.server.Db.ExecContext(f.ctx, `DROP FUNCTION IF EXISTS edge_credential_reject_job()`)
			require.NoError(t, err)
		}
	}
	_, err := f.server.Db.ExecContext(f.ctx, `CREATE TRIGGER edge_credential_reject_job BEFORE INSERT ON camerajob BEGIN SELECT RAISE(ABORT, 'fixture enrollment job failure'); END`)
	require.NoError(t, err)
	return func() {
		_, err := f.server.Db.ExecContext(f.ctx, `DROP TRIGGER IF EXISTS edge_credential_reject_job`)
		require.NoError(t, err)
	}
}

func TestCameraPolicyDefaultCredentialPreparationRollsBackWithJob(t *testing.T) {
	f := newEdgeCredentialFixture(t, true, false)
	remove := edgeCredentialJobFault(t, f)
	_, err := api.NewCameraPolicyServiceClient(f.workerConn).Observe(f.ctx, f.request)
	require.Error(t, err)
	for _, table := range []string{"cameracredential", "cameracredentialversion", "camerabinding", "camerajob", "source"} {
		var count int
		require.NoError(t, f.server.Db.QueryRowContext(f.ctx, "SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count, "a failed job insert cannot commit an orphan "+table)
	}
	count, err := f.server.Ent.CameraDevice.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the preceding observation remains visible")
	remove()
	result, err := api.NewCameraPolicyServiceClient(f.workerConn).Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, "enrolled", result.GetCode())
	count, err = f.server.Ent.CameraCredentialVersion.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestCameraPolicyDefaultCredentialPreparationPreservesImportedManagement(t *testing.T) {
	f := newEdgeCredentialFixture(t, false, true)
	client := api.NewCameraPolicyServiceClient(f.workerConn)
	first, err := client.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, "credentials_unavailable", first.GetCode())
	require.Nil(t, first.GetJob())
	payload := credentialPayload(t, "existing-manager\xff", "installed-password\xfe\x00")
	defer clear(payload)
	imported, err := api.NewCameraCredentialServiceClient(f.admin).Import(f.ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), Purpose: "management", IntentKey: "existing-management-import", Access: "configure", Material: payload}.Build())
	require.NoError(t, err)
	result, err := client.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.Equal(t, "enrolled", result.GetCode())
	inputs := result.GetJob().GetInputs()
	require.Nil(t, inputs.GetInitialCredential())
	require.False(t, inputs.GetAllowActivation())
	for _, ref := range []*api.CameraCredentialReference{inputs.GetManagementCredential(), inputs.GetStreamingCredential(), imported.GetCredential()} {
		value := edgeCredentialResolve(t, f.ctx, f.admin, ref, nil)
		require.Equal(t, payload, value, "imported username/password bytes must survive the new activation-only username option")
		clear(value)
	}
	count, err := f.server.Ent.CameraCredential.Query().Where(cameracredential.PurposeEQ("initial_admin")).Count(f.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	rows, err := f.server.Ent.CameraCredential.Query().All(f.ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, pdid.MustParse(cameraPolicyID(t, f.site.GetId())).Uuid(), row.SiteId)
	}
	repeated, err := client.Observe(f.ctx, f.request)
	require.NoError(t, err)
	require.True(t, proto.Equal(inputs, repeated.GetJob().GetInputs()))
}
