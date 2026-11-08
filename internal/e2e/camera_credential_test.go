package e2e_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	cpauth "github.com/lesomnus/shale/camera/credential/cp"
	"github.com/lesomnus/shale/camera/credential/material"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func credentialPayload(t *testing.T, username, password string) []byte {
	t.Helper()
	m, err := material.New(username, password)
	require.NoError(t, err)
	b, err := m.MarshalBinary()
	require.NoError(t, err)
	return b
}

func credentialID(t *testing.T, raw []byte) string {
	t.Helper()
	id, err := pdid.From(raw)
	require.NoError(t, err)
	return id.String()
}

func TestCameraCredentialDurabilityAndOrdinaryRedaction(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	credentials := api.NewCameraCredentialServiceClient(admin)
	n := cameraNetwork(t, admin, "credential-lan", nil)
	d, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), DeviceKey: "credential-device"}.Build())
	require.NoError(t, err)
	dref := api.CameraDeviceRef_builder{Id: d.GetId()}.Build()
	prepare := api.CameraCredentialPrepareRequest_builder{Device: dref, Purpose: "initial_admin", IntentKey: "durable-initial", Username: []byte("initial-admin")}.Build()
	first, err := credentials.Prepare(ctx, prepare)
	require.NoError(t, err)
	// Discarding and replaying the response models an uncertain successful RPC.
	again, err := credentials.Prepare(ctx, proto.Clone(prepare).(*api.CameraCredentialPrepareRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(first, again), "an uncertain Prepare response cannot rotate the installed secret/ref")
	changedUsername := proto.Clone(prepare).(*api.CameraCredentialPrepareRequest)
	changedUsername.SetUsername([]byte("different-initial-admin"))
	_, err = credentials.Prepare(ctx, changedUsername)
	require.Equal(t, codes.Aborted, status.Code(err))
	lease, err := credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: first.GetCredential()}.Build())
	require.NoError(t, err)
	original := bytes.Clone(lease.GetMaterial())
	clear(lease.GetMaterial())
	newIntent := proto.Clone(prepare).(*api.CameraCredentialPrepareRequest)
	newIntent.SetIntentKey("second-binding-intent")
	newIntent.SetUsername([]byte("new-account-only-option"))
	copied, err := credentials.Prepare(ctx, newIntent)
	require.NoError(t, err)
	require.NotEqual(t, first.GetCredential().GetVersion(), copied.GetCredential().GetVersion())
	require.Equal(t, api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_REUSED, copied.GetOrigin())
	lease, err = credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: copied.GetCredential()}.Build())
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, lease.GetMaterial()), "new intent metadata never regenerates an existing account")
	clear(lease.GetMaterial())
	const password = "fixture-central-import-never-on-ordinary-wire"
	payload := credentialPayload(t, "imported-user\xff", password+"\xfe\x00")
	importedRequest := api.CameraCredentialImportRequest_builder{Device: dref, Purpose: "initial_admin", IntentKey: "manual-import", Access: "administrator", Material: payload, ExpectedCurrent: copied.GetCredential()}.Build()
	imported, err := credentials.Import(ctx, importedRequest)
	require.NoError(t, err)
	replayed, err := credentials.Import(ctx, importedRequest)
	require.NoError(t, err, "lost Import response replays before its now-stale ExpectedCurrent is checked")
	require.True(t, proto.Equal(imported, replayed))
	conflict := proto.Clone(importedRequest).(*api.CameraCredentialImportRequest)
	conflict.SetMaterial(credentialPayload(t, "imported-user\xff", "different-fixture-password"))
	_, err = credentials.Import(ctx, conflict)
	require.Equal(t, codes.Aborted, status.Code(err))
	conflict = proto.Clone(importedRequest).(*api.CameraCredentialImportRequest)
	conflict.SetIntentKey("new-intent-stale-current")
	_, err = credentials.Import(ctx, conflict)
	require.Equal(t, codes.Aborted, status.Code(err))
	versions, err := api.NewCameraCredentialVersionServiceClient(admin).List(ctx, api.CameraCredentialVersionListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, versions.GetItems(), 3)
	for _, v := range versions.GetItems() {
		require.Empty(t, v.GetSealedEnvelope())
		require.Empty(t, v.GetImportTag())
		require.Empty(t, v.GetPrepareTag())
		require.Empty(t, v.GetRequestHash())
	}
	canaries := [][]byte{[]byte(password), []byte(base64.StdEncoding.EncodeToString([]byte(password))), []byte(hex.EncodeToString([]byte(password)))}
	privateRows, err := c.running.CP.Ent.CameraCredentialVersion.Query().All(ctx)
	require.NoError(t, err)
	for _, row := range privateRows {
		require.NotEmpty(t, row.SealedEnvelope)
		for _, canary := range canaries {
			require.False(t, bytes.Contains(row.SealedEnvelope, canary), "default backend stored unprotected material")
		}
	}
	audit, err := c.running.CP.Ent.Audit.Query().All(ctx)
	require.NoError(t, err)
	for _, row := range audit {
		for _, canary := range canaries {
			require.False(t, bytes.Contains(row.Value, canary) || bytes.Contains(row.Patch, canary), "ordinary audit contains credential transfer data")
		}
	}
	_, err = c.running.CP.Identity.AddPerson(ctx, "acme", "credential-viewer", "Viewer")
	require.NoError(t, err)
	_, err = api.NewCameraCredentialServiceClient(c.dial("@acme/credential-viewer")).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: first.GetCredential()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	boss, _, err := c.running.CP.Identity.Seed(ctx, "globex", "boss")
	require.NoError(t, err)
	_, err = c.running.CP.Identity.Grant(ctx, "globex", identity.OperatorRole, []string{identity.Everything}, boss.Id)
	require.NoError(t, err)
	_, err = api.NewCameraCredentialServiceClient(c.dial("@globex/boss")).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: first.GetCredential()}.Build())
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err))
	// Same DB + KEK, reconstructed CP; no Worker memory is needed for recovery.
	c.cfg.Server.Addr, c.cfg.Cluster.Addr = c.running.TenantAddr, c.running.ClusterAddr
	c.stopCP()
	c.startCP()
	require.Eventually(t, func() bool {
		lease, err = credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: imported.GetCredential()}.Build())
		return err == nil
	}, 15*time.Second, 50*time.Millisecond)
	require.True(t, bytes.Equal(payload, lease.GetMaterial()), "imported binary bytes changed during restart")
	clear(lease.GetMaterial())
	lease, err = credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: first.GetCredential()}.Build())
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, lease.GetMaterial()), "old immutable pins must not substitute the latest password")
	clear(lease.GetMaterial())
	_, err = credentials.Revoke(ctx, api.CameraCredentialRevokeRequest_builder{Credential: first.GetCredential()}.Build())
	require.NoError(t, err)
	_, err = credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: first.GetCredential()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	lease, err = credentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: imported.GetCredential()}.Build())
	require.NoError(t, err)
	clear(lease.GetMaterial())
}

// This exercises authority and real machine certificates. Media evidence below
// is an operator/Worker checkpoint fixture; real independent decode is #202.
func TestCameraCredentialWorkerProducerAndPurposeGrants(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-site", true: "site"}[scoped], func(t *testing.T) { cameraCredentialMachineGrants(t, scoped) })
	}
}

func cameraCredentialMachineGrants(t *testing.T, scoped bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	var site, otherSite *api.SiteRef
	if scoped {
		tenant := "acme"
		for _, target := range []struct {
			alias string
			ref   **api.SiteRef
		}{{"credential-west", &site}, {"credential-east", &otherSite}} {
			v, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: target.alias}.Build())
			require.NoError(t, err)
			*target.ref = api.SiteRef_builder{Id: v.GetId()}.Build()
		}
	}
	worker, workerConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "credential-worker", site)
	_, otherConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "credential-other-worker", otherSite)
	producer, _, producerConn := cameraWorkerProducer(t, ctx, server, cp, admin, "credential-producer", site)
	n := cameraNetwork(t, admin, "credential-scope-lan", site)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	d, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "tls-credential-camera"}.Build())
	require.NoError(t, err)
	dref := api.CameraDeviceRef_builder{Id: d.GetId()}.Build()
	b, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: dref, LogicalKey: "credential-door", SourceAlias: "credential-door"}.Build())
	require.NoError(t, err)
	bref := api.CameraBindingRef_builder{Id: b.GetId()}.Build()
	credentials := api.NewCameraCredentialServiceClient(admin)
	initial, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: dref, Purpose: "initial_admin", IntentKey: "initial-account"}.Build())
	require.NoError(t, err)
	management, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: dref, Purpose: "management", IntentKey: "management-account", Reuse: initial.GetCredential()}.Build())
	require.NoError(t, err)
	stream, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: dref, Binding: bref, Purpose: "streaming", IntentKey: "stream-account", Reuse: management.GetCredential()}.Build())
	require.NoError(t, err)
	// Another device in this same network/site is not authorized by the first
	// device's job or Source, even for this already approved machine identity.
	otherDevice, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "tls-credential-other-camera"}.Build())
	if err != nil {
		t.Fatal("second scoped credential device could not be prepared")
	}
	otherDeviceRef := api.CameraDeviceRef_builder{Id: otherDevice.GetId()}.Build()
	otherInitial, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: otherDeviceRef, Purpose: "initial_admin", IntentKey: "other-initial-account"}.Build())
	if err != nil {
		t.Fatal("second device initial reference could not be prepared")
	}
	otherManagement, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: otherDeviceRef, Purpose: "management", IntentKey: "other-management-account", Reuse: otherInitial.GetCredential()}.Build())
	if err != nil {
		t.Fatal("second device management reference could not be prepared")
	}
	otherStream, err := credentials.Prepare(ctx, api.CameraCredentialPrepareRequest_builder{Device: otherDeviceRef, Purpose: "streaming", IntentKey: "other-stream-account", Reuse: otherManagement.GetCredential()}.Build())
	if err != nil {
		t.Fatal("second device streaming reference could not be prepared")
	}
	denyResolve := func(client api.CameraCredentialServiceClient, request *api.CameraCredentialResolveRequest) {
		t.Helper()
		response, resolveErr := client.Resolve(ctx, request)
		hasMaterial := len(response.GetMaterial()) != 0
		clear(response.GetMaterial())
		require.Equal(t, codes.PermissionDenied, status.Code(resolveErr), "the exact current device/version grant must reject a substituted credential")
		require.False(t, hasMaterial, "a rejected credential resolve returned material")
	}
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build()
	jobs := api.NewCameraJobServiceClient(workerConn)
	job, err := api.NewCameraJobServiceClient(admin).Create(ctx, api.CameraJobCreateRequest_builder{Binding: bref, RequestKey: "credential-job", Inputs: api.CameraJobInputs_builder{Desired: profile, AllowActivation: true, InitialCredential: initial.GetCredential(), ManagementCredential: management.GetCredential(), StreamingCredential: stream.GetCredential()}.Build()}.Build())
	require.NoError(t, err)
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	job, err = jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "credential-claim", LeaseSeconds: 60}.Build())
	require.NoError(t, err)
	grant := api.CameraWorkerCheckJobRequest_builder{Ref: jref, LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration()}.Build()
	workerCredentials := api.NewCameraCredentialServiceClient(workerConn)
	for _, ref := range []*api.CameraCredentialReference{initial.GetCredential(), management.GetCredential(), stream.GetCredential()} {
		v, err := workerCredentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: ref, Job: grant}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, v.GetMaterial())
		clear(v.GetMaterial())
	}
	for _, foreign := range []*api.CameraCredentialReference{otherManagement.GetCredential(), otherStream.GetCredential()} {
		denyResolve(workerCredentials, api.CameraCredentialResolveRequest_builder{Credential: foreign, Job: grant}.Build())
	}
	wrongVersion := proto.Clone(stream.GetCredential()).(*api.CameraCredentialReference)
	wrongVersion.SetVersion("ungranted-version")
	denyResolve(workerCredentials, api.CameraCredentialResolveRequest_builder{Credential: wrongVersion, Job: grant}.Build())
	_, err = workerCredentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential(), Job: grant, CacheSeconds: 30, SourceRevision: 1}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "Workers cannot ask for an offline streaming cache grant")
	_, err = api.NewCameraCredentialServiceClient(otherConn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential(), Job: grant}.Build())
	require.Error(t, err)
	wrongPurpose := proto.Clone(stream.GetCredential()).(*api.CameraCredentialReference)
	wrongPurpose.SetPurpose("management")
	_, err = workerCredentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: wrongPurpose, Job: grant}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewCameraCredentialServiceClient(producerConn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: management.GetCredential()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewCameraCredentialServiceClient(producerConn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "unpublished sources do not grant Producer credentials")
	_, err = workerCredentials.Get(ctx, api.CameraCredentialGetRequest_builder{Ref: api.CameraCredentialRef_builder{Id: pdid.MustParse(stream.GetCredential().GetId()).Bytes()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	observed := api.CameraJobObservation_builder{Effective: profile, Verified: api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: 60, ObservedAt: timestamppb.Now()}.Build()}.Build()
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "credential-applying", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING}.Build())
	require.NoError(t, err)
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "credential-verifying", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, Observed: observed}.Build())
	require.NoError(t, err)
	pub, err := jobs.Publish(ctx, api.CameraJobPublishRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "credential-publish", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Spec: api.CameraCaptureSpec_builder{BindingId: b.GetId(), SourceId: b.GetSource().GetId(), Alias: "credential-door", Input: "rtsp://192.0.2.10/stream", Profile: profile, Credential: stream.GetCredential()}.Build(), Observed: observed}.Build())
	require.NoError(t, err)
	scope := cameramodel.EnrollmentScope{TenantID: credentialID(t, d.GetTenant().GetId()), SetID: credentialID(t, a.GetSet().GetId()), ProducerID: credentialID(t, producer.GetId()), NetworkID: credentialID(t, n.GetId())}
	if site != nil {
		scope.SiteID = credentialID(t, site.GetId())
	}
	provider, err := cpauth.New(producerConn, cpauth.Options{Role: cpauth.Producer, Scope: scope, SourceID: credentialID(t, b.GetSource().GetId()), SourceRevision: pub.GetBinding().GetDesired().GetRevision()})
	require.NoError(t, err)
	ref := cameramodel.CredentialRef{Provider: "cp", ID: stream.GetCredential().GetId(), Version: stream.GetCredential().GetVersion(), Purpose: cameramodel.AuthStreaming, Scope: scope, Recoverable: true}
	consumerLease, err := cameramodel.Acquire(ctx, provider, cameramodel.AuthRequest{Scope: scope, Purpose: cameramodel.AuthStreaming, ExistingRef: &ref, Requirement: cameramodel.AuthRequirement{Access: cameramodel.AccessRead, Recoverable: true}})
	require.NoError(t, err)
	require.NoError(t, consumerLease.Close())
	resolveStream := api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential(), Source: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build(), SourceRevision: pub.GetBinding().GetDesired().GetRevision(), CacheSeconds: 300}.Build()
	lease, err := api.NewCameraCredentialServiceClient(producerConn).Resolve(ctx, resolveStream)
	require.NoError(t, err)
	require.NotNil(t, lease.GetCacheUntil())
	require.LessOrEqual(t, time.Until(lease.GetCacheUntil().AsTime()), 300*time.Second)
	exactMaterial := bytes.Clone(lease.GetMaterial())
	defer clear(exactMaterial)
	clear(lease.GetMaterial())
	producerCredentials := api.NewCameraCredentialServiceClient(producerConn)
	for _, substituted := range []*api.CameraCredentialReference{otherStream.GetCredential(), wrongVersion} {
		request := proto.Clone(resolveStream).(*api.CameraCredentialResolveRequest)
		request.SetCredential(substituted)
		denyResolve(producerCredentials, request)
	}
	// A separately approved Worker recovers the exact pinned secret after
	// assignment replacement; the old machine cannot reuse its old job grant.
	replacementWorker, replacementWorkerConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "credential-replacement-worker", site)
	assignments := api.NewCameraAssignmentServiceClient(admin)
	aref := api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()
	a, err = assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	a, err = assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Ref: aref, ExpectedVersion: a.GetDateUpdated(), Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "replacement", WorkerId: replacementWorker.GetId()}.Build())
	require.NoError(t, err)
	_, err = workerCredentials.Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential(), Job: grant}.Build())
	require.Error(t, err)
	job, err = api.NewCameraJobServiceClient(admin).Get(ctx, api.CameraJobGetRequest_builder{Ref: jref}.Build())
	require.NoError(t, err)
	job, err = api.NewCameraJobServiceClient(replacementWorkerConn).Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "replacement-recover", LeaseSeconds: 60}.Build())
	require.NoError(t, err)
	recovered, err := api.NewCameraCredentialServiceClient(replacementWorkerConn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential(), Job: api.CameraWorkerCheckJobRequest_builder{Ref: jref, LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, exactMaterial, recovered.GetMaterial())
	clear(recovered.GetMaterial())
	// Producer handover uses a NEW private key and preserves the Set, Source
	// and credential version. No old key/credential file is copied.
	agent := &hostagent.Agent{Kind: 22, HardwareId: "credential-replacement-producer", Store: pki.Store{Dir: t.TempDir()}, Cp: cp, CaHash: pki.Fingerprint(server.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.NoError(t, agent.Init())
	bootstrap, err := agent.Dial(ctx)
	require.NoError(t, err)
	defer bootstrap.Close()
	join := func(ctx context.Context, conn *grpc.ClientConn, h *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewProducerServiceClient(conn).Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: h}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	host, err := agent.HostJoin()
	require.NoError(t, err)
	pending, err := join(ctx, bootstrap, host)
	require.NoError(t, err)
	a, err = assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	_, err = assignments.Handover(ctx, api.CameraAssignmentHandoverRequest_builder{Ref: aref, ExpectedVersion: a.GetDateUpdated(), ExpectedGeneration: a.GetGeneration(), Replacement: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = agent.Join(ctx, join)
	require.NoError(t, err)
	newProducerConn, err := agent.Dial(ctx)
	require.NoError(t, err)
	defer newProducerConn.Close()
	recovered, err = api.NewCameraCredentialServiceClient(newProducerConn).Resolve(ctx, resolveStream)
	require.NoError(t, err)
	require.Equal(t, exactMaterial, recovered.GetMaterial())
	clear(recovered.GetMaterial())
	_, err = api.NewCameraCredentialServiceClient(producerConn).Resolve(ctx, resolveStream)
	require.Error(t, err)
	_, err = credentials.Revoke(ctx, api.CameraCredentialRevokeRequest_builder{Credential: stream.GetCredential()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraCredentialServiceClient(newProducerConn).Resolve(ctx, resolveStream)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "revocation also rejects the separately approved replacement")
	_, err = api.NewCameraCredentialServiceClient(producerConn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: stream.GetCredential()}.Build())
	require.Contains(t, []codes.Code{codes.PermissionDenied, codes.Unauthenticated}, status.Code(err))
	audit, err := server.Ent.Audit.Query().All(ctx)
	require.NoError(t, err)
	found := false
	for _, record := range audit {
		if bytes.Equal(record.ActorId[:], worker.GetId()) && record.Action == api.CameraCredentialService_Resolve_FullMethodName {
			found = true
		}
	}
	require.True(t, found, "machine secret access needs an explicit metadata-only audit beyond TrailOfPeople")
}
