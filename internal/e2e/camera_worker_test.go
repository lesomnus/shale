package e2e_test

import (
	"context"
	"crypto/x509"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func cameraWorkerMachine(t *testing.T, ctx context.Context, server *cmd.Server, cp string, admin *grpc.ClientConn, hardware string, site *api.SiteRef, tenants ...string) (*api.CameraWorker, *grpc.ClientConn, *hostagent.Agent) {
	t.Helper()
	x := require.New(t)
	tenant := "acme"
	if len(tenants) > 0 {
		tenant = tenants[0]
	}
	a := &hostagent.Agent{Kind: pdid.Domain(31), HardwareId: hardware, Store: pki.Store{Dir: t.TempDir()}, Cp: cp, CaHash: pki.Fingerprint(server.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	x.NoError(a.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewCameraWorkerServiceClient(conn).Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: tenant, Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := a.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { bootstrap.Close() })
	host, err := a.HostJoin()
	x.NoError(err)
	pending, err := join(ctx, bootstrap, host)
	x.NoError(err)
	w, err := api.NewCameraWorkerServiceClient(admin).Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: pending.GetId()}.Build(), Site: site}.Build())
	x.NoError(err)
	_, err = a.Join(ctx, join)
	x.NoError(err)
	conn, err := a.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { conn.Close() })
	return w, conn, a
}

func cameraWorkerProducer(t *testing.T, ctx context.Context, server *cmd.Server, cp string, admin *grpc.ClientConn, hardware string, site *api.SiteRef) (*api.Producer, *api.Set, *grpc.ClientConn) {
	t.Helper()
	x := require.New(t)
	tenant := "acme"
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: hardware, Site: site}.Build())
	x.NoError(err)
	a := &hostagent.Agent{Kind: pdid.Domain(22), HardwareId: hardware, Store: pki.Store{Dir: t.TempDir()}, Cp: cp, CaHash: pki.Fingerprint(server.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	x.NoError(a.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewProducerServiceClient(conn).Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := a.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { bootstrap.Close() })
	host, err := a.HostJoin()
	x.NoError(err)
	pending, err := join(ctx, bootstrap, host)
	x.NoError(err)
	p, err := api.NewProducerServiceClient(admin).Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	x.NoError(err)
	_, err = a.Join(ctx, join)
	x.NoError(err)
	conn, err := a.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { conn.Close() })
	return p, set, conn
}

// Real TLS/CSR enrollment, separate keys, exact certificate serial replacement,
// persisted machine identity and scoped RPC denial without a human session.
func TestCameraWorkerTLSIdentityAndRevocation(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	state := t.TempDir()
	const workerDomain pdid.Domain = 31
	agent := &hostagent.Agent{Kind: workerDomain, HardwareId: "tls-worker-machine", Store: pki.Store{Dir: state}, Cp: cp, CaHash: pki.Fingerprint(server.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	x.NoError(agent.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewCameraWorkerServiceClient(conn).Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: "acme", Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := agent.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { bootstrap.Close() })
	host, err := agent.HostJoin()
	x.NoError(err)
	pending, err := join(ctx, bootstrap, host)
	x.NoError(err)
	x.Equal(api.HostState_HOST_STATE_PENDING, pending.GetState())
	x.Empty(pending.GetCertificate(), "pending machines receive no usable credential")
	_, err = api.NewCameraWorkerServiceClient(bootstrap).Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.Equal(codes.Unauthenticated, status.Code(err))
	operator := api.NewCameraWorkerServiceClient(admin)
	ref := api.CameraWorkerRef_builder{Id: pending.GetId()}.Build()
	approved, err := operator.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: ref, Alias: "edge-worker"}.Build())
	x.NoError(err)
	x.Equal(api.HostState_HOST_STATE_ADOPTED, approved.GetState())
	_, err = agent.Join(ctx, join)
	x.NoError(err)
	x.Equal(workerDomain, agent.Id().Domain())
	leaf, err := agent.Leaf()
	x.NoError(err)
	x.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	x.NotContains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth, "outbound Workers cannot serve another host's TLS role")
	old, err := agent.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { old.Close() })
	worker := api.NewCameraWorkerServiceClient(old)
	_, err = worker.Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{CaHash: pki.Fingerprint(server.CA.Cert), Version: "worker-test"}.Build())
	x.NoError(err)
	view, err := worker.Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.NoError(err)
	x.Empty(view.GetAssignments())
	x.Empty(view.GetJobs(), "an adopted but unassigned machine sees no tenant-wide jobs")
	_, err = api.NewSourceServiceClient(old).List(ctx, api.SourceListRequest_builder{}.Build())
	x.Equal(codes.PermissionDenied, status.Code(err), "a Worker gets no generic Producer Source read capability")
	_, err = api.NewCameraNetworkServiceClient(old).List(ctx, api.CameraNetworkListRequest_builder{}.Build())
	x.Equal(codes.PermissionDenied, status.Code(err), "inventory is returned only through the assigned-work boundary")
	_, err = worker.Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: ref}.Build())
	x.Equal(codes.PermissionDenied, status.Code(err))
	otherKey, err := pki.NewHostKey()
	x.NoError(err)
	otherCSR, err := otherKey.CSR("another-worker-key")
	x.NoError(err)
	_, err = worker.RenewCertificate(ctx, api.CameraWorkerRenewCertificateRequest_builder{Csr: otherCSR}.Build())
	x.Contains([]codes.Code{codes.PermissionDenied, codes.InvalidArgument, codes.FailedPrecondition}, status.Code(err), "renewal cannot bypass approval of another private key")
	x.NoError(agent.Renew(ctx, old, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
		v, err := api.NewCameraWorkerServiceClient(conn).RenewCertificate(ctx, api.CameraWorkerRenewCertificateRequest_builder{Csr: csr}.Build())
		if err != nil {
			return nil, nil, err
		}
		return v.GetCertificate(), v.GetCaBundle(), nil
	}))
	_, err = worker.Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.Equal(codes.Unauthenticated, status.Code(err), "the already-open connection retains only its now-replaced serial")
	restarted := &hostagent.Agent{Kind: workerDomain, HardwareId: "tls-worker-machine", Store: pki.Store{Dir: state}, Cp: cp, Log: agent.Log}
	x.NoError(restarted.Init())
	x.Equal(agent.Id(), restarted.Id())
	current, err := restarted.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { current.Close() })
	_, err = api.NewCameraWorkerServiceClient(current).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.NoError(err, "restart uses its persisted machine key/certificate, without a person session")
	row, err := operator.Get(ctx, api.CameraWorkerGetRequest_builder{Ref: ref}.Build())
	x.NoError(err)
	_, err = operator.Revoke(ctx, api.CameraWorkerRevokeRequest_builder{Ref: ref, ExpectedVersion: row.GetDateUpdated()}.Build())
	x.NoError(err)
	_, err = api.NewCameraWorkerServiceClient(current).Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.Equal(codes.Unauthenticated, status.Code(err))
}

func TestCameraWorkerAssignedJobsAndReplacementFences(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	first, firstConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "scope-worker-first", nil)
	second, secondConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "scope-worker-second", nil)
	producer, _, producerConn := cameraWorkerProducer(t, ctx, server, cp, admin, "scope-producer", nil)
	tenant := "acme"
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: "scope-network", Cidrs: []string{"192.0.2.0/24"}}.Build())
	x.NoError(err)
	nref := api.CameraNetworkRef_builder{Id: network.GetId()}.Build()
	assignments := api.NewCameraAssignmentServiceClient(admin)
	assignment, err := assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: first.GetId()}.Build())
	x.NoError(err)
	aref := api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()
	device, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "scope-door"}.Build())
	x.NoError(err)
	bindings := api.NewCameraBindingServiceClient(admin)
	binding, err := bindings.Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "door", SourceAlias: "door"}.Build())
	x.NoError(err)
	bref := api.CameraBindingRef_builder{Id: binding.GetId()}.Build()
	jobs := api.NewCameraJobServiceClient(admin)
	job, err := jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: bref, RequestKey: "scope-intent", Inputs: api.CameraJobInputs_builder{Desired: api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build(), StreamingCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "door-stream", Version: "1", Purpose: "streaming"}.Build()}.Build()}.Build())
	x.NoError(err)
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	firstWorker, secondWorker := api.NewCameraWorkerServiceClient(firstConn), api.NewCameraWorkerServiceClient(secondConn)
	view, err := firstWorker.Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.NoError(err)
	x.Len(view.GetAssignments(), 1)
	x.Len(view.GetJobs(), 1)
	x.Equal(job.GetId(), view.GetJobs()[0].GetJob().GetId())
	other, err := secondWorker.Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.NoError(err)
	x.Empty(other.GetAssignments())
	x.Empty(other.GetJobs())
	_, err = api.NewCameraWorkerServiceClient(producerConn).Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.Equal(codes.PermissionDenied, status.Code(err), "a Producer certificate is not a Worker identity")
	claim := api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "claim-first", LeaseSeconds: 2}.Build()
	firstJobs := api.NewCameraJobServiceClient(firstConn)
	claimed, err := firstJobs.Claim(ctx, claim)
	x.NoError(err)
	replayed, err := firstJobs.Claim(ctx, claim)
	x.NoError(err)
	x.Equal(claimed.GetLeaseGeneration(), replayed.GetLeaseGeneration(), "a lost Claim response reuses the same execution grant")
	grant := api.CameraWorkerCheckJobRequest_builder{Ref: jref, LeaseGeneration: claimed.GetLeaseGeneration(), AssignmentGeneration: assignment.GetGeneration()}.Build()
	work, err := firstWorker.CheckJob(ctx, grant)
	x.NoError(err)
	x.Equal(binding.GetId(), work.GetBinding().GetId())
	_, err = secondWorker.CheckJob(ctx, grant)
	x.Error(err, "another approved machine cannot obtain this device grant")
	_, err = api.NewCameraJobServiceClient(secondConn).Claim(ctx, claim)
	x.Error(err)
	fresh, err := assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: aref}.Build())
	x.NoError(err)
	changed, err := assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Ref: aref, ExpectedVersion: fresh.GetDateUpdated(), Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: second.GetId()}.Build())
	x.NoError(err)
	x.Greater(changed.GetGeneration(), assignment.GetGeneration())
	_, err = firstWorker.CheckJob(ctx, grant)
	x.Error(err, "assignment replacement fences the old execution before any new device step")
	secondJobs := api.NewCameraJobServiceClient(secondConn)
	var resumed *api.CameraJob
	x.Eventually(func() bool {
		resumed, err = secondJobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: claimed.GetDateUpdated(), OperationKey: "claim-replacement", LeaseSeconds: 30}.Build())
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "the approved replacement resumes the same pinned intent after the old grant is fenced or expires")
	x.Equal(job.GetInputs(), resumed.GetInputs())
	x.Greater(resumed.GetLeaseGeneration(), claimed.GetLeaseGeneration())
	report := api.CameraJobReportRequest_builder{Ref: jref, ExpectedVersion: resumed.GetDateUpdated(), OperationKey: "report-replacement", LeaseGeneration: resumed.GetLeaseGeneration(), AssignmentGeneration: changed.GetGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING}.Build()
	reported, err := secondJobs.Report(ctx, report)
	x.NoError(err)
	reportedAgain, err := secondJobs.Report(ctx, report)
	x.NoError(err)
	x.Equal(reported.GetDateUpdated(), reportedAgain.GetDateUpdated(), "lost Report acknowledgement is idempotent")
	currentBinding, err := bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: bref}.Build())
	x.NoError(err)
	_, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: bref, ExpectedVersion: currentBinding.GetDateUpdated(), Collect: false}.Build())
	x.NoError(err)
	_, err = secondJobs.Report(ctx, report)
	x.Error(err, "a Worker cannot replay a canceled execution grant as current authority")
	final, err := jobs.Get(ctx, api.CameraJobGetRequest_builder{Ref: jref}.Build())
	x.NoError(err)
	x.Equal(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, final.GetStage())
}

func cameraWorkerJob(t *testing.T, ctx context.Context, server *cmd.Server, cp string, admin *grpc.ClientConn, worker *api.CameraWorker, stem string, site *api.SiteRef, pinned ...*api.CameraJobInputs) *api.CameraJob {
	t.Helper()
	x := require.New(t)
	producer, _, _ := cameraWorkerProducer(t, ctx, server, cp, admin, stem+"-producer", site)
	tenant := "acme"
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Site: site, Alias: stem + "-network", Cidrs: []string{"192.0.2.0/24"}}.Build())
	x.NoError(err)
	nref := api.CameraNetworkRef_builder{Id: network.GetId()}.Build()
	assignment, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: worker.GetId()}.Build())
	x.NoError(err)
	device, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: stem + "-camera"}.Build())
	x.NoError(err)
	binding, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: stem, SourceAlias: stem + "-camera"}.Build())
	x.NoError(err)
	inputs := api.CameraJobInputs_builder{Desired: api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build(), StreamingCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "stream", Version: "1", Purpose: "streaming"}.Build()}.Build()
	if len(pinned) > 0 {
		inputs = pinned[0]
	}
	job, err := api.NewCameraJobServiceClient(admin).Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), RequestKey: stem + "-intent", Inputs: inputs}.Build())
	x.NoError(err)
	return job
}

func TestCameraWorkerRuntimeCancelsARealAssignedDeviceStep(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	row, enrolledConn, enrolled := cameraWorkerMachine(t, ctx, server, cp, admin, "runtime-worker", nil)
	job := cameraWorkerJob(t, ctx, server, cp, admin, row, "runtime", nil)
	// CP committed renewal, but the Worker did not persist the response. The
	// otherwise time-valid local serial must recover through approved-key Join.
	host, err := enrolled.HostJoin()
	x.NoError(err)
	_, err = api.NewCameraWorkerServiceClient(enrolledConn).RenewCertificate(ctx, api.CameraWorkerRenewCertificateRequest_builder{Csr: host.GetCsr()}.Build())
	x.NoError(err)
	started := make(chan *api.CameraWorkerWork, 1)
	stopped := make(chan error, 1)
	escape := make(chan error, 1)
	var writes atomic.Int32
	runtime, err := cameraworker.New(cameraworker.Config{
		StateDir: enrolled.Store.Dir, Cp: cp, Tenant: "acme", HardwareID: "runtime-worker", CaHash: pki.Fingerprint(server.CA.Cert),
		HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 3, Log: enrolled.Log,
		Executor: cameraworker.ExecutorFunc(func(ctx context.Context, e *cameraworker.Execution) error {
			err := e.Step(ctx, func(step context.Context, work *api.CameraWorkerWork) error {
				started <- work
				<-step.Done()
				return step.Err()
			})
			stopped <- err
			escape <- e.Step(context.Background(), func(context.Context, *api.CameraWorkerWork) error { writes.Add(1); return nil })
			return ctx.Err()
		}),
	})
	x.NoError(err)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("outbound Worker did not stop")
		}
	})
	select {
	case grant := <-started:
		x.Equal(job.GetId(), grant.GetJob().GetId())
		x.Equal(row.GetId(), grant.GetJob().GetLeaseOwner())
		x.Positive(grant.GetJob().GetLeaseGeneration())
	case <-ctx.Done():
		t.Fatal("assigned executor never obtained a real CP grant")
	}
	operators := api.NewCameraWorkerServiceClient(admin)
	ref := api.CameraWorkerRef_builder{Id: row.GetId()}.Build()
	x.Eventually(func() bool {
		fresh, getErr := operators.Get(ctx, api.CameraWorkerGetRequest_builder{Ref: ref}.Build())
		if getErr != nil {
			return false
		}
		_, revokeErr := operators.Revoke(ctx, api.CameraWorkerRevokeRequest_builder{Ref: ref, ExpectedVersion: fresh.GetDateUpdated()}.Build())
		return revokeErr == nil
	}, 3*time.Second, 20*time.Millisecond, "revoke uses CAS alongside the actual heartbeat")
	select {
	case err := <-stopped:
		x.Error(err)
	case <-time.After(3 * time.Second):
		t.Fatal("revoked runtime did not cancel the in-flight cooperative device operation")
	}
	select {
	case err := <-escape:
		x.Error(err, "a fresh caller context cannot escape the revoked execution")
	case <-time.After(3 * time.Second):
		t.Fatal("revoked execution attempted to retain an independent context")
	}
	x.Zero(writes.Load())
}

func TestCameraWorkerExpiredApprovedKeyRecoversThroughPublicJoinOnly(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	row, open, original := cameraWorkerMachine(t, ctx, server, cp, admin, "expiry-worker", nil)
	ref := api.CameraWorkerRef_builder{Id: row.GetId()}.Build()
	_, err := api.NewCameraWorkerServiceClient(open).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.NoError(err)
	// The CP expiry is checked on already-open RPC transports as well.
	_, err = server.Ungated.CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{Ref: ref, DateCertExpires: timestamppb.New(time.Now().Add(-time.Minute)), DateUpdatedForce: boolPtr(true)}.Build())
	x.NoError(err)
	_, err = api.NewCameraWorkerServiceClient(open).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.Error(err)
	host, err := original.HostJoin()
	x.NoError(err)
	expired, err := server.CA.IssueClientHost(host.GetCsr(), original.Id(), time.Now().Add(-pki.HostLifetime-time.Hour))
	x.NoError(err)
	expiredPEM := pki.EncodeCerts(expired)
	serial := pki.Serial(expired)
	_, err = server.Ungated.CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{Ref: ref, Certificate: expiredPEM, CertSerial: &serial, DateCertExpires: timestamppb.New(expired.NotAfter), DateCertIssued: timestamppb.New(expired.NotBefore), DateUpdatedForce: boolPtr(true)}.Build())
	x.NoError(err)
	x.NoError(original.Store.SetCert(expiredPEM, pki.EncodeCerts(server.CA.Cert)))
	restarted := &hostagent.Agent{Kind: pdid.Domain(31), HardwareId: "expiry-worker", Store: original.Store, Cp: cp, Log: original.Log}
	x.NoError(restarted.Init())
	x.False(restarted.Ready(time.Now()))
	bad, err := restarted.Dial(ctx)
	x.NoError(err)
	_, err = api.NewCameraWorkerServiceClient(bad).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.Error(err, "ordinary machine RPCs cannot omit an expired certificate to gain authority")
	bad.Close()
	// An unrelated pending CSR must neither receive the approved certificate nor
	// prevent the approved private key from recovering its own expired identity.
	untrusted := &hostagent.Agent{Kind: pdid.Domain(31), HardwareId: "expiry-worker", Store: pki.Store{Dir: t.TempDir()}, Cp: cp, Log: original.Log}
	x.NoError(untrusted.Init())
	bootstrap, err := untrusted.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { bootstrap.Close() })
	newHost, err := untrusted.HostJoin()
	x.NoError(err)
	pending, err := api.NewCameraWorkerServiceClient(bootstrap).Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: "acme", Host: newHost}.Build())
	x.NoError(err)
	x.Equal(api.HostState_HOST_STATE_PENDING, pending.GetAnswer().GetState())
	x.Empty(pending.GetAnswer().GetCertificate())
	_, err = restarted.JoinBootstrap(ctx, func(ctx context.Context, conn *grpc.ClientConn, request *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewCameraWorkerServiceClient(conn).Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: "acme", Host: request}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	})
	x.NoError(err)
	x.True(restarted.Ready(time.Now()))
	x.Equal(original.Id(), restarted.Id())
	current, err := restarted.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { current.Close() })
	_, err = api.NewCameraWorkerServiceClient(current).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.NoError(err)
}

func boolPtr(v bool) *bool { return &v }

func TestCameraWorkerTenantAndSiteWalls(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server, cp, admin := cameraTLSControl(t)
	tenant := "acme"
	sites := api.NewSiteServiceClient(admin)
	west, err := sites.Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: "west"}.Build())
	x.NoError(err)
	east, err := sites.Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: "east"}.Build())
	x.NoError(err)
	wref, eref := api.SiteRef_builder{Id: west.GetId()}.Build(), api.SiteRef_builder{Id: east.GetId()}.Build()
	westWorker, westConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "west-worker", wref)
	eastWorker, eastConn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "east-worker", eref)
	unsited, _, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "unsited-worker", nil)
	westJob := cameraWorkerJob(t, ctx, server, cp, admin, westWorker, "west", wref)
	eastJob := cameraWorkerJob(t, ctx, server, cp, admin, eastWorker, "east", eref)
	for _, peer := range []struct {
		conn         *grpc.ClientConn
		own, foreign *api.CameraJob
	}{{westConn, westJob, eastJob}, {eastConn, eastJob, westJob}} {
		view, pollErr := api.NewCameraWorkerServiceClient(peer.conn).Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
		x.NoError(pollErr)
		x.Len(view.GetAssignments(), 1)
		x.Len(view.GetJobs(), 1)
		x.Equal(peer.own.GetId(), view.GetJobs()[0].GetJob().GetId())
		_, claimErr := api.NewCameraJobServiceClient(peer.conn).Claim(ctx, api.CameraJobClaimRequest_builder{Ref: api.CameraJobRef_builder{Id: peer.foreign.GetId()}.Build(), ExpectedVersion: peer.foreign.GetDateUpdated(), OperationKey: "foreign-site", LeaseSeconds: 30}.Build())
		x.Error(claimErr, "a known foreign-site job ID cannot be used as execution authority")
	}
	assignments := api.NewCameraAssignmentServiceClient(admin)
	assignment, err := assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: eastJob.GetAssignment().GetId()}.Build()}.Build())
	x.NoError(err)
	for _, id := range [][]byte{westWorker.GetId(), unsited.GetId()} {
		_, err = assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), ExpectedVersion: assignment.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: assignment.GetNetwork().GetId()}.Build(), Producer: api.ProducerRef_builder{Id: assignment.GetProducer().GetId()}.Build(), WorkerSlot: "edge", WorkerId: id}.Build())
		x.Error(err, "wrong-site or site-less machines cannot be assigned across the exact site boundary")
	}
	_, err = assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), ExpectedVersion: assignment.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: assignment.GetNetwork().GetId()}.Build(), Producer: api.ProducerRef_builder{Id: assignment.GetProducer().GetId()}.Build(), WorkerSlot: "edge", WorkerId: assignment.GetProducer().GetId()}.Build())
	x.Equal(codes.InvalidArgument, status.Code(err), "an existing Producer ID is still not a Worker ID")
	boss, _, err := server.Identity.Seed(ctx, "globex", "boss")
	x.NoError(err)
	_, err = server.Identity.Grant(ctx, "globex", identity.OperatorRole, []string{identity.Everything}, boss.Id)
	x.NoError(err)
	_, err = server.ProvisionTenant(ctx, "globex")
	x.NoError(err)
	foreignAdmin := cameraTLSPrincipal(t, server, cp, "@globex/boss")
	foreign, foreignConn, _ := cameraWorkerMachine(t, ctx, server, cp, foreignAdmin, "foreign-worker", nil, "globex")
	view, err := api.NewCameraWorkerServiceClient(foreignConn).Poll(ctx, api.CameraWorkerPollRequest_builder{}.Build())
	x.NoError(err)
	x.Empty(view.GetAssignments())
	x.Empty(view.GetJobs())
	_, err = api.NewCameraJobServiceClient(foreignConn).Claim(ctx, api.CameraJobClaimRequest_builder{Ref: api.CameraJobRef_builder{Id: westJob.GetId()}.Build(), ExpectedVersion: westJob.GetDateUpdated(), OperationKey: "foreign-tenant", LeaseSeconds: 30}.Build())
	x.Error(err)
	_, err = assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), ExpectedVersion: assignment.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: assignment.GetNetwork().GetId()}.Build(), Producer: api.ProducerRef_builder{Id: assignment.GetProducer().GetId()}.Build(), WorkerSlot: "edge", WorkerId: foreign.GetId()}.Build())
	x.Error(err, "even a known adopted foreign-tenant Worker cannot be bound to this network")
}
