package e2e_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	catalog "github.com/lesomnus/shale/source"
	catalogcp "github.com/lesomnus/shale/source/cp"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sourceCatalogID(t *testing.T, raw []byte) string {
	t.Helper()
	id, err := pdid.From(raw)
	require.NoError(t, err)
	return id.String()
}

func sourceCatalogSet(t *testing.T, admin *grpc.ClientConn, alias string) *api.Set {
	t.Helper()
	v, err := api.NewSetServiceClient(admin).Add(t.Context(), api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias,
	}.Build())
	require.NoError(t, err)
	return v
}

func sourceCatalogAdopt(t *testing.T, c *cluster, admin *grpc.ClientConn, set *api.Set, name string) *api.Producer {
	t.Helper()
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR(name)
	require.NoError(t, err)
	joined, err := api.NewProducerServiceClient(plainConn(t, c.running.TenantAddr)).Join(t.Context(), api.ProducerJoinRequest_builder{
		Tenant: "acme", Host: api.HostJoin_builder{HardwareId: "source-catalog-" + name, Hostname: name, Csr: csr, KeyFingerprint: key.Fingerprint()}.Build(),
	}.Build())
	require.NoError(t, err)
	v, err := api.NewProducerServiceClient(admin).Adopt(t.Context(), api.ProducerAdoptRequest_builder{
		Ref: api.ProducerRef_builder{Id: joined.GetAnswer().GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	return v
}

func sourceCatalogAssignment(t *testing.T, admin *grpc.ClientConn, network *api.CameraNetwork, host *api.Producer) *api.CameraAssignment {
	t.Helper()
	v, err := api.NewCameraAssignmentServiceClient(admin).Assign(t.Context(), api.CameraAssignmentAssignRequest_builder{
		Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "catalog-fixture",
	}.Build())
	require.NoError(t, err)
	return v
}

type sourceCatalogCommit func(context.Context, provision.Deployment) (publication.Receipt, error)

func (f sourceCatalogCommit) Commit(ctx context.Context, d provision.Deployment) (publication.Receipt, error) {
	return f(ctx, d)
}

type sourceCatalogPrepared struct {
	request    *api.CameraJobPublishRequest
	deployment provision.Deployment
}

// This helper supplies authorized operator evidence for the CP authority path.
// It is deliberately not a substitute for #202's independent camera/live decode.
func sourceCatalogPrepare(t *testing.T, admin *grpc.ClientConn, assignment *api.CameraAssignment, alias, endpoint string) sourceCatalogPrepared {
	t.Helper()
	ctx := t.Context()
	devices := api.NewCameraDeviceServiceClient(admin)
	device, err := devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{
		Network: api.CameraNetworkRef_builder{Id: assignment.GetNetwork().GetId()}.Build(), DeviceKey: alias + "-physical",
	}.Build())
	require.NoError(t, err)
	device, err = devices.Observe(ctx, api.CameraDeviceObserveRequest_builder{
		Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), ExpectedVersion: device.GetDateUpdated(),
		Identity:  api.CameraIdentityEvidence_builder{Authenticated: true, Manufacturer: "synthetic", Serial: alias}.Build(),
		Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: endpoint, Provenance: "authenticated", ObservedAt: timestamppb.Now()}.Build()},
	}.Build())
	require.NoError(t, err)
	binding, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{
		Assignment: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(),
		LogicalKey: alias + "-logical", SourceAlias: alias,
	}.Build())
	require.NoError(t, err)
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 160, Height: 90, Fps: 10, MaxBitrateBps: 1_000_000}.Build()
	jobs := api.NewCameraJobServiceClient(admin)
	job, err := jobs.Create(ctx, api.CameraJobCreateRequest_builder{
		Binding: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), RequestKey: alias + "-desired",
		Inputs: api.CameraJobInputs_builder{Desired: profile}.Build(),
	}.Build())
	require.NoError(t, err)
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	job, err = jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: alias + "-claim", LeaseSeconds: 300}.Build())
	require.NoError(t, err)
	observed := api.CameraJobObservation_builder{Effective: profile,
		Verified: api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: 20, ObservedAt: timestamppb.Now()}.Build(),
	}.Build()
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{
		Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: alias + "-verified", Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING,
		LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Observed: observed,
	}.Build())
	require.NoError(t, err)
	operation := alias + "-publish"
	return sourceCatalogPrepared{
		request: api.CameraJobPublishRequest_builder{
			Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: operation, LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(),
			Spec: api.CameraCaptureSpec_builder{BindingId: binding.GetId(), SourceId: binding.GetSource().GetId(), Alias: alias, Input: endpoint, Profile: profile}.Build(), Observed: observed,
		}.Build(),
		deployment: provision.Deployment{OperationID: operation, Request: provision.Request{
			ID: alias + "-desired", BindingID: binding.GetLogicalKey(), Alias: alias,
			Scope: cameramodel.EnrollmentScope{TenantID: sourceCatalogID(t, assignment.GetTenant().GetId()), SetID: sourceCatalogID(t, assignment.GetSet().GetId()), ProducerID: sourceCatalogID(t, assignment.GetProducer().GetId()), NetworkID: sourceCatalogID(t, assignment.GetNetwork().GetId())},
		}},
	}
}

func sourceCatalogPublish(t *testing.T, admin *grpc.ClientConn, assignment *api.CameraAssignment, prepared sourceCatalogPrepared) (*api.CameraJobPublishResponse, publication.Receipt) {
	t.Helper()
	v, err := api.NewCameraJobServiceClient(admin).Publish(t.Context(), prepared.request)
	require.NoError(t, err)
	b := v.GetBinding()
	return v, publication.Receipt{
		Revision: catalog.Revision{AssignmentID: sourceCatalogID(t, assignment.GetId()), Generation: assignment.GetGeneration(), Catalog: v.GetCatalogRevision()},
		Binding:  provision.BindingState{BindingID: b.GetLogicalKey(), Alias: b.GetSourceAlias(), SourceID: sourceCatalogID(t, b.GetSource().GetId()), DeploymentID: prepared.deployment.OperationID, DeployedAt: v.GetJob().GetDatePublished().AsTime()},
	}
}

func TestSourceCatalogAuthorityAndRecovery(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "catalog")
	host := sourceCatalogAdopt(t, c, admin, set, "catalog-owner")
	network := cameraNetwork(t, admin, "catalog-lan", nil)
	assignment := sourceCatalogAssignment(t, admin, network, host)
	producerConn := c.dial(sourceCatalogID(t, host.GetId()))
	cache := &catalog.Memory{}
	options := catalogcp.Options{ProducerID: sourceCatalogID(t, host.GetId()), TenantID: sourceCatalogID(t, set.GetTenant().GetId()), SetID: sourceCatalogID(t, set.GetId()), PollInterval: 30 * time.Millisecond, CallTimeout: 300 * time.Millisecond, Cache: cache}
	provider, err := catalogcp.New(producerConn, options)
	require.NoError(t, err)
	empty, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Empty(t, empty.Sources, "an authorized empty catalog is a successful read")

	otherSet := sourceCatalogSet(t, admin, "other-catalog")
	otherHost := sourceCatalogAdopt(t, c, admin, otherSet, "catalog-other")
	otherAssignment := sourceCatalogAssignment(t, admin, network, otherHost)
	for _, foreign := range []*api.CameraAssignmentRef{
		api.CameraAssignmentRef_builder{Id: otherAssignment.GetId()}.Build(),
		api.CameraAssignmentBySet(api.SetRef_builder{Id: otherSet.GetId()}.Build()),
	} {
		_, err := api.NewCameraAssignmentServiceClient(producerConn).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: foreign}.Build())
		require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err), "the Producer cannot read another Set's known assignment")
	}
	_, err = api.NewCameraAssignmentServiceClient(c.dial(sourceCatalogID(t, otherHost.GetId()))).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewCameraAssignmentServiceClient(producerConn).Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "snapshot permission does not grant generic inventory reads")
	_, err = c.running.CP.Identity.AddPerson(ctx, "acme", "catalog-viewer", "Catalog viewer")
	require.NoError(t, err)
	for _, conn := range []*grpc.ClientConn{c.dial("@acme/catalog-viewer"), exportReader(t, c, admin)} {
		_, err = api.NewCameraAssignmentServiceClient(conn).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()}.Build())
		require.Equal(t, codes.PermissionDenied, status.Code(err), "viewer/Reader roles do not receive capture configuration")
	}

	first := sourceCatalogPrepare(t, admin, assignment, "door", "rtsp://192.0.2.10/main")
	var publicationResponse *api.CameraJobPublishResponse
	publisher := &publication.Publisher{AssignmentID: sourceCatalogID(t, assignment.GetId()), Scope: empty.Scope, Provider: provider, Memory: cache,
		Committer: sourceCatalogCommit(func(_ context.Context, d provision.Deployment) (publication.Receipt, error) {
			require.Equal(t, first.deployment.OperationID, d.OperationID)
			var receipt publication.Receipt
			publicationResponse, receipt = sourceCatalogPublish(t, admin, assignment, first)
			beforeNotify, err := cache.Snapshot(ctx)
			require.NoError(t, err)
			require.Equal(t, empty, beforeNotify, "a central commit does not manufacture an in-memory hint")
			return receipt, nil
		}),
	}
	bound, err := publisher.Publish(ctx, first.deployment)
	require.NoError(t, err)
	accepted, err := cache.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, accepted.Sources, 1)
	require.Equal(t, bound.SourceID, accepted.Sources[0].SourceID)
	require.Zero(t, accepted.Sources[0].Profile.KeyframeInterval, "a declared profile is not actual cadence proof (#190)")
	stale := proto.Clone(first.request).(*api.CameraJobPublishRequest)
	stale.SetOperationKey("stale-publication")
	_, err = api.NewCameraJobServiceClient(admin).Publish(ctx, stale)
	require.Equal(t, codes.Aborted, status.Code(err))
	unchanged, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, accepted, unchanged)

	second := sourceCatalogPrepare(t, admin, assignment, "lobby", "rtsp://192.0.2.11/main")
	_, _ = sourceCatalogPublish(t, admin, assignment, second)
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	changed, err := provider.WaitForChange(wait, accepted.Revision)
	cancel()
	require.NoError(t, err, "bounded CP reads recover a publication without any Publisher notification")
	require.Greater(t, changed.Catalog, accepted.Revision.Catalog)
	full, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, full.Sources, 2, "a read is the complete catalog, not the newest binding alone")

	c.stopCP()
	_, err = provider.Snapshot(ctx)
	require.ErrorIs(t, err, catalog.ErrUnavailable, "a failed read is not a successful empty catalog")
	retained, err := cache.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, full, retained)
	c.startCP()
	var recovered catalog.Snapshot
	require.Eventually(t, func() bool {
		recovered, err = provider.Snapshot(ctx)
		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
	require.Equal(t, full, recovered, "the same authenticated client recovers the exact committed catalog")

	stopped, err := api.NewCameraBindingServiceClient(admin).SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{
		Ref: api.CameraBindingRef_builder{Id: publicationResponse.GetBinding().GetId()}.Build(), ExpectedVersion: publicationResponse.GetBinding().GetDateUpdated(), Collect: false,
	}.Build())
	require.NoError(t, err)
	require.False(t, stopped.GetCollect())
	tail, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, tail.Sources, 1)
	require.Equal(t, "lobby", tail.Sources[0].Alias)
	require.Greater(t, tail.Revision.Catalog, recovered.Revision.Catalog)
	_, err = api.NewSourceServiceClient(admin).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: publicationResponse.GetBinding().GetSource().GetId()}.Build()}.Build())
	require.NoError(t, err, "collection stop keeps the stable Source row")
	_, err = api.NewProducerServiceClient(admin).Erase(ctx, api.ProducerRef_builder{Id: host.GetId()}.Build())
	require.NoError(t, err)
	_, err = provider.Snapshot(ctx)
	require.Error(t, err, "the old identity cannot continue reading after revocation")
	retained, err = cache.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, tail, retained, "revocation is an explicit error, not an invented empty catalog")
}

func TestSourceCatalogProducerLoadsAfterAuthentication(t *testing.T) {
	for _, binary := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Skip(binary + " unavailable")
		}
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set := sourceCatalogSet(t, admin, "managed-runtime")
	configured := make(chan struct{})
	sessions := make(chan producer.SourceSession, 1)
	var calls atomic.Int32
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "managed-producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: 4 * time.Second, HeartbeatInterval: time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
		SourceProviderFactory: func(ctx context.Context, session producer.SourceSession) (catalog.Provider, error) {
			calls.Add(1)
			sessions <- session
			select {
			case <-configured:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return catalogcp.New(session.Conn, catalogcp.Options{ProducerID: session.ProducerID, TenantID: session.TenantID, SiteID: session.SiteID, SetID: session.SetID, CallTimeout: time.Second})
		},
	})
	require.NoError(t, err)
	require.Zero(t, calls.Load(), "the constructor must not read a catalog without host authentication")
	exited := make(chan struct{})
	done := make(chan error, 1)
	go func() { defer close(exited); done <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			t.Error("managed Producer outlived cancellation")
		}
	})
	producers := api.NewProducerServiceClient(admin)
	var pending *api.Producer
	require.Eventually(t, func() bool {
		rows, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 100}.Build())
		if err != nil {
			return false
		}
		for _, row := range rows.GetItems() {
			if row.GetState() == api.HostState_HOST_STATE_PENDING {
				pending = row
				return true
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond)
	require.Zero(t, calls.Load(), "a joined but unadopted machine cannot read its future catalog")
	host, err := producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	var session producer.SourceSession
	select {
	case session = <-sessions:
	case err := <-done:
		t.Fatalf("Producer ended before its authenticated factory: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Equal(t, sourceCatalogID(t, host.GetId()), session.ProducerID)
	require.Equal(t, sourceCatalogID(t, set.GetTenant().GetId()), session.TenantID)
	require.Equal(t, sourceCatalogID(t, set.GetId()), session.SetID)
	_, err = api.NewSetServiceClient(session.Conn).Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err, "the factory receives the actual adopted machine's usable CP connection")
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{
		Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "loopback-camera", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo",
	}.Build())
	require.NoError(t, err)
	assignment := sourceCatalogAssignment(t, admin, network, host)
	prepared := sourceCatalogPrepare(t, admin, assignment, "managed-camera", cameratest.ServeRTSP(t))
	published, _ := sourceCatalogPublish(t, admin, assignment, prepared)
	close(configured)
	select {
	case <-p.Ready:
	case err := <-done:
		t.Fatalf("managed Producer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Equal(t, int32(1), calls.Load())
	sourceID := published.GetBinding().GetSource().GetId()
	rows, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{
		api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build(),
	}}.Build())
	require.NoError(t, err)
	require.Len(t, rows.GetItems(), 1)
	require.Equal(t, sourceID, rows.GetItems()[0].GetId(), "registration consumes the explicit CP Source ID without creating another row")
	require.Eventually(t, func() bool {
		current, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: host.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		for _, report := range current.GetStatus().GetSources() {
			if string(report.GetSourceId()) == string(sourceID) && report.GetInputUp() && report.GetFrameRate() > 0 && report.GetMeasuredBitrate() > 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "the managed RTSP specification starts actual capture")
	lamina, _ := committedLamina(t, ctx, admin, set)
	require.Equal(t, sourceID, lamina.GetSource().GetId())
	require.False(t, lamina.GetIncomplete())
	require.GreaterOrEqual(t, lamina.GetDateStarted().AsTime(), published.GetJob().GetDatePublished().AsTime(), "the managed catalog produces a new recording after publication")
}
