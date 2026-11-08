package e2e_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
)

func cameraControlCluster(t *testing.T) *cluster {
	t.Helper()
	return start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: 10 * time.Millisecond}
	})
}

func cameraNetwork(t *testing.T, conn *grpc.ClientConn, alias string, site *api.SiteRef) *api.CameraNetwork {
	t.Helper()
	v, err := api.NewCameraNetworkServiceClient(conn).Configure(context.Background(), api.CameraNetworkConfigureRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Site: site,
		Alias: alias, Name: alias, Cidrs: []string{"192.0.2.0/24"}, InterfaceHint: "camera0", Multicast: true,
	}.Build())
	require.NoError(t, err)
	return v
}

// These are native API calls through the real tenant wall and roster policy,
// not direct calls to an ungated core or a fixture-owned database.
func TestCameraControlScopeAndCAS(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	networks := api.NewCameraNetworkServiceClient(admin)
	n := cameraNetwork(t, admin, "cameras", nil)
	ref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()

	updated, err := networks.Configure(ctx, api.CameraNetworkConfigureRequest_builder{
		Ref: ref, Alias: n.GetAlias(), Name: "Updated", Cidrs: []string{"192.0.2.0/24"}, InterfaceHint: "camera0", ExpectedVersion: n.GetDateUpdated(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "Updated", updated.GetName())
	_, err = networks.Configure(ctx, api.CameraNetworkConfigureRequest_builder{
		Ref: ref, Alias: n.GetAlias(), Name: "Stale overwrite", Cidrs: []string{"192.0.2.0/24"}, ExpectedVersion: n.GetDateUpdated(),
	}.Build())
	require.Contains(t, []codes.Code{codes.Aborted, codes.FailedPrecondition}, status.Code(err))
	current, err := networks.Get(ctx, api.CameraNetworkGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Equal(t, "Updated", current.GetName(), "a stale writer cannot overwrite a newer network")

	_, err = networks.Add(ctx, api.CameraNetworkAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: n.GetTenant().GetId()}.Build(), Alias: "bypass", Cidrs: []string{"not a CIDR"},
	}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "generated writes cannot bypass Configure validation")
	_, err = networks.Configure(ctx, api.CameraNetworkConfigureRequest_builder{
		Tenant: api.TenantRef_builder{Id: n.GetTenant().GetId()}.Build(), Alias: "invalid", Cidrs: []string{"not a CIDR"},
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = api.NewCameraNetworkServiceClient(c.dialCluster(c.operator())).List(ctx, api.CameraNetworkListRequest_builder{}.Build())
	require.Contains(t, []codes.Code{codes.Unimplemented, codes.PermissionDenied}, status.Code(err), "camera authority belongs on the tenant surface")

	_, err = c.running.CP.Identity.AddPerson(ctx, "acme", "camera-viewer", "Camera viewer")
	require.NoError(t, err)
	_, err = api.NewCameraNetworkServiceClient(c.dial("@acme/camera-viewer")).List(ctx, api.CameraNetworkListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "ordinary viewers do not get provisioning inventory by default")

	boss, _, err := c.running.CP.Identity.Seed(ctx, "globex", "boss")
	require.NoError(t, err)
	_, err = c.running.CP.Identity.Grant(ctx, "globex", identity.OperatorRole, []string{identity.Everything}, boss.Id)
	require.NoError(t, err)
	other := c.dial("@globex/boss")
	_, err = api.NewCameraNetworkServiceClient(other).Get(ctx, api.CameraNetworkGetRequest_builder{Ref: ref}.Build())
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err), "a foreign tenant cannot resolve a known network ID")
	_, err = api.NewCameraDeviceServiceClient(other).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: ref, DeviceKey: "stolen"}.Build())
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err), "custom writes cannot use a foreign network ref")

	const sentinel = "camera-test-password-never-persist"
	_, err = api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{
		Network: ref, DeviceKey: "userinfo", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{
			Uri: "rtsp://admin:" + sentinel + "@192.0.2.10/stream", Provenance: "seed", ObservedAt: timestamppb.Now(),
		}.Build()},
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotContains(t, err.Error(), sentinel)
	_, err = api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{
		Network: ref, DeviceKey: "outside", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{
			Uri: "http://198.51.100.10/onvif/device_service", Provenance: "seed", ObservedAt: timestamppb.Now(),
		}.Build()},
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "camera endpoints must stay within the assigned camera LAN")
}

func TestCameraControlSiteWall(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	tenant := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()
	sites := api.NewSiteServiceClient(admin)
	west, err := sites.Add(ctx, api.SiteAddRequest_builder{Tenant: tenant, Alias: "west"}.Build())
	require.NoError(t, err)
	east, err := sites.Add(ctx, api.SiteAddRequest_builder{Tenant: tenant, Alias: "east"}.Build())
	require.NoError(t, err)
	w := cameraNetwork(t, admin, "west-cameras", api.SiteRef_builder{Id: west.GetId()}.Build())
	e := cameraNetwork(t, admin, "east-cameras", api.SiteRef_builder{Id: east.GetId()}.Build())

	person, err := c.running.CP.Identity.AddPerson(ctx, "acme", "west-operator", "West operator")
	require.NoError(t, err)
	_, err = c.running.CP.Identity.Grant(ctx, "acme", "camera-inventory-read", []string{
		api.HolderService_Get_FullMethodName,
		api.CameraNetworkService_Get_FullMethodName, api.CameraNetworkService_List_FullMethodName,
	}, person.Id)
	require.NoError(t, err)
	conn := c.dial("@acme/west-operator")
	holders := api.NewHolderServiceClient(admin)
	_, err = api.NewHolderServiceClient(conn).Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: person.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	row, err := holders.Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: person.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	_, err = holders.Patch(ctx, api.HolderPatchRequest_builder{
		Ref: api.HolderRef_builder{Id: row.GetId()}.Build(), DateUpdated: row.GetDateUpdated(), AllSites: z.Ptr(false),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{
		Tenant: tenant, Site: api.SiteRef_builder{Id: west.GetId()}.Build(), Holder: api.HolderRef_builder{Id: person.Id.Bytes()}.Build(),
	}.Build())
	require.NoError(t, err)
	networks := api.NewCameraNetworkServiceClient(conn)
	require.Eventually(t, func() bool {
		vs, err := networks.List(ctx, api.CameraNetworkListRequest_builder{}.Build())
		return err == nil && len(vs.GetItems()) == 1 && string(vs.GetItems()[0].GetId()) == string(w.GetId())
	}, 15*time.Second, 50*time.Millisecond, "service grants do not remove the site's row predicate")
	_, err = networks.Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: e.GetId()}.Build()}.Build())
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied}, status.Code(err))
}

func TestCameraControlDurablePublication(t *testing.T) {
	c := cameraControlCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	set, live, _ := liveProducer(t, ctx, c, admin, producer.SourceConfig{
		Alias: "seed", Input: "raw:" + sample, Format: "h264", Fps: 30, MaxBitrate: 2_000_000,
	})
	// Import a Source that already owns an actual committed recording. The
	// managed collection flag must preserve that recording's metadata as well.
	laminae := api.NewLaminaServiceClient(admin)
	archive, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: live.GetSourceId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-time.Hour).Truncate(time.Second)),
	}.Build())
	require.NoError(t, err)
	media, err := os.ReadFile(filepath.Join("..", "fmp4", "testdata", "av.mp4"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, put(t, archive, media, archive.GetDateStarted().AsTime().Add(6*time.Second)))
	historyRef := api.LaminaRef_builder{Id: archive.GetLaminaId()}.Build()
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: historyRef}.Build())
		return err == nil && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 10*time.Second, 50*time.Millisecond, "the existing recording is committed before enrollment")
	sources := api.NewSourceServiceClient(admin)
	sourceRef := api.SourceRef_builder{Id: live.GetSourceId()}.Build()
	legacy, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: sourceRef}.Build())
	require.NoError(t, err)
	_, err = sources.Patch(ctx, api.SourcePatchRequest_builder{Ref: sourceRef, Labels: map[string]string{"legacy-owner": "preserve"}, DateUpdated: legacy.GetDateUpdated()}.Build())
	require.NoError(t, err)
	producers, err := api.NewProducerServiceClient(admin).List(ctx, api.ProducerListRequest_builder{Size: 100}.Build())
	require.NoError(t, err)
	var host *api.Producer
	for _, v := range producers.GetItems() {
		if string(v.GetSet().GetId()) == string(set.GetId()) {
			host = v
		}
	}
	require.NotNil(t, host)
	n := cameraNetwork(t, admin, "camera-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	devices := api.NewCameraDeviceServiceClient(admin)
	claim := api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "inventory-camera-1", Name: "Front door"}.Build()
	device, err := devices.Claim(ctx, claim)
	require.NoError(t, err)
	again, err := devices.Claim(ctx, claim)
	require.NoError(t, err)
	require.Equal(t, device.GetId(), again.GetId(), "discovery retry preserves the physical inventory row")
	device, err = devices.Observe(ctx, api.CameraDeviceObserveRequest_builder{
		Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), ExpectedVersion: device.GetDateUpdated(),
		Identity: api.CameraIdentityEvidence_builder{Authenticated: true, Manufacturer: "fixture", Serial: "camera-1", Macs: []string{"02:00:00:00:00:01"}}.Build(),
		Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{
			Uri: "rtsp://192.0.2.10/stream", Provenance: "authenticated", ObservedAt: timestamppb.Now(),
		}.Build()},
	}.Build())
	require.NoError(t, err)
	assignments := api.NewCameraAssignmentServiceClient(admin)
	assignment, err := assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{
		Network: nref, Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "floor4",
	}.Build())
	require.NoError(t, err)
	aref := api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()
	bindings := api.NewCameraBindingServiceClient(admin)
	bind := api.CameraBindingBindRequest_builder{
		Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "front-door",
		SourceAlias: "seed", Source: api.SourceRef_builder{Id: live.GetSourceId()}.Build(),
	}.Build()
	binding, err := bindings.Bind(ctx, bind)
	require.NoError(t, err)
	bindingAgain, err := bindings.Bind(ctx, bind)
	require.NoError(t, err)
	require.Equal(t, binding.GetId(), bindingAgain.GetId())
	require.Equal(t, live.GetSourceId(), binding.GetSource().GetId(), "an explicit import preserves the existing Source")

	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build()
	credential := api.CameraCredentialReference_builder{Provider: "cp", Id: "front-door-stream", Version: "1", Purpose: "streaming"}.Build()
	jobs := api.NewCameraJobServiceClient(admin)
	create := api.CameraJobCreateRequest_builder{
		Binding: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), RequestKey: "desired-1",
		Inputs: api.CameraJobInputs_builder{Desired: profile, StreamingCredential: credential}.Build(),
	}.Build()
	job, err := jobs.Create(ctx, create)
	require.NoError(t, err)
	jobAgain, err := jobs.Create(ctx, create)
	require.NoError(t, err)
	require.Equal(t, job.GetId(), jobAgain.GetId())
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	claimJob := api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "claim-1", LeaseSeconds: 60}.Build()
	job, err = jobs.Claim(ctx, claimJob)
	require.NoError(t, err)
	jobAgain, err = jobs.Claim(ctx, claimJob)
	require.NoError(t, err)
	require.Equal(t, job.GetLeaseGeneration(), jobAgain.GetLeaseGeneration(), "a lost Claim response does not issue a second lease")
	observed := api.CameraJobObservation_builder{
		Effective: profile,
		Verified:  api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: 60, ObservedAt: timestamppb.Now()}.Build(),
	}.Build()
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{
		Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "applying-1",
		LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING,
	}.Build())
	require.NoError(t, err)
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{
		Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "verifying-1", Observed: observed,
		LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING,
	}.Build())
	require.NoError(t, err)
	publish := api.CameraJobPublishRequest_builder{
		Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "publish-1", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(),
		Spec:     api.CameraCaptureSpec_builder{BindingId: binding.GetId(), SourceId: live.GetSourceId(), Input: "rtsp://192.0.2.10/stream", Profile: profile, Credential: credential, Alias: "seed", Name: "Front door", Zone: "entry", Labels: map[string]string{"floor": "4F"}}.Build(),
		Observed: observed,
	}.Build()
	publication, err := jobs.Publish(ctx, publish)
	require.NoError(t, err)
	replay, err := jobs.Publish(ctx, publish)
	require.NoError(t, err)
	require.Equal(t, publication.GetCatalogRevision(), replay.GetCatalogRevision(), "publication response loss does not publish a new catalog generation")
	snapshot, err := assignments.Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	require.Len(t, snapshot.GetSources(), 1)
	require.Equal(t, live.GetSourceId(), snapshot.GetSources()[0].GetSourceId())
	require.Equal(t, publication.GetCatalogRevision(), snapshot.GetRevision())
	metadata, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: sourceRef}.Build())
	require.NoError(t, err)
	require.Equal(t, "Front door", metadata.GetName())
	require.Equal(t, "entry", metadata.GetZone())
	require.Equal(t, "4F", metadata.GetLabels()["floor"])
	require.Equal(t, "preserve", metadata.GetLabels()["legacy-owner"])
	require.Equal(t, "front-door", metadata.GetLabels()["shale.provision.binding"])
	require.Equal(t, "inventory-camera-1", metadata.GetLabels()["shale.provision.device"], "published capture labels must agree with normal Producer registration")
	_, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{
		Ref: jref, ExpectedVersion: publication.GetJob().GetDateUpdated(), OperationKey: "unsupported-active",
		LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE,
	}.Build())
	require.Contains(t, []codes.Code{codes.InvalidArgument, codes.FailedPrecondition}, status.Code(err), "an Active claim without actual live/media evidence is insufficient")

	// Reopen the same CP database using established API clients. The actual
	// listeners, rather than unbound sampled ports, determine reconnect targets.
	c.cfg.Server.Addr, c.cfg.Cluster.Addr = c.running.TenantAddr, c.running.ClusterAddr
	c.stopCP()
	c.startCP()
	var recovered *api.CameraCatalogSnapshot
	require.Eventually(t, func() bool {
		recovered, err = assignments.Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: aref}.Build())
		return err == nil
	}, 15*time.Second, 50*time.Millisecond, "the original API client reconnects after CP restart")
	require.Equal(t, snapshot.GetRevision(), recovered.GetRevision())
	require.Equal(t, snapshot.GetAssignment().GetId(), recovered.GetAssignment().GetId())
	require.Equal(t, snapshot.GetAssignment().GetGeneration(), recovered.GetAssignment().GetGeneration())
	require.Len(t, recovered.GetSources(), 1)
	require.True(t, proto.Equal(snapshot.GetSources()[0], recovered.GetSources()[0]), "the authoritative Source specification survives CP restart without worker memory")
	_, err = jobs.Create(ctx, create)
	require.NoError(t, err)
	stopped, err := bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{
		Ref: api.CameraBindingRef_builder{Id: publication.GetBinding().GetId()}.Build(), ExpectedVersion: publication.GetBinding().GetDateUpdated(), Collect: false,
	}.Build())
	require.NoError(t, err)
	require.False(t, stopped.GetCollect())
	empty, err := assignments.Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	require.Empty(t, empty.GetSources())
	require.Greater(t, empty.GetRevision(), recovered.GetRevision())
	_, err = api.NewSourceServiceClient(admin).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: live.GetSourceId()}.Build()}.Build())
	require.NoError(t, err, "removing a managed capture does not erase its Source")
	history, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: historyRef}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, history.GetState(), "prior committed recording metadata survives collection stop")
}

func TestCameraControlLeaseFencingAndHandover(t *testing.T) {
	c := cameraControlCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	sourceConfig := producer.SourceConfig{Alias: "seed", Input: "raw:" + sample, Format: "h264", Fps: 30, MaxBitrate: 2_000_000}
	set, live, _ := liveProducer(t, ctx, c, admin, sourceConfig)
	producers := api.NewProducerServiceClient(admin)
	rows, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 100}.Build())
	require.NoError(t, err)
	var old *api.Producer
	for _, v := range rows.GetItems() {
		if string(v.GetSet().GetId()) == string(set.GetId()) {
			old = v
		}
	}
	require.NotNil(t, old)
	n := cameraNetwork(t, admin, "camera-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	device, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "physical-camera"}.Build())
	require.NoError(t, err)
	device, err = api.NewCameraDeviceServiceClient(admin).Observe(ctx, api.CameraDeviceObserveRequest_builder{
		Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), ExpectedVersion: device.GetDateUpdated(),
		Identity: api.CameraIdentityEvidence_builder{Authenticated: true, Serial: "fixture-camera"}.Build(),
	}.Build())
	require.NoError(t, err)
	assignments := api.NewCameraAssignmentServiceClient(admin)
	assignment, err := assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: old.GetId()}.Build(), WorkerSlot: "old-slot"}.Build())
	require.NoError(t, err)
	aref := api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()
	binding, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{
		Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "door", SourceAlias: "seed", Source: api.SourceRef_builder{Id: live.GetSourceId()}.Build(),
	}.Build())
	require.NoError(t, err)
	other, err := c.running.CP.Identity.AddPerson(ctx, "acme", "second-executor", "Second executor")
	require.NoError(t, err)
	_, err = c.running.CP.Identity.Grant(ctx, "acme", "camera-executor", []string{identity.Everything}, other.Id)
	require.NoError(t, err)
	clients := []api.CameraJobServiceClient{api.NewCameraJobServiceClient(admin), api.NewCameraJobServiceClient(c.dial("@acme/second-executor"))}
	job, err := clients[0].Create(ctx, api.CameraJobCreateRequest_builder{
		Binding: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), RequestKey: "desired-1",
		Inputs: api.CameraJobInputs_builder{
			Desired:             api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build(),
			StreamingCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "door-stream", Version: "1", Purpose: "streaming"}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	type result struct {
		i   int
		job *api.CameraJob
		err error
	}
	ready, results := make(chan struct{}), make(chan result, 2)
	for i, client := range clients {
		go func(i int, client api.CameraJobServiceClient) {
			<-ready
			v, err := client.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: []string{"executor-a", "executor-b"}[i], LeaseSeconds: 60}.Build())
			results <- result{i, v, err}
		}(i, client)
	}
	close(ready)
	got := []result{<-results, <-results}
	var winner result
	successes := 0
	for _, r := range got {
		if r.err == nil {
			winner, successes = r, successes+1
		} else {
			require.Contains(t, []codes.Code{codes.Aborted, codes.FailedPrecondition}, status.Code(r.err))
		}
	}
	require.Equal(t, 1, successes, "only one concurrent executor may own the physical device")
	require.NotEmpty(t, winner.job.GetLeaseOwner())
	report := api.CameraJobReportRequest_builder{
		Ref: jref, ExpectedVersion: winner.job.GetDateUpdated(), OperationKey: "owner-only-report",
		LeaseGeneration: winner.job.GetLeaseGeneration(), AssignmentGeneration: winner.job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING,
	}.Build()
	_, err = clients[1-winner.i].Report(ctx, report)
	require.Contains(t, []codes.Code{codes.PermissionDenied, codes.FailedPrecondition}, status.Code(err), "knowing another executor's lease fields does not confer ownership")

	currentAssignment, err := assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	changed, err := assignments.Assign(ctx, api.CameraAssignmentAssignRequest_builder{
		Ref: aref, Network: nref, Producer: api.ProducerRef_builder{Id: old.GetId()}.Build(), WorkerSlot: "new-slot", ExpectedVersion: currentAssignment.GetDateUpdated(),
	}.Build())
	require.NoError(t, err)
	require.Greater(t, changed.GetGeneration(), assignment.GetGeneration())
	_, err = clients[winner.i].Report(ctx, report)
	require.Contains(t, []codes.Code{codes.Aborted, codes.FailedPrecondition}, status.Code(err), "a superseded assignment fences the previously valid lease")

	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR("replacement-camera-producer")
	require.NoError(t, err)
	joined, err := api.NewProducerServiceClient(plainConn(t, c.running.TenantAddr)).Join(ctx, api.ProducerJoinRequest_builder{
		Tenant: "acme", Host: api.HostJoin_builder{HardwareId: "camera-control-replacement", Hostname: "replacement-camera-producer", Csr: csr, KeyFingerprint: key.Fingerprint()}.Build(),
	}.Build())
	require.NoError(t, err)
	pending, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: joined.GetAnswer().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.HostState_HOST_STATE_PENDING, pending.GetState(), "a replacement joins with its own key and physical identity")
	oldID, err := pdid.From(old.GetId())
	require.NoError(t, err)
	oldSources := api.NewSourceServiceClient(c.dial(oldID.String()))
	list := api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build()
	_, err = oldSources.List(ctx, list)
	require.NoError(t, err, "the old adopted identity is usable before handover")
	handed, err := assignments.Handover(ctx, api.CameraAssignmentHandoverRequest_builder{
		Ref: aref, ExpectedVersion: changed.GetDateUpdated(), ExpectedGeneration: changed.GetGeneration(), Replacement: api.ProducerRef_builder{Id: pending.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Greater(t, handed.GetGeneration(), changed.GetGeneration())
	require.Equal(t, set.GetId(), handed.GetSet().GetId())
	require.Equal(t, pending.GetId(), handed.GetProducer().GetId())
	_, err = oldSources.List(ctx, list)
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied, codes.Unauthenticated}, status.Code(err), "handover revokes the old collector's CP identity; outstanding data-plane tokens have their separate bounded lifetime")
	kept, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: binding.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, live.GetSourceId(), kept.GetSource().GetId())
	row, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.HostState_HOST_STATE_ADOPTED, row.GetState())
	require.Equal(t, set.GetId(), row.GetSet().GetId(), "handover reassigns the same immutable Set instead of migrating Sources")
}

// The trigger is a private-database fault injector, not the API under test:
// fail the second write of Bind after Source.Add has already succeeded.
func cameraBindingInsertFault(t *testing.T, c *cluster) func() {
	t.Helper()
	ctx := context.Background()
	if c.cfg.Db.Driver == "pgx" {
		_, err := c.running.CP.Db.ExecContext(ctx, `CREATE FUNCTION cxz_reject_camera_binding() RETURNS trigger AS $$
BEGIN
  IF NEW.logical_key = 'rollback-binding' THEN
    RAISE EXCEPTION 'fixture binding write failure';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql`)
		require.NoError(t, err)
		_, err = c.running.CP.Db.ExecContext(ctx, `CREATE TRIGGER cxz_reject_camera_binding BEFORE INSERT ON camerabinding FOR EACH ROW EXECUTE FUNCTION cxz_reject_camera_binding()`)
		require.NoError(t, err)
		return func() {
			_, err := c.running.CP.Db.ExecContext(ctx, `DROP TRIGGER IF EXISTS cxz_reject_camera_binding ON camerabinding`)
			require.NoError(t, err)
			_, err = c.running.CP.Db.ExecContext(ctx, `DROP FUNCTION IF EXISTS cxz_reject_camera_binding()`)
			require.NoError(t, err)
		}
	}
	_, err := c.running.CP.Db.ExecContext(ctx, `CREATE TRIGGER cxz_reject_camera_binding BEFORE INSERT ON camerabinding
WHEN NEW.logical_key = 'rollback-binding'
BEGIN SELECT RAISE(ABORT, 'fixture binding write failure'); END`)
	require.NoError(t, err)
	return func() {
		_, err := c.running.CP.Db.ExecContext(ctx, `DROP TRIGGER IF EXISTS cxz_reject_camera_binding`)
		require.NoError(t, err)
	}
}

func TestCameraControlNewSourceAtomicityAndPendingStop(t *testing.T) {
	c := cameraControlCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	set, _, _ := liveProducer(t, ctx, c, admin, producer.SourceConfig{Alias: "seed", Input: "raw:" + sample, Format: "h264", Fps: 30, MaxBitrate: 2_000_000})
	rows, err := api.NewProducerServiceClient(admin).List(ctx, api.ProducerListRequest_builder{Size: 100}.Build())
	require.NoError(t, err)
	var host *api.Producer
	for _, v := range rows.GetItems() {
		if string(v.GetSet().GetId()) == string(set.GetId()) {
			host = v
		}
	}
	require.NotNil(t, host)
	n := cameraNetwork(t, admin, "camera-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	device, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "new-device"}.Build())
	require.NoError(t, err)
	assignment, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: host.GetId()}.Build(), WorkerSlot: "floor4"}.Build())
	require.NoError(t, err)
	aref := api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build()
	bindings := api.NewCameraBindingServiceClient(admin)
	create := api.CameraBindingBindRequest_builder{Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "new-camera", SourceAlias: "fresh-camera"}.Build()
	binding, err := bindings.Bind(ctx, create)
	require.NoError(t, err, "a newly discovered camera can create its Source inside the binding transaction")
	repeated, err := bindings.Bind(ctx, create)
	require.NoError(t, err)
	require.Equal(t, binding.GetId(), repeated.GetId())
	require.Equal(t, binding.GetSource().GetId(), repeated.GetSource().GetId())
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 2, "one seed and one fresh camera, even after Bind replay")
	func() {
		undo := cameraBindingInsertFault(t, c)
		defer undo()
		_, err = bindings.Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "rollback-binding", SourceAlias: "rollback-camera"}.Build())
		require.Error(t, err, "the fixture fails Binding insertion after creating the Source")
		_, err = api.NewSourceServiceClient(admin).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Slug: api.SourceRefBySlug_builder{
			Alias: z.Ptr("rollback-camera"), Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
		}.Build()}.Build()}.Build())
		require.Equal(t, codes.NotFound, status.Code(err))
		var count int
		require.NoError(t, c.running.CP.Db.QueryRowContext(ctx, `SELECT count(*) FROM source WHERE alias = 'rollback-camera'`).Scan(&count))
		require.Zero(t, count, "rollback leaves no hidden or soft-erased orphan Source")
	}()
	first, err := anypb.New(api.CameraBindingBindRequest_builder{Assignment: aref, Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), LogicalKey: "batch-rollback", SourceAlias: "batch-camera"}.Build())
	require.NoError(t, err)
	second, err := anypb.New(api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "invalid-batch-network", Cidrs: []string{"not-a-cidr"}}.Build())
	require.NoError(t, err)
	_, err = pdpb.NewBatchServiceClient(admin).Do(ctx, pdpb.BatchRequest_builder{Ops: []*pdpb.Op{
		pdpb.Op_builder{Method: api.CameraBindingService_Bind_FullMethodName, Request: first}.Build(),
		pdpb.Op_builder{Method: api.CameraNetworkService_Configure_FullMethodName, Request: second}.Build(),
	}}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "the second operation rejects its CIDR after Bind succeeds")
	var batchSources, batchBindings int
	require.NoError(t, c.running.CP.Db.QueryRowContext(ctx, `SELECT count(*) FROM source WHERE alias = 'batch-camera'`).Scan(&batchSources))
	require.NoError(t, c.running.CP.Db.QueryRowContext(ctx, `SELECT count(*) FROM camerabinding WHERE logical_key = 'batch-rollback'`).Scan(&batchBindings))
	require.Zero(t, batchSources, "Source creation joins the external batch transaction")
	require.Zero(t, batchBindings, "Binding creation joins the external batch transaction")

	// A newly bound camera has not been collected yet. An explicit stop must
	// still cancel its pending work rather than taking a collect=false shortcut.
	require.False(t, binding.GetCollect())
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 30, MaxBitrateBps: 2_000_000}.Build()
	credential := api.CameraCredentialReference_builder{Provider: "cp", Id: "new-camera-stream", Version: "1", Purpose: "streaming"}.Build()
	jobs := api.NewCameraJobServiceClient(admin)
	job, err := jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), RequestKey: "initial-settings", Inputs: api.CameraJobInputs_builder{Desired: profile, StreamingCredential: credential}.Build()}.Build())
	require.NoError(t, err)
	jref := api.CameraJobRef_builder{Id: job.GetId()}.Build()
	job, err = jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "claim", LeaseSeconds: 60}.Build())
	require.NoError(t, err)
	observed := api.CameraJobObservation_builder{Effective: profile, Verified: api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: 60, ObservedAt: timestamppb.Now()}.Build()}.Build()
	job, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "verify", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, Observed: observed}.Build())
	require.NoError(t, err)
	_, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), ExpectedVersion: binding.GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	stoppedJob, err := jobs.Get(ctx, api.CameraJobGetRequest_builder{Ref: jref}.Build())
	require.NoError(t, err)
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, stoppedJob.GetStage(), "stop applies even before the first publication")
	_, err = jobs.Publish(ctx, api.CameraJobPublishRequest_builder{Ref: jref, ExpectedVersion: job.GetDateUpdated(), OperationKey: "late-publish", LeaseGeneration: job.GetLeaseGeneration(), AssignmentGeneration: job.GetAssignmentGeneration(), Observed: observed,
		Spec: api.CameraCaptureSpec_builder{BindingId: binding.GetId(), SourceId: binding.GetSource().GetId(), Input: "rtsp://192.0.2.11/stream", Profile: profile, Credential: credential, Alias: "fresh-camera"}.Build(),
	}.Build())
	require.Error(t, err, "late publication cannot restart a stopped camera")
	snapshot, err := api.NewCameraAssignmentServiceClient(admin).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: aref}.Build())
	require.NoError(t, err)
	require.Empty(t, snapshot.GetSources())
}
