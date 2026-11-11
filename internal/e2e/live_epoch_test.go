package e2e_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These exercise actual CP authorization and token issuance using synthetic
// Producer ACKs. They do not qualify camera media or claim decoded live frames;
// the lifecycle/relay media suites independently test those facts.
func liveEpochApply(t *testing.T, c *cluster, admin *grpc.ClientConn, a *api.CameraAssignment, alias string) (*api.CameraJobPublishResponse, *api.CameraApplication, *grpc.ClientConn) {
	t.Helper()
	prepared := sourceCatalogPrepare(t, admin, a, alias, "rtsp://192.0.2.10/"+alias)
	pub, _ := sourceCatalogPublish(t, admin, a, prepared)
	target, started, profile, owner := liveEpochBegin(t, c, a, pub.GetBinding().GetId(), alias+"-authority-test")
	return pub, liveEpochACK(t, owner, target, started, profile), owner
}

func liveEpochBegin(t *testing.T, c *cluster, a *api.CameraAssignment, binding []byte, intent string) (*api.CameraApplyTarget, *api.CameraApplication, *api.CameraCaptureProfile, *grpc.ClientConn) {
	t.Helper()
	ctx := t.Context()
	owner := c.dial(sourceCatalogID(t, a.GetProducer().GetId()))
	apps := api.NewCameraAssignmentServiceClient(owner)
	snapshot, err := apps.Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
	require.NoError(t, err)
	var desired *api.CameraCaptureSpec
	var proposals []*api.SourceProposal
	for _, spec := range snapshot.GetSources() {
		if bytes.Equal(spec.GetBindingId(), binding) {
			desired = spec
		}
		// Every fixture capture requests 1 Mbps. Admission is for the full
		// collecting catalog, including existing feeds and pending captures.
		proposals = append(proposals, api.SourceProposal_builder{Source: api.SourceRef_builder{Id: spec.GetSourceId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 1_128_000, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build())
	}
	require.NotNil(t, desired)
	_, err = api.NewSetServiceClient(owner).Negotiate(ctx, api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: a.GetSet().GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: snapshot.GetRevision()}.Build(), Sources: proposals}.Build())
	require.NoError(t, err)
	target := api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), BindingId: binding, SourceId: desired.GetSourceId(), CatalogRevision: snapshot.GetRevision(), SpecRevision: desired.GetRevision()}.Build()
	current, err := apps.Application(ctx, api.CameraApplicationRequest_builder{Target: target}.Build())
	require.NoError(t, err)
	started, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: current.GetStarting().GetSequence(), IntentKey: intent}.Build())
	require.NoError(t, err)
	return target, started, desired.GetProfile(), owner
}

func liveEpochACK(t *testing.T, owner *grpc.ClientConn, target *api.CameraApplyTarget, started *api.CameraApplication, profile *api.CameraCaptureProfile) *api.CameraApplication {
	t.Helper()
	now := time.Now()
	applied, err := api.NewCameraAssignmentServiceClient(owner).Applied(t.Context(), api.CameraAppliedRequest_builder{Target: target, Sequence: started.GetSequence(), CaptureEpoch: started.GetCaptureEpoch(), Media: api.CameraRuntimeMedia_builder{Profile: profile, FirstMediaAt: timestamppb.New(now.Add(-time.Second)), LastMediaAt: timestamppb.New(now), Frames: 10, Keyframes: 1}.Build()}.Build())
	require.NoError(t, err)
	return applied
}

func liveEpochView(t *testing.T, admin *grpc.ClientConn, source, relay []byte, epoch string) {
	t.Helper()
	live, err := api.NewSourceServiceClient(admin).Live(t.Context(), api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: source}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, relay, live.GetSource().GetRelayId())
	require.Equal(t, epoch, live.GetSource().GetCaptureEpoch())
	claims, err := token.Parse(live.GetSource().GetViewToken())
	require.NoError(t, err)
	require.Equal(t, api.TokenOp_TOKEN_OP_VIEW, claims.GetOp())
	require.Equal(t, source, claims.GetSource())
	require.Equal(t, relay, claims.GetAud())
	require.Equal(t, epoch, claims.GetCaptureEpoch())
}

func TestLiveEpochUsesActualProducerRelayAndKeepsHealthySetMembers(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "epoch-live")
	first := sourceCatalogAdopt(t, c, admin, set, "epoch-first")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "epoch-first-lan", nil), first)
	pubA, appA, ownerA := liveEpochApply(t, c, admin, a, "epoch-door")
	pubB, appB, _ := liveEpochApply(t, c, admin, a, "epoch-lobby")
	other, _ := c.startRelay("epoch-other-relay")
	relays := api.NewRelayServiceClient(c.dialCluster(c.operator()))
	list, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
	require.NoError(t, err)
	var original *api.Relay
	for _, r := range list.GetItems() {
		if !bytes.Equal(r.GetId(), other.Id().Bytes()) {
			original = r
			break
		}
	}
	require.NotNil(t, original)
	for _, relay := range [][]byte{original.GetId(), other.Id().Bytes()} {
		_, err = relays.Assign(ctx, api.RelayAssignRequest_builder{Ref: api.RelayRef_builder{Id: relay}.Build(), Producer: api.ProducerRef_builder{Id: first.GetId()}.Build()}.Build())
		require.NoError(t, err)
		liveEpochView(t, admin, pubA.GetBinding().GetSource().GetId(), relay, appA.GetCaptureEpoch())
		liveEpochView(t, admin, pubB.GetBinding().GetSource().GetId(), relay, appB.GetCaptureEpoch())
		assignment, err := api.NewProducerServiceClient(ownerA).Relay(ctx, api.ProducerRelayRequest_builder{}.Build())
		require.NoError(t, err)
		require.Equal(t, relay, assignment.GetRelay().GetRelayId())
		claims, err := token.Parse(assignment.GetRelay().GetPublishToken())
		require.NoError(t, err)
		require.Equal(t, relay, claims.GetAud())
		require.Equal(t, first.GetId(), claims.GetActor())
		grants := map[string]*api.CaptureEpochGrant{}
		for _, grant := range claims.GetCaptureEpochs() {
			grants[string(grant.GetSourceId())] = grant
		}
		require.Equal(t, appA.GetCaptureEpoch(), grants[string(pubA.GetBinding().GetSource().GetId())].GetCaptureEpoch())
		require.Equal(t, appB.GetCaptureEpoch(), grants[string(pubB.GetBinding().GetSource().GetId())].GetCaptureEpoch())
	}
	bindings := api.NewCameraBindingServiceClient(admin)
	fresh, err := bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pubB.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: fresh.GetId()}.Build(), ExpectedVersion: fresh.GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	pending := sourceCatalogPrepare(t, admin, a, "epoch-pending", "rtsp://192.0.2.11/pending")
	pubPending, _ := sourceCatalogPublish(t, admin, a, pending)
	view, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, view.GetSources(), 1)
	require.Equal(t, pubA.GetBinding().GetSource().GetId(), view.GetSources()[0].GetSourceId())
	for _, id := range [][]byte{pubB.GetBinding().GetSource().GetId(), pubPending.GetBinding().GetSource().GetId()} {
		_, err := api.NewSourceServiceClient(admin).Live(ctx, api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: id}.Build()}.Build())
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	}

	// A Set has one adopted Producer. Replacement uses the public Handover
	// transaction, rather than inventing an unsupported multi-owner fixture.
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR("epoch-replacement")
	require.NoError(t, err)
	joined, err := api.NewProducerServiceClient(plainConn(t, c.running.TenantAddr)).Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: api.HostJoin_builder{HardwareId: "epoch-replacement", Hostname: "epoch-replacement", Csr: csr, KeyFingerprint: key.Fingerprint()}.Build()}.Build())
	require.NoError(t, err)
	replacement := api.ProducerRef_builder{Id: joined.GetAnswer().GetId()}.Build()
	producers := api.NewProducerServiceClient(admin)
	_, err = producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: replacement, Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	pendingHost, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: replacement}.Build())
	require.NoError(t, err)
	require.Equal(t, api.HostState_HOST_STATE_PENDING, pendingHost.GetState())
	require.Empty(t, pendingHost.GetSet().GetId())
	assignments := api.NewCameraAssignmentServiceClient(admin)
	freshAssignment, err := assignments.Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
	require.NoError(t, err)
	handed, err := assignments.Handover(ctx, api.CameraAssignmentHandoverRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), ExpectedVersion: freshAssignment.GetDateUpdated(), ExpectedGeneration: freshAssignment.GetGeneration(), Replacement: replacement}.Build())
	require.NoError(t, err)
	require.Greater(t, handed.GetGeneration(), a.GetGeneration())
	require.Equal(t, set.GetId(), handed.GetSet().GetId())
	require.Equal(t, replacement.GetId(), handed.GetProducer().GetId())
	_, err = api.NewProducerServiceClient(ownerA).Relay(ctx, api.ProducerRelayRequest_builder{}.Build())
	require.Contains(t, []codes.Code{codes.NotFound, codes.PermissionDenied, codes.Unauthenticated}, status.Code(err), "old owner cannot mint future publish capabilities; issued tokens retain their own TTL")
	for _, pub := range []*api.CameraJobPublishResponse{pubA, pubB, pubPending} {
		kept, err := bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
		require.NoError(t, err)
		require.Equal(t, pub.GetBinding().GetId(), kept.GetId())
		require.Equal(t, pub.GetBinding().GetSource().GetId(), kept.GetSource().GetId())
	}
	refA := api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: pubA.GetBinding().GetSource().GetId()}.Build()}.Build()
	_, err = api.NewSourceServiceClient(admin).Live(ctx, refA)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "replacement has no applied live incarnation")
	_, err = relays.Assign(ctx, api.RelayAssignRequest_builder{Ref: api.RelayRef_builder{Id: original.GetId()}.Build(), Producer: replacement}.Build())
	require.NoError(t, err)
	target, started, profile, owner := liveEpochBegin(t, c, handed, pubA.GetBinding().GetId(), "epoch-replacement-authority-test")
	require.Greater(t, started.GetSequence(), appA.GetSequence())
	require.NotEqual(t, appA.GetCaptureEpoch(), started.GetCaptureEpoch())
	_, err = api.NewSourceServiceClient(admin).Live(ctx, refA)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Begin alone is not an applied capture")
	newApplication := liveEpochACK(t, owner, target, started, profile)
	liveEpochView(t, admin, pubA.GetBinding().GetSource().GetId(), original.GetId(), newApplication.GetCaptureEpoch())
}

func TestLiveEpochManagedStarvationKeepsPinnedCeilingAndLegacyAutoRaise(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "epoch-starvation")
	set, err := api.NewSetServiceClient(admin).Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), AutoRaise: z.Ptr(true), DateUpdated: set.GetDateUpdated()}.Build())
	require.NoError(t, err)
	host := sourceCatalogAdopt(t, c, admin, set, "epoch-starvation-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "epoch-starvation-lan", nil), host)
	pub, app, owner := liveEpochApply(t, c, admin, a, "epoch-managed")
	sources := api.NewSourceServiceClient(admin)
	managed, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: pub.GetBinding().GetSource().GetId()}.Build()}.Build())
	require.NoError(t, err)
	legacy, err := sources.Add(ctx, api.SourceAddRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: "epoch-legacy", Profile: api.SegmentProfile_builder{MaxBitrate: 1_000_000, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: z.Ptr(true)}.Build())
	require.NoError(t, err)
	response, err := api.NewProducerServiceClient(owner).Heartbeat(ctx, api.ProducerHeartbeatRequest_builder{Sources: []*api.SourceReport{api.SourceReport_builder{SourceId: managed.GetId(), CaptureEpoch: app.GetCaptureEpoch(), Episodes: 50, SecondsTotal: 1000}.Build(), api.SourceReport_builder{SourceId: legacy.GetId(), Episodes: 50, SecondsTotal: 1000}.Build()}}.Build())
	require.NoError(t, err)
	got, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: managed.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, managed.GetProfile().GetMaxBitrate(), got.GetProfile().GetMaxBitrate(), "a managed application cannot be silently rescaled by starvation")
	gotLegacy, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: legacy.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Greater(t, gotLegacy.GetProfile().GetMaxBitrate(), legacy.GetProfile().GetMaxBitrate())
	managedSuggestion, legacySuggestion := false, false
	for _, s := range response.GetSuggestions() {
		if bytes.Equal(s.GetSourceId(), managed.GetId()) {
			managedSuggestion = true
			require.False(t, s.GetApplied())
		}
		if bytes.Equal(s.GetSourceId(), legacy.GetId()) {
			legacySuggestion = true
			require.True(t, s.GetApplied())
		}
	}
	require.True(t, managedSuggestion)
	require.True(t, legacySuggestion)
}
