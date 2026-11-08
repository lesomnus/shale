package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The most recently applied ticket remains useful audit state while many
// reconnects have not produced a new ACK. That pointer must not let an evicted
// historical ticket obtain fresh storage authorization beyond the bounded log.
func TestProducerHistoricalDrainEvictsOldAppliedTicket(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "history-eviction")
	host := sourceCatalogAdopt(t, c, admin, set, "history-eviction-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "history-eviction-lan", nil), host)
	pub, applied, owner := liveEpochApply(t, c, admin, a, "history-eviction-camera")
	apps := api.NewCameraAssignmentServiceClient(owner)
	laminae := api.NewLaminaServiceClient(owner)
	sref := api.SourceRef_builder{Id: pub.GetBinding().GetSource().GetId()}.Build()
	request := api.LaminaAllocateRequest_builder{Source: sref, CaptureEpoch: applied.GetCaptureEpoch(), DateStarted: applied.GetDateStarted()}.Build()
	var allocation *api.Allocation
	require.Eventually(t, func() bool {
		v, err := laminae.Allocate(ctx, request)
		if err != nil || len(v.GetCandidates()) == 0 {
			return false
		}
		allocation = v
		return true
	}, 10*time.Second, 100*time.Millisecond)
	current := applied
	var retained *api.CameraApplication
	for seq := 2; seq <= 18; seq++ {
		var err error
		current, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: applied.GetTarget(), ExpectedSequence: current.GetSequence(), IntentKey: fmt.Sprintf("restart-%d", seq)}.Build())
		require.NoError(t, err)
		if seq == 2 {
			retained = current
		}
	}
	b, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, b.GetPreviousApplications(), 16)
	require.Equal(t, applied.GetCaptureEpoch(), b.GetAppliedApplication().GetCaptureEpoch(), "audit state may still name the last real ACK")
	for _, old := range b.GetPreviousApplications() {
		require.NotEqual(t, applied.GetCaptureEpoch(), old.GetCaptureEpoch())
		require.True(t, old.GetDrainDeadline().AsTime().After(time.Now()), "this is eviction, not expiry")
	}
	_, err = laminae.Allocate(ctx, request)
	require.Equal(t, codes.Aborted, status.Code(err), "the stale applied audit pointer does not bypass bounded history")
	_, err = laminae.Renew(ctx, api.LaminaRenewRequest_builder{Ref: api.LaminaRef_builder{Id: allocation.GetLaminaId()}.Build(), Attempt: api.AttemptRef_builder{Id: allocation.GetCandidates()[0].GetAttemptId()}.Build()}.Build())
	require.Equal(t, codes.Aborted, status.Code(err))
	_, err = laminae.Reallocate(ctx, api.LaminaReallocateRequest_builder{Ref: api.LaminaRef_builder{Id: allocation.GetLaminaId()}.Build()}.Build())
	require.Equal(t, codes.Aborted, status.Code(err))
	producerAPI := api.NewProducerServiceClient(owner)
	_, err = producerAPI.Heartbeat(ctx, api.ProducerHeartbeatRequest_builder{Sources: []*api.SourceReport{api.SourceReport_builder{SourceId: sref.GetId(), CaptureEpoch: applied.GetCaptureEpoch(), InputUp: true, FrameRate: 10}.Build()}}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "eviction also ends the old report's known-drain status")
	_, err = laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{Source: sref, CaptureEpoch: retained.GetCaptureEpoch(), DateStarted: retained.GetDateStarted()}.Build())
	require.NoError(t, err, "a retained, unexpired reconnect ticket can still finish its old bytes")
	_, err = producerAPI.Heartbeat(ctx, api.ProducerHeartbeatRequest_builder{Sources: []*api.SourceReport{api.SourceReport_builder{SourceId: sref.GetId(), CaptureEpoch: retained.GetCaptureEpoch(), InputUp: true, FrameRate: 10}.Build()}}.Build())
	require.NoError(t, err)
	updated, err := api.NewProducerServiceClient(admin).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: host.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, updated.GetStatus().GetSources(), 1)
	require.False(t, updated.GetStatus().GetSources()[0].GetInputUp(), "retained old frames never count as current ingest")
	require.Zero(t, updated.GetStatus().GetSources()[0].GetFrameRate())
}

// This tests CP authorization and frozen limits, without claiming synthetic
// metadata is a real recorded/decoded clip. The separate lifecycle test uploads
// actual camera bytes through the same path.
func TestProducerHistoricalDrainUsesFrozenEpochLimits(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "historical-drain")
	host := sourceCatalogAdopt(t, c, admin, set, "historical-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "historical-lan", nil), host)
	prepared := sourceCatalogPrepare(t, admin, a, "historical-camera", "rtsp://192.0.2.10/main")
	pub, _ := sourceCatalogPublish(t, admin, a, prepared)
	conn := c.dial(sourceCatalogID(t, host.GetId()))
	apps := api.NewCameraAssignmentServiceClient(conn)
	profile := api.SegmentProfile_builder{MaxBitrate: 2_128_000, DurationSeconds: 10, KeyframeIntervalMs: 1000}.Build()
	_, err := api.NewSetServiceClient(conn).Negotiate(ctx, api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision()}.Build(), Sources: []*api.SourceProposal{api.SourceProposal_builder{Source: api.SourceRef_builder{Id: pub.GetBinding().GetSource().GetId()}.Build(), Profile: profile, ContentType: "video/mp4", LiveCapable: true}.Build()}}.Build())
	require.NoError(t, err)
	target := api.CameraApplyTarget_builder{AssignmentId: a.GetId(), BindingId: pub.GetBinding().GetId(), SourceId: pub.GetBinding().GetSource().GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision(), SpecRevision: pub.GetCatalogRevision()}.Build()
	first, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, IntentKey: "before-reconnect"}.Build())
	require.NoError(t, err)
	_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: first.GetSequence(), IntentKey: "after-reconnect"}.Build())
	require.NoError(t, err)
	bindings := api.NewCameraBindingServiceClient(admin)
	bref := api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()
	b, err := bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: bref}.Build())
	require.NoError(t, err)
	require.Len(t, b.GetPreviousApplications(), 1)
	_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: b.GetStartingApplication().GetSequence(), IntentKey: "before-reconnect"}.Build())
	require.Equal(t, codes.Aborted, status.Code(err), "a known retired Begin intent cannot become a new capture")
	b, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: bref, ExpectedVersion: b.GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	sources := api.NewSourceServiceClient(admin)
	sref := api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()
	source, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: sref}.Build())
	require.NoError(t, err)
	source, err = sources.Patch(ctx, api.SourcePatchRequest_builder{Ref: sref, Profile: api.SegmentProfile_builder{MaxBitrate: 128_000, DurationSeconds: 1, KeyframeIntervalMs: 1000}.Build(), DateUpdated: source.GetDateUpdated()}.Build())
	require.NoError(t, err)
	laminae := api.NewLaminaServiceClient(conn)
	request := api.LaminaAllocateRequest_builder{Source: sref, CaptureEpoch: first.GetCaptureEpoch(), DateStarted: first.GetDateStarted()}.Build()
	var allocation *api.Allocation
	require.Eventually(t, func() bool {
		v, err := laminae.Allocate(ctx, request)
		if err != nil || len(v.GetCandidates()) == 0 {
			return false
		}
		allocation = v
		return true
	}, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, first.GetCaptureEpoch(), allocation.GetCaptureEpoch())
	require.True(t, proto.Equal(first.GetSegmentProfile(), allocation.GetProfile()), "unallocated old bytes use their ticket's frozen profile after new Source metadata shrank")
	require.Greater(t, allocation.GetMaxLength(), core.MaxLength(source.GetProfile()))
	claims, err := token.Parse(allocation.GetCandidates()[0].GetToken())
	require.NoError(t, err)
	require.Equal(t, first.GetCaptureEpoch(), claims.GetRecord().GetCaptureEpoch())
	require.Equal(t, allocation.GetMaxLength(), claims.GetMaxLength())
	bad := proto.Clone(request).(*api.LaminaAllocateRequest)
	bad.SetCaptureEpoch("")
	_, err = laminae.Allocate(ctx, bad)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a managed Source never uses empty legacy epoch authorization")
	bad.SetCaptureEpoch("00000000000000000000000000000000")
	_, err = laminae.Allocate(ctx, bad)
	require.Equal(t, codes.Aborted, status.Code(err))
	bad.SetCaptureEpoch(first.GetCaptureEpoch())
	bad.SetDateStarted(timestamppb.New(b.GetPreviousApplications()[0].GetDrainCutoff().AsTime().Add(6 * time.Second)))
	_, err = laminae.Allocate(ctx, bad)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "historical drain cannot allocate media captured after retirement")
	// Simulate time passing only in the private test store. Public generic
	// Binding.Patch remains denied and cannot choose its own drain deadline.
	history := []*api.CameraApplication{proto.Clone(b.GetPreviousApplications()[0]).(*api.CameraApplication)}
	history[0].SetDrainDeadline(timestamppb.New(time.Now().Add(-time.Second)))
	_, err = c.running.CP.Ungated.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: bref, PreviousApplications: history, DateUpdatedForce: z.Ptr(true)}.Build())
	require.NoError(t, err)
	_, err = laminae.Allocate(ctx, request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = laminae.Renew(ctx, api.LaminaRenewRequest_builder{Ref: api.LaminaRef_builder{Id: allocation.GetLaminaId()}.Build(), Attempt: api.AttemptRef_builder{Id: allocation.GetCandidates()[0].GetAttemptId()}.Build()}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "expiry cannot be bypassed by renewing an already allocated old row")
	_, err = laminae.Reallocate(ctx, api.LaminaReallocateRequest_builder{Ref: api.LaminaRef_builder{Id: allocation.GetLaminaId()}.Build()}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
