package e2e_test

import (
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These are authority/protocol tests, not claims of observed camera media.
// Actual Producer read-loop/recording proof is tested by the lifecycle fixture.
func TestProducerApplicationTicketsFenceDelayedAcknowledgments(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "application")
	host := sourceCatalogAdopt(t, c, admin, set, "application-owner")
	network := cameraNetwork(t, admin, "application-lan", nil)
	a := sourceCatalogAssignment(t, admin, network, host)
	prepared := sourceCatalogPrepare(t, admin, a, "door", "rtsp://192.0.2.10/main")
	pub, _ := sourceCatalogPublish(t, admin, a, prepared)
	owner := c.dial(sourceCatalogID(t, host.GetId()))
	apps := api.NewCameraAssignmentServiceClient(owner)
	_, err := api.NewSetServiceClient(owner).Negotiate(ctx, api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision()}.Build(), Sources: []*api.SourceProposal{api.SourceProposal_builder{Source: api.SourceRef_builder{Id: pub.GetBinding().GetSource().GetId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 1_128_000, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build()}}.Build())
	require.NoError(t, err)

	target := api.CameraApplyTarget_builder{AssignmentId: a.GetId(), BindingId: pub.GetBinding().GetId(), SourceId: pub.GetBinding().GetSource().GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision(), SpecRevision: pub.GetCatalogRevision()}.Build()
	for _, conn := range []*grpc.ClientConn{admin, exportReader(t, c, admin)} {
		_, err := api.NewCameraAssignmentServiceClient(conn).BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, IntentKey: "not-the-producer"}.Build())
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	first, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: 0, IntentKey: "first-process"}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(1), first.GetSequence())
	require.NotEmpty(t, first.GetCaptureEpoch())
	require.Nil(t, first.GetDateApplied())
	retry, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: 0, IntentKey: "first-process"}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(first, retry), "lost Begin response retries must not allocate a different incarnation")
	replayBinding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Empty(t, replayBinding.GetPreviousApplications(), "lost Begin replay never appends duplicate historical tickets")
	_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: 0, IntentKey: "delayed-stale-begin"}.Build())
	require.Equal(t, codes.Aborted, status.Code(err))
	second, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: first.GetSequence(), IntentKey: "second-process-same-spec"}.Build())
	require.NoError(t, err)
	require.Greater(t, second.GetSequence(), first.GetSequence())
	require.NotEqual(t, first.GetCaptureEpoch(), second.GetCaptureEpoch())
	now := time.Now()
	media := api.CameraRuntimeMedia_builder{Profile: prepared.request.GetSpec().GetProfile(), FirstMediaAt: timestamppb.New(now.Add(-time.Second)), LastMediaAt: timestamppb.New(now), Frames: 10, Keyframes: 1}.Build()
	ack := func(ticket *api.CameraApplication) *api.CameraAppliedRequest {
		return api.CameraAppliedRequest_builder{Target: target, Sequence: ticket.GetSequence(), CaptureEpoch: ticket.GetCaptureEpoch(), Media: media}.Build()
	}
	_, err = apps.Applied(ctx, ack(first))
	require.Equal(t, codes.Aborted, status.Code(err), "the old loop cannot acknowledge after a newer Begin")
	bad := ack(second)
	wrong := proto.Clone(media).(*api.CameraRuntimeMedia)
	wrong.SetProfile(proto.Clone(media.GetProfile()).(*api.CameraCaptureProfile))
	wrong.GetProfile().SetWidth(80)
	bad.SetMedia(wrong)
	_, err = apps.Applied(ctx, bad)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a valid IDR of the old resolution does not apply the new specification")
	state, err := apps.Application(ctx, api.CameraApplicationRequest_builder{Target: target}.Build())
	require.NoError(t, err)
	require.Nil(t, state.GetApplied())
	applied, err := apps.Applied(ctx, ack(second))
	require.NoError(t, err)
	require.NotNil(t, applied.GetDateApplied())
	repeated, err := apps.Applied(ctx, ack(second))
	require.NoError(t, err)
	require.True(t, proto.Equal(applied, repeated), "duplicate ACK never refreshes the application time")
	third, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: second.GetSequence(), IntentKey: "restart-without-spec-change"}.Build())
	require.NoError(t, err)
	require.Greater(t, third.GetSequence(), second.GetSequence())
	_, err = apps.Applied(ctx, ack(second))
	require.Equal(t, codes.Aborted, status.Code(err), "a previously successful ACK cannot overwrite a current restarting ticket")
	current, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, third.GetCaptureEpoch(), current.GetStartingApplication().GetCaptureEpoch())
	require.Equal(t, second.GetCaptureEpoch(), current.GetAppliedApplication().GetCaptureEpoch())
}

func TestProducerManagedAdmissionPreservesExistingFeedsAndHistory(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "admission")
	host := sourceCatalogAdopt(t, c, admin, set, "admission-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "admission-lan", nil), host)
	first := sourceCatalogPrepare(t, admin, a, "door", "rtsp://192.0.2.10/main")
	pub, _ := sourceCatalogPublish(t, admin, a, first)
	owner := c.dial(sourceCatalogID(t, host.GetId()))
	sets := api.NewSetServiceClient(owner)
	proposal := func(b *api.CameraBinding) *api.SourceProposal {
		return api.SourceProposal_builder{Source: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 1_128_000, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build()
	}
	request := api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision()}.Build(), Sources: []*api.SourceProposal{proposal(pub.GetBinding())}, Preview: true}.Build()
	_, err := sets.Negotiate(ctx, request)
	require.NoError(t, err)
	sources := api.NewSourceServiceClient(admin)
	ref := api.SourceRef_builder{Id: pub.GetBinding().GetSource().GetId()}.Build()
	before, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Zero(t, before.GetProfile().GetMaxBitrate(), "preview does not rewrite capture metadata")
	request.SetPreview(false)
	_, err = sets.Negotiate(ctx, request)
	require.NoError(t, err)
	before, err = sources.Get(ctx, api.SourceGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(1_128_000), before.GetProfile().GetMaxBitrate())
	set, err = api.NewSetServiceClient(admin).Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = api.NewSetServiceClient(admin).Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), MaxBitrateTotal: z.Ptr(int64(2_000_000)), DateUpdated: set.GetDateUpdated()}.Build())
	require.NoError(t, err)
	second := sourceCatalogPrepare(t, admin, a, "lobby", "rtsp://192.0.2.11/main")
	pub2, _ := sourceCatalogPublish(t, admin, a, second)
	request.GetCatalog().SetCatalogRevision(pub2.GetCatalogRevision())
	request.SetSources([]*api.SourceProposal{proposal(pub.GetBinding()), proposal(pub2.GetBinding())})
	_, err = sets.Negotiate(ctx, request)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	unchanged, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(before.GetProfile(), unchanged.GetProfile()), "new capacity cannot scale down an already copying feed")
	stopped, err := api.NewCameraBindingServiceClient(admin).SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub2.GetBinding().GetId()}.Build(), ExpectedVersion: pub2.GetBinding().GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	snapshot, err := api.NewCameraAssignmentServiceClient(owner).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
	require.NoError(t, err)
	request.GetCatalog().SetCatalogRevision(snapshot.GetRevision())
	request.SetSources([]*api.SourceProposal{proposal(pub.GetBinding())})
	_, err = sets.Negotiate(ctx, request)
	require.NoError(t, err, "a stopped historical Source is not charged as an active camera")
	_, err = sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: stopped.GetSource().GetId()}.Build()}.Build())
	require.NoError(t, err, "collection stop preserves Source/history identity")
}

// A stale nonzero Source profile is not proof that its newer desired capture
// passed the strict full-catalog transaction. Only the exact admitted per-source
// revision/generation may obtain a new ticket; an unchanged camera's reconnect
// remains possible while an unrelated new camera is waiting for capacity.
func TestProducerApplicationRequiresAdmittedSpecification(t *testing.T) {
	for _, larger := range []bool{false, true} {
		t.Run(map[bool]string{false: "same ceiling new endpoint", true: "larger rejected ceiling"}[larger], func(t *testing.T) {
			c := cameraControlCluster(t)
			ctx := t.Context()
			admin := c.dial("@acme/admin")
			set := sourceCatalogSet(t, admin, "admission-ticket")
			host := sourceCatalogAdopt(t, c, admin, set, "admission-ticket-owner")
			a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "admission-ticket-lan", nil), host)
			first, applied, owner := liveEpochApply(t, c, admin, a, "admission-ticket-camera")
			sets := api.NewSetServiceClient(admin)
			set, err := sets.Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
			require.NoError(t, err)
			_, err = sets.Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), DateUpdated: set.GetDateUpdated(), MaxBitrateTotal: z.Ptr(int64(1_500_000))}.Build())
			require.NoError(t, err)
			profile := proto.Clone(first.GetBinding().GetDesired().GetProfile()).(*api.CameraCaptureProfile)
			ceiling := int64(1_128_000)
			if larger {
				profile.SetMaxBitrateBps(3_000_000)
				ceiling = 3_128_000
			}
			pub := lifecyclePublish(t, admin, a, first.GetBinding(), first.GetJob(), "rtsp://192.0.2.11/new-endpoint", profile, nil, "admission-ticket-replacement")
			target := api.CameraApplyTarget_builder{AssignmentId: a.GetId(), BindingId: pub.GetBinding().GetId(), SourceId: pub.GetBinding().GetSource().GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision(), SpecRevision: pub.GetBinding().GetDesired().GetRevision()}.Build()
			request := api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision()}.Build(), Sources: []*api.SourceProposal{api.SourceProposal_builder{Source: api.SourceRef_builder{Id: target.GetSourceId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: ceiling, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build()}}.Build()
			apps := api.NewCameraAssignmentServiceClient(owner)
			if larger {
				_, err = api.NewSetServiceClient(owner).Negotiate(ctx, request)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			}
			_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: applied.GetSequence(), IntentKey: "unadmitted-replacement"}.Build())
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "old negotiated storage metadata cannot authorize a newer desired specification")
			binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
			require.NoError(t, err)
			require.Equal(t, applied.GetCaptureEpoch(), binding.GetStartingApplication().GetCaptureEpoch())
			if larger {
				set, err = sets.Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
				require.NoError(t, err)
				_, err = sets.Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), DateUpdated: set.GetDateUpdated(), MaxBitrateTotal: z.Ptr(int64(10_000_000))}.Build())
				require.NoError(t, err)
			}
			_, err = api.NewSetServiceClient(owner).Negotiate(ctx, request)
			require.NoError(t, err)
			started, err := apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: applied.GetSequence(), IntentKey: "admitted-replacement"}.Build())
			require.NoError(t, err)
			require.NotEqual(t, applied.GetCaptureEpoch(), started.GetCaptureEpoch())
			require.Equal(t, ceiling, started.GetSegmentProfile().GetMaxBitrate())
			bindings := api.NewCameraBindingServiceClient(admin)
			binding, err = bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: pub.GetBinding().GetId()}.Build()}.Build())
			require.NoError(t, err)
			require.Equal(t, target.GetSpecRevision(), binding.GetAdmittedSpecRevision())
			require.Equal(t, target.GetAssignmentGeneration(), binding.GetAdmittedAssignmentGeneration())
			binding, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), ExpectedVersion: binding.GetDateUpdated(), Collect: false}.Build())
			require.NoError(t, err)
			require.Zero(t, binding.GetAdmittedSpecRevision())
			_, err = bindings.SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: binding.GetId()}.Build(), ExpectedVersion: binding.GetDateUpdated(), Collect: true}.Build())
			require.NoError(t, err)
			snapshot, err := apps.Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
			require.NoError(t, err)
			target.SetCatalogRevision(snapshot.GetRevision())
			_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: started.GetSequence(), IntentKey: "re-enabled-without-reservation"}.Build())
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "re-enabling collection must not reuse capacity released by stop")
			request.GetCatalog().SetCatalogRevision(snapshot.GetRevision())
			_, err = api.NewSetServiceClient(owner).Negotiate(ctx, request)
			require.NoError(t, err)
			_, err = apps.BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: started.GetSequence(), IntentKey: "re-enabled-after-reservation"}.Build())
			require.NoError(t, err)
		})
	}
}

func TestProducerApplicationUnchangedReconnectDuringRejectedCatalog(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "reconnect-budget")
	host := sourceCatalogAdopt(t, c, admin, set, "reconnect-budget-owner")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "reconnect-budget-lan", nil), host)
	first, applied, owner := liveEpochApply(t, c, admin, a, "reconnect-anchor")
	sets := api.NewSetServiceClient(admin)
	set, err := sets.Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = sets.Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), DateUpdated: set.GetDateUpdated(), MaxBitrateTotal: z.Ptr(int64(2_000_000))}.Build())
	require.NoError(t, err)
	input := "rtsp://192.0.2.12/oversized"
	other := lifecycleBind(t, admin, a, "reconnect-oversized", input)
	profile := proto.Clone(first.GetBinding().GetDesired().GetProfile()).(*api.CameraCaptureProfile)
	profile.SetMaxBitrateBps(3_000_000)
	pub := lifecyclePublish(t, admin, a, other, nil, input, profile, nil, "reconnect-pending-camera")
	var proposals []*api.SourceProposal
	for _, v := range []struct {
		id      []byte
		ceiling int64
	}{{first.GetBinding().GetSource().GetId(), 1_128_000}, {other.GetSource().GetId(), 3_128_000}} {
		proposals = append(proposals, api.SourceProposal_builder{Source: api.SourceRef_builder{Id: v.id}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: v.ceiling, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build())
	}
	_, err = api.NewSetServiceClient(owner).Negotiate(ctx, api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Catalog: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: pub.GetCatalogRevision()}.Build(), Sources: proposals}.Build())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	target := proto.Clone(applied.GetTarget()).(*api.CameraApplyTarget)
	target.SetCatalogRevision(pub.GetCatalogRevision())
	reconnected, err := api.NewCameraAssignmentServiceClient(owner).BeginApply(ctx, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: applied.GetSequence(), IntentKey: "unchanged-anchor-reconnect"}.Build())
	require.NoError(t, err, "an unrelated rejected capture does not revoke an unchanged camera's admitted specification")
	require.NotEqual(t, applied.GetCaptureEpoch(), reconnected.GetCaptureEpoch())
	require.Equal(t, applied.GetTarget().GetSpecRevision(), reconnected.GetTarget().GetSpecRevision())
}
