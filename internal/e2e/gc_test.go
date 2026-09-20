package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/server/core"
)

// TestGcReclaimsToTarget is §21's protocol end to end: a small sink fills
// past its low watermark, its objects are rescheduled to expire now, the
// node proposes, the CP approves, files go, events close the rows, and
// the round stops once free space reaches the target rather than
// deleting everything approvable.
func TestGcReclaimsToTarget(t *testing.T) {
	c := start(t)
	ctx := context.Background()

	const capacity = 4 << 20
	sinkDir := filepath.Join(t.TempDir(), "sink-gc")
	node, _ := c.startNodeAt("gc", filepath.Join(t.TempDir(), "node-gc"), sinkDir, func(cfg *storage.Config) {
		cfg.Sinks[0].Capacity = capacity
		cfg.GcInterval = time.Second
	})
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
	var sinkId []byte
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(node.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				sinkId = s.GetId()
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond)

	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	objects := api.NewObjectServiceClient(conn)
	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "gc"}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)

	// 13 objects of 310 KB on the small sink: free falls under the low
	// watermark (5%) but stays above critical (3%).
	body := make([]byte, 310_000)
	for i := range body {
		body[i] = byte(i * 7)
	}
	base := time.Now().Add(-600 * time.Hour)
	var ids [][]byte
	for i := range 13 {
		var al *api.Allocation
		var cand *api.Candidate
		for j := range 40 {
			a, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
				Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(base.Add(time.Duration(i*40+j) * time.Hour)),
			}.Build())
			require.NoError(t, err)
			for _, cd := range a.GetCandidates() {
				if string(cd.GetSinkId()) == string(sinkId) {
					al, cand = a, cd
				}
			}
			if al != nil {
				break
			}
		}
		require.NotNil(t, al, "a slot on the small sink")
		require.Equal(t, 201, putTo(t, cand, body, al.GetDateStarted().AsTime().Add(time.Minute)))
		ids = append(ids, al.GetObjectId())
	}
	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkId}.Build()}.Build())

		return err == nil && s.GetObjects() == 13 && s.GetPressure() == api.Pressure_PRESSURE_RECLAIM
	}, 20*time.Second, 200*time.Millisecond, "the sink is under pressure with every object indexed")

	// Nothing is approvable while every object is within retention.
	time.Sleep(3 * time.Second)
	for _, id := range ids {
		o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: id}.Build()}.Build())
		require.NoError(t, err)
		require.Equal(t, api.ObjectState_OBJECT_STATE_COMMITTED, o.GetState(), "retention holds")
	}

	// Expire them all: the dates reach the xattrs, the next round
	// proposes, and the CP approves until the target is met. In pages of
	// two (§20.3): a call whose deadline is nearer than the margin does one
	// page and says how many remain; the same request again does the rest,
	// and a third finds nothing left to change.
	saved := core.ReschedulePage
	core.ReschedulePage = 2
	t.Cleanup(func() { core.ReschedulePage = saved })
	expire := api.ObjectRescheduleRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(base.Add(-time.Hour)), To: timestamppb.New(time.Now()),
		DateExpired: timestamppb.New(time.Now().Add(-time.Minute)), Reason: "make room",
	}.Build()
	short, cancel := context.WithTimeout(ctx, 4*time.Second)
	res, err := objects.Reschedule(short, expire)
	cancel()
	require.NoError(t, err)
	require.Equal(t, int64(2), res.GetChanged(), "one page before the deadline")
	require.Equal(t, int64(11), res.GetRemaining())
	res, err = objects.Reschedule(ctx, expire)
	require.NoError(t, err)
	require.Equal(t, int64(11), res.GetChanged(), "the rest")
	require.Zero(t, res.GetRemaining())
	res, err = objects.Reschedule(ctx, expire)
	require.NoError(t, err)
	require.Zero(t, res.GetChanged(), "nothing selected twice")
	core.ReschedulePage = saved
	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkId}.Build()}.Build())

		return err == nil && s.GetPressure() == api.Pressure_PRESSURE_NORMAL && float64(s.GetFree()) >= 0.08*capacity
	}, 30*time.Second, 300*time.Millisecond, "free space is back at the target")

	deleted, kept := 0, 0
	require.Eventually(t, func() bool {
		deleted, kept = 0, 0
		for _, id := range ids {
			o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: id}.Build()}.Build())
			if err != nil {
				return false
			}
			switch o.GetState() {
			case api.ObjectState_OBJECT_STATE_DELETED:
				deleted++
			case api.ObjectState_OBJECT_STATE_COMMITTED:
				kept++
			default:
				return false
			}
		}

		return deleted > 0
	}, 20*time.Second, 300*time.Millisecond, "deletions closed their rows")
	require.Greater(t, kept, 6, "the round stopped at the target instead of deleting everything: %d deleted, %d kept", deleted, kept)
}

// putTo uploads a buffered body to one candidate and answers the status.
func putTo(t *testing.T, cand *api.Candidate, body []byte, ended time.Time) int {
	t.Helper()
	al := api.Allocation_builder{ObjectKey: cand.GetObjectKey(), Candidates: []*api.Candidate{cand}, DateStarted: timestamppb.New(ended.Add(-time.Minute))}.Build()

	return put(t, al, body, ended)
}
