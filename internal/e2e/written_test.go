package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
)

// TestBacklogAfterOutage's twin under `retain: written` (§12.2, §16): while
// a node takes the parts, a source holds no more than about one part in
// RAM whatever its segment length; when the node dies under a segment
// whose parts it had, that segment is cut short at the node's offset
// rather than kept or moved, nothing is reported lost, and the node back
// makes an incomplete object of what it holds (§15). The segments after
// it wait whole, and drain when the node returns.
func TestBacklogAfterOutageWritten(t *testing.T) {
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.ts"))
	require.NoError(t, err)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ops := c.dialCluster("@cluster/ops")

	// Segments of a few MB, so one spans many parts.
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "parts", Version: 1,
		Bounds: api.UploadBounds_builder{MinObject: 1 << 20, TargetObject: 4 << 20, MaxObject: 16 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewUploadPolicyServiceClient(ops).Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

	// The built-in node's device is quarantined: only node B takes writes.
	sinks := api.NewSinkServiceClient(ops)
	vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, vs.GetItems(), 1)
	_, err = api.NewDeviceServiceClient(ops).Quarantine(ctx, api.DeviceQuarantineRequest_builder{
		Ref: api.DeviceRef_builder{Id: vs.GetItems()[0].GetDevice().GetId()}.Build(), Reason: "the drill",
	}.Build())
	require.NoError(t, err)
	stateB := filepath.Join(t.TempDir(), "node-b")
	sinkB := filepath.Join(t.TempDir(), "sink-b")
	nodeB, stopB := c.startNodeAt("b", stateB, sinkB)
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "node B's sink is attached")

	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "written",
	}.Build())
	require.NoError(t, err)
	const part = 256 << 10
	p, err := producer.New(producer.Config{
		StateDir:          filepath.Join(t.TempDir(), "producer"),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		Retain:            producer.RetainWritten,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		// The shortest abandon timeout there is, so the node back finalizes
		// the cut segment within the test.
		IdleTimeout:    10 * time.Second,
		AbandonTimeout: time.Minute,
		Buffer:         64 << 20,
		Upload:         producer.UploadConfig{ResumeTimeout: 5 * time.Second, Part: part},
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	require.Eventually(t, func() bool { return p.Stats().Stored >= 1 }, 60*time.Second, 500*time.Millisecond, "recording to node B")

	// Bounded RAM: the segment is 4 MB, the parts 256 KiB, and what is
	// held stays around one part.
	for range 20 {
		require.Less(t, p.Stats().Held, int64(3*part), "one part and a tail, not the segment: %+v", p.Stats())
		time.Sleep(100 * time.Millisecond)
	}

	// The outage catches a segment early on, with parts on B already.
	require.Eventually(t, func() bool {
		r := p.Stats().Released

		return r > 0 && r < 2<<20
	}, 60*time.Second, 20*time.Millisecond, "a segment with parts on B")
	stopB()
	before := p.Stats().Stored

	// That segment cannot move: it is cut short at B's offset once B stays
	// silent past resume_timeout. Nothing is lost, and what follows waits.
	require.Eventually(t, func() bool { return p.Stats().Cut == 1 }, 30*time.Second, 200*time.Millisecond, "cut short: %+v", p.Stats())
	time.Sleep(20 * time.Second)
	mid := p.Stats()
	require.Equal(t, int64(1), mid.Cut)
	require.Equal(t, int64(0), mid.Lost, "nothing given up within the budget")
	require.Equal(t, before, mid.Stored, "nothing stored while every sink is out of reach")

	// B is back: the backlog drains, and B's abandon rule makes an
	// incomplete object of the cut segment, with the size it had.
	c.startNodeAt("b", stateB, sinkB)
	require.Eventually(t, func() bool {
		st := p.Stats()

		return st.Stored >= before+2 && st.Lost == 0
	}, 120*time.Second, 500*time.Millisecond, "the backlog is stored: %+v", p.Stats())
	objects := api.NewObjectServiceClient(admin)
	require.Eventually(t, func() bool {
		list, err := objects.List(ctx, api.ObjectListRequest_builder{
			Filters: []*api.ObjectFilter{api.ObjectFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    200,
		}.Build())
		if err != nil {
			return false
		}
		for _, o := range list.GetItems() {
			require.NotEqual(t, api.ObjectState_OBJECT_STATE_LOST, o.GetState())
			if o.GetState() == api.ObjectState_OBJECT_STATE_COMMITTED && o.GetIncomplete() && o.GetSize() >= part {
				return true
			}
		}

		return false
	}, 150*time.Second, time.Second, "the cut segment is an incomplete object of at least a part")
	require.Equal(t, int64(0), p.Stats().Lost)
}
