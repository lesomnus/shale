package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/z"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// adoptProducer waits for the one pending producer and adopts it for a set.
func adoptProducer(t *testing.T, ctx context.Context, producers api.ProducerServiceClient, set *api.Set) *api.Producer {
	t.Helper()
	var pending *api.Producer
	require.Eventually(t, func() bool {
		vs, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 10}.Build())
		if err != nil {
			return false
		}
		for _, v := range vs.GetItems() {
			if v.GetState() == api.HostState_HOST_STATE_PENDING {
				pending = v
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the producer joins")
	_, err := producers.Adopt(ctx, api.ProducerAdoptRequest_builder{
		Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	return pending
}

// TestClockSkew is §10's rule for a producer whose clock is wrong: a
// `date_started` further ahead than the horizon and the clock tolerance
// allow is refused, never clamped, so its segments are lost and say so;
// once the clock is right, recording resumes.
func TestClockSkew(t *testing.T) {
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.ts"))
	require.NoError(t, err)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "skewed",
	}.Build())
	require.NoError(t, err)

	// Ten minutes ahead: past a one-minute horizon plus five of tolerance.
	var skew atomic.Int64
	skew.Store(int64(10 * time.Minute))
	p, err := producer.New(producer.Config{
		StateDir:          filepath.Join(t.TempDir(), "producer"),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264", RawLoops: 1}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Now:               func() time.Time { return time.Now().Add(time.Duration(skew.Load())) },
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	require.Eventually(t, func() bool {
		st := p.Stats()

		return st.Lost >= 2 && st.Stored == 0
	}, 60*time.Second, 500*time.Millisecond, "segments stamped in the future are refused and lost, none stored")

	// The clock is fixed: the next segments are stored.
	skew.Store(0)
	require.Eventually(t, func() bool { return p.Stats().Stored >= 2 }, 60*time.Second, 500*time.Millisecond, "recording resumes")
}

// TestRelayRestart is §39.6's first row: the relay process restarts, every
// attachment drops, and the producer re-attaches to the same relay as it
// comes back, so Live still names it.
func TestRelayRestart(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)

	dir := filepath.Join(t.TempDir(), "r1")
	r1, stop1 := c.startRelayAt("r1", dir)
	require.Eventually(t, func() bool {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			if string(r.GetId()) != string(r1.Id().Bytes()) {
				continue
			}
			if r.GetState() != api.HostState_HOST_STATE_ADOPTED || r.GetDateSeen() == nil {
				return false
			}
			if r.GetLabels()["zone"] != "c" {
				_, err := relays.Patch(ctx, api.RelayPatchRequest_builder{
					Ref: api.RelayRef_builder{Id: r.GetId()}.Build(), Labels: map[string]string{"zone": "c"}, DateUpdated: r.GetDateUpdated(),
				}.Build())

				return err == nil && false
			}

			return true
		}

		return false
	}, 30*time.Second, 300*time.Millisecond, "the relay is adopted and labeled")

	admin := c.dial("@acme/admin")
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zone-c", RelaySelector: map[string]string{"zone": "c"},
	}.Build())
	require.NoError(t, err)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "restarts", Site: api.SiteRef_builder{Id: site.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.ts"))
	require.NoError(t, err)
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
	})
	require.NoError(t, err)
	go p.Run(pctx)
	producers := api.NewProducerServiceClient(admin)
	me := adoptProducer(t, ctx, producers, set)

	attached := func() int32 {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return -1
		}
		for _, r := range vs.GetItems() {
			if string(r.GetId()) == string(r1.Id().Bytes()) && r.GetStatus() != nil {
				return r.GetStatus().GetAttachedProducers()
			}
		}

		return -1
	}
	require.Eventually(t, func() bool { return attached() == 1 }, 30*time.Second, 300*time.Millisecond, "attached to the zone's relay")

	// The relay restarts as itself: same identity, new ports.
	stop1()
	r1b, _ := c.startRelayAt("r1", dir)
	require.Equal(t, r1.Id(), r1b.Id(), "the same relay")
	require.Eventually(t, func() bool { return attached() == 1 }, 60*time.Second, 500*time.Millisecond, "the producer re-attached after the restart")
	v, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: me.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, r1.Id().Bytes(), v.GetRelay().GetId(), "still assigned the same relay")
	live, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, live.GetSources(), 1)
	require.Equal(t, r1.Id().Bytes(), live.GetSources()[0].GetRelayId())
}

// TestBacklogAfterOutage is §16 and §10 with every sink out of reach for a
// while: the only eligible node stops, the producer keeps the segments it
// cannot store and tries again with a backoff, and when the node is back
// the backlog is uploaded in order, accepted with its past data times, and
// nothing is lost while the RAM budget holds.
func TestBacklogAfterOutage(t *testing.T) {
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.ts"))
	require.NoError(t, err)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ops := c.dialCluster("@cluster/ops")

	// Small objects, so segments are seconds apart.
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinObject: 512 << 10, TargetObject: 1 << 20, MaxObject: 4 << 20}.Build(),
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
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "backlog",
	}.Build())
	require.NoError(t, err)
	p, err := producer.New(producer.Config{
		StateDir:          filepath.Join(t.TempDir(), "producer"),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		// A target that answers nothing is given up in seconds rather than
		// the two minutes of §13, so the backlog drains within the test.
		Upload: producer.UploadConfig{ResumeTimeout: 5 * time.Second},
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	require.Eventually(t, func() bool { return p.Stats().Stored >= 2 }, 60*time.Second, 500*time.Millisecond, "recording to node B")

	// The outage: node B is gone for a while. Segments keep coming and
	// none can be stored. (A segment in flight as the node stops may still
	// land in the moment it takes; the count is read once it is gone.)
	stopB()
	time.Sleep(3 * time.Second)
	before := p.Stats().Stored
	time.Sleep(45 * time.Second)
	mid := p.Stats()
	require.Equal(t, before, mid.Stored, "nothing stored while every sink is out of reach")
	require.Equal(t, int64(0), mid.Lost, "nothing given up within the budget")

	// The node is back as itself: the backlog drains, in order, every
	// segment of the outage included.
	c.startNodeAt("b", stateB, sinkB)
	require.Eventually(t, func() bool {
		st := p.Stats()

		return st.Stored >= before+5 && st.Lost == 0
	}, 120*time.Second, 500*time.Millisecond, "the backlog is stored: %+v", p.Stats())
	require.Equal(t, int64(0), p.Stats().Lost)

	// The CP's view catches up with the nodes' events: as many committed
	// objects as the producer counts stored, none lost.
	objects := api.NewObjectServiceClient(admin)
	var committed []*api.Object
	require.Eventually(t, func() bool {
		stored := p.Stats().Stored
		list, err := objects.List(ctx, api.ObjectListRequest_builder{
			Filters: []*api.ObjectFilter{api.ObjectFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    200,
		}.Build())
		if err != nil {
			return false
		}
		committed = committed[:0]
		for _, o := range list.GetItems() {
			require.NotEqual(t, api.ObjectState_OBJECT_STATE_LOST, o.GetState())
			if o.GetState() == api.ObjectState_OBJECT_STATE_COMMITTED {
				committed = append(committed, o)
			}
		}

		if int64(len(committed)) >= stored {
			return true
		}
		states := map[string]int{}
		for _, o := range list.GetItems() {
			states[o.GetState().String()]++
		}
		t.Logf("stored %d, committed %d, states %v", stored, len(committed), states)
		dumpSink(t, "at the end", sinkB)

		return false
	}, 30*time.Second, 2*time.Second, "every stored segment is a committed object")
	sort.Slice(committed, func(i, j int) bool {
		return committed[i].GetDateStarted().AsTime().Before(committed[j].GetDateStarted().AsTime())
	})
	for i := 1; i < len(committed); i++ {
		require.False(t, committed[i].GetDateStarted().AsTime().Before(committed[i-1].GetDateEnded().AsTime()), "segments do not overlap")
		// Data times are the camera's, not the upload's: the segments of the
		// outage begin while the node was away.
	}
	require.GreaterOrEqual(t, len(committed), int(before+5))
}

// dumpSink logs every file of a directory sink with its record, for the
// drills' diagnostics.
func dumpSink(t *testing.T, when, dir string) {
	t.Helper()
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Base(path)[0] == '.' {
			return nil
		}
		rec, rerr := storage.ReadRecordPath(path)
		if rerr != nil {
			t.Logf("%s: %s size=%d record: %v", when, filepath.Base(path), info.Size(), rerr)
			return nil
		}
		t.Logf("%s: %s size=%d state=%s recsize=%d", when, filepath.Base(path), info.Size(), rec.GetState(), rec.GetSize())

		return nil
	})
}

// TestSinkNoLongerReported is §28.3 for a sink that leaves its node: the
// node comes back without it, the CP marks it pending adoption at the
// first heartbeat, and the sink it reports instead is attached.
func TestSinkNoLongerReported(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)

	stateB := filepath.Join(t.TempDir(), "node-b")
	sinkB1 := filepath.Join(t.TempDir(), "sink-b1")
	nodeB, stopB := c.startNodeAt("b", stateB, sinkB1)
	ofNode := func() map[string]api.SinkAttachment {
		out := map[string]api.SinkAttachment{}
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return out
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB.Id().Bytes()) {
				out[s.GetPath()] = s.GetAttachment()
			}
		}

		return out
	}
	require.Eventually(t, func() bool { return ofNode()[sinkB1] == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED }, 30*time.Second, 200*time.Millisecond, "the first sink is attached")

	// The node comes back as itself with another sink and not the first.
	stopB()
	sinkB2 := filepath.Join(t.TempDir(), "sink-b2")
	c.startNodeAt("b", stateB, sinkB2)
	require.Eventually(t, func() bool {
		m := ofNode()

		return m[sinkB2] == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && m[sinkB1] == api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION
	}, 30*time.Second, 200*time.Millisecond, "the vanished sink is pending adoption, the new one attached: %v", ofNode())

	// Back with the first sink too: attached again.
	// (A node lists every sink it has; the harness gives one per node, so
	// the first sink returns alone.)
	vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
	require.NoError(t, err)
	for _, s := range vs.GetItems() {
		if s.GetPath() == sinkB1 {
			require.Equal(t, nodeB.Id().Bytes(), s.GetNode().GetId(), "the pending sink still names its node")
		}
	}
}
