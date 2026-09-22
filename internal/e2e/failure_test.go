package e2e_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// TestSinkCriticalRefusesWrites is §12.1's first table and §21.1: a sink
// with no room left refuses a new upload with 503 and a `Retry-After`,
// whatever token the producer holds, and the CP stops offering it.
func TestSinkCriticalRefusesWrites(t *testing.T) {
	// A sink that a few segments fill. Nothing here expires, so GC has
	// nothing to reclaim and the sink stays where it lands (§21.2).
	c := start(t, func(c *cmd.Config) { c.Storage.Sinks[0].Capacity = "2MiB" })
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)

	_, src, allocate := camera(t, ctx, conn, "full", nil)
	var held []*api.Allocation
	for i := 1; i <= 10; i++ {
		held = append(held, allocate(time.Duration(i)*time.Hour))
	}
	sinkId := held[0].GetCandidates()[0].GetSinkId()

	// Every token is good; the sink fills anyway, and the upload that finds
	// it critical is refused rather than half written.
	body := make([]byte, 512<<10)
	rand.Read(body)
	refusedAt := -1
	for i, al := range held {
		code, hdr := putAt(t, al, al.GetCandidates()[0], body, "?1")
		if code == http.StatusServiceUnavailable {
			require.Equal(t, "30", hdr.Get(storage.HdrRetryAfter), "a full sink says when to come back")
			refusedAt = i
			break
		}
		require.Equal(t, http.StatusCreated, code, "upload %d", i)
	}
	require.Greater(t, refusedAt, 0, "the sink took a few and then refused")
	code, _ := headAt(t, held[refusedAt].GetCandidates()[0])
	require.Equal(t, http.StatusNotFound, code, "the refused upload wrote nothing")

	// The CP hears it on the next heartbeat, and a sink at CRITICAL is no
	// longer a place to write (§11.1): with one sink there is nowhere left.
	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkId}.Build()}.Build())

		return err == nil && s.GetPressure() == api.Pressure_PRESSURE_CRITICAL
	}, 20*time.Second, 200*time.Millisecond, "the sink reports itself critical")
	require.Eventually(t, func() bool {
		_, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
			Source:      api.SourceRef_builder{Id: src.GetId()}.Build(),
			DateStarted: timestamppb.New(time.Now().Add(-30 * time.Hour)),
		}.Build())

		return err != nil
	}, 20*time.Second, 500*time.Millisecond, "no sink can take the write")
}

// TestDeviceDeclaredDead is §28.2: a device that is gone takes only what
// was on its own sinks. Those laminae are LOST, which the timeline says
// plainly, its sinks are retired, and every other device keeps working.
func TestDeviceDeclaredDead(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
	devices := api.NewDeviceServiceClient(ops)

	nodeB, _ := c.startNode("b", filepath.Join(t.TempDir(), "sink-b"))
	nodeC, _ := c.startNode("c", filepath.Join(t.TempDir(), "sink-c"))
	sinkOf := func(node []byte) *api.Sink {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return nil
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(node) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				return s
			}
		}

		return nil
	}
	require.Eventually(t, func() bool {
		return sinkOf(nodeB.Id().Bytes()) != nil && sinkOf(nodeC.Id().Bytes()) != nil
	}, 30*time.Second, 200*time.Millisecond, "both nodes have their sinks")
	sinkB, sinkC := sinkOf(nodeB.Id().Bytes()), sinkOf(nodeC.Id().Bytes())

	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "dead", nil)

	// One lamina on each of the two nodes.
	body := make([]byte, 64<<10)
	rand.Read(body)
	store := func(ago time.Duration, sink []byte) *api.Allocation {
		al := allocate(ago)
		for _, cd := range al.GetCandidates() {
			if string(cd.GetSinkId()) == string(sink) {
				code, _ := putAt(t, al, cd, body, "?1")
				require.Equal(t, http.StatusCreated, code)

				return al
			}
		}
		t.Fatal("the sink was not a candidate")

		return nil
	}
	onB := store(2*time.Hour, sinkB.GetId())
	onC := store(time.Hour, sinkC.GetId())
	stateOf := func(al *api.Allocation) api.LaminaState {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		if err != nil {
			return api.LaminaState_LAMINA_STATE_UNSPECIFIED
		}

		return o.GetState()
	}
	require.Eventually(t, func() bool {
		return stateOf(onB) == api.LaminaState_LAMINA_STATE_COMMITTED && stateOf(onC) == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 20*time.Second, 200*time.Millisecond, "both commit")

	// The disk under node B is gone for good.
	d, err := devices.DeclareDead(ctx, api.DeviceDeclareDeadRequest_builder{
		Ref: api.DeviceRef_builder{Id: sinkB.GetDevice().GetId()}.Build(), Reason: "the drill",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, api.DeviceHealth_DEVICE_HEALTH_DEAD, d.GetHealth())

	// What it held is lost, and said so: no rebuild, no restoration (§28.1).
	require.Eventually(t, func() bool { return stateOf(onB) == api.LaminaState_LAMINA_STATE_LOST }, 20*time.Second, 200*time.Millisecond, "the dead device's lamina is lost")
	ts := timelineAround(t, ctx, laminae, set, onB.GetDateStarted().AsTime())
	var lost bool
	for _, g := range ts.GetGaps() {
		if g.GetReason() == api.GapReason_GAP_REASON_LOST {
			lost = true
		}
	}
	require.True(t, lost, "the timeline says the span is lost")

	// Its sinks are retired and no longer offered.
	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkB.GetId()}.Build()}.Build())

		return err == nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_RETIRED
	}, 20*time.Second, 200*time.Millisecond, "the dead device's sink is retired")
	al := allocate(3 * time.Hour)
	for _, cd := range al.GetCandidates() {
		require.NotEqual(t, sinkB.GetId(), cd.GetSinkId(), "a dead device is not a candidate")
	}

	// Every other device kept working, and its laminae read as they were.
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, stateOf(onC))
	tsC := timelineAround(t, ctx, laminae, set, onC.GetDateStarted().AsTime())
	require.Len(t, tsC.GetLaminae(), 1)
	require.Equal(t, body, fetch(t, tsC.GetLaminae()[0].GetUrl()))
	require.NotNil(t, sinkOf(nodeC.Id().Bytes()), "node C's sink is untouched")
}

// TestProducerDown is §39.6's fourth row: a producer that goes takes its
// cameras with it, for viewers and for the recording alike. The relay it
// was attached to carries nothing, and no lamina is stored after it.
func TestProducerDown(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)
	laminae := api.NewLaminaServiceClient(admin)

	// Segments of a few MB, so one is recorded within the drill.
	policies := api.NewUploadPolicyServiceClient(ops)
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "short-laminae", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 1 << 20, TargetLamina: 4 << 20, MaxLamina: 16 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "gone",
	}.Build())
	require.NoError(t, err)

	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + samplePath(t), Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
	})
	require.NoError(t, err)
	go p.Run(pctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	attached := func() int32 {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return -1
		}
		var n int32
		for _, r := range vs.GetItems() {
			n += r.GetStatus().GetAttachedProducers()
		}

		return n
	}
	require.Eventually(t, func() bool { return attached() == 1 && p.Stats().Stored >= 1 }, 60*time.Second, 300*time.Millisecond, "attached to the relay and recording")
	live, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, live.GetSources(), 1, "a viewer is offered the camera while the producer is up")

	committed := func() int {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    200,
		}.Build())
		if err != nil {
			return -1
		}
		n := 0
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				n++
			}
		}

		return n
	}

	// The producer goes.
	pcancel()

	// The relay has nothing to show: the ingest stream is gone with it.
	require.Eventually(t, func() bool { return attached() == 0 }, 30*time.Second, 200*time.Millisecond, "the relay carries nothing")

	// And nothing more is recorded: the camera is off both ways.
	require.Eventually(t, func() bool { return committed() > 0 }, 30*time.Second, 500*time.Millisecond, "what it stored before is there")
	was := committed()
	time.Sleep(10 * time.Second)
	require.Equal(t, was, committed(), "no lamina is stored after the producer is gone")
}
