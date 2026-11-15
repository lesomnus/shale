package e2e_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/satellite"
	"github.com/lesomnus/shale/internal/storage"
)

// startSatelliteWith is startSatellite with its configuration edited.
func (c *cluster) startSatelliteWith(name, stateDir string, edit func(*satellite.Config)) (*satellite.Satellite, context.CancelFunc) {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := satellite.Config{
		StateDir:          stateDir,
		Cp:                "http://" + c.running.ClusterAddr,
		Dev:               true,
		HardwareId:        "test-" + name,
		Addr:              "127.0.0.1:0",
		HeartbeatInterval: time.Second,
		Log:               slog.Default().With("satellite", name),
	}
	if edit != nil {
		edit(&cfg)
	}
	s, err := satellite.New(cfg)
	require.NoError(c.t, err)
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx)
		close(done)
	}()
	select {
	case <-s.Ready:
	case err := <-done:
		cancel()
		c.t.Fatalf("satellite %s: %v", name, err)
	case <-time.After(30 * time.Second):
		cancel()
		c.t.Fatalf("satellite %s did not come up", name)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	c.t.Cleanup(stop)

	sats := api.NewSatelliteServiceClient(c.dialCluster(c.operator()))
	require.Eventually(c.t, func() bool {
		v, err := sats.Get(context.Background(), api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: s.Id().Bytes()}.Build()}.Build())
		return err == nil && v.GetState() == api.HostState_HOST_STATE_ADOPTED && v.GetStatus().GetInFlightBudget() > 0 &&
			v.GetStatus().GetIncarnation() != ""
	}, 30*time.Second, 200*time.Millisecond, "satellite %s is adopted and heartbeats", name)

	return s, stop
}

// recordRetaining runs a producer of the bench recording for a new set,
// its uploads going the way via says and kept the way retain says, with a
// hardware identity of its own so several run at once, and adopts it.
func (c *cluster) recordRetaining(ctx context.Context, alias string, via producer.Via, retain string) (*producer.Producer, *api.Set, *api.Producer) {
	c.t.Helper()
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias,
	}.Build())
	require.NoError(c.t, err)
	p, err := producer.New(producer.Config{
		StateDir:          filepath.Join(c.t.TempDir(), "producer-"+alias),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		HardwareId:        "test-producer-" + alias,
		Sources:           []producer.SourceConfig{{Alias: "door-" + alias, Input: "raw:" + samplePath(c.t), Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		Retain:            retain,
		HeartbeatInterval: time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Upload:            producer.UploadConfig{Via: via, ResumeTimeout: 10 * time.Second},
	})
	require.NoError(c.t, err)
	go p.Run(ctx)
	row := adoptProducer(c.t, ctx, api.NewProducerServiceClient(admin), set)

	return p, set, row
}

// onlyNodes quarantines the built-in node's device, so the nodes a test
// starts, and can stop, are the only ones taking writes; it answers once
// they are, each with its state and sink directories.
func (c *cluster) onlyNodes(ctx context.Context, names ...string) map[string]*testNode {
	c.t.Helper()
	ops := c.dialCluster(c.operator())
	sinks := api.NewSinkServiceClient(ops)
	vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
	require.NoError(c.t, err)
	require.Len(c.t, vs.GetItems(), 1)
	_, err = api.NewDeviceServiceClient(ops).Quarantine(ctx, api.DeviceQuarantineRequest_builder{
		Ref: api.DeviceRef_builder{Id: vs.GetItems()[0].GetDevice().GetId()}.Build(), Reason: "the drill",
	}.Build())
	require.NoError(c.t, err)

	out := map[string]*testNode{}
	for _, name := range names {
		n := &testNode{c: c, name: name, state: filepath.Join(c.t.TempDir(), "node-"+name), sink: filepath.Join(c.t.TempDir(), "sink-"+name)}
		n.start()
		out[name] = n
	}
	require.Eventually(c.t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		attached := 0
		for _, s := range vs.GetItems() {
			for _, n := range out {
				if string(s.GetNode().GetId()) == string(n.node.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
					attached++
				}
			}
		}

		return attached == len(out)
	}, 30*time.Second, 200*time.Millisecond, "the nodes' sinks are attached")

	return out
}

// testNode is a node a test stops and starts again as itself.
type testNode struct {
	c                 *cluster
	name, state, sink string
	node              *storage.Node
	stop              context.CancelFunc
}

func (n *testNode) start() { n.node, n.stop = n.c.startNodeAt(n.name, n.state, n.sink) }

// heldBy is the laminae of a set that a satellite took, and those of them
// in a state.
func (c *cluster) heldBy(ctx context.Context, set *api.Set, sat *satellite.Satellite, state api.LaminaState) (held, in []*api.Lamina) {
	for _, o := range c.laminaeOf(ctx, set) {
		if string(o.GetSatellite().GetId()) != string(sat.Id().Bytes()) {
			continue
		}
		held = append(held, o)
		if o.GetState() == state {
			in = append(in, o)
		}
	}

	return held, in
}

// attemptsOf is a lamina's attempts.
func (c *cluster) attemptsOf(ctx context.Context, o *api.Lamina) []*api.Attempt {
	c.t.Helper()
	vs := untilOK(c.t, func() (*api.AttemptListResponse, error) {
		return api.NewAttemptServiceClient(c.dial("@acme/admin")).List(ctx, api.AttemptListRequest_builder{
			Filters: []*api.AttemptFilter{api.AttemptFilter_builder{Lamina: api.LaminaRef_builder{Id: o.GetId()}.Build()}.Build()},
		}.Build())
	})

	return vs.GetItems()
}

// TestSatelliteAccepted is `retain: accepted` (§41.9). A producer under it
// hands each segment to the satellite, which takes the bytes and delivers
// them; the producer lets them go as the satellite says it holds them.
// With every node down the producer goes on handing segments off, holding
// next to nothing, while no lamina commits: its RAM was freed before any
// node had the bytes. The nodes back, the satellite delivers what it
// held, and it plays. A node that fails under a delivery is the
// satellite's to fail over from, to the next candidate, in the producer's
// name; and nothing is lost.
func TestSatelliteAccepted(t *testing.T) {
	c := start(t, noBuiltInSatellite, func(c *cmd.Config) { c.Control.NodeDownAfter = 10 * time.Minute })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	nodes := c.onlyNodes(ctx, "b", "c")
	sat, _ := c.startSatelliteWith("sat", filepath.Join(t.TempDir(), "sat"), nil)
	p, set, row := c.recordRetaining(ctx, "accepted", producer.ViaSatellite, producer.RetainAccepted)

	require.Eventually(t, func() bool {
		held, committed := c.heldBy(ctx, set, sat, api.LaminaState_LAMINA_STATE_COMMITTED)
		return p.Stats().HandedOff >= 3 && len(committed) >= 3 && len(held) >= 3
	}, 120*time.Second, 500*time.Millisecond, "handed off and delivered: %+v", p.Stats())
	st := p.Stats()
	require.Zero(t, st.Lost)
	require.Zero(t, st.Cut)
	require.Positive(t, st.HandedOffBytes)

	// What the CP is told: the producer's retain and its handoffs, the
	// satellite's pool.
	producers := api.NewProducerServiceClient(c.dial("@acme/admin"))
	require.Eventually(t, func() bool {
		v, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: row.GetId()}.Build()}.Build())
		return err == nil && v.GetStatus().GetUpload().GetRetain() == "accepted"
	}, 10*time.Second, 200*time.Millisecond)
	sats := api.NewSatelliteServiceClient(c.dialCluster(c.operator()))
	require.Eventually(t, func() bool {
		v, err := sats.Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
		s := v.GetStatus()
		return err == nil && s.GetAccept() && s.GetRetainedBudget() > 0 && s.GetDelivered() >= 3 && s.GetAcceptedBytes() > 0
	}, 10*time.Second, 200*time.Millisecond)

	// Every node down: the producer goes on handing off, holding no more
	// than a part, and the satellite holds what it took.
	nodes["b"].stop()
	nodes["c"].stop()
	down := p.Stats().HandedOff
	require.Eventually(t, func() bool { return p.Stats().HandedOff >= down+3 && len(sat.Held()) >= 3 }, 60*time.Second, 200*time.Millisecond,
		"handed off with no node up: %+v, held %d", p.Stats(), len(sat.Held()))
	for range 10 {
		require.Less(t, p.Stats().Held, int64(3<<20), "a part and the segment being cut, not the outage: %+v", p.Stats())
		time.Sleep(100 * time.Millisecond)
	}
	retained, _, _ := sat.Retained()
	require.Positive(t, retained)
	require.Positive(t, sat.RetainedBy(mustPdid(t, row.GetId())))
	pending := sat.Held()

	// The nodes back: what the satellite held is delivered and plays.
	nodes["b"].start()
	nodes["c"].start()
	require.Eventually(t, func() bool {
		for _, id := range pending {
			o := untilOK(t, func() (*api.Lamina, error) {
				return api.NewLaminaServiceClient(c.dial("@acme/admin")).Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: id}.Build()}.Build())
			})
			require.NotEqual(t, api.LaminaState_LAMINA_STATE_LOST, o.GetState())
			if o.GetState() != api.LaminaState_LAMINA_STATE_COMMITTED {
				return false
			}
		}
		return true
	}, 120*time.Second, 500*time.Millisecond, "delivered once the nodes are back")
	admin := c.dial("@acme/admin")
	var src []byte
	for _, o := range c.laminaeOf(ctx, set) {
		src = o.GetSource().GetId()
	}
	tl, err := api.NewLaminaServiceClient(admin).Timeline(ctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: src}.Build(),
		From:   timestamppb.New(time.Now().Add(-10 * time.Minute)), To: timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.NoError(t, err)
	played := 0
	for _, o := range tl.GetSources()[0].GetLaminae() {
		if o.GetState() != api.ReadState_READ_STATE_AVAILABLE {
			continue
		}
		b := fetch(t, o.GetUrl())
		require.Equal(t, int64(len(b)), o.GetSize())
		playsOnItsOwn(t, b)
		played++
	}
	require.GreaterOrEqual(t, played, 6)

	// A node that fails under deliveries: the satellite reports the
	// attempt failed, for the producer, and the next candidate takes it.
	nodes["b"].stop()
	failedOver := func() bool {
		for _, o := range c.laminaeOf(ctx, set) {
			if o.GetState() != api.LaminaState_LAMINA_STATE_COMMITTED {
				continue
			}
			for _, a := range c.attemptsOf(ctx, o) {
				if a.GetState() == api.AttemptState_ATTEMPT_STATE_FAILED && string(a.GetNode().GetId()) == string(nodes["b"].node.Id().Bytes()) &&
					strings.HasPrefix(a.GetFailureReason(), "satellite ") && strings.Contains(a.GetFailureReason(), "for its producer") {
					return true
				}
			}
		}
		return false
	}
	require.Eventually(t, failedOver, 120*time.Second, time.Second, "a lamina failed over from node b, by the satellite")
	st = p.Stats()
	require.Zero(t, st.Lost)
	require.Zero(t, st.Cut)
	_, lost := c.heldBy(ctx, set, sat, api.LaminaState_LAMINA_STATE_LOST)
	require.Empty(t, lost, "nothing lost")
}

// When the satellite will not take a segment the producer keeps it, as
// under `committed` (§41.9): a retained pool with no room for one passes
// every segment through to the node; a satellite that refuses `accepted`
// takes none, its assignment saying so, and what still asks it is passed
// through. Either way the segments are stored and nothing is lost.
func TestSatelliteAcceptedPassedThrough(t *testing.T) {
	c := start(t, noBuiltInSatellite, func(c *cmd.Config) { c.Control.SatelliteDownAfter = 10 * time.Minute })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	dir := filepath.Join(t.TempDir(), "sat")
	// A pool smaller than any segment's reservation.
	sat, stop := c.startSatelliteWith("sat", dir, func(cfg *satellite.Config) { cfg.RetainedBudget = 512 << 10 })
	p, set, _ := c.recordRetaining(ctx, "passed", producer.ViaSatellite, producer.RetainAccepted)

	require.Eventually(t, func() bool { return p.Stats().Stored >= 3 }, 120*time.Second, 500*time.Millisecond, "stored as under committed: %+v", p.Stats())
	st := p.Stats()
	require.Zero(t, st.HandedOff)
	require.Positive(t, st.Passed)
	require.Zero(t, st.Lost)
	_, _, _, passed := sat.Accepted()
	require.Positive(t, passed)
	require.Empty(t, sat.Held())

	// `satellite.accept: false`, the same satellite.
	stop()
	sat, _ = c.startSatelliteWith("sat", dir, func(cfg *satellite.Config) { cfg.RefuseAccepted = true })
	stored := p.Stats().Stored
	require.Eventually(t, func() bool { return p.Stats().Stored >= stored+3 }, 120*time.Second, 500*time.Millisecond,
		"stored as under committed: %+v", p.Stats())
	require.Empty(t, sat.Held())
	acceptedBytes, _, _, _ := sat.Accepted()
	require.Zero(t, acceptedBytes, "it took nothing")
	_, _, budget := sat.Retained()
	require.Zero(t, budget, "it keeps no retained pool")
	require.Zero(t, p.Stats().HandedOff)
	require.Zero(t, p.Stats().Lost)
	require.Eventually(t, func() bool { n, _ := c.committedBytes(ctx, set); return n >= 6 }, 30*time.Second, 300*time.Millisecond,
		"what it stored is committed")
	v, err := api.NewSatelliteServiceClient(c.dialCluster(c.operator())).Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.False(t, v.GetStatus().GetAccept())
	require.Zero(t, v.GetStatus().GetRetainedBudget())
}

// A satellite that dies holding bytes it accepted loses them: those
// laminae are LOST, and the loss is the satellite's, on the lamina's row
// and in its attempts' reasons, not the producer's (§41.9). A producer
// under `committed` through the same satellite loses nothing.
func TestSatelliteAcceptedLost(t *testing.T) {
	c := start(t, noBuiltInSatellite, func(c *cmd.Config) {
		c.Control.NodeDownAfter = 10 * time.Minute
		c.Control.SatelliteDownAfter = 10 * time.Minute
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	nodes := c.onlyNodes(ctx, "b")
	dir := filepath.Join(t.TempDir(), "sat")
	sat, stop := c.startSatelliteWith("sat", dir, nil)
	accepting, accSet, _ := c.recordRetaining(ctx, "handing", producer.ViaSatellite, producer.RetainAccepted)
	committing, comSet, _ := c.recordRetaining(ctx, "keeping", producer.ViaSatellite, producer.RetainCommitted)
	require.Eventually(t, func() bool {
		return accepting.Stats().HandedOff >= 2 && committing.Stats().Stored >= 2
	}, 120*time.Second, 500*time.Millisecond, "both record through the satellite")

	// The node down, the satellite holds what it took, and the producer
	// under committed holds its own.
	nodes["b"].stop()
	require.Eventually(t, func() bool { return len(sat.Held()) >= 2 }, 60*time.Second, 200*time.Millisecond, "the satellite holds undelivered segments")
	held := sat.Held()

	// The satellite dies with them, and comes back as itself.
	stop()
	sat, _ = c.startSatelliteWith("sat", dir, nil)
	require.Eventually(t, func() bool {
		_, lost := c.heldBy(ctx, accSet, sat, api.LaminaState_LAMINA_STATE_LOST)
		return len(lost) >= len(held)
	}, 60*time.Second, 500*time.Millisecond, "what it held is LOST")
	_, lost := c.heldBy(ctx, accSet, sat, api.LaminaState_LAMINA_STATE_LOST)
	row, err := api.NewSatelliteServiceClient(c.dialCluster(c.operator())).Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
	require.NoError(t, err)
	for _, o := range lost {
		require.Equal(t, sat.Id().Bytes(), o.GetSatellite().GetId(), "lost by the satellite")
		reasons := ""
		for _, a := range c.attemptsOf(ctx, o) {
			reasons += a.GetFailureReason() + "; "
		}
		require.Contains(t, reasons, "satellite "+row.GetAlias(), "the attempts say whose loss it is")
	}
	require.Zero(t, accepting.Stats().Lost, "not the producer's loss")

	// The node back: the producer under committed stores everything.
	nodes["b"].start()
	stored := committing.Stats().Stored
	require.Eventually(t, func() bool { return committing.Stats().Stored >= stored+3 }, 120*time.Second, 500*time.Millisecond, "%+v", committing.Stats())
	require.Zero(t, committing.Stats().Lost)
	for _, o := range c.laminaeOf(ctx, comSet) {
		require.NotEqual(t, api.LaminaState_LAMINA_STATE_LOST, o.GetState(), "the committed producer lost nothing")
	}
}

// TestSatelliteAcceptedFeedsLive is `retain: accepted` with the live
// fan-out (§41.8, §41.9): a segment handed off still feeds the relay as it
// is recorded, from the bytes the satellite takes, across laminae; the
// delivery to the node, which reads them again, feeds nothing more. The
// producer's own tee sends nothing.
func TestSatelliteAcceptedFeedsLive(t *testing.T) {
	c := start(t, noBuiltInSatellite)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	sat, _ := c.startSatelliteWith("sat", filepath.Join(t.TempDir(), "sat"), nil)
	p, set := c.satelliteLive(ctx, "porch", "av.mp4", producer.ViaSatellite, func(cfg *producer.Config) {
		cfg.Retain = producer.RetainAccepted
		cfg.HardwareId = "test-producer-porch"
	})
	fedBy(t, sat, p, "porch")
	live := c.liveOf(ctx, set)

	v := watch(t, live)
	defer v.close()
	v.video(t, 50, 20*time.Second)
	a := timeVideo(v)
	began := time.Now()
	handed := p.Stats().HandedOff
	require.Eventually(t, func() bool { return p.Stats().HandedOff >= handed+3 }, 90*time.Second, 200*time.Millisecond,
		"three segments handed off while the viewer watches")
	time.Sleep(time.Second)
	ended := time.Now()
	gap := a.gap(began, ended)
	t.Logf("%d video packets over %s, the longest wait %s", a.count(), ended.Sub(began).Round(time.Millisecond), gap)
	require.Less(t, gap, keyInterval, "no handoff costs the viewer a keyframe")
	require.Zero(t, p.LiveSent("porch"), "the satellite fed it all along")

	// Fed once: what went to the relay is no more than what the satellite
	// took, though it delivered all of it to the node as well.
	_, sent, _ := sat.Live()
	accepted, delivered, _, _ := sat.Accepted()
	require.Positive(t, delivered)
	require.Positive(t, sent)
	require.LessOrEqual(t, sent, accepted, "the delivery did not feed the relay again")
	require.Zero(t, p.Stats().Lost)
}

// A satellite makes lamina calls only for the producers assigned to it
// (§41.9): for a lamina of a set no producer of its records, the CP
// refuses every one of them, and recording it as the lamina's holder.
func TestSatelliteDelegatedCalls(t *testing.T) {
	c := start(t, noBuiltInSatellite)
	ctx := context.Background()
	sat, _ := c.startSatelliteWith("sat", filepath.Join(t.TempDir(), "sat"), nil)
	_, _, allocate := camera(t, ctx, c.dial("@acme/admin"), "foreign", nil)
	al := allocate(time.Hour)
	as := c.dialCluster(sat.Id().String())
	laminae := api.NewLaminaServiceClient(as)
	ref := api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()
	attempt := api.AttemptRef_builder{Id: al.GetCandidates()[0].GetAttemptId()}.Build()

	_, err := laminae.Renew(ctx, api.LaminaRenewRequest_builder{Ref: ref, Attempt: attempt}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "renew: %v", err)
	_, err = laminae.ReportAttempt(ctx, api.LaminaReportAttemptRequest_builder{Ref: ref, Attempt: attempt, FailureReason: "x"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "report attempt: %v", err)
	_, err = laminae.Reallocate(ctx, api.LaminaReallocateRequest_builder{Ref: ref}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "reallocate: %v", err)
	_, err = laminae.ReportFailure(ctx, api.LaminaReportFailureRequest_builder{Ref: ref, Reason: "x"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "report failure: %v", err)
	_, err = api.NewSatelliteServiceClient(as).Accept(ctx, api.SatelliteAcceptRequest_builder{LaminaId: al.GetLaminaId()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "accept: %v", err)
	// Nor anything else of a lamina's.
	_, err = laminae.Skip(ctx, api.LaminaSkipRequest_builder{Ref: ref, Reason: api.LaminaSkipReason_LAMINA_SKIP_REASON_DARK}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "skip: %v", err)
	_, err = laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{Source: api.SourceRef_builder{Id: al.GetSourceId()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "allocate: %v", err)

	o, err := api.NewLaminaServiceClient(c.dial("@acme/admin")).Get(ctx, api.LaminaGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, o.GetState())
	require.Empty(t, o.GetSatellite().GetId())
}
