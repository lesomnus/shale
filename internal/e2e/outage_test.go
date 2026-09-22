package e2e_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
)

// downAfter is what these drills set `node_down_after` to: three of the
// harness's heartbeats, so a node that stops counts as down in seconds
// rather than in §36.1's half minute.
const downAfter = 3 * time.Second

// timelineAround answers the set's timeline over an hour around a slot,
// which for one source is one entry or one gap (§19).
func timelineAround(t *testing.T, ctx context.Context, laminae api.LaminaServiceClient, set *api.Set, at time.Time) *api.TimelineSource {
	t.Helper()
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set:  api.SetRef_builder{Id: set.GetId()}.Build(),
		From: timestamppb.New(at.Add(-time.Minute)), To: timestamppb.New(at.Add(time.Hour)), Size: 20,
	}.Build())
	require.NoError(t, err)
	if len(tl.GetSources()) == 0 {
		return api.TimelineSource_builder{}.Build()
	}

	return tl.GetSources()[0]
}

// TestReadsWhileNodeDown is §28.3: a node that stops takes the laminae on
// its sinks with it, and they are unavailable, not lost. The timeline
// answers the span as a gap that says so instead of a URL nothing would
// serve, the row stays COMMITTED, and the same node back makes it readable
// again with nothing else changed.
func TestReadsWhileNodeDown(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Control.NodeDownAfter = downAfter })
	ctx := context.Background()

	stateB := filepath.Join(t.TempDir(), "node-b")
	sinkB := filepath.Join(t.TempDir(), "sink-b")
	nodeB, stopB := c.startNodeAt("b", stateB, sinkB)
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
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

	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "down", nil)

	al := allocate(time.Hour)
	var cand *api.Candidate
	for _, cd := range al.GetCandidates() {
		if string(cd.GetNodeId()) == string(nodeB.Id().Bytes()) {
			cand = cd
			break
		}
	}
	require.NotNil(t, cand, "node B is a candidate")
	body := make([]byte, 64<<10)
	rand.Read(body)
	code, _ := putAt(t, al, cand, body, "?1")
	require.Equal(t, http.StatusCreated, code)
	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 20*time.Second, 200*time.Millisecond, "the upload commits")

	started := al.GetDateStarted().AsTime()
	ts := timelineAround(t, ctx, laminae, set, started)
	require.Len(t, ts.GetLaminae(), 1, "the lamina is served while its node is up")
	require.Equal(t, api.ReadState_READ_STATE_AVAILABLE, ts.GetLaminae()[0].GetState())
	require.Equal(t, body, fetch(t, ts.GetLaminae()[0].GetUrl()))

	// The node stops. Until the CP calls it down the lamina is still
	// offered; once it does, the span reads as a gap that says the bytes
	// are somewhere unreachable, not that they are gone.
	stopB()
	t.Log("waiting for the CP to consider node B down")
	time.Sleep(downAfter + 2*time.Second)
	require.Eventually(t, func() bool {
		ts := timelineAround(t, ctx, laminae, set, started)
		if len(ts.GetLaminae()) > 0 {
			return false
		}
		for _, g := range ts.GetGaps() {
			if g.GetReason() == api.GapReason_GAP_REASON_UNAVAILABLE && !g.GetFrom().AsTime().After(started) {
				return true
			}
		}

		return false
	}, 30*time.Second, time.Second, "the span reads unavailable while the node is down")

	o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, o.GetState(), "a node that is down loses nothing: the row is untouched")
	require.Equal(t, cand.GetSinkId(), o.GetSink().GetId(), "and the lamina is still on its sink")

	// The same machine back: the same node, the same sink, and the lamina
	// is served again. Nothing was rebuilt and nothing moved (§28.1).
	nodeB2, _ := c.startNodeAt("b", stateB, sinkB)
	require.Equal(t, nodeB.Id(), nodeB2.Id(), "the same node")
	var url string
	require.Eventually(t, func() bool {
		ts := timelineAround(t, ctx, laminae, set, started)
		if len(ts.GetLaminae()) != 1 {
			return false
		}
		url = ts.GetLaminae()[0].GetUrl()

		return ts.GetLaminae()[0].GetState() == api.ReadState_READ_STATE_AVAILABLE
	}, 30*time.Second, 500*time.Millisecond, "the lamina is available again")
	require.Equal(t, body, fetch(t, url))
}

// TestSinkClaimRefusedWhileOwnerAlive is §28.3's step 3a, and §33.7's rule
// that a node's word about a sink it does not hold changes nothing: a
// second node that finds a live node's disk opens it, reports it, and is
// told to serve nothing. The sink stays where it is and its owner keeps
// answering for it.
func TestSinkClaimRefusedWhileOwnerAlive(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
	nodes := api.NewNodeServiceClient(ops)

	var own *api.Sink
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				own = s
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the cluster's own node has its sink")
	owner := own.GetNode().GetId()

	// One lamina on that sink, so there is something to serve.
	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "claim", nil)
	al := allocate(time.Hour)
	cand := al.GetCandidates()[0]
	require.Equal(t, own.GetId(), cand.GetSinkId(), "the one sink takes it")
	body := make([]byte, 64<<10)
	rand.Read(body)
	code, _ := putAt(t, al, cand, body, "?1")
	require.Equal(t, http.StatusCreated, code)
	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 20*time.Second, 200*time.Millisecond, "the upload commits")

	// A second node is given the same directory while the first is alive
	// and heartbeating: it is adopted as a node of its own.
	nodeB, _ := c.startNodeAt("b", filepath.Join(t.TempDir(), "node-b"), c.cfg.Storage.Sinks[0].Path)
	require.Eventually(t, func() bool {
		vs, err := nodes.List(ctx, api.NodeListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, n := range vs.GetItems() {
			if string(n.GetId()) == string(nodeB.Id().Bytes()) && n.GetDateSeen() != nil {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the second node joined and heartbeats")

	// It opened the sink, and is told to serve nothing on it: a claim on a
	// live node's sink is refused, so the data plane answers nobody.
	require.Eventually(t, func() bool {
		for _, s := range nodeB.Sinks() {
			if s.Id == mustId(own.GetId()) {
				return !s.Serves()
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the second node holds the sink open and serves it not at all")

	// Several heartbeats later the row has not moved.
	time.Sleep(3 * c.cfg.Storage.HeartbeatInterval)
	still, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: own.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, owner, still.GetNode().GetId(), "the sink still belongs to the node that holds it")
	require.Equal(t, api.SinkAttachment_SINK_ATTACHMENT_ATTACHED, still.GetAttachment(), "and is attached, not pending adoption")
	for _, s := range nodeB.Sinks() {
		require.False(t, s.Serves(), "the claiming node serves nothing")
	}

	// And the owner keeps answering for the lamina.
	ts := timelineAround(t, ctx, laminae, set, al.GetDateStarted().AsTime())
	require.Len(t, ts.GetLaminae(), 1)
	require.Equal(t, body, fetch(t, ts.GetLaminae()[0].GetUrl()))
}

// TestSinkAutoAdopted is §28.3's step 4 without an operator: a sink whose
// node has been down for `sink_auto_adopt_after` and that another node
// reports is adopted there by the leader, and its laminae are readable
// again through the new node with nothing copied.
func TestSinkAutoAdopted(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Control.NodeDownAfter = downAfter
		c.Control.SinkAutoAdoptAfter = 5 * time.Second
		c.Control.JobsEvery = time.Second
	})
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)

	sinkDir := filepath.Join(t.TempDir(), "sink-b")
	nodeB, stopB := c.startNodeAt("b", filepath.Join(t.TempDir(), "node-b"), sinkDir)
	var sinkId []byte
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				sinkId = s.GetId()
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "node B's sink is attached")

	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "adopt", nil)
	al := allocate(time.Hour)
	var cand *api.Candidate
	for _, cd := range al.GetCandidates() {
		if string(cd.GetSinkId()) == string(sinkId) {
			cand = cd
			break
		}
	}
	require.NotNil(t, cand, "node B's sink is a candidate")
	body := make([]byte, 64<<10)
	rand.Read(body)
	code, _ := putAt(t, al, cand, body, "?1")
	require.Equal(t, http.StatusCreated, code)
	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 20*time.Second, 200*time.Millisecond, "the upload commits")

	// The machine dies and its disk is put in another one, which reports
	// the sink: while the old node is only recently down the sink waits.
	stopB()
	time.Sleep(downAfter + time.Second)
	nodeC, _ := c.startNodeAt("c", filepath.Join(t.TempDir(), "node-c"), sinkDir)
	state := func() (*api.Sink, error) {
		return sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkId}.Build()}.Build())
	}
	require.Eventually(t, func() bool {
		s, err := state()

		return err == nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION
	}, 20*time.Second, 200*time.Millisecond, "the sink is pending adoption first")

	// Nobody runs `sink adopt`: past sink_auto_adopt_after the leader hands
	// it to the node that reports it.
	require.Eventually(t, func() bool {
		s, err := state()

		return err == nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && string(s.GetNode().GetId()) == string(nodeC.Id().Bytes())
	}, 30*time.Second, 500*time.Millisecond, "the sink is adopted by the node reporting it")
	require.Eventually(t, func() bool {
		s, err := state()

		return err == nil && s.GetDateReconciled() != nil
	}, 20*time.Second, 200*time.Millisecond, "and reconciled where it now is")

	// The lamina reads again, from the new node, with the same bytes.
	var url string
	require.Eventually(t, func() bool {
		ts := timelineAround(t, ctx, laminae, set, al.GetDateStarted().AsTime())
		if len(ts.GetLaminae()) != 1 {
			return false
		}
		url = ts.GetLaminae()[0].GetUrl()

		return ts.GetLaminae()[0].GetState() == api.ReadState_READ_STATE_AVAILABLE
	}, 30*time.Second, 500*time.Millisecond, "the lamina is available again")
	require.Contains(t, url, fmt.Sprintf(":%d/", portOf(t, nodeC.DataAddr)), "served by the node that adopted the sink")
	// The row moves at the leader's job; the new node answers for the sink
	// from its next heartbeat, when the CP tells it the sink is its own.
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, 20*time.Second, 200*time.Millisecond, "the node that adopted the sink serves it")
	require.Equal(t, body, fetch(t, url))
	o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, sinkId, o.GetSink().GetId(), "the lamina did not move: its sink did")
}
