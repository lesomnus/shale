package e2e_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/server/core"
)

// startNode runs one more Storage Node in this process against the
// cluster's CP, with its own identity and a declared device, and answers
// it with a way to stop it.
func (c *cluster) startNode(name, sinkDir string) (*storage.Node, context.CancelFunc) {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := storage.Config{
		StateDir:          filepath.Join(c.t.TempDir(), name),
		Cp:                "http://" + c.running.ClusterAddr,
		Dev:               true,
		HardwareId:        "test-" + name,
		Addr:              "127.0.0.1:0",
		ControlAddr:       "127.0.0.1:0",
		Sinks:             []storage.SinkConfig{{Path: sinkDir, Capacity: 1 << 30, Device: "disk-" + filepath.Base(sinkDir)}},
		HeartbeatInterval: time.Second,
		Log:               slog.Default().With("node", name),
	}
	n, err := storage.New(cfg)
	require.NoError(c.t, err)
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	select {
	case <-n.Ready:
	case err := <-done:
		cancel()
		c.t.Fatalf("node %s: %v", name, err)
	case <-time.After(30 * time.Second):
		cancel()
		c.t.Fatalf("node %s did not come up", name)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	c.t.Cleanup(stop)

	return n, stop
}

// TestMultiNode is E5's acceptance on directory sinks: three nodes; a
// quarantine that stops writes within a heartbeat and a release that
// resumes them; a reschedule that reaches the xattr within seconds; a
// delete-now that removes the file; and a sink that moves to another node
// and serves again, with every object indexed, after `sink adopt`.
func TestMultiNode(t *testing.T) {
	c := start(t)
	ctx := context.Background()

	dirB, dirC := filepath.Join(t.TempDir(), "sink-b"), filepath.Join(t.TempDir(), "sink-c")
	nodeB, _ := c.startNode("b", dirB)
	nodeC, stopC := c.startNode("c", dirC)

	ops := c.dialCluster("@cluster/ops")
	nodes := api.NewNodeServiceClient(ops)
	sinks := api.NewSinkServiceClient(ops)
	devices := api.NewDeviceServiceClient(ops)

	// Three adopted nodes with three attached sinks.
	var sinkOf map[pdid.Id]*api.Sink // by node
	require.Eventually(t, func() bool {
		ns, err := nodes.List(ctx, api.NodeListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		adopted := 0
		for _, n := range ns.GetItems() {
			if n.GetState() == api.HostState_HOST_STATE_ADOPTED && n.GetControlAddress() != "" {
				adopted++
			}
		}
		ss, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		sinkOf = map[pdid.Id]*api.Sink{}
		for _, s := range ss.GetItems() {
			if s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && len(s.GetNode().GetId()) > 0 {
				sinkOf[mustId(s.GetNode().GetId())] = s
			}
		}

		return adopted == 3 && len(sinkOf) == 3
	}, 30*time.Second, 200*time.Millisecond)
	sinkB, sinkC := sinkOf[nodeB.Id()], sinkOf[nodeC.Id()]
	require.NotNil(t, sinkB)
	require.NotNil(t, sinkC)

	// A set with three sources, buffered uploads. Placement is per source
	// and epoch, and the set spread puts the members on different sinks
	// (§11), so three sources cover three sinks.
	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	objects := api.NewObjectServiceClient(conn)
	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "multi"}.Build())
	require.NoError(t, err)
	var srcs []*api.Source
	var proposals []*api.SourceProposal
	for i := range 3 {
		src, err := sources.Add(ctx, api.SourceAddRequest_builder{
			Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: fmt.Sprintf("cam-%d", i), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		}.Build())
		require.NoError(t, err)
		srcs = append(srcs, src)
		proposals = append(proposals, api.SourceProposal_builder{
			Source: api.SourceRef_builder{Id: src.GetId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 4_000_000}.Build(),
		}.Build())
	}
	neg, err := sets.Negotiate(ctx, api.SetNegotiateRequest_builder{
		Ref:     setRef("multi"),
		Link:    api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_BUFFERED}.Build(),
		Sources: proposals,
	}.Build())
	require.NoError(t, err)
	dur := time.Duration(neg.GetSources()[0].GetProfile().GetDurationSeconds()) * time.Second

	// Ten segments per source, spread over the three sinks by placement.
	const n = 10
	base := time.Now().Add(-time.Duration(n+2) * dur).Truncate(time.Second)
	body := make([]byte, 64<<10)
	for i := range body {
		body[i] = byte(i)
	}
	type stored struct {
		al   *api.Allocation
		sink pdid.Id
	}
	var all []stored
	bySink := map[pdid.Id]int{}
	for _, src := range srcs {
		for i := range n {
			al, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
				Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(base.Add(time.Duration(i) * dur)),
			}.Build())
			require.NoError(t, err)
			require.Len(t, al.GetCandidates(), 3, "every sink is a candidate")
			sid := mustId(al.GetCandidates()[0].GetSinkId())
			require.Equal(t, http.StatusCreated, put(t, al, body, base.Add(time.Duration(i+1)*dur)))
			all = append(all, stored{al, sid})
			bySink[sid]++
		}
	}
	require.Len(t, bySink, 3, "placement used every sink: %v", bySink)
	require.Eventually(t, func() bool {
		for _, s := range all {
			o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: s.al.GetObjectId()}.Build()}.Build())
			if err != nil || o.GetState() != api.ObjectState_OBJECT_STATE_COMMITTED {
				return false
			}
		}

		return true
	}, 20*time.Second, 200*time.Millisecond, "every upload commits")

	// ---- quarantine: B's device stops taking writes within a heartbeat.
	// allocTo is a fresh allocation whose first candidate is the sink.
	slot := n
	allocTo := func(want pdid.Id) *api.Allocation {
		for range 50 {
			slot++
			for _, src := range srcs {
				al, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
					Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(base.Add(time.Duration(slot) * dur)),
				}.Build())
				require.NoError(t, err)
				if mustId(al.GetCandidates()[0].GetSinkId()) == want {
					return al
				}
			}
		}
		t.Fatal("no allocation landed on the sink")

		return nil
	}
	helds := []*api.Allocation{allocTo(mustId(sinkB.GetId())), allocTo(mustId(sinkB.GetId()))}
	_, err = devices.Quarantine(ctx, api.DeviceQuarantineRequest_builder{Ref: api.DeviceRef_builder{Id: sinkB.GetDevice().GetId()}.Build(), Reason: "test"}.Build())
	require.NoError(t, err)
	// The directive goes out on the leader's next tick (500 ms here); a
	// producer holding an allocation to the sink is refused from then on.
	time.Sleep(3 * c.cfg.Control.DirectivesEvery)
	require.Equal(t, http.StatusServiceUnavailable, put(t, helds[0], body, helds[0].GetDateStarted().AsTime().Add(dur)), "the node refuses the upload once told")
	// And placement no longer offers the sink at all.
	for _, src := range srcs {
		slot++
		al, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
			Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(base.Add(time.Duration(slot) * dur)),
		}.Build())
		require.NoError(t, err)
		for _, cand := range al.GetCandidates() {
			require.NotEqual(t, sinkB.GetId(), cand.GetSinkId(), "a quarantined device is not a candidate")
		}
	}
	// Released: on probation, but writing again.
	_, err = devices.Release(ctx, api.DeviceReleaseRequest_builder{Ref: api.DeviceRef_builder{Id: sinkB.GetDevice().GetId()}.Build(), Reason: "test"}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return put(t, helds[1], body, helds[1].GetDateStarted().AsTime().Add(dur)) == http.StatusCreated
	}, 5*time.Second, 200*time.Millisecond, "the node takes the upload again once released")
	d, err := devices.Get(ctx, api.DeviceGetRequest_builder{Ref: api.DeviceRef_builder{Id: sinkB.GetDevice().GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.DeviceHealth_DEVICE_HEALTH_HEALTHY, d.GetHealth())
	require.NotNil(t, d.GetQuarantine().GetDateProbationEnds())

	// ---- reschedule: the new date reaches the xattr within seconds.
	onA := all[0]
	for _, s := range all {
		if s.sink != mustId(sinkB.GetId()) && s.sink != mustId(sinkC.GetId()) {
			onA = s
			break
		}
	}
	keep := time.Now().Add(10 * 24 * time.Hour).Truncate(time.Millisecond)
	res, err := objects.Reschedule(ctx, api.ObjectRescheduleRequest_builder{
		Ref: api.ObjectRef_builder{Id: onA.al.GetObjectId()}.Build(), DateExpired: timestamppb.New(keep), DateDeleted: timestamppb.New(keep), Reason: "incident",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(1), res.GetChanged())
	pathA := filepath.Join(c.cfg.Storage.Sinks[0].Path, onA.al.GetObjectKey())
	require.Eventually(t, func() bool {
		rec, err := storage.ReadRecordPath(pathA)

		return err == nil && rec.GetDateExpiredMs() == keep.UnixMilli() && rec.GetDateDeletedMs() == keep.UnixMilli()
	}, 5*time.Second, 200*time.Millisecond, "SetDates rewrote the xattr")
	o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: onA.al.GetObjectId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, o.GetDatesSynced())

	// ---- delete now: the file goes within seconds, and the row follows.
	var onB stored
	for _, s := range all {
		if s.sink == mustId(sinkB.GetId()) {
			onB = s
			break
		}
	}
	pathB := filepath.Join(dirB, onB.al.GetObjectKey())
	_, err = os.Stat(pathB)
	require.NoError(t, err)
	_, err = objects.Reschedule(ctx, api.ObjectRescheduleRequest_builder{
		Ref: api.ObjectRef_builder{Id: onB.al.GetObjectId()}.Build(), DeleteNow: true, Reason: "privacy request",
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := os.Stat(pathB)
		if !os.IsNotExist(err) {
			return false
		}
		o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: onB.al.GetObjectId()}.Build()}.Build())

		return err == nil && o.GetState() == api.ObjectState_OBJECT_STATE_DELETED
	}, 10*time.Second, 200*time.Millisecond, "Delete removed the file and the event closed the row")

	// ---- a sink moves: C dies, D attaches C's disk, and after `sink adopt`
	// the objects on it are readable again and every one is indexed.
	var onC stored
	for _, s := range all {
		if s.sink == mustId(sinkC.GetId()) {
			onC = s
			break
		}
	}
	// One row the CP lost: reconciliation brings it back from the xattr.
	lostId := mustId(onC.al.GetObjectId()).Uuid()
	_, err = c.running.CP.Ent.Attempt.Delete().Where(attempt.ObjectIdEQ(lostId)).Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, c.running.CP.Ent.Object.DeleteOneId(lostId).Exec(ctx))
	stopC()
	t.Log("waiting for the CP to consider node C down")
	time.Sleep(core.DefaultNodeDownAfter + 2*time.Second)
	nodeD, _ := c.startNode("d", dirC)
	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkC.GetId()}.Build()}.Build())

		return err == nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION
	}, 10*time.Second, 200*time.Millisecond, "a sink reported by another node while its own is down is pending adoption")
	adopted, err := sinks.Adopt(ctx, api.SinkAdoptRequest_builder{
		Ref: api.SinkRef_builder{Id: sinkC.GetId()}.Build(), Node: api.NodeRef_builder{Id: nodeD.Id().Bytes()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, nodeD.Id().Bytes(), adopted.GetNode().GetId())

	require.Eventually(t, func() bool {
		s, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: sinkC.GetId()}.Build()}.Build())

		return err == nil && s.GetDateReconciled() != nil
	}, 15*time.Second, 200*time.Millisecond, "the leader reconciled the moved sink")
	back, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: onC.al.GetObjectId()}.Build()}.Build())
	require.NoError(t, err, "the erased row came back from the sink")
	require.Equal(t, api.ObjectState_OBJECT_STATE_COMMITTED, back.GetState())
	require.Equal(t, sinkC.GetId(), back.GetSink().GetId())

	// Readable through D.
	tl, err := objects.Timeline(ctx, api.ObjectTimelineRequest_builder{
		Set: setRef("multi"), From: timestamppb.New(onC.al.GetDateStarted().AsTime()), To: timestamppb.New(onC.al.GetDateStarted().AsTime().Add(dur)), Size: 10,
	}.Build())
	require.NoError(t, err)
	var url string
	for _, src := range tl.GetSources() {
		for _, o := range src.GetObjects() {
			if string(o.GetObjectId()) == string(onC.al.GetObjectId()) {
				url = o.GetUrl()
			}
		}
	}
	require.NotEmpty(t, url)
	require.Contains(t, url, fmt.Sprintf(":%d/", portOf(t, nodeD.DataAddr)), "the URL points at D, the sink's new node")
	require.Equal(t, body, fetch(t, url))
}

func mustId(b []byte) pdid.Id {
	id, err := pdid.From(b)
	if err != nil {
		panic(err)
	}

	return id
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	var host string
	var port int
	_, err := fmt.Sscanf(addr[len(addr)-6:], "%1s%d", &host, &port)
	if err != nil {
		for i := len(addr) - 1; i >= 0; i-- {
			if addr[i] == ':' {
				fmt.Sscanf(addr[i+1:], "%d", &port)
				break
			}
		}
	}

	return port
}
