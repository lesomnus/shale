package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/server/core"
)

// TestSinkSwapped is how an operator changes the disk under a node
// (§22.2, §28.1, §28.3). Nothing moves: a second sink is added beside the
// first, the first is retired so that nothing more is written to it, its
// device is declared dead because what is on it is not being kept, and the
// node then comes back without it.
//
// What is left has to go on working. The retired sink keeps its node in
// the row — the "no longer reported" sweep is for attached sinks — so the
// CP must not spend every pass asking a node about a sink it does not
// have: one absent sink would take the deletes and the reconciliation of
// the disk that replaced it down with it.
func TestSinkSwapped(t *testing.T) {
	// A node that goes and comes back is told everything again (§34.9),
	// which is the pass the swap has to survive; the default 30 s would
	// outlast the restart below and skip it.
	c := start(t, func(cfg *cmd.Config) { cfg.Control.NodeDownAfter = 3 * time.Second })
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
	devices := api.NewDeviceServiceClient(ops)

	state := filepath.Join(t.TempDir(), "node-swap")
	borrowed := filepath.Join(t.TempDir(), "borrowed")
	bought := filepath.Join(t.TempDir(), "bought")
	node, stop := c.startNodeAt("swap", state, borrowed, func(cfg *storage.Config) {
		cfg.Sinks = append(cfg.Sinks, storage.SinkConfig{Path: bought, Capacity: 1 << 30, Device: "disk-bought"})
	})

	ofNode := func() map[string]*api.Sink {
		out := map[string]*api.Sink{}
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{Size: 20}.Build())
		if err != nil {
			return out
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(node.Id().Bytes()) {
				out[s.GetPath()] = s
			}
		}

		return out
	}
	attached := func(path string) bool {
		s := ofNode()[path]

		return s != nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED
	}
	require.Eventually(t, func() bool { return attached(borrowed) && attached(bought) },
		30*time.Second, 200*time.Millisecond, "both sinks are attached: %v", ofNode())
	old := ofNode()[borrowed]

	// Retired: the node is told to take no more writes, and placement
	// stops offering it.
	_, err := sinks.Retire(ctx, api.SinkRetireRequest_builder{
		Ref: api.SinkRef_builder{Id: old.GetId()}.Build(), Reason: "the volume goes back",
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		s := ofNode()[borrowed]

		return s != nil && !s.GetAcceptWrites()
	}, 30*time.Second, 200*time.Millisecond, "the retirement reached the node")

	admin := c.dial("@acme/admin")
	set, src, allocate := camera(t, ctx, admin, "swapped", nil)
	offered := 0
	for i := 1; i <= 5; i++ {
		for _, cd := range allocate(time.Duration(i) * time.Hour).GetCandidates() {
			require.NotEqual(t, old.GetId(), cd.GetSinkId(), "a retired sink is not a candidate")
			offered++
		}
	}
	require.Greater(t, offered, 0, "the new sink is")

	// The data on it is not being kept, so the device goes with it.
	_, err = devices.DeclareDead(ctx, api.DeviceDeclareDeadRequest_builder{
		Ref: api.DeviceRef_builder{Id: old.GetDevice().GetId()}.Build(), Reason: "returned",
	}.Build())
	require.NoError(t, err)

	// And the node comes back without it, as a rolled DaemonSet does,
	// long enough for the CP to see it go.
	stop()
	time.Sleep(4 * time.Second)
	restarted := time.Now()
	c.startNodeAt("swap", state, bought)
	require.Eventually(t, func() bool { return attached(bought) }, 30*time.Second, 200*time.Millisecond, "the sink that is left is attached")
	require.Eventually(t, func() bool {
		return ofNode()[bought].GetDateReconciled().AsTime().After(restarted)
	}, 60*time.Second, 200*time.Millisecond, "a node that comes back is told everything again, so the sink that is left is reconciled")
	require.Equal(t, api.SinkAttachment_SINK_ATTACHMENT_RETIRED, ofNode()[borrowed].GetAttachment(),
		"a retired sink stays retired rather than going pending: it is not waiting for a home")

	// The directives of the sink that is there still run. A GC round is
	// asked for by a label the leader clears once it has run, so a label
	// that goes away is a directive that reached the node.
	left := ofNode()[bought]
	_, err = sinks.Gc(ctx, api.SinkGcRequest_builder{Ref: api.SinkRef_builder{Id: left.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return ofNode()[bought].GetLabels()[core.LabelGc] == ""
	}, 30*time.Second, 200*time.Millisecond, "the GC round ran on the sink that is left")

	// And it still stores: a lamina written after the swap reads back.
	al := allocate(30 * time.Minute)
	require.NotEmpty(t, al.GetCandidates())
	require.Equal(t, 201, putTo(t, al.GetCandidates()[0], []byte("after the swap"), time.Now()))
	laminae := api.NewLaminaServiceClient(admin)
	require.Eventually(t, func() bool {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{
				Set:    api.SetRef_builder{Id: set.GetId()}.Build(),
				Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
			}.Build()},
			Size: 10,
		}.Build())
		if err != nil {
			return false
		}
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the new disk took the write")
}
