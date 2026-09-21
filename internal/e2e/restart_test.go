package e2e_test

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/z"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/server/core"
)

// TestSegmentsAfterStops is §15 and §12.1 with a camera that stops and comes
// back within a slot: the recording plays once per capture, so every
// restart starts a new segment in the same slot, and each gets a lamina
// of its own, none refused for a slot "already stored".
func TestSegmentsAfterStops(t *testing.T) {
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.ts"))
	require.NoError(t, err)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "stops",
	}.Build())
	require.NoError(t, err)

	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"),
		Cp:       "http://" + c.running.TenantAddr,
		Dev:      true,
		// The 6 s recording once per capture: the camera stops every 6 s and
		// is back a second or two later, well within a slot of minutes.
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264", RawLoops: 1}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
	})
	require.NoError(t, err)
	go p.Run(ctx)

	producers := api.NewProducerServiceClient(admin)
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
	_, err = producers.Adopt(ctx, api.ProducerAdoptRequest_builder{
		Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	// Every play that ended is a lamina of its own: as many committed
	// laminae as capture restarts (the play in progress has none yet), and
	// at least four of them. A play folded into the lamina before it would
	// leave the count one short for good, the plays being identical bytes
	// the node takes as a retry.
	laminae := api.NewLaminaServiceClient(admin)
	var committed []*api.Lamina
	require.Eventually(t, func() bool {
		me, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		var restarts int64
		for _, r := range me.GetStatus().GetSources() {
			restarts += r.GetCaptureRestarts()
		}
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    100,
		}.Build())
		if err != nil {
			return false
		}
		committed = committed[:0]
		for _, o := range vs.GetItems() {
			switch o.GetState() {
			case api.LaminaState_LAMINA_STATE_COMMITTED:
				committed = append(committed, o)
			case api.LaminaState_LAMINA_STATE_LOST:
				t.Fatalf("lamina %x is LOST", o.GetId())
			}
		}

		return len(committed) >= 4 && int64(len(committed)) >= restarts
	}, 120*time.Second, 500*time.Millisecond, "every ended play is a committed lamina")
	keys := map[string]bool{}
	for _, o := range committed {
		keys[o.GetLaminaKey()] = true
	}
	require.Len(t, keys, len(committed), "one key per lamina")

	// They tile the recording, each ending before the next begins, and
	// at least two share a slot: the plays are seconds, the slots minutes.
	sort.Slice(committed, func(i, j int) bool {
		return committed[i].GetDateStarted().AsTime().Before(committed[j].GetDateStarted().AsTime())
	})
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{
		Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 1)
	src := sources.GetItems()[0]
	d := time.Duration(src.GetProfile().GetDurationSeconds()) * time.Second
	phase := core.Phase(set.GetId(), int(src.GetOrdinal()), 1, d)
	slots := map[time.Time]int{}
	for i, o := range committed {
		require.NotNil(t, o.GetDateEnded(), "a segment the camera ended has its end")
		if i > 0 {
			require.False(t, o.GetDateStarted().AsTime().Before(committed[i-1].GetDateEnded().AsTime()), "segments do not overlap")
		}
		slot := o.GetDateStarted().AsTime().Add(-phase).Truncate(d).Add(phase)
		slots[slot]++
	}
	most := 0
	for _, n := range slots {
		most = max(most, n)
	}
	require.GreaterOrEqual(t, most, 2, "a slot holds the segments of every return: %v", slots)
}
