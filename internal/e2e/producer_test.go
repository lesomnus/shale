package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
)

func samplePath(t *testing.T) string {
	t.Helper()
	for _, p := range []string{os.Getenv("SHALE_SAMPLE_TS"), filepath.Join("..", "producer", "testdata", "sample.ts")} {
		if p == "" {
			continue
		}
		if abs, err := filepath.Abs(p); err == nil {
			if _, err := os.Stat(abs); err == nil {
				return abs
			}
		}
	}
	t.Skip("no sample recording")

	return ""
}

// TestProducerRecords runs the producer against the cluster with the bench
// recording as its camera (§38): it joins, is adopted for a set, registers
// its source, negotiates, cuts segments at keyframes, uploads them live,
// and the Timeline lists them tiling the recording.
func TestProducerRecords(t *testing.T) {
	sample := samplePath(t)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	admin := c.dial("@acme/admin")
	ops := c.dialCluster("@cluster/ops")

	// Small laminae, so a 15 s recording makes several: an UploadPolicy of
	// the test's own, activated through the cluster API.
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias:   "small",
		Version: 1,
		Bounds: api.UploadBounds_builder{
			MinLamina:    512 << 10,
			TargetLamina: 1 << 20,
			MaxLamina:    4 << 20,
		}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewUploadPolicyServiceClient(ops).Activate(ctx, api.UploadPolicyActivateRequest_builder{
		Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "lobby",
	}.Build())
	require.NoError(t, err)

	pdir := filepath.Join(t.TempDir(), "producer")
	cfg := producer.Config{
		StateDir: pdir,
		Cp:       "http://" + c.running.TenantAddr,
		Dev:      true,
		Sources: []producer.SourceConfig{{
			Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264",
		}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
	}
	p, err := producer.New(cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// The producer joins and waits; the admin adopts it for the set.
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
	adopted, err := producers.Adopt(ctx, api.ProducerAdoptRequest_builder{
		Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(),
		Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, api.HostState_HOST_STATE_ADOPTED, adopted.GetState())

	select {
	case <-p.Ready:
	case err := <-done:
		t.Fatalf("producer: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the producer did not start recording")
	}

	// The source was registered from the configuration with ordinal 0.
	sources := api.NewSourceServiceClient(admin)
	var src *api.Source
	require.Eventually(t, func() bool {
		vs, err := sources.List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
		}.Build())
		if err != nil || len(vs.GetItems()) == 0 {
			return false
		}
		src = vs.GetItems()[0]

		return true
	}, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, "door", src.GetAlias())
	require.Equal(t, int32(0), src.GetOrdinal())
	require.Equal(t, int64(2_000_000), src.GetProfile().GetMaxBitrate())
	// 1 MiB at 2 Mbps is about 4 s per segment.
	require.LessOrEqual(t, src.GetProfile().GetDurationSeconds(), int64(16))

	// Segments commit as the recording plays; the recording loops, so they
	// keep coming.
	laminae := api.NewLaminaServiceClient(admin)
	var committed []*api.Lamina
	require.Eventually(t, func() bool {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build()},
			Size:    100,
		}.Build())
		if err != nil {
			return false
		}
		committed = committed[:0]
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				committed = append(committed, o)
			}
		}

		return len(committed) >= 3
	}, 90*time.Second, 500*time.Millisecond, "segments commit")

	// The heartbeat reached the CP.
	me, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: adopted.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotNil(t, me.GetDateSeen())
	require.NotNil(t, me.GetStatus())

	// Timeline: the committed segments tile the span they cover, each one
	// starting with the tables and a keyframe.
	from := committed[0].GetDateStarted().AsTime().Add(-time.Second)
	to := time.Now().Add(time.Minute)
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
		From:   timestamppb.New(from), To: timestamppb.New(to),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	objs := tl.GetSources()[0].GetLaminae()
	require.GreaterOrEqual(t, len(objs), 3)
	for i, o := range objs {
		require.Equal(t, api.ReadState_READ_STATE_AVAILABLE, o.GetState())
		require.False(t, o.GetEndedEstimated(), "a complete segment declares its end")
		if i > 0 {
			require.WithinDuration(t, objs[i-1].GetDateEnded().AsTime(), o.GetDateStarted().AsTime(), time.Millisecond, "segments tile")
		}
		b := fetch(t, o.GetUrl())
		require.Equal(t, int64(len(b)), o.GetSize())
		require.Equal(t, byte(0x47), b[0])
		r := producer.NewReader(bytesReader(b))
		var pk producer.Packet
		require.NoError(t, r.Next(&pk))
		require.Equal(t, uint16(0), pk.PID, "starts with the PAT")
		require.NoError(t, r.Next(&pk))
		require.NoError(t, r.Next(&pk))
		require.True(t, r.IsKeyframe(&pk), "then a keyframe")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
}
