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
	"github.com/lesomnus/shale/internal/producer"
)

// TestDarkSkip runs a producer whose source skips the segments of a dark
// scene (§38.10): the test is the ffmpeg that measures it, feeding
// blackframe's line once a second. After `dark_after` the segments that
// open are skipped, the CP's timeline shows the span as DARK rather than
// NOT_RECEIVED, the heartbeat says so, and the first lit second stores
// again from the segment that was open.
func TestDarkSkip(t *testing.T) {
	sample := samplePath(t)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	ops := c.dialCluster("@cluster/ops")

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
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "office",
	}.Build())
	require.NoError(t, err)

	dir := t.TempDir()
	const darkAfter = 3 * time.Second
	cfg := producer.Config{
		StateDir: filepath.Join(dir, "producer"),
		Cp:       "http://" + c.running.TenantAddr,
		Dev:      true,
		Sources: []producer.SourceConfig{
			{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264", Idle: &producer.IdleConfig{DarkAfter: darkAfter}},
		},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
	}
	p, err := producer.New(cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

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
		Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(),
		Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	select {
	case <-p.Ready:
	case err := <-done:
		t.Fatalf("producer: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the producer did not start recording")
	}

	laminae := api.NewLaminaServiceClient(admin)
	inState := func(st api.LaminaState) []*api.Lamina {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    200,
		}.Build())
		if err != nil {
			return nil
		}
		var out []*api.Lamina
		for _, o := range vs.GetItems() {
			if o.GetState() == st {
				out = append(out, o)
			}
		}

		return out
	}
	dark := func() bool {
		v, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
		if err != nil || len(v.GetStatus().GetSources()) == 0 {
			return false
		}

		return v.GetStatus().GetSources()[0].GetDark()
	}

	// Lit: the recording is stored.
	require.Eventually(t, func() bool { return len(inState(api.LaminaState_LAMINA_STATE_COMMITTED)) >= 1 }, 60*time.Second, 200*time.Millisecond, "a lit scene is stored")
	require.False(t, dark())

	// The lights go off: blackframe's line once a second.
	const line = "[Parsed_blackframe_2 @ 0xaaaa] frame:1 pblack:100 pts:1 t:1.000000 type:I last_keyframe:1"
	lit := make(chan struct{})
	go func() {
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-lit:
				return
			case <-tk.C:
				p.Observe("door", line)
			}
		}
	}()
	require.Eventually(t, dark, 30*time.Second, 200*time.Millisecond, "the heartbeat says dark after dark_after")
	darkSince := time.Now()
	var skipped []*api.Lamina
	require.Eventually(t, func() bool {
		skipped = inState(api.LaminaState_LAMINA_STATE_SKIPPED)
		return len(skipped) >= 1
	}, 60*time.Second, 200*time.Millisecond, "a segment opened in the dark is skipped at its close")
	for _, o := range skipped {
		require.Equal(t, api.LaminaSkipReason_LAMINA_SKIP_REASON_DARK, o.GetSkipReason())
		require.True(t, o.HasDateEnded(), "a skipped lamina carries its span")
		require.Zero(t, o.GetSize())
	}
	// Nothing was lost: skipping is not failing.
	require.Empty(t, inState(api.LaminaState_LAMINA_STATE_LOST))

	// The lights come on: the lines stop, and storing resumes with the
	// segment that was open, so a lamina starts before the light did.
	close(lit)
	require.Eventually(t, func() bool { return !dark() }, 30*time.Second, 200*time.Millisecond, "lit within seconds of the lines stopping")
	litAt := time.Now()
	require.Eventually(t, func() bool {
		for _, o := range inState(api.LaminaState_LAMINA_STATE_COMMITTED) {
			if o.GetDateStarted().AsTime().After(darkSince) {
				return true
			}
		}

		return false
	}, 60*time.Second, 200*time.Millisecond, "stored again once lit")
	var resumed *api.Lamina
	for _, o := range inState(api.LaminaState_LAMINA_STATE_COMMITTED) {
		if o.GetDateStarted().AsTime().After(darkSince) && (resumed == nil || o.GetDateStarted().AsTime().Before(resumed.GetDateStarted().AsTime())) {
			resumed = o
		}
	}
	require.NotNil(t, resumed)
	require.True(t, resumed.GetDateStarted().AsTime().Before(litAt), "the segment open when the light came on is kept whole: pre-roll")

	// The timeline names the dark span for what it was.
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set:  api.SetRef_builder{Id: set.GetId()}.Build(),
		From: timestamppb.New(time.Now().Add(-10 * time.Minute)),
		To:   timestamppb.New(time.Now().Add(time.Minute)),
		Size: 500,
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	darkGaps := 0
	for _, g := range tl.GetSources()[0].GetGaps() {
		if g.GetReason() == api.GapReason_GAP_REASON_DARK {
			darkGaps++
			require.True(t, g.GetTo().AsTime().After(g.GetFrom().AsTime()))
		}
	}
	require.GreaterOrEqual(t, darkGaps, 1, "the skipped span is a DARK gap: %v", tl.GetSources()[0].GetGaps())

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the producer did not stop")
	}
}
