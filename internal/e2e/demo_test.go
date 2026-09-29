package e2e_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
)

// TestDemoProducer is §38.1's demo input from end to end: a producer with
// nothing attached records three patterns ffmpeg draws for itself, every
// one of them registers as a camera and commits laminae, and what comes
// back is video with sound in it. This is the tutorial's producer, so it
// is the tutorial that breaks when it stops working.
func TestDemoProducer(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	ops := c.dialCluster("@cluster/ops")

	// Laminae of a few MB, so each camera stores one inside the test.
	policies := api.NewUploadPolicyServiceClient(ops)
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "short-laminae", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 1 << 20, TargetLamina: 2 << 20, MaxLamina: 16 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "demo",
	}.Build())
	require.NoError(t, err)

	const cameras = 3
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Sources:           producer.DemoSources(cameras),
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	// Every demo source registers as a camera of the set, with the alias
	// and the mode the pattern was made with.
	sources := api.NewSourceServiceClient(admin)
	var srcs []*api.Source
	require.Eventually(t, func() bool {
		vs, err := sources.List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    10,
		}.Build())
		if err != nil {
			return false
		}
		srcs = vs.GetItems()

		return len(srcs) == cameras
	}, 60*time.Second, 500*time.Millisecond, "the three demo cameras registered")
	aliases := map[string]bool{}
	for _, s := range srcs {
		aliases[s.GetAlias()] = true
		require.Equal(t, int64(2_000_000), s.GetProfile().GetMaxBitrate(), "%s: §38.5's starting ceiling for 720p30", s.GetAlias())
	}
	require.Equal(t, map[string]bool{"demo-01": true, "demo-02": true, "demo-03": true}, aliases)

	// And records: a committed lamina from each of them.
	laminae := api.NewLaminaServiceClient(admin)
	stored := map[string]int{}
	require.Eventually(t, func() bool {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    200,
		}.Build())
		if err != nil {
			return false
		}
		stored = map[string]int{}
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				stored[string(o.GetSource().GetId())]++
			}
		}

		return len(stored) == cameras
	}, 180*time.Second, time.Second, "every demo camera stored a lamina: %v", stored)
	require.Zero(t, p.Stats().Lost)

	// What was stored plays: the tables, a keyframe, and both a picture
	// and a sound, which is what makes it a sample worth watching.
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set:  api.SetRef_builder{Id: set.GetId()}.Build(),
		From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.New(time.Now().Add(time.Hour)), Size: 20,
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), cameras)
	for _, ts := range tl.GetSources() {
		require.NotEmpty(t, ts.GetLaminae())
		b := fetch(t, ts.GetLaminae()[0].GetUrl())
		r := producer.NewReader(bytesReader(b))
		for {
			var pk producer.Packet
			if err := r.Next(&pk); err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
		}
		st := r.Streams()
		require.NotZero(t, st.VideoPID, "a picture")
		require.Equal(t, byte(0x1B), st.Video, "H.264")
		require.NotEmpty(t, st.AudioPIDs, "and a sound")
		require.Equal(t, []byte{0x0F}, st.AudioTypes, "AAC, which every player finds")
		require.False(t, st.AudioAnon)
	}
}
