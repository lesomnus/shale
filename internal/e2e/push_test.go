package e2e_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
)

// TestPushedSources runs a producer whose sources are written to its
// listener (§38.9) rather than captured: the bench recording pushed as TS
// is cut at keyframes like a camera's, and a raw stream of framed records
// is cut at frame boundaries, each lamina starting with the prefix frame
// and carrying the data times the frames declared.
func TestPushedSources(t *testing.T) {
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
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "robot",
	}.Build())
	require.NoError(t, err)

	dir := t.TempDir()
	sock := "unix:" + filepath.Join(dir, "push.sock")
	cfg := producer.Config{
		StateDir: filepath.Join(dir, "producer"),
		Cp:       "http://" + c.running.TenantAddr,
		Dev:      true,
		Push:     sock,
		PushIdle: 2 * time.Second,
		Sources: []producer.SourceConfig{
			{Alias: "door", Input: producer.InputPush, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"},
			{Alias: "telemetry", Input: producer.InputPush, Kind: producer.KindRaw, MaxBitrate: 1_000_000, ContentType: "application/x-mcap"},
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

	// The TS source: the recording, pushed three times over, then the
	// stream completed.
	pushErr := make(chan error, 2)
	go func() {
		w := &producer.Pusher{Addr: sock, Alias: "door", Part: 1 << 20}
		var off int64
		for i := 0; i < 3; i++ {
			f, err := os.Open(sample)
			if err != nil {
				pushErr <- err
				return
			}
			off, err = w.Push(ctx, f, off, false)
			f.Close()
			if err != nil {
				pushErr <- err
				return
			}
		}
		_, err := w.Push(ctx, bytes.NewReader(nil), off, true)
		pushErr <- err
	}()

	// The raw source: a prefix, then a record a second for a minute, dated
	// twenty minutes ago as a writer flushing what it kept while offline.
	t0 := time.Now().Add(-20 * time.Minute).Truncate(time.Millisecond)
	const records = 60
	go func() {
		var buf bytes.Buffer
		require.NoError(t, producer.WriteFrame(&buf, &producer.Frame{Kind: producer.FramePrefix, Payload: []byte("HDR\n")}))
		rec := bytes.Repeat([]byte{'r'}, 64<<10)
		for i := 0; i < records; i++ {
			require.NoError(t, producer.WriteFrame(&buf, &producer.Frame{Kind: producer.FrameData, Time: t0.Add(time.Duration(i) * time.Second), Payload: rec}))
		}
		w := &producer.Pusher{Addr: sock, Alias: "telemetry", Part: 256 << 10}
		_, err := w.Push(ctx, &buf, 0, true)
		pushErr <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-pushErr:
			require.NoError(t, err)
		case <-time.After(60 * time.Second):
			t.Fatal("pushing did not finish")
		}
	}

	sources := api.NewSourceServiceClient(admin)
	byAlias := map[string]*api.Source{}
	require.Eventually(t, func() bool {
		vs, err := sources.List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
		}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			byAlias[s.GetAlias()] = s
		}

		return len(byAlias) == 2
	}, 10*time.Second, 100*time.Millisecond)
	// Negotiation told the CP what the bytes are (§38.9, §7).
	require.Equal(t, "video/mp2t", byAlias["door"].GetContentType())
	require.Equal(t, "application/x-mcap", byAlias["telemetry"].GetContentType())

	laminae := api.NewLaminaServiceClient(admin)
	committed := func(src *api.Source, want int) []*api.Lamina {
		var out []*api.Lamina
		require.Eventually(t, func() bool {
			vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
				Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build()},
				Size:    100,
			}.Build())
			if err != nil {
				return false
			}
			out = out[:0]
			for _, o := range vs.GetItems() {
				if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
					out = append(out, o)
				}
			}

			return len(out) >= want
		}, 90*time.Second, 500*time.Millisecond, "%s: laminae commit", src.GetAlias())

		return out
	}

	// TS pushed is TS cut: every lamina starts with the tables and a
	// keyframe.
	door := committed(byAlias["door"], 3)
	first := door[0].GetDateStarted().AsTime()
	for _, o := range door {
		if s := o.GetDateStarted().AsTime(); s.Before(first) {
			first = s
		}
	}
	doorTl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: byAlias["door"].GetId()}.Build(),
		From:   timestamppb.New(first.Add(-time.Second)), To: timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.NoError(t, err)
	require.Len(t, doorTl.GetSources(), 1)
	require.GreaterOrEqual(t, len(doorTl.GetSources()[0].GetLaminae()), 3)
	for _, o := range doorTl.GetSources()[0].GetLaminae()[:3] {
		require.Equal(t, api.ReadState_READ_STATE_AVAILABLE, o.GetState())
		b := fetch(t, o.GetUrl())
		r := producer.NewReader(bytesReader(b))
		var pk producer.Packet
		require.NoError(t, r.Next(&pk))
		require.Equal(t, uint16(0), pk.PID, "starts with the PAT")
		require.NoError(t, r.Next(&pk))
		require.NoError(t, r.Next(&pk))
		require.True(t, r.IsKeyframe(&pk), "then a keyframe")
	}

	// Raw: laminae cut at record boundaries on the schedule, each starting
	// with the prefix, dated by the records rather than by their arrival,
	// tiling the minute, the last one ending where the stream did.
	telemetry := byAlias["telemetry"]
	committed(telemetry, 3)
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: telemetry.GetId()}.Build(),
		From:   timestamppb.New(t0.Add(-time.Second)), To: timestamppb.New(t0.Add(records * time.Second)),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	objs := tl.GetSources()[0].GetLaminae()
	require.GreaterOrEqual(t, len(objs), 3)
	var total int64
	for i, o := range objs {
		require.Equal(t, api.ReadState_READ_STATE_AVAILABLE, o.GetState())
		started := o.GetDateStarted().AsTime()
		offset := started.Sub(t0)
		require.Equal(t, time.Duration(0), offset%time.Second, "a lamina starts at a record's own time: %s", started)
		require.GreaterOrEqual(t, offset, time.Duration(0))
		if i > 0 {
			require.WithinDuration(t, objs[i-1].GetDateEnded().AsTime(), started, time.Millisecond, "laminae tile")
		}
		b := fetch(t, o.GetUrl())
		require.True(t, bytes.HasPrefix(b, []byte("HDR\n")), "starts with the prefix frame")
		require.Equal(t, int64(0), int64(len(b)-4)%(64<<10), "whole records after it")
		total += int64(len(b) - 4)
	}
	require.Equal(t, int64(records*(64<<10)), total, "every record is in exactly one lamina")
	last := objs[len(objs)-1]
	require.False(t, last.GetEndedEstimated(), "a completed stream declares its end")
	require.WithinDuration(t, t0.Add((records-1)*time.Second), last.GetDateEnded().AsTime(), time.Millisecond, "the stream ended at its last record")
	// The query reached a second before the first record and a second
	// past the last: those are the only gaps.
	for _, g := range tl.GetSources()[0].GetGaps() {
		require.Equal(t, api.GapReason_GAP_REASON_NOT_RECEIVED, g.GetReason())
		require.True(t, !g.GetTo().AsTime().After(t0) || !g.GetFrom().AsTime().Before(t0.Add((records-1)*time.Second)),
			"a gap inside the pushed minute: %s to %s", g.GetFrom().AsTime(), g.GetTo().AsTime())
	}

	// A raw source has nothing a relay could show.
	_, err = sources.Live(ctx, api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: telemetry.GetId()}.Build()}.Build())
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "nothing a relay could show")

	// A person's content type outlives the producer's proposal, and labels
	// are what a source is listed by.
	custom := "application/x-robot-log"
	// Commits keep touching the row (its observed rate), so the patch is
	// made against the row as it is now.
	current, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: telemetry.GetId()}.Build()}.Build())
	require.NoError(t, err)
	patched, err := sources.Patch(ctx, api.SourcePatchRequest_builder{
		Ref: api.SourceRef_builder{Id: telemetry.GetId()}.Build(), ContentType: &custom,
		Labels: map[string]string{"robot": "r7"}, DateUpdated: current.GetDateUpdated(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, custom, patched.GetContentType())
	_, err = api.NewSetServiceClient(admin).Negotiate(ctx, api.SetNegotiateRequest_builder{
		Ref: api.SetRef_builder{Id: set.GetId()}.Build(),
		Sources: []*api.SourceProposal{api.SourceProposal_builder{
			Source: api.SourceRef_builder{Id: telemetry.GetId()}.Build(), Profile: patched.GetProfile(), ContentType: "application/x-mcap",
		}.Build()},
	}.Build())
	require.NoError(t, err)
	again, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: telemetry.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, custom, again.GetContentType(), "the person's value stays")
	vs, err := sources.List(ctx, api.SourceListRequest_builder{
		Filters: []*api.SourceFilter{api.SourceFilter_builder{Labels: map[string]string{"robot": "r7"}}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, vs.GetItems(), 1)
	require.Equal(t, "telemetry", vs.GetItems()[0].GetAlias())

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the producer did not stop")
	}
}
