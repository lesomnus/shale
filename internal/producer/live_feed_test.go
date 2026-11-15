package producer

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/satellite"
)

func TestParseLiveFeed(t *testing.T) {
	for in, want := range map[string]LiveFeed{"": LiveFeedAuto, "auto": LiveFeedAuto, " Producer ": LiveFeedProducer} {
		v, err := ParseLiveFeed(in)
		require.NoError(t, err, in)
		require.Equal(t, want, v, in)
	}
	_, err := ParseLiveFeed("satellite")
	require.Error(t, err)
}

// liveSegment is an open segment of a source the satellite may feed.
func liveSegment(body string) *Segment {
	seg := NewSegment(time.Now(), []byte(body))
	seg.Live = true

	return seg
}

// §41.8: a live request through the satellite, of a source whose live may
// be handed over, says where its live begins; the producer then hands the
// source's live over, and says so at once in its next heartbeat. A live
// request that goes directly takes it back, and so do `live_feed:
// producer`, a segment the satellite may not feed, and `direct`.
func TestUploadHandsTheLiveToTheSatellite(t *testing.T) {
	node, sat := newHost(t), newHost(t)
	src := pdid.New(8)
	at := func(h *host, key string) *api.Allocation {
		al := allocationAt(t, h.srv.Listener.Addr().String(), key)
		al.SetSourceId(src.Bytes())
		return al
	}
	upload := func(link *SatelliteLink, key string, seg *Segment) {
		t.Helper()
		done := make(chan Result, 1)
		go func() { done <- uploaderVia(link, noLaminae{}).Upload(context.Background(), at(node, key), seg) }()
		time.Sleep(50 * time.Millisecond)
		seg.Write([]byte(" more"))
		seg.Close(time.Now())
		res := <-done
		require.True(t, res.Stored, "%v", res.Err)
	}

	link := NewSatelliteLink(ViaAuto, true, slog.New(slog.DiscardHandler))
	link.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
	woke := 0
	link.wake = func() { woke++ }
	upload(link, "laminae/live", liveSegment("live"))
	require.Equal(t, "0", sat.header.Get(satellite.HdrLive), "live from where the request begins")
	require.Equal(t, [][]byte{src.Bytes()}, link.Handed())
	require.Equal(t, 1, woke, "the heartbeat says so at once")
	rep := link.report()
	require.Equal(t, [][]byte{src.Bytes()}, rep.GetLiveSources())
	require.Equal(t, "auto", rep.GetLiveFeed())

	// The same again changes nothing.
	upload(link, "laminae/again", liveSegment("again"))
	require.Equal(t, 1, woke)

	// A closed segment is no live: nothing said, nothing changed.
	res := uploaderVia(link, noLaminae{}).Upload(context.Background(), at(node, "laminae/closed"), closedSegment("closed"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Empty(t, sat.header.Get(satellite.HdrLive))
	require.Len(t, link.Handed(), 1)

	// One the satellite may not feed: taken back.
	seg := liveSegment("helper")
	seg.Live = false
	upload(link, "laminae/helper", seg)
	require.Empty(t, sat.header.Get(satellite.HdrLive))
	require.Empty(t, link.Handed())
	require.Equal(t, 2, woke)

	// Directly, on `auto` after a failure: taken back.
	upload(link, "laminae/back", liveSegment("back"))
	require.Len(t, link.Handed(), 1)
	link.failed(io.EOF)
	upload(link, "laminae/direct", liveSegment("direct"))
	require.Empty(t, node.header.Get(satellite.HdrLive), "a node is never told")
	require.Empty(t, link.Handed())

	// `live_feed: producer` hands nothing over.
	keep := NewSatelliteLink(ViaSatellite, true, slog.New(slog.DiscardHandler))
	keep.SetLiveFeed(LiveFeedProducer)
	keep.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
	upload(keep, "laminae/keep", liveSegment("keep"))
	require.Empty(t, sat.header.Get(satellite.HdrLive))
	require.Empty(t, keep.Handed())
	require.Equal(t, "producer", keep.report().GetLiveFeed())

	// `direct` reports nothing, and says nothing.
	direct := NewSatelliteLink(ViaDirect, true, slog.New(slog.DiscardHandler))
	upload(direct, "laminae/plain", liveSegment("plain"))
	require.Empty(t, node.header.Get(satellite.HdrLive))
	require.Nil(t, direct.report())
}

// The live of a request begins where it begins when the bytes there are
// recent, else at the first written since, else where the next will be:
// a backlog is not live.
func TestSegmentLiveFrom(t *testing.T) {
	seg := NewSegment(time.Now(), []byte("init"))
	seg.Write([]byte("one"))
	seg.Write([]byte("two"))
	now := time.Now()
	require.EqualValues(t, 0, seg.LiveFrom(0, now, time.Minute))
	require.EqualValues(t, 5, seg.LiveFrom(5, now, time.Minute))
	later := now.Add(10 * time.Second)
	require.EqualValues(t, 10, seg.LiveFrom(0, later, time.Second), "nothing recent: the end")
	seg.marks[2].at = later
	require.EqualValues(t, 7, seg.LiveFrom(0, later, time.Second), "from the first recent write")
	require.EqualValues(t, 8, seg.LiveFrom(8, later, time.Second))
	seg.Release(8)
	require.Len(t, seg.marks, 1)
	require.EqualValues(t, 8, seg.LiveFrom(8, later, time.Second))
}

// A stream whose audio needs the live helper stays on the producer's tee;
// one with Opus, or no audio, may be handed over; nothing is under
// `live_feed: producer` or `direct` (§41.8).
func TestLiveFeedable(t *testing.T) {
	read := func(name string) []byte {
		b, err := os.ReadFile("testdata/" + name)
		require.NoError(t, err)
		r := NewMP4Reader(bytes.NewReader(b))
		var f Frame
		require.NoError(t, r.Next(&f))
		require.Equal(t, FramePrefix, f.Kind)
		return f.Payload
	}
	for _, c := range []struct {
		name     string
		via      Via
		feed     LiveFeed
		prefix   []byte
		feedable bool
	}{
		{"opus", ViaSatellite, LiveFeedAuto, read("av.mp4"), true},
		{"video only", ViaAuto, LiveFeedAuto, read("sample.mp4"), true},
		{"aac", ViaSatellite, LiveFeedAuto, read("aac.mp4"), false},
		{"raw", ViaSatellite, LiveFeedAuto, []byte("a raw prefix"), false},
		{"producer", ViaSatellite, LiveFeedProducer, read("av.mp4"), false},
		{"direct", ViaDirect, LiveFeedAuto, read("av.mp4"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := &Producer{cfg: Config{Upload: UploadConfig{Via: c.via, LiveFeed: c.feed}}, satellite: NewSatelliteLink(c.via, true, nil)}
			require.Equal(t, c.feedable, p.liveFeedable(&source{prefix: c.prefix}))
		})
	}
}
