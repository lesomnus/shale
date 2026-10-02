package producer

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/fmp4"
)

// A capture's fragmented MP4, cut (§38.1, §38.2): the recording through the
// MP4 reader and the cutter, as a capture's stdout goes. Every lamina is
// the init segment, then fragments from one that begins with a keyframe,
// then the index of its key fragments; together they hold every fragment
// once, in order.
func TestCutterMP4(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	require.NoError(t, err)

	// The clock is the recording's own: the video's decode time, from a
	// whole second, so the 2 s slots fall on its keyframes.
	t0 := time.Date(2026, 9, 21, 7, 0, 0, 0, time.UTC)
	clock := t0
	var prefix []byte
	var out []*Segment
	c := &Cutter{
		Schedule: func() Schedule { return Schedule{Duration: 2 * time.Second, Ceiling: 1 << 30} },
		Now:      func() time.Time { return clock },
		Prefix:   func() []byte { return prefix },
		OnPrefix: func(b []byte) { prefix = b },
		Out:      func(s *Segment) { out = append(out, s) },
	}

	r := NewMP4Reader(bytes.NewReader(b))
	var frags [][]byte
	keys := 0
	first := int64(-1)
	for {
		var f Frame
		err := r.Next(&f)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if f.Kind == FrameData {
			frags = append(frags, f.Payload)
			if f.Frames > 0 {
				if first < 0 {
					first = f.Ticks
				}
				ts := int64(r.Init().Video().Timescale)
				clock = t0.Add(time.Duration((f.Ticks - first) * int64(time.Second) / ts))
			}
			if f.Key {
				keys++
			}
		}
		c.Feed(&f)
	}
	c.Stop()

	init := r.Init()
	require.NotNil(t, init)
	video := init.Video()
	require.NotNil(t, video)
	require.Equal(t, 3, keys, "6 s with a keyframe every 2 s")
	require.Len(t, out, keys, "one lamina from each keyframe")
	require.True(t, out[len(out)-1].Stopped)

	var got [][]byte
	for i, seg := range out {
		lamina := seg.Bytes()
		require.True(t, bytes.HasPrefix(lamina, init.Bytes), "lamina %d starts with the init segment", i)

		units := fmp4.NewReader(bytes.NewReader(lamina))
		u, err := units.Next()
		require.NoError(t, err)
		require.NotNil(t, u.Init, "lamina %d: the init segment first", i)
		var mine []*fmp4.Fragment
		for {
			u, err := units.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			require.NotNil(t, u.Frag)
			mine = append(mine, u.Frag)
			got = append(got, u.Frag.Bytes)
		}
		require.NotEmpty(t, mine)
		require.True(t, mine[0].Key(init), "lamina %d starts at a keyframe", i)

		// The index: found from the last 16 bytes, for the video track, and
		// every entry a key fragment's moof in this lamina.
		n := fmp4.MfraSize(lamina)
		require.Positive(t, n, "lamina %d ends with its index", i)
		track, idx, err := fmp4.ParseMfra(lamina[len(lamina)-n:])
		require.NoError(t, err)
		require.Equal(t, video.ID, track)
		require.Len(t, idx, 1, "one keyframe in each 2 s lamina")
		require.Equal(t, int64(len(init.Bytes)), idx[0].Offset, "the first fragment, right after the init segment")
		require.Equal(t, "moof", string(lamina[idx[0].Offset+4:idx[0].Offset+8]))
		require.Equal(t, mine[0].Traf(video.ID).Time, idx[0].Time)
	}
	require.Equal(t, frags, got, "every fragment once, in order")
}
