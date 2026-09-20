package producer

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sample is a 15 s recording from the bench (Raspberry Pi hardware
// encoder, 1080p30, 2 Mbps), fetched from the Pi into the scratchpad; the
// tests skip without it.
func sample(t *testing.T) []byte {
	t.Helper()
	for _, p := range []string{
		os.Getenv("SHALE_SAMPLE_TS"),
		filepath.Join("testdata", "sample.ts"),
		"/tmp/claude-1000/-workspace/e1dcad89-586a-4215-9245-85bafb989871/scratchpad/sample.ts",
	} {
		if p == "" {
			continue
		}
		if b, err := os.ReadFile(p); err == nil {
			return b
		}
	}
	t.Skip("no sample recording; set SHALE_SAMPLE_TS")

	return nil
}

func TestReaderFindsTablesAndKeyframes(t *testing.T) {
	x := require.New(t)
	b := sample(t)
	r := NewReader(bytes.NewReader(b))

	var p Packet
	frames, keys, n := 0, 0, 0
	for {
		err := r.Next(&p)
		if err == io.EOF {
			break
		}
		x.NoError(err)
		n++
		if r.IsVideoFrame(&p) {
			frames++
		}
		if r.IsKeyframe(&p) {
			keys++
		}
	}
	x.Equal(len(b)/PacketSize, n)
	s := r.Streams()
	x.Equal(uint16(4096), s.PmtPID)
	x.Equal(uint16(256), s.VideoPID)
	x.Equal(byte(0x1b), s.Video)
	x.Equal(450, frames, "30 fps for 15 s")
	// The Pi's encoder flags every frame; the IDR slices are the keyframes.
	x.Equal(8, keys)
	x.Len(r.Tables(), 2*PacketSize)
}

func TestReaderResyncs(t *testing.T) {
	x := require.New(t)
	b := sample(t)
	// Junk before the stream and a torn packet in the middle.
	torn := append([]byte("garbage-garbage"), b[:PacketSize*100]...)
	torn = append(torn, b[PacketSize*100+7:]...)
	r := NewReader(bytes.NewReader(torn))
	var p Packet
	n := 0
	for {
		if err := r.Next(&p); err != nil {
			break
		}
		n++
	}
	x.InDelta(len(b)/PacketSize, n, 2)
}

func TestCutterSegmentsPlayOnTheirOwn(t *testing.T) {
	x := require.New(t)
	b := sample(t)
	r := NewReader(bytes.NewReader(b))

	// A clock that advances a frame per video PES start, so the recording
	// plays at 30 fps in test time.
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	sched := Schedule{Duration: 4 * time.Second, Phase: 0}
	var out []*Segment
	c := &Cutter{
		Reader:   r,
		Schedule: func() Schedule { return sched },
		Out:      func(s *Segment) { out = append(out, s) },
		Now:      func() time.Time { return now },
	}
	var p Packet
	for {
		err := r.Next(&p)
		if err == io.EOF {
			break
		}
		x.NoError(err)
		if r.IsVideoFrame(&p) {
			now = now.Add(time.Second / 30)
		}
		c.Feed(&p)
	}
	c.Stop()

	// 15 s at 4 s segments cut at 2 s keyframes: boundaries at the first
	// keyframe after 4, 8, and 12 s, then the stop.
	x.GreaterOrEqual(len(out), 4)
	total := 0
	for i, s := range out {
		d := s.Bytes()
		total += len(d) - 2*PacketSize
		// PAT, PMT, then a keyframe of the video stream.
		x.Equal(byte(0x47), d[0])
		x.Equal(uint16(0), uint16(d[1]&0x1f)<<8|uint16(d[2]), "segment %d starts with the PAT", i)
		x.Equal(uint16(4096), uint16(d[PacketSize+1]&0x1f)<<8|uint16(d[PacketSize+2]), "then the PMT")
		rr := NewReader(bytes.NewReader(d))
		var q Packet
		x.NoError(rr.Next(&q))
		x.NoError(rr.Next(&q))
		x.NoError(rr.Next(&q))
		x.True(rr.IsKeyframe(&q), "segment %d then a keyframe", i)
		x.True(s.Closed())
		x.False(s.Ended.Before(s.Started))
		if i > 0 {
			x.Equal(out[i-1].Ended, s.Started, "segments tile the timeline")
		}
	}
	// Everything after the first keyframe is in some segment.
	x.Greater(total, len(b)*9/10)
	st := c.Stats()
	x.Equal(int64(8), st.Keyframes)
	x.Equal(int64(0), st.EarlyCuts)
}

func TestEarlyCut(t *testing.T) {
	x := require.New(t)
	b := sample(t)
	r := NewReader(bytes.NewReader(b))
	now := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	// A ceiling far below the stream: every segment is cut early at the
	// next keyframe.
	sched := Schedule{Duration: time.Hour, Ceiling: 200_000}
	var out []*Segment
	c := &Cutter{Reader: r, Schedule: func() Schedule { return sched }, Out: func(s *Segment) { out = append(out, s) }, Now: func() time.Time { return now }}
	var p Packet
	for {
		if err := r.Next(&p); err != nil {
			break
		}
		if r.IsVideoFrame(&p) {
			now = now.Add(time.Second / 30)
		}
		c.Feed(&p)
	}
	c.Stop()
	x.GreaterOrEqual(len(out), 5)
	for _, s := range out[:len(out)-1] {
		x.True(s.Early)
	}
	x.Equal(int64(len(out)-1), c.Stats().EarlyCuts)
}

func TestSegmentReaderStreams(t *testing.T) {
	x := require.New(t)
	s := newSegment(time.Now(), time.Now(), []byte("ab"))
	rd := s.ReaderFrom(0)
	go func() {
		time.Sleep(20 * time.Millisecond)
		s.Write([]byte("cd"))
		time.Sleep(20 * time.Millisecond)
		s.Close(time.Now())
	}()
	b, err := io.ReadAll(rd)
	x.NoError(err)
	x.Equal("abcd", string(b))

	// From an offset, after the close.
	b, err = io.ReadAll(s.ReaderFrom(3))
	x.NoError(err)
	x.Equal("d", string(b))
}
