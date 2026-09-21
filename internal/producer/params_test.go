package producer

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/mpegts"
)

// readAll reads a stream through one reader, answering every packet.
func readAll(t *testing.T, r *Reader, path string) []*Packet {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", path))
	require.NoError(t, err)
	r.r.Reset(bytes.NewReader(b))
	var ps []*Packet
	for {
		p := new(Packet)
		if err := r.Next(p); err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		ps = append(ps, p)
	}

	return ps
}

// TestParamSets is §38.2's parameter sets: a keyframe that comes without
// them gets the last ones seen, as TS packets in front of it that a
// demuxer reads as one PES stamped like the keyframe (#84).
func TestParamSets(t *testing.T) {
	// The sample stream carries them with every keyframe: nothing to add,
	// and the reader remembers them.
	r := NewReader(bytes.NewReader(nil))
	var sampleKeys int
	for _, p := range readAll(t, r, "sample.ts") {
		if r.IsKeyframe(p) {
			sampleKeys++
			require.Nil(t, r.ParamSets(p))
		}
	}
	require.Greater(t, sampleKeys, 0)
	require.NotNil(t, r.Params())
	require.Zero(t, r.NoParams)
	ps := r.Params()

	// A bare stream through a reader that has seen none: nothing to add
	// either, and the keyframes are counted.
	bare := NewReader(bytes.NewReader(nil))
	var bareKeys int
	for _, p := range readAll(t, bare, "nosps.ts") {
		if bare.IsKeyframe(p) {
			bareKeys++
			require.Nil(t, bare.ParamSets(p))
		}
	}
	require.GreaterOrEqual(t, bareKeys, 1)
	require.Equal(t, int64(bareKeys), bare.NoParams)

	// The same bare stream through the reader that remembers: the
	// keyframe gets a PES of its own in front, on the video PID, whose
	// continuity counter runs into the keyframe's.
	packets := readAll(t, r, "nosps.ts")
	var key *Packet
	for _, p := range packets {
		if r.IsKeyframe(p) {
			key = p
			break
		}
	}
	require.NotNil(t, key)
	added := r.ParamSets(key)
	require.NotNil(t, added)
	require.Equal(t, 0, len(added)%PacketSize)
	first := new(Packet)
	copy(first.Data[:], added)
	r.parse(first)
	require.Equal(t, r.Streams().VideoPID, first.PID)
	require.True(t, first.PUSI)
	require.Equal(t, (key.CC-1)&0x0f, first.CC)

	// Tables, the parameter sets, then the stream from the keyframe on:
	// what a lamina cut there holds. A demuxer reads the parameter sets
	// as a unit stamped like the keyframe, then the keyframe.
	var lamina []byte
	lamina = append(lamina, r.Tables()...)
	lamina = append(lamina, added...)
	from := false
	for _, p := range packets {
		if p == key {
			from = true
		}
		if from {
			lamina = append(lamina, p.Data[:]...)
		}
	}
	if out := os.Getenv("SHALE_DUMP_LAMINA"); out != "" {
		require.NoError(t, os.WriteFile(out, lamina, 0o644))
	}
	d := mpegts.New()
	units, err := d.Write(lamina)
	require.NoError(t, err)
	units = append(units, d.Flush()...)
	require.GreaterOrEqual(t, len(units), 2)
	require.Equal(t, ps, units[0].Data)
	require.Equal(t, units[1].PTS, units[0].PTS)
	require.True(t, units[1].Keyframe)
	require.Equal(t, ps, d.Params())

	// The cutter does the same at every segment it starts on a bare
	// keyframe: tables, the parameter sets, the keyframe.
	rr := NewReader(bytes.NewReader(nil))
	readAll(t, rr, "sample.ts")
	require.NotNil(t, rr.Params())
	b, err := os.ReadFile(filepath.Join("testdata", "nosps.ts"))
	require.NoError(t, err)
	rr.r.Reset(bytes.NewReader(b))
	now := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	var segs []*Segment
	c := &Cutter{
		Reader:   rr,
		Schedule: func() Schedule { return Schedule{Duration: 4 * time.Second} },
		Out:      func(s *Segment) { segs = append(segs, s) },
		Now:      func() time.Time { return now },
	}
	for {
		p := new(Packet)
		if err := rr.Next(p); err != nil {
			break
		}
		if rr.IsVideoFrame(p) {
			now = now.Add(time.Second / 30)
		}
		c.Feed(p)
	}
	c.Stop()
	require.NotEmpty(t, segs)
	require.Equal(t, int64(len(segs)), c.Stats().ParamsAdded)
	seg := segs[0].Bytes()
	third := new(Packet)
	copy(third.Data[:], seg[2*PacketSize:])
	rr.parse(third)
	require.Equal(t, rr.Streams().VideoPID, third.PID)
	require.True(t, third.PUSI)
	require.Equal(t, 7, mpegts.NALType(mpegts.NALUnits(pesES(third.Data[third.Payload:]))[0], mpegts.StreamH264))
}
