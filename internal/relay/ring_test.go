package relay

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The recent window (§39.4): bytes kept as they came, handed out from a
// keyframe on with the tables in front, trimmed by time and by budget, and
// never from before a tear.
func TestRing(t *testing.T) {
	r := newRing()
	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	tables := []byte("PATPMT")
	// Ten chunks of ten bytes, one per second; keyframes at 0, 25, 55, 85.
	for i := range 10 {
		b := bytes.Repeat([]byte{byte('a' + i)}, 10)
		r.add(b, int64(i*10), t0.Add(time.Duration(i)*time.Second))
	}
	for _, k := range []int64{0, 25, 55, 85} {
		r.key(k, k*3000, byte(k%16), t0.Add(time.Duration(k/10)*time.Second), true)
	}
	r.unit(99 * 3000)
	require.Equal(t, int64(100), r.bytes)

	// The whole window starts at the oldest keyframe.
	k, ok := r.start(time.Time{})
	require.True(t, ok)
	require.Equal(t, int64(0), k.off)
	out := r.snapshot(k, -1, tables, nil, 0x100)
	require.Equal(t, "PATPMT"+string(bytes.Repeat([]byte("a"), 10))[:6], string(out[:12]))
	require.Len(t, out, 6+100)
	require.InDelta(t, 3.3, r.seconds(k), 0.01)

	// Up to where the open unit began: whole frames only.
	out = r.snapshot(k, 93, tables, nil, 0x100)
	require.Len(t, out, 6+93)
	require.Equal(t, "jjj", string(out[len(out)-3:]))

	// Since 5 s ago: the first keyframe that came from then on, and the
	// bytes from it, cut inside a chunk.
	k, ok = r.start(t0.Add(5 * time.Second))
	require.True(t, ok)
	require.Equal(t, int64(55), k.off)
	out = r.snapshot(k, -1, tables, nil, 0x100)
	require.Equal(t, 6+45, len(out))
	require.Equal(t, "fffffgggggggggg", string(out[6:21]))

	// A tear at 60 leaves the keyframe at 85 as the only start.
	r.tear(60)
	k, ok = r.start(time.Time{})
	require.True(t, ok)
	require.Equal(t, int64(85), k.off)

	// Trimming by time drops the chunks and the keyframes that fell off.
	r.trim(t0.Add(3 * time.Second))
	require.Equal(t, int64(70), r.bytes)
	require.Equal(t, int64(30), r.chunks[0].off)
	require.Equal(t, int64(55), r.keys[0].off, "the keyframe at 25 fell off with its chunk")

	// Eviction by bytes is the same from the front.
	require.Equal(t, int64(30), r.evict(25), "whole chunks go")
	require.Equal(t, int64(40), r.bytes)
	require.Equal(t, int64(60), r.chunks[0].off)
	require.Equal(t, int64(85), r.keys[0].off)

	// A keyframe without its parameter sets in-band gets them as packets
	// of their own, between the tables and the bytes.
	r.reset()
	require.Zero(t, r.bytes)
	r.add(bytes.Repeat([]byte("k"), 188), 0, t0)
	r.key(0, 3000, 9, t0, false)
	k, _ = r.start(time.Time{})
	out = r.snapshot(k, -1, tables, []byte{0, 0, 0, 1, 0x67, 1, 2, 3, 0, 0, 0, 1, 0x68, 4}, 0x100)
	require.Len(t, out, 6+188+188)
	require.Equal(t, byte(0x47), out[6], "a TS packet of parameter sets")
	require.Equal(t, byte(8), out[9]&0x0f, "its counter runs up to the keyframe's")
}
