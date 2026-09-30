package relay

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The recent window (§39.4): fragments kept as they came, handed out from
// a key fragment on with the init segment in front, trimmed by time and by
// budget, and never from before a tear.
func TestRing(t *testing.T) {
	r := newRing()
	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	init := []byte("INIT")
	// Ten fragments of ten bytes, one per second, 15360 ticks each; the
	// keys are the fragments at 0, 3, 6 and 9.
	var offs []int64
	for i := range 10 {
		b := bytes.Repeat([]byte{byte('a' + i)}, 10)
		at := t0.Add(time.Duration(i) * time.Second)
		off := r.add(b, at)
		offs = append(offs, off)
		if i%3 == 0 {
			r.key(off, int64(i)*15360, at)
		}
		r.unit(int64(i+1) * 15360)
	}
	require.Equal(t, int64(100), r.bytes)
	require.Equal(t, []int64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90}, offs)

	// The whole window starts at the oldest key, and lasts the ten
	// fragments from it.
	k, ok := r.start(time.Time{})
	require.True(t, ok)
	require.Equal(t, int64(0), k.off)
	out := r.snapshot(k, init)
	require.Equal(t, "INITaaaaaaaaaabbbbbbbbbb", string(out[:24]))
	require.Len(t, out, 4+100)
	require.InDelta(t, 10.0, r.seconds(k, 15360), 0.01)

	// Since 5 s ago: the first key that came from then on.
	k, ok = r.start(t0.Add(5 * time.Second))
	require.True(t, ok)
	require.Equal(t, int64(60), k.off)
	out = r.snapshot(k, init)
	require.Equal(t, "INITgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj", string(out))
	require.InDelta(t, 4.0, r.seconds(k, 15360), 0.01)

	// A tear at the fragment at 40 (fragments went missing before it):
	// the key at 30 is from before the gap, the one at 60 is the start.
	r.tear(40)
	k, ok = r.start(time.Time{})
	require.True(t, ok)
	require.Equal(t, int64(60), k.off)

	// Trimming by time drops the fragments and the keys that fell off.
	r.trim(t0.Add(4 * time.Second))
	require.Equal(t, int64(60), r.bytes)
	require.Equal(t, int64(40), r.chunks[0].off)
	require.Equal(t, int64(60), r.keys[0].off, "the keys at 0 and 30 fell off with their fragments")

	// Eviction by bytes is the same from the front.
	require.Equal(t, int64(30), r.evict(25), "whole fragments go")
	require.Equal(t, int64(30), r.bytes)
	require.Equal(t, int64(70), r.chunks[0].off)
	require.Equal(t, int64(90), r.keys[0].off)

	// Nothing kept, nothing to start at.
	r.reset()
	require.Zero(t, r.bytes)
	_, ok = r.start(time.Time{})
	require.False(t, ok)
}
