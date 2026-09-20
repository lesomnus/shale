package mpegts

import (
	"os"
	"testing"
	"time"
)

// The bench recording: 450 frames at 30 fps, 8 IDR frames.
func TestDemuxSample(t *testing.T) {
	b, err := os.ReadFile("../producer/testdata/sample.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	d := New()
	var units []AccessUnit
	for i := 0; i+PacketSize*64 <= len(b); i += PacketSize * 64 {
		us, err := d.Write(b[i : i+PacketSize*64])
		if err != nil {
			t.Fatal(err)
		}
		units = append(units, us...)
	}
	rest := len(b) - len(b)%PacketSize
	if us, err := d.Write(b[len(units)*0+rest-(rest%(PacketSize*64)) : rest]); err == nil {
		units = append(units, us...)
	}
	units = append(units, d.Flush()...)

	if d.Codec() != StreamH264 {
		t.Fatalf("codec %#x", d.Codec())
	}
	keys := 0
	for _, u := range units {
		if u.Keyframe {
			keys++
		}
		if u.PTS < 0 {
			t.Fatal("a unit without a PTS")
		}
	}
	if len(units) < 440 || len(units) > 460 {
		t.Fatalf("%d access units", len(units))
	}
	if keys != 8 {
		t.Fatalf("%d keyframes", keys)
	}
	if !units[0].Keyframe {
		t.Fatal("the recording does not start with a keyframe")
	}
	dur := Duration(units[0].PTS, units[1].PTS, 0)
	if dur < 30*time.Millisecond || dur > 40*time.Millisecond {
		t.Fatalf("frame duration %s", dur)
	}
}
