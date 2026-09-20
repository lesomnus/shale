package mpegts

import (
	"os"
	"testing"
)

// Five seconds of Opus at 48 kb/s as ffmpeg muxes it: a registration
// descriptor names the stream, and every PES holds control-headed packets.
func TestDemuxOpus(t *testing.T) {
	b, err := os.ReadFile("testdata/opus.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	d := New()
	var units []AudioUnit
	b = b[:len(b)-len(b)%PacketSize]
	_, au, err := d.WriteAll(b)
	if err != nil {
		t.Fatal(err)
	}
	units = append(units, au...)
	d.Flush()
	if !d.HasOpus() {
		t.Fatal("no Opus stream found")
	}
	// 5 s of 20 ms frames is 250 packets.
	if len(units) < 240 || len(units) > 260 {
		t.Fatalf("%d packets", len(units))
	}
	for i, u := range units {
		if len(u.Data) == 0 || u.PTS < 0 {
			t.Fatalf("packet %d: %d bytes, pts %d", i, len(u.Data), u.PTS)
		}
		// An Opus packet starts with a TOC byte; 48 kb/s frames are a few
		// dozen to a few hundred bytes.
		if len(u.Data) > 1500 {
			t.Fatalf("packet %d is %d bytes", i, len(u.Data))
		}
	}
	if d := Duration(units[0].PTS, units[1].PTS, 0); d < 19e6 || d > 21e6 {
		t.Fatalf("frame duration %s", d)
	}
}
