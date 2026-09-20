package mpegts

import (
	"os"
	"testing"
)

// The tables may change under a session (§38.7): the same Opus stream on
// another PID follows the first, and the demuxer follows the new PMT.
func TestDemuxerFollowsNewTables(t *testing.T) {
	a, err := os.ReadFile("testdata/opus.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	b, err := os.ReadFile("testdata/opus2.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	d := New()
	_, au1, err := d.WriteAll(a[:len(a)-len(a)%PacketSize])
	if err != nil {
		t.Fatal(err)
	}
	_, au2, err := d.WriteAll(b[:len(b)-len(b)%PacketSize])
	if err != nil {
		t.Fatal(err)
	}
	if !d.HasOpus() {
		t.Fatal("no Opus stream")
	}
	if len(au1) < 240 || len(au2) < 240 {
		t.Fatalf("%d then %d packets: the second table was not followed", len(au1), len(au2))
	}
}
