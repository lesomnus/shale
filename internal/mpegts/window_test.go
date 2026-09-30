package mpegts

import (
	"os"
	"testing"
)

// feedAll hands a recording to a demuxer in 64-packet writes and answers
// the units, as the relay's source does.
func feedAll(t *testing.T, d *Demuxer, b []byte) []AccessUnit {
	t.Helper()
	var units []AccessUnit
	step := PacketSize * 64
	for i := 0; i < len(b); i += step {
		end := min(i+step, len(b))
		end -= (end - i) % PacketSize
		us, err := d.Write(b[i:end])
		if err != nil {
			t.Fatal(err)
		}
		units = append(units, us...)
	}

	return append(units, d.Flush()...)
}

// The bytes as they came (§39.4): every unit knows the packet it began
// at, a keyframe's is a unit start on the video PID, the tables are the
// first PAT and PMT, and an intact stream has no gap.
func TestDemuxerOffsets(t *testing.T) {
	b, err := os.ReadFile("../producer/testdata/sample.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	d := New()
	units := feedAll(t, d, b)
	if d.Pos() != int64(len(b)-len(b)%PacketSize) {
		t.Fatalf("pos %d of %d", d.Pos(), len(b))
	}
	if d.Gaps() != 0 {
		t.Fatalf("%d gaps in an intact recording", d.Gaps())
	}
	tables := d.Tables()
	if len(tables) != 2*PacketSize || tables[0] != syncByte || tables[PacketSize] != syncByte {
		t.Fatalf("tables: %d bytes", len(tables))
	}
	if pid := uint16(tables[1]&0x1f)<<8 | uint16(tables[2]); pid != 0 {
		t.Fatalf("the tables start with PID %d, not the PAT", pid)
	}
	keys := 0
	for i, u := range units {
		if u.Torn {
			t.Fatalf("unit %d torn", i)
		}
		p := b[u.Offset : u.Offset+PacketSize]
		pid := uint16(p[1]&0x1f)<<8 | uint16(p[2])
		if p[0] != syncByte || pid != d.VideoPID() || p[1]&0x40 == 0 {
			t.Fatalf("unit %d: offset %d is not a unit start on the video PID", i, u.Offset)
		}
		if p[3]&0x0f != u.CC {
			t.Fatalf("unit %d: CC %d, packet says %d", i, u.CC, p[3]&0x0f)
		}
		if u.Keyframe {
			keys++
			if !u.HasParams {
				t.Fatalf("unit %d: the sample's keyframes carry their parameter sets", i)
			}
		}
		if i > 0 && u.Offset <= units[i-1].Offset {
			t.Fatalf("unit %d: offset %d after %d", i, u.Offset, units[i-1].Offset)
		}
	}
	if keys != 8 {
		t.Fatalf("%d keyframes", keys)
	}
}

// Packets that go missing show as a skip of the continuity counter, and
// the units around the hole are torn while the rest are not.
func TestDemuxerTorn(t *testing.T) {
	b, err := os.ReadFile("../producer/testdata/sample.ts")
	if err != nil {
		t.Skip("no sample:", err)
	}
	// Cut 40 packets out of the middle.
	n := len(b) / PacketSize
	cut := n / 2
	holed := append([]byte(nil), b[:cut*PacketSize]...)
	holed = append(holed, b[(cut+40)*PacketSize:]...)
	d := New()
	units := feedAll(t, d, holed)
	if d.Gaps() == 0 {
		t.Fatal("no gap seen")
	}
	torn := 0
	for _, u := range units {
		if u.Torn {
			torn++
		}
	}
	if torn == 0 || torn > 3 {
		t.Fatalf("%d torn units; the hole spans one or two", torn)
	}
}

// PTSBytes round-trips through the PES header the demuxer reads.
func TestPTSBytes(t *testing.T) {
	for _, pts := range []int64{0, 1, 90000, 1 << 32, 1<<33 - 1} {
		h := PTSBytes(pts)
		got := int64(h[0]&0x0e)<<29 | int64(h[1])<<22 | int64(h[2]&0xfe)<<14 | int64(h[3])<<7 | int64(h[4]>>1)
		if got != pts {
			t.Fatalf("%d came back as %d", pts, got)
		}
		if h[0]&0x21 != 0x21 || h[2]&1 != 1 || h[4]&1 != 1 {
			t.Fatalf("%d: marker bits", pts)
		}
	}
}

// ParamPackets is a PES on the given PID that the demuxer takes as a unit
// carrying the parameter sets, with the stamp it was given.
func TestParamPackets(t *testing.T) {
	sps := []byte{0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1f, 0xac}
	pps := []byte{0, 0, 0, 1, 0x68, 0xeb, 0xe3, 0xcb}
	params := append(append([]byte(nil), sps...), pps...)
	pkts := ParamPackets(0x100, PTSBytes(180000), 7, params)
	if len(pkts)%PacketSize != 0 || len(pkts) != PacketSize {
		t.Fatalf("%d bytes", len(pkts))
	}
	if cc := pkts[3] & 0x0f; cc != 6 {
		t.Fatalf("the one packet's counter is %d, the keyframe's less one is 6", cc)
	}
	// A demuxer that knows the PID from the tables reads it back.
	d := New()
	d.videoPID, d.codec = 0x100, StreamH264
	units, err := d.Write(pkts)
	if err != nil {
		t.Fatal(err)
	}
	units = append(units, d.Flush()...)
	if len(units) != 1 || units[0].PTS != 180000 || !units[0].HasParams {
		t.Fatalf("units %+v", units)
	}
	if string(d.Params()) != string(params) {
		t.Fatalf("params %x", d.Params())
	}
}
