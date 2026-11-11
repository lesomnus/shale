package mp4export

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

var testStart = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../fmp4/testdata/" + name + ".mp4")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func source(b []byte, at time.Time) Source {
	return Source{Reader: bytes.NewReader(b), Size: int64(len(b)), Start: at}
}

func prepare(t *testing.T, ss []Source, start, end time.Time) *Plan {
	t.Helper()
	p, err := Prepare(context.Background(), ss, start, end)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// written is the plan's file, which must be as long as it said.
func written(t *testing.T, p *Plan) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := p.WriteTo(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if int64(out.Len()) != p.Size() {
		t.Fatalf("size %d, wrote %d", p.Size(), out.Len())
	}
	return out.Bytes()
}

func bx(typ string, payload ...[]byte) []byte {
	n := 8
	for _, p := range payload {
		n += len(p)
	}
	out := binary.BigEndian.AppendUint32(nil, uint32(n))
	out = append(out, typ...)
	for _, p := range payload {
		out = append(out, p...)
	}
	return out
}

func fullBx(typ string, version byte, payload ...[]byte) []byte {
	return bx(typ, append([][]byte{{version, 0, 0, 0}}, payload...)...)
}

func u32s(xs ...uint32) []byte {
	var out []byte
	for _, x := range xs {
		out = binary.BigEndian.AppendUint32(out, x)
	}
	return out
}

func u64s(xs ...uint64) []byte {
	var out []byte
	for _, x := range xs {
		out = binary.BigEndian.AppendUint64(out, x)
	}
	return out
}

// boxes is the top-level boxes of a file.
func boxes(t *testing.T, b []byte) []fmp4.Box {
	t.Helper()
	xs, err := fmp4.Children(b)
	if err != nil {
		t.Fatal(err)
	}
	return xs
}

// parts is a fixture's init segment and its fragments (each its moof and
// what follows up to the next), without its index.
func parts(t *testing.T, b []byte) ([]byte, [][]byte) {
	t.Helper()
	var init []byte
	var frags [][]byte
	for _, x := range boxes(t, b) {
		switch x.Type {
		case "moof":
			frags = append(frags, x.Whole(b))
		case "mfra":
		default:
			if frags == nil {
				init = append(init, x.Whole(b)...)
			} else {
				frags[len(frags)-1] = append(frags[len(frags)-1], x.Whole(b)...)
			}
		}
	}
	return init, frags
}

// times is each fragment's tracks' decode times, in order.
func times(t *testing.T, b []byte) []map[uint32]int64 {
	t.Helper()
	moov, _ := fmp4.Find(b, "moov")
	have, _, err := tracks(moov.Whole(b))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[uint32]int64
	for _, x := range walk(b, 0, len(b)) {
		if x.Type != "moof" {
			continue
		}
		f, err := readMoof(b[x.Off:x.End], have)
		if err != nil {
			t.Fatal(err)
		}
		m := map[uint32]int64{}
		for _, tr := range f.trafs {
			m[tr.track] = tr.time
		}
		out = append(out, m)
	}
	return out
}

// media is a file's mdat bytes, in order.
func media(t *testing.T, b []byte) []byte {
	t.Helper()
	var out []byte
	for _, x := range boxes(t, b) {
		if x.Type == "mdat" {
			out = append(out, x.Whole(b)...)
		}
	}
	return out
}

// length is where a recording's last sample ends.
func length(t *testing.T, b []byte) time.Duration {
	t.Helper()
	init, _ := parts(t, b)
	have, _, err := tracks(init)
	if err != nil {
		t.Fatal(err)
	}
	var end time.Duration
	for _, x := range walk(b, 0, len(b)) {
		if x.Type != "moof" {
			continue
		}
		m, err := readMoof(b[x.Off:x.End], have)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range m.trafs {
			end = max(end, time.Duration(float64(tr.time+tr.duration)/float64(have[tr.track].timescale)*float64(time.Second)))
		}
	}
	return end
}

// ended is a recording's date_ended when its first indexed keyframe came at
// testStart: when its last sample ends, on the wall clock.
func ended(t *testing.T, b []byte) time.Time {
	t.Helper()
	m, err := read(context.Background(), source(b, testStart))
	if err != nil {
		t.Fatal(err)
	}
	key := time.Duration(float64(m.keys[0].Time) / float64(m.timescale) * float64(time.Second))
	return testStart.Add(length(t, b) - key)
}
