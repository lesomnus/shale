package fmp4

import (
	"bytes"
	"math/rand/v2"
	"os"
	"testing"
)

// split writes b into a Splitter in pieces of the sizes `next` answers and
// answers what came out.
func split(t *testing.T, b []byte, want func(int64) bool, next func() int) (*Splitter, []*Unit) {
	t.Helper()
	var units []*Unit
	s := &Splitter{Want: want, Unit: func(u *Unit) { units = append(units, u) }}
	for len(b) > 0 {
		k := min(next(), len(b))
		n, err := s.Write(b[:k])
		if err != nil || n != k {
			t.Fatalf("write %d: %d, %v", k, n, err)
		}
		b = b[k:]
	}

	return s, units
}

// The Splitter answers what a Reader answers, however the bytes come: one
// at a time, in random pieces, or at once; a lamina's mfra and ffmpeg's
// boxes are passed over.
func TestSplitterIsAReader(t *testing.T) {
	for _, name := range []string{"av.mp4", "sample.mp4", "aac.mp4"} {
		b, err := os.ReadFile("testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		init, frags := readAll(t, b)
		rng := rand.New(rand.NewPCG(1, 2))
		for _, sizes := range map[string]func() int{
			"one":    func() int { return 1 },
			"random": func() int { return 1 + rng.IntN(40_000) },
			"whole":  func() int { return len(b) },
		} {
			s, units := split(t, b, nil, sizes)
			if s.Err() != nil {
				t.Fatalf("%s: %v", name, s.Err())
			}
			if len(units) != len(frags)+1 || units[0].Init == nil || !bytes.Equal(units[0].Init.Bytes, init.Bytes) {
				t.Fatalf("%s: %d units for %d fragments", name, len(units), len(frags))
			}
			for i, f := range frags {
				u := units[i+1]
				if u.Frag == nil || !bytes.Equal(u.Frag.Bytes, f.Bytes) || u.Frag.Seq != f.Seq || u.Frag.Key(init) != f.Key(init) {
					t.Fatalf("%s: fragment %d differs", name, i)
				}
			}
		}
	}
}

// Fragments nobody wants go by and are kept nowhere; one it began to skip
// is skipped whole, and the next is whole again.
func TestSplitterSkips(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	if err != nil {
		t.Fatal(err)
	}
	_, frags := readAll(t, b)
	wanted := false
	var got []*Unit
	s := &Splitter{Want: func(int64) bool { return wanted }, Unit: func(u *Unit) { got = append(got, u) }}
	// The init segment and the first fragments, unwanted.
	half := 0
	for i, f := range frags {
		if i == len(frags)/2 {
			break
		}
		half += len(f.Bytes)
	}
	init := bytes.Index(b, frags[0].Bytes)
	if init < 0 || !bytes.Equal(b[init+half:init+half+len(frags[len(frags)/2].Bytes)], frags[len(frags)/2].Bytes) {
		t.Fatal("the fragments are not where they were expected")
	}
	s.Write(b[:init+half+10])
	if len(got) != 1 || got[0].Init == nil {
		t.Fatalf("unwanted: %d units", len(got))
	}
	// Wanted from the middle of a fragment: that one is skipped whole.
	wanted = true
	s.Write(b[init+half+10:])
	if s.Err() != nil {
		t.Fatal(s.Err())
	}
	rest := got[1:]
	if len(rest) == 0 || len(rest) >= len(frags)-len(frags)/2 {
		t.Fatalf("%d fragments after the middle", len(rest))
	}
	if !bytes.Equal(rest[0].Frag.Bytes, frags[len(frags)/2+1].Bytes) {
		t.Fatal("the first fragment wanted is the one after the one it began to skip")
	}
}

// Want is asked at the offset each fragment begins at, so a reader of a
// stream that began at a known offset knows where each one is.
func TestSplitterOffsets(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	if err != nil {
		t.Fatal(err)
	}
	_, frags := readAll(t, b)
	var ats []int64
	_, units := split(t, b, func(at int64) bool { ats = append(ats, at); return true }, func() int { return 777 })
	if len(units) != len(frags)+1 || len(ats) < len(frags) {
		t.Fatalf("%d units, %d asks", len(units), len(ats))
	}
	for i, f := range frags {
		if want := int64(bytes.Index(b, f.Bytes)); ats[i] != want {
			t.Fatalf("fragment %d asked at %d, is at %d", i, ats[i], want)
		}
	}
}

// A stream that lost its framing says so, and the writer is not told.
func TestSplitterTorn(t *testing.T) {
	s := &Splitter{}
	n, err := s.Write([]byte{0, 0, 0, 0, 'm', 'o', 'o', 'f'})
	if n != 8 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	if s.Err() == nil {
		t.Fatal("a box of size zero is no stream")
	}
}
