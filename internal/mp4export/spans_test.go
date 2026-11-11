package mp4export

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"
)

type countingReader struct {
	r     *bytes.Reader
	reads *atomic.Int64
	max   *atomic.Int64
}

func (c countingReader) ReadAt(p []byte, off int64) (int, error) {
	c.reads.Add(1)
	for {
		m := c.max.Load()
		if int64(len(p)) <= m || c.max.CompareAndSwap(m, int64(len(p))) {
			break
		}
	}
	return c.r.ReadAt(p, off)
}

// The media of an export is read a span at a time, not a run at a time, and
// what is written is exactly the runs, in order.
func TestStreamReadsSpansNotRuns(t *testing.T) {
	b := fixture(t, "av")
	var reads, biggest atomic.Int64
	var ss []Source
	for i := range 5 {
		ss = append(ss, Source{Reader: countingReader{r: bytes.NewReader(b), reads: &reads, max: &biggest}, Size: int64(len(b)), Start: testStart.Add(time.Duration(i) * 4 * time.Minute)})
	}
	p := prepare(t, ss, testStart, testStart.Add(time.Hour))
	if len(p.chunks) <= len(ss) {
		t.Fatalf("fixture has only %d chunks for %d sources", len(p.chunks), len(ss))
	}
	reads.Store(0)
	var got bytes.Buffer
	if err := p.WriteTo(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n > int64(len(ss)) {
		t.Fatalf("%d reads for %d sources (%d chunks)", n, len(ss), len(p.chunks))
	}
	if biggest.Load() > MediaChunk {
		t.Fatalf("read %d bytes at once", biggest.Load())
	}

	// The same file, chunk by chunk.
	var want bytes.Buffer
	want.Write(p.ftyp)
	head := make([]byte, 16)
	binary.BigEndian.PutUint32(head, 1)
	copy(head[4:], "mdat")
	binary.BigEndian.PutUint64(head[8:], uint64(p.mediaBytes)+16)
	want.Write(head)
	for _, c := range p.chunks {
		want.Write(b[c.off : c.off+c.size])
	}
	want.Write(p.moov)
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatal("span reads wrote another file than run reads")
	}
}

func TestSpansStayWithinOneSourceAndOneBuffer(t *testing.T) {
	p := &Plan{chunks: []chunk{
		{source: 0, off: 100, size: 10},
		{source: 0, off: 120, size: 10},                  // a small gap: same span
		{source: 0, off: 120 + 20 + maxSpanGap, size: 5}, // too far: a new span
		{source: 1, off: 0, size: 10},                    // another source
		{source: 1, off: 5, size: 10},                    // goes back: a new span
		{source: 1, off: 100, size: MediaChunk},          // fits one buffer only alone
		{source: 1, off: 100 + MediaChunk, size: 1},
		{source: 1, off: 200 + MediaChunk, size: 2 * MediaChunk}, // larger than a buffer
	}}
	var got [][2]int
	for _, s := range p.spans() {
		if s.size > MediaChunk && s.last-s.first > 1 {
			t.Fatalf("span %+v of several chunks exceeds a buffer", s)
		}
		got = append(got, [2]int{s.first, s.last})
	}
	want := [][2]int{{0, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}}
	if len(got) != len(want) {
		t.Fatalf("spans=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("spans=%v want %v", got, want)
		}
	}
}
