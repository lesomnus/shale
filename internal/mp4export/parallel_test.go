package mp4export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowReader counts how many scans read at once and whether each was told
// its scan was done.
type slowReader struct {
	r       *bytes.Reader
	active  *atomic.Int32
	peak    *atomic.Int32
	scanned atomic.Bool
	fail    error
}

func (s *slowReader) ReadAt(p []byte, off int64) (int, error) {
	if s.fail != nil {
		return 0, s.fail
	}
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		peak := s.peak.Load()
		if n <= peak || s.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	return s.r.ReadAt(p, off)
}

func (s *slowReader) Scanned() { s.scanned.Store(true) }

func slowSources(t *testing.T, n int) ([]Source, []*slowReader) {
	b := fixture(t, "av")
	var active, peak atomic.Int32
	var ss []Source
	var rs []*slowReader
	for i := range n {
		r := &slowReader{r: bytes.NewReader(b), active: &active, peak: &peak}
		rs = append(rs, r)
		ss = append(ss, Source{Reader: r, Size: int64(len(b)), Start: testStart.Add(time.Duration(i) * 4 * time.Minute)})
	}
	return ss, rs
}

func TestScanRunsAFewSourcesAtOnce(t *testing.T) {
	ss, rs := slowSources(t, 9)
	p, err := Prepare(context.Background(), ss, testStart, testStart.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	peak := rs[0].peak.Load()
	if peak < 2 || peak > ScanParallel {
		t.Fatalf("scans at once: %d, want 2..%d", peak, ScanParallel)
	}
	for i, r := range rs {
		if !r.scanned.Load() {
			t.Fatalf("source %d was not told its scan was done", i)
		}
	}
	// The plan is the one a scan one after another makes.
	var parallel, one bytes.Buffer
	if err := p.WriteTo(context.Background(), &parallel); err != nil {
		t.Fatal(err)
	}
	plain := make([]Source, len(ss))
	for i, s := range ss {
		plain[i] = Source{Reader: s.Reader.(*slowReader).r, Size: s.Size, Start: s.Start}
	}
	q, err := Prepare(context.Background(), plain, testStart, testStart.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.WriteTo(context.Background(), &one); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(parallel.Bytes(), one.Bytes()) {
		t.Fatal("a parallel scan made another file")
	}
}

func TestScanFailureNamesTheFirstFailedSource(t *testing.T) {
	ss, rs := slowSources(t, 8)
	broken := errors.New("storage refused")
	rs[5].fail = broken
	rs[2].fail = broken
	_, err := Prepare(context.Background(), ss, testStart, testStart.Add(time.Hour))
	if !errors.Is(err, broken) || !strings.Contains(err.Error(), "source 2:") {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ss, _ = slowSources(t, 8)
	if _, err := Prepare(ctx, ss, testStart, testStart.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// failingWriter stops a stream partway; the read running ahead must stop too.
type failingWriter struct {
	mu   sync.Mutex
	n    int
	stop int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.n += len(p); w.n > w.stop {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func TestStreamStopsWhenTheClientDoes(t *testing.T) {
	s := largeSource(t, 64<<20)
	p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
	if err := p.WriteTo(context.Background(), &failingWriter{stop: 9 << 20}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err=%v", err)
	}
}
