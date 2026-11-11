package mp4export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

// A whole recording comes out as it went in: its init segment, which now says
// how long it is, then its fragments byte for byte, without its index.
func TestWholeRecording(t *testing.T) {
	for _, name := range []string{"sample", "aac", "av"} {
		t.Run(name, func(t *testing.T) {
			b := fixture(t, name)
			init, frags := parts(t, b)
			s := source(b, testStart)
			s.End = ended(t, b)
			p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
			out := written(t, p)
			xs := boxes(t, out)
			if xs[0].Type != "ftyp" || xs[1].Type != "moov" || xs[2].Type != "moof" || xs[len(xs)-1].Type != "mdat" {
				t.Fatalf("not one init segment and fragments: %+v", xs[:3])
			}
			header := xs[1].End
			if !bytes.Equal(out[header:], bytes.Join(frags, nil)) {
				t.Fatal("the fragments changed")
			}
			if header != len(init)+20 {
				t.Fatalf("init segment of %d bytes, want %d and an mehd", header, len(init)+20)
			}
			moov := xs[1].Payload(out)
			mvex, _ := fmp4.Find(moov, "mvex")
			mehd, ok := fmp4.Find(mvex.Payload(moov), "mehd")
			if !ok || mehd.Off != 0 {
				t.Fatal("no mehd first in mvex")
			}
			mvhd, _ := fmp4.Find(moov, "mvhd")
			m := mvhd.Payload(moov)
			scale := binary.BigEndian.Uint32(m[12:])
			if m[0] == 1 {
				scale = binary.BigEndian.Uint32(m[20:])
			}
			length := float64(binary.BigEndian.Uint64(mehd.Payload(mvex.Payload(moov))[4:])) / float64(scale)
			if want := s.End.Sub(p.Start()).Seconds(); math.Abs(length-want) > 0.001 {
				t.Fatalf("says it is %.3f s long, not %.3f", length, want)
			}
			// The first indexed keyframe came at date_started; the output
			// starts at its first sample.
			if d := p.Start().Sub(testStart); d < -100*time.Millisecond || d > 0 {
				t.Fatalf("starts %s from date_started", d)
			}
		})
	}
}

// A range is cut at keyframes as the console cuts it: from the last at or
// before its start to the first after its end, and placed at zero.
func TestCutsAtKeyframes(t *testing.T) {
	b := fixture(t, "av")
	_, keys, err := fmp4.ParseMfra(b[len(b)-fmp4.MfraSize(b[len(b)-16:]):])
	if err != nil {
		t.Fatal(err)
	}
	wall := func(k fmp4.KeyAt) time.Time {
		return testStart.Add(time.Duration(float64(k.Time-keys[0].Time) / 15360 * float64(time.Second)))
	}
	// The fixture indexes every fragment, as a producer indexes its keyframes.
	from, to := wall(keys[3]).Add(time.Millisecond), wall(keys[7]).Add(-time.Millisecond)
	p := prepare(t, []Source{source(b, testStart)}, from, to)
	out := written(t, p)
	header := boxes(t, out)[1].End
	if len(out)-header != int(keys[7].Offset-keys[3].Offset) || !bytes.Equal(media(t, out), media(t, b[keys[3].Offset:keys[7].Offset])) {
		t.Fatal("not the fragments from the keyframe at or before the start to the one after the end")
	}
	ts := times(t, out)
	in := times(t, b[:keys[7].Offset])[len(times(t, b[:keys[3].Offset])):]
	lead := math.Inf(1)
	for track, tm := range in[0] {
		lead = min(lead, float64(tm)/float64(map[uint32]float64{1: 15360, 2: 48000, 3: 48000}[track]))
	}
	for i := range ts {
		for track, tm := range ts[i] {
			scale := map[uint32]float64{1: 15360, 2: 48000, 3: 48000}[track]
			if want := in[i][track] - int64(math.Floor(lead*scale+0.5)); tm != want {
				t.Fatalf("fragment %d track %d at %d, want %d", i, track, tm, want)
			}
		}
	}
	if seq := binary.BigEndian.Uint32(out[header+20:]); seq != 1 {
		t.Fatalf("first fragment numbered %d", seq)
	}
	if d := p.Start().Sub(wall(keys[3])); d < -100*time.Millisecond || d > 0 {
		t.Fatalf("starts %s from its first keyframe", d)
	}
}

// A recording of another session is placed by the wall clock: a gap stays a gap.
func TestRestartKeepsTheGap(t *testing.T) {
	b := fixture(t, "av")
	p := prepare(t, []Source{source(b, testStart), source(b, testStart.Add(10*time.Second))}, testStart, testStart.Add(time.Hour))
	out := written(t, p)
	_, frags := parts(t, b)
	ts := times(t, out)
	if len(ts) != 2*len(frags) {
		t.Fatalf("%d fragments, want %d", len(ts), 2*len(frags))
	}
	one := times(t, b)
	for i, f := range ts {
		for track, tm := range f {
			scale := map[uint32]int64{1: 15360, 2: 48000, 3: 48000}[track]
			want := one[i%len(one)][track]
			if i >= len(one) {
				want += 10 * scale
			}
			if tm != want {
				t.Fatalf("fragment %d track %d at %d, want %d", i, track, tm, want)
			}
		}
	}
	n := uint32(0)
	for _, x := range walk(out, 0, len(out)) {
		if x.Type == "moof" {
			if n++; binary.BigEndian.Uint32(out[x.Off+20:]) != n {
				t.Fatalf("fragment %d numbered %d", n, binary.BigEndian.Uint32(out[x.Off+20:]))
			}
		}
	}
}

// A recording that goes on from the one before it keeps the session's own
// times, whatever its date_started says within the slack. Its parts here
// have no index, as a recording cut short has not.
func TestSessionGoesOnAcrossRecordings(t *testing.T) {
	b := fixture(t, "av")
	init, frags := parts(t, b)
	a := append(append([]byte(nil), init...), bytes.Join(frags[:4], nil)...)
	z := append(append([]byte(nil), init...), bytes.Join(frags[4:], nil)...)
	second := time.Duration(float64(times(t, b)[4][1]) / 15360 * float64(time.Second))
	whole := written(t, prepare(t, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour)))
	for _, jitter := range []time.Duration{-1500 * time.Millisecond, 0, 400 * time.Millisecond, 1900 * time.Millisecond} {
		p := prepare(t, []Source{source(a, testStart), source(z, testStart.Add(second+jitter))}, testStart, testStart.Add(time.Hour))
		out := written(t, p)
		if !bytes.Equal(out[boxes(t, out)[1].End:], whole[boxes(t, whole)[1].End:]) {
			t.Fatalf("jitter %s moved the fragments", jitter)
		}
	}
	// Further than the slack, it is placed by the wall clock.
	p := prepare(t, []Source{source(a, testStart), source(z, testStart.Add(second+3*time.Second))}, testStart, testStart.Add(time.Hour))
	if got := times(t, written(t, p))[4][1]; got != times(t, b)[4][1]+3*15360 {
		t.Fatalf("placed at %d", got)
	}
}

// A later recording that would overlap what came before starts where it ends.
func TestNeverBeforeWhatCameBefore(t *testing.T) {
	b := fixture(t, "av")
	p := prepare(t, []Source{source(b, testStart), source(b, testStart.Add(4*time.Second))}, testStart, testStart.Add(time.Hour))
	ts := times(t, written(t, p))
	n := len(ts) / 2
	for track := range ts[0] {
		if ts[n][track] < ts[n-1][track] {
			t.Fatalf("track %d goes back", track)
		}
	}
}

func TestRefusesAnEncoderChange(t *testing.T) {
	a, z := fixture(t, "sample"), fixture(t, "av")
	_, err := Prepare(context.Background(), []Source{source(a, testStart), source(z, testStart.Add(time.Minute))}, testStart, testStart.Add(time.Hour))
	if !errors.Is(err, ErrChanged) {
		t.Fatal(err)
	}
}

func TestRefusesWhatItCannotExport(t *testing.T) {
	b := fixture(t, "av")
	init, _ := parts(t, b)
	for _, tc := range []struct {
		name       string
		sources    []Source
		start, end time.Time
	}{
		{"empty", nil, testStart, testStart.Add(time.Hour)},
		{"reversed", []Source{source(b, testStart)}, testStart, testStart.Add(-time.Second)},
		{"no fragments", []Source{source(init, testStart)}, testStart, testStart.Add(time.Hour)},
		{"no init segment", []Source{source(b[len(init):], testStart)}, testStart, testStart.Add(time.Hour)},
		{"too many", make([]Source, MaxSources+1), testStart, testStart.Add(time.Hour)},
		{"after", []Source{{Reader: bytes.NewReader(b), Size: int64(len(b)), Start: testStart, End: testStart.Add(6 * time.Second)}}, testStart.Add(time.Hour), testStart.Add(2 * time.Hour)},
		{"before", []Source{source(b, testStart)}, testStart.Add(-time.Hour), testStart.Add(-time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Prepare(context.Background(), tc.sources, tc.start, tc.end); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// countingReader counts what is read, and how many recordings are read at once.
type countingReader struct {
	r      *bytes.Reader
	active *atomic.Int32
	peak   *atomic.Int32
	mu     sync.Mutex
	reads  int
	bytes  int
	max    int
	fail   error
}

func (c *countingReader) ReadAt(p []byte, off int64) (int, error) {
	if c.fail != nil {
		return 0, c.fail
	}
	n := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		peak := c.peak.Load()
		if n <= peak || c.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	c.mu.Lock()
	c.reads++
	c.bytes += len(p)
	c.max = max(c.max, len(p))
	c.mu.Unlock()
	return c.r.ReadAt(p, off)
}

func counted(b []byte, n int) ([]Source, []*countingReader) {
	var active, peak atomic.Int32
	var ss []Source
	var rs []*countingReader
	for i := range n {
		r := &countingReader{r: bytes.NewReader(b), active: &active, peak: &peak}
		rs = append(rs, r)
		ss = append(ss, Source{Reader: r, Size: int64(len(b)), Start: testStart.Add(time.Duration(i) * time.Minute)})
	}
	return ss, rs
}

// Planning reads each recording's tail and init segment, a few recordings at
// once, and the first recording where the range begins: never the range.
func TestPlanningReadsLittle(t *testing.T) {
	b := fixture(t, "av")
	ss, rs := counted(b, 20)
	p, err := Prepare(context.Background(), ss, testStart, testStart.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if peak := rs[0].peak.Load(); peak < 2 || peak > ScanParallel {
		t.Fatalf("%d recordings read at once, want 2..%d", peak, ScanParallel)
	}
	for i, r := range rs {
		reads, limit := 2, tailProbe+1<<10*2
		if i == 0 {
			reads, limit = 3, limit+headWindow
		}
		if r.reads > reads || r.bytes > limit {
			t.Fatalf("recording %d: %d reads of %d bytes", i, r.reads, r.bytes)
		}
	}
	// Streaming reads large, and no more than the range.
	for _, r := range rs {
		r.reads, r.bytes = 0, 0
	}
	out := written(t, p)
	total := 0
	for _, r := range rs {
		total += r.bytes
		if r.max > MediaChunk {
			t.Fatalf("a read of %d bytes", r.max)
		}
	}
	if total != len(out)-boxes(t, out)[1].End {
		t.Fatalf("read %d bytes for %d", total, len(out))
	}
}

func TestFailureNamesTheFirstRecordingThatFailed(t *testing.T) {
	ss, rs := counted(fixture(t, "av"), 12)
	broken := errors.New("storage refused")
	rs[9].fail = broken
	rs[3].fail = broken
	_, err := Prepare(context.Background(), ss, testStart, testStart.Add(time.Hour))
	if !errors.Is(err, broken) || !strings.Contains(err.Error(), "source 3:") {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ss, _ = counted(fixture(t, "av"), 12)
	if _, err := Prepare(ctx, ss, testStart, testStart.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// virtualReader is a recording of one fragment whose media is zeros, as large
// as asked, read without being held.
type virtualReader struct {
	head, tail []byte
	size       int64
	mu         sync.Mutex
	max        int
}

func (r *virtualReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.max = max(r.max, len(p))
	r.mu.Unlock()
	if off < 0 || off+int64(len(p)) > r.size {
		return 0, io.EOF
	}
	clear(p)
	if off < int64(len(r.head)) {
		copy(p, r.head[off:])
	}
	if at := r.size - int64(len(r.tail)); off+int64(len(p)) > at {
		from := max(off, at)
		copy(p[from-off:], r.tail[from-at:])
	}
	return len(p), nil
}

func largeSource(t *testing.T, payload uint64) (Source, *virtualReader) {
	t.Helper()
	b := fixture(t, "sample")
	init, _ := parts(t, b)
	moof := func(off uint32) []byte {
		// One sample of all of it, as far as 32 bits say: nothing here reads sample sizes.
		tfhd := bx("tfhd", u32s(0x020018, 1, 512, uint32(min(payload, math.MaxUint32))))
		trun := bx("trun", u32s(0x000001, 1, off))
		return bx("moof", fullBx("mfhd", 0, u32s(1)), bx("traf", tfhd, fullBx("tfdt", 1, u64s(0)), trun))
	}
	m := moof(0)
	m = moof(uint32(len(m) + 16))
	head := append(append([]byte(nil), init...), m...)
	head = append(head, u32s(1)...)
	head = append(head, "mdat"...)
	head = append(head, u64s(payload+16)...)
	tail := fmp4.Mfra(1, []fmp4.KeyAt{{Time: 0, Offset: int64(len(init))}})
	r := &virtualReader{head: head, tail: tail, size: int64(len(head)) + int64(payload) + int64(len(tail))}
	return Source{Reader: r, Size: r.size, Start: testStart}, r
}

// However large, an export streams through two reads' buffers, and a box
// over 4 GiB passes through.
func TestLargeExportStreamsBounded(t *testing.T) {
	s, r := largeSource(t, 5<<30)
	p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
	if p.Size() <= 5<<30 {
		t.Fatalf("size %d", p.Size())
	}
	r.max = 0
	n := &countWriter{}
	if err := p.WriteTo(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if n.n != p.Size() || r.max > MediaChunk {
		t.Fatalf("wrote %d of %d, read up to %d at once", n.n, p.Size(), r.max)
	}
}

type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }

// failingWriter stops a stream partway; the read running ahead must stop too.
type failingWriter struct {
	n    int
	stop int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n += len(p); w.n > w.stop {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func TestStreamStopsWhenTheClientDoes(t *testing.T) {
	s, _ := largeSource(t, 64<<20)
	p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
	if err := p.WriteTo(context.Background(), &failingWriter{stop: 9 << 20}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.WriteTo(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// The walker sees the same boxes however the reads split them.
func TestBoxesAcrossReads(t *testing.T) {
	b := fixture(t, "av")
	init, frags := parts(t, b)
	body := bytes.Join(frags, nil)
	have, _, err := tracks(init)
	if err != nil {
		t.Fatal(err)
	}
	run := func(step int) []byte {
		j := joiner{have: have, end: map[uint32]int64{}, shift: map[uint32]int64{1: 7, 2: 7, 3: 7}, begun: true}
		var w boxWalker
		var out bytes.Buffer
		for off := 0; off < len(body); off += step {
			err := w.feed(body[off:min(off+step, len(body))], func(p []byte) error { out.Write(p); return nil }, j.moof)
			if err != nil {
				t.Fatal(err)
			}
		}
		if !w.idle() {
			t.Fatal("ends inside a box")
		}
		return out.Bytes()
	}
	want := run(len(body))
	for _, step := range []int{1, 7, 8, 9, 15, 16, 17, 4096} {
		if !bytes.Equal(run(step), want) {
			t.Fatalf("reads of %d bytes made another file", step)
		}
	}
	if bytes.Equal(want, body) {
		t.Fatal("nothing was rewritten")
	}
}

// Set MP4EXPORT_ARTIFACTS for independent ffprobe/browser validation
// (ts/test/export-download.mjs). These files contain only committed synthetic
// media; production recordings never enter tests.
func TestValidationArtifacts(t *testing.T) {
	dir := os.Getenv("MP4EXPORT_ARTIFACTS")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	b := fixture(t, "av")
	at := func(b []byte, start time.Duration) Source {
		s := source(b, testStart.Add(start))
		s.End = ended(t, b).Add(start)
		return s
	}
	type artifact struct {
		name       string
		ss         []Source
		start, end time.Time
	}
	cases := []artifact{
		{"full-av", []Source{at(b, 0)}, testStart, testStart.Add(time.Hour)},
		{"gap-av", []Source{at(b, 0), at(b, 10*time.Second)}, testStart, testStart.Add(time.Hour)},
		// The fixture indexes every fragment; those at 0, 2 and 4 s begin with a keyframe.
		{"clipped-av", []Source{at(b, 0)}, testStart.Add(2100 * time.Millisecond), testStart.Add(4200 * time.Millisecond)},
	}
	if bframe, err := os.ReadFile(filepath.Join(dir, "bframes-av.mp4")); err == nil {
		cases = append(cases, artifact{"full-bframes", []Source{at(bframe, 0)}, testStart, testStart.Add(time.Hour)})
	}
	if os.Getenv("MP4EXPORT_LARGE") != "" {
		ss := make([]Source, 450)
		for i := range ss {
			ss[i] = at(b, time.Duration(i)*6100*time.Millisecond)
		}
		cases = append(cases, artifact{"large-av", ss, testStart, testStart.Add(time.Hour)})
	}
	for _, tc := range cases {
		p := prepare(t, tc.ss, tc.start, tc.end)
		f, err := os.Create(filepath.Join(dir, tc.name+"-export.mp4"))
		if err != nil {
			t.Fatal(err)
		}
		if err := p.WriteTo(context.Background(), f); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: start=%s bytes=%d", tc.name, p.Start().Format(time.RFC3339Nano), p.Size())
	}
}
