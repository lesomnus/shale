package mp4export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
func writePlan(t *testing.T, p *Plan) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := p.WriteTo(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if int64(out.Len()) != p.Size() {
		t.Fatalf("size=%d, bytes=%d", p.Size(), out.Len())
	}
	return out.Bytes()
}
func input(t *testing.T, b []byte) (*fmp4.Init, []*fmp4.Fragment) {
	t.Helper()
	r := fmp4.NewReader(bytes.NewReader(b))
	var init *fmp4.Init
	var fs []*fmp4.Fragment
	for {
		u, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if u.Init != nil {
			init = u.Init
		} else {
			fs = append(fs, u.Frag)
		}
	}
	return init, fs
}

type packet struct {
	hash           [32]byte
	size, duration uint32
	cts            int32
	mediaTime      int64
	sync           bool
}

// outputPackets independently walks the regular sample tables, not Plan internals.
func outputPackets(t *testing.T, b []byte) map[uint32][]packet {
	t.Helper()
	moov, ok := fmp4.Find(b, "moov")
	if !ok {
		t.Fatal("no moov")
	}
	mp := moov.Payload(b)
	tracks, err := fmp4.Children(mp)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[uint32][]packet)
	for _, tr := range tracks {
		if tr.Type != "trak" {
			continue
		}
		tp := tr.Payload(mp)
		tkhd, _ := childPayload(tp, "tkhd")
		id := binary.BigEndian.Uint32(tkhd[20:])
		mdia, _ := childPayload(tp, "mdia")
		minf, _ := childPayload(mdia, "minf")
		stbl, _ := childPayload(minf, "stbl")
		sz, _ := childPayload(stbl, "stsz")
		count := binary.BigEndian.Uint32(sz[8:])
		sizes := make([]uint32, count)
		for i := range sizes {
			sizes[i] = binary.BigEndian.Uint32(sz[12+i*4:])
		}
		times, _ := childPayload(stbl, "stts")
		durations := expandRuns(t, times, count)
		cts := make([]uint32, count)
		if p, err := childPayload(stbl, "ctts"); err == nil {
			cts = expandRuns(t, p, count)
		}
		sync := make(map[uint32]bool)
		stss, err := childPayload(stbl, "stss")
		if err == nil {
			for i := uint32(0); i < binary.BigEndian.Uint32(stss[4:]); i++ {
				sync[binary.BigEndian.Uint32(stss[8+4*i:])] = true
			}
		}
		co, _ := childPayload(stbl, "co64")
		chunks := binary.BigEndian.Uint32(co[4:])
		sc, _ := childPayload(stbl, "stsc")
		nr := binary.BigEndian.Uint32(sc[4:])
		r := uint32(0)
		sampleIndex := uint32(0)
		var dts int64
		for ci := uint32(1); ci <= chunks; ci++ {
			for r+1 < nr && binary.BigEndian.Uint32(sc[8+(r+1)*12:]) <= ci {
				r++
			}
			ns := binary.BigEndian.Uint32(sc[12+r*12:])
			off := binary.BigEndian.Uint64(co[8+(ci-1)*8:])
			for j := uint32(0); j < ns; j++ {
				if sampleIndex >= count {
					t.Fatal("stsc exceeds samples")
				}
				size := sizes[sampleIndex]
				if off+uint64(size) > uint64(len(b)) {
					t.Fatal("chunk past file")
				}
				out[id] = append(out[id], packet{hash: sha256.Sum256(b[off : off+uint64(size)]), size: size, duration: durations[sampleIndex], cts: int32(cts[sampleIndex]), mediaTime: dts, sync: err != nil || sync[sampleIndex+1]})
				off += uint64(size)
				dts += int64(durations[sampleIndex])
				sampleIndex++
			}
		}
		if sampleIndex != count {
			t.Fatalf("stsc left %d samples", count-sampleIndex)
		}
	}
	return out
}
func expandRuns(t *testing.T, p []byte, want uint32) []uint32 {
	t.Helper()
	var out []uint32
	for i := uint32(0); i < binary.BigEndian.Uint32(p[4:]); i++ {
		count := binary.BigEndian.Uint32(p[8+i*8:])
		value := binary.BigEndian.Uint32(p[12+i*8:])
		if uint64(len(out))+uint64(count) > uint64(want) {
			t.Fatal("table exceeds samples")
		}
		for j := uint32(0); j < count; j++ {
			out = append(out, value)
		}
	}
	if len(out) != int(want) {
		t.Fatalf("table %d samples, want %d", len(out), want)
	}
	return out
}

func TestPayloadAndSampleTables(t *testing.T) {
	for _, name := range []string{"sample", "aac", "av"} {
		t.Run(name, func(t *testing.T) {
			b := fixture(t, name)
			init, fs := input(t, b)
			p := prepare(t, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour))
			out := writePlan(t, p)
			boxes, err := fmp4.Children(out)
			if err != nil {
				t.Fatal(err)
			}
			if len(boxes) != 3 || boxes[0].Type != "ftyp" || boxes[1].Type != "mdat" || boxes[2].Type != "moov" {
				t.Fatalf("not tail-moov: %+v", boxes)
			}
			if _, ok := fmp4.Find(boxes[2].Payload(out), "mvex"); ok {
				t.Fatal("fragment metadata in regular MP4")
			}
			got := outputPackets(t, out)
			want := make(map[uint32][]packet)
			for _, f := range fs {
				for _, tr := range f.Trafs {
					for _, s := range tr.Samples {
						want[tr.TrackID] = append(want[tr.TrackID], packet{hash: sha256.Sum256(f.Bytes[s.Off : s.Off+s.Size]), size: uint32(s.Size), duration: s.Duration, cts: s.CTS, sync: s.Sync()})
					}
				}
			}
			for _, track := range init.Tracks {
				a, z := got[track.ID], want[track.ID]
				if len(a) != len(z) {
					t.Fatalf("track%d %d packets want%d", track.ID, len(a), len(z))
				}
				for i := range a {
					if a[i].hash != z[i].hash || a[i].duration != z[i].duration || a[i].cts != z[i].cts || a[i].sync != z[i].sync {
						t.Fatalf("track%d packet%d changed", track.ID, i)
					}
				}
			}
			if p.Start() != testStart {
				t.Fatalf("start %s", p.Start())
			}
			t.Logf("%s duration %s size%d", name, p.Duration(), p.Size())
		})
	}
}
func TestClipsAtPrecedingKeyframe(t *testing.T) {
	b := fixture(t, "av")
	p := prepare(t, []Source{source(b, testStart)}, testStart.Add(2500*time.Millisecond), testStart.Add(4200*time.Millisecond))
	if p.Start() != testStart.Add(2*time.Second) {
		t.Fatalf("start=%s", p.Start())
	}
	out := writePlan(t, p)
	got := outputPackets(t, out)
	if len(got[1]) != 120 {
		t.Fatalf("video samples%d want120", len(got[1]))
	}
	if !got[1][0].sync {
		t.Fatal("first video is not keyframe")
	}
	if p.Duration() < 4*time.Second || p.Duration() > 4100*time.Millisecond {
		t.Fatalf("duration%s", p.Duration())
	}
}

func TestRestartKeepsGapInSampleTimes(t *testing.T) {
	b := fixture(t, "av")
	p := prepare(t, []Source{source(b, testStart), source(b, testStart.Add(10*time.Second))}, testStart, testStart.Add(time.Hour))
	out := writePlan(t, p)
	got := outputPackets(t, out)
	if len(got[1]) != 360 || len(got[2]) != 566 || len(got[3]) != 602 {
		t.Fatal("missing or duplicated packets")
	}
	if p.Duration() < 16*time.Second || p.Duration() > 16100*time.Millisecond {
		t.Fatalf("duration=%s", p.Duration())
	}
	if got[1][180].mediaTime != 10*15360 {
		t.Fatalf("restart at %d ticks, want153600", got[1][180].mediaTime)
	}
	if got[1][179].duration <= 4*15360 {
		t.Fatal("stts lost the gap")
	}
	moov, _ := fmp4.Find(out, "moov")
	mp := moov.Payload(out)
	boxes, _ := fmp4.Children(mp)
	for _, b := range boxes {
		if b.Type != "trak" {
			continue
		}
		if _, err := childPayload(b.Payload(mp), "edts"); err == nil {
			t.Fatal("identity/interior edit unexpectedly present")
		}
	}
}

func TestSameSessionIgnoresArrivalJitter(t *testing.T) {
	b := fixture(t, "av")
	init, fs := input(t, b)
	var a, z []byte
	a = append(a, init.Bytes...)
	z = append(z, init.Bytes...)
	for i, f := range fs {
		if i < 4 {
			a = append(a, f.Bytes...)
		} else {
			z = append(z, f.Bytes...)
		}
	}
	p := prepare(t, []Source{source(a, testStart), source(z, testStart.Add(2400*time.Millisecond))}, testStart, testStart.Add(time.Hour))
	whole := prepare(t, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour))
	if p.Duration() != whole.Duration() {
		t.Fatalf("jitter changed duration %s vs %s", p.Duration(), whole.Duration())
	}
	got := outputPackets(t, writePlan(t, p))
	if len(got[1]) != 180 {
		t.Fatal("samples missing")
	}
}

type guardedReader struct {
	*bytes.Reader
	media   []extent
	enabled bool
	max     int
}

func (g *guardedReader) ReadAt(p []byte, off int64) (int, error) {
	g.max = max(g.max, len(p))
	if g.enabled {
		for _, x := range g.media {
			if off < x.end && off+int64(len(p)) > x.off+int64(x.hdr) {
				return 0, errors.New("media read in Prepare")
			}
		}
	}
	return g.Reader.ReadAt(p, off)
}
func TestPrepareDoesNotReadMediaAndWriteIsBounded(t *testing.T) {
	b := fixture(t, "av")
	boxes, _ := fmp4.Children(b)
	g := &guardedReader{Reader: bytes.NewReader(b), enabled: true}
	for _, x := range boxes {
		if x.Type == "mdat" {
			g.media = append(g.media, extent{off: int64(x.Off), end: int64(x.End), hdr: x.Hdr})
		}
	}
	p := prepare(t, []Source{{Reader: g, Size: int64(len(b)), Start: testStart}}, testStart, testStart.Add(time.Hour))
	g.enabled = false
	g.max = 0
	if err := p.WriteTo(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if g.max > 1<<20 {
		t.Fatalf("media buffer%d", g.max)
	}
}

func TestCancelAndWriterFailure(t *testing.T) {
	b := fixture(t, "sample")
	p := prepare(t, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.WriteTo(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Prepare(ctx, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := errors.New("destination closed")
	if err := p.WriteTo(context.Background(), errorWriter{want}); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRejectsIncompatibleBeforeMedia(t *testing.T) {
	a, z := fixture(t, "sample"), fixture(t, "av")
	_, err := Prepare(context.Background(), []Source{source(a, testStart), source(z, testStart.Add(time.Hour))}, testStart, testStart.Add(2*time.Hour))
	if err == nil || !strings.Contains(err.Error(), "encoder configuration changed") {
		t.Fatal(err)
	}
}
func TestRejectsInvalidAndExcessiveMetadata(t *testing.T) {
	b := fixture(t, "av")
	for _, tc := range []struct {
		name       string
		sources    []Source
		start, end time.Time
	}{{"empty", nil, testStart, testStart.Add(time.Hour)}, {"reversed", []Source{source(b, testStart)}, testStart, testStart.Add(-time.Second)}, {"truncated", []Source{source(b[:len(b)-1], testStart)}, testStart, testStart.Add(time.Hour)}, {"sources", make([]Source, MaxSources+1), testStart, testStart.Add(time.Hour)}, {"past", []Source{source(b, testStart)}, testStart.Add(time.Hour), testStart.Add(2 * time.Hour)}} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Prepare(context.Background(), tc.sources, tc.start, tc.end); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
}

// Set MP4EXPORT_ARTIFACTS for independent ffprobe/browser validation. These files
// contain only committed synthetic media; production recordings never enter tests.
func TestValidationArtifacts(t *testing.T) {
	dir := os.Getenv("MP4EXPORT_ARTIFACTS")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	b := fixture(t, "av")
	cases := []struct {
		name       string
		ss         []Source
		start, end time.Time
	}{{"full-av", []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour)}, {"gap-av", []Source{source(b, testStart), source(b, testStart.Add(10*time.Second))}, testStart, testStart.Add(time.Hour)}, {"clipped-av", []Source{source(b, testStart)}, testStart.Add(2500 * time.Millisecond), testStart.Add(4200 * time.Millisecond)}}
	if bframe, err := os.ReadFile(filepath.Join(dir, "bframes-av.mp4")); err == nil {
		cases = append(cases, struct {
			name       string
			ss         []Source
			start, end time.Time
		}{"full-bframes", []Source{source(bframe, testStart)}, testStart, testStart.Add(time.Hour)})
	}
	for _, tc := range cases {
		p := prepare(t, tc.ss, tc.start, tc.end)
		if err := os.WriteFile(filepath.Join(dir, tc.name+"-export.mp4"), writePlan(t, p), 0600); err != nil {
			t.Fatal(err)
		}
		t.Log(fmt.Sprintf("%s: start=%s duration=%s bytes=%d", tc.name, p.Start().Format(time.RFC3339Nano), p.Duration(), p.Size()))
	}
}

// A virtual payload verifies that media size does not drive planning allocation
// and that exports larger than the former browser's 256 MiB limit stream fully.
type virtualReader struct {
	head []byte
	size int64
	max  int
}

func (r *virtualReader) ReadAt(p []byte, off int64) (int, error) {
	r.max = max(r.max, len(p))
	if off < 0 || off+int64(len(p)) > r.size {
		return 0, io.EOF
	}
	clear(p)
	if off < int64(len(r.head)) {
		copy(p, r.head[off:])
	}
	return len(p), nil
}
func largeSource(t *testing.T, payload uint32) Source {
	t.Helper()
	b := fixture(t, "sample")
	init, _ := input(t, b)
	meta := init.Video()
	tfhd := box("tfhd", u32s(0x20018, meta.ID, meta.Timescale, payload))
	tfdt := full("tfdt", 0, u32s(0))
	makeMoof := func(off uint32) []byte {
		return box("moof", full("mfhd", 0, u32s(1)), box("traf", tfhd, tfdt, box("trun", u32s(1, 1, off))))
	}
	moof := makeMoof(0)
	moof = makeMoof(uint32(len(moof) + 16))
	head := append(append([]byte(nil), init.Bytes...), moof...)
	mdat := make([]byte, 16)
	binary.BigEndian.PutUint32(mdat, 1)
	copy(mdat[4:], "mdat")
	binary.BigEndian.PutUint64(mdat[8:], uint64(payload)+16)
	head = append(head, mdat...)
	r := &virtualReader{head: head, size: int64(len(head)) + int64(payload)}
	return Source{Reader: r, Size: r.size, Start: testStart}
}
func TestLargeExportUsesBoundedBuffer(t *testing.T) {
	s := largeSource(t, 300<<20)
	p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
	if p.Size() <= 300<<20 {
		t.Fatalf("size=%d", p.Size())
	}
	r := s.Reader.(*virtualReader)
	r.max = 0
	if err := p.WriteTo(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if r.max > 1<<20 {
		t.Fatalf("buffer=%d", r.max)
	}
}
func TestCo64SupportsOffsetsBeyondFourGiB(t *testing.T) {
	s := largeSource(t, ^uint32(0))
	z := largeSource(t, 1024)
	z.Start = testStart.Add(2 * time.Second)
	p := prepare(t, []Source{s, z}, testStart, testStart.Add(time.Hour))
	if len(p.chunks) != 2 || p.chunks[1].out <= 1<<32 {
		t.Fatalf("co64 offset %+v", p.chunks)
	}
	moov, _ := fmp4.Find(p.moov, "moov")
	tr, _ := fmp4.Find(moov.Payload(p.moov), "trak")
	mdia, _ := childPayload(tr.Payload(moov.Payload(p.moov)), "mdia")
	minf, _ := childPayload(mdia, "minf")
	stbl, _ := childPayload(minf, "stbl")
	co, _ := childPayload(stbl, "co64")
	if got := binary.BigEndian.Uint64(co[16:]); got != uint64(p.chunks[1].out) {
		t.Fatalf("co64=%d want%d", got, p.chunks[1].out)
	}
}
func TestMetadataBudgetsRejectBeforeSamplesAllocated(t *testing.T) {
	s := largeSource(t, 1024)
	_, err := scan(context.Background(), s, &metadataBudget{bytes: MaxMetadataBytes})
	if err == nil || !strings.Contains(err.Error(), "metadata exceeds") {
		t.Fatal(err)
	}
	b := fixture(t, "sample")
	init, fs := input(t, b)
	moof, _ := fmp4.Find(fs[0].Bytes, "moof")
	_, err = parseMoof(moof.Whole(fs[0].Bytes), 0, init, &metadataBudget{samples: MaxSamples})
	if err == nil || !strings.Contains(err.Error(), "samples") {
		t.Fatal(err)
	}
}

func TestLargeValidationArtifact(t *testing.T) {
	dir := os.Getenv("MP4EXPORT_ARTIFACTS")
	if dir == "" || os.Getenv("MP4EXPORT_LARGE") == "" {
		return
	}
	b := fixture(t, "av")
	ss := make([]Source, 450)
	for i := range ss {
		ss[i] = source(b, testStart.Add(time.Duration(i)*6100*time.Millisecond))
	}
	p := prepare(t, ss, testStart, testStart.Add(time.Hour))
	f, err := os.Create(filepath.Join(dir, "large-av-export.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := p.WriteTo(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	t.Logf("large-av: duration=%s bytes=%d", p.Duration(), p.Size())
}
