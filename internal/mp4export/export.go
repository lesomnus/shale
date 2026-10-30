// Package mp4export remuxes stored fragmented MP4 recordings to a normal MP4.
// Prepare reads only container metadata; WriteTo copies media with bounded
// buffering and appends the complete sample tables in a single tail moov.
package mp4export

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

const (
	movieScale     = 1_000_000
	maxMetadataBox = 16 << 20
	epoch1904      = 2082844800
	// Limits bound planning metadata, rather than the downloaded media size.
	MaxSources       = 1024
	MaxSamples       = 2_000_000
	MaxMetadataBytes = 64 << 20
)

// Source is one complete, immutable lamina. Start is date_started: the wall
// clock corresponding to its first indexed video keyframe (or first audio
// sample for an audio-only recording). Reader must remain open through WriteTo.
// Network ReaderAt implementations must honor the request's context themselves.
type Source struct {
	Reader io.ReaderAt
	Size   int64
	Start  time.Time
}

// Plan has validated all input metadata and selected the keyframe-aligned range.
// Its memory is proportional to sample/chunk metadata, never the media payload.
// A Plan is immutable and can be written once or repeatedly while sources remain
// available and unchanged. It does not own or close the source readers.
type Plan struct {
	sources    []Source
	start      time.Time
	duration   time.Duration
	size       int64
	ftyp, moov []byte
	chunks     []chunk
	tracks     []*track
	mediaBytes int64
}

func (p *Plan) Start() time.Time        { return p.start }
func (p *Plan) Duration() time.Duration { return p.duration }
func (p *Plan) Size() int64             { return p.size }

type sample struct {
	duration, size uint32
	cts            int32
	sync           bool
	// output decode time, including gaps, in the track's timescale.
	time int64
}
type chunk struct {
	source    int
	off, size int64
	track     uint32
	count     uint32
	out       int64
}
type track struct {
	meta      *fmp4.Track
	raw, stsd []byte
	samples   []sample
	chunks    []int
}
type fragment struct {
	trafs []traf
	key   bool
}
type traf struct {
	id      uint32
	time    int64
	samples []inputSample
}
type inputSample struct {
	sample
	off int64
}
type scanned struct {
	init       *fmp4.Init
	ftyp, moov []byte
	tracks     map[uint32]*track
	frags      []fragment
	zero       time.Time
	anchor     int64
	scale      uint32
}

var ErrEmpty = errors.New("mp4export: no samples in the requested range")

// Prepare aligns start to the preceding video keyframe and includes complete
// fragments until the first keyframe strictly after end, matching Shale's viewer.
// Restarts are placed on the wall clock and each track is kept non-overlapping.
// The two-second same-session tolerance matches the viewer's export semantics.
// Sources must contain a complete init and fragment metadata, and a byte-exact
// compatible sample description. No media bytes are fetched during preparation.
func Prepare(ctx context.Context, sources []Source, start, end time.Time) (*Plan, error) {
	if !start.Before(end) {
		return nil, errors.New("mp4export: start must precede end")
	}
	if len(sources) == 0 {
		return nil, ErrEmpty
	}
	if len(sources) > MaxSources {
		return nil, fmt.Errorf("mp4export: more than %d source files; select a shorter range", MaxSources)
	}
	ordered := append([]Source(nil), sources...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Start.Before(ordered[j].Start) })
	scans := make([]*scanned, len(ordered))
	budget := &metadataBudget{}
	for i, s := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		x, err := scan(ctx, s, budget)
		if err != nil {
			return nil, fmt.Errorf("mp4export: source %d: %w", i, err)
		}
		scans[i] = x
		if i > 0 && !compatible(scans[0], x) {
			return nil, fmt.Errorf("mp4export: encoder configuration changed at %s", s.Start.Format(time.RFC3339Nano))
		}
	}
	p := &Plan{sources: ordered, ftyp: scans[0].ftyp}
	ids := make(map[uint32]*track)
	for _, meta := range scans[0].init.Tracks {
		t := scans[0].tracks[meta.ID]
		p.tracks = append(p.tracks, t)
		ids[meta.ID] = t
	}
	ends := make(map[uint32]int64)
	shifts := make(map[uint32]int64)
	var place time.Duration
	begun := false
	for si, x := range scans {
		from, to := selectFragments(x, start, end)
		if from >= to {
			continue
		}
		first := make(map[uint32]int64)
		for _, f := range x.frags[from:to] {
			for _, t := range f.trafs {
				if len(t.samples) > 0 {
					if _, ok := first[t.id]; !ok {
						first[t.id] = t.time
					}
				}
			}
		}
		if len(first) == 0 {
			continue
		}
		if !begun {
			var lead time.Duration = math.MaxInt64
			for id, t := range first {
				lead = min(lead, ticksDuration(t, ids[id].meta.Timescale))
			}
			p.start = x.zero.Add(lead)
			place = -lead
			begun = true
			for id, t := range first {
				shifts[id] = max(durationTicks(place, ids[id].meta.Timescale), ends[id]-t)
			}
		} else {
			wall := x.zero.Sub(p.start)
			same := absDuration(wall-place) <= 2*time.Second
			for id, t := range first {
				if shift, ok := shifts[id]; ok && t+shift < ends[id] {
					same = false
				}
			}
			if !same {
				place = wall
				shifts = make(map[uint32]int64)
			}
			for id, t := range first {
				if _, ok := shifts[id]; !ok {
					shifts[id] = max(durationTicks(place, ids[id].meta.Timescale), ends[id]-t)
				}
			}
		}
		for _, f := range x.frags[from:to] {
			for _, tr := range f.trafs {
				t := ids[tr.id]
				at := tr.time + shifts[tr.id]
				if at < 0 || at < ends[tr.id] {
					return nil, fmt.Errorf("mp4export: overlapping or negative decode time in track %d", tr.id)
				}
				for _, s := range tr.samples {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if len(t.samples) >= math.MaxUint32 {
						return nil, errors.New("mp4export: too many samples")
					}
					s.time = at
					at += int64(s.duration)
					t.samples = append(t.samples, s.sample)
					// Adjacent samples of this track in the source are one output chunk.
					n := len(p.chunks)
					if n > 0 && p.chunks[n-1].source == si && p.chunks[n-1].track == tr.id && p.chunks[n-1].off+p.chunks[n-1].size == s.off {
						p.chunks[n-1].size += int64(s.size)
						p.chunks[n-1].count++
					} else {
						t.chunks = append(t.chunks, n)
						p.chunks = append(p.chunks, chunk{source: si, off: s.off, size: int64(s.size), track: tr.id, count: 1})
					}
					if p.mediaBytes > math.MaxInt64-int64(s.size) {
						return nil, errors.New("mp4export: output too large")
					}
					p.mediaBytes += int64(s.size)
				}
				ends[tr.id] = at
			}
		}
	}
	if !begun || p.mediaBytes == 0 {
		return nil, ErrEmpty
	}
	for _, t := range p.tracks {
		if len(t.samples) == 0 {
			return nil, fmt.Errorf("mp4export: track %d has no selected samples", t.meta.ID)
		}
	}
	off := int64(len(p.ftyp) + 16)
	for i := range p.chunks {
		p.chunks[i].out = off
		off += p.chunks[i].size
	}
	var err error
	p.moov, err = p.makeMoov(scans[0].moov)
	if err != nil {
		return nil, err
	}
	if off > math.MaxInt64-int64(len(p.moov)) {
		return nil, errors.New("mp4export: output too large")
	}
	p.size = off + int64(len(p.moov))
	return p, nil
}

// WriteTo writes ftyp, one sized mdat and the final moov. The destination's
// backpressure controls reading, and cancellation is checked between 1 MiB
// copies. A read/write failure leaves a truncated download, not a valid MP4.
func (p *Plan) WriteTo(ctx context.Context, w io.Writer) error {
	write := func(b []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := w.Write(b)
		if err == nil && n != len(b) {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := write(p.ftyp); err != nil {
		return err
	}
	head := make([]byte, 16)
	binary.BigEndian.PutUint32(head, 1)
	copy(head[4:], "mdat")
	binary.BigEndian.PutUint64(head[8:], uint64(p.mediaBytes)+16)
	if err := write(head); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	for _, c := range p.chunks {
		for off := int64(0); off < c.size; {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := min(int64(len(buf)), c.size-off)
			if _, err := p.sources[c.source].Reader.ReadAt(buf[:n], c.off+off); err != nil {
				return err
			}
			if err := write(buf[:n]); err != nil {
				return err
			}
			off += n
		}
	}
	return write(p.moov)
}

func compatible(a, b *scanned) bool {
	if len(a.tracks) != len(b.tracks) {
		return false
	}
	for id, x := range a.tracks {
		y, ok := b.tracks[id]
		if !ok {
			return false
		}
		u, v := x.meta, y.meta
		if u.Timescale != v.Timescale || u.Handler != v.Handler || !bytes.Equal(x.stsd, y.stsd) || u.DefaultDuration != v.DefaultDuration || u.DefaultSize != v.DefaultSize || u.DefaultFlags != v.DefaultFlags {
			return false
		}
	}
	return true
}
func selectFragments(x *scanned, start, end time.Time) (int, int) {
	from, to := -1, len(x.frags)
	for i, f := range x.frags {
		if !f.key {
			continue
		}
		at := x.fragWall(f)
		if !at.After(start) {
			from = i
		}
		if at.After(end) {
			to = i
			break
		}
	}
	if from < 0 {
		for i, f := range x.frags {
			if f.key {
				if x.fragWall(f).After(end) {
					return 0, 0
				}
				from = i
				break
			}
		}
	}
	if from < 0 || from >= to {
		return 0, 0
	}
	// Skip a source whose final selected sample ended before the request.
	maxEnd := time.Time{}
	for _, f := range x.frags[from:to] {
		for _, t := range f.trafs {
			at := t.time
			for _, s := range t.samples {
				at += int64(s.duration)
			}
			wall := x.zero.Add(ticksDuration(at, x.tracks[t.id].meta.Timescale))
			if wall.After(maxEnd) {
				maxEnd = wall
			}
		}
	}
	if !maxEnd.After(start) {
		return 0, 0
	}
	return from, to
}
func (x *scanned) fragWall(f fragment) time.Time {
	var lead time.Duration = math.MaxInt64
	for _, t := range f.trafs {
		if len(t.samples) > 0 && (x.init.Video() == nil || t.id == x.init.Video().ID) {
			lead = min(lead, ticksDuration(t.time, x.tracks[t.id].meta.Timescale))
		}
	}
	return x.zero.Add(lead)
}
func ticksDuration(n int64, scale uint32) time.Duration {
	return time.Duration(n/int64(scale))*time.Second + time.Duration(n%int64(scale))*time.Second/time.Duration(scale)
}
func durationTicks(d time.Duration, scale uint32) int64 {
	return int64(d/time.Second)*int64(scale) + int64(math.Round(float64(d%time.Second)*float64(scale)/float64(time.Second)))
}
func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
