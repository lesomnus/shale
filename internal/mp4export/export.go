// Package mp4export streams a range of stored recordings as one fragmented
// MP4, the console's export (ts/src/export.ts): the first recording's init
// segment, then every recording's fragments from a keyframe on, each `tfdt`
// moved to where its recording sits on the wall clock and each `mfhd`
// renumbered, in place. Nothing is remuxed and no sizes change, so planning
// reads only each recording's tail, index and init segment, the length is
// known before the first byte, and the fragments stream through as they are
// read.
//
// Placement is `join`'s: a recording that goes on from the one before it in
// the same encoder session keeps the session's decode times; anything else is
// placed by the wall clock, never before what came before it. A gap stays a
// gap. The init segment is written first, so its durations come from the
// plan's cut times rather than from the last sample: a player reads them as
// the length, and the fragments' times are exact.
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
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

const (
	// MaxSources bounds the recordings of one export.
	MaxSources = 1024
	// ScanParallel is how many recordings an export reads the index and
	// init segment of at once.
	ScanParallel = 8
	// MediaChunk is one read of media while an export streams; the next is
	// read while this one is written.
	MediaChunk = 4 << 20

	// slack is how far, in seconds, a recording's wall clock placement may
	// be from its predecessor's session for it still to go on from it:
	// `date_started` is when its first fragment arrived, not when it began.
	slack = 2
	// epoch1904 is 1904-01-01, where an MP4's clock begins, in Unix seconds.
	epoch1904 = 2082844800
	// tailProbe is read from a recording's end: its `mfro`, and an index of
	// a few hundred keyframes, in one read.
	tailProbe = 16 << 10
	// headWindow is what a recording without an index is read for its init
	// segment and first fragment, and what the first recording is read at
	// its cut for its first samples' times.
	headWindow = 512 << 10
	// maxInit, maxIndex and maxMoof bound the headers held for a recording.
	maxInit  = 1 << 20
	maxIndex = 16 << 20
	maxMoof  = MediaChunk
)

// Source is one complete, immutable lamina. Start is its `date_started`: the
// wall clock time of its first indexed keyframe. End, when known, is its
// `date_ended`, which says how long the export is when the range runs to the
// recording's end. Reader must remain open through WriteTo. Network ReaderAt
// implementations must honor the request's context themselves.
type Source struct {
	Reader io.ReaderAt
	Size   int64
	Start  time.Time
	End    time.Time
}

var (
	ErrEmpty = errors.New("mp4export: no samples in the requested range")
	// ErrChanged is a range across a change of the encoder's configuration,
	// which one init segment cannot describe.
	ErrChanged = errors.New("mp4export: encoder configuration changed")
	errLong    = errors.New("mp4export: the range is too long for this recording to export in one file")
)

// Plan is a planned export. It holds each recording's init segment and index,
// never its media, and can be written while its sources stay available.
type Plan struct {
	sources []Source
	pieces  []piece
	header  []byte
	have    map[uint32]trackInfo
	origin  float64 // ms: the first sample's wall clock time
	size    int64
}

// piece is one recording's part: whole fragments [from, to).
type piece struct {
	source   int
	from, to int64
	// zero is the wall clock, in ms, at decode time zero of its session.
	zero float64
	have map[uint32]trackInfo
}

// trackInfo is what an init segment says of a track that its fragments are
// read by.
type trackInfo struct {
	timescale uint32
	duration  uint32 // `trex`'s default sample duration
	// config is what has to be the same for fragments to share the init
	// segment: the handler, the sample entries, the `trex` defaults.
	config [3][]byte
}

// recording is what planning reads of one recording.
type recording struct {
	init      []byte
	have      map[uint32]trackInfo
	timescale uint32 // the indexed track's
	keys      []fmp4.KeyAt
	dataEnd   int64
}

func (p *Plan) Size() int64 { return p.size }

// Start is the first sample's wall clock time.
func (p *Plan) Start() time.Time {
	return time.UnixMilli(int64(math.Floor(p.origin))).UTC()
}

// Prepare plans an export of [start, end), cut at keyframes as the console
// cuts it: from the last keyframe at or before start to the first after end.
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
	recs, err := readAll(ctx, ordered)
	if err != nil {
		return nil, err
	}
	a, b := float64(start.UnixMilli()), float64(end.UnixMilli())
	p := &Plan{sources: ordered}
	var last float64 // ms: the wall clock time where the last piece ends
	for i, s := range ordered {
		// As the console picks recordings: those that overlap the range.
		if !s.End.IsZero() && float64(s.End.UnixMilli()) <= a {
			continue
		}
		m := recs[i]
		first := m.keys[0]
		begun := float64(s.Start.UnixMilli())
		wall := func(k fmp4.KeyAt) float64 {
			return begun + float64(k.Time-first.Time)/float64(m.timescale)*1000
		}
		from, to := first.Offset, m.dataEnd
		ends := wall(m.keys[len(m.keys)-1])
		if !s.End.IsZero() {
			ends = float64(s.End.UnixMilli())
		}
		for _, k := range m.keys {
			at := wall(k)
			if at <= a {
				from = k.Offset
			}
			if at > b {
				to, ends = k.Offset, at
				break
			}
		}
		if to <= from {
			continue
		}
		if len(p.pieces) == 0 {
			p.have = m.have
		} else if !sameTracks(p.have, m.have) {
			return nil, fmt.Errorf("%w at %s", ErrChanged, s.Start.UTC().Format(time.RFC3339Nano))
		}
		p.pieces = append(p.pieces, piece{source: i, from: from, to: to, zero: begun - float64(first.Time)/float64(m.timescale)*1000, have: m.have})
		p.size += to - from
		last = ends
	}
	if len(p.pieces) == 0 {
		return nil, ErrEmpty
	}

	// The output begins at the earliest first sample of any track.
	head := p.pieces[0]
	window := make([]byte, min(headWindow, head.to-head.from))
	if _, err := ordered[head.source].Reader.ReadAt(window, head.from); err != nil {
		return nil, fmt.Errorf("mp4export: source %d: %w", head.source, err)
	}
	firsts, v0, err := firstTimes(window, head.have)
	if err != nil {
		return nil, err
	}
	p.origin = head.zero - atStart(firsts, head.have)*1000

	// The durations a player reads as the length, from the plan's last cut.
	seconds := max(0, (last-p.origin)/1000)
	ends := make(map[uint32]int64, len(p.have))
	for track, info := range p.have {
		ends[track] = round(seconds * float64(info.timescale))
		if v0 && ends[track] > math.MaxUint32 {
			return nil, errLong
		}
	}
	p.header, err = initHeader(recs[head.source].init, p.origin, ends)
	if err != nil {
		return nil, err
	}
	p.size += int64(len(p.header))
	return p, nil
}

// atStart is the placement, in seconds, that begins the output with a
// session's earliest first sample: minus that sample's time.
func atStart(first map[uint32]int64, have map[uint32]trackInfo) float64 {
	lead := math.Inf(1)
	for track, t := range first {
		lead = min(lead, float64(t)/float64(have[track].timescale))
	}
	return -lead
}

// round is JavaScript's Math.round, as the console rounds: halves go up.
func round(x float64) int64 { return int64(math.Floor(x + 0.5)) }

// readAll reads every recording's tail, index and init segment, a few at
// once. An error names the first recording, in order, that failed.
func readAll(ctx context.Context, sources []Source) ([]recording, error) {
	out := make([]recording, len(sources))
	errs := make([]error, len(sources))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan int)
	var wg sync.WaitGroup
	for range min(ScanParallel, len(sources)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				out[i], errs[i] = read(ctx, sources[i])
				if errs[i] != nil {
					cancel()
				}
			}
		}()
	}
	for i := range sources {
		select {
		case <-ctx.Done():
		case work <- i:
			continue
		}
		break
	}
	close(work)
	wg.Wait()
	for i, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("mp4export: source %d: %w", i, err)
		}
	}
	for i, err := range errs {
		if out[i].init == nil {
			if err == nil {
				err = ctx.Err()
			}
			return nil, fmt.Errorf("mp4export: source %d: %w", i, err)
		}
	}
	return out, nil
}

// read reads a recording's index from its tail and its init segment from its
// head. A recording without an index (cut short) is read as the console
// plays it: from its first fragment, which its head says where and when.
func read(ctx context.Context, s Source) (recording, error) {
	if err := ctx.Err(); err != nil {
		return recording{}, err
	}
	if s.Reader == nil || s.Size < 16 {
		return recording{}, errors.New("recording too short")
	}
	var m recording
	tail := make([]byte, min(tailProbe, s.Size))
	if _, err := s.Reader.ReadAt(tail, s.Size-int64(len(tail))); err != nil {
		return recording{}, err
	}
	var track uint32
	if n := int64(fmp4.MfraSize(tail[len(tail)-16:])); n > 16 && n < s.Size && n <= maxIndex {
		index := tail[max(0, int64(len(tail))-n):]
		if n > int64(len(tail)) {
			index = make([]byte, n)
			if _, err := s.Reader.ReadAt(index, s.Size-n); err != nil {
				return recording{}, err
			}
		}
		indexed, keys, err := fmp4.ParseMfra(index)
		if err == nil && len(keys) > 0 && keys[0].Offset > 0 && keys[0].Offset <= maxInit {
			for i := range keys {
				if keys[i].Offset >= s.Size-n || (i > 0 && keys[i].Offset <= keys[i-1].Offset) {
					return recording{}, errors.New("an index out of order")
				}
			}
			m.keys, m.dataEnd, track = keys, s.Size-n, indexed
		}
	}
	var head []byte
	if m.keys != nil {
		m.init = make([]byte, m.keys[0].Offset)
		if _, err := s.Reader.ReadAt(m.init, 0); err != nil {
			return recording{}, err
		}
	} else {
		head = make([]byte, min(headWindow, s.Size))
		if _, err := s.Reader.ReadAt(head, 0); err != nil {
			return recording{}, err
		}
		for _, x := range walk(head, 0, len(head)) {
			if x.Type == "moof" {
				m.init = append([]byte(nil), head[:x.Off]...)
				break
			}
		}
		if m.init == nil {
			return recording{}, errors.New("no fragment in the recording's head")
		}
		m.dataEnd = s.Size
	}
	have, video, err := tracks(m.init)
	if err != nil {
		return recording{}, err
	}
	m.have = have
	if _, ok := have[track]; !ok {
		track = video
	}
	m.timescale = have[track].timescale
	if m.keys == nil {
		off := len(m.init)
		x := walk(head, off, len(head))
		if len(x) == 0 {
			return recording{}, errors.New("no whole fragment in the recording's head")
		}
		t, ok := trackTime(head, x[0], track)
		if !ok {
			return recording{}, errors.New("no time in the recording's first fragment")
		}
		m.keys = []fmp4.KeyAt{{Time: t, Offset: int64(off)}}
	}
	return m, nil
}

// WriteTo streams the export. One read runs ahead of the write, with two
// buffers, and each `moof` is held only while it is rewritten.
func (p *Plan) WriteTo(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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
	if err := write(p.header); err != nil {
		return err
	}
	type chunk struct {
		piece int
		b     []byte
		err   error
	}
	free := make(chan []byte, 2)
	free <- make([]byte, MediaChunk)
	free <- make([]byte, MediaChunk)
	full := make(chan chunk)
	go func() {
		defer close(full)
		for i, pc := range p.pieces {
			r := p.sources[pc.source].Reader
			for off := pc.from; off < pc.to; {
				var buf []byte
				select {
				case <-ctx.Done():
					return
				case buf = <-free:
				}
				n := min(int64(len(buf)), pc.to-off)
				c := chunk{piece: i, b: buf[:n]}
				if _, err := r.ReadAt(c.b, off); err != nil {
					c.err = fmt.Errorf("mp4export: source %d: %w", pc.source, err)
				}
				select {
				case <-ctx.Done():
					return
				case full <- c:
				}
				if c.err != nil {
					return
				}
				off += n
			}
		}
	}()
	j := joiner{have: p.have, origin: p.origin, end: map[uint32]int64{}}
	var walker boxWalker
	at := -1
	for c := range full {
		if c.err != nil {
			return c.err
		}
		if c.piece != at {
			if !walker.idle() {
				return fmt.Errorf("mp4export: source %d ends inside a box", p.pieces[at].source)
			}
			at = c.piece
			head := c.b
			if at == 0 {
				// The bytes Prepare placed the output's start by.
				head = head[:min(len(head), headWindow)]
			}
			if err := j.begin(p.pieces[at], head); err != nil {
				return err
			}
		}
		if err := walker.feed(c.b, write, j.moof); err != nil {
			return err
		}
		free <- c.b[:cap(c.b)]
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !walker.idle() {
		return errors.New("mp4export: the export ends inside a box")
	}
	return nil
}

// joiner is `join`'s placement, decided as the fragments pass.
type joiner struct {
	have   map[uint32]trackInfo
	origin float64
	// place is the current session's placement, in seconds: output time is
	// session time plus place.
	place float64
	begun bool
	// shift is what is added to each track's decode times, and end where
	// the output so far ends, in the track's ticks.
	shift map[uint32]int64
	end   map[uint32]int64
	seq   uint32
}

// begin places a recording by the first fragments of its first read, as
// `join` places a piece by all of its fragments. A track that appears only
// after them is placed when it does (moof).
func (j *joiner) begin(pc piece, head []byte) error {
	first, _, err := firstTimes(head, pc.have)
	if err != nil {
		return err
	}
	if !j.begun {
		j.begun = true
		j.place = atStart(first, pc.have)
		j.shift = shifts(pc.have, first, j.end, j.place)
		return nil
	}
	wall := (pc.zero - j.origin) / 1000
	goesOn := math.Abs(wall-j.place) <= slack
	for track, t := range first {
		if s, ok := j.shift[track]; ok && t+s < j.end[track] {
			goesOn = false
		}
	}
	if !goesOn {
		j.place = wall
		j.shift = shifts(pc.have, first, j.end, wall)
		return nil
	}
	// A track the session had not sent yet joins at the session's placement.
	for track, s := range shifts(pc.have, first, j.end, j.place) {
		if _, ok := j.shift[track]; !ok {
			j.shift[track] = s
		}
	}
	return nil
}

// moof renumbers a fragment and moves its decode times, in place.
func (j *joiner) moof(b []byte) error {
	f, err := readMoof(b, j.have)
	if err != nil {
		return err
	}
	j.seq++
	binary.BigEndian.PutUint32(b[f.mfhd:], j.seq)
	for _, t := range f.trafs {
		s, ok := j.shift[t.track]
		if !ok {
			s = shifts(j.have, map[uint32]int64{t.track: t.time}, j.end, j.place)[t.track]
			j.shift[t.track] = s
		}
		time := t.time + s
		if t.v1 {
			if time < 0 {
				return errLong
			}
			binary.BigEndian.PutUint64(b[t.tfdt:], uint64(time))
		} else {
			// A 32-bit tfdt holds 13 hours at 90 kHz; growing the box would move the samples.
			if time < 0 || time > math.MaxUint32 {
				return errLong
			}
			binary.BigEndian.PutUint32(b[t.tfdt:], uint32(time))
		}
		j.end[t.track] = max(j.end[t.track], time+t.duration)
	}
	return nil
}

// shifts places a session's first samples at `place` seconds of the output,
// each track no earlier than where it ends so far.
func shifts(have map[uint32]trackInfo, first, end map[uint32]int64, place float64) map[uint32]int64 {
	out := make(map[uint32]int64, len(first))
	for track, t := range first {
		out[track] = max(round(place*float64(have[track].timescale)), end[track]-t)
	}
	return out
}

// boxWalker follows top-level boxes across reads: a `moof` is gathered and
// handed over whole; anything else passes through as it comes.
type boxWalker struct {
	hdr  [16]byte
	hn   int
	moof []byte
	need int64 // the size of the moof being gathered
	pass int64 // what is left of a box passing through
}

func (w *boxWalker) idle() bool { return w.hn == 0 && w.need == 0 && w.pass == 0 }

func (w *boxWalker) feed(b []byte, out func([]byte) error, moof func([]byte) error) error {
	for len(b) > 0 {
		switch {
		case w.pass > 0:
			n := min(int64(len(b)), w.pass)
			if err := out(b[:n]); err != nil {
				return err
			}
			b, w.pass = b[n:], w.pass-n
		case w.need > 0:
			n := min(int64(len(b)), w.need-int64(len(w.moof)))
			w.moof = append(w.moof, b[:n]...)
			b = b[n:]
			if int64(len(w.moof)) < w.need {
				continue
			}
			if err := moof(w.moof); err != nil {
				return err
			}
			if err := out(w.moof); err != nil {
				return err
			}
			w.moof, w.need = w.moof[:0], 0
		default:
			want := 8
			if w.hn >= 8 && binary.BigEndian.Uint32(w.hdr[:]) == 1 {
				want = 16
			}
			n := copy(w.hdr[w.hn:want], b)
			w.hn += n
			b = b[n:]
			if w.hn < want {
				continue
			}
			size := int64(binary.BigEndian.Uint32(w.hdr[:]))
			if size == 1 {
				if want == 8 {
					continue
				}
				size = int64(binary.BigEndian.Uint64(w.hdr[8:]))
			}
			if size < int64(w.hn) {
				return fmt.Errorf("mp4export: a box of size %d", size)
			}
			if string(w.hdr[4:8]) == "moof" {
				if size > maxMoof || size == int64(w.hn) {
					return fmt.Errorf("mp4export: a moof of %d bytes", size)
				}
				w.moof = append(w.moof[:0], w.hdr[:w.hn]...)
				w.need = size
			} else {
				if err := out(w.hdr[:w.hn]); err != nil {
					return err
				}
				w.pass = size - int64(w.hn)
			}
			w.hn = 0
		}
	}
	return nil
}

// box is one box of a byte slice, at its offset in the slice, so that what is
// found in it can be rewritten in place.
type box struct {
	Type          string
	Off, Hdr, End int
}

// walk is the boxes of b[off:end] that lie whole within it, in order.
func walk(b []byte, off, end int) []box {
	var out []box
	for off+8 <= end {
		size := int64(binary.BigEndian.Uint32(b[off:]))
		hdr := 8
		if size == 1 {
			if off+16 > end {
				break
			}
			size, hdr = int64(binary.BigEndian.Uint64(b[off+8:])), 16
		}
		if size < int64(hdr) || size > int64(end-off) {
			break
		}
		out = append(out, box{Type: string(b[off+4 : off+8]), Off: off, Hdr: hdr, End: off + int(size)})
		off += int(size)
	}
	return out
}

func find(b []byte, in box, typ string) (box, bool) {
	for _, x := range walk(b, in.Off+in.Hdr, in.End) {
		if x.Type == typ {
			return x, true
		}
	}
	return box{}, false
}

// field is the offset of a full box's field that is at v0 in a version 0
// box and at v1 in a version 1 box, counted from the version byte.
func field(b []byte, x box, v0, v1 int) int {
	p := x.Off + x.Hdr
	if b[p] == 1 {
		return p + v1
	}
	return p + v0
}

// sized is whether a full box is as long as its version says.
func sized(b []byte, x box, v0, v1 int) bool {
	if x.End-x.Off < x.Hdr+4 {
		return false
	}
	if b[x.Off+x.Hdr] == 1 {
		return x.End-x.Off >= v1
	}
	return x.End-x.Off >= v0
}

func be32(b []byte, at int) uint32 {
	if at < 0 || at+4 > len(b) {
		return 0
	}
	return binary.BigEndian.Uint32(b[at:])
}

// tracks reads an init segment's tracks by ID, as the console's `tracks`
// does, and the first video track, or the first track if none is video.
func tracks(init []byte) (map[uint32]trackInfo, uint32, error) {
	whole := box{Off: 0, Hdr: 0, End: len(init)}
	moov, ok := find(init, whole, "moov")
	if !ok {
		return nil, 0, errors.New("no init segment")
	}
	trex := map[uint32]box{}
	if mvex, ok := find(init, moov, "mvex"); ok {
		for _, x := range walk(init, mvex.Off+mvex.Hdr, mvex.End) {
			if x.Type == "trex" && x.End-x.Off >= 32 {
				trex[be32(init, x.Off+x.Hdr+4)] = x
			}
		}
	}
	out := map[uint32]trackInfo{}
	var video, any uint32
	for _, trak := range walk(init, moov.Off+moov.Hdr, moov.End) {
		if trak.Type != "trak" {
			continue
		}
		tkhd, ok1 := find(init, trak, "tkhd")
		mdia, ok2 := find(init, trak, "mdia")
		if !ok1 || !ok2 {
			return nil, 0, errors.New("a track without tkhd or mdia")
		}
		mdhd, ok3 := find(init, mdia, "mdhd")
		hdlr, ok4 := find(init, mdia, "hdlr")
		minf, ok5 := find(init, mdia, "minf")
		if !ok3 || !ok4 || !ok5 || !sized(init, tkhd, 92, 104) || !sized(init, mdhd, 32, 44) || hdlr.End-hdlr.Off < 20 {
			return nil, 0, errors.New("a track without its mdhd, hdlr or minf")
		}
		stbl, ok := find(init, minf, "stbl")
		var stsd box
		if ok {
			stsd, ok = find(init, stbl, "stsd")
		}
		if !ok {
			return nil, 0, errors.New("a track without its sample entries")
		}
		id := be32(init, field(init, tkhd, 12, 20))
		scale := be32(init, field(init, mdhd, 12, 20))
		if scale == 0 {
			return nil, 0, errors.New("a track without a timescale")
		}
		handler := init[hdlr.Off+hdlr.Hdr+8 : hdlr.Off+hdlr.Hdr+12]
		info := trackInfo{timescale: scale, config: [3][]byte{handler, init[stsd.Off:stsd.End], nil}}
		if x, ok := trex[id]; ok {
			info.duration = be32(init, x.Off+x.Hdr+12)
			info.config[2] = init[x.Off:x.End]
		}
		out[id] = info
		if any == 0 {
			any = id
		}
		if video == 0 && string(handler) == "vide" {
			video = id
		}
	}
	if len(out) == 0 {
		return nil, 0, errors.New("an init segment without tracks")
	}
	if video == 0 {
		video = any
	}
	return out, video, nil
}

// sameTracks is whether fragments of b read the same under a's init segment.
func sameTracks(a, b map[uint32]trackInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for id, x := range a {
		y, ok := b[id]
		if !ok || x.timescale != y.timescale {
			return false
		}
		for i := range x.config {
			if !bytes.Equal(x.config[i], y.config[i]) {
				return false
			}
		}
	}
	return true
}

// trackTime is the decode time of a track in a fragment.
func trackTime(b []byte, moof box, track uint32) (int64, bool) {
	if moof.Type != "moof" {
		return 0, false
	}
	for _, traf := range walk(b, moof.Off+moof.Hdr, moof.End) {
		if traf.Type != "traf" {
			continue
		}
		tfhd, ok1 := find(b, traf, "tfhd")
		tfdt, ok2 := find(b, traf, "tfdt")
		if ok1 && ok2 && sized(b, tfdt, 16, 20) && be32(b, tfhd.Off+tfhd.Hdr+4) == track {
			t, _ := readTime(b, tfdt)
			return t, true
		}
	}
	return 0, false
}

// firstTimes is each track's first decode time among the whole fragments at
// the start of b, and whether any of their `tfdt`s is 32-bit.
func firstTimes(b []byte, have map[uint32]trackInfo) (map[uint32]int64, bool, error) {
	first := map[uint32]int64{}
	v0 := false
	for _, x := range walk(b, 0, len(b)) {
		if x.Type != "moof" {
			continue
		}
		f, err := readMoof(b[x.Off:x.End], have)
		if err != nil {
			return nil, false, err
		}
		for _, t := range f.trafs {
			if _, ok := first[t.track]; !ok {
				first[t.track] = t.time
			}
			v0 = v0 || !t.v1
		}
	}
	if len(first) == 0 {
		return nil, false, errors.New("mp4export: no whole fragment where a recording's part begins")
	}
	return first, v0, nil
}

// readTime is a `tfdt`'s decode time, and whether it is 64-bit. The box
// is as long as its version says (sized).
func readTime(b []byte, tfdt box) (int64, bool) {
	p := tfdt.Off + tfdt.Hdr
	if b[p] == 1 {
		return int64(binary.BigEndian.Uint64(b[p+4:])), true
	}
	return int64(be32(b, p+4)), false
}

// moofInfo is where a `moof`'s sequence number is, and each traf's track,
// time and duration.
type moofInfo struct {
	mfhd  int
	trafs []trafInfo
}

type trafInfo struct {
	track    uint32
	tfdt     int // where the time is
	v1       bool
	time     int64
	duration int64
}

// readMoof reads one whole `moof` (b is the box), as the console's `scan`
// reads each.
func readMoof(b []byte, have map[uint32]trackInfo) (moofInfo, error) {
	var f moofInfo
	boxes := walk(b, 0, len(b))
	if len(boxes) != 1 || boxes[0].Type != "moof" {
		return f, errors.New("mp4export: not a whole moof")
	}
	m := boxes[0]
	mfhd, ok := find(b, m, "mfhd")
	if !ok || mfhd.End-mfhd.Off < 16 {
		return f, errors.New("mp4export: a fragment without its mfhd")
	}
	f.mfhd = mfhd.Off + mfhd.Hdr + 4
	for _, traf := range walk(b, m.Off+m.Hdr, m.End) {
		if traf.Type != "traf" {
			continue
		}
		tfhd, ok1 := find(b, traf, "tfhd")
		tfdt, ok2 := find(b, traf, "tfdt")
		if !ok1 || !ok2 || tfhd.End-tfhd.Off < 16 || !sized(b, tfdt, 16, 20) {
			return f, errors.New("mp4export: a fragment without its tfhd or tfdt")
		}
		p := tfhd.Off + tfhd.Hdr
		flags := be32(b, p) & 0xffffff
		track := be32(b, p+4)
		info, known := have[track]
		if !known {
			return f, errors.New("mp4export: a fragment of a track its init segment does not have")
		}
		// Data offsets from anywhere but the moof would point elsewhere once moved.
		if flags&0x01 != 0 {
			return f, errors.New("mp4export: a fragment with an absolute data offset")
		}
		q := p + 8
		if flags&0x02 != 0 {
			q += 4
		}
		fallback := int64(info.duration)
		if flags&0x08 != 0 {
			fallback = int64(be32(b, q))
		}
		var duration int64
		for _, trun := range walk(b, traf.Off+traf.Hdr, traf.End) {
			if trun.Type != "trun" {
				continue
			}
			r := trun.Off + trun.Hdr
			tf := be32(b, r) & 0xffffff
			n := int64(be32(b, r+4))
			if tf&0x100 == 0 {
				duration += n * fallback
				continue
			}
			s := r + 8
			if tf&0x01 != 0 {
				s += 4
			}
			if tf&0x04 != 0 {
				s += 4
			}
			step := 4
			for _, bit := range []uint32{0x200, 0x400, 0x800} {
				if tf&bit != 0 {
					step += 4
				}
			}
			if int64(s)+n*int64(step) > int64(trun.End) {
				return f, errors.New("mp4export: a trun runs past its box")
			}
			for k := int64(0); k < n; k, s = k+1, s+step {
				duration += int64(be32(b, s))
			}
		}
		time, v1 := readTime(b, tfdt)
		f.trafs = append(f.trafs, trafInfo{track: track, tfdt: tfdt.Off + tfdt.Hdr + 4, v1: v1, time: time, duration: duration})
	}
	return f, nil
}

// initHeader is the export's init segment, as the console's `header` makes
// it. `mvhd`'s creation time is the first sample's wall clock time (at, in
// ms). An `empty_moov` says nothing of how long the fragments are, and a
// player that guesses from the first few guesses short, so the file says: an
// `mehd`, and the durations of the movie and of each track (end, in its
// ticks).
func initHeader(src []byte, at float64, end map[uint32]int64) ([]byte, error) {
	init := append([]byte(nil), src...)
	moov, ok := find(init, box{End: len(init)}, "moov")
	if !ok {
		return nil, errors.New("no init segment")
	}
	mvhd, ok := find(init, moov, "mvhd")
	if !ok || !sized(init, mvhd, 108, 120) {
		return init, nil
	}
	// put writes a field that is 64 bits in a version 1 box and 32 in a version 0.
	put := func(x box, v0, v1 int, n int64) {
		if init[x.Off+x.Hdr] == 1 {
			binary.BigEndian.PutUint64(init[field(init, x, v0, v1):], uint64(n))
		} else {
			binary.BigEndian.PutUint32(init[field(init, x, v0, v1):], uint32(min(n, math.MaxUint32)))
		}
	}
	timescale := be32(init, field(init, mvhd, 12, 20))
	put(mvhd, 4, 4, int64(math.Floor(at/1000))+epoch1904)
	var duration int64
	for _, trak := range walk(init, moov.Off+moov.Hdr, moov.End) {
		if trak.Type != "trak" {
			continue
		}
		tkhd, ok1 := find(init, trak, "tkhd")
		mdia, ok2 := find(init, trak, "mdia")
		if !ok1 || !ok2 {
			continue
		}
		mdhd, ok := find(init, mdia, "mdhd")
		if !ok {
			continue
		}
		id := be32(init, field(init, tkhd, 12, 20))
		scale := be32(init, field(init, mdhd, 12, 20))
		ticks := end[id]
		movie := round(float64(ticks) / float64(scale) * float64(timescale))
		put(tkhd, 20, 28, movie)
		put(mdhd, 16, 24, ticks)
		duration = max(duration, movie)
	}
	put(mvhd, 16, 24, duration)

	mvex, ok := find(init, moov, "mvex")
	if !ok {
		return init, nil
	}
	if mehd, ok := find(init, mvex, "mehd"); ok {
		if sized(init, mehd, 16, 20) {
			put(mehd, 4, 4, duration)
		}
		return init, nil
	}
	// A version 1 `mehd` first in `mvex`; the boxes around it grow by its size.
	mehd := make([]byte, 20)
	binary.BigEndian.PutUint32(mehd, 20)
	copy(mehd[4:], "mehd")
	mehd[8] = 1
	binary.BigEndian.PutUint64(mehd[12:], uint64(duration))
	for _, x := range []box{moov, mvex} {
		if x.Hdr == 16 {
			binary.BigEndian.PutUint64(init[x.Off+8:], uint64(x.End-x.Off+20))
		} else {
			binary.BigEndian.PutUint32(init[x.Off:], uint32(x.End-x.Off+20))
		}
	}
	cut := mvex.Off + mvex.Hdr
	out := make([]byte, 0, len(init)+20)
	out = append(out, init[:cut]...)
	out = append(out, mehd...)
	return append(out, init[cut:]...), nil
}
