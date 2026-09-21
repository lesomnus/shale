package producer

import (
	"errors"
	"io"
	"sync"
	"time"
)

// Segment is one lamina in the making: bytes that grow while the capture
// runs and an uploader reads them, closed when the segment is cut (§38.2).
// It is the RAM the producer holds under its `retain` policy (§16).
type Segment struct {
	// Started is the arrival time of the first keyframe; Ended the arrival
	// time of the next segment's first keyframe, or the last packet when
	// the source stopped (§38.2).
	Started time.Time
	Ended   time.Time
	// Slot is the schedule slot the segment belongs to.
	Slot time.Time
	// Early says the segment was cut early for running over its ceiling.
	Early bool
	// Stopped says the camera stopped rather than the schedule cutting it.
	Stopped bool

	mu   sync.Mutex
	cond *sync.Cond
	// dark says the segment opened while the scene was dark past the
	// source's threshold, and is skipped unless the scene is lit before
	// it closes (§38.10).
	dark bool
	// buf holds the bytes from base on: under `retain: written` the bytes
	// a node reported durable are released and base moves up (§12.2).
	buf       []byte
	base      int64
	closed    bool
	discarded bool
}

// NewSegment makes a segment for a test or a synthetic producer.
func NewSegment(started time.Time, tables []byte) *Segment {
	return newSegment(started, started, tables)
}

func newSegment(started, slot time.Time, tables []byte) *Segment {
	s := &Segment{Started: started, Slot: slot}
	s.cond = sync.NewCond(&s.mu)
	s.buf = append(s.buf, tables...)

	return s
}

// Write appends bytes. A discarded segment counts them and keeps none.
func (s *Segment) Write(b []byte) {
	s.mu.Lock()
	if s.discarded {
		s.base += int64(len(b))
	} else {
		s.buf = append(s.buf, b...)
	}
	s.mu.Unlock()
	s.cond.Broadcast()
}

// Discard lets every byte go, now and as they keep arriving: the segment
// was given up (lost, or cut short at a node's offset) while the capture
// still writes into it, and what comes until its cut has nowhere to go.
// Len keeps counting so the schedule cuts it as it would any other.
func (s *Segment) Discard() {
	s.mu.Lock()
	s.discarded = true
	s.base += int64(len(s.buf))
	s.buf = nil
	s.mu.Unlock()
	s.cond.Broadcast()
}

// Dark says whether the segment is one to skip (§38.10).
func (s *Segment) Dark() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.dark
}

// SetDark marks the segment as one to skip, or not any more.
func (s *Segment) SetDark(v bool) {
	s.mu.Lock()
	s.dark = v
	s.mu.Unlock()
	s.cond.Broadcast()
}

// Close ends the segment.
func (s *Segment) Close(ended time.Time) {
	s.mu.Lock()
	s.closed = true
	s.Ended = ended
	s.mu.Unlock()
	s.cond.Broadcast()
}

// Len is the bytes so far, released ones included.
func (s *Segment) Len() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.base + int64(len(s.buf))
}

// Held is the bytes still in RAM: what the budget counts (§16).
func (s *Segment) Held() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return int64(len(s.buf))
}

// Released is the offset below which the bytes were let go (§12.2).
func (s *Segment) Released() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.base
}

// Release lets the bytes below `upTo` go: a node reported them durable and
// `retain: written` keeps only what is above (§12.2). The rest is copied
// out so the memory really goes. A reader below the offset then fails, so
// the segment can no longer be sent from its start to anyone else.
func (s *Segment) Release(upTo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if upTo <= s.base {
		return
	}
	if end := s.base + int64(len(s.buf)); upTo > end {
		upTo = end
	}
	rest := s.buf[upTo-s.base:]
	s.buf = append(make([]byte, 0, max(2*len(rest), 64<<10)), rest...)
	s.base = upTo
}

// Closed says whether the segment is complete.
func (s *Segment) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// Bytes is the segment, for a buffered upload after Close: what is held,
// which is the whole of it unless bytes were released.
func (s *Segment) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf
}

// ReaderFrom reads the segment from an offset, blocking for bytes that have
// not arrived yet and ending when the segment is closed, so a live upload
// streams what a capture produces (§12.2).
func (s *Segment) ReaderFrom(offset int64) io.Reader {
	return &segReader{s: s, off: offset}
}

type segReader struct {
	s   *Segment
	off int64
}

var (
	errPastEnd  = errors.New("segment: offset past the end")
	errReleased = errors.New("segment: the bytes at this offset were released")
)

func (r *segReader) Read(p []byte) (int, error) {
	s := r.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.off < s.base {
		return 0, errReleased
	}
	for s.base+int64(len(s.buf)) <= r.off {
		if s.closed {
			if s.base+int64(len(s.buf)) < r.off {
				return 0, errPastEnd
			}

			return 0, io.EOF
		}
		s.cond.Wait()
	}
	n := copy(p, s.buf[r.off-s.base:])
	r.off += int64(n)

	return n, nil
}

// Schedule is when a source's segments are due (§12.2, §38.2).
type Schedule struct {
	// Duration is the agreed segment duration; Phase the member's offset.
	Duration time.Duration
	Phase    time.Duration
	// Ceiling is max_bitrate × duration in bytes: a segment that reaches it
	// before its phase is cut early (§38.2).
	Ceiling int64
}

// SlotOf is the slot a time falls in.
func (s Schedule) SlotOf(t time.Time) time.Time {
	if s.Duration <= 0 {
		return t
	}

	return t.Add(-s.Phase).Truncate(s.Duration).Add(s.Phase)
}

// Due says whether a keyframe arriving at t should start a new segment
// (§38.2): the current segment's slot has passed, or it is over its
// ceiling.
func (s Schedule) Due(cur *Segment, t time.Time) (due bool, early bool) {
	if cur == nil {
		return true, false
	}
	if s.Duration <= 0 {
		return false, false
	}
	if !t.Before(cur.Slot.Add(s.Duration)) {
		return true, false
	}
	if s.Ceiling > 0 && cur.Len() >= s.Ceiling {
		return true, true
	}

	return false, false
}

// Cutter turns packets into segments for one source.
type Cutter struct {
	// Reader is the TS stream; nil for a raw source, whose frames come
	// through FeedFrame (§38.9).
	Reader   *Reader
	Schedule func() Schedule
	// Out receives every closed segment; Open every segment as it opens.
	Out  func(*Segment)
	Open func(*Segment)
	Now  func() time.Time
	// Prefix is what a raw source's laminae start with, the last prefix
	// frame; OnPrefix is told when a new one arrives, so the source keeps
	// it across streams.
	Prefix   func() []byte
	OnPrefix func([]byte)

	cur   *Segment
	stats CutStats
	// last is the data time of the last frame of a raw source, which is
	// where its segment ends when the source stops.
	last time.Time
}

// CutStats is what the heartbeat reports about the cutting (§38.6).
type CutStats struct {
	Segments  int64
	EarlyCuts int64
	Frames    int64
	Keyframes int64
	Bytes     int64
	LastKey   time.Time
	// KeyInterval is the last measured keyframe interval.
	KeyInterval time.Duration
	// ParamsAdded counts segments given the parameter sets their first
	// keyframe came without (§38.2).
	ParamsAdded int64
}

// Stats is a copy of the counters.
func (c *Cutter) Stats() CutStats { return c.stats }

// Feed handles one packet.
func (c *Cutter) Feed(p *Packet) {
	now := c.now()
	if c.Reader.IsVideoFrame(p) {
		c.stats.Frames++
	}
	if c.Reader.IsKeyframe(p) {
		c.stats.Keyframes++
		if !c.stats.LastKey.IsZero() {
			c.stats.KeyInterval = now.Sub(c.stats.LastKey)
		}
		c.stats.LastKey = now
		if due, early := c.Schedule().Due(c.cur, now); due {
			c.cut(now, early)
			if c.cur != nil {
				// A keyframe without its parameter sets: the last ones
				// seen go in front, or the lamina would not play on its
				// own (§38.2).
				if ps := c.Reader.ParamSets(p); ps != nil {
					c.cur.Write(ps)
					c.stats.ParamsAdded++
				}
			}
		}
	}
	if c.cur == nil {
		// Before the first keyframe: nothing playable to keep.
		return
	}
	c.cur.Write(p.Data[:])
	c.stats.Bytes += PacketSize
}

func (c *Cutter) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}

	return time.Now()
}

// FeedFrame handles one frame of a raw source (§38.9): a prefix frame is
// kept for the laminae to come, a data frame may start a lamina, and its
// timestamp is the data time when the writer gave one.
func (c *Cutter) FeedFrame(f *Frame) {
	t := f.Time
	if t.IsZero() {
		t = c.now()
	}
	if f.Kind == FramePrefix {
		if c.OnPrefix != nil {
			c.OnPrefix(append([]byte(nil), f.Payload...))
		}

		return
	}
	c.stats.Frames++
	c.stats.Keyframes++
	c.stats.LastKey = c.now()
	if due, early := c.Schedule().Due(c.cur, t); due {
		c.cut(t, early)
	}
	c.last = t
	c.cur.Write(f.Payload)
	c.stats.Bytes += int64(len(f.Payload))
}

// prefix is what a new segment starts with: the TS tables, or a raw
// source's prefix frame. A TS segment without tables could not play on its
// own and is not started; a raw source may have no prefix at all.
func (c *Cutter) prefix() ([]byte, bool) {
	if c.Reader != nil {
		tables := c.Reader.Tables()

		return tables, tables != nil
	}
	if c.Prefix != nil {
		return c.Prefix(), true
	}

	return nil, true
}

// cut closes the current segment and opens the next one at this keyframe.
func (c *Cutter) cut(now time.Time, early bool) {
	tables, ok := c.prefix()
	if !ok {
		// No PAT/PMT yet: the segment could not play on its own.
		return
	}
	if c.cur != nil {
		c.cur.Early = early
		c.cur.Close(now)
		if early {
			c.stats.EarlyCuts++
		}
		if c.Out != nil {
			c.Out(c.cur)
		}
	}
	c.cur = newSegment(now, c.Schedule().SlotOf(now), tables)
	c.stats.Segments++
	if c.Open != nil {
		c.Open(c.cur)
	}
}

// Stop closes the current segment because the source stopped (§15).
func (c *Cutter) Stop() {
	if c.cur == nil {
		return
	}
	c.cur.Stopped = true
	ended := c.now()
	if c.Reader == nil && !c.last.IsZero() {
		// A raw source's data ended where its last frame said.
		ended = c.last
	}
	c.cur.Close(ended)
	if c.Out != nil {
		c.Out(c.cur)
	}
	c.cur = nil
}

// Current is the open segment, or nil.
func (c *Cutter) Current() *Segment { return c.cur }
