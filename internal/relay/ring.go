package relay

import (
	"time"

	"github.com/lesomnus/shale/internal/mpegts"
)

// The recent window (§39.4): per source, the TS bytes the producer sent
// over the last while, kept as they came, and handed out from a keyframe
// on as one TS in the shape of a lamina, tables first. It is the open
// lamina's bytes: what nobody can read from a Storage Node yet, and what a
// viewer wants first after an incident. The window is what the token
// says, which the CP sized from the source's segment duration; a budget
// over every source bounds the relay's memory.

// chunk is one Data message as it came.
type chunk struct {
	b   []byte
	off int64
	at  time.Time
}

// keyAt is a keyframe's place in the stream.
type keyAt struct {
	off    int64
	pts    int64
	cc     byte
	at     time.Time
	params bool
}

type ring struct {
	chunks []chunk
	keys   []keyAt
	// torn is the offset of the last unit around which packets went
	// missing; bytes from there on are not handed out until the next
	// keyframe. -1 is none.
	torn  int64
	bytes int64
	// last is the last unit's PTS, for how long the window is.
	last int64
}

func newRing() *ring { return &ring{torn: -1, last: -1} }

// reset forgets everything: a new stream begins at offset 0.
func (r *ring) reset() { *r = ring{torn: -1, last: -1} }

// add keeps a chunk that begins at `off` in the stream.
func (r *ring) add(b []byte, off int64, at time.Time) {
	r.chunks = append(r.chunks, chunk{b: append([]byte(nil), b...), off: off, at: at})
	r.bytes += int64(len(b))
}

// key notes a keyframe at `off`.
func (r *ring) key(off, pts int64, cc byte, at time.Time, params bool) {
	r.keys = append(r.keys, keyAt{off: off, pts: pts, cc: cc, at: at, params: params})
}

// tear notes that packets went missing around the unit at `off`.
func (r *ring) tear(off int64) {
	if off > r.torn {
		r.torn = off
	}
}

// unit notes the last unit's PTS.
func (r *ring) unit(pts int64) { r.last = pts }

// trim drops what came before `t`.
func (r *ring) trim(t time.Time) {
	n := 0
	for n < len(r.chunks) && r.chunks[n].at.Before(t) {
		r.bytes -= int64(len(r.chunks[n].b))
		n++
	}
	if n > 0 {
		r.chunks = append(r.chunks[:0], r.chunks[n:]...)
		r.dropKeys()
	}
}

// evict drops the oldest chunks until at least `n` bytes are gone, or the
// ring is empty; it answers how many went.
func (r *ring) evict(n int64) int64 {
	var gone int64
	i := 0
	for i < len(r.chunks) && gone < n {
		gone += int64(len(r.chunks[i].b))
		i++
	}
	if i > 0 {
		r.chunks = append(r.chunks[:0], r.chunks[i:]...)
		r.bytes -= gone
		r.dropKeys()
	}

	return gone
}

// dropKeys forgets the keyframes that fell off the front.
func (r *ring) dropKeys() {
	if len(r.chunks) == 0 {
		r.keys = r.keys[:0]
		return
	}
	base := r.chunks[0].off
	n := 0
	for n < len(r.keys) && r.keys[n].off < base {
		n++
	}
	if n > 0 {
		r.keys = append(r.keys[:0], r.keys[n:]...)
	}
}

// start is the keyframe a snapshot begins at: the oldest kept one that
// came at or after `since`, past the last tear.
func (r *ring) start(since time.Time) (keyAt, bool) {
	for _, k := range r.keys {
		if k.off <= r.torn || k.at.Before(since) {
			continue
		}

		return k, true
	}

	return keyAt{}, false
}

// snapshot is the bytes from the keyframe `k` up to `end` (the whole ring
// when negative), as one TS: the tables, the parameter sets when the
// keyframe has none in-band, then the stream as it came. `end` is where
// the unit still being assembled began, so the stream ends with whole
// frames.
func (r *ring) snapshot(k keyAt, end int64, tables, params []byte, pid uint16) []byte {
	out := append([]byte(nil), tables...)
	if !k.params && len(params) > 0 {
		out = append(out, mpegts.ParamPackets(pid, mpegts.PTSBytes(k.pts), k.cc, params)...)
	}
	for _, c := range r.chunks {
		from, to := c.off, c.off+int64(len(c.b))
		if to <= k.off || (end >= 0 && from >= end) {
			continue
		}
		lo, hi := int64(0), int64(len(c.b))
		if from < k.off {
			lo = k.off - from
		}
		if end >= 0 && to > end {
			hi = end - from
		}
		if hi > lo {
			out = append(out, c.b[lo:hi]...)
		}
	}

	return out
}

// seconds is how long the window from `k` on is, by the stamps.
func (r *ring) seconds(k keyAt) float64 {
	if r.last < 0 || r.last < k.pts {
		return 0
	}

	return float64(r.last-k.pts) / 90000
}
