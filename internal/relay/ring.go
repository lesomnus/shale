package relay

import (
	"time"
)

// The recent window (§39.4): per source, the fragments the producer sent
// over the last while, kept as they came, and handed out from a key
// fragment on as one fragmented MP4 with the init segment first, which is
// the shape of a lamina. It is the open lamina's bytes: what nobody can
// read from a Storage Node yet, and what a viewer wants first after an
// incident. The window is what the token says, which the CP sized from
// the source's segment duration; a budget over every source bounds the
// relay's memory.

// chunk is one fragment as it came.
type chunk struct {
	b   []byte
	off int64
	at  time.Time
}

// keyAt is a key fragment's place in the stream.
type keyAt struct {
	off int64
	// time is the fragment's decode time in the video track's timescale.
	time int64
	at   time.Time
}

type ring struct {
	chunks []chunk
	keys   []keyAt
	// next is the offset the next chunk gets: what came so far.
	next int64
	// torn is the offset of the last fragment that came after a gap;
	// fragments from before it are not handed out with it. -1 is none.
	torn  int64
	bytes int64
	// last is where the last fragment's video ends, in the track's
	// timescale, for how long the window is; -1 before the first.
	last int64
}

func newRing() *ring { return &ring{torn: -1, last: -1} }

// reset forgets everything: a new stream begins.
func (r *ring) reset() { *r = ring{torn: -1, last: -1} }

// add keeps a fragment and answers its offset.
func (r *ring) add(b []byte, at time.Time) int64 {
	off := r.next
	r.chunks = append(r.chunks, chunk{b: append([]byte(nil), b...), off: off, at: at})
	r.bytes += int64(len(b))
	r.next += int64(len(b))

	return off
}

// key notes a key fragment at `off`.
func (r *ring) key(off, time int64, at time.Time) {
	r.keys = append(r.keys, keyAt{off: off, time: time, at: at})
}

// tear notes that fragments went missing before the one at `off`.
func (r *ring) tear(off int64) {
	if off > r.torn {
		r.torn = off
	}
}

// unit notes where the last fragment's video ends.
func (r *ring) unit(end int64) { r.last = end }

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

// evict drops the oldest fragments until at least `n` bytes are gone, or
// the ring is empty; it answers how many went.
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

// dropKeys forgets the keys that fell off the front.
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

// start is the key fragment a snapshot begins at: the oldest kept one
// that came at or after `since`, at or past the last tear.
func (r *ring) start(since time.Time) (keyAt, bool) {
	for _, k := range r.keys {
		if k.off < r.torn || k.at.Before(since) {
			continue
		}

		return k, true
	}

	return keyAt{}, false
}

// snapshot is the fragments from the key `k` on, with the init segment
// in front: one fragmented MP4, the shape of a lamina.
func (r *ring) snapshot(k keyAt, init []byte) []byte {
	out := append([]byte(nil), init...)
	for _, c := range r.chunks {
		if c.off >= k.off {
			out = append(out, c.b...)
		}
	}

	return out
}

// seconds is how long the window from `k` on is, by the stamps.
func (r *ring) seconds(k keyAt, timescale int64) float64 {
	if r.last < 0 || r.last < k.time || timescale <= 0 {
		return 0
	}

	return float64(r.last-k.time) / float64(timescale)
}
