package satellite

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// held is a segment the satellite took under `retain: accepted` (§41.9):
// its bytes as they arrive from the producer, in RAM only, read by the
// delivery while they grow, as a live upload reads a producer's segment.
// It is the upload client's Segment, and it releases nothing: the whole
// segment stays until a node commits it, so a failed node is followed by
// the next candidate from the start, as under `committed`.
type held struct {
	lamina   pdid.Id
	producer pdid.Id
	// key is the first candidate's, which the producer's requests name.
	key     string
	handoff *api.Handoff
	maxLen  int64
	// ready is closed once the entry is admitted, or failed to be; err
	// says which.
	ready chan struct{}
	err   error

	mu     sync.Mutex
	cond   *sync.Cond
	chunks [][]byte
	n      int64
	// charged is what the retained pool counts for it: the reservation,
	// then the bytes past it.
	charged int64
	closed  bool
	started time.Time
	ended   time.Time
	// acked is the most the producer was told the satellite holds: what
	// is the satellite's to lose.
	acked int64
	// touched is the producer's last request or byte.
	touched time.Time
	// read is the furthest any delivery read: bytes handed on to a node.
	read int64
	// aborted ends every reader: the segment is let go.
	aborted error
	done    bool
	cancel  context.CancelFunc
}

// heldChunk is how much one allocation holds, as a producer's segment.
const heldChunk = 1 << 20

func newHeld(lamina, producer pdid.Id, key string, h *api.Handoff, maxLen int64, started time.Time) *held {
	v := &held{lamina: lamina, producer: producer, key: key, handoff: h, maxLen: maxLen, started: started, ready: make(chan struct{}), touched: time.Now()}
	v.cond = sync.NewCond(&v.mu)

	return v
}

// write appends bytes from the producer.
func (h *held) write(b []byte) {
	h.mu.Lock()
	h.n += int64(len(b))
	for len(b) > 0 {
		n := len(h.chunks)
		if n == 0 || len(h.chunks[n-1]) == cap(h.chunks[n-1]) {
			h.chunks = append(h.chunks, make([]byte, 0, heldChunk))
			n++
		}
		last := h.chunks[n-1]
		k := min(len(b), cap(last)-len(last))
		h.chunks[n-1] = append(last, b[:k]...)
		b = b[k:]
	}
	h.touched = time.Now()
	h.mu.Unlock()
	h.cond.Broadcast()
}

// close ends the segment: the producer sent its last byte.
func (h *held) close(ended time.Time) {
	h.mu.Lock()
	h.closed = true
	if !ended.IsZero() {
		h.ended = ended
	}
	h.mu.Unlock()
	h.cond.Broadcast()
}

// abort lets the segment go: every reader fails from now on.
func (h *held) abort(err error) {
	h.mu.Lock()
	if h.aborted == nil {
		h.aborted = err
	}
	h.chunks = nil
	h.mu.Unlock()
	h.cond.Broadcast()
}

// ack records that the producer was told the satellite holds n bytes.
func (h *held) ack(n int64) {
	h.mu.Lock()
	if n > h.acked {
		h.acked = n
	}
	h.touched = time.Now()
	h.mu.Unlock()
}

// touch is a request from the producer.
func (h *held) touch() {
	h.mu.Lock()
	h.touched = time.Now()
	h.mu.Unlock()
}

// state is what the satellite holds, whether that is all of it, and what
// it told the producer.
func (h *held) state() (n int64, closed bool, acked int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.n, h.closed, h.acked
}

func (h *held) Len() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.n
}

func (h *held) Closed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.closed
}

// Released is zero: the satellite keeps the whole segment until a node
// commits it.
func (h *held) Released() int64 { return 0 }
func (h *held) Release(int64)   {}

func (h *held) Span() (time.Time, time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.started, h.ended
}

func (h *held) Epoch() string { return h.handoff.GetAllocation().GetCaptureEpoch() }

// ReaderFrom reads the segment from an offset, blocking for bytes that
// have not arrived and ending when the segment is closed.
func (h *held) ReaderFrom(offset int64) io.Reader { return &heldReader{h: h, off: offset} }

var errPastEnd = errors.New("satellite: offset past the end of the segment")

type heldReader struct {
	h   *held
	off int64
}

func (r *heldReader) Read(p []byte) (int, error) {
	h := r.h
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.aborted == nil && h.n <= r.off {
		if h.closed {
			if h.n < r.off {
				return 0, errPastEnd
			}
			return 0, io.EOF
		}
		h.cond.Wait()
	}
	if h.aborted != nil {
		return 0, h.aborted
	}
	rel := r.off
	var src []byte
	for _, c := range h.chunks {
		if rel < int64(len(c)) {
			src = c[rel:]
			break
		}
		rel -= int64(len(c))
	}
	n := copy(p, src)
	r.off += int64(n)
	if r.off > h.read {
		h.read = r.off
	}

	return n, nil
}
