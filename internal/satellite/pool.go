package satellite

import (
	"sync/atomic"
)

// pool is the in-flight pool of the memory budget (§41.4): what the
// requests passing through hold, as a reservation of a fixed size each. A
// request that finds no room is refused, never queued: a refused producer
// keeps its bytes and comes back, as it does for a busy node.
//
// A request reserves twice `request_buffer`: the buffer its body is copied
// through on its way to the node, and what HTTP/2 lets the producer send
// ahead of it, which the listener bounds to the same size. The buffer is a
// request's own, not one handed on to the next: the transport to the node
// may still be reading it after the node has answered.
//
// The retained pool of `retain: accepted` (§41.8) is to sit beside this
// one, in the same budget; nothing here assumes it is the only pool.
type pool struct {
	budget int64
	each   int64

	used     atomic.Int64
	peak     atomic.Int64
	requests atomic.Int64
}

func newPool(budget int64, size int) *pool {
	return &pool{budget: budget, each: 2 * int64(size)}
}

// take reserves one request's share, or answers false when the pool has no
// room for it.
func (p *pool) take() bool {
	for {
		used := p.used.Load()
		if used+p.each > p.budget {
			return false
		}
		if p.used.CompareAndSwap(used, used+p.each) {
			p.requests.Add(1)
			for {
				peak := p.peak.Load()
				if used+p.each <= peak || p.peak.CompareAndSwap(peak, used+p.each) {
					break
				}
			}

			return true
		}
	}
}

// give returns one request's share.
func (p *pool) give() {
	p.used.Add(-p.each)
	p.requests.Add(-1)
}
