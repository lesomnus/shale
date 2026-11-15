package satellite

import (
	"sort"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/upload"
)

// retained is the retained pool of the memory budget (§41.9): the bytes of
// the segments the satellite took under `retain: accepted` and has not
// delivered. A segment is admitted with a reservation, the size its
// allocation expects, and grows past it as its bytes come; the pool counts
// the larger of the two, so a segment admitted has room to arrive.
//
// Each producer has a share: the pool over the producers using it, those
// holding bytes or asking for acceptance in the last activeFor, capped by
// `retained_per_producer`. A new segment is admitted only within its
// producer's share and the pool, so a producer stuck behind a failing node
// fills its share and no more, and the others still get theirs. A segment
// already admitted grows within the pool alone: shares move as producers
// come and go, and a segment half taken is not refused for that.
//
// Nothing here is ever evicted. Bytes leave the pool when their segment
// is delivered or given up.
type retained struct {
	budget int64
	// cap bounds a producer's share; zero is none but the equal share.
	cap int64
	now func() time.Time

	mu   sync.Mutex
	used int64
	peak int64
	by   map[pdid.Id]*use
}

// use is one producer's part of the pool.
type use struct {
	bytes   int64
	laminae int
	// asked is when it last asked for acceptance.
	asked time.Time
}

// activeFor is how long a producer that asked for acceptance counts among
// those the pool is shared by, holding bytes or not: about a segment, so a
// producer whose segments are delivered as fast as they come keeps its
// share.
const activeFor = 2 * time.Minute

func newRetained(budget, cap int64) *retained {
	return &retained{budget: budget, cap: cap, now: time.Now, by: map[pdid.Id]*use{}}
}

// shareOf is what producer p may hold now. The lock is held.
func (r *retained) shareOf(p pdid.Id, now time.Time) int64 {
	n := 0
	for id, u := range r.by {
		if id == p || u.bytes > 0 || now.Sub(u.asked) < activeFor {
			n++
		}
	}
	if _, ok := r.by[p]; !ok {
		n++
	}
	s := r.budget / int64(max(n, 1))
	if r.cap > 0 && s > r.cap {
		s = r.cap
	}

	return s
}

// admit reserves n bytes for a new segment of producer p, or answers why
// not: its share is taken (upload.PassShare), or the pool is
// (upload.PassFull).
func (r *retained) admit(p pdid.Id, n int64) (bool, string) {
	if r == nil || r.budget <= 0 {
		return false, upload.PassRefused
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	u := r.by[p]
	if u == nil {
		u = &use{}
		r.by[p] = u
	}
	u.asked = now
	if r.used+n > r.budget {
		return false, upload.PassFull
	}
	if u.bytes+n > r.shareOf(p, now) {
		return false, upload.PassShare
	}
	u.bytes += n
	u.laminae++
	r.take(n)

	return true, ""
}

// grow takes up to n more bytes for a segment of p already admitted,
// within the pool alone, and answers how many it took.
func (r *retained) grow(p pdid.Id, n int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	n = min(n, r.budget-r.used)
	if n <= 0 {
		return 0
	}
	u := r.by[p]
	if u == nil {
		u = &use{asked: r.now()}
		r.by[p] = u
	}
	u.bytes += n
	r.take(n)

	return n
}

// hasRoom says whether the pool has room for any byte more.
func (r *retained) hasRoom() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.used < r.budget
}

// take counts n more bytes used. The lock is held.
func (r *retained) take(n int64) {
	r.used += n
	if r.used > r.peak {
		r.peak = r.used
	}
}

// free gives back what a segment of p held, when it is delivered or given
// up.
func (r *retained) free(p pdid.Id, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.used -= n
	u := r.by[p]
	if u == nil {
		return
	}
	u.bytes -= n
	u.laminae--
	if u.bytes <= 0 && u.laminae <= 0 && r.now().Sub(u.asked) >= activeFor {
		delete(r.by, p)
	}
}

// usage is what the pool holds, the most it held, and its bound.
func (r *retained) usage() (used, peak, budget int64) {
	if r == nil {
		return 0, 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.used, r.peak, r.budget
}

// shares is the heartbeat's account of the pool by producer: those that
// hold bytes or asked lately, each with its share now.
func (r *retained) shares() []*api.RetainedShare {
	if r == nil {
		return nil
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*api.RetainedShare, 0, len(r.by))
	for id, u := range r.by {
		if u.bytes <= 0 && now.Sub(u.asked) >= activeFor {
			delete(r.by, id)
			continue
		}
		out = append(out, api.RetainedShare_builder{
			ProducerId: id.Bytes(), Bytes: u.bytes, Laminae: int32(u.laminae), Share: r.shareOf(id, now),
		}.Build())
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].GetProducerId()) < string(out[j].GetProducerId()) })

	return out
}

// holding is what producer p holds of the pool.
func (r *retained) holding(p pdid.Id) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if u := r.by[p]; u != nil {
		return u.bytes
	}

	return 0
}
