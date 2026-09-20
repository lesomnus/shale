package storage

import (
	"context"
	"errors"
	"sync"
)

// The Device Queue (§24): one worker per physical device, shared by every
// sink on it, so the I/O of different jobs is never interleaved on the
// device. Jobs are bounded (a part, a read chunk, a maintenance step) and
// taken by deficit round robin by bytes across WRITE, READ, and MAINT
// (§24.1): work-conserving, with a bounded wait for a backlogged class.

// Class is what a job is for.
type Class int

const (
	ClassWrite Class = iota
	ClassRead
	ClassMaint
	classes
)

func (c Class) String() string {
	switch c {
	case ClassWrite:
		return "WRITE"
	case ClassRead:
		return "READ"
	case ClassMaint:
		return "MAINT"
	}

	return "?"
}

// ErrBacklog is a class at its backlog cap: the work is refused rather
// than queued (§24.1).
var ErrBacklog = errors.New("the device queue is full")

// Quanta are the bytes each class may take per round; a class with no
// backlog gives its share to the others.
type Quanta [classes]int64

// DefaultQuanta are §24.1's weights over a 16 MB part: WRITE : READ :
// MAINT = 1 : 1 : 0.1, with MAINT never below maint_quantum (1 MB).
var DefaultQuanta = Quanta{16 << 20, 16 << 20, 2 << 20}

// Caps bound each class's backlog in jobs.
type Caps [classes]int

// DefaultCaps: the WRITE backlog is bounded by the part buffer pool as
// well; READ by read_backlog (64 chunks); MAINT generously.
var DefaultCaps = Caps{256, 64, 4096}

type job struct {
	class Class
	cost  int64
	fn    func() error
	done  chan error
}

// Queue is one device's worker.
type Queue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queues  [classes][]*job
	deficit [classes]int64
	quanta  Quanta
	caps    Caps
	closed  bool
	running bool
	// cur is the class being served; charged says its quantum for this
	// visit was credited.
	cur     Class
	charged bool
}

// NewQueue makes a queue; Run drives it.
func NewQueue(q Quanta, caps Caps) *Queue {
	for i := range q {
		if q[i] <= 0 {
			q[i] = DefaultQuanta[i]
		}
		if caps[i] <= 0 {
			caps[i] = DefaultCaps[i]
		}
	}
	dq := &Queue{quanta: q, caps: caps}
	dq.cond = sync.NewCond(&dq.mu)

	return dq
}

// Submit hands a job to the device and waits for it to run. It answers
// ErrBacklog when the class is at its cap, and the context's error when
// the caller gave up waiting; a job that started runs to its end.
func (q *Queue) Submit(ctx context.Context, class Class, cost int64, fn func() error) error {
	if cost < 0 {
		cost = 0
	}
	j := &job{class: class, cost: cost, fn: fn, done: make(chan error, 1)}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()

		return errors.New("the device queue is closed")
	}
	if len(q.queues[class]) >= q.caps[class] {
		q.mu.Unlock()

		return ErrBacklog
	}
	q.queues[class] = append(q.queues[class], j)
	q.mu.Unlock()
	q.cond.Broadcast()

	select {
	case err := <-j.done:
		return err
	case <-ctx.Done():
		// Taken back when it has not started; else its end is awaited.
		q.mu.Lock()
		for i, x := range q.queues[class] {
			if x == j {
				q.queues[class] = append(q.queues[class][:i], q.queues[class][i+1:]...)
				q.mu.Unlock()

				return ctx.Err()
			}
		}
		q.mu.Unlock()

		return <-j.done
	}
}

// Depth is the backlog of a class.
func (q *Queue) Depth(class Class) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.queues[class])
}

// Run is the worker: until the context is done, it takes jobs by deficit
// round robin across the classes.
func (q *Queue) Run(ctx context.Context) {
	q.mu.Lock()
	q.running = true
	q.mu.Unlock()
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		q.cond.Broadcast()
	}()

	for {
		j := q.next()
		if j == nil {
			return
		}
		err := j.fn()
		j.done <- err
	}
}

// next is the job to run now, or nil when the queue is closed. The worker
// stays on a class while its deficit covers the job at its head, then
// moves to the next; a class is credited its quantum on arrival, keeps
// what it did not spend while it has a backlog, and starts over once it
// empties.
func (q *Queue) next() *job {
	q.mu.Lock()
	defer q.mu.Unlock()
	empties := 0
	for {
		if q.closed {
			return nil
		}
		c := q.cur
		if len(q.queues[c]) == 0 {
			q.deficit[c] = 0
			q.advance()
			empties++
			if empties >= int(classes) {
				q.cond.Wait()
				empties = 0
			}
			continue
		}
		empties = 0
		if !q.charged {
			q.deficit[c] += q.quanta[c]
			q.charged = true
		}
		head := q.queues[c][0]
		if q.deficit[c] < head.cost {
			// Not this round: the credit carries over.
			q.advance()
			continue
		}
		q.queues[c] = q.queues[c][1:]
		q.deficit[c] -= head.cost
		if len(q.queues[c]) == 0 {
			q.deficit[c] = 0
			q.advance()
		}

		return head
	}
}

// advance moves the worker to the next class, to be credited on arrival.
func (q *Queue) advance() {
	q.cur = (q.cur + 1) % classes
	q.charged = false
}
