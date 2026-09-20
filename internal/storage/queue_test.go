package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A backlogged class is served at least once per round: a MAINT job does
// not wait behind every WRITE, and a WRITE does not wait behind a walk.
func TestQueueRoundRobin(t *testing.T) {
	q := NewQueue(Quanta{4, 4, 1}, Caps{100, 100, 100})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hold the worker until every job is queued.
	gate := make(chan struct{})
	var order []string
	var mu sync.Mutex
	go q.Run(ctx)
	var wg sync.WaitGroup
	submit := func(class Class, cost int64, name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.Submit(ctx, class, cost, func() error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()

				return nil
			})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.Submit(ctx, ClassWrite, 0, func() error { <-gate; return nil })
	}()
	time.Sleep(50 * time.Millisecond)
	for i := range 4 {
		submit(ClassWrite, 4, "w")
		submit(ClassRead, 4, "r")
		_ = i
	}
	submit(ClassMaint, 1, "m")
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	// Nine jobs ran; the MAINT one did not run last.
	if len(order) != 9 {
		t.Fatalf("%d jobs ran: %v", len(order), order)
	}
	last := order[len(order)-1]
	if last == "m" {
		t.Fatalf("MAINT starved: %v", order)
	}
}

func TestQueueBacklog(t *testing.T) {
	q := NewQueue(DefaultQuanta, Caps{1, 1, 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No worker: the first job fills the cap, the second is refused.
	go q.Submit(ctx, ClassRead, 1, func() error { return nil })
	time.Sleep(20 * time.Millisecond)
	if err := q.Submit(ctx, ClassRead, 1, func() error { return nil }); err != ErrBacklog {
		t.Fatalf("got %v", err)
	}
	// A caller that gives up takes its job back.
	cctx, ccancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer ccancel()
	if err := q.Submit(cctx, ClassWrite, 1, func() error { return nil }); err == nil {
		t.Fatal("a job nobody ran did not time out")
	}
	if q.Depth(ClassWrite) != 0 {
		t.Fatal("the abandoned job is still queued")
	}
}
