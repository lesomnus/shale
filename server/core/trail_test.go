package core_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/trail"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/server/core"
)

// countingStore is a trail with nothing in it that counts how often it is
// asked.
type countingStore struct {
	asked atomic.Int32
	seen  chan struct{}
}

func (s *countingStore) Older(context.Context, trail.Kinds, time.Time, int) (trail.Rows, error) {
	s.asked.Add(1)
	select {
	case s.seen <- struct{}{}:
	default:
	}

	return nil, nil
}

func (s *countingStore) Count(context.Context, trail.Kinds, time.Time) (int, error) { return 0, nil }

func (s *countingStore) Forget(context.Context, []any) (int, error) { return 0, nil }

// The sweep passes on the leader and not elsewhere (§34.9).
func TestTrailSweepOnLeader(t *testing.T) {
	p := trail.Policy{Every: time.Hour, Keep: trail.Keep{Retain: time.Hour, Discard: true}}
	require.NoError(t, p.Valid())

	t.Run("leader", func(t *testing.T) {
		s := &countingStore{seen: make(chan struct{}, 1)}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- core.TrailSweep(s, p, func(context.Context) bool { return true }).Spin(ctx) }()
		select {
		case <-s.seen:
		case <-time.After(5 * time.Second):
			t.Fatal("the leader did not sweep")
		}
		cancel()
		require.NoError(t, <-done)
	})

	t.Run("follower", func(t *testing.T) {
		s := &countingStore{seen: make(chan struct{}, 1)}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		require.NoError(t, core.TrailSweep(s, p, func(context.Context) bool { return false }).Spin(ctx))
		require.Zero(t, s.asked.Load(), "a follower swept")
	})

	t.Run("alone", func(t *testing.T) {
		s := &countingStore{seen: make(chan struct{}, 1)}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- core.TrailSweep(s, p, nil).Spin(ctx) }()
		select {
		case <-s.seen:
		case <-time.After(5 * time.Second):
			t.Fatal("a lone control plane did not sweep")
		}
		cancel()
		require.NoError(t, <-done)
	})
}
