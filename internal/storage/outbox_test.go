package storage

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
)

// A batch whose call fails stays queued, in order, and is sent again as it
// was: the CP answers Unavailable when the store could not apply an event
// of it (#343). Only a call that answers takes it off the queue.
func TestOutboxKeepsABatchItsCallFailed(t *testing.T) {
	o := NewOutbox()
	var evs []*api.Event
	for i := range 3 {
		ev := api.Event_builder{Missing: api.LaminaMissing_builder{LaminaKey: string(rune('a' + i))}.Build()}.Build()
		evs = append(evs, ev)
		o.Push(ev)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sent := make(chan []*api.Event, 4)
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- o.Drain(ctx, func(ctx context.Context, batch []*api.Event) error {
			calls++
			sent <- append([]*api.Event(nil), batch...)
			if calls == 1 {
				return status.Error(codes.Unavailable, "the store could not apply an event of the batch just now; send the batch again")
			}
			return nil
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	first := <-sent
	require.Equal(t, evs, first)
	require.Equal(t, 3, o.Len(), "a failed call leaves the batch queued")
	second := <-sent
	require.Equal(t, evs, second, "sent again as it was, in order")
	require.Eventually(t, func() bool { return o.Len() == 0 }, 5*time.Second, 10*time.Millisecond, "an answered call takes it off the queue")
	cancel()
	require.NoError(t, <-done)
}
