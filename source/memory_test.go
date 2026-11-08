package source_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
)

func TestMemoryPublicationAndIsolatedOwnership(t *testing.T) {
	ctx := context.Background()
	v := managed(1)
	p, err := source.NewMemory(v)
	require.NoError(t, err)
	v.Sources[0].Labels["room"] = "caller-write"
	v.Sources[0].Credential.Version = "caller-write"
	accepted, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, "entrance", accepted.Sources[0].Labels["room"])
	require.Equal(t, "1", accepted.Sources[0].Credential.Version)
	accepted.Sources[0].Labels["room"] = "consumer-write"
	accepted.Sources[0].Credential.Version = "consumer-write"
	before, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, "entrance", before.Sources[0].Labels["room"])
	require.Equal(t, "1", before.Sources[0].Credential.Version)

	next := before.Clone()
	next.Revision.Catalog = 2
	next.Sources[0].Revision = 2
	next.Sources[0].Input = "rtsp://192.0.2.11/stream"
	require.NoError(t, p.Publish(ctx, next))
	next.Sources[0].Input = "rtsp://192.0.2.12/stream"
	current, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, current.Revision.Catalog)
	require.Equal(t, "rtsp://192.0.2.11/stream", current.Sources[0].Input)
}

func TestMemoryRejectsDivergentAndRegressingSnapshots(t *testing.T) {
	tests := []struct {
		name string
		edit func(*source.Snapshot)
		want error
	}{
		{"same revision changed content", func(v *source.Snapshot) { v.Sources[0].Name = "different" }, source.ErrConflict},
		{"older catalog", func(v *source.Snapshot) { v.Revision.Catalog = 1; v.Sources[0].Revision = 1 }, source.ErrStale},
		{"older generation newer catalog", func(v *source.Snapshot) { v.Revision.Generation = 1; v.Revision.Catalog = 3 }, source.ErrStale},
		{"newer generation older catalog", func(v *source.Snapshot) { v.Revision.Generation = 3; v.Revision.Catalog = 1; v.Sources[0].Revision = 1 }, source.ErrStale},
		{"foreign assignment", func(v *source.Snapshot) { v.Revision.AssignmentID = pdid.New(28).String() }, source.ErrConflict},
		{"foreign producer", func(v *source.Snapshot) { v.Revision.Catalog = 3; v.Scope.ProducerID = pdid.New(22).String() }, source.ErrConflict},
		{"new generation content changed without catalog", func(v *source.Snapshot) { v.Revision.Generation = 3; v.Sources[0].Name = "different" }, source.ErrConflict},
		{"invalid endpoint", func(v *source.Snapshot) {
			v.Revision.Catalog = 3
			v.Sources[0].Input = "rtsp://operator:private@192.0.2.10/stream"
		}, source.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := managed(2)
			before.Revision.Generation = 2
			p, err := source.NewMemory(before)
			require.NoError(t, err)
			next := before.Clone()
			tt.edit(&next)
			require.ErrorIs(t, p.Publish(context.Background(), next), tt.want)
			current, err := p.Snapshot(context.Background())
			require.NoError(t, err)
			require.Equal(t, before, current, "a rejected read/update never replaces last successful state")
		})
	}
}

func TestMemoryWaitCoalescesAndResynchronizes(t *testing.T) {
	ctx := context.Background()
	initial := managed(1)
	p, err := source.NewMemory(initial)
	require.NoError(t, err)
	require.NoError(t, p.Publish(ctx, initial.Clone()), "identical delivery is idempotent")
	unchanged, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, err = p.WaitForChange(unchanged, initial.Revision)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// No subscriber sees revision two: the subsequent full read still recovers
	// all of revision three without applying an incomplete delta sequence.
	next := initial.Clone()
	next.Revision.Catalog, next.Sources[0].Revision = 2, 2
	require.NoError(t, p.Publish(ctx, next))
	next.Revision.Catalog, next.Sources[0].Revision = 3, 3
	next.Sources[0].Input = "rtsp://192.0.2.13/stream"
	require.NoError(t, p.Publish(ctx, next))
	revision, err := p.WaitForChange(ctx, initial.Revision)
	require.NoError(t, err)
	require.Equal(t, next.Revision, revision)
	current, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, next, current)

	// A newer ownership fence may leave capture intent unchanged.
	next.Revision.Generation++
	require.NoError(t, p.Publish(ctx, next))
	revision, err = p.WaitForChange(ctx, current.Revision)
	require.NoError(t, err)
	require.Equal(t, next.Revision, revision)
}

func TestMemoryUnavailableEmptyAndCancellationAreDistinct(t *testing.T) {
	ctx := context.Background()
	var unavailable source.Memory
	_, err := unavailable.Snapshot(ctx)
	require.ErrorIs(t, err, source.ErrUnavailable)
	_, err = unavailable.WaitForChange(ctx, source.Revision{})
	require.ErrorIs(t, err, source.ErrUnavailable)

	empty := managed(0)
	empty.Sources = nil
	p, err := source.NewMemory(empty)
	require.NoError(t, err)
	v, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.Empty(t, v.Sources)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = p.Snapshot(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = p.WaitForChange(canceled, v.Revision)
	require.ErrorIs(t, err, context.Canceled)
	next := empty.Clone()
	next.Revision.Catalog = 1
	require.ErrorIs(t, p.Publish(canceled, next), context.Canceled)
	still, err := p.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, v, still)

	future := v.Revision
	future.Catalog++
	_, err = p.WaitForChange(ctx, future)
	require.ErrorIs(t, err, source.ErrStale)
	foreign := v.Revision
	foreign.AssignmentID = pdid.New(28).String()
	_, err = p.WaitForChange(ctx, foreign)
	require.ErrorIs(t, err, source.ErrConflict)
}

func TestMemoryConcurrentConsumersAndPublication(t *testing.T) {
	initial := managed(1)
	p, err := source.NewMemory(initial)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const finalRevision = 40
	results := make(chan error, 8)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for {
				v, err := p.Snapshot(ctx)
				if err != nil {
					results <- err
					return
				}
				if v.Revision.Catalog == finalRevision {
					results <- nil
					return
				}
				// Consumer writes must not race a different consumer or publisher.
				v.Sources[0].Labels["consumer"] = "private-copy"
				v.Sources[0].Credential.ID = "private-copy"
				if _, err = p.WaitForChange(ctx, v.Revision); err != nil {
					results <- err
					return
				}
			}
		})
	}
	for revision := int64(2); revision <= finalRevision; revision++ {
		next := initial.Clone()
		next.Revision.Catalog, next.Sources[0].Revision = revision, revision
		require.NoError(t, p.Publish(ctx, next))
	}
	readers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
}
