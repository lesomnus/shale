package producer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	catalog "github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
)

func TestSourceRegistryRetainsRAMAndCounters(t *testing.T) {
	p := &Producer{cfg: Config{Buffer: 3}, sources: map[string]*source{}}
	p.relay = newRelayLink(p)
	old := &source{cfg: SourceConfig{Alias: "camera"}, pending: []*Segment{NewSegment(time.Now(), []byte("data"))}, stored: 7, lost: 2, cut: 1}
	require.NoError(t, p.installSource(old))
	p.retireSource(old)
	require.Empty(t, p.activeSources())
	require.Nil(t, p.sourceByAlias("camera"))
	require.True(t, p.overBudget(), "draining uploads still consume the host RAM budget")
	require.Equal(t, int64(4), p.Stats().Held)
	require.Equal(t, int64(7), p.Stats().Stored)
	next := &source{cfg: SourceConfig{Alias: "camera"}}
	require.NoError(t, p.installSource(next))
	p.retireSource(old) // a late old-run callback must not remove its replacement.
	require.Same(t, next, p.sourceByAlias("camera"))
	old.pending[0].Discard()
	old.pending = nil // lifecycle calls forget only after its capture/uploader join.
	p.forgetDrainedSource(old)
	p.forgetDrainedSource(old) // terminal accounting is idempotent.
	require.False(t, p.overBudget())
	st := p.Stats()
	require.Equal(t, int64(7), st.Stored)
	require.Equal(t, int64(2), st.Lost)
	require.Equal(t, int64(1), st.Cut)
	require.Zero(t, st.Held)
}

func TestSourceRegistryRejectsDuplicateIdentity(t *testing.T) {
	p := &Producer{}
	first := &source{cfg: SourceConfig{Alias: "one"}, row: api.Source_builder{Id: []byte("stable-source")}.Build()}
	require.NoError(t, p.installSource(first))
	require.ErrorIs(t, p.installSource(&source{cfg: SourceConfig{Alias: "one"}}), catalog.ErrConflict)
	require.ErrorIs(t, p.installSource(&source{cfg: SourceConfig{Alias: "renamed"}, row: first.row}), catalog.ErrConflict)
	snapshot := p.activeSources()
	snapshot[0] = nil
	require.Same(t, first, p.activeSources()[0], "readers must not be able to mutate registry storage")
}

func TestSourceRegistryConcurrentRetirement(t *testing.T) {
	p := &Producer{sources: map[string]*source{}}
	p.relay = newRelayLink(p)
	var readers sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = p.activeSources()
				_ = p.retainedSources()
				_ = p.sourceByAlias("camera")
				_ = p.memberCount()
				_ = p.Stats()
				_ = p.overBudget()
			}
		})
	}
	for i := range 200 {
		s := &source{cfg: SourceConfig{Alias: fmt.Sprint(i)}, stored: 1}
		require.NoError(t, p.installSource(s))
		p.retireSource(s)
		p.forgetDrainedSource(s)
	}
	close(stop)
	readers.Wait()
	require.Equal(t, int64(200), p.Stats().Stored)
	require.Empty(t, p.retainedSources())
}
