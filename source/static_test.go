package source_test

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
)

func TestStaticPreservesLegacyAliasesWithoutSourceIDs(t *testing.T) {
	inputs := []string{"v4l2:/dev/video0", "raw:/camera-fixture.mp4", "push", ""}
	specs := make([]source.Spec, len(inputs))
	for i, input := range inputs {
		specs[i] = source.Spec{Alias: []string{"usb", "fixture", "pushed", "local-command"}[i], Input: input, Labels: map[string]string{"position": "original"}}
	}
	p, err := source.NewStatic(specs)
	require.NoError(t, err)
	require.Zero(t, specs[0].Revision, "adapter must not mutate the caller's configuration")
	specs[0].Labels["position"] = "caller-write"
	v, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	for i, spec := range v.Sources {
		require.Equal(t, inputs[i], spec.Input)
		require.Empty(t, spec.SourceID, "legacy Source ID is assigned by normal registration")
		require.Empty(t, spec.BindingID)
		require.EqualValues(t, 1, spec.Revision)
	}
	require.Equal(t, "original", v.Sources[0].Labels["position"])
	v.Sources[0].Labels["position"] = "consumer-write"
	again, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, "original", again.Sources[0].Labels["position"])
}

func TestStaticWaitAndEmptyState(t *testing.T) {
	p, err := source.NewStatic(nil)
	require.NoError(t, err)
	v, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	require.Empty(t, v.Sources)
	r, err := p.WaitForChange(context.Background(), source.Revision{})
	require.NoError(t, err)
	require.Equal(t, v.Revision, r)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = p.WaitForChange(ctx, r)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = p.WaitForChange(context.Background(), source.Revision{Catalog: 2})
	require.ErrorIs(t, err, source.ErrStale)
	var uninitialized source.Static
	_, err = uninitialized.Snapshot(context.Background())
	require.ErrorIs(t, err, source.ErrUnavailable)
}
