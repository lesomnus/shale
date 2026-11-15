package reader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/hostagent"
)

func TestNewNeedsStateAndCp(t *testing.T) {
	_, err := New(Config{Cp: "cp.example.com"})
	require.ErrorContains(t, err, "state directory")
	_, err = New(Config{StateDir: t.TempDir()})
	require.ErrorContains(t, err, "cp")

	r, err := New(Config{StateDir: t.TempDir(), Cp: "cp.example.com"})
	require.NoError(t, err)
	require.Equal(t, DefaultHeartbeatInterval, r.cfg.HeartbeatInterval)
	require.Equal(t, hostagent.JoinInterval, r.cfg.RetryInterval)
	require.True(t, r.Id().IsZero())
}

// A reader whose CP will not answer keeps asking until it is stopped, and
// a stop is not an error.
func TestRunStopsWhileJoining(t *testing.T) {
	r, err := New(Config{StateDir: t.TempDir(), Cp: "http://127.0.0.1:1", Dev: true, RetryInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, r.Run(ctx))
	k, err := r.Store().Key()
	require.NoError(t, err)
	require.NotEmpty(t, k.Fingerprint(), "the key is made before the first join")
}
