package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// TestLeaderFailover is the CP outage drill of §55 on PostgreSQL: several
// CP processes share the database and exactly one leads; when the leader
// goes, another takes the lock within a few ticks and the jobs go on.
// It runs only with SHALE_E2E_DB_DSN.
func TestLeaderFailover(t *testing.T) {
	if os.Getenv("SHALE_E2E_DB_DSN") == "" {
		t.Skip("SHALE_E2E_DB_DSN is not set")
	}
	c := start(t)
	ctx := context.Background()

	// Two more CP processes over the same database.
	build := func() (*cmd.Server, context.CancelFunc) {
		s, err := cmd.Build(ctx, *c.cfg)
		require.NoError(t, err)
		require.NotNil(t, s.Leader, "PostgreSQL means a lease")
		sctx, cancel := context.WithCancel(ctx)
		go func() { <-sctx.Done(); s.Close() }()

		return s, cancel
	}
	a, stopA := build()
	b, stopB := build()
	defer stopA()
	defer stopB()

	leaders := func() int {
		n := 0
		for _, s := range []*cmd.Server{c.running.CP, a, b} {
			if s.Leader.Is(ctx) {
				n++
			}
		}

		return n
	}
	require.Eventually(t, func() bool { return leaders() == 1 }, 10*time.Second, 200*time.Millisecond, "exactly one leads")

	// The leader goes: its connection, and the lock with it.
	var leader *cmd.Server
	for _, s := range []*cmd.Server{c.running.CP, a, b} {
		if s.Leader.Is(ctx) {
			leader = s
		}
	}
	require.NotNil(t, leader)
	switch leader {
	case a:
		stopA()
	case b:
		stopB()
	default:
		// The harness's own process leads: take its lease away instead.
		leader.Leader.Close()
	}
	require.Eventually(t, func() bool {
		n := 0
		for _, s := range []*cmd.Server{c.running.CP, a, b} {
			if s == leader {
				continue
			}
			if s.Leader.Is(ctx) {
				n++
			}
		}

		return n == 1
	}, 15*time.Second, 200*time.Millisecond, "another process leads")
}
