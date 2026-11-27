package cmd_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/watch"

	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
)

// TestClosingAServerClosesItsBroker.
//
// A broker is what a server holds besides its pool, its lease and roster,
// and `Close` did not close it. On `watch.broker: postgres` that is a
// listener and a publisher with a connection each, outside the pool, which
// nothing ended until the process did (lesomnus/payday#323): CI's PostgreSQL
// job, which builds a control plane a test, ran out of the server's hundred
// connections a third of the way through.
//
// Asserted on `memory`, which runs wherever this does and is closed by the
// same call: a subscription open when the server closes has ended. That the
// `postgres` broker then lets go of its connections is payday's own test.
// The roster this process embeds is built with no broker, so it has none
// to give back.
func TestClosingAServerClosesItsBroker(t *testing.T) {
	x := require.New(t)
	ctx := context.Background()

	c := &cmd.Config{}
	cli.ApplyDev(c, t.TempDir())
	c.Watch = config.WatchConfig{Broker: config.BrokerMemory}

	s, err := cmd.Build(ctx, *c)
	x.NoError(err)

	broadcast, stop := s.Watch.Subscribe()
	defer stop()
	scoped, err := s.Watch.Open(ctx, watch.Interest{})
	x.NoError(err)
	defer scoped.Close()

	x.NoError(s.Close())

	ended := func(c <-chan watch.Event) bool {
		select {
		case _, ok := <-c:
			return !ok
		case <-time.After(5 * time.Second):
			return false
		}
	}
	x.True(ended(broadcast), "a subscription outlived the server")
	x.True(ended(scoped.Events), "a scoped subscription outlived the server")
}
