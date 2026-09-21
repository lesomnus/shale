package cmd_test

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
)

// The trail's retention policy is refused where the process comes up
// (§26.5): a window with nowhere to put what leaves it is the configuration
// mistake worth being loudest about, and a kind this app does not have is
// a typo to find while somebody is watching.
func TestAuditPolicyRefusedAtBuild(t *testing.T) {
	ctx := context.Background()

	t.Run("no archive", func(t *testing.T) {
		c := &cmd.Config{}
		cli.ApplyDev(c, t.TempDir())
		c.Audit.Retain = time.Hour
		_, err := cmd.Build(ctx, *c)
		require.ErrorContains(t, err, "audit")
	})

	t.Run("no such kind", func(t *testing.T) {
		c := &cmd.Config{}
		cli.ApplyDev(c, t.TempDir())
		c.Audit.Discard = true
		c.Audit.Retain = time.Hour
		c.Audit.By = map[string]config.AuditKeepConfig{"segment": {Retain: time.Hour}}
		_, err := cmd.Build(ctx, *c)
		require.ErrorContains(t, err, "segment")
	})

	t.Run("discarded", func(t *testing.T) {
		c := &cmd.Config{}
		cli.ApplyDev(c, t.TempDir())
		c.Audit.Discard = true
		c.Audit.Retain = time.Hour
		s, err := cmd.Build(ctx, *c)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	})
}
