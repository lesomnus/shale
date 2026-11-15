package cli

import (
	"path/filepath"
	"testing"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// The satellite of `serve all` (§41.10) joins as the relay of the process
// does: the cluster listener on loopback, the state directory beside the
// others. In development mode it takes a port of loopback the system picks,
// so many `serve all --dev` on one machine do not meet, while `serve
// satellite --dev` keeps its own; elsewhere it listens where a satellite
// does, and `satellite.*` is read as `serve satellite` reads it.
func TestInProcessSatelliteConfig(t *testing.T) {
	t.Run("development mode", func(t *testing.T) {
		c := cmd.Config{}
		ApplyDev(&c, t.TempDir())
		cfg, ok, err := inProcessSatelliteConfig(&c, "127.0.0.1:41234")
		require.NoError(t, err)
		require.True(t, ok, "on by default")
		require.True(t, cfg.Dev)
		require.Equal(t, "http://127.0.0.1:41234", cfg.Cp, "the cluster listener actually bound, not the CLI's")
		require.Equal(t, "127.0.0.1:0", cfg.Addr)
		require.Equal(t, filepath.Join(c.Dev, "satellite"), cfg.StateDir)

		alone, err := SatelliteConfig(&c)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:7460", alone.Addr, "serve satellite --dev is unchanged")
		require.Equal(t, c.Client.ClusterAddr, alone.Cp)
	})
	t.Run("every interface", func(t *testing.T) {
		c := cmd.Config{State: "/var/lib/shale"}
		c.Satellite.Advertise = "192.0.2.10"
		c.Satellite.MemoryBudget = "64MiB"
		cfg, ok, err := inProcessSatelliteConfig(&c, "[::]:7401")
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, cfg.Dev)
		require.Equal(t, "https://127.0.0.1:7401", cfg.Cp, "loopback, wherever the listener is")
		require.Empty(t, cfg.Addr, "the satellite's own default, :7460")
		require.Equal(t, "192.0.2.10", cfg.Advertise)
		require.Equal(t, int64(64<<20), cfg.MemoryBudget)
		require.Equal(t, "/var/lib/shale/satellite", cfg.StateDir)
	})
	t.Run("what the file says", func(t *testing.T) {
		c := cmd.Config{Cp: "https://cp.example.com:7401"}
		ApplyDev(&c, t.TempDir())
		c.Satellite.Addr = "127.0.0.1:7470"
		cfg, ok, err := inProcessSatelliteConfig(&c, "127.0.0.1:41234")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "https://cp.example.com:7401", cfg.Cp, "cp, as the node and the relay take it")
		require.Equal(t, "127.0.0.1:7470", cfg.Addr)
	})
	t.Run("off", func(t *testing.T) {
		c := cmd.Config{}
		ApplyDev(&c, t.TempDir())
		c.Satellite.InProcess = z.Ptr(false)
		_, ok, err := inProcessSatelliteConfig(&c, "127.0.0.1:41234")
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("a bad setting stops serve all", func(t *testing.T) {
		c := cmd.Config{}
		ApplyDev(&c, t.TempDir())
		c.Satellite.MemoryBudget = "lots"
		_, _, err := inProcessSatelliteConfig(&c, "127.0.0.1:41234")
		require.Error(t, err)
	})
}
