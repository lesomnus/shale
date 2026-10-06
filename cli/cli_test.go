package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// isolate leaves a test only the SHALE_ variables it sets itself: one set
// in the shell that runs the tests is read like any other.
func isolate(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "SHALE_") {
			t.Setenv(k, "") // and back as it was after the test
			os.Unsetenv(k)
		}
	}
}

// run runs a command line through the tree a process runs, with file as
// the configuration file, and answers what it printed.
func run(t *testing.T, c *cmd.Config, file string, args ...string) (string, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shale.yaml")
	require.NoError(t, os.WriteFile(path, []byte(file), 0o600))

	var out bytes.Buffer
	x := Cmd(c)
	x.Writer = &out
	x.ErrWriter = io.Discard
	err := x.Run(context.Background(), append([]string{"--config", path}, args...))

	return out.String(), err
}

// TestFlagGivenEmpty is a flag given with nothing (§34.1): it clears what
// the file says, as an empty variable does, where it used to count as a
// flag not given. The root's flags and those of `serve` alike; a flag not
// given leaves the file's value.
func TestFlagGivenEmpty(t *testing.T) {
	isolate(t)
	var served *cmd.Config
	prev := serveRelay
	serveRelay = func(_ context.Context, c *cmd.Config) error { served = c; return nil }
	t.Cleanup(func() { serveRelay = prev })

	var c cmd.Config
	_, err := run(t, &c, `
cp: https://cp.example:7401
state: /srv/shale
client:
  addr: https://cp.example:7400
  cluster_addr: https://cp.example:7401
`, "--addr=", "serve", "relay", "--cp=", "--state=")
	require.NoError(t, err)
	require.NotNil(t, served)
	require.Empty(t, served.Cp)
	require.Empty(t, served.State)
	require.Empty(t, served.Client.Addr)
	require.Equal(t, "https://cp.example:7401", served.Client.ClusterAddr)
}

// TestFlagOverEnvironment is a flag read with the configuration (§34.1):
// over the environment as over the file, `config` says the value came from
// the flag, and help names the variable that says the same.
func TestFlagOverEnvironment(t *testing.T) {
	isolate(t)
	t.Setenv("SHALE_CLIENT_ADDR", "https://env.example:7400")
	t.Setenv("SHALE_CP", "https://env.example:7401")

	var c cmd.Config
	out, err := run(t, &c, "client:\n  addr: https://file.example:7400\n", "--addr", "https://flag.example:7400", "config")
	require.NoError(t, err)
	require.Equal(t, "https://flag.example:7400", c.Client.Addr)
	require.Contains(t, out, "  addr: https://flag.example:7400  # --addr\n")
	require.Contains(t, out, "cp: https://env.example:7401  # SHALE_CP\n", "no flag, so the environment's")

	for _, v := range []struct {
		args []string
		vars []string
	}{
		{[]string{"--help"}, []string{"SHALE_CLIENT_AS", "SHALE_CLIENT_ADDR", "SHALE_CLIENT_CLUSTER_ADDR"}},
		{[]string{"serve", "relay", "--help"}, []string{"SHALE_CP", "SHALE_STATE"}},
		{[]string{"serve", "producer", "--help"}, []string{"SHALE_CP", "SHALE_STATE", "SHALE_PRODUCER_DEMO"}},
		{[]string{"producer", "push", "--help"}, []string{"SHALE_PRODUCER_PUSH"}},
	} {
		out, err := run(t, &cmd.Config{}, "", v.args...)
		require.NoError(t, err, v.args)
		for _, name := range v.vars {
			require.Contains(t, out, "[$"+name+"]", v.args)
		}
	}
}
