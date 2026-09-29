package cli

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// TestApplyDevBrowser is what development mode owes a browser (§40.4): the
// console is served from the tenant listener, so both listeners answer what
// a page can speak, and each names the origins a page calls it from -- the
// console's own, by either spelling of loopback, and vite's. Without them
// the page loads and every call it makes is answered with the page.
func TestApplyDevBrowser(t *testing.T) {
	c := &cmd.Config{}
	ApplyDev(c, t.TempDir())

	require.True(t, c.Server.Http.AllowWeb)
	require.True(t, c.Cluster.Http.AllowWeb)
	for _, o := range []string{"http://127.0.0.1:7402", "http://localhost:7402", "http://127.0.0.1:5173", "http://localhost:5173"} {
		require.Contains(t, c.Cluster.Http.Origins, o, "the cluster listener is another origin than the console")
		require.Contains(t, c.Server.Http.Origins, o)
	}
	require.True(t, c.Cluster.Http.Origin()("http://localhost:7402"))
	require.False(t, c.Cluster.Http.Origin()("http://evil.example"))
}

// TestApplyDevKeeps is what was written down winning over the defaults, so
// a development deployment can be moved off loopback or given a sink of its
// own without its settings being overwritten here.
func TestApplyDevKeeps(t *testing.T) {
	dir := t.TempDir()
	c := &cmd.Config{}
	c.Server.Http.Addr = "10.0.0.5:9402"
	c.Cluster.Http.Origins = []string{"https://console.example"}
	c.Storage.Sinks = []cmd.SinkConfig{{Path: "/srv/shale", Capacity: "200GiB"}}
	ApplyDev(c, dir)

	require.Equal(t, []string{"https://console.example"}, c.Cluster.Http.Origins)
	require.Equal(t, []cmd.SinkConfig{{Path: "/srv/shale", Capacity: "200GiB"}}, c.Storage.Sinks)
	require.Contains(t, c.Server.Http.Origins, "http://10.0.0.5:9402", "the address it was told to serve on")
	require.Contains(t, c.Server.Http.Origins, "http://10.0.0.5:5173")

	c = &cmd.Config{}
	ApplyDev(c, dir)
	require.Equal(t, []cmd.SinkConfig{{Path: filepath.Join(dir, "sink")}}, c.Storage.Sinks)
}
