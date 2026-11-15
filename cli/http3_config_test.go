package cli

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/h3"
)

// HTTP/3 is the operator's to turn on, at both ends (§35.6): a node serves
// it, and offers it to browsers with Alt-Svc, only under `storage.http3:
// on`, and a producer speaks it only under `producer.http3`. A
// configuration that says nothing keeps both on TCP.
func TestHTTP3IsOptIn(t *testing.T) {
	var c cmd.Config
	nc, err := storageConfig(&c)
	require.NoError(t, err)
	require.False(t, nc.HTTP3, "a node says nothing of HTTP/3 by default")
	pc, err := ProducerConfig(&c)
	require.NoError(t, err)
	require.Equal(t, h3.Off, pc.Upload.HTTP3)

	for v, want := range map[string]bool{"on": true, "true": true, "off": false, "false": false} {
		c.Storage.Http3 = v
		nc, err := storageConfig(&c)
		require.NoError(t, err, v)
		require.Equal(t, want, nc.HTTP3, v)
	}
	c.Storage.Http3 = "auto"
	_, err = storageConfig(&c)
	require.Error(t, err, "a node has no auto")
}
