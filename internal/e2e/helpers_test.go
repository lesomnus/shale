package e2e_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lesomnus/payday/auth"
)

// dialCluster is the cluster API as somebody.
func (c *cluster) dialCluster(as string) *grpc.ClientConn {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	opts = append(opts, auth.Inject(auth.PlainProvider(as))...)
	conn, err := grpc.NewClient(c.running.ClusterAddr, opts...)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { conn.Close() })

	return conn
}

func fetch(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return b
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
