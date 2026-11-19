package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lesomnus/payday/auth"

	"github.com/lesomnus/shale/api"
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

// anyLamina reads a source's laminae page by page until `match` holds of
// one, and says whether it did. A source's look-ahead of short segments
// can hold hundreds of laminae ahead (§12.1), more than one page.
func anyLamina(ctx context.Context, conn *grpc.ClientConn, source []byte, match func(*api.Lamina) bool) bool {
	after := ""
	for {
		rows, err := api.NewLaminaServiceClient(conn).List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: source}.Build()}.Build()},
			Size:    500,
			After:   after,
		}.Build())
		if err != nil {
			return false
		}
		for _, v := range rows.GetItems() {
			if match(v) {
				return true
			}
		}
		if after = rows.GetNext(); after == "" {
			return false
		}
	}
}
