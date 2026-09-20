package e2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/config"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
)

// TestActorLimit is §35.1's per-actor limit: past the burst, a caller is
// answered RESOURCE_EXHAUSTED while the tenant's other callers go on.
func TestActorLimit(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Control.ActorLimit = config.LimitConfig{Rate: 1, Burst: 3} })
	ctx := context.Background()
	admin := api.NewSetServiceClient(c.dial("@acme/admin"))

	exhausted := 0
	for range 10 {
		if _, err := admin.List(ctx, api.SetListRequest_builder{}.Build()); status.Code(err) == codes.ResourceExhausted {
			exhausted++
		}
	}
	require.Greater(t, exhausted, 0, "the admin ran past its burst")

	// Another actor of the same tenant is not the one being limited.
	_, err := api.NewSetServiceClient(c.dial("@cluster/ops")).List(ctx, api.SetListRequest_builder{}.Build())
	require.NotEqual(t, codes.ResourceExhausted, status.Code(err))
}
