package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// Laminae a slot holds with one date_started are told apart by their id: the
// slot's last is the one made last, whichever order the database keeps a tie
// in (#158). Run on PostgreSQL (SHALE_E2E_DB_DSN), the order without the id
// follows where the rows were written.
func TestSlotTiesAreOrderedById(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	src := exportSource(t, admin)
	laminae := api.NewLaminaServiceClient(admin)
	at := timestamppb.New(time.Now().Add(-time.Hour).Truncate(time.Second))
	allocate := func(after []byte) []byte {
		req := api.LaminaAllocateRequest_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: at}.Build()
		if after != nil {
			req.SetAfter(api.LaminaRef_builder{Id: after}.Build())
		}
		a, err := laminae.Allocate(ctx, req)
		require.NoError(t, err)
		return a.GetLaminaId()
	}
	last := allocate(nil)
	for range 4 {
		next := allocate(last)
		require.NotEqual(t, last, next, "`after` names the segment before, so the next gets a lamina of its own")
		// The earlier one changes after the later one was made. PostgreSQL
		// writes the changed row anew, so a tie no longer comes back in the
		// order the rows were made.
		_, err := laminae.ReportFailure(ctx, api.LaminaReportFailureRequest_builder{Ref: api.LaminaRef_builder{Id: last}.Build(), Reason: "test"}.Build())
		require.NoError(t, err)
		last = next
		for range 3 {
			require.Equal(t, last, allocate(nil), "the slot's last lamina is the one made last")
		}
	}
}
