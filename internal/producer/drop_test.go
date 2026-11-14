package producer

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/server/core"
)

// failures records the laminae a producer reports LOST.
type failures struct {
	api.LaminaServiceClient
	mu   sync.Mutex
	refs [][]byte
	why  []string
}

func (f *failures) ReportFailure(_ context.Context, req *api.LaminaReportFailureRequest, _ ...grpc.CallOption) (*api.Lamina, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refs = append(f.refs, req.GetRef().GetId())
	f.why = append(f.why, req.GetReason())

	return api.Lamina_builder{}.Build(), nil
}

func dropRuntime(t *testing.T) (*Producer, *source, *failures) {
	t.Helper()
	f := &failures{}
	log := slog.New(slog.DiscardHandler)
	p := &Producer{log: log, sources: map[string]*source{}, uploader: &Uploader{Laminae: f, Log: log}}
	p.m = newMetrics(context.Background())
	s := &source{cfg: SourceConfig{Alias: "door"}, taken: map[string]handed{}, sched: Schedule{Duration: time.Minute}}
	s.pending = []*Segment{NewSegment(time.Now(), []byte("fragments"))}

	return p, s, f
}

// A segment whose upload failed has let its allocation go for fresh
// attempts, and the RAM budget drops it before it is tried again: the lamina
// it was handed is reported LOST all the same (#157, §13, §16).
func TestDropReportsTheHeadsLaminaLost(t *testing.T) {
	p, s, f := dropRuntime(t)
	lamina := pdid.New(core.DomLamina).Bytes()
	s.hand(api.Allocation_builder{LaminaId: lamina}.Build(), 0)
	s.headAlloc = nil // as `!res.Stored` leaves it
	p.drop(context.Background(), s, s.pending[0], "over the RAM budget")
	require.Equal(t, [][]byte{lamina}, f.refs)
	require.Equal(t, []string{"over the RAM budget"}, f.why)
	require.Empty(t, s.pending)
	require.Equal(t, int64(1), s.lost)
	require.Nil(t, s.headObj, "the next segment starts with nothing handed")
	require.Equal(t, lamina, s.lastObj, "and names this one as `after`")
}

// With its allocation at hand, that allocation's lamina is the one reported.
func TestDropReportsTheAllocationsLamina(t *testing.T) {
	p, s, f := dropRuntime(t)
	lamina := pdid.New(core.DomLamina).Bytes()
	al := api.Allocation_builder{LaminaId: lamina, LaminaKey: "laminae/x"}.Build()
	s.hand(al, 0)
	s.headAlloc = al
	p.drop(context.Background(), s, s.pending[0], "no allocation: refused")
	require.Equal(t, [][]byte{lamina}, f.refs)
}

// A segment never handed a lamina has none to report.
func TestDropWithoutALaminaReportsNothing(t *testing.T) {
	p, s, f := dropRuntime(t)
	p.drop(context.Background(), s, s.pending[0], "no allocation: refused")
	require.Empty(t, f.refs)
	require.Equal(t, int64(1), s.lost)
}
