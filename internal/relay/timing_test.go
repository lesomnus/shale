package relay

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/lesomnus/payday/pdid"
)

// The producer's queue and the uplink are measured from the stamps a
// payload brings (§39.3): only on one the source took, an uplink below zero
// is a clock skew and not a time, and a producer that sends no stamps
// measures nothing.
func TestIngestTiming(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	r := &Relay{m: newMetrics(ctx), log: slog.New(slog.DiscardHandler)}
	s := newSource(r, pdid.New(pdid.Domain(8)))

	t1 := time.Now()
	s.feedTimed(nil, "", []byte("x"), stamps{read: t1, sent: t1.Add(40 * time.Millisecond), arrived: t1.Add(140 * time.Millisecond)})
	// The producer's clock 50 ms ahead of the relay's, over a 10 ms uplink.
	s.feedTimed(nil, "", []byte("x"), stamps{read: t1, sent: t1.Add(60 * time.Millisecond), arrived: t1.Add(20 * time.Millisecond)})
	// A producer that predates the stamps.
	s.feedEpoch(nil, "", []byte("x"))
	// A payload of another epoch the source does not take.
	s.feedTimed(nil, "other", []byte("x"), stamps{read: t1, sent: t1, arrived: t1})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	hist := map[string]metricdata.HistogramDataPoint[float64]{}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				hist[m.Name] = d.DataPoints[0]
			case metricdata.Sum[int64]:
				sums[m.Name] = d.DataPoints[0].Value
			}
		}
	}
	queue := hist["shale.relay.producer_queue_ms"]
	require.Equal(t, uint64(2), queue.Count)
	require.InDelta(t, 100, queue.Sum, 1e-9, "40 and 60 ms")
	uplink := hist["shale.relay.uplink_ms"]
	require.Equal(t, uint64(1), uplink.Count, "the skewed one is not a time")
	require.InDelta(t, 100, uplink.Sum, 1e-9)
	require.Equal(t, int64(1), sums["shale.relay.uplink_skewed"])
}
