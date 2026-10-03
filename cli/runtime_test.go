package cli

import (
	"context"
	"runtime"
	"testing"

	"github.com/lesomnus/otx"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The runtime's numbers are there when the meter collects, and say what a
// process that has collected garbage and holds a heap would.
func TestRuntimeMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	require.NoError(t, registerRuntimeMetrics(ctx))
	runtime.GC()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				got[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Sum[float64]:
				got[m.Name] = d.DataPoints[0].Value
			case metricdata.Gauge[int64]:
				got[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Gauge[float64]:
				got[m.Name] = d.DataPoints[0].Value
			}
		}
	}
	for _, name := range []string{"shale.runtime.gc.cycles", "shale.runtime.gc.cpu", "shale.runtime.gc.assist", "shale.runtime.heap.live", "shale.runtime.heap.goal", "shale.runtime.memory", "shale.runtime.goroutines", "shale.runtime.sched.latency_max"} {
		require.Contains(t, got, name)
	}
	require.Positive(t, got["shale.runtime.gc.cycles"], "runtime.GC ran one")
	require.Positive(t, got["shale.runtime.heap.live"])
	require.GreaterOrEqual(t, got["shale.runtime.heap.goal"], got["shale.runtime.heap.live"])
	require.GreaterOrEqual(t, got["shale.runtime.memory"], got["shale.runtime.heap.live"])
	require.Positive(t, got["shale.runtime.goroutines"])
}
