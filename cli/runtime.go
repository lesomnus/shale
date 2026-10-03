package cli

import (
	"context"
	"math"
	"runtime/metrics"
	"sync"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/metric"
)

// The Go runtime's own numbers (§31), for every role: how often the
// collector runs and what it costs, how much it keeps, and whether
// goroutines wait to be run. A producer on a Raspberry Pi once stalled its
// capture readers for seconds at a time in GC assist, all three cameras at
// once, and nothing said so until a goroutine dump; these say so.
//
// They are read with runtime/metrics when the meter collects, once an
// interval, and cost nothing when no exporter is configured: the callback
// is never called.

// runtimeSamples are what is read, in this order.
var runtimeSamples = []string{
	"/gc/cycles/total:gc-cycles",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/gc/mark/assist:cpu-seconds",
	"/gc/heap/live:bytes",
	"/gc/heap/goal:bytes",
	"/memory/classes/total:bytes",
	"/sched/goroutines:goroutines",
	"/sched/latencies:seconds",
}

// registerRuntimeMetrics puts the runtime's numbers on the context's meter.
func registerRuntimeMetrics(ctx context.Context) error {
	m := otx.Meter(ctx)
	cycles, err := m.Int64ObservableCounter("shale.runtime.gc.cycles", metric.WithDescription("garbage collections completed"))
	if err != nil {
		return err
	}
	gcCPU, err := m.Float64ObservableCounter("shale.runtime.gc.cpu", metric.WithDescription("CPU time the collector took, assists included"), metric.WithUnit("s"))
	if err != nil {
		return err
	}
	assist, err := m.Float64ObservableCounter("shale.runtime.gc.assist", metric.WithDescription("CPU time goroutines spent helping the collector instead of their own work"), metric.WithUnit("s"))
	if err != nil {
		return err
	}
	live, err := m.Int64ObservableGauge("shale.runtime.heap.live", metric.WithDescription("heap the last collection found in use"), metric.WithUnit("By"))
	if err != nil {
		return err
	}
	goal, err := m.Int64ObservableGauge("shale.runtime.heap.goal", metric.WithDescription("heap size at which the next collection starts"), metric.WithUnit("By"))
	if err != nil {
		return err
	}
	mem, err := m.Int64ObservableGauge("shale.runtime.memory", metric.WithDescription("memory the Go runtime has mapped, all of it"), metric.WithUnit("By"))
	if err != nil {
		return err
	}
	goroutines, err := m.Int64ObservableGauge("shale.runtime.goroutines", metric.WithDescription("live goroutines"))
	if err != nil {
		return err
	}
	latency, err := m.Float64ObservableGauge("shale.runtime.sched.latency_max", metric.WithDescription("the longest a goroutine waited to run since the last collection, as the upper bound of its histogram bucket"), metric.WithUnit("s"))
	if err != nil {
		return err
	}

	samples := make([]metrics.Sample, len(runtimeSamples))
	for i, name := range runtimeSamples {
		samples[i].Name = name
	}
	var mu sync.Mutex
	var lastLatency []uint64
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()
		metrics.Read(samples)
		if v, ok := uint64Of(samples[0]); ok {
			o.ObserveInt64(cycles, int64(v))
		}
		if v, ok := float64Of(samples[1]); ok {
			o.ObserveFloat64(gcCPU, v)
		}
		if v, ok := float64Of(samples[2]); ok {
			o.ObserveFloat64(assist, v)
		}
		for i, g := range []metric.Int64Observable{live, goal, mem, goroutines} {
			if v, ok := uint64Of(samples[3+i]); ok {
				o.ObserveInt64(g, int64(v))
			}
		}
		if samples[7].Value.Kind() == metrics.KindFloat64Histogram {
			h := samples[7].Value.Float64Histogram()
			o.ObserveFloat64(latency, maxNewBucket(h, lastLatency))
			lastLatency = append(lastLatency[:0], h.Counts...)
		}

		return nil
	}, cycles, gcCPU, assist, live, goal, mem, goroutines, latency)

	return err
}

func uint64Of(s metrics.Sample) (uint64, bool) {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0, false
	}

	return s.Value.Uint64(), true
}

func float64Of(s metrics.Sample) (float64, bool) {
	if s.Value.Kind() != metrics.KindFloat64 {
		return 0, false
	}

	return s.Value.Float64(), true
}

// maxNewBucket is the upper bound of the highest bucket that gained counts
// since `last` (the lower bound when the upper is infinite), or 0.
func maxNewBucket(h *metrics.Float64Histogram, last []uint64) float64 {
	for i := len(h.Counts) - 1; i >= 0; i-- {
		prev := uint64(0)
		if i < len(last) {
			prev = last[i]
		}
		if h.Counts[i] > prev {
			if up := h.Buckets[i+1]; !math.IsInf(up, 1) {
				return up
			}

			return h.Buckets[i]
		}
	}

	return 0
}
