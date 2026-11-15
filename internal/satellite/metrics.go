package satellite

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/metric"
)

// The satellite's instruments (§31, §41.4): read off its counters at every
// heartbeat, so they say what the CP is told.
type metrics struct {
	inFlight  metric.Int64Gauge
	budget    metric.Int64Gauge
	uploads   metric.Int64Gauge
	producers metric.Int64Gauge
	forwarded metric.Int64Gauge
	refused   metric.Int64Gauge
	cpu       metric.Float64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		inFlight:  o.Int64Gauge("shale.satellite.in_flight_bytes", metric.WithDescription("what the requests passing through hold of the in-flight pool"), metric.WithUnit("By")),
		budget:    o.Int64Gauge("shale.satellite.in_flight_budget", metric.WithDescription("the in-flight pool's bound, from satellite.memory_budget"), metric.WithUnit("By")),
		uploads:   o.Int64Gauge("shale.satellite.uploads", metric.WithDescription("requests passing through to nodes now")),
		producers: o.Int64Gauge("shale.satellite.producers", metric.WithDescription("producers assigned to this satellite, as it was last told")),
		forwarded: o.Int64Gauge("shale.satellite.forwarded_bytes", metric.WithDescription("bytes forwarded to nodes since the satellite started"), metric.WithUnit("By")),
		refused:   o.Int64Gauge("shale.satellite.refused", metric.WithDescription("requests refused since the satellite started: for want of room, or from a caller it does not serve")),
		cpu:       o.Float64Gauge("shale.satellite.cpu", metric.WithDescription("one-minute load average over the CPU count")),
	}
}
