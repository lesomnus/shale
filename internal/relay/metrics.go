package relay

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The relay's instruments (§31, §39).
type metrics struct {
	attached metric.Int64Gauge
	active   metric.Int64Gauge
	viewers  metric.Int64Gauge
	sessions metric.Int64Counter
	// What goes out, how long a viewer waits for the first frame, and the
	// host.
	egress     metric.Int64Counter
	firstFrame metric.Float64Histogram
	cpu        metric.Float64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		attached:   o.Int64Gauge("shale.relay.attached_producers", metric.WithDescription("producers with a stream open")),
		active:     o.Int64Gauge("shale.relay.active_sources", metric.WithDescription("sources being sent")),
		viewers:    o.Int64Gauge("shale.relay.viewers", metric.WithDescription("WHEP sessions open")),
		sessions:   o.Int64Counter("shale.relay.sessions", metric.WithDescription("sessions by outcome: started, or refused with the reason")),
		egress:     o.Int64Counter("shale.relay.egress_bytes", metric.WithDescription("media bytes handed to viewers' tracks"), metric.WithUnit("By")),
		firstFrame: o.Float64Histogram("shale.relay.first_frame_ms", metric.WithDescription("from a session's offer to the connection over which its first frame goes"), metric.WithUnit("ms")),
		cpu:        o.Float64Gauge("shale.relay.cpu", metric.WithDescription("one-minute load average over the CPU count")),
	}
}

func outcomeAttr(outcome string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", outcome))
}
