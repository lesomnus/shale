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
	// What comes in and goes out, how long a viewer waits for the first
	// frame, and the host.
	ingress    metric.Int64Counter
	egress     metric.Int64Counter
	recent     metric.Int64Counter
	rewind     metric.Int64Gauge
	firstFrame metric.Float64Histogram
	// How long a joining viewer took to catch up with the live edge, and
	// the packets viewers asked for again (§39.4).
	catchUp metric.Float64Histogram
	nacked  metric.Int64Counter
	cpu     metric.Float64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		attached:   o.Int64Gauge("shale.relay.attached_producers", metric.WithDescription("producers with a stream open")),
		active:     o.Int64Gauge("shale.relay.active_sources", metric.WithDescription("sources being sent")),
		viewers:    o.Int64Gauge("shale.relay.viewers", metric.WithDescription("WHEP sessions open")),
		sessions:   o.Int64Counter("shale.relay.sessions", metric.WithDescription("sessions by outcome: started, renewed with a fresh token, or refused with the reason")),
		ingress:    o.Int64Counter("shale.relay.ingress_bytes", metric.WithDescription("TS bytes taken from producers"), metric.WithUnit("By")),
		egress:     o.Int64Counter("shale.relay.egress_bytes", metric.WithDescription("media bytes handed to viewers' tracks"), metric.WithUnit("By")),
		recent:     o.Int64Counter("shale.relay.recent_bytes", metric.WithDescription("bytes of the recent window handed out"), metric.WithUnit("By")),
		rewind:     o.Int64Gauge("shale.relay.rewind_bytes", metric.WithDescription("bytes the recent windows hold together"), metric.WithUnit("By")),
		firstFrame: o.Float64Histogram("shale.relay.first_frame_ms", metric.WithDescription("from a session's offer to the connection over which its first frame goes"), metric.WithUnit("ms")),
		catchUp:    o.Float64Histogram("shale.relay.catch_up_ms", metric.WithDescription("from a session's keyframe to the end of its group of pictures, sent faster than real time, at the live edge"), metric.WithUnit("ms")),
		nacked:     o.Int64Counter("shale.relay.nacked_packets", metric.WithDescription("video packets viewers reported lost and asked for again")),
		cpu:        o.Float64Gauge("shale.relay.cpu", metric.WithDescription("one-minute load average over the CPU count")),
	}
}

func outcomeAttr(outcome string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", outcome))
}
