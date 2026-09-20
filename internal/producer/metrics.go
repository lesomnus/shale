package producer

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The producer's instruments (§31, §38.6): what the heartbeat reports, as
// numbers a dashboard reads.
type metrics struct {
	rate       metric.Int64Gauge
	input      metric.Int64Gauge
	fps        metric.Float64Gauge
	keyframe   metric.Int64Gauge
	restarts   metric.Int64Gauge
	segments   metric.Int64Counter
	dropped    metric.Int64Gauge
	transcodes metric.Int64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		rate:       o.Int64Gauge("shale.producer.rate_bps", metric.WithDescription("measured bitrate per source"), metric.WithUnit("bit/s")),
		input:      o.Int64Gauge("shale.producer.input_up", metric.WithDescription("1 while the source delivers frames")),
		fps:        o.Float64Gauge("shale.producer.frame_rate", metric.WithDescription("frames per second per source")),
		keyframe:   o.Int64Gauge("shale.producer.keyframe_interval_ms", metric.WithDescription("keyframe interval per source"), metric.WithUnit("ms")),
		restarts:   o.Int64Gauge("shale.producer.capture_restarts", metric.WithDescription("capture restarts per source")),
		segments:   o.Int64Counter("shale.producer.segments", metric.WithDescription("segments by outcome: stored, lost, or cut short at a node's offset (§12.2)")),
		dropped:    o.Int64Gauge("shale.producer.live_dropped", metric.WithDescription("live batches dropped because the relay lagged")),
		transcodes: o.Int64Gauge("shale.producer.live_transcodes", metric.WithDescription("live helpers running: watched sources whose audio is encoded as Opus")),
	}
}

func sourceAttr(alias string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("source", alias))
}
