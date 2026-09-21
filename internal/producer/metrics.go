package producer

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The producer's instruments (§31, §38.6): what the heartbeat reports, as
// numbers a dashboard reads, and what the upload path finds out.
type metrics struct {
	rate       metric.Int64Gauge
	input      metric.Int64Gauge
	fps        metric.Float64Gauge
	keyframe   metric.Int64Gauge
	restarts   metric.Int64Gauge
	segments   metric.Int64Counter
	dropped    metric.Int64Gauge
	transcodes metric.Int64Gauge

	// The host, and the uplink the sources use together.
	cpu         metric.Float64Gauge
	temperature metric.Float64Gauge
	uplink      metric.Int64Gauge
	// Uploads: how long a segment took to be stored, the retries on the
	// way, and the cuts made early.
	uploadDuration metric.Float64Histogram
	retries        metric.Int64Counter
	earlyCuts      metric.Int64Counter
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

		cpu:            o.Float64Gauge("shale.producer.cpu", metric.WithDescription("one-minute load average over the CPU count")),
		temperature:    o.Float64Gauge("shale.producer.temperature", metric.WithDescription("SoC temperature where Linux offers one"), metric.WithUnit("Cel")),
		uplink:         o.Int64Gauge("shale.producer.uplink_bps", metric.WithDescription("the sources' measured rates together: what the uplink carries"), metric.WithUnit("bit/s")),
		uploadDuration: o.Float64Histogram("shale.producer.upload_duration_ms", metric.WithDescription("from a segment's first upload attempt to its 201, per source"), metric.WithUnit("ms")),
		retries:        o.Int64Counter("shale.producer.retries", metric.WithDescription("upload retries by kind: same_target after a 503, placement after a failed candidate (§13)")),
		earlyCuts:      o.Int64Counter("shale.producer.early_cuts", metric.WithDescription("segments cut early for running over their ceiling, per source (§38.2)")),
	}
}

func sourceAttr(alias string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("source", alias))
}

func kindAttr(kind string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("kind", kind))
}
