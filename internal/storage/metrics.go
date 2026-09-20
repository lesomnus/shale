package storage

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The node's instruments (§31), from the telemetry the context carries; a
// context without any answers no-op instruments.
type metrics struct {
	uploads   metric.Int64UpDownCounter
	reads     metric.Int64UpDownCounter
	refused   metric.Int64Counter
	committed metric.Int64Counter
	bytes     metric.Int64Counter
	reclaimed metric.Int64Counter
	free      metric.Int64Gauge
	pressure  metric.Int64Gauge
	indexed   metric.Int64Gauge
}

func newMetrics(ctx context.Context) *metrics {
	o := otx.From(ctx)

	return &metrics{
		uploads:   o.Int64UpDownCounter("shale.node.uploads_in_flight", metric.WithDescription("uploads open per sink")),
		reads:     o.Int64UpDownCounter("shale.node.read_sessions", metric.WithDescription("read sessions open")),
		refused:   o.Int64Counter("shale.node.refused", metric.WithDescription("requests refused, by reason")),
		committed: o.Int64Counter("shale.node.uploads_committed", metric.WithDescription("uploads committed, complete or incomplete")),
		bytes:     o.Int64Counter("shale.node.bytes_committed", metric.WithDescription("bytes of committed uploads"), metric.WithUnit("By")),
		reclaimed: o.Int64Counter("shale.node.gc_reclaimed_bytes", metric.WithDescription("bytes GC reclaimed per sink"), metric.WithUnit("By")),
		free:      o.Int64Gauge("shale.node.sink_free_bytes", metric.WithDescription("free bytes per sink"), metric.WithUnit("By")),
		pressure:  o.Int64Gauge("shale.node.sink_pressure", metric.WithDescription("pressure state per sink: 1 normal, 2 low, 3 critical")),
		indexed:   o.Int64Gauge("shale.node.index_objects", metric.WithDescription("objects in the index per sink")),
	}
}

func sinkAttr(s *Sink) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("sink", s.Id.String()))
}

func reasonAttr(reason string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("reason", reason))
}
