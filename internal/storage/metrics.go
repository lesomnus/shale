package storage

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The node's instruments (§31), from the telemetry the context carries; a
// context without any answers no-op instruments. Every line §31 gives the
// node is one of these; the heartbeat records the gauges, the paths record
// the counters and histograms where the thing happens.
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

	// Ingest (§31): resumes, abandons, the buffer pool, the live lag, the
	// rate.
	resumes   metric.Int64Counter
	abandoned metric.Int64Counter
	poolUsed  metric.Int64Gauge
	uploadAge metric.Int64Gauge
	ingestBps metric.Int64Gauge
	// Device (§24): queue depth and wait per class.
	queueDepth metric.Int64Gauge
	queueWait  metric.Float64Histogram
	// Read (§17): latency and sessions the client gave up on.
	readLatency  metric.Float64Histogram
	readsAborted metric.Int64Counter
	// Sink (§21): GC proposed and approved, the sweep among them.
	gcProposed metric.Int64Counter
	gcApproved metric.Int64Counter
	// Node: NICs, and the startup scan per sink.
	nicRx    metric.Int64Gauge
	nicTx    metric.Int64Gauge
	scanDone metric.Int64Gauge
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
		indexed:   o.Int64Gauge("shale.node.index_laminae", metric.WithDescription("laminae in the index per sink")),

		resumes:      o.Int64Counter("shale.node.upload_resumes", metric.WithDescription("uploads resumed from an offset, per sink")),
		abandoned:    o.Int64Counter("shale.node.uploads_abandoned", metric.WithDescription("uploads the abandon rule ended, by outcome and why")),
		poolUsed:     o.Int64Gauge("shale.node.part_buffer_used_bytes", metric.WithDescription("part buffer pool in use"), metric.WithUnit("By")),
		uploadAge:    o.Int64Gauge("shale.node.oldest_upload_age_ms", metric.WithDescription("age of the oldest open upload's data time per sink: the live lag"), metric.WithUnit("ms")),
		ingestBps:    o.Int64Gauge("shale.node.ingest_bps", metric.WithDescription("bytes committed per second since the last heartbeat, times eight"), metric.WithUnit("bit/s")),
		queueDepth:   o.Int64Gauge("shale.node.device_queue_depth", metric.WithDescription("jobs waiting per device and class")),
		queueWait:    o.Float64Histogram("shale.node.device_wait_ms", metric.WithDescription("time a job waited for the device, per class"), metric.WithUnit("ms")),
		readLatency:  o.Float64Histogram("shale.node.read_latency_ms", metric.WithDescription("time to serve a read, per sink"), metric.WithUnit("ms")),
		readsAborted: o.Int64Counter("shale.node.reads_aborted", metric.WithDescription("reads the client gave up on, per sink")),
		gcProposed:   o.Int64Counter("shale.node.gc_proposed", metric.WithDescription("files proposed to GC, per sink and reason")),
		gcApproved:   o.Int64Counter("shale.node.gc_approved", metric.WithDescription("files GC approved and the node unlinked, per sink and reason")),
		nicRx:        o.Int64Gauge("shale.node.nic_rx_bps", metric.WithDescription("received on the interface since the last heartbeat"), metric.WithUnit("bit/s")),
		nicTx:        o.Int64Gauge("shale.node.nic_tx_bps", metric.WithDescription("sent on the interface since the last heartbeat"), metric.WithUnit("bit/s")),
		scanDone:     o.Int64Gauge("shale.node.scan_done", metric.WithDescription("1 once the startup scan of the sink finished")),
	}
}

func sinkAttr(s *Sink) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("sink", s.Id.String()))
}

func reasonAttr(reason string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("reason", reason))
}

func abandonAttr(outcome, why string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("outcome", outcome), attribute.String("why", why))
}

func gcAttr(s *Sink, reason string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("sink", s.Id.String()), attribute.String("reason", reason))
}

func classAttr(device string, class Class) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("device", device), attribute.String("class", class.String()))
}
