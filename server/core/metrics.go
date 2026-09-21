package core

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics are the control plane's instruments (§31): every line §31 gives
// the control plane is one of these, the counters and histograms recorded
// where the thing happens and the gauges by the leader's jobs once a tick.
type Metrics struct {
	Allocations     metric.Int64Counter
	Lost            metric.Int64Counter
	DirectiveErrors metric.Int64Counter
	Reconciled      metric.Int64Counter
	Deleting        metric.Int64Gauge
	PendingHosts    metric.Int64Gauge
	StoredBytes     metric.Int64Gauge

	// Ingest: the three latencies of §31, and what the write path found.
	AllocLatency  metric.Float64Histogram
	WriteLatency  metric.Float64Histogram
	CommitLatency metric.Float64Histogram
	Duplicates    metric.Int64Counter
	Recovered     metric.Int64Counter
	// Sink and capacity: what GC approved, and how long the cluster has.
	GcApprovedBytes metric.Int64Counter
	Runway          metric.Int64Gauge
	// Device: score, quarantines, and the I/O errors reports carried.
	DeviceScore metric.Float64Gauge
	Quarantines metric.Int64Counter
	IoErrors    metric.Int64Counter
	// Control plane: certificates, relays, directives, reconciliation, rows.
	CertsDue          metric.Int64Gauge
	ProducersPerRelay metric.Int64Gauge
	Reassignments     metric.Int64Counter
	DatesUnsynced     metric.Int64Gauge
	ReconcileLag      metric.Int64Gauge
	Pruned            metric.Int64Counter
}

// NewMetrics makes the instruments from the telemetry the context carries.
func NewMetrics(ctx context.Context) *Metrics {
	o := otx.From(ctx)

	return &Metrics{
		Allocations:     o.Int64Counter("shale.cp.allocations", metric.WithDescription("allocations answered")),
		Lost:            o.Int64Counter("shale.cp.laminae_lost", metric.WithDescription("laminae that became LOST")),
		DirectiveErrors: o.Int64Counter("shale.cp.directive_errors", metric.WithDescription("rounds of directives a node did not take")),
		Reconciled:      o.Int64Counter("shale.cp.reconciliations", metric.WithDescription("sinks reconciled")),
		Deleting:        o.Int64Gauge("shale.cp.laminae_deleting", metric.WithDescription("laminae waiting for their node to unlink them")),
		PendingHosts:    o.Int64Gauge("shale.cp.hosts_pending", metric.WithDescription("hosts waiting for adoption, by kind")),
		StoredBytes:     o.Int64Gauge("shale.cp.stored_bytes", metric.WithDescription("bytes stored per tenant"), metric.WithUnit("By")),

		AllocLatency:  o.Float64Histogram("shale.cp.allocation_latency_ms", metric.WithDescription("time to answer an allocation"), metric.WithUnit("ms")),
		WriteLatency:  o.Float64Histogram("shale.cp.write_latency_ms", metric.WithDescription("from an attempt's allocation to its commit landing here"), metric.WithUnit("ms")),
		CommitLatency: o.Float64Histogram("shale.cp.commit_latency_ms", metric.WithDescription("from the node's commit to the event landing here"), metric.WithUnit("ms")),
		Duplicates:    o.Int64Counter("shale.cp.duplicates", metric.WithDescription("stored attempts another attempt had already won")),
		Recovered:     o.Int64Counter("shale.cp.recovered", metric.WithDescription("files reconciliation learned from a sink, per sink")),

		GcApprovedBytes: o.Int64Counter("shale.cp.gc_approved_bytes", metric.WithDescription("bytes GC approved, per sink and reason"), metric.WithUnit("By")),
		Runway:          o.Int64Gauge("shale.cp.capacity_runway_s", metric.WithDescription("seconds until the attached sinks are full at the sources' observed rates; -1 when nothing is being written"), metric.WithUnit("s")),

		DeviceScore: o.Float64Gauge("shale.cp.device_failure_score", metric.WithDescription("a device's failure score as last scored")),
		Quarantines: o.Int64Counter("shale.cp.quarantines", metric.WithDescription("transitions into QUARANTINED, by kind")),
		IoErrors:    o.Int64Counter("shale.cp.device_io_errors", metric.WithDescription("I/O errors device reports carried, per device")),

		CertsDue:          o.Int64Gauge("shale.cp.certs_due", metric.WithDescription("host certificates expiring within thirty days, by kind")),
		ProducersPerRelay: o.Int64Gauge("shale.cp.producers_per_relay", metric.WithDescription("adopted producers assigned to the relay")),
		Reassignments:     o.Int64Counter("shale.cp.relay_reassignments", metric.WithDescription("producers moved to another relay")),
		DatesUnsynced:     o.Int64Gauge("shale.cp.laminae_dates_unsynced", metric.WithDescription("laminae whose rescheduled dates the node has not confirmed: directives pending")),
		ReconcileLag:      o.Int64Gauge("shale.cp.reconcile_lag_s", metric.WithDescription("seconds since the sink was last reconciled"), metric.WithUnit("s")),
		Pruned:            o.Int64Counter("shale.cp.rows_pruned", metric.WithDescription("rows the retention sweeps removed, by kind")),
	}
}

func (d *Deps) metrics() *Metrics {
	if d.M == nil {
		d.M = NewMetrics(context.Background())
	}

	return d.M
}

func kindAttr(kind string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("kind", kind))
}

func idAttr(name, id string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(name, id))
}

func pairAttr(k1, v1, k2, v2 string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(k1, v1), attribute.String(k2, v2))
}
