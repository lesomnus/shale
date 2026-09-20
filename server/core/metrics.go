package core

import (
	"context"

	"github.com/lesomnus/otx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics are the control plane's instruments (§31).
type Metrics struct {
	Allocations     metric.Int64Counter
	Lost            metric.Int64Counter
	DirectiveErrors metric.Int64Counter
	Reconciled      metric.Int64Counter
	Deleting        metric.Int64Gauge
	PendingHosts    metric.Int64Gauge
	StoredBytes     metric.Int64Gauge
}

// NewMetrics makes the instruments from the telemetry the context carries.
func NewMetrics(ctx context.Context) *Metrics {
	o := otx.From(ctx)

	return &Metrics{
		Allocations:     o.Int64Counter("shale.cp.allocations", metric.WithDescription("allocations answered")),
		Lost:            o.Int64Counter("shale.cp.objects_lost", metric.WithDescription("objects that became LOST")),
		DirectiveErrors: o.Int64Counter("shale.cp.directive_errors", metric.WithDescription("rounds of directives a node did not take")),
		Reconciled:      o.Int64Counter("shale.cp.reconciliations", metric.WithDescription("sinks reconciled")),
		Deleting:        o.Int64Gauge("shale.cp.objects_deleting", metric.WithDescription("objects waiting for their node to unlink them")),
		PendingHosts:    o.Int64Gauge("shale.cp.hosts_pending", metric.WithDescription("hosts waiting for adoption, by kind")),
		StoredBytes:     o.Int64Gauge("shale.cp.stored_bytes", metric.WithDescription("bytes stored per tenant"), metric.WithUnit("By")),
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
