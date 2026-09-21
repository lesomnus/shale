package core

import (
	"context"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/reader"
	"github.com/lesomnus/shale/internal/ent/relay"
	"github.com/lesomnus/shale/internal/ent/sink"
	"github.com/lesomnus/shale/internal/ent/source"
)

// certsDueWithin is how far ahead a certificate counts as due (§31).
const certsDueWithin = 30 * 24 * time.Hour

// gauges are the control plane's gauges of §31 that are read off the rows
// once a tick of the leader's jobs: certificates due, producers per relay,
// directives pending, reconciliation lag, and the capacity runway.
func (j *Jobs) gauges(ctx context.Context, now time.Time) {
	m := j.d.metrics()
	due := now.Add(certsDueWithin)
	if n, err := j.d.Ent.Node.Query().Where(node.DateErasedIsNil(), node.DateCertExpiresLT(due)).Count(ctx); err == nil {
		m.CertsDue.Record(ctx, int64(n), kindAttr("node"))
	}
	if n, err := j.d.Ent.Producer.Query().Where(producer.DateErasedIsNil(), producer.DateCertExpiresLT(due)).Count(ctx); err == nil {
		m.CertsDue.Record(ctx, int64(n), kindAttr("producer"))
	}
	if n, err := j.d.Ent.Reader.Query().Where(reader.DateErasedIsNil(), reader.DateCertExpiresLT(due)).Count(ctx); err == nil {
		m.CertsDue.Record(ctx, int64(n), kindAttr("reader"))
	}
	if n, err := j.d.Ent.Relay.Query().Where(relay.DateErasedIsNil(), relay.DateCertExpiresLT(due)).Count(ctx); err == nil {
		m.CertsDue.Record(ctx, int64(n), kindAttr("relay"))
	}

	if ps, err := j.d.Ent.Producer.Query().Where(producer.DateErasedIsNil(), producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED))).All(ctx); err == nil {
		per := map[[16]byte]int64{}
		for _, p := range ps {
			if !isZero(p.RelayId) {
				per[p.RelayId]++
			}
		}
		for id, n := range per {
			m.ProducersPerRelay.Record(ctx, n, idAttr("relay", pdid.Id(id).String()))
		}
	}

	if n, err := j.d.Ent.Lamina.Query().Where(lamina.DatesSyncedEQ(false), lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_COMMITTED))).Count(ctx); err == nil {
		m.DatesUnsynced.Record(ctx, int64(n))
	}

	sinks, err := j.d.Ent.Sink.Query().Where(sink.DateErasedIsNil(), sink.AttachmentEQ(int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED))).All(ctx)
	if err != nil {
		return
	}
	var free int64
	for _, s := range sinks {
		free += s.Free
		lag := int64(-1)
		if s.DateReconciled != nil {
			lag = int64(now.Sub(*s.DateReconciled).Seconds())
		}
		m.ReconcileLag.Record(ctx, lag, idAttr("sink", pdid.Id(s.Id).String()))
	}
	// The runway (§11.1): what is free against what the sources write, at
	// their observed rates or, before any is observed, their ceilings.
	var bps int64
	if srcs, err := j.d.Ent.Source.Query().Where(source.DateErasedIsNil()).All(ctx); err == nil {
		for _, s := range srcs {
			rate := s.Observed.GetExpected()
			if rate <= 0 {
				rate = s.Profile.GetMaxBitrate()
			}
			bps += rate
		}
	}
	runway := int64(-1)
	if bps > 0 {
		runway = free / (bps / 8)
	}
	m.Runway.Record(ctx, runway)
}
