package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/relay"
	"github.com/lesomnus/shale/internal/ent/signingkey"
	"github.com/lesomnus/shale/internal/ent/sink"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The Control Plane's background work (§34.9): attempts that expire, laminae
// nobody stored, rows past their retention, keys past their rotation, sinks
// whose node is gone. Exactly one CP replica runs it, elected by a lease the
// deployment supplies; `shale serve all` and a single control process need
// none.

// Jobs is what the leader runs.
type Jobs struct {
	d *Deps
	// Leader says whether this process may run the jobs now.
	Leader func(ctx context.Context) bool
	Every  time.Duration
	Log    *slog.Logger
}

// NewJobs makes the jobs; with a nil Leader every call runs them.
func NewJobs(d *Deps) *Jobs {
	return &Jobs{d: d, Every: time.Minute}
}

// Spin runs the jobs on a clock until the context is done, which is what
// payday's spin.Run looks for.
func (j *Jobs) Spin(ctx context.Context) error {
	t := time.NewTicker(j.Every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if j.Leader != nil && !j.Leader(ctx) {
			continue
		}
		if err := j.Once(ctx); err != nil {
			j.log().Warn("jobs", "err", err.Error())
		}
	}
}

func (j *Jobs) log() *slog.Logger {
	if j.Log != nil {
		return j.Log
	}

	return j.d.log()
}

// Once runs every job one time.
func (j *Jobs) Once(ctx context.Context) error {
	now := j.d.now()
	own := j.d.Own

	// Attempts past their TTL with no event are ABANDONED (§12.1).
	expired, err := j.d.Ent.Attempt.Query().
		Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresLT(now)).
		Limit(1000).All(ctx)
	if err != nil {
		return err
	}
	for _, a := range expired {
		st := api.AttemptState_ATTEMPT_STATE_ABANDONED
		if _, err := own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
			Ref: api.AttemptRef_builder{Id: a.Id[:]}.Build(), State: &st,
			DateFinished: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			j.log().Warn("abandon attempt", "err", err.Error())
		}
	}

	// Laminae none of whose attempts stored anything, abandon_grace after
	// the last one expired, are removed (§12.1, §20.4).
	pending, err := j.d.Ent.Lamina.Query().
		Where(lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_PENDING)), lamina.DateCreatedLT(now.Add(-DefaultAbandonGrace))).
		Limit(1000).All(ctx)
	if err != nil {
		return err
	}
	for _, o := range pending {
		open, err := j.d.Ent.Attempt.Query().
			Where(attempt.LaminaIdEQ(o.Id), attempt.Or(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateFinishedGT(now.Add(-DefaultAbandonGrace)))).
			Count(ctx)
		if err != nil {
			return err
		}
		if open > 0 {
			continue
		}
		if err := j.eraseLamina(ctx, o.Id); err != nil {
			j.log().Warn("remove lamina", "err", err.Error())
		} else {
			j.d.metrics().Pruned.Add(ctx, 1, kindAttr("lamina_unused"))
		}
	}

	// Row retention (§20.4): DELETED and LOST laminae a month after they
	// ended.
	old, err := j.d.Ent.Lamina.Query().
		Where(lamina.StateIn(int32(api.LaminaState_LAMINA_STATE_DELETED), int32(api.LaminaState_LAMINA_STATE_LOST)), lamina.DateFinishedLT(now.Add(-DefaultRowRetention))).
		Limit(1000).All(ctx)
	if err != nil {
		return err
	}
	for _, o := range old {
		if o.DateDeleted != nil && o.DateDeleted.Add(DefaultRowRetention).After(now) {
			continue
		}
		if err := j.eraseLamina(ctx, o.Id); err != nil {
			j.log().Warn("prune lamina", "err", err.Error())
		} else {
			j.d.metrics().Pruned.Add(ctx, 1, kindAttr("lamina"))
		}
	}
	// Attempts in a terminal state other than STORED, a week after they
	// ended, whose lamina is gone.
	stale, err := j.d.Ent.Attempt.Query().
		Where(attempt.StateNEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateFinishedLT(now.Add(-7*24*time.Hour))).
		Limit(1000).All(ctx)
	if err != nil {
		return err
	}
	for _, a := range stale {
		if _, err := j.d.Ent.Lamina.Get(ctx, a.LaminaId); err == nil {
			continue
		}
		if _, err := own.Attempt().Erase(ctx, api.AttemptRef_builder{Id: a.Id[:]}.Build()); err == nil {
			j.d.metrics().Pruned.Add(ctx, 1, kindAttr("attempt"))
		}
	}
	j.gauges(ctx, now)

	// Key rotation (§33.3): an ACTIVE key becomes the signing key once every
	// live node holds it; a key demoted from signing is retired after the
	// longest token lifetime.
	keys, err := j.d.Ent.SigningKey.Query().Where(signingkey.DateErasedIsNil()).All(ctx)
	if err != nil {
		return err
	}
	b, _, _, _, _ := Core{d: j.d}.policies(ctx)
	var signing []string
	for _, k := range keys {
		if k.State == int32(api.SigningKeyState_SIGNING_KEY_STATE_SIGNING) {
			signing = append(signing, k.Alias)
		}
	}
	nodes, err := j.d.Ent.Node.Query().All(ctx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		switch api.SigningKeyState(k.State) {
		case api.SigningKeyState_SIGNING_KEY_STATE_ACTIVE:
			if k.DateSigning != nil {
				// Demoted: retire after the longest token lifetime.
				if now.Sub(k.DateUpdated) > tokenLifetimeMax(b) {
					own.SigningKey().Patch(ctx, api.SigningKeyPatchRequest_builder{
						Ref: api.SigningKeyRef_builder{Id: k.Id[:]}.Build(), State: z.Ptr(api.SigningKeyState_SIGNING_KEY_STATE_RETIRED),
						DateRetired: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
					}.Build())
				}
				continue
			}
			if len(signing) > 0 && k.DateCreated.Before(now.Add(-keysTTL)) {
				// A newer key waiting: promote it once every live node holds it.
				held := true
				for _, n := range nodes {
					if n.State != int32(api.HostState_HOST_STATE_ADOPTED) || n.DateSeen == nil || now.Sub(*n.DateSeen) > DefaultNodeDownAfter {
						continue
					}
					if !contains(n.KeyIds, k.Alias) {
						held = false
						break
					}
				}
				older := false
				for _, s := range keys {
					if s.State == int32(api.SigningKeyState_SIGNING_KEY_STATE_SIGNING) && s.DateCreated.Before(k.DateCreated) {
						older = true
					}
				}
				if held && older {
					if err := j.promote(ctx, k.Id, now); err != nil {
						j.log().Warn("promote key", "err", err.Error())
					}
				}
			}
		}
	}

	// The gauges of §31: hosts pending adoption, laminae waiting to be
	// unlinked, bytes per tenant.
	m := j.d.metrics()
	if n, err := j.d.Ent.Node.Query().Where(node.StateEQ(int32(api.HostState_HOST_STATE_PENDING))).Count(ctx); err == nil {
		m.PendingHosts.Record(ctx, int64(n), kindAttr("node"))
	}
	if n, err := j.d.Ent.Producer.Query().Where(producer.StateEQ(int32(api.HostState_HOST_STATE_PENDING))).Count(ctx); err == nil {
		m.PendingHosts.Record(ctx, int64(n), kindAttr("producer"))
	}
	if n, err := j.d.Ent.Relay.Query().Where(relay.StateEQ(int32(api.HostState_HOST_STATE_PENDING))).Count(ctx); err == nil {
		m.PendingHosts.Record(ctx, int64(n), kindAttr("relay"))
	}
	if n, err := j.d.Ent.Lamina.Query().Where(lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_DELETING))).Count(ctx); err == nil {
		m.Deleting.Record(ctx, int64(n))
	}
	if ts, err := j.d.Ent.Tenant.Query().All(ctx); err == nil {
		for _, t := range ts {
			m.StoredBytes.Record(ctx, t.StoredBytes, metric.WithAttributes(attribute.String("tenant", t.Alias)))
		}
	}

	// Devices return from SUSPECT, and from quarantine after the cool-down,
	// on decay alone (§27).
	if err := (Core{d: j.d}).healthSweep(ctx, own, now); err != nil {
		j.log().Warn("health sweep", "err", err.Error())
	}

	// Sinks whose node has been down for sink_auto_adopt_after and that
	// another node reports are adopted there (§28.3).
	pendingSinks, err := j.d.Ent.Sink.Query().Where(sink.AttachmentEQ(int32(api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION))).All(ctx)
	if err != nil {
		return err
	}
	for _, sk := range pendingSinks {
		if sk.ReportedBy == nil || isZero(sk.NodeId) || *sk.ReportedBy == sk.NodeId {
			continue
		}
		n, err := j.d.Ent.Node.Get(ctx, sk.NodeId)
		if err != nil {
			continue
		}
		if n.DateSeen != nil && now.Sub(*n.DateSeen) < 10*time.Minute {
			continue
		}
		j.log().Info("sink adopted by the node reporting it; its node has been down", "sink", sk.Alias, "node", pdid.Id(*sk.ReportedBy).String())
		own.Sink().Patch(ctx, api.SinkPatchRequest_builder{
			Ref:                api.SinkRef_builder{Id: sk.Id[:]}.Build(),
			Node:               api.NodeRef_builder{Id: pdid.Id(*sk.ReportedBy).Bytes()}.Build(),
			Attachment:         z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED),
			DateReconciledNull: z.Ptr(true),
			DateUpdatedForce:   z.Ptr(true),
		}.Build())
	}

	return nil
}

// eraseLamina removes a lamina row with its attempts, which reference it.
func (j *Jobs) eraseLamina(ctx context.Context, id [16]byte) error {
	ats, err := j.d.Ent.Attempt.Query().Where(attempt.LaminaIdEQ(id)).All(ctx)
	if err != nil {
		return err
	}
	for _, a := range ats {
		if _, err := j.d.Own.Attempt().Erase(ctx, api.AttemptRef_builder{Id: a.Id[:]}.Build()); err != nil {
			return err
		}
	}
	_, err = j.d.Own.Lamina().Erase(ctx, api.LaminaRef_builder{Id: id[:]}.Build())

	return err
}

// promote makes a key the signing key and demotes the rest.
func (j *Jobs) promote(ctx context.Context, id [16]byte, now time.Time) error {
	keys, err := j.d.Ent.SigningKey.Query().Where(signingkey.StateEQ(int32(api.SigningKeyState_SIGNING_KEY_STATE_SIGNING))).All(ctx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := j.d.Own.SigningKey().Patch(ctx, api.SigningKeyPatchRequest_builder{
			Ref: api.SigningKeyRef_builder{Id: k.Id[:]}.Build(), State: z.Ptr(api.SigningKeyState_SIGNING_KEY_STATE_ACTIVE),
			DateSigning: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}
	}
	_, err = j.d.Own.SigningKey().Patch(ctx, api.SigningKeyPatchRequest_builder{
		Ref: api.SigningKeyRef_builder{Id: id[:]}.Build(), State: z.Ptr(api.SigningKeyState_SIGNING_KEY_STATE_SIGNING),
		DateSigning: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
	}.Build())
	if err == nil {
		j.d.Keys.mu.Lock()
		j.d.Keys.at = time.Time{}
		j.d.Keys.mu.Unlock()
	}

	return err
}

func contains(vs []string, v string) bool {
	for _, x := range vs {
		if x == v {
			return true
		}
	}

	return false
}
