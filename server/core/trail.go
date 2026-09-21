package core

import (
	"context"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/spin"
	"github.com/lesomnus/payday/trail"

	"github.com/lesomnus/shale/server/bare"
)

// TrailOfPeople is the audit trail narrowed to what people do (§20.4,
// §26.5). payday's recorder is told about every write inside the
// transaction that makes it, which is right for a trail that must not miss
// a decision. Most of what this app writes decides nothing: a producer
// allocating, a node's events landing, the leader's jobs moving dates and
// deleting, a heartbeat's date_seen, the tenant's byte count. On the load
// run those were thirteen rows a lamina and five sixths of the database,
// with no clock to leave by. A person's write is the evidence a trail is
// for, so a write is recorded when the frame's actor is a person, and the
// system's own bookkeeping, the hosts' and the deployment's, is not.
func TrailOfPeople(next bare.Recorder) bare.Recorder {
	return trailOfPeople{next}
}

type trailOfPeople struct{ next bare.Recorder }

func (r trailOfPeople) Record(ctx context.Context, s bare.Server, c bare.Change) error {
	f, ok := frame.From(ctx)
	if !ok || f == nil || kindOf(f.Actor) != DomHolder {
		return nil
	}

	return r.next.Record(ctx, s, c)
}

// TrailSweep applies the trail's retention policy (§26.5) on its clock,
// on the leader (§34.9). payday's own sweep is safe on every replica, at
// the cost of each one archiving the same rows; one leader sweeping keeps
// the archive to one copy of each and the work to one process. A replica
// that is not leading skips the pass and looks again at the next tick,
// which is a day by default: a window that took a day longer to close
// after a failover is not a row anybody was promised would be gone.
//
// A nil leader is a deployment with one control plane, which always
// leads.
func TrailSweep(store trail.Store, p trail.Policy, leader func(ctx context.Context) bool) spin.Spinner {
	every := p.Every
	if every <= 0 {
		every = trail.Swept
	}

	return spin.Every(every, func(ctx context.Context) error {
		if leader != nil && !leader(ctx) {
			return nil
		}
		p.Pass(ctx, store)

		return nil
	})
}
