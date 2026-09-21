package storage

import (
	"context"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// Lazy GC (§21): under pressure the node proposes expired files, oldest
// first, about gc_proposal_factor times the bytes it needs, in pages; the
// daily sweep proposes every file whose date_deleted has passed (§20.1).

type gcStats struct {
	proposed  int64
	deleted   int64
	reclaimed int64
}

func (n *Node) gcLoop(ctx context.Context) error {
	pressure := time.NewTicker(n.cfg.GcInterval)
	defer pressure.Stop()
	// The first sweep an hour after start, then every sweep_interval.
	sweep := time.NewTimer(time.Hour)
	defer sweep.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-pressure.C:
			for _, s := range n.sinks {
				if !s.Index.Scanned() {
					continue
				}
				if s.Pressure() == api.Pressure_PRESSURE_NORMAL {
					s.critical.Store(false)
					continue
				}
				if _, err := n.gcRound(ctx, s, api.GcReason_GC_REASON_PRESSURE, false); err != nil {
					n.log.Warn("gc", "sink", s.Id.String(), "err", err.Error())
				}
			}
		case <-sweep.C:
			for _, s := range n.sinks {
				if !s.Index.Scanned() {
					continue
				}
				if _, err := n.gcRound(ctx, s, api.GcReason_GC_REASON_SWEEP, false); err != nil {
					n.log.Warn("sweep", "sink", s.Id.String(), "err", err.Error())
				}
			}
			sweep.Reset(n.cfg.SweepInterval)
		}
	}
}

// gcRound is one round of proposing and applying (§21.2).
func (n *Node) gcRound(ctx context.Context, s *Sink, reason api.GcReason, force bool) (gcStats, error) {
	var st gcStats
	now := time.Now()
	capacity, free := s.Free()

	var needed int64
	if reason == api.GcReason_GC_REASON_PRESSURE {
		target := int64(float64(capacity) * s.Marks.Target)
		needed = target - free
		if needed <= 0 && !force {
			return st, nil
		}
		if needed < 0 {
			needed = 0
		}
	}

	// Candidates from the index, by the xattr dates.
	var cands []*Entry
	for _, e := range s.Index.Snapshot() {
		switch reason {
		case api.GcReason_GC_REASON_SWEEP:
			if !e.Deleted.IsZero() && !e.Deleted.After(now) {
				cands = append(cands, e)
			}
		default:
			if !e.Expired.IsZero() && !e.Expired.After(now) {
				cands = append(cands, e)
			}
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if reason == api.GcReason_GC_REASON_SWEEP {
			return cands[i].Deleted.Before(cands[j].Deleted)
		}

		return cands[i].Expired.Before(cands[j].Expired)
	})

	if reason == api.GcReason_GC_REASON_PRESSURE {
		budget := int64(float64(needed) * n.cfg.GcProposalFactor)
		var sum int64
		cut := len(cands)
		for i, e := range cands {
			if sum >= budget {
				cut = i
				break
			}
			sum += e.Size
		}
		cands = cands[:cut]
	}

	if len(cands) == 0 {
		if reason == api.GcReason_GC_REASON_PRESSURE && free < int64(float64(capacity)*s.Marks.Critical) {
			// Nothing approvable and below critical: refuse uploads (§21.1).
			if !s.critical.Load() {
				n.log.Warn("sink critical: nothing to reclaim", "sink", s.Id.String())
			}
			s.critical.Store(true)
		}

		return st, nil
	}

	conn, err := n.client(ctx)
	if err != nil {
		return st, err
	}
	client := api.NewSinkServiceClient(conn)

	for i := 0; i < len(cands); i += n.cfg.GcPage {
		j := i + n.cfg.GcPage
		if j > len(cands) {
			j = len(cands)
		}
		page := cands[i:j]
		var gcs []*api.GcCandidate
		for _, e := range page {
			c := api.GcCandidate_builder{
				LaminaKey: e.Key, LaminaId: e.Record.GetLaminaId(), AttemptId: e.Record.GetAttemptId(),
				Size: e.Size, TenantId: e.Record.GetTenantId(),
			}
			if !e.Expired.IsZero() {
				c.DateExpired = timestamppb.New(e.Expired)
			}
			if !e.Deleted.IsZero() {
				c.DateDeleted = timestamppb.New(e.Deleted)
			}
			gcs = append(gcs, c.Build())
		}
		st.proposed += int64(len(gcs))

		cctx, cancel := context.WithTimeout(ctx, time.Minute)
		resp, err := client.ProposeGc(cctx, api.SinkProposeGcRequest_builder{
			Ref: api.SinkRef_builder{Id: s.Id.Bytes()}.Build(), Reason: reason, Candidates: gcs, BytesNeeded: needed - st.reclaimed,
		}.Build())
		cancel()
		if err != nil {
			n.resetClient()
			return st, err
		}

		for _, d := range resp.GetDecisions() {
			e, ok := s.Index.Get(d.GetLaminaKey())
			if !ok {
				continue
			}
			if d.GetApproved() {
				if n.unlink(s, d.GetLaminaKey()) == api.DeleteResult_DELETE_RESULT_DELETED {
					st.deleted++
					st.reclaimed += e.Size
				}
				continue
			}
			if d.GetDateExpired() != nil || d.GetDateDeleted() != nil {
				// The CP knows newer dates: rewrite the xattr (§21.2 step 5).
				n.setDates(s, d.GetLaminaKey(), d.GetDateExpired(), d.GetDateDeleted())
			}
		}
		// Enough: a round stops at the target rather than at the end of
		// its candidates (§21.1).
		if reason == api.GcReason_GC_REASON_PRESSURE && !force {
			if _, f := s.Free(); f >= int64(float64(capacity)*s.Marks.Target) {
				break
			}
		}
	}

	_, free = s.Free()
	if reason == api.GcReason_GC_REASON_PRESSURE && free >= int64(float64(capacity)*s.Marks.Critical) {
		s.critical.Store(false)
	}
	n.log.Info("gc", "sink", s.Id.String(), "reason", reason.String(), "proposed", st.proposed, "deleted", st.deleted, "reclaimed", st.reclaimed)
	n.m.reclaimed.Add(ctx, st.reclaimed, sinkAttr(s))

	return st, nil
}
