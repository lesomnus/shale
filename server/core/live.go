package core

import (
	"context"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// Live viewing (§39) lands with the relay: assignment, publish and view
// tokens, and the Live RPCs. Until then the CP has no relay to assign.

// relayAssignment is the relay a producer sends to, or nil when none is
// assigned or available.
func (s Core) relayAssignment(ctx context.Context, producer pdid.Id, set *api.Set) (*api.RelayAssignment, error) {
	return nil, nil
}

func (s coreSet) Live(ctx context.Context, req *api.SetLiveRequest) (*api.SetLiveResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	return nil, status.Error(codes.Unimplemented, "live viewing arrives with the relay")
}

func (s coreSource) Live(ctx context.Context, req *api.SourceLiveRequest) (*api.SourceLiveResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	return nil, status.Error(codes.Unimplemented, "live viewing arrives with the relay")
}

// Starvation thresholds (§38.5).
const (
	starvedEpisodes = 50
	starvedShare    = 0.10
	raiseFactor     = 1.25
)

// starvation folds a producer's per-source report into the source rows
// (§38.5): seconds at the cap and episodes accumulate over a day; a source
// with at least 50 episodes in a day while fewer than 10% of its seconds
// were at the cap is starved, and the CP suggests +25%. A set with
// auto_raise applies it, at most once a day, within the policy cap and the
// set's total; ceilings are never lowered here.
func (s Core) starvation(ctx context.Context, p *api.Producer, reports []*api.SourceReport) ([]*api.Suggestion, error) {
	if len(reports) == 0 {
		return nil, nil
	}
	b, _, _, _, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	var set *api.Set
	if len(p.GetSet().GetId()) > 0 {
		set, _ = s.d.Own.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: p.GetSet().GetId()}.Build()}.Build())
	}
	now := s.d.now()

	var out []*api.Suggestion
	for _, r := range reports {
		if len(r.GetSourceId()) == 0 {
			continue
		}
		src, err := s.d.Own.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: r.GetSourceId()}.Build()}.Build())
		if err != nil {
			continue
		}
		if set != nil && string(src.GetSet().GetId()) != string(set.GetId()) {
			continue
		}
		st := src.GetStarvation()
		if st == nil || st.GetDateReset() == nil || now.Sub(st.GetDateReset().AsTime()) > 24*time.Hour {
			st = api.Starvation_builder{DateReset: timestamppb.New(now)}.Build()
		}
		st.SetSecondsAtCap(st.GetSecondsAtCap() + r.GetSecondsAtCap())
		st.SetEpisodes(st.GetEpisodes() + r.GetEpisodes())
		st.SetSecondsTotal(st.GetSecondsTotal() + r.GetSecondsTotal())
		starved := st.GetEpisodes() >= starvedEpisodes && st.GetSecondsTotal() > 0 &&
			float64(st.GetSecondsAtCap())/float64(st.GetSecondsTotal()) < starvedShare
		st.SetStarved(starved)

		ceiling := src.GetProfile().GetMaxBitrate()
		patch := api.SourcePatchRequest_builder{
			Ref:              api.SourceRef_builder{Id: src.GetId()}.Build(),
			Starvation:       st,
			DateUpdatedForce: z.Ptr(true),
		}
		if starved && ceiling > 0 {
			suggested := int64(float64(ceiling) * raiseFactor)
			if suggested > b.MaxBitrateCap {
				suggested = b.MaxBitrateCap
			}
			st.SetSuggestedMaxBitrate(suggested)
			sg := api.Suggestion_builder{SourceId: src.GetId(), MaxBitrate: ceiling, Suggested: suggested,
				Reason: "starved: episodes at the cap while the day was mostly below it"}
			if set != nil && set.GetAutoRaise() && suggested > ceiling && (st.GetDateReset() == nil || now.Sub(st.GetDateReset().AsTime()) < time.Minute) {
				// At most once a day: the counters reset when a raise is applied.
				prof := src.GetProfile()
				prof.SetMaxBitrate(suggested)
				patch.Profile = prof
				st.SetDateReset(timestamppb.New(now))
				st.SetEpisodes(0)
				st.SetSecondsAtCap(0)
				st.SetSecondsTotal(0)
				sg.Applied = true
			}
			out = append(out, sg.Build())
		}
		if _, err := s.d.Own.Source().Patch(ctx, patch.Build()); err != nil {
			s.d.log().Warn("starvation", "source", src.GetAlias(), "err", err.Error())
		}
	}

	return out, nil
}
