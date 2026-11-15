package core

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// Starvation and auto_raise (§38.5). The producer counts, per source, the
// seconds its stream sat at its ceiling and the episodes, runs of 3 to 60
// of them; the CP adds them up over a day, says whether the source is
// starved, and suggests a quarter more. A set with auto_raise applies the
// suggestion itself.

const (
	starvedEpisodes = 50
	starvedShare    = 0.10
	raiseFactor     = 1.25
	// starvationDay is how long the counts add up for, and the least time
	// between two raises of one source.
	starvationDay = 24 * time.Hour
	// starvedWhy is what a suggestion says before anything else.
	starvedWhy = "starved: episodes at the cap while the day was mostly below it"
)

// raisedFloor is the ceiling auto_raise raised src to, which negotiation
// agrees at least while the set's auto_raise is on (§38.5, §12.6); zero
// when there is none.
func raisedFloor(set *api.Set, src *api.Source) int64 {
	if !set.GetAutoRaise() {
		return 0
	}

	return src.GetStarvation().GetRaisedMaxBitrate()
}

// ceilingOf is a source's ceiling as its next negotiation agrees it: the
// stored one, or the raise auto_raise applied since.
func ceilingOf(set *api.Set, src *api.Source) int64 {
	return max(src.GetProfile().GetMaxBitrate(), raisedFloor(set, src))
}

// raiseLimit says which limit of §38.5 a raise of one ceiling from `from`
// to `to` would break, nil when it breaks none: the upload policy's cap on
// any one ceiling, the set's max_bitrate_total on their sum, and the
// producer's declared uplink (zero when none) on that sum as producer
// admission counts it (UplinkNeed). `total` is the set's ceilings summed
// before the raise; `live` says the producer's live policy is `always`.
func raiseLimit(b Bounds, set *api.Set, uplink int64, live bool, total, from, to int64) *api.RaiseBlocked {
	if to > b.MaxBitrateCap {
		return api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_UPLOAD_POLICY, Required: to, Available: b.MaxBitrateCap}.Build()
	}
	sum := total - from + to
	if cap := set.GetMaxBitrateTotal(); cap > 0 && sum > cap {
		return api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_SET_BITRATE, Required: sum, Available: cap}.Build()
	}
	if need := UplinkNeed(sum, live); uplink > 0 && need > uplink {
		return api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_UPLINK, Required: need, Available: uplink}.Build()
	}

	return nil
}

// heldBy says, for a suggestion's reason, what limit holds a raise back.
func heldBy(v *api.RaiseBlocked) string {
	switch v.GetLimit() {
	case api.RaiseLimit_RAISE_LIMIT_UPLOAD_POLICY:
		return fmt.Sprintf("the upload policy caps a ceiling at %d bps", v.GetAvailable())
	case api.RaiseLimit_RAISE_LIMIT_SET_BITRATE:
		return fmt.Sprintf("the set's ceilings would sum to %d bps, over its max_bitrate_total of %d", v.GetRequired(), v.GetAvailable())
	case api.RaiseLimit_RAISE_LIMIT_UPLINK:
		return fmt.Sprintf("recording and live would need %d bps of the producer's uplink of %d", v.GetRequired(), v.GetAvailable())
	default:
		return "a limit"
	}
}

// newDay starts the day the counts are of at `now` (§38.5). What the last
// raise was is kept: it is history, and the floor of negotiation.
func newDay(st *api.Starvation, now time.Time) {
	st.SetDateReset(timestamppb.New(now))
	st.SetSecondsAtCap(0)
	st.SetEpisodes(0)
	st.SetSecondsTotal(0)
	st.SetStarved(false)
	st.SetSuggestedMaxBitrate(0)
	st.ClearBlocked()
}

// counted is a source's starvation with one report added to the day's
// counts (§38.5): a report more than a day after the day began starts
// another. Starved is at least 50 episodes in the day while fewer than 10%
// of its seconds were at the cap.
func counted(prev *api.Starvation, r *api.SourceReport, now time.Time) *api.Starvation {
	st := &api.Starvation{}
	if prev != nil {
		st = proto.CloneOf(prev)
	}
	if st.GetDateReset() == nil || now.Sub(st.GetDateReset().AsTime()) > starvationDay {
		newDay(st, now)
	}
	st.SetSecondsAtCap(st.GetSecondsAtCap() + max(r.GetSecondsAtCap(), 0))
	st.SetEpisodes(st.GetEpisodes() + max(r.GetEpisodes(), 0))
	st.SetSecondsTotal(st.GetSecondsTotal() + max(r.GetSecondsTotal(), 0))
	st.SetStarved(st.GetEpisodes() >= starvedEpisodes && st.GetSecondsTotal() > 0 &&
		float64(st.GetSecondsAtCap())/float64(st.GetSecondsTotal()) < starvedShare)

	return st
}

// raiseDue says the last raise was a day ago or more, or never was.
func raiseDue(st *api.Starvation, now time.Time) bool {
	return st.GetDateRaised() == nil || now.Sub(st.GetDateRaised().AsTime()) >= starvationDay
}

// starvation folds a producer's per-source reports into the source rows
// and answers what it suggests (§38.5). A starved source earns a quarter
// more. A set with auto_raise applies it to a source whose producer says it
// encodes it with VBR, that no camera Binding pins, whose last raise was a
// day ago or more, and only inside every limit raiseLimit checks; `uplink`
// is the one the producer declared. A raise that would break a limit is
// held there, kept on the source for the console, and the CP warns when it
// first is. An applied raise bumps the set's profile version, so the
// producer negotiates again at once and is agreed the raised ceiling.
func (s Core) starvation(ctx context.Context, p *api.Producer, uplink int64, reports []*api.SourceReport) ([]*api.Suggestion, error) {
	if len(reports) == 0 || len(p.GetSet().GetId()) == 0 {
		return nil, nil
	}
	b, _, _, _, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	set, err := s.own(ctx).Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: p.GetSet().GetId()}.Build()}.Build())
	if err != nil {
		s.d.log().Warn("starvation", "producer", p.GetAlias(), "err", err.Error())
		return nil, nil
	}
	live := livePolicy(p.GetLive()) == api.LivePolicy_LIVE_POLICY_ALWAYS
	now := s.d.now()

	// The set's ceilings summed, read when a source is first found starved
	// and kept up with the raises applied here.
	total := int64(-1)
	var out []*api.Suggestion
	var raised []*api.SourcePatchRequest
	var applied []int
	for _, r := range reports {
		if len(r.GetSourceId()) == 0 {
			continue
		}
		src, err := s.own(ctx).Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: r.GetSourceId()}.Build()}.Build())
		if err != nil || !bytes.Equal(src.GetSet().GetId(), set.GetId()) {
			continue
		}
		st := counted(src.GetStarvation(), r, now)
		ceiling := ceilingOf(set, src)
		if !st.GetStarved() || ceiling <= 0 {
			st.SetSuggestedMaxBitrate(0)
			st.ClearBlocked()
			s.keepStarvation(ctx, src, st)
			continue
		}
		suggested := int64(float64(ceiling) * raiseFactor)
		st.SetSuggestedMaxBitrate(suggested)
		if total < 0 {
			if total, err = s.ceilings(ctx, set); err != nil {
				return nil, err
			}
		}
		managed, err := s.cameraHasBinding(ctx, src)
		if err != nil {
			return nil, err
		}
		// A CBR source sits at its ceiling by definition, and a copied
		// stream's rate is the camera's: neither is raised this way, nor a
		// camera whose Binding pins its ceiling, nor a source whose
		// producer did not say.
		auto := set.GetAutoRaise() && !managed && r.GetRateControl() == api.RateControl_RATE_CONTROL_VBR
		sg := api.Suggestion_builder{SourceId: src.GetId(), MaxBitrate: ceiling, Suggested: suggested, Reason: starvedWhy}
		held := raiseLimit(b, set, uplink, live, total, ceiling, suggested)
		switch was := src.GetStarvation().GetBlocked(); {
		case held != nil:
			if was.GetLimit() == held.GetLimit() && was.GetDateSince() != nil {
				held.SetDateSince(was.GetDateSince())
			} else {
				held.SetDateSince(timestamppb.New(now))
				if auto {
					s.d.log().Warn("auto_raise held: the raise would break a limit", "source", src.GetAlias(), "max_bitrate", ceiling, "suggested", suggested,
						"limit", held.GetLimit().String(), "required", held.GetRequired(), "available", held.GetAvailable())
				}
			}
			st.SetBlocked(held)
			sg.Reason = starvedWhy + "; held: " + heldBy(held)
		case !auto || !raiseDue(st, now):
			st.ClearBlocked()
		default:
			st.SetDateRaised(timestamppb.New(now))
			st.SetRaisedFrom(ceiling)
			st.SetRaisedMaxBitrate(suggested)
			// A new day: what the source did at its old ceiling says
			// nothing of the new one.
			newDay(st, now)
			total += suggested - ceiling
			sg.Applied = true
			sg.Reason = starvedWhy + "; raised by auto_raise"
			raised = append(raised, starvationPatch(src, st))
			applied = append(applied, len(out))
			out = append(out, sg.Build())
			s.d.log().Info("auto_raise", "source", src.GetAlias(), "from", ceiling, "to", suggested)
			continue
		}
		out = append(out, sg.Build())
		s.keepStarvation(ctx, src, st)
	}
	if len(raised) == 0 {
		return out, nil
	}

	// The raises and a new profile version together: the version is what
	// makes the producer negotiate again, and that negotiation is where the
	// raised ceilings are agreed (§12.6).
	err = s.ownTx(ctx, func(ctx context.Context, nx api.Server) error {
		for _, patch := range raised {
			if _, err := nx.Source().Patch(ctx, patch); err != nil {
				return err
			}
		}
		cur, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		v := cur.GetProfileVersion() + 1
		_, err = nx.Set().Patch(ctx, api.SetPatchRequest_builder{
			Ref:            api.SetRef_builder{Id: set.GetId()}.Build(),
			ProfileVersion: &v,
			DateUpdated:    cur.GetDateUpdated(),
		}.Build())

		return err
	})
	if err != nil {
		// Not applied: the source is still starved at its next report, and
		// is raised then.
		s.d.log().Warn("auto_raise not stored", "set", set.GetAlias(), "err", err.Error())
		for _, i := range applied {
			out[i].SetApplied(false)
			out[i].SetReason(starvedWhy)
		}
	}

	return out, nil
}

// starvationPatch writes a source's starvation and nothing else.
func starvationPatch(src *api.Source, st *api.Starvation) *api.SourcePatchRequest {
	return api.SourcePatchRequest_builder{
		Ref:              api.SourceRef_builder{Id: src.GetId()}.Build(),
		Starvation:       st,
		DateUpdatedForce: z.Ptr(true),
	}.Build()
}

// keepStarvation stores what starvation made of a source's report; a
// failure costs one report's counts, and is logged.
func (s Core) keepStarvation(ctx context.Context, src *api.Source, st *api.Starvation) {
	if _, err := s.own(ctx).Source().Patch(ctx, starvationPatch(src, st)); err != nil {
		s.d.log().Warn("starvation", "source", src.GetAlias(), "err", err.Error())
	}
}

// ceilings is the sum of a set's ceilings as its next negotiation agrees
// them.
func (s Core) ceilings(ctx context.Context, set *api.Set) (int64, error) {
	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return 0, err
	}
	var total int64
	for _, m := range members {
		total += ceilingOf(set, m)
	}

	return total, nil
}
