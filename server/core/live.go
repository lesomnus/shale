package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/relay"
)

// Live viewing (§39): a producer is assigned a relay and a publish token;
// a viewer gets, per source, the relay's WHEP endpoint and a view token.
// The relay never asks the CP anything: the tokens carry it all (§39.7).

// relayAssignment is the relay a producer sends to: the one it has while
// that relay is alive, else the least loaded one whose labels match the
// set's site (§39.2). Nil when no relay is available.
func (s Core) relayAssignment(ctx context.Context, producerId pdid.Id, set *api.Set) (*api.RelayAssignment, error) {
	now := s.d.now()
	p, err := s.d.Ent.Producer.Get(ctx, producerId.Uuid())
	if err != nil {
		return nil, err
	}
	var chosen *ent.Relay
	if !isZero(p.RelayId) {
		if r, err := s.d.Ent.Relay.Get(ctx, p.RelayId); err == nil && relayAlive(r, now) {
			chosen = r
		}
	}
	if chosen == nil {
		chosen, err = s.pickRelay(ctx, set, now)
		if err != nil || chosen == nil {
			return nil, err
		}
		if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref:              api.ProducerRef_builder{Id: producerId.Bytes()}.Build(),
			Relay:            api.RelayRef_builder{Id: chosen.Id[:]}.Build(),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		s.d.log().Info("relay assigned", "producer", p.Alias, "relay", chosen.Alias)
	}

	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return nil, err
	}
	var sources [][]byte
	for _, m := range members {
		sources = append(sources, m.GetId())
	}
	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	exp := now.Add(b.PublishTokenTTL)
	tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
		Exp: timestamppb.New(exp), Iat: timestamppb.New(now),
		Aud: chosen.Id[:], Op: api.TokenOp_TOKEN_OP_PUBLISH, Sources: sources, Actor: producerId.Bytes(),
	}.Build())
	if err != nil {
		return nil, err
	}

	return api.RelayAssignment_builder{
		RelayId:      chosen.Id[:],
		Endpoints:    s.relayEndpoints(chosen, chosen.IngestAddress, callerOf(ctx), address),
		PublishToken: tok,
		DateExpires:  timestamppb.New(exp),
	}.Build(), nil
}

func relayAlive(r *ent.Relay, now time.Time) bool {
	return r != nil && r.DateErased == nil && r.State == int32(api.HostState_HOST_STATE_ADOPTED) &&
		r.DateSeen != nil && now.Sub(*r.DateSeen) <= DefaultNodeDownAfter
}

// pickRelay is the live relay with the least attached bitrate among those
// whose labels match the set's site (§39.2).
func (s Core) pickRelay(ctx context.Context, set *api.Set, now time.Time) (*ent.Relay, error) {
	relays, err := s.d.Ent.Relay.Query().Where(relay.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), relay.DateErasedIsNil()).All(ctx)
	if err != nil {
		return nil, err
	}
	var selector map[string]string
	if len(set.GetSite().GetId()) > 0 {
		if site, err := s.d.Ent.Site.Get(ctx, mustId(set.GetSite().GetId()).Uuid()); err == nil {
			selector = site.RelaySelector
		}
	}
	// Attached bitrate per relay: the max_bitrate_total of every set whose
	// producers it carries.
	load := map[[16]byte]int64{}
	producers, err := s.d.Ent.Producer.Query().Where(producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil()).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range producers {
		if isZero(p.RelayId) || isZero(p.SetId) {
			continue
		}
		if st, err := s.d.Ent.Set.Get(ctx, p.SetId); err == nil {
			load[p.RelayId] += max(st.MaxBitrateTotal, 1)
		}
	}

	var best *ent.Relay
	for _, r := range relays {
		if !relayAlive(r, now) || !matches(r.Labels, selector) || r.IngestAddress == "" {
			continue
		}
		if best == nil || load[r.Id] < load[best.Id] {
			best = r
		}
	}

	return best, nil
}

// matches says whether the labels carry every pair of the selector.
func matches(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}

	return true
}

// relayEndpoints resolves one of a relay's listeners for a caller, like a
// node's data plane (§34.10).
func (s Core) relayEndpoints(r *ent.Relay, addr, caller string, p *api.AddressParams) []*api.Endpoint {
	if r == nil || addr == "" {
		return nil
	}
	res := s.d.Resolve
	if res == nil {
		res = Advertised{}
	}

	return res.Endpoints(NodeAddresses{Id: pdid.Id(r.Id), Alias: r.Alias, Interfaces: r.Interfaces, DataAddress: addr, Dev: s.d.Dev}, caller, p)
}

// liveSources is the Live answer for these sources of a set: the relay of
// the set's producer, a view token per source, and the WHEP URL (§39.4).
func (s Core) liveSources(ctx context.Context, f *frame.Frame, set *api.Set, sources []*api.Source) ([]*api.LiveSource, error) {
	for _, src := range sources {
		// A raw source (§38.9) has nothing a relay could show.
		if ct := src.GetContentType(); ct != "" && !strings.HasPrefix(ct, "video/") {
			return nil, status.Errorf(codes.FailedPrecondition, "%s is %s: nothing a relay could show", src.GetAlias(), ct)
		}
	}
	now := s.d.now()
	producers, err := s.d.Ent.Producer.Query().
		Where(producer.SetIdEQ(mustId(set.GetId()).Uuid()), producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	var r *ent.Relay
	for _, p := range producers {
		if isZero(p.RelayId) {
			continue
		}
		if v, err := s.d.Ent.Relay.Get(ctx, p.RelayId); err == nil && relayAlive(v, now) {
			r = v
			break
		}
	}
	if r == nil {
		return nil, status.Error(codes.FailedPrecondition, "no relay carries this set's cameras yet: its producer is not assigned one, or the relay is down")
	}
	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	eps := s.relayEndpoints(r, r.WhepAddress, callerOf(ctx), address)
	if len(eps) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "the relay has no WHEP address for this caller")
	}
	exp := now.Add(b.ViewTokenTTL)

	var out []*api.LiveSource
	for _, src := range sources {
		tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
			Exp: timestamppb.New(exp), Iat: timestamppb.New(now),
			Aud: r.Id[:], Op: api.TokenOp_TOKEN_OP_VIEW, Source: src.GetId(),
			Actor: f.Actor.Bytes(), ActorTenant: f.Tenant.Bytes(),
		}.Build())
		if err != nil {
			return nil, err
		}
		out = append(out, api.LiveSource_builder{
			SourceId:    src.GetId(),
			Ordinal:     src.GetOrdinal(),
			RelayId:     r.Id[:],
			Endpoints:   eps,
			WhepUrl:     fmt.Sprintf("%s://%s:%d/whep/%s", eps[0].GetScheme(), eps[0].GetHost(), eps[0].GetPort(), mustId(src.GetId()).String()),
			ViewToken:   tok,
			DateExpires: timestamppb.New(exp),
		}.Build())
	}

	return out, nil
}

func (s coreSet) Live(ctx context.Context, req *api.SetLiveRequest) (*api.SetLiveResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	set, err := s.SetServiceServer.Get(ctx, api.SetGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return nil, err
	}
	vs, err := s.liveSources(ctx, f, set, members)
	if err != nil {
		return nil, err
	}

	return api.SetLiveResponse_builder{Sources: vs}.Build(), nil
}

func (s coreSource) Live(ctx context.Context, req *api.SourceLiveRequest) (*api.SourceLiveResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	src, err := s.SourceServiceServer.Get(ctx, api.SourceGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	set, err := s.Next().Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: src.GetSet().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	vs, err := s.liveSources(ctx, f, set, []*api.Source{src})
	if err != nil {
		return nil, err
	}

	return api.SourceLiveResponse_builder{Source: vs[0]}.Build(), nil
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
