package core

import (
	"context"
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
	p, err := s.ent(ctx).Producer.Get(ctx, producerId.Uuid())
	if err != nil {
		return nil, err
	}
	var chosen *ent.Relay
	if !isZero(p.RelayId) {
		if r, err := s.ent(ctx).Relay.Get(ctx, p.RelayId); err == nil && relayAlive(r, now, s.d.relayDownAfter()) {
			chosen = r
		}
	}
	if chosen == nil {
		chosen, err = s.pickRelay(ctx, set, now)
		if err != nil || chosen == nil {
			return nil, err
		}
		if _, err := s.own(ctx).Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref:              api.ProducerRef_builder{Id: producerId.Bytes()}.Build(),
			Relay:            api.RelayRef_builder{Id: chosen.Id[:]}.Build(),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		if !isZero(p.RelayId) {
			s.d.metrics().Reassignments.Add(ctx, 1)
		}
		s.d.log().Info("relay assigned", "producer", p.Alias, "relay", chosen.Alias)
	}

	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return nil, err
	}
	shown, epochs, err := s.publishable(ctx, producerId, members)
	if err != nil {
		return nil, err
	}
	sources := make([][]byte, 0, len(shown))
	for _, m := range shown {
		sources = append(sources, m.GetId())
	}
	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	live := livePolicy(api.LivePolicy(p.Live))
	exp := now.Add(b.PublishTokenTTL)
	tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
		Exp: timestamppb.New(exp), Iat: timestamppb.New(now),
		Aud: chosen.Id[:], Op: api.TokenOp_TOKEN_OP_PUBLISH, Sources: sources, Actor: producerId.Bytes(), ActorTenant: p.TenantId[:], CaptureEpochs: epochs,
		Live: live, RewindSeconds: rewindSeconds(shown, b),
	}.Build())
	if err != nil {
		return nil, err
	}

	return api.RelayAssignment_builder{
		RelayId:      chosen.Id[:],
		Endpoints:    s.relayEndpoints(chosen, chosen.IngestAddress, callerOf(ctx), address),
		PublishToken: tok,
		DateExpires:  timestamppb.New(exp),
		Live:         live,
	}.Build(), nil
}

// publishable are the members of a producer's set a publish token names
// (§39.3), with the capture grants of the managed ones among them.
func (s Core) publishable(ctx context.Context, producerId pdid.Id, members []*api.Source) ([]*api.Source, []*api.CaptureEpochGrant, error) {
	var shown []*api.Source
	var epochs []*api.CaptureEpochGrant
	for _, m := range members {
		// A raw source (§38.9) has nothing a relay could show, and a
		// policy of `always` would start it for nothing.
		if !showable(m) {
			continue
		}
		grant, managed, err := s.currentSourcePublishGrant(ctx, m, producerId)
		if err != nil {
			if managed && status.Code(err) == codes.PermissionDenied {
				continue
			}
			return nil, nil, err
		}
		if managed {
			if grant == nil {
				continue
			}
			epochs = append(epochs, grant)
		}
		shown = append(shown, m)
	}

	return shown, epochs, nil
}

// rewindMargin is what the recent window reaches back past the segment
// duration: the last part's upload and the commit (§39.4).
const rewindMargin = 15

// rewindSeconds is how much of each source the relay keeps (§39.4): the
// longest segment duration among the sources plus the commit's lag, so
// that what the relay holds reaches back past the end of the last lamina
// a Storage Node can serve. A source that never negotiated is sized as
// negotiation would size it, from the target lamina and its ceiling.
func rewindSeconds(sources []*api.Source, b Bounds) int64 {
	var longest int64
	for _, src := range sources {
		d := src.GetProfile().GetDurationSeconds()
		if d <= 0 {
			if br := src.GetProfile().GetMaxBitrate(); br > 0 {
				d = int64(float64(b.TargetLamina) / (float64(br) / 8))
			}
		}
		longest = max(longest, d)
	}
	if longest <= 0 {
		return 0
	}

	return longest + rewindMargin
}

// livePolicy is a producer's policy as the relay is told it: unspecified
// is `always`, since the case Shale is for wants the recent window of
// every camera there whether or not anyone watches (§39.3).
func livePolicy(v api.LivePolicy) api.LivePolicy {
	if v == api.LivePolicy_LIVE_POLICY_ON_DEMAND {
		return v
	}

	return api.LivePolicy_LIVE_POLICY_ALWAYS
}

// showable says whether a relay could show a source (§39.3): what its
// producer said at negotiation, since only the producer knows whether it
// reads the stream or takes raw frames it cannot show (§38.9).
func showable(src *api.Source) bool { return src.GetLiveCapable() }

// relayAlive says whether a relay still counts as up: `downAfter` is
// `relay_down_after` (§39.2). One that was down and heartbeats again is
// up from that heartbeat on: nothing else marks it.
func relayAlive(r *ent.Relay, now time.Time, downAfter time.Duration) bool {
	return r != nil && r.DateErased == nil && r.State == int32(api.HostState_HOST_STATE_ADOPTED) &&
		r.DateSeen != nil && now.Sub(*r.DateSeen) <= downAfter
}

// pickRelay is the live relay with the least attached bitrate among those
// whose labels match the set's site (§39.2).
func (s Core) pickRelay(ctx context.Context, set *api.Set, now time.Time) (*ent.Relay, error) {
	relays, err := s.ent(ctx).Relay.Query().Where(relay.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), relay.DateErasedIsNil()).All(ctx)
	if err != nil {
		return nil, err
	}
	var selector map[string]string
	if len(set.GetSite().GetId()) > 0 {
		if site, err := s.ent(ctx).Site.Get(ctx, mustId(set.GetSite().GetId()).Uuid()); err == nil {
			selector = site.RelaySelector
		}
	}
	// Attached bitrate per relay: the max_bitrate_total of every set whose
	// producers it carries.
	load := map[[16]byte]int64{}
	producers, err := s.ent(ctx).Producer.Query().Where(producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil()).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range producers {
		if isZero(p.RelayId) || isZero(p.SetId) {
			continue
		}
		if st, err := s.ent(ctx).Set.Get(ctx, p.SetId); err == nil {
			load[p.RelayId] += max(st.MaxBitrateTotal, 1)
		}
	}

	// The least loaded among those whose recent windows still fit their
	// budget (§39.5); one that is over it only when there is no other.
	var best *ent.Relay
	for _, full := range []bool{false, true} {
		for _, r := range relays {
			if !relayAlive(r, now, s.d.relayDownAfter()) || !matches(r.Labels, selector) || r.IngestAddress == "" {
				continue
			}
			if !full && rewindFull(r.Status) {
				continue
			}
			if best == nil || load[r.Id] < load[best.Id] {
				best = r
			}
		}
		if best != nil {
			break
		}
	}

	return best, nil
}

// rewindFull says a relay's recent windows are at their budget.
func rewindFull(st *api.RelayStatus) bool {
	return st != nil && st.GetRewindBudget() > 0 && st.GetRewindBytes() >= st.GetRewindBudget()
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

// whepEndpoints resolves a relay's WHEP listener for a caller: a name the
// relay reports is handed out as it is, since only an operator writes one
// (`relay.whep_advertise`), for a certificate a browser trusts, which names
// that and no IP (§39.4). An IP goes through the resolver like ingest.
func (s Core) whepEndpoints(r *ent.Relay, caller string, p *api.AddressParams) []*api.Endpoint {
	if r == nil {
		return nil
	}
	if ep := named(r.WhepAddress, s.d.Dev); ep != nil {
		return []*api.Endpoint{ep}
	}

	return s.relayEndpoints(r, r.WhepAddress, caller, p)
}

// liveSources is the Live answer for these sources of a set: the relay of
// the set's producer, a view token per source, and the WHEP URL (§39.4).
func (s Core) liveSources(ctx context.Context, f *frame.Frame, set *api.Set, sources []*api.Source) ([]*api.LiveSource, error) {
	return s.liveSourcesUntil(ctx, f, set, sources, time.Time{})
}

// liveSourcesUntil additionally bounds a job-scoped execution capability by its
// lease. Ordinary viewer calls retain the configured ViewTokenTTL.
func (s Core) liveSourcesUntil(ctx context.Context, f *frame.Frame, set *api.Set, sources []*api.Source, notAfter time.Time) ([]*api.LiveSource, error) {
	now := s.d.now()
	producers, err := s.ent(ctx).Producer.Query().
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
		if v, err := s.ent(ctx).Relay.Get(ctx, p.RelayId); err == nil && relayAlive(v, now, s.d.relayDownAfter()) {
			r = v
			break
		}
	}
	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	exp := now.Add(b.ViewTokenTTL)
	if !notAfter.IsZero() && notAfter.Before(exp) {
		exp = notAfter
	}
	if !exp.After(now) {
		return nil, failed("live capability lease expired")
	}

	var out []*api.LiveSource
	for _, src := range sources {
		app, managed, err := s.currentSourceApplication(ctx, src)
		if err != nil {
			if managed && (status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound || status.Code(err) == codes.PermissionDenied) {
				continue
			}
			return nil, err
		}
		if !showable(src) {
			if managed {
				continue
			}
			return nil, failed("nothing a relay could show; the producer takes raw frames or has not negotiated")
		}
		carrying := r
		epoch := ""
		if managed {
			epoch = app.GetCaptureEpoch()
			p, err := s.ent(ctx).Producer.Get(ctx, mustId(app.GetProducerId()).Uuid())
			if err != nil {
				return nil, err
			}
			carrying = nil
			if !isZero(p.RelayId) {
				if v, err := s.ent(ctx).Relay.Get(ctx, p.RelayId); err == nil && relayAlive(v, now, s.d.relayDownAfter()) {
					carrying = v
				}
			}
		}
		if carrying == nil {
			if managed {
				continue
			}
			return nil, failed("no relay carries this set's cameras yet")
		}
		eps := s.whepEndpoints(carrying, callerOf(ctx), address)
		if len(eps) == 0 {
			if managed {
				continue
			}
			return nil, failed("the relay has no WHEP address for this caller")
		}
		claims := api.TokenClaims_builder{
			Iat: timestamppb.New(now), Aud: carrying.Id[:], Op: api.TokenOp_TOKEN_OP_VIEW, Source: src.GetId(), CaptureEpoch: epoch,
		}
		claimsFor(ctx, f, &claims, now, exp)
		tok, err := s.d.Keys.Sign(ctx, claims.Build())
		if err != nil {
			return nil, err
		}
		out = append(out, api.LiveSource_builder{
			SourceId:     src.GetId(),
			Ordinal:      src.GetOrdinal(),
			RelayId:      carrying.Id[:],
			Endpoints:    eps,
			WhepUrl:      endpointURL(eps[0], "whep/"+mustId(src.GetId()).String(), nil),
			RecentUrl:    endpointURL(eps[0], "recent/"+mustId(src.GetId()).String(), nil),
			ViewToken:    tok,
			DateExpires:  claims.Exp,
			CaptureEpoch: epoch,
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
	if err := s.reachGone(ctx); err != nil {
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
	if len(vs) == 0 {
		return nil, failed("the managed source has no current ready live incarnation")
	}

	return api.SourceLiveResponse_builder{Source: vs[0]}.Build(), nil
}
