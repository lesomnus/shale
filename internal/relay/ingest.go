package relay

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ingest struct {
	api.UnimplementedRelayIngestServer
	r *Relay
}

// A stream can refresh its authenticated membership in place. All grants are
// signed by the CP; unsigned Reset/Data fields only select within those grants.
type attachment struct {
	r        *Relay
	stream   api.RelayIngest_AttachServer
	sendMu   sync.Mutex
	mu       sync.Mutex
	updating bool
	pending  map[pdid.Id]bool
	sources  map[pdid.Id]*source
	grants   map[pdid.Id]*api.CaptureEpochGrant
	actor    pdid.Id
	tenant   []byte
	claims   *api.TokenClaims
	// satellite is the satellite this stream is, from its feed token's
	// `feeder`; Nil for a producer's own stream (§39.3, §41.8). feeds are
	// the sources it feeds now, as its last Feed said.
	satellite pdid.Id
	feeds     map[pdid.Id]bool
	// membershipAt never moves backwards when an equivalent older grant is
	// replayed; otherwise that replay could reopen the membership shrink gate.
	membershipAt time.Time
}

func (a *attachment) send(msg *api.AttachResponse) {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if err := a.stream.Send(msg); err != nil {
		a.r.log.Debug("ingest send", "code", status.Code(err).String())
	}
}

func (a *attachment) control(id pdid.Id, start bool) {
	a.mu.Lock()
	if a.updating {
		a.pending[id] = start
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	// Serialize the state check with sending. A timer from the previous
	// incarnation must not send Stop after a newer Start already went out.
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	s := a.r.sources.get(id)
	s.mu.Lock()
	current := s.feeder == a
	allowed := !start && (!current || !s.started) || start && current && s.started && !s.stopped
	s.mu.Unlock()
	if !allowed {
		return
	}
	var msg *api.AttachResponse
	if start {
		msg = api.AttachResponse_builder{Start: api.AttachResponse_Start_builder{SourceId: id.Bytes()}.Build()}.Build()
	} else {
		msg = api.AttachResponse_builder{Stop: api.AttachResponse_Stop_builder{SourceId: id.Bytes()}.Build()}.Build()
	}
	if err := a.stream.Send(msg); err != nil {
		a.r.log.Debug("ingest control", "code", status.Code(err).String())
	}
}
func (a *attachment) start(id pdid.Id) { a.control(id, true) }
func (a *attachment) stop(id pdid.Id)  { a.control(id, false) }

func (a *attachment) welcome() {
	accepted := make([][]byte, 0, len(a.sources))
	for id := range a.sources {
		accepted = append(accepted, id.Bytes())
	}
	a.send(api.AttachResponse_builder{Welcome: api.AttachResponse_Welcome_builder{Sources: accepted}.Build()}.Build())
	a.mu.Lock()
	pending := a.pending
	a.pending = map[pdid.Id]bool{}
	a.updating = false
	a.mu.Unlock()
	for id, start := range pending {
		a.control(id, start)
	}
}

func validEpoch(v string) bool {
	if len(v) > 128 {
		return false
	}
	for _, b := range []byte(v) {
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.' || b == ':' || b == '/' {
			continue
		}
		return false
	}
	return true
}

// authorize returns false for a delayed valid Hello, keeping current sources
// and peers intact. Epoch sequence is the authority for media. Signed Iat is
// only an anti-replay fence for membership; equal-Iat shrink needs a new token.
func (a *attachment) authorize(claims *api.TokenClaims) (bool, error) {
	actor, err := pdid.From(claims.GetActor())
	if err != nil || actor.Domain() != 22 || claims.GetIat() == nil || claims.GetIat().CheckValid() != nil || claims.GetExp() == nil || claims.GetExp().CheckValid() != nil || !time.Now().Before(claims.GetExp().AsTime()) || len(claims.GetSources()) > 4096 {
		return false, status.Error(codes.PermissionDenied, "invalid publication identity or membership")
	}
	satellite, err := feederOf(claims)
	if err != nil {
		return false, err
	}
	if a.claims != nil && (actor != a.actor || !bytes.Equal(claims.GetActorTenant(), a.tenant) || satellite != a.satellite) {
		return false, status.Error(codes.PermissionDenied, "publication identity cannot change on an open stream")
	}
	next := map[pdid.Id]*source{}
	for _, raw := range claims.GetSources() {
		id, e := pdid.From(raw)
		if e != nil || id.Domain() != 8 {
			return false, status.Error(codes.PermissionDenied, "invalid publication source")
		}
		if next[id] != nil {
			return false, status.Error(codes.PermissionDenied, "duplicate publication source")
		}
		next[id] = a.r.sources.get(id)
	}
	grants := map[pdid.Id]*api.CaptureEpochGrant{}
	for _, g := range claims.GetCaptureEpochs() {
		id, e := pdid.From(g.GetSourceId())
		if g == nil || e != nil || next[id] == nil || grants[id] != nil || !validEpoch(g.GetCaptureEpoch()) || g.GetSequence() < 0 || g.GetCaptureEpoch() != "" && g.GetSequence() == 0 {
			return false, status.Error(codes.PermissionDenied, "invalid publication capture grant")
		}
		grants[id] = g
	}
	always := claims.GetLive() != api.LivePolicy_LIVE_POLICY_ON_DEMAND
	window := time.Duration(max(0, min(claims.GetRewindSeconds(), int64(24*time.Hour/time.Second)))) * time.Second
	// Lock the union in a stable order: a concurrent Hello, Reset or removal
	// cannot invalidate one grant between the preflight and a partial commit.
	// No source state changes until every grant and membership fence passes.
	all := make(map[pdid.Id]*source, len(a.sources)+len(next))
	for id, s := range a.sources {
		all[id] = s
	}
	for id, s := range next {
		all[id] = s
	}
	ids := make([]pdid.Id, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(x, y pdid.Id) int { return bytes.Compare(x.Bytes(), y.Bytes()) })
	var after []func()
	accepted := func() bool {
		for _, id := range ids {
			all[id].mu.Lock()
		}
		defer func() {
			for i := len(ids) - 1; i >= 0; i-- {
				all[ids[i]].mu.Unlock()
			}
		}()
		higher := false
		for id, s := range next {
			if s.checkGrant(grants[id]) != nil {
				return false
			}
			if g := grants[id]; g != nil && g.GetSequence() > s.epochSequence {
				higher = true
			}
		}
		if a.claims != nil {
			changed, shrink := len(a.sources) != len(next), false
			for id := range a.sources {
				if next[id] == nil {
					changed, shrink = true, true
				}
			}
			for id := range next {
				if a.sources[id] == nil {
					changed = true
				}
			}
			if changed && !claims.GetIat().AsTime().After(a.membershipAt) && (shrink || !higher) {
				return false
			}
		}
		a.mu.Lock()
		a.updating = true
		a.mu.Unlock()
		for id, s := range next {
			after = append(after, s.authorizeLocked(a, !satellite.IsZero(), grants[id], always, window))
		}
		for id, s := range a.sources {
			if next[id] == nil {
				if action := s.removePublisherLocked(a); action != nil {
					after = append(after, action)
				}
				after = append(after, func() { a.stop(id) })
			}
		}
		a.sources, a.grants, a.actor, a.tenant, a.claims = next, grants, actor, append([]byte(nil), claims.GetActorTenant()...), proto.Clone(claims).(*api.TokenClaims)
		a.satellite = satellite
		if claims.GetIat().AsTime().After(a.membershipAt) {
			a.membershipAt = claims.GetIat().AsTime()
		}
		return true
	}()
	if !accepted {
		return false, nil
	}
	for _, action := range after {
		action()
	}
	// A satellite's stream feeds what its last Feed named, of what its
	// token names now.
	if !a.satellite.IsZero() {
		a.feed(a.wanted())
	}
	a.welcome()
	return true, nil
}

// feederOf is the satellite a token is for: a feed token names one, a
// publish token none (§39.3, §41.8).
func feederOf(claims *api.TokenClaims) (pdid.Id, error) {
	switch claims.GetOp() {
	case api.TokenOp_TOKEN_OP_FEED:
		id, err := pdid.From(claims.GetFeeder())
		if err != nil || id.Domain() != DomSatellite {
			return pdid.Nil, status.Error(codes.PermissionDenied, "a feed capability names no satellite")
		}
		return id, nil
	case api.TokenOp_TOKEN_OP_PUBLISH:
		if len(claims.GetFeeder()) > 0 {
			return pdid.Nil, status.Error(codes.PermissionDenied, "a publish capability names no satellite")
		}
		return pdid.Nil, nil
	}

	return pdid.Nil, status.Error(codes.PermissionDenied, "not a publication capability")
}

// wanted is what a satellite's stream asked to feed, as a set.
func (a *attachment) wanted() map[pdid.Id]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[pdid.Id]bool, len(a.feeds))
	for id := range a.feeds {
		out[id] = true
	}

	return out
}

// feed takes a satellite's Feed (§41.8): it feeds these of its token's
// sources now and no others. A source it begins to feed is its from now
// on, over the producer's own stream; one it stops feeding goes back to
// the next publisher, the producer's stream when it is attached.
func (a *attachment) feed(ids map[pdid.Id]bool) {
	a.mu.Lock()
	a.feeds = ids
	a.mu.Unlock()
	for id, s := range a.sources {
		if action := s.feeds(a, ids[id]); action != nil {
			action()
		}
	}
}

func (g *ingest) Attach(stream api.RelayIngest_AttachServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHello() == nil {
		return status.Error(codes.InvalidArgument, "the first message is Hello")
	}
	claims, err := g.r.verify(first.GetHello().GetPublishToken(), api.TokenOp_TOKEN_OP_PUBLISH, api.TokenOp_TOKEN_OP_FEED)
	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid publication capability")
	}
	// A feed token is good only from the satellite it names, as its
	// certificate says (§39.7).
	if sat, err := feederOf(claims); err != nil {
		return err
	} else if !sat.IsZero() {
		if err := g.r.peerIs(stream.Context(), sat); err != nil {
			g.r.log.Info("ingest refused: a feed capability from another host", "satellite", sat.String(), "err", err.Error())
			return status.Error(codes.PermissionDenied, "a feed capability from another host than the satellite it names")
		}
	}
	a := &attachment{r: g.r, stream: stream, sources: map[pdid.Id]*source{}, grants: map[pdid.Id]*api.CaptureEpochGrant{}, pending: map[pdid.Id]bool{}, updating: true, feeds: map[pdid.Id]bool{}}
	accepted, err := a.authorize(claims)
	if err != nil {
		return err
	}
	if !accepted {
		return status.Error(codes.PermissionDenied, "stale publication capability")
	}
	// A producer's own stream is an attached producer; a satellite's for
	// it is not another (its own stream stays attached, §39.3).
	counted := a.satellite.IsZero()
	if counted {
		g.r.sources.attached(1)
	}
	defer func() {
		for _, s := range a.sources {
			s.detach(a)
		}
		if counted {
			g.r.sources.attached(-1)
		}
	}()
	// Recv is unblocked by gRPC when this handler returns at expiry, including
	// an idle zero-source publisher. At most one receive is outstanding.
	// at is t3 (§39.3), taken as the message comes off the stream rather
	// than when this loop gets to it.
	type received struct {
		msg *api.AttachRequest
		err error
		at  time.Time
	}
	input := make(chan received)
	go func() {
		for {
			msg, err := stream.Recv()
			select {
			case input <- received{msg, err, time.Now()}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	expiry := time.NewTimer(time.Until(a.claims.GetExp().AsTime()))
	defer expiry.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-expiry.C:
			return status.Error(codes.PermissionDenied, "publication capability expired")
		case received := <-input:
			if errors.Is(received.err, io.EOF) {
				return nil
			}
			if received.err != nil {
				return received.err
			}
			msg := received.msg
			if hello := msg.GetHello(); hello != nil {
				fresh, err := g.r.verify(hello.GetPublishToken(), api.TokenOp_TOKEN_OP_PUBLISH, api.TokenOp_TOKEN_OP_FEED)
				if err != nil {
					return status.Error(codes.Unauthenticated, "invalid publication capability")
				}
				ok, err := a.authorize(fresh)
				if err != nil {
					return err
				}
				if ok {
					if !expiry.Stop() {
						select {
						case <-expiry.C:
						default:
						}
					}
					expiry.Reset(time.Until(a.claims.GetExp().AsTime()))
				} else {
					a.welcome()
				}
				continue
			}
			if !time.Now().Before(a.claims.GetExp().AsTime()) {
				return status.Error(codes.PermissionDenied, "publication capability expired")
			}
			if feed := msg.GetFeed(); feed != nil {
				// A satellite's alone; a producer's stream feeds what its
				// token names.
				if a.satellite.IsZero() {
					continue
				}
				ids := map[pdid.Id]bool{}
				for _, raw := range feed.GetSources() {
					if id, err := pdid.From(raw); err == nil && a.sources[id] != nil {
						ids[id] = true
					}
				}
				a.feed(ids)
				continue
			}
			if reset := msg.GetReset(); reset != nil {
				id, err := pdid.From(reset.GetSourceId())
				if err == nil && validEpoch(reset.GetCaptureEpoch()) {
					if s := a.sources[id]; s != nil {
						s.resetEpoch(a, a.grants[id], reset.GetCaptureEpoch(), reset.GetStopped())
					}
				}
				continue
			}
			if d := msg.GetData(); d != nil {
				id, err := pdid.From(d.GetSourceId())
				if err != nil {
					continue
				}
				s := a.sources[id]
				if s == nil {
					continue
				}
				grant := a.grants[id]
				s.mu.Lock()
				allowed := s.checkGrant(grant) == nil
				s.mu.Unlock()
				if !allowed || grant != nil && (grant.GetCaptureEpoch() == "" || grant.GetCaptureEpoch() != d.GetCaptureEpoch()) {
					continue
				}
				// The host whose link it came over: the producer, or the
				// satellite that feeds its sources.
				host := a.actor
				if !a.satellite.IsZero() {
					host = a.satellite
				}
				st := stamps{arrived: received.at, producer: host.String()}
				if d.HasReadAt() {
					st.read = d.GetReadAt().AsTime()
				}
				if d.HasSentAt() {
					st.sent = d.GetSentAt().AsTime()
				}
				s.feedTimed(a, d.GetCaptureEpoch(), d.GetPayload(), st)
			}
		}
	}
}
