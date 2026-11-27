package core

import (
	"bytes"
	"context"
	"crypto/x509"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/satellite"
	"github.com/lesomnus/shale/internal/pki"
)

// Satellites (§41): the host producers upload through. It joins and is
// adopted like a relay, heartbeats its memory, and is told the producers
// assigned to it, which are the only callers it serves; a producer is
// assigned one as it is a relay (§41.2).

type coreSatellite struct {
	Core
	api.SatelliteServiceServer
}

func (s Core) Satellite() api.SatelliteServiceServer {
	return coreSatellite{s, s.Next().Satellite()}
}

// validateSatelliteAddresses checks what a satellite says about where it
// listens, as a node's and a relay's are checked.
func validateSatelliteAddresses(ifs []*api.HostInterface, upload string) error {
	if err := validateInterfaces(ifs); err != nil {
		return err
	}

	return validateAddr("upload_address", upload)
}

// satelliteNames are the names its certificate carries: those the address
// policy hands out for its upload listener, as a node's data plane.
func (s Core) satelliteNames(ctx context.Context, id pdid.Id, alias string, ifs []*api.HostInterface, upload string) pki.Names {
	_, _, _, address, _ := s.policies(ctx)
	dns, ips := NamesFor(NodeAddresses{Id: id, Alias: alias, Interfaces: ifs, DataAddress: upload}, address)

	return pki.Names{DNS: dns, IPs: ips}
}

func (s coreSatellite) Join(ctx context.Context, req *api.SatelliteJoinRequest) (*api.SatelliteJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	if err := validateSatelliteAddresses(req.GetInterfaces(), req.GetUploadAddress()); err != nil {
		return nil, err
	}
	from, _ := peerAddr(ctx)

	return polled(s.Core, "satellite", func() (*api.SatelliteJoinResponse, error) {
		return s.join(ctx, req, from)
	}, func(a *api.JoinAnswer) *api.SatelliteJoinResponse {
		return api.SatelliteJoinResponse_builder{Answer: a}.Build()
	})
}

// join is one poll of Join, as a producer's is (host.go).
func (s coreSatellite) join(ctx context.Context, req *api.SatelliteJoinRequest, from string) (*api.SatelliteJoinResponse, error) {
	hj := req.GetHost()
	row, err := s.ent(ctx).Satellite.Query().
		Where(satellite.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), satellite.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	if s.d.joinHeld != nil {
		s.d.joinHeld(ctx)
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial, version: timestamppb.New(row.DateUpdated)}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle(ctx)}
	resp := api.SatelliteJoinResponse_builder{}
	auto := s.autoAdopt(DomSatellite, hj, from)

	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomSatellite)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.ent(ctx).Satellite.Query().Where(satellite.AliasEQ(a), satellite.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		v, err := s.own(ctx).Satellite().Add(ctx, api.SatelliteAddRequest_builder{
			Id:            id.Bytes(),
			Alias:         alias,
			HardwareId:    strings.ToLower(hj.GetHardwareId()),
			Hostname:      hj.GetHostname(),
			State:         api.HostState_HOST_STATE_PENDING,
			Join:          s.joinRecord(hj, from, false),
			Interfaces:    req.GetInterfaces(),
			UploadAddress: req.GetUploadAddress(),
			Version:       hj.GetVersion(),
		}.Build())
		if err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale satellite adopt " + alias
		if auto {
			v, err = s.adoptSatellite(ctx, s.own(ctx), v, "")
			if err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = v.GetJoin().GetCertificate()
			answer.Message = ""
		}
	case decPending:
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
		answer.Rejoin = js.join.GetRejoin()
		v, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
			Ref:           api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
			Join:          s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:      z.Ptr(hj.GetHostname()),
			Interfaces:    req.GetInterfaces(),
			UploadAddress: z.Ptr(req.GetUploadAddress()),
			Version:       z.Ptr(hj.GetVersion()),
			DateUpdated:   js.version,
		}.Build())
		if err != nil {
			return nil, pollWrote(err, &answer)
		}
		if auto {
			v, err = s.adoptSatellite(ctx, s.own(ctx), v, "")
			if err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = v.GetJoin().GetCertificate()
			answer.Message = ""
		}
	case decIssued:
		answer.State = api.HostState_HOST_STATE_ADOPTED
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Certificate = js.join.GetCertificate()
	case decRejoin:
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Rejoin = true
		answer.Message = "known host, new key: waiting for adoption"
		rec := s.joinRecord(hj, from, true)
		if s.d.Readopt == "auto" || auto {
			pem, serial, exp, err := s.issue(ctx, rec, js.id, s.satelliteNames(ctx, js.id, row.Alias, req.GetInterfaces(), req.GetUploadAddress()))
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
				Ref:             api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
				Join:            rec,
				CertSerial:      z.Ptr(serial),
				DateCertExpires: timestamppb.New(exp),
				Interfaces:      req.GetInterfaces(),
				UploadAddress:   z.Ptr(req.GetUploadAddress()),
				DateUpdated:     js.version,
			}.Build()); err != nil {
				return nil, pollWrote(err, &answer)
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
			answer.Message = ""
		} else {
			if _, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
				Ref:         api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
				Join:        rec,
				DateUpdated: js.version,
			}.Build()); err != nil {
				return nil, pollWrote(err, &answer)
			}
		}
	}

	if answer.State == api.HostState_HOST_STATE_ADOPTED {
		keys, err := s.d.Keys.Public(ctx)
		if err != nil {
			return nil, err
		}
		resp.Keys = keys
	}
	resp.Answer = answer.Build()

	return resp.Build(), nil
}

func (s Core) adoptSatellite(ctx context.Context, srv api.Server, v *api.Satellite, alias string) (*api.Satellite, error) {
	rec := v.GetJoin()
	if rec == nil || len(rec.GetCsr()) == 0 {
		return nil, failed("this satellite has no pending join request")
	}
	id := mustId(v.GetId())
	if alias == "" {
		alias = v.GetAlias()
	}
	pem, serial, exp, err := s.issue(ctx, rec, id, s.satelliteNames(ctx, id, alias, v.GetInterfaces(), v.GetUploadAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	rec.SetRejoin(false)
	now := s.d.now()

	return srv.Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
		Ref:              api.SatelliteRef_builder{Id: v.GetId()}.Build(),
		Alias:            z.Ptr(alias),
		State:            z.Ptr(api.HostState_HOST_STATE_ADOPTED),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateAdopted:      timestamppb.New(now),
		DateCertExpires:  timestamppb.New(exp),
		DateSeen:         timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build())
}

func (s coreSatellite) Adopt(ctx context.Context, req *api.SatelliteAdoptRequest) (*api.Satellite, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	v, err := s.SatelliteServiceServer.Get(ctx, api.SatelliteGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}

	return s.adoptSatellite(ctx, s.Next(), v, strings.TrimSpace(req.GetAlias()))
}

// Patch is an operator's for the alias, the name, the description and the
// labels its sites select it by (§41.2); the rest of the row is the
// system's, written by the join, adoption and the heartbeat.
func (s coreSatellite) Patch(ctx context.Context, req *api.SatellitePatchRequest) (*api.Satellite, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	if req.HasHardwareId() || req.HasHostname() || req.HasState() || req.HasCertSerial() ||
		req.HasJoin() || req.HasJoinNull() || req.HasDateAdopted() || req.HasDateAdoptedNull() || req.HasDateSeen() || req.HasDateSeenNull() ||
		req.HasDateCertExpires() || req.HasDateCertExpiresNull() || len(req.GetKeyIds()) > 0 || req.HasCaHash() || req.HasVersion() ||
		req.HasUploadAddress() || req.HasStatus() || req.HasStatusNull() || len(req.GetInterfaces()) > 0 {
		return nil, status.Error(codes.PermissionDenied, "a satellite's alias, name, desc and labels are an operator's to patch; the rest of the row is the system's")
	}

	return s.SatelliteServiceServer.Patch(ctx, req)
}

func (s coreSatellite) RenewCertificate(ctx context.Context, req *api.SatelliteRenewCertificateRequest) (*api.SatelliteRenewCertificateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomSatellite {
		return nil, status.Error(codes.PermissionDenied, "only a satellite renews its own certificate")
	}
	if _, err := x509.ParseCertificateRequest(req.GetCsr()); err != nil {
		return nil, invalid("csr", err.Error())
	}
	v, err := s.own(ctx).Satellite().Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	rec := v.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	pem, serial, exp, err := s.issue(ctx, rec, f.Actor, s.satelliteNames(ctx, f.Actor, v.GetAlias(), v.GetInterfaces(), v.GetUploadAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
		Ref:              api.SatelliteRef_builder{Id: v.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.SatelliteRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle(ctx)}.Build(), nil
}

func (s coreSatellite) Heartbeat(ctx context.Context, req *api.SatelliteHeartbeatRequest) (*api.SatelliteHeartbeatResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomSatellite {
		return nil, status.Error(codes.PermissionDenied, "only a satellite heartbeats")
	}
	v, err := s.own(ctx).Satellite().Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := validateSatelliteAddresses(req.GetInterfaces(), req.GetUploadAddress()); err != nil {
		return nil, err
	}
	now := s.d.now()
	st := req.GetStatus()
	if st == nil {
		st = &api.SatelliteStatus{}
	}
	st.SetDateReported(timestamppb.New(now))
	patch := api.SatellitePatchRequest_builder{
		Ref:              api.SatelliteRef_builder{Id: v.GetId()}.Build(),
		DateSeen:         timestamppb.New(now),
		KeyIds:           req.GetKeyIds(),
		CaHash:           z.Ptr(req.GetCaHash()),
		Version:          z.Ptr(req.GetVersion()),
		Status:           st,
		DateUpdatedForce: z.Ptr(true),
	}
	if len(req.GetInterfaces()) > 0 {
		patch.Interfaces = req.GetInterfaces()
	}
	if req.GetUploadAddress() != "" {
		patch.UploadAddress = z.Ptr(req.GetUploadAddress())
	}
	if _, err := s.own(ctx).Satellite().Patch(ctx, patch.Build()); err != nil {
		return nil, err
	}
	// A new incarnation is a satellite that restarted: what the last one
	// accepted and did not deliver is gone (§41.9).
	s.satelliteRestarted(ctx, f.Actor, v.GetStatus(), st)

	producers, err := s.satelliteProducers(ctx, f.Actor)
	if err != nil {
		return nil, err
	}
	resp := api.SatelliteHeartbeatResponse_builder{Producers: producers}
	if exp := v.GetDateCertExpires(); exp != nil && now.After(renewAt(v.GetDateAdopted(), exp)) {
		resp.RenewCertificate = true
	}
	if !resp.RenewCertificate && s.outdated(ctx, NodeAddresses{Id: f.Actor, Alias: v.GetAlias()}, v.GetJoin().GetCertificate()) {
		resp.RenewCertificate = true
	}
	if !resp.RenewCertificate && s.renewForCa(ctx, req.GetCaHash(), v.GetJoin().GetCertificate()) {
		resp.RenewCertificate = true
	}

	return resp.Build(), nil
}

// satelliteProducers are the producers a satellite serves (§41.3): the
// adopted ones assigned to it, each with the serial of the certificate its
// row records, which is the one it may present.
func (s Core) satelliteProducers(ctx context.Context, id pdid.Id) ([]*api.SatelliteProducer, error) {
	ps, err := s.ent(ctx).Producer.Query().
		Where(producer.SatelliteIdEQ(id.Uuid()), producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*api.SatelliteProducer, 0, len(ps))
	now := s.d.now()
	for _, p := range ps {
		v := api.SatelliteProducer_builder{ProducerId: pdid.Id(p.Id).Bytes(), CertSerial: p.CertSerial}
		ra, err := s.satelliteFeed(ctx, id, p, now)
		if err != nil {
			s.d.log().Warn("satellite feed", "producer", p.Alias, "err", err.Error())
		}
		v.Relay = ra
		out = append(out, v.Build())
	}

	return out, nil
}

// SatelliteFeedTokenTTL is how long a satellite's feed token lives, at most
// (§41.8): every heartbeat brings a fresh one, so it need not outlive a
// satellite that stops being told.
const SatelliteFeedTokenTTL = 10 * time.Minute

// satelliteFeed is the relay a satellite feeds a producer's live to, with a
// feed token for the sources the producer hands it (§41.8): those of its
// last heartbeat's report that its publish token names, when the report
// names this satellite. Nil when it hands none, or its relay is down or
// predates satellites, which then never see one: the producer's own stream
// feeds every source, as before.
func (s Core) satelliteFeed(ctx context.Context, sat pdid.Id, p *ent.Producer, now time.Time) (*api.RelayAssignment, error) {
	up := p.Status.GetUpload()
	if len(up.GetLiveSources()) == 0 || !bytes.Equal(up.GetSatelliteId(), sat.Bytes()) || isZero(p.RelayId) || isZero(p.SetId) {
		return nil, nil
	}
	r, err := s.ent(ctx).Relay.Get(ctx, p.RelayId)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !relayAlive(r, now, s.d.relayDownAfter()) || !r.Status.GetSatelliteFeeders() || r.IngestAddress == "" {
		return nil, nil
	}
	members, err := s.ownMembers(ctx, p.SetId[:])
	if err != nil {
		return nil, err
	}
	producerId := pdid.Id(p.Id)
	shown, grants, err := s.publishable(ctx, producerId, members)
	if err != nil {
		return nil, err
	}
	handed := map[string]bool{}
	for _, v := range up.GetLiveSources() {
		handed[string(v)] = true
	}
	var sources [][]byte
	var epochs []*api.CaptureEpochGrant
	for _, m := range shown {
		if handed[string(m.GetId())] {
			sources = append(sources, m.GetId())
		}
	}
	if len(sources) == 0 {
		return nil, nil
	}
	for _, g := range grants {
		if handed[string(g.GetSourceId())] {
			epochs = append(epochs, g)
		}
	}
	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	live := livePolicy(api.LivePolicy(p.Live))
	exp := now.Add(min(b.PublishTokenTTL, SatelliteFeedTokenTTL))
	// The producer's own token in all but its operation, the sources it
	// hands over and the satellite it names: the same actor, policy and
	// recent window, so the relay keeps the source as it was whoever feeds.
	tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
		Exp: timestamppb.New(exp), Iat: timestamppb.New(now),
		Aud: r.Id[:], Op: api.TokenOp_TOKEN_OP_FEED, Sources: sources, Actor: producerId.Bytes(), ActorTenant: p.TenantId[:], CaptureEpochs: epochs,
		Live: live, RewindSeconds: rewindSeconds(shown, b), Feeder: sat.Bytes(),
	}.Build())
	if err != nil {
		return nil, err
	}

	return api.RelayAssignment_builder{
		RelayId:      r.Id[:],
		Endpoints:    s.relayEndpoints(r, r.IngestAddress, callerOf(ctx), address),
		PublishToken: tok,
		DateExpires:  timestamppb.New(exp),
		Live:         live,
	}.Build(), nil
}

// ownMembers are a set's sources, read past the wall: a satellite asks for
// its producers' feeds, and holds no tenant's rows itself (§41.7).
func (s Core) ownMembers(ctx context.Context, setId []byte) ([]*api.Source, error) {
	var out []*api.Source
	after := ""
	for {
		vs, err := s.own(ctx).Source().List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: setId}.Build()}.Build()},
			Size:    500,
			After:   after,
		}.Build())
		if err != nil {
			return nil, err
		}
		out = append(out, vs.GetItems()...)
		if vs.GetNext() == "" {
			return out, nil
		}
		after = vs.GetNext()
	}
}

// Route is a node's data plane for the satellite that asks: the endpoints
// the resolver gives for the network the satellite calls from, as a
// producer there would be given (§34.10, §41.3). An erased node, or one not
// adopted, has none.
func (s coreSatellite) Route(ctx context.Context, req *api.SatelliteRouteRequest) (*api.SatelliteRouteResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomSatellite {
		return nil, status.Error(codes.PermissionDenied, "only a satellite asks for a route")
	}
	id, err := pdid.From(req.GetNodeId())
	if err != nil || id.Domain() != DomNode {
		return nil, invalid("node_id", "a node's id")
	}
	n, err := s.ent(ctx).Node.Get(ctx, id.Uuid())
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, status.Error(codes.NotFound, "no such node")
		}
		return nil, err
	}
	if n.DateErased != nil || n.State != int32(api.HostState_HOST_STATE_ADOPTED) {
		return nil, status.Error(codes.NotFound, "no such node")
	}
	_, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	eps := s.endpoints(n, callerOf(ctx), address)
	if len(eps) == 0 {
		return nil, status.Error(codes.Unavailable, "the node has no data plane address for this satellite")
	}

	return api.SatelliteRouteResponse_builder{
		Endpoints:   eps,
		DateExpires: timestamppb.New(s.d.now().Add(SatelliteRouteTTL)),
	}.Build(), nil
}

func (s coreSatellite) Assign(ctx context.Context, req *api.SatelliteAssignRequest) (*api.Satellite, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	v, err := s.SatelliteServiceServer.Get(ctx, api.SatelliteGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	if v.GetState() != api.HostState_HOST_STATE_ADOPTED {
		return nil, failed("satellite %s is not adopted", v.GetAlias())
	}
	p, err := s.own(ctx).Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: req.GetProducer()}.Build())
	if err != nil {
		return nil, err
	}
	if _, err := s.own(ctx).Producer().Patch(ctx, api.ProducerPatchRequest_builder{
		Ref:              api.ProducerRef_builder{Id: p.GetId()}.Build(),
		Satellite:        api.SatelliteRef_builder{Id: v.GetId()}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return v, nil
}

// satelliteAssignment is the satellite a producer uploads through, when it
// says it wants one (`upload_via`, §41.5): the one it has while that one is
// alive, else one whose labels match the set's site, with room before
// without, the least loaded first (§41.2). Nil when it does not want one,
// or no satellite is available, which is every cluster that runs none: a
// producer then uploads to the nodes directly, as it always has. A
// producer that stops wanting one is let go by the one it had, which then
// no longer serves it.
func (s Core) satelliteAssignment(ctx context.Context, producerId pdid.Id, set *api.Set, wants bool) (*api.SatelliteAssignment, error) {
	now := s.d.now()
	p, err := s.ent(ctx).Producer.Get(ctx, producerId.Uuid())
	if err != nil {
		return nil, err
	}
	if !wants {
		if !isZero(p.SatelliteId) {
			if _, err := s.own(ctx).Producer().Patch(ctx, api.ProducerPatchRequest_builder{
				Ref:              api.ProducerRef_builder{Id: producerId.Bytes()}.Build(),
				SatelliteNull:    z.Ptr(true),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			s.d.log().Info("satellite unassigned: the producer uploads directly", "producer", p.Alias)
		}

		return nil, nil
	}
	var chosen *ent.Satellite
	if !isZero(p.SatelliteId) {
		if v, err := s.ent(ctx).Satellite.Get(ctx, p.SatelliteId); err == nil && satelliteAlive(v, now, s.d.satelliteDownAfter()) && v.UploadAddress != "" {
			chosen = v
		}
	}
	if chosen == nil {
		chosen, err = s.pickSatellite(ctx, set, now)
		if err != nil {
			return nil, err
		}
		if chosen == nil {
			if !isZero(p.SatelliteId) {
				// The one it had is gone and there is no other: the edge is
				// let go, so that satellite no longer serves it.
				if _, err := s.own(ctx).Producer().Patch(ctx, api.ProducerPatchRequest_builder{
					Ref:              api.ProducerRef_builder{Id: producerId.Bytes()}.Build(),
					SatelliteNull:    z.Ptr(true),
					DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return nil, err
				}
				s.d.log().Info("satellite unassigned: none is available", "producer", p.Alias)
			}

			return nil, nil
		}
		if _, err := s.own(ctx).Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref:              api.ProducerRef_builder{Id: producerId.Bytes()}.Build(),
			Satellite:        api.SatelliteRef_builder{Id: chosen.Id[:]}.Build(),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		s.d.log().Info("satellite assigned", "producer", p.Alias, "satellite", chosen.Alias)
	}

	_, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	eps := s.satelliteEndpoints(chosen, callerOf(ctx), address)
	if len(eps) == 0 {
		return nil, nil
	}

	return api.SatelliteAssignment_builder{
		SatelliteId: chosen.Id[:],
		Endpoints:   eps,
		DateExpires: timestamppb.New(now.Add(SatelliteAssignmentTTL)),
		// What its last heartbeat said: a satellite of an earlier release
		// says nothing, and is handed no segment under `accepted` (§41.9).
		Accept: chosen.Status.GetAccept(),
	}.Build(), nil
}

// satelliteAlive says whether a satellite counts as up: adopted, not
// erased, and heard within `satellite_down_after` (§41.2).
func satelliteAlive(v *ent.Satellite, now time.Time, downAfter time.Duration) bool {
	return v != nil && v.DateErased == nil && v.State == int32(api.HostState_HOST_STATE_ADOPTED) &&
		v.DateSeen != nil && now.Sub(*v.DateSeen) <= downAfter
}

// satelliteFull says a satellite's in-flight pool had no room at its last
// heartbeat (§41.4).
func satelliteFull(st *api.SatelliteStatus) bool {
	return st != nil && st.GetInFlightBudget() > 0 && st.GetInFlightBytes() >= st.GetInFlightBudget()
}

// pickSatellite is the live satellite with the least attached bitrate
// among those whose labels match the set's site, preferring those whose
// in-flight pool has room (§41.2).
func (s Core) pickSatellite(ctx context.Context, set *api.Set, now time.Time) (*ent.Satellite, error) {
	sats, err := s.ent(ctx).Satellite.Query().Where(satellite.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), satellite.DateErasedIsNil()).All(ctx)
	if err != nil || len(sats) == 0 {
		return nil, err
	}
	var selector map[string]string
	if len(set.GetSite().GetId()) > 0 {
		if site, err := s.ent(ctx).Site.Get(ctx, mustId(set.GetSite().GetId()).Uuid()); err == nil {
			selector = site.SatelliteSelector
		}
	}
	// Attached bitrate per satellite: the max_bitrate_total of every set
	// whose producers it is assigned.
	load := map[[16]byte]int64{}
	producers, err := s.ent(ctx).Producer.Query().
		Where(producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil(), producer.SatelliteIdNotNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range producers {
		if isZero(p.SatelliteId) || isZero(p.SetId) {
			continue
		}
		if st, err := s.ent(ctx).Set.Get(ctx, p.SetId); err == nil {
			load[p.SatelliteId] += max(st.MaxBitrateTotal, 1)
		}
	}

	var best *ent.Satellite
	for _, full := range []bool{false, true} {
		for _, v := range sats {
			if !satelliteAlive(v, now, s.d.satelliteDownAfter()) || !matches(v.Labels, selector) || v.UploadAddress == "" {
				continue
			}
			if !full && satelliteFull(v.Status) {
				continue
			}
			if best == nil || load[v.Id] < load[best.Id] {
				best = v
			}
		}
		if best != nil {
			break
		}
	}

	return best, nil
}

// satelliteEndpoints resolves a satellite's upload listener for a caller,
// like a node's data plane (§34.10): a name it reports is handed out as it
// is, an IP or every interface goes through the resolver.
func (s Core) satelliteEndpoints(v *ent.Satellite, caller string, p *api.AddressParams) []*api.Endpoint {
	if v == nil || v.UploadAddress == "" {
		return nil
	}

	return s.nodeEndpoints(NodeAddresses{Id: pdid.Id(v.Id), Alias: v.Alias, Interfaces: v.Interfaces, DataAddress: v.UploadAddress, Dev: s.d.Dev}, caller, p)
}

// ---- retain: accepted ---------------------------------------------------

// Accept records the satellite as the holder of a lamina's bytes under
// `retain: accepted` (§41.9), which it asks before it tells the producer it
// holds any: from then on a loss of them is the satellite's. Only for a
// PENDING lamina of a producer assigned to it.
func (s coreSatellite) Accept(ctx context.Context, req *api.SatelliteAcceptRequest) (*api.SatelliteAcceptResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomSatellite {
		return nil, status.Error(codes.PermissionDenied, "only a satellite takes a lamina's bytes")
	}
	id, err := pdid.From(req.GetLaminaId())
	if err != nil || id.Domain() != DomLamina {
		return nil, invalid("lamina_id", "a lamina's id")
	}
	o, err := s.ent(ctx).Lamina.Get(ctx, id.Uuid())
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, status.Error(codes.NotFound, "no such lamina")
		}
		return nil, err
	}
	if o.State != int32(api.LaminaState_LAMINA_STATE_PENDING) {
		return nil, failed("lamina is %s, not pending", api.LaminaState(o.State))
	}
	who, err := s.satelliteActsFor(ctx, f.Actor, o.SetId[:])
	if err != nil {
		return nil, err
	}
	if _, err := s.own(ctx).Lamina().Patch(ctx, api.LaminaPatchRequest_builder{
		Ref:              api.LaminaRef_builder{Id: id.Bytes()}.Build(),
		Satellite:        api.SatelliteRef_builder{Id: f.Actor.Bytes()}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.SatelliteAcceptResponse_builder{ProducerId: who.Bytes()}.Build(), nil
}

// satelliteActsFor is the producer a satellite acts for on a lamina of a
// set (§41.9): the adopted producer that records the set, which must be
// assigned to the satellite now. A satellite makes the producer's lamina
// calls for the producers it serves and for nobody else.
func (s Core) satelliteActsFor(ctx context.Context, sat pdid.Id, setId []byte) (pdid.Id, error) {
	set, err := pdid.From(setId)
	if err != nil {
		return pdid.Nil, status.Error(codes.PermissionDenied, "the lamina has no set")
	}
	ps, err := s.ent(ctx).Producer.Query().
		Where(producer.SetIdEQ(set.Uuid()), producer.SatelliteIdEQ(sat.Uuid()), producer.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), producer.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return pdid.Nil, err
	}
	if len(ps) == 0 {
		return pdid.Nil, status.Error(codes.PermissionDenied, "the producer of this lamina's set is not assigned to this satellite")
	}

	return pdid.Id(ps[0].Id), nil
}

// satelliteName is how history names a satellite: its alias, or its id.
func (s Core) satelliteName(ctx context.Context, id pdid.Id) string {
	if v, err := s.ent(ctx).Satellite.Get(ctx, id.Uuid()); err == nil && v.Alias != "" {
		return v.Alias
	}

	return id.String()
}

// satelliteLost makes LOST the PENDING laminae a satellite held whose
// bytes it no longer has (§41.9): it restarted, or it is down and nothing
// can deliver them any more. The open attempts fail with the reason, the
// lamina's row keeps the satellite it was lost by, and the loss is counted
// as the satellite's. lost says which of its laminae are; a late
// LaminaStored still brings one back (§14).
//
// Every write tests the version of the row as this read it, so that a
// node's stored event that landed after the read is not written over: a
// lamina that moved is read again and lost only if it is still PENDING
// through this satellite and still one `lost` says is. Refused a second
// time, it is left as it is: the leader's jobs ask again of a satellite
// that is down, a minute on; a restart asks once (satelliteRestarted), and
// a lamina it left PENDING is removed by the jobs as one nobody stored
// once its attempts expired and abandon_grace passed (§12.1).
func (s Core) satelliteLost(ctx context.Context, sat pdid.Id, why string, lost func(*ent.Lamina) bool) (int, error) {
	os, err := s.ent(ctx).Lamina.Query().
		Where(lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_PENDING)), lamina.SatelliteIdEQ(sat.Uuid())).
		All(ctx)
	if err != nil || len(os) == 0 {
		return 0, err
	}
	if s.d.lostHeld != nil {
		s.d.lostHeld(ctx)
	}
	name := s.satelliteName(ctx, sat)
	reason := "satellite " + name + " lost what it accepted: " + why
	now := s.d.now()
	n := 0
	for _, o := range os {
		if lost != nil && !lost(o) {
			continue
		}
		done, err := s.loseLamina(ctx, sat, o, reason, now, lost)
		if err != nil {
			s.d.log().Warn("lose a satellite's lamina", "satellite", name, "err", err.Error())
			continue
		}
		if !done {
			continue
		}
		n++
		s.d.metrics().Lost.Add(ctx, 1, metric.WithAttributes(attribute.String("by", "satellite")))
		s.d.log().Warn("lamina lost with the satellite that held it", "satellite", name, "lamina", pdid.Id(o.Id).String(), "key", o.LaminaKey, "why", why)
	}

	return n, nil
}

// loseLamina makes one lamina of satelliteLost LOST and its open attempts
// FAILED, each with the version it was read at, and says whether it did.
func (s Core) loseLamina(ctx context.Context, sat pdid.Id, o *ent.Lamina, reason string, now time.Time, lost func(*ent.Lamina) bool) (bool, error) {
	for again := false; ; again = true {
		err := s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
			open, err := s.ent(ctx).Attempt.Query().
				Where(attempt.LaminaIdEQ(o.Id), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED))).
				All(ctx)
			if err != nil {
				return err
			}
			for _, a := range open {
				st := api.AttemptState_ATTEMPT_STATE_FAILED
				if _, err := own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
					Ref:           api.AttemptRef_builder{Id: a.Id[:]}.Build(),
					State:         &st,
					FailureReason: z.Ptr(reason),
					DateFinished:  timestamppb.New(now),
					DateUpdated:   timestamppb.New(a.DateUpdated),
				}.Build()); err != nil {
					return err
				}
			}
			st := api.LaminaState_LAMINA_STATE_LOST
			_, err = own.Lamina().Patch(ctx, api.LaminaPatchRequest_builder{
				Ref:          api.LaminaRef_builder{Id: o.Id[:]}.Build(),
				State:        &st,
				DateFinished: timestamppb.New(now),
				DateUpdated:  timestamppb.New(o.DateUpdated),
			}.Build())

			return err
		})
		if !moved(err) {
			return err == nil, err
		}
		if again {
			s.d.log().Warn("a satellite's lamina moved twice while it was lost; left as it is", "satellite", sat.String(), "lamina", pdid.Id(o.Id).String())
			return false, nil
		}
		o, err = s.ent(ctx).Lamina.Get(ctx, o.Id)
		if ent.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if o.State != int32(api.LaminaState_LAMINA_STATE_PENDING) || o.SatelliteId != sat.Uuid() || (lost != nil && !lost(o)) {
			return false, nil
		}
	}
}

// satelliteRestarted makes LOST what a satellite held before it restarted
// (§41.9): a heartbeat with another incarnation than the last one's is a
// new process, which holds nothing of what the last one accepted but what
// it says it holds now.
func (s Core) satelliteRestarted(ctx context.Context, sat pdid.Id, last, now *api.SatelliteStatus) {
	if last.GetIncarnation() == "" || now.GetIncarnation() == "" || last.GetIncarnation() == now.GetIncarnation() {
		return
	}
	holds := map[[16]byte]bool{}
	for _, b := range now.GetHeld() {
		if id, err := pdid.From(b); err == nil {
			holds[id.Uuid()] = true
		}
	}
	if _, err := s.satelliteLost(ctx, sat, "it restarted", func(o *ent.Lamina) bool { return !holds[o.Id] }); err != nil {
		s.d.log().Warn("satellite restarted", "satellite", sat.String(), "err", err.Error())
	}
}

// satellitesDown makes LOST what satellites that are down held, once
// nothing can deliver it (§41.9): unheard for satellite_down_after, erased
// or no longer adopted, and the lamina has no attempt left whose token is
// good. A satellite only cut off from the CP still delivers with the
// tokens it has, until they expire; after that what it held is lost
// wherever it is. The leader's jobs ask, every minute.
func (s Core) satellitesDown(ctx context.Context, now time.Time) error {
	vs, err := s.ent(ctx).Satellite.Query().All(ctx)
	if err != nil {
		return err
	}
	for _, v := range vs {
		if satelliteAlive(v, now, s.d.satelliteDownAfter()) {
			continue
		}
		if _, err := s.satelliteLost(ctx, pdid.Id(v.Id), "it is down, and the attempts it held expired", func(o *ent.Lamina) bool {
			open, err := s.ent(ctx).Attempt.Query().
				Where(attempt.LaminaIdEQ(o.Id), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(now)).
				Count(ctx)
			return err == nil && open == 0
		}); err != nil {
			return err
		}
	}

	return nil
}
