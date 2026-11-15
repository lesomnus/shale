package core

import (
	"bytes"
	"context"
	"crypto/x509"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
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

	row, err := s.ent(ctx).Satellite.Query().
		Where(satellite.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), satellite.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
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
		v, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
			Ref:              api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			Interfaces:       req.GetInterfaces(),
			UploadAddress:    z.Ptr(req.GetUploadAddress()),
			Version:          z.Ptr(hj.GetVersion()),
			DateUpdatedForce: z.Ptr(true),
		}.Build())
		if err != nil {
			return nil, err
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
		answer.Rejoin = js.join.GetRejoin()
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
		rec := s.joinRecord(hj, from, true)
		if s.d.Readopt == "auto" || auto {
			pem, serial, exp, err := s.issue(ctx, rec, js.id, s.satelliteNames(ctx, js.id, row.Alias, req.GetInterfaces(), req.GetUploadAddress()))
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
				Ref:              api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				CertSerial:       z.Ptr(serial),
				DateCertExpires:  timestamppb.New(exp),
				Interfaces:       req.GetInterfaces(),
				UploadAddress:    z.Ptr(req.GetUploadAddress()),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
		} else {
			if _, err := s.own(ctx).Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
				Ref:              api.SatelliteRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.Message = "known host, new key: waiting for adoption"
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Rejoin = true
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
