package core

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/slug"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/reader"
	"github.com/lesomnus/shale/internal/ent/relay"
	"github.com/lesomnus/shale/internal/ent/tenant"
	"github.com/lesomnus/shale/internal/pki"
)

// Joining and adoption (§33.4), for the four kinds of host. A host calls
// Join without a credential until an operator adopts it; adoption issues
// its certificate; the next Join hands the certificate back.

// joinState is what a decision needs to know about the row a join found.
type joinState struct {
	id         pdid.Id
	state      api.HostState
	join       *api.HostJoin
	certSerial string
}

// decision is what to do about a join.
type decision int

const (
	decCreate  decision = iota // no row: make a pending one
	decPending                 // a pending row: refresh its request
	decIssued                  // adopted, same key: hand the certificate back
	decRejoin                  // adopted, new key: pending again unless readopt is auto
)

func (s Core) decide(row *joinState, hj *api.HostJoin) decision {
	switch {
	case row == nil:
		return decCreate
	case row.state != api.HostState_HOST_STATE_ADOPTED:
		return decPending
	case row.join.GetKeyFingerprint() == hj.GetKeyFingerprint() && len(row.join.GetCertificate()) > 0:
		return decIssued
	default:
		return decRejoin
	}
}

// validateJoin checks what every join must say.
func validateJoin(hj *api.HostJoin) error {
	if hj == nil {
		return invalid("host", "required")
	}
	if strings.TrimSpace(hj.GetHardwareId()) == "" {
		return invalid("host.hardware_id", "required")
	}
	// What a host says about itself lands in an alias, in rows and in
	// logs before anybody adopted it (§33.7): a short printable line.
	if id := hj.GetHardwareId(); len(id) > 128 || strings.ContainsFunc(id, unprintable) || strings.ContainsAny(id, " \t") {
		return invalid("host.hardware_id", "at most 128 printable characters, no whitespace")
	}
	if h := hj.GetHostname(); len(h) > 253 || strings.ContainsFunc(h, unprintable) {
		return invalid("host.hostname", "at most 253 printable characters")
	}
	if len(hj.GetCsr()) == 0 {
		return invalid("host.csr", "required")
	}
	if _, err := x509.ParseCertificateRequest(hj.GetCsr()); err != nil {
		return invalid("host.csr", err.Error())
	}

	return nil
}

// joinRecord is the request as the row keeps it.
func (s Core) joinRecord(hj *api.HostJoin, from string, rejoin bool) *api.HostJoin {
	return api.HostJoin_builder{
		HardwareId:     strings.ToLower(strings.TrimSpace(hj.GetHardwareId())),
		HardwareIdKind: hj.GetHardwareIdKind(),
		Hostname:       hj.GetHostname(),
		Csr:            hj.GetCsr(),
		KeyFingerprint: hj.GetKeyFingerprint(),
		DateJoined:     timestamppb.New(s.d.now()),
		Version:        hj.GetVersion(),
		SourceAddress:  from,
		Rejoin:         rejoin,
	}.Build()
}

// issue signs the CSR a row holds and answers the PEM and the serial.
func (s Core) issue(hj *api.HostJoin, id pdid.Id, names pki.Names) (pem []byte, serial string, expires time.Time, err error) {
	if s.d.CA == nil {
		return nil, "", time.Time{}, status.Error(codes.FailedPrecondition, "this control plane has no CA; adoption cannot issue a certificate")
	}
	c, err := s.d.CA.IssueHost(hj.GetCsr(), id, names, s.d.now())
	if err != nil {
		return nil, "", time.Time{}, err
	}

	return pki.EncodeCerts(c), pki.Serial(c), c.NotAfter, nil
}

func (s Core) bundle() []byte {
	if s.d.CA == nil {
		return nil
	}

	return s.d.CA.Bundle()
}

// aliasFor picks a unique alias for a host from its hostname.
func aliasFor(hostname string, taken func(string) bool) string {
	base := strings.ToLower(strings.TrimSpace(hostname))
	var b strings.Builder
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '.', r == '_', r == ' ':
			b.WriteRune('-')
		}
	}
	base = strings.Trim(b.String(), "-")
	for strings.Contains(base, "--") {
		base = strings.ReplaceAll(base, "--", "-")
	}
	if _, err := slug.ParseAlias(base); base == "" || err != nil {
		base = "host"
	}
	if !taken(base) {
		return base
	}
	for i := 2; i < 1000; i++ {
		v := fmt.Sprintf("%s-%d", base, i)
		if !taken(v) {
			return v
		}
	}

	return base + "-" + aliasSuffix(pdid.New(DomNode))
}

// tenantFor is the tenant a producer or reader joins: the one named, or the
// only tenant there is besides the cluster's (§7).
func (s Core) tenantFor(ctx context.Context, alias string) (*api.Tenant, error) {
	if alias != "" {
		return s.d.Own.Tenant().Get(ctx, api.TenantGetRequest_builder{
			Ref: api.TenantRef_builder{Alias: z.Ptr(alias)}.Build(),
		}.Build())
	}

	vs, err := s.d.Own.Tenant().List(ctx, api.TenantListRequest_builder{Size: 100}.Build())
	if err != nil {
		return nil, err
	}
	var found *api.Tenant
	for _, t := range vs.GetItems() {
		if mustId(t.GetId()) == s.d.ClusterTenant {
			continue
		}
		if found != nil {
			return nil, status.Error(codes.InvalidArgument, "tenant: several tenants exist; say which one this host is for")
		}
		found = t
	}
	if found == nil {
		return nil, status.Error(codes.FailedPrecondition, "no tenant exists; run `shale init`")
	}

	return found, nil
}

// autoAdopt says whether this join is adopted at once.
func (s Core) autoAdopt(kind pdid.Domain, hj *api.HostJoin, from string) bool {
	return s.d.AutoAdopt != nil && s.d.AutoAdopt(kind, hj, from)
}

// ---- Producer ----------------------------------------------------------

type coreProducer struct {
	Core
	api.ProducerServiceServer
}

func (s Core) Producer() api.ProducerServiceServer {
	return coreProducer{s, s.Next().Producer()}
}

func (s coreProducer) Join(ctx context.Context, req *api.ProducerJoinRequest) (*api.ProducerJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	from, _ := peerAddr(ctx)

	t, err := s.tenantFor(ctx, req.GetTenant())
	if err != nil {
		return nil, err
	}
	tid := mustId(t.GetId())

	row, err := s.d.Ent.Producer.Query().
		Where(producer.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), producer.TenantIdEQ(tid.Uuid()), producer.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}

	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle()}
	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomProducer)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.d.Ent.Producer.Query().Where(producer.AliasEQ(a), producer.TenantIdEQ(tid.Uuid()), producer.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		if _, err := s.d.Own.Producer().Add(ctx, api.ProducerAddRequest_builder{
			Id:         id.Bytes(),
			Tenant:     tenantRef(tid),
			Alias:      alias,
			HardwareId: strings.ToLower(hj.GetHardwareId()),
			Hostname:   hj.GetHostname(),
			State:      api.HostState_HOST_STATE_PENDING,
			Join:       s.joinRecord(hj, from, false),
			Version:    hj.GetVersion(),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale producer adopt " + alias + " --set <set>"
	case decPending:
		if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref:              api.ProducerRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			Version:          z.Ptr(hj.GetVersion()),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
		answer.Rejoin = js.join.GetRejoin()
	case decIssued:
		answer.State = api.HostState_HOST_STATE_ADOPTED
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Certificate = js.join.GetCertificate()
	case decRejoin:
		rec := s.joinRecord(hj, from, true)
		if s.d.Readopt == "auto" || s.autoAdopt(DomProducer, hj, from) {
			pem, serial, exp, err := s.issue(rec, js.id, pki.Names{})
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
				Ref:              api.ProducerRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				CertSerial:       z.Ptr(serial),
				DateCertExpires:  timestamppb.New(exp),
				Hostname:         z.Ptr(hj.GetHostname()),
				Version:          z.Ptr(hj.GetVersion()),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
		} else {
			if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
				Ref:              api.ProducerRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				Hostname:         z.Ptr(hj.GetHostname()),
				Version:          z.Ptr(hj.GetVersion()),
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

	return api.ProducerJoinResponse_builder{Answer: answer.Build()}.Build(), nil
}

// Adopt accepts a pending producer and assigns its set; the site follows the
// set (§33.4).
func (s coreProducer) Adopt(ctx context.Context, req *api.ProducerAdoptRequest) (*api.Producer, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	if req.GetSet() == nil {
		return nil, invalid("set", "a producer is adopted for a set")
	}

	p, err := s.ProducerServiceServer.Get(ctx, api.ProducerGetRequest_builder{
		Ref: req.GetRef(),
	}.Build())
	if err != nil {
		return nil, err
	}
	set, err := s.Next().Set().Get(ctx, api.SetGetRequest_builder{Ref: req.GetSet()}.Build())
	if err != nil {
		return nil, err
	}
	if string(set.GetTenant().GetId()) != string(p.GetTenant().GetId()) {
		return nil, invalid("set", "a set of another tenant")
	}

	// One producer per set.
	others, err := s.d.Ent.Producer.Query().
		Where(producer.SetIdEQ(mustId(set.GetId()).Uuid()), producer.DateErasedIsNil(), producer.IdNEQ(mustId(p.GetId()).Uuid())).
		Count(ctx)
	if err != nil {
		return nil, err
	}
	if others > 0 {
		return nil, failed("set %s already has a producer", set.GetAlias())
	}

	rec := p.GetJoin()
	if rec == nil || len(rec.GetCsr()) == 0 {
		return nil, failed("this producer has no pending join request")
	}
	pem, serial, exp, err := s.issue(rec, mustId(p.GetId()), pki.Names{})
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	rec.SetRejoin(false)

	patch := api.ProducerPatchRequest_builder{
		Ref:             api.ProducerRef_builder{Id: p.GetId()}.Build(),
		Set:             api.SetRef_builder{Id: set.GetId()}.Build(),
		State:           z.Ptr(api.HostState_HOST_STATE_ADOPTED),
		Join:            rec,
		CertSerial:      z.Ptr(serial),
		DateAdopted:     timestamppb.New(s.d.now()),
		DateCertExpires: timestamppb.New(exp),
		DateUpdated:     p.GetDateUpdated(),
	}
	if len(set.GetSite().GetId()) > 0 {
		patch.Site = api.SiteRef_builder{Id: set.GetSite().GetId()}.Build()
	} else {
		patch.SiteNull = z.Ptr(true)
	}
	if a := strings.TrimSpace(req.GetAlias()); a != "" {
		patch.Alias = z.Ptr(a)
	}

	return s.ProducerServiceServer.Patch(ctx, patch.Build())
}

func (s coreProducer) RenewCertificate(ctx context.Context, req *api.ProducerRenewCertificateRequest) (*api.ProducerRenewCertificateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomProducer {
		return nil, status.Error(codes.PermissionDenied, "only a producer renews its own certificate")
	}
	if _, err := x509.ParseCertificateRequest(req.GetCsr()); err != nil {
		return nil, invalid("csr", err.Error())
	}

	p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	rec := p.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	pem, serial, exp, err := s.issue(rec, f.Actor, pki.Names{})
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
		Ref:              api.ProducerRef_builder{Id: p.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.ProducerRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle()}.Build(), nil
}

func (s coreProducer) Heartbeat(ctx context.Context, req *api.ProducerHeartbeatRequest) (*api.ProducerHeartbeatResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomProducer {
		return nil, status.Error(codes.PermissionDenied, "only a producer heartbeats")
	}

	p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{
		Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}

	now := s.d.now()
	if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
		Ref:      api.ProducerRef_builder{Id: p.GetId()}.Build(),
		DateSeen: timestamppb.New(now),
		Status: api.ProducerStatus_builder{
			Sources:      req.GetSources(),
			Load:         req.GetLoad(),
			DateReported: timestamppb.New(now),
		}.Build(),
		Version:          z.Ptr(req.GetVersion()),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	// Starvation accounting on each source (§38.5).
	suggestions, err := s.starvation(ctx, p, req.GetSources())
	if err != nil {
		return nil, err
	}

	resp := api.ProducerHeartbeatResponse_builder{Suggestions: suggestions}
	if len(p.GetSet().GetId()) > 0 {
		if set, err := s.d.Own.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: p.GetSet().GetId()}.Build()}.Build()); err == nil {
			resp.ProfileVersion = set.GetProfileVersion()
			if ra, err := s.relayAssignment(ctx, f.Actor, set); err == nil && ra != nil {
				resp.Relay = ra
			}
		}
	}
	if exp := p.GetDateCertExpires(); exp != nil && now.After(renewAt(p.GetDateAdopted(), exp)) {
		resp.RenewCertificate = true
	}

	return resp.Build(), nil
}

// renewAt is two thirds of the way from issue to expiry (§33.5).
func renewAt(issued, expires *timestamppb.Timestamp) time.Time {
	e := expires.AsTime()
	i := e.Add(-pki.HostLifetime)
	if issued != nil && issued.AsTime().After(i) {
		i = issued.AsTime()
	}

	return i.Add(time.Duration(float64(e.Sub(i)) * pki.RenewFraction))
}

func (s coreProducer) Relay(ctx context.Context, req *api.ProducerRelayRequest) (*api.ProducerRelayResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomProducer {
		return nil, status.Error(codes.PermissionDenied, "only a producer asks for its relay")
	}
	p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if len(p.GetSet().GetId()) == 0 {
		return nil, failed("this producer has no set")
	}
	set, err := s.d.Own.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: p.GetSet().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	ra, err := s.relayAssignment(ctx, f.Actor, set)
	if err != nil {
		return nil, err
	}
	if ra == nil {
		return nil, status.Error(codes.Unavailable, "no relay is available")
	}

	return api.ProducerRelayResponse_builder{Relay: ra}.Build(), nil
}

// ---- Reader ------------------------------------------------------------

type coreReader struct {
	Core
	api.ReaderServiceServer
}

func (s Core) Reader() api.ReaderServiceServer {
	return coreReader{s, s.Next().Reader()}
}

func (s coreReader) Join(ctx context.Context, req *api.ReaderJoinRequest) (*api.ReaderJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	from, _ := peerAddr(ctx)

	t, err := s.tenantFor(ctx, req.GetTenant())
	if err != nil {
		return nil, err
	}
	tid := mustId(t.GetId())

	row, err := s.d.Ent.Reader.Query().
		Where(reader.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), reader.TenantIdEQ(tid.Uuid()), reader.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle()}
	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomReader)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.d.Ent.Reader.Query().Where(reader.AliasEQ(a), reader.TenantIdEQ(tid.Uuid()), reader.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		if _, err := s.d.Own.Reader().Add(ctx, api.ReaderAddRequest_builder{
			Id:         id.Bytes(),
			Tenant:     tenantRef(tid),
			Alias:      alias,
			HardwareId: strings.ToLower(hj.GetHardwareId()),
			Hostname:   hj.GetHostname(),
			State:      api.HostState_HOST_STATE_PENDING,
			Join:       s.joinRecord(hj, from, false),
			Version:    hj.GetVersion(),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale reader adopt " + alias
	case decPending:
		if _, err := s.d.Own.Reader().Patch(ctx, api.ReaderPatchRequest_builder{
			Ref:              api.ReaderRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
	case decIssued:
		answer.State = api.HostState_HOST_STATE_ADOPTED
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Certificate = js.join.GetCertificate()
	case decRejoin:
		rec := s.joinRecord(hj, from, true)
		if s.d.Readopt == "auto" || s.autoAdopt(DomReader, hj, from) {
			pem, serial, exp, err := s.issue(rec, js.id, pki.Names{})
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.d.Own.Reader().Patch(ctx, api.ReaderPatchRequest_builder{
				Ref:              api.ReaderRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				CertSerial:       z.Ptr(serial),
				DateCertExpires:  timestamppb.New(exp),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
		} else {
			if _, err := s.d.Own.Reader().Patch(ctx, api.ReaderPatchRequest_builder{
				Ref:              api.ReaderRef_builder{Id: js.id.Bytes()}.Build(),
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

	return api.ReaderJoinResponse_builder{Answer: answer.Build()}.Build(), nil
}

func (s coreReader) Adopt(ctx context.Context, req *api.ReaderAdoptRequest) (*api.Reader, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	r, err := s.ReaderServiceServer.Get(ctx, api.ReaderGetRequest_builder{
		Ref: req.GetRef(),
	}.Build())
	if err != nil {
		return nil, err
	}
	rec := r.GetJoin()
	if rec == nil || len(rec.GetCsr()) == 0 {
		return nil, failed("this reader has no pending join request")
	}
	pem, serial, exp, err := s.issue(rec, mustId(r.GetId()), pki.Names{})
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	rec.SetRejoin(false)

	var out *api.Reader
	err = s.tx(ctx, func(nx api.Server) error {
		patch := api.ReaderPatchRequest_builder{
			Ref:             api.ReaderRef_builder{Id: r.GetId()}.Build(),
			State:           z.Ptr(api.HostState_HOST_STATE_ADOPTED),
			Join:            rec,
			CertSerial:      z.Ptr(serial),
			DateAdopted:     timestamppb.New(s.d.now()),
			DateCertExpires: timestamppb.New(exp),
			AllSites:        z.Ptr(len(req.GetSites()) == 0),
			DateUpdated:     r.GetDateUpdated(),
		}
		if a := strings.TrimSpace(req.GetAlias()); a != "" {
			patch.Alias = z.Ptr(a)
		}
		v, err := nx.Reader().Patch(ctx, patch.Build())
		if err != nil {
			return err
		}
		out = v

		for _, site := range req.GetSites() {
			st, err := nx.Site().Get(ctx, api.SiteGetRequest_builder{Ref: site}.Build())
			if err != nil {
				return err
			}
			if _, err := nx.SiteMember().Add(ctx, api.SiteMemberAddRequest_builder{
				Tenant: tenantRef(f.Tenant),
				Site:   api.SiteRef_builder{Id: st.GetId()}.Build(),
				Reader: api.ReaderRef_builder{Id: r.GetId()}.Build(),
			}.Build()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s coreReader) RenewCertificate(ctx context.Context, req *api.ReaderRenewCertificateRequest) (*api.ReaderRenewCertificateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomReader {
		return nil, status.Error(codes.PermissionDenied, "only a reader renews its own certificate")
	}
	if _, err := x509.ParseCertificateRequest(req.GetCsr()); err != nil {
		return nil, invalid("csr", err.Error())
	}
	r, err := s.d.Own.Reader().Get(ctx, api.ReaderGetRequest_builder{Ref: api.ReaderRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	rec := r.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	pem, serial, exp, err := s.issue(rec, f.Actor, pki.Names{})
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.d.Own.Reader().Patch(ctx, api.ReaderPatchRequest_builder{
		Ref:              api.ReaderRef_builder{Id: r.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.ReaderRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle()}.Build(), nil
}

func (s coreReader) Heartbeat(ctx context.Context, req *api.ReaderHeartbeatRequest) (*api.ReaderHeartbeatResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomReader {
		return nil, status.Error(codes.PermissionDenied, "only a reader heartbeats")
	}
	r, err := s.d.Own.Reader().Get(ctx, api.ReaderGetRequest_builder{Ref: api.ReaderRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	now := s.d.now()
	if _, err := s.d.Own.Reader().Patch(ctx, api.ReaderPatchRequest_builder{
		Ref:              api.ReaderRef_builder{Id: r.GetId()}.Build(),
		DateSeen:         timestamppb.New(now),
		Version:          z.Ptr(req.GetVersion()),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}
	resp := api.ReaderHeartbeatResponse_builder{}
	if exp := r.GetDateCertExpires(); exp != nil && now.After(renewAt(r.GetDateAdopted(), exp)) {
		resp.RenewCertificate = true
	}

	return resp.Build(), nil
}

// ---- Node --------------------------------------------------------------

type coreNode struct {
	Core
	api.NodeServiceServer
}

func (s Core) Node() api.NodeServiceServer {
	return coreNode{s, s.Next().Node()}
}

func (s Core) nodeNames(ctx context.Context, id pdid.Id, alias string, ifs []*api.HostInterface, data string) pki.Names {
	_, _, _, address, _ := s.policies(ctx)
	dns, ips := NamesFor(NodeAddresses{Id: id, Alias: alias, Interfaces: ifs, DataAddress: data}, address)

	return pki.Names{DNS: dns, IPs: ips}
}

func (s coreNode) Join(ctx context.Context, req *api.NodeJoinRequest) (*api.NodeJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	if err := validateNodeAddresses(req.GetInterfaces(), req.GetControlAddress(), req.GetDataAddress()); err != nil {
		return nil, err
	}
	from, _ := peerAddr(ctx)

	row, err := s.d.Ent.Node.Query().
		Where(node.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), node.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle()}
	resp := api.NodeJoinResponse_builder{}
	auto := s.autoAdopt(DomNode, hj, from)

	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomNode)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.d.Ent.Node.Query().Where(node.AliasEQ(a), node.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		v, err := s.d.Own.Node().Add(ctx, api.NodeAddRequest_builder{
			Id:             id.Bytes(),
			Alias:          alias,
			HardwareId:     strings.ToLower(hj.GetHardwareId()),
			Hostname:       hj.GetHostname(),
			State:          api.HostState_HOST_STATE_PENDING,
			Join:           s.joinRecord(hj, from, false),
			Interfaces:     req.GetInterfaces(),
			ControlAddress: req.GetControlAddress(),
			DataAddress:    req.GetDataAddress(),
			Version:        hj.GetVersion(),
		}.Build())
		if err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale node adopt " + alias
		if auto {
			v, err = s.adoptNode(ctx, s.d.Own, v, "", req.GetSinks(), req.GetDevices())
			if err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = v.GetJoin().GetCertificate()
			answer.Message = ""
		}
	case decPending:
		v, err := s.d.Own.Node().Patch(ctx, api.NodePatchRequest_builder{
			Ref:              api.NodeRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			Interfaces:       req.GetInterfaces(),
			ControlAddress:   z.Ptr(req.GetControlAddress()),
			DataAddress:      z.Ptr(req.GetDataAddress()),
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
			v, err = s.adoptNode(ctx, s.d.Own, v, "", req.GetSinks(), req.GetDevices())
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
			pem, serial, exp, err := s.issue(rec, js.id, s.nodeNames(ctx, js.id, row.Alias, req.GetInterfaces(), req.GetDataAddress()))
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.d.Own.Node().Patch(ctx, api.NodePatchRequest_builder{
				Ref:              api.NodeRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				CertSerial:       z.Ptr(serial),
				DateCertExpires:  timestamppb.New(exp),
				Interfaces:       req.GetInterfaces(),
				ControlAddress:   z.Ptr(req.GetControlAddress()),
				DataAddress:      z.Ptr(req.GetDataAddress()),
				Version:          z.Ptr(hj.GetVersion()),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
		} else {
			if _, err := s.d.Own.Node().Patch(ctx, api.NodePatchRequest_builder{
				Ref:              api.NodeRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				Interfaces:       req.GetInterfaces(),
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

// adoptNode issues the certificate, marks the node adopted, and registers
// the sinks and devices it reported.
func (s Core) adoptNode(ctx context.Context, srv api.Server, n *api.Node, alias string, sinks []*api.SinkReport, devices []*api.DeviceReport) (*api.Node, error) {
	rec := n.GetJoin()
	if rec == nil || len(rec.GetCsr()) == 0 {
		return nil, failed("this node has no pending join request")
	}
	id := mustId(n.GetId())
	if alias == "" {
		alias = n.GetAlias()
	}
	pem, serial, exp, err := s.issue(rec, id, s.nodeNames(ctx, id, alias, n.GetInterfaces(), n.GetDataAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	rec.SetRejoin(false)

	now := s.d.now()
	v, err := srv.Node().Patch(ctx, api.NodePatchRequest_builder{
		Ref:              api.NodeRef_builder{Id: n.GetId()}.Build(),
		Alias:            z.Ptr(alias),
		State:            z.Ptr(api.HostState_HOST_STATE_ADOPTED),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateAdopted:      timestamppb.New(now),
		DateCertExpires:  timestamppb.New(exp),
		DateSeen:         timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build())
	if err != nil {
		return nil, err
	}

	if _, err := s.registerSinks(ctx, srv, id, devices, sinks); err != nil {
		return nil, err
	}

	return v, nil
}

func (s coreNode) Adopt(ctx context.Context, req *api.NodeAdoptRequest) (*api.Node, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	n, err := s.NodeServiceServer.Get(ctx, api.NodeGetRequest_builder{
		Ref: req.GetRef(),
	}.Build())
	if err != nil {
		return nil, err
	}

	var out *api.Node
	err = s.tx(ctx, func(nx api.Server) error {
		v, err := s.adoptNode(ctx, nx, n, strings.TrimSpace(req.GetAlias()), nil, nil)
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (s coreNode) RenewCertificate(ctx context.Context, req *api.NodeRenewCertificateRequest) (*api.NodeRenewCertificateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomNode {
		return nil, status.Error(codes.PermissionDenied, "only a node renews its own certificate")
	}
	if _, err := x509.ParseCertificateRequest(req.GetCsr()); err != nil {
		return nil, invalid("csr", err.Error())
	}
	n, err := s.d.Own.Node().Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	rec := n.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	pem, serial, exp, err := s.issue(rec, f.Actor, s.nodeNames(ctx, f.Actor, n.GetAlias(), n.GetInterfaces(), n.GetDataAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.d.Own.Node().Patch(ctx, api.NodePatchRequest_builder{
		Ref:              api.NodeRef_builder{Id: n.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.NodeRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle()}.Build(), nil
}

func (s coreNode) Heartbeat(ctx context.Context, req *api.NodeHeartbeatRequest) (*api.NodeHeartbeatResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomNode {
		return nil, status.Error(codes.PermissionDenied, "only a node heartbeats")
	}

	n, err := s.d.Own.Node().Get(ctx, api.NodeGetRequest_builder{
		Ref: api.NodeRef_builder{Id: f.Actor.Bytes()}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	if n.GetState() != api.HostState_HOST_STATE_ADOPTED {
		return nil, status.Error(codes.PermissionDenied, "this node is not adopted")
	}
	if err := validateNodeAddresses(req.GetInterfaces(), req.GetControlAddress(), req.GetDataAddress()); err != nil {
		return nil, err
	}

	now := s.d.now()
	patch := api.NodePatchRequest_builder{
		Ref:      api.NodeRef_builder{Id: n.GetId()}.Build(),
		DateSeen: timestamppb.New(now),
		KeyIds:   req.GetKeyIds(),
		CaHash:   z.Ptr(req.GetCaHash()),
		Version:  z.Ptr(req.GetVersion()),
		Status: api.NodeStatus_builder{
			DateReported:    timestamppb.New(now),
			Sinks:           int32(len(req.GetSinks())),
			Devices:         int32(len(req.GetDevices())),
			UploadsInFlight: req.GetUploadsInFlight(),
			IndexLaminae:    req.GetIndexLaminae(),
			Warnings:        req.GetWarnings(),
		}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}
	if len(req.GetInterfaces()) > 0 {
		patch.Interfaces = req.GetInterfaces()
	}
	if addr := req.GetControlAddress(); addr != "" {
		// The CP dials the control API on the cluster-facing IP the node
		// came from, never through the resolver (§34.10): a node reports
		// the port it bound, on whatever interface it listens on.
		if host, port, err := net.SplitHostPort(addr); err == nil && (host == "" || host == "0.0.0.0" || host == "::") {
			if p, ok := peerAddr(ctx); ok {
				addr = net.JoinHostPort(p, port)
			}
		}
		patch.ControlAddress = z.Ptr(addr)
	}
	if req.GetDataAddress() != "" {
		patch.DataAddress = z.Ptr(req.GetDataAddress())
	}
	if _, err := s.d.Own.Node().Patch(ctx, patch.Build()); err != nil {
		return nil, err
	}

	var answers []*api.SinkAnswer
	err = s.ownTx(ctx, func(own api.Server) error {
		vs, err := s.registerSinks(ctx, own, f.Actor, req.GetDevices(), req.GetSinks())
		answers = vs

		return err
	})
	if err != nil {
		return nil, err
	}

	resp := api.NodeHeartbeatResponse_builder{Sinks: answers}
	if exp := n.GetDateCertExpires(); exp != nil && now.After(renewAt(n.GetDateAdopted(), exp)) {
		resp.RenewCertificate = true
	}

	return resp.Build(), nil
}

func (s coreNode) Resolve(ctx context.Context, req *api.NodeResolveRequest) (*api.NodeResolveResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	n, err := s.NodeServiceServer.Get(ctx, api.NodeGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	_, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	from := req.GetFrom()
	if from == "" {
		from, _ = peerAddr(ctx)
	}
	r := s.d.Resolve
	if r == nil {
		r = Advertised{}
	}

	return api.NodeResolveResponse_builder{Endpoints: r.Endpoints(NodeAddresses{
		Id: mustId(n.GetId()), Alias: n.GetAlias(), Interfaces: n.GetInterfaces(), DataAddress: n.GetDataAddress(), Dev: s.d.Dev,
	}, from, address)}.Build(), nil
}

// ---- Relay -------------------------------------------------------------

type coreRelay struct {
	Core
	api.RelayServiceServer
}

func (s Core) Relay() api.RelayServiceServer {
	return coreRelay{s, s.Next().Relay()}
}

func (s Core) relayNames(ctx context.Context, id pdid.Id, alias string, ifs []*api.HostInterface, ingest, whep string) pki.Names {
	_, _, _, address, _ := s.policies(ctx)
	dns, ips := NamesFor(NodeAddresses{Id: id, Alias: alias, Interfaces: ifs, DataAddress: ingest}, address)
	if host, _ := splitAddr(whep); host != "" {
		dns2, ips2 := NamesFor(NodeAddresses{Id: id, Alias: alias, DataAddress: whep}, nil)
		dns = append(dns, dns2...)
		ips = append(ips, ips2...)
	}

	return pki.Names{DNS: dns, IPs: ips}
}

func (s coreRelay) Join(ctx context.Context, req *api.RelayJoinRequest) (*api.RelayJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	if err := validateRelayAddresses(req.GetInterfaces(), req.GetIngestAddress(), req.GetWhepAddress()); err != nil {
		return nil, err
	}
	from, _ := peerAddr(ctx)

	row, err := s.d.Ent.Relay.Query().
		Where(relay.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), relay.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle()}
	resp := api.RelayJoinResponse_builder{}
	auto := s.autoAdopt(DomRelay, hj, from)

	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomRelay)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.d.Ent.Relay.Query().Where(relay.AliasEQ(a), relay.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		v, err := s.d.Own.Relay().Add(ctx, api.RelayAddRequest_builder{
			Id:            id.Bytes(),
			Alias:         alias,
			HardwareId:    strings.ToLower(hj.GetHardwareId()),
			Hostname:      hj.GetHostname(),
			State:         api.HostState_HOST_STATE_PENDING,
			Join:          s.joinRecord(hj, from, false),
			Interfaces:    req.GetInterfaces(),
			IngestAddress: req.GetIngestAddress(),
			WhepAddress:   req.GetWhepAddress(),
			Version:       hj.GetVersion(),
		}.Build())
		if err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale relay adopt " + alias
		if auto {
			v, err = s.adoptRelay(ctx, s.d.Own, v, "")
			if err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = v.GetJoin().GetCertificate()
			answer.Message = ""
		}
	case decPending:
		v, err := s.d.Own.Relay().Patch(ctx, api.RelayPatchRequest_builder{
			Ref:              api.RelayRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			Interfaces:       req.GetInterfaces(),
			IngestAddress:    z.Ptr(req.GetIngestAddress()),
			WhepAddress:      z.Ptr(req.GetWhepAddress()),
			Version:          z.Ptr(hj.GetVersion()),
			DateUpdatedForce: z.Ptr(true),
		}.Build())
		if err != nil {
			return nil, err
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
		if auto {
			v, err = s.adoptRelay(ctx, s.d.Own, v, "")
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
			pem, serial, exp, err := s.issue(rec, js.id, s.relayNames(ctx, js.id, row.Alias, req.GetInterfaces(), req.GetIngestAddress(), req.GetWhepAddress()))
			if err != nil {
				return nil, err
			}
			rec.SetCertificate(pem)
			if _, err := s.d.Own.Relay().Patch(ctx, api.RelayPatchRequest_builder{
				Ref:              api.RelayRef_builder{Id: js.id.Bytes()}.Build(),
				Join:             rec,
				CertSerial:       z.Ptr(serial),
				DateCertExpires:  timestamppb.New(exp),
				Interfaces:       req.GetInterfaces(),
				IngestAddress:    z.Ptr(req.GetIngestAddress()),
				WhepAddress:      z.Ptr(req.GetWhepAddress()),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return nil, err
			}
			answer.State = api.HostState_HOST_STATE_ADOPTED
			answer.Certificate = pem
		} else {
			if _, err := s.d.Own.Relay().Patch(ctx, api.RelayPatchRequest_builder{
				Ref:              api.RelayRef_builder{Id: js.id.Bytes()}.Build(),
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

func (s Core) adoptRelay(ctx context.Context, srv api.Server, r *api.Relay, alias string) (*api.Relay, error) {
	rec := r.GetJoin()
	if rec == nil || len(rec.GetCsr()) == 0 {
		return nil, failed("this relay has no pending join request")
	}
	id := mustId(r.GetId())
	if alias == "" {
		alias = r.GetAlias()
	}
	pem, serial, exp, err := s.issue(rec, id, s.relayNames(ctx, id, alias, r.GetInterfaces(), r.GetIngestAddress(), r.GetWhepAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	rec.SetRejoin(false)
	now := s.d.now()

	return srv.Relay().Patch(ctx, api.RelayPatchRequest_builder{
		Ref:              api.RelayRef_builder{Id: r.GetId()}.Build(),
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

func (s coreRelay) Adopt(ctx context.Context, req *api.RelayAdoptRequest) (*api.Relay, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	r, err := s.RelayServiceServer.Get(ctx, api.RelayGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}

	return s.adoptRelay(ctx, s.Next(), r, strings.TrimSpace(req.GetAlias()))
}

func (s coreRelay) RenewCertificate(ctx context.Context, req *api.RelayRenewCertificateRequest) (*api.RelayRenewCertificateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomRelay {
		return nil, status.Error(codes.PermissionDenied, "only a relay renews its own certificate")
	}
	if _, err := x509.ParseCertificateRequest(req.GetCsr()); err != nil {
		return nil, invalid("csr", err.Error())
	}
	r, err := s.d.Own.Relay().Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	rec := r.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	pem, serial, exp, err := s.issue(rec, f.Actor, s.relayNames(ctx, f.Actor, r.GetAlias(), r.GetInterfaces(), r.GetIngestAddress(), r.GetWhepAddress()))
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.d.Own.Relay().Patch(ctx, api.RelayPatchRequest_builder{
		Ref:              api.RelayRef_builder{Id: r.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.RelayRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle()}.Build(), nil
}

func (s coreRelay) Heartbeat(ctx context.Context, req *api.RelayHeartbeatRequest) (*api.RelayHeartbeatResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomRelay {
		return nil, status.Error(codes.PermissionDenied, "only a relay heartbeats")
	}
	r, err := s.d.Own.Relay().Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	now := s.d.now()
	st := req.GetStatus()
	if st == nil {
		st = &api.RelayStatus{}
	}
	st.SetDateReported(timestamppb.New(now))
	if err := validateRelayAddresses(req.GetInterfaces(), req.GetIngestAddress(), req.GetWhepAddress()); err != nil {
		return nil, err
	}
	patch := api.RelayPatchRequest_builder{
		Ref:              api.RelayRef_builder{Id: r.GetId()}.Build(),
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
	if req.GetIngestAddress() != "" {
		patch.IngestAddress = z.Ptr(req.GetIngestAddress())
	}
	if req.GetWhepAddress() != "" {
		patch.WhepAddress = z.Ptr(req.GetWhepAddress())
	}
	if _, err := s.d.Own.Relay().Patch(ctx, patch.Build()); err != nil {
		return nil, err
	}
	resp := api.RelayHeartbeatResponse_builder{}
	if exp := r.GetDateCertExpires(); exp != nil && now.After(renewAt(r.GetDateAdopted(), exp)) {
		resp.RenewCertificate = true
	}

	return resp.Build(), nil
}

func (s coreRelay) Assign(ctx context.Context, req *api.RelayAssignRequest) (*api.Relay, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	r, err := s.RelayServiceServer.Get(ctx, api.RelayGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: req.GetProducer()}.Build())
	if err != nil {
		return nil, err
	}
	if _, err := s.d.Own.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
		Ref:              api.ProducerRef_builder{Id: p.GetId()}.Build(),
		Relay:            api.RelayRef_builder{Id: r.GetId()}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return r, nil
}

// tenantName is the alias of a tenant by id, for messages.
func (s Core) tenantName(ctx context.Context, id pdid.Id) string {
	t, err := s.d.Ent.Tenant.Query().Where(tenant.IdEQ(id.Uuid())).Only(ctx)
	if err != nil {
		return id.String()
	}

	return t.Alias
}
