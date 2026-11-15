package core

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/cameraworker"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/reader"
	"github.com/lesomnus/shale/internal/ent/relay"
	"github.com/lesomnus/shale/internal/ent/satellite"
	"github.com/lesomnus/shale/internal/pki"
)

// The built-in CA and its rollover (§33.5).
//
// Until the first rollover the CA is the one `shale init` wrote into the
// state directory, and nothing here is stored. A rollover records that CA
// as a CertificateAuthority row beside the next one, and from then on the
// rows are the CA set every CP process reads, as SigningKey rows are the
// key set (§33.3). A rollover is three steps, each taken by the leader
// (§34.9) once every live host has caught up with the one before:
//
//  1. distributing: the next CA is made and the bundle carries both; the
//     old one still issues. Hosts holding another bundle are asked to renew,
//     which hands them this one.
//  2. replacing: once every live host reports that bundle, the new CA issues
//     host certificates and the CP's own; hosts holding a certificate from
//     the old one are asked to renew.
//  3. once every live host still reports the bundle and none holds a
//     certificate from the old CA, the old CA is dropped: the bundle is the
//     new one alone, and hosts are asked to renew once more for it.
//
// The new CA is trusted everywhere before anything is issued from it, and
// the old one is dropped only once nothing live holds what it issued, so no
// cross-signed certificate is needed to bridge the two -- and none could:
// every CA init made has a path length of zero (internal/pki's
// TestCrossSigningCannotBridgeTheRollover).
//
// A live host is an adopted, unerased host whose certificate has not
// expired: one that can still call the CP without being adopted again. A
// host that never comes back stops being waited for when its certificate
// expires, at most a host certificate lifetime after its last renewal, or
// at once when it is erased; it is then adopted again like any host whose
// certificate expired (§33.4).

// Defaults of the rollover's clocks (§33.5, §36.1).
const (
	// DefaultCaRolloverBefore is how long before the active CA expires a
	// rollover begins by itself.
	DefaultCaRolloverBefore = 365 * 24 * time.Hour
	// DefaultCaSettle is the least time each step lasts. Every CP process
	// reads the CA set again at half of it, so each has read a step before
	// the next is taken: none refuses a certificate from a CA it has not
	// heard of yet.
	DefaultCaSettle = time.Minute
)

func (d *Deps) caRolloverBefore() time.Duration {
	if d.CaRolloverBefore > 0 {
		return d.CaRolloverBefore
	}

	return DefaultCaRolloverBefore
}

func (d *Deps) caSettle() time.Duration {
	if d.CaSettle > 0 {
		return d.CaSettle
	}

	return DefaultCaSettle
}

// caKek is what a stored CA's key is wrapped under: the KEK the signing
// keys are (§33.3), derived for CA keys alone.
func (d *Deps) caKek() Kek {
	k := d.kek()
	if k == nil {
		return nil
	}

	return k.Derive("shale ca key")
}

// Authorities is the CA set as this process last read it.
func (d *Deps) Authorities() *Authorities {
	d.casOnce.Do(func() { d.cas = &Authorities{d: d} })

	return d.cas
}

// Authorities is a CP process's view of its CAs: which one issues, what the
// bundle is, and what it trusts. It reads the CertificateAuthority rows
// again every half of `control.ca_settle`, and at once after a write of its
// own.
type Authorities struct {
	d *Deps

	mu  sync.Mutex
	at  time.Time
	set *CaSet
}

// CaSet is one reading of the CA set.
type CaSet struct {
	// Stored says the set is the rows: a rollover has begun at least once.
	// Before that the CA init wrote is the only one, and nothing about the
	// bundles hosts hold is asked of them.
	Stored bool
	// Phase is where a rollover is.
	Phase api.CertificateAuthorityPhase
	// Issuer issues host certificates and the CP's own.
	Issuer *pki.CA
	// Next is the CA a rollover made, while it is not issuing yet; Retiring
	// the one it replaces, while it is still trusted.
	Next, Retiring *x509.Certificate
	// Trusted is what the bundle holds, oldest first.
	Trusted []*x509.Certificate
	// Bundle is Trusted as PEM, and Hash what a host holding it reports.
	Bundle []byte
	Hash   string
	Pool   *x509.CertPool
	// Rows are the stored CAs, oldest first, keys included.
	Rows []*api.CertificateAuthority

	known []*x509.Certificate
}

// Known says whether c was issued by a CA this cluster has had, dropped ones
// and init's included: what tells the CP's own certificate from an external
// one.
func (c *CaSet) Known(leaf *x509.Certificate) bool {
	if c == nil {
		return false
	}
	for _, ca := range c.known {
		if pki.IssuedBy(leaf, ca) {
			return true
		}
	}

	return false
}

func (c *CaSet) row(state api.CertificateAuthorityState) *api.CertificateAuthority {
	for _, v := range c.Rows {
		if v.GetState() == state {
			return v
		}
	}

	return nil
}

// Current is the CA set, read again when it is older than half of
// `control.ca_settle`. Nil, without an error, where there is no CA at all:
// a deployment with external certificates only, or the sandbox before it
// seeded one. A set that cannot be read again leaves the last one standing.
func (a *Authorities) Current(ctx context.Context) (*CaSet, error) {
	a.mu.Lock()
	set, at := a.set, a.at
	a.mu.Unlock()
	if !at.IsZero() && a.d.now().Sub(at) < a.d.caSettle()/2 {
		return set, nil
	}

	fresh, err := a.Reload(ctx)
	if err != nil && set != nil {
		a.d.log().Warn("ca: reading the CA set again", "err", err.Error())
		return set, nil
	}

	return fresh, err
}

// Reload reads the set now.
func (a *Authorities) Reload(ctx context.Context) (*CaSet, error) {
	set, err := a.load(ctx)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.set, a.at = set, a.d.now()
	a.mu.Unlock()

	return set, nil
}

// invalidate has the next reading go to the rows.
func (a *Authorities) invalidate() {
	a.mu.Lock()
	a.at = time.Time{}
	a.mu.Unlock()
}

func (a *Authorities) load(ctx context.Context) (*CaSet, error) {
	if a.d.Own == nil {
		return a.unstored(), nil
	}
	vs, err := a.d.Own.CertificateAuthority().List(ctx, api.CertificateAuthorityListRequest_builder{Size: 100}.Build())
	if err != nil {
		return nil, err
	}
	if len(vs.GetItems()) == 0 {
		return a.unstored(), nil
	}

	set := &CaSet{Stored: true, Pool: x509.NewCertPool()}
	if a.d.CA != nil {
		set.known = append(set.known, a.d.CA.Cert)
	}
	seen := map[api.CertificateAuthorityState]bool{}
	for _, v := range vs.GetItems() {
		certs, err := pki.ParseCerts(v.GetCertificate())
		if err != nil || len(certs) == 0 {
			return nil, fmt.Errorf("ca %s: no certificate", v.GetAlias())
		}
		c := certs[0]
		set.known = append(set.known, c)
		st := v.GetState()
		if st == api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRED || seen[st] {
			// Two of a kind are two `ca rotate` at once on two processes:
			// the older stands, and the leader erases the other.
			continue
		}
		seen[st] = true
		set.Rows = append(set.Rows, v)
		set.Trusted = append(set.Trusted, c)
		set.Pool.AddCert(c)
		switch st {
		case api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE:
			kek := a.d.caKek()
			if kek == nil {
				return nil, errors.New("ca: no KEK to open the CA key with")
			}
			kb, err := kek.Unwrap(v.GetPrivateKey())
			if err != nil {
				return nil, fmt.Errorf("ca %s: %w", v.GetAlias(), err)
			}
			ca, err := pki.ParseCA(v.GetCertificate(), kb)
			if err != nil {
				return nil, fmt.Errorf("ca %s: %w", v.GetAlias(), err)
			}
			set.Issuer = ca
		case api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT:
			set.Next = c
		case api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRING:
			set.Retiring = c
		}
	}
	if set.Issuer == nil {
		return nil, errors.New("ca: the stored CA set has no active CA")
	}
	set.Bundle = pki.EncodeCerts(set.Trusted...)
	set.Hash = pki.BundleHash(set.Bundle)
	switch {
	case set.Retiring != nil:
		set.Phase = api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_REPLACING
	case set.Next != nil:
		set.Phase = api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_DISTRIBUTING
	default:
		set.Phase = api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE
	}

	return set, nil
}

// unstored is the set before any rollover: init's CA alone.
func (a *Authorities) unstored() *CaSet {
	ca := a.d.CA
	if ca == nil {
		return nil
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	bundle := ca.Bundle()

	return &CaSet{
		Phase:   api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE,
		Issuer:  ca,
		Trusted: []*x509.Certificate{ca.Cert},
		Bundle:  bundle,
		Hash:    pki.BundleHash(bundle),
		Pool:    pool,
		known:   []*x509.Certificate{ca.Cert},
	}
}

// renewForCa says whether a host that reports bundle hash `held` and holds
// `chain` (as its row records it) is to renew now for the CA set (§33.5):
// it holds a bundle other than the one hosts receive now, or a certificate
// from the CA a rollover is replacing. Before the first rollover nothing is
// asked, and a host that reports no hash is never asked for its bundle: a
// renewal would not change what it says.
func (s Core) renewForCa(ctx context.Context, held string, chain []byte) bool {
	set, err := s.d.Authorities().Current(ctx)
	if err != nil || set == nil || !set.Stored {
		return false
	}
	if held != "" && held != set.Hash {
		return true
	}

	return set.Retiring != nil && pki.IssuedBy(leafOf(chain), set.Retiring)
}

func leafOf(chain []byte) *x509.Certificate {
	cs, err := pki.ParseCerts(chain)
	if err != nil || len(cs) == 0 {
		return nil
	}

	return cs[0]
}

// caHost is a live host as a rollover sees it.
type caHost struct {
	id      pdid.Id
	kind    string
	alias   string
	hash    string
	chain   []byte
	version string
	seen    *time.Time
	expires *time.Time
}

// liveHosts are the hosts a rollover waits for: adopted, not erased, and
// holding a certificate that has not expired, of every kind.
func (s Core) liveHosts(ctx context.Context, now time.Time) ([]caHost, error) {
	adopted := int32(api.HostState_HOST_STATE_ADOPTED)
	var out []caHost
	ns, err := s.ent(ctx).Node.Query().Where(node.StateEQ(adopted), node.DateErasedIsNil(), node.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range ns {
		out = append(out, caHost{pdid.Id(v.Id), "node", v.Alias, v.CaHash, v.Join.GetCertificate(), v.Version, v.DateSeen, v.DateCertExpires})
	}
	rs, err := s.ent(ctx).Relay.Query().Where(relay.StateEQ(adopted), relay.DateErasedIsNil(), relay.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range rs {
		out = append(out, caHost{pdid.Id(v.Id), "relay", v.Alias, v.CaHash, v.Join.GetCertificate(), v.Version, v.DateSeen, v.DateCertExpires})
	}
	ss, err := s.ent(ctx).Satellite.Query().Where(satellite.StateEQ(adopted), satellite.DateErasedIsNil(), satellite.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range ss {
		out = append(out, caHost{pdid.Id(v.Id), "satellite", v.Alias, v.CaHash, v.Join.GetCertificate(), v.Version, v.DateSeen, v.DateCertExpires})
	}
	ps, err := s.ent(ctx).Producer.Query().Where(producer.StateEQ(adopted), producer.DateErasedIsNil(), producer.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range ps {
		out = append(out, caHost{pdid.Id(v.Id), "producer", v.Alias, v.CaHash, v.Join.GetCertificate(), v.Version, v.DateSeen, v.DateCertExpires})
	}
	ds, err := s.ent(ctx).Reader.Query().Where(reader.StateEQ(adopted), reader.DateErasedIsNil(), reader.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range ds {
		out = append(out, caHost{pdid.Id(v.Id), "reader", v.Alias, v.CaHash, v.Join.GetCertificate(), v.Version, v.DateSeen, v.DateCertExpires})
	}
	ws, err := s.ent(ctx).CameraWorker.Query().Where(cameraworker.StateEQ(adopted), cameraworker.DateErasedIsNil(), cameraworker.DateCertExpiresGT(now)).All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range ws {
		var chain []byte
		if v.Certificate != nil {
			chain = *v.Certificate
		}
		out = append(out, caHost{pdid.Id(v.Id), "camera-worker", v.Alias, v.CaHash, chain, v.Version, v.DateSeen, v.DateCertExpires})
	}

	return out, nil
}

// caWaiting are the live hosts the next step of a rollover waits for: one
// that does not report the bundle, or that holds a certificate from the CA
// being replaced. Nothing waits outside a rollover.
func (s Core) caWaiting(ctx context.Context, set *CaSet, now time.Time) (waiting []*api.CertificateAuthorityWaiting, live int, err error) {
	hosts, err := s.liveHosts(ctx, now)
	if err != nil {
		return nil, 0, err
	}
	for _, h := range hosts {
		if set.Phase == api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE {
			break
		}
		old := set.Retiring != nil && pki.IssuedBy(leafOf(h.chain), set.Retiring)
		if h.hash == set.Hash && !old {
			continue
		}
		w := api.CertificateAuthorityWaiting_builder{
			Id: h.id.Bytes(), Kind: h.kind, Alias: h.alias, CaHash: h.hash, OldCertificate: old, Version: h.version,
		}
		if h.seen != nil {
			w.DateSeen = timestamppb.New(*h.seen)
		}
		if h.expires != nil {
			w.DateCertExpires = timestamppb.New(*h.expires)
		}
		waiting = append(waiting, w.Build())
	}

	return waiting, len(hosts), nil
}

// caRollover is the leader's part (§33.5): begin a rollover a year before
// the active CA expires, then take each step once the live hosts caught up
// with the one before and it has stood for `control.ca_settle`.
func (s Core) caRollover(ctx context.Context, now time.Time) error {
	a := s.d.Authorities()
	set, err := a.Reload(ctx)
	if err != nil || set == nil {
		return err
	}
	settle := s.d.caSettle()
	switch set.Phase {
	case api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE:
		due := set.Issuer.Cert.NotAfter.Add(-s.d.caRolloverBefore())
		if now.Before(due) {
			return nil
		}
		v, err := s.startRollover(ctx, now)
		if err != nil {
			return err
		}
		s.d.log().Info("ca: rollover begun; the active CA expires soon", "next", v.GetAlias(), "fingerprint", v.GetFingerprint(), "expires", set.Issuer.Cert.NotAfter)

		return nil
	case api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_DISTRIBUTING:
		next := set.row(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT)
		if err := s.eraseSpare(ctx, set); err != nil {
			return err
		}
		if now.Sub(next.GetDateCreated().AsTime()) < settle {
			return nil
		}
		waiting, live, err := s.caWaiting(ctx, set, now)
		if err != nil || len(waiting) > 0 {
			return err
		}
		active := set.row(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE)
		err = s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
			if _, err := own.CertificateAuthority().Patch(ctx, api.CertificateAuthorityPatchRequest_builder{
				Ref:         api.CertificateAuthorityRef_builder{Id: active.GetId()}.Build(),
				State:       z.Ptr(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRING),
				DateUpdated: active.GetDateUpdated(),
			}.Build()); err != nil {
				return err
			}
			_, err := own.CertificateAuthority().Patch(ctx, api.CertificateAuthorityPatchRequest_builder{
				Ref:         api.CertificateAuthorityRef_builder{Id: next.GetId()}.Build(),
				State:       z.Ptr(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE),
				DateActive:  timestamppb.New(now),
				DateUpdated: next.GetDateUpdated(),
			}.Build())

			return err
		})
		if err != nil {
			return err
		}
		a.invalidate()
		s.d.log().Info("ca: every live host holds the bundle; the new CA issues now", "ca", next.GetAlias(), "live", live)
	case api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_REPLACING:
		active := set.row(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE)
		retiring := set.row(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRING)
		if since := active.GetDateActive(); since != nil && now.Sub(since.AsTime()) < settle {
			return nil
		}
		waiting, live, err := s.caWaiting(ctx, set, now)
		if err != nil || len(waiting) > 0 {
			return err
		}
		// The row stays, so the trail and `ca ls` can say what the cluster
		// trusted; the key goes, since nothing may sign with it again.
		if _, err := s.d.Own.CertificateAuthority().Patch(ctx, api.CertificateAuthorityPatchRequest_builder{
			Ref:         api.CertificateAuthorityRef_builder{Id: retiring.GetId()}.Build(),
			State:       z.Ptr(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRED),
			PrivateKey:  []byte{},
			DateRetired: timestamppb.New(now),
			DateUpdated: retiring.GetDateUpdated(),
		}.Build()); err != nil {
			return err
		}
		a.invalidate()
		s.d.log().Info("ca: no live host holds a certificate from the old CA; it is dropped", "ca", retiring.GetAlias(), "live", live)
	}

	return nil
}

// eraseSpare erases a second NEXT row, which only two rotations begun at
// once on two processes leave; Load already stood by the older.
func (s Core) eraseSpare(ctx context.Context, set *CaSet) error {
	vs, err := s.d.Own.CertificateAuthority().List(ctx, api.CertificateAuthorityListRequest_builder{Size: 100}.Build())
	if err != nil {
		return err
	}
	for _, v := range vs.GetItems() {
		if v.GetState() != api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT || bytes.Equal(v.GetId(), set.row(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT).GetId()) {
			continue
		}
		if _, err := s.d.Own.CertificateAuthority().Erase(ctx, api.CertificateAuthorityRef_builder{Id: v.GetId()}.Build()); err != nil {
			return err
		}
	}

	return nil
}

// caAlias names a CA row by its fingerprint, so the CA init made is
// recorded once however many processes record it at the same time.
func caAlias(c *x509.Certificate) string {
	return "ca-" + strings.TrimPrefix(pki.Fingerprint(c), "sha256:")[:12]
}

// startRollover makes the next CA (§33.5): from now on every bundle carries
// it. The CA init made is recorded first when nothing is stored yet.
func (s Core) startRollover(ctx context.Context, now time.Time) (*api.CertificateAuthority, error) {
	kek := s.d.caKek()
	if kek == nil {
		return nil, failed("this control plane has no KEK to keep a CA key under; run `shale init`")
	}

	var out *api.CertificateAuthority
	err := s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
		vs, err := own.CertificateAuthority().List(ctx, api.CertificateAuthorityListRequest_builder{Size: 100}.Build())
		if err != nil {
			return err
		}
		var active *api.CertificateAuthority
		for _, v := range vs.GetItems() {
			switch v.GetState() {
			case api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT, api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRING:
				return failed("a CA rollover is under way (%s); `shale ca status` says where", v.GetAlias())
			case api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE:
				active = v
			}
		}
		if active == nil {
			// The first rollover: init's CA becomes a row, so that it can
			// be told to stop issuing and dropped like any other.
			init := s.d.CA
			if init == nil {
				return failed("this control plane has no CA to roll over")
			}
			if _, err := s.addCa(ctx, own, init, api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE, init.Cert.NotBefore, kek); err != nil {
				return err
			}
		}
		next, err := pki.NewCA("shale-ca "+now.UTC().Format("2006-01-02T15:04:05Z"), now)
		if err != nil {
			return err
		}
		out, err = s.addCa(ctx, own, next, api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT, now, kek)

		return err
	})
	if err != nil {
		return nil, err
	}
	s.d.Authorities().invalidate()
	out.SetPrivateKey(nil)

	return out, nil
}

func (s Core) addCa(ctx context.Context, own api.Server, ca *pki.CA, state api.CertificateAuthorityState, created time.Time, kek Kek) (*api.CertificateAuthority, error) {
	kb, err := pki.EncodeKey(ca.Key)
	if err != nil {
		return nil, err
	}
	wrapped, err := kek.Wrap(kb)
	if err != nil {
		return nil, err
	}
	add := api.CertificateAuthorityAddRequest_builder{
		Alias:       caAlias(ca.Cert),
		Name:        ca.Cert.Subject.CommonName,
		Certificate: ca.Bundle(),
		PrivateKey:  wrapped,
		State:       state,
		Fingerprint: pki.Fingerprint(ca.Cert),
		DateExpires: timestamppb.New(ca.Cert.NotAfter),
		DateCreated: timestamppb.New(created),
	}
	if state == api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE {
		add.DateActive = timestamppb.New(created)
	}

	return own.CertificateAuthority().Add(ctx, add.Build())
}

// ---- the service ---------------------------------------------------------

type coreCertificateAuthority struct {
	Core
	api.CertificateAuthorityServiceServer
}

func (s Core) CertificateAuthority() api.CertificateAuthorityServiceServer {
	return coreCertificateAuthority{s, s.Next().CertificateAuthority()}
}

// Rotate begins a rollover now (§33.5): `shale ca rotate`.
func (s coreCertificateAuthority) Rotate(ctx context.Context, _ *api.CertificateAuthorityRotateRequest) (*api.CertificateAuthority, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	v, err := s.startRollover(ctx, s.d.now())
	if err != nil {
		return nil, err
	}
	s.d.log().InfoContext(ctx, "ca: rollover begun by `ca rotate`", "next", v.GetAlias(), "fingerprint", v.GetFingerprint())

	return v, nil
}

// Status is `shale ca status`: the phase, the CAs, the bundle, and the live
// hosts the next step waits for.
func (s coreCertificateAuthority) Status(ctx context.Context, _ *api.CertificateAuthorityStatusRequest) (*api.CertificateAuthorityStatusResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	set, err := s.d.Authorities().Reload(ctx)
	if err != nil {
		return nil, err
	}
	if set == nil {
		return nil, failed("this control plane has no CA: its certificates are external")
	}
	now := s.d.now()
	waiting, live, err := s.caWaiting(ctx, set, now)
	if err != nil {
		return nil, err
	}
	out := api.CertificateAuthorityStatusResponse_builder{
		Phase:        set.Phase,
		Bundle:       string(set.Bundle),
		BundleHash:   set.Hash,
		DateRollover: timestamppb.New(set.Issuer.Cert.NotAfter.Add(-s.d.caRolloverBefore())),
		Live:         int32(live),
		Waiting:      waiting,
	}
	if set.Stored {
		vs, err := s.d.Own.CertificateAuthority().List(ctx, api.CertificateAuthorityListRequest_builder{Size: 100}.Build())
		if err != nil {
			return nil, err
		}
		for _, v := range vs.GetItems() {
			v.SetPrivateKey(nil)
			out.Authorities = append(out.Authorities, v)
		}
	} else {
		c := set.Issuer.Cert
		out.Authorities = []*api.CertificateAuthority{api.CertificateAuthority_builder{
			Alias:       caAlias(c),
			Name:        c.Subject.CommonName,
			Certificate: set.Bundle,
			State:       api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE,
			Fingerprint: pki.Fingerprint(c),
			DateExpires: timestamppb.New(c.NotAfter),
		}.Build()}
	}

	return out.Build(), nil
}
