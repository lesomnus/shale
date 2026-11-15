// Package hostagent is what every host does alike (§33.4): keep a key in its
// state directory, join the Control Plane until adopted, hold the certificate
// it was issued, renew it, and dial the CP with it.
package hostagent

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostid"
	"github.com/lesomnus/shale/internal/pki"
)

// JoinInterval is how often a host asks again while pending (§33.4).
const JoinInterval = 10 * time.Second

// Version is stamped by the build.
var Version = "dev"

// Agent is one host's identity handling.
type Agent struct {
	// HardwareId overrides the machine's identity when set (§33.4).
	HardwareId string
	Kind       pdid.Domain
	Store      pki.Store
	// Cp is the Control Plane address: host:port, or a URL with scheme
	// `https` (the default) or `http` (development only).
	Cp string
	// CaHash makes the first-contact pin a check.
	CaHash string
	// CpRoots are CAs the operator trusts explicitly for the CP's server
	// certificate, a public or corporate CA its `control.cert_file` comes
	// from: trusted beside the pinned bundle, and in place of the
	// first-contact pin, which is then not taken (§33.5). Nil, the default,
	// is the pinned bundle alone.
	CpRoots *x509.CertPool
	// JoinEvery is how often Join asks again while pending; JoinInterval
	// when zero.
	JoinEvery time.Duration
	// Dev allows plaintext to the CP.
	Dev bool
	Log *slog.Logger

	key      *pki.HostKey
	id       pdid.Id
	hostname string
	hw       hostid.Identity

	// trust is the bundle trustOf as a pool (TrustPool).
	trustMu sync.Mutex
	trust   *x509.CertPool
	trustOf []byte
}

// Init reads the key and the hardware identity.
func (a *Agent) Init() error {
	k, err := a.Store.Key()
	if err != nil {
		return err
	}
	a.key = k

	hw, err := hostid.ReadOr(a.Store.Dir)
	if err != nil {
		return err
	}
	if a.HardwareId != "" {
		// Declared: a second host in one process (a test, a lab) must not
		// share the machine's identity.
		hw = hostid.Identity{Id: a.HardwareId, Kind: api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID}
	}
	a.hw = hw
	a.hostname, _ = os.Hostname()
	if a.Log == nil {
		a.Log = slog.Default()
	}

	// The certificate names the row, so a host that has one knows its id.
	if chain, err := a.Store.Cert(); err == nil && len(chain) > 0 {
		if id, ok := pki.IdOf(chain[0]); ok {
			a.id = id
		}
	}

	return nil
}

// Id is the host's row, Nil until adopted.
func (a *Agent) Id() pdid.Id { return a.id }

// Hostname is what the host calls itself.
func (a *Agent) Hostname() string { return a.hostname }

// Fingerprint is what the host prints beside "waiting for adoption".
func (a *Agent) Fingerprint() string { return a.key.Fingerprint() }

// HostJoin is the request every Join carries.
func (a *Agent) HostJoin() (*api.HostJoin, error) {
	csr, err := a.key.CSR(a.hostname)
	if err != nil {
		return nil, err
	}

	return api.HostJoin_builder{
		HardwareId:     a.hw.Id,
		HardwareIdKind: a.hw.Kind,
		Hostname:       a.hostname,
		Csr:            csr,
		KeyFingerprint: a.key.Fingerprint(),
		DateJoined:     timestamppb.Now(),
		Version:        Version,
	}.Build(), nil
}

// Certificate is the chain the host holds, nil before adoption.
func (a *Agent) Certificate() (*tls.Certificate, error) {
	pem, err := a.Store.CertPEM()
	if err != nil || len(pem) == 0 {
		return nil, err
	}
	kb, err := pki.EncodeKey(a.key.Key)
	if err != nil {
		return nil, err
	}
	c, err := tls.X509KeyPair(pem, kb)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// Leaf is the host certificate itself, for its serial and expiry.
func (a *Agent) Leaf() (*x509.Certificate, error) {
	chain, err := a.Store.Cert()
	if err != nil || len(chain) == 0 {
		return nil, err
	}

	return chain[0], nil
}

// endpoint splits the CP address into what to dial and whether to use TLS.
func (a *Agent) endpoint() (addr string, plain bool, err error) {
	v := a.Cp
	if v == "" {
		return "", false, errors.New("no control plane address; set `cp` or pass --cp")
	}
	if !strings.Contains(v, "://") {
		v = "https://" + v
	}
	u, err := url.Parse(v)
	if err != nil {
		return "", false, err
	}
	addr = u.Host
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "7400")
	}
	plain = u.Scheme == "http"
	if plain && !a.Dev {
		return "", false, errors.New("a plaintext control plane address needs --dev")
	}

	return addr, plain, nil
}

// tlsConfig verifies the CP against the pinned bundle, or pins on first
// contact (§33.4), and presents the host certificate when it has one.
func (a *Agent) tlsConfig() (*tls.Config, error) {
	return a.tlsConfigPresent(true)
}

func (a *Agent) tlsConfigPresent(present bool) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if present {
		if cert, err := a.Certificate(); err == nil && cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
	}

	bundle, err := a.Store.Bundle()
	if err != nil {
		return nil, err
	}
	if a.CpRoots != nil {
		// Trusted explicitly: verified like any server, by its name, with
		// the bundle beside so a CP serving its shale certificate still
		// passes; nothing is pinned.
		pool := a.CpRoots.Clone()
		if len(bundle) > 0 && !pool.AppendCertsFromPEM(bundle) {
			return nil, errors.New("pki: no certificate in the bundle")
		}
		cfg.RootCAs = pool

		return cfg, nil
	}
	if len(bundle) > 0 {
		pool, err := pki.Pool(bundle)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool

		return cfg, nil
	}

	// First contact: accept what is presented, check it against ca_hash
	// when given, and pin it.
	cfg.InsecureSkipVerify = true
	cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("no certificate presented")
		}
		var chain []*x509.Certificate
		for _, b := range raw {
			c, err := x509.ParseCertificate(b)
			if err != nil {
				return err
			}
			chain = append(chain, c)
		}
		root := chain[len(chain)-1]
		if !root.IsCA {
			return errors.New("the control plane presented no CA in its chain; set control.names or ca_hash")
		}
		if a.CaHash != "" && !strings.EqualFold(pki.Fingerprint(root), a.CaHash) {
			return fmt.Errorf("the control plane's CA is %s, not the expected %s", pki.Fingerprint(root), a.CaHash)
		}
		if err := a.Store.SetBundle(pki.EncodeCerts(root)); err != nil {
			return err
		}
		a.Log.Info("pinned the control plane's CA", "fingerprint", pki.Fingerprint(root))

		return nil
	}

	return cfg, nil
}

// Dial connects to the CP: mTLS with the host certificate when it has one,
// plaintext with the plain header in development.
func (a *Agent) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	return a.dial(ctx, true)
}

// DialBootstrap is only for a public Join. An expired certificate is omitted so
// TLS can reach that public endpoint; ordinary Dial never weakens machine auth.
// The public RPC must independently verify the previously approved key.
func (a *Agent) DialBootstrap(ctx context.Context) (*grpc.ClientConn, error) {
	return a.dial(ctx, a.Ready(time.Now()))
}

func (a *Agent) dial(ctx context.Context, present bool) (*grpc.ClientConn, error) {
	addr, plain, err := a.endpoint()
	if err != nil {
		return nil, err
	}

	opts := []grpc.DialOption{Keepalive()}
	if plain {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if present && !a.id.IsZero() {
			opts = append(opts, auth.Inject(auth.PlainProvider(a.id.String()))...)
		}
	} else {
		cfg, err := a.tlsConfigPresent(present)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	}

	return grpc.NewClient(addr, opts...)
}

// DialAddr opens a gRPC connection to another host (a relay) that trusts
// the pinned CA and the system's roots (§33.5) and presents the host
// certificate; `plain` is development mode.
func (a *Agent) DialAddr(ctx context.Context, addr string, plain bool) (*grpc.ClientConn, error) {
	if plain {
		return grpc.NewClient(addr, Keepalive(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cfg, err := a.PeerTLS()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		// Never first contact: that pins the CP's CA, not another host's.
		return nil, errors.New("no CA pinned yet to verify another host against; join the control plane first")
	}

	return grpc.NewClient(addr, Keepalive(), grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
}

// PeerTLS is the client TLS toward another host's server, a node's data
// plane or a relay: it trusts the pinned CA and the system's roots, so a
// node or relay serving a certificate from a public CA verifies as well as
// one serving its host certificate (§33.5), and it presents the host
// certificate held at each handshake, so a renewed one is presented
// without a restart. Nil before a CA is pinned. The CP connection is not
// this: it trusts the pinned bundle alone, or pins on first contact
// (tlsConfig).
func (a *Agent) PeerTLS() (*tls.Config, error) {
	bundle, err := a.Store.Bundle()
	if err != nil {
		return nil, err
	}
	if len(bundle) == 0 {
		return nil, nil
	}
	pool, err := pki.PeerPool(bundle)
	if err != nil {
		return nil, err
	}

	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, GetClientCertificate: a.clientCertificate}, nil
}

// clientCertificate is the host certificate for a server that asks for
// one, when it is from a CA the server names; none otherwise, as Go does
// with a fixed certificate. A node of an earlier release trusts the bundle
// it started with, and is sent no certificate from a CA it has not heard
// of: the data planes authenticate by token, and a certificate is checked
// only when one is given (§33.5).
func (a *Agent) clientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cert, err := a.Certificate()
	if err != nil || cert == nil || cri.SupportsCertificate(cert) != nil {
		return &tls.Certificate{}, nil
	}

	return cert, nil
}

// TrustPool is the bundle held as a pool, read again when the bundle
// changes: what a server of this host verifies a client certificate
// against at each handshake, so a bundle a renewal brought -- the next CA
// of a rollover (§33.5) -- is trusted without a restart. Nil before a
// bundle is pinned.
func (a *Agent) TrustPool() (*x509.CertPool, error) {
	bundle, err := a.Store.Bundle()
	if err != nil || len(bundle) == 0 {
		return nil, err
	}
	a.trustMu.Lock()
	defer a.trustMu.Unlock()
	if a.trust != nil && bytes.Equal(a.trustOf, bundle) {
		return a.trust, nil
	}
	pool, err := pki.Pool(bundle)
	if err != nil {
		return nil, err
	}
	a.trust, a.trustOf = pool, bundle

	return pool, nil
}

// The keepalive between hosts and the CP, and producers and relays
// (§38.6): a link that stalls, a producer on WiFi above all, is noticed
// in seconds and redialed, rather than found out by the next call's
// deadline and ended by the server a minute later.
const (
	KeepaliveTime    = 10 * time.Second
	KeepaliveTimeout = 5 * time.Second
)

// Keepalive is the client side: a ping every KeepaliveTime on an idle
// connection too, answered within KeepaliveTimeout or the connection is
// closed and the next call redials.
func Keepalive() grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: KeepaliveTime, Timeout: KeepaliveTimeout, PermitWithoutStream: true})
}

// KeepaliveServer is the server side of the same: it permits the client's
// pings, and pings a silent client itself so a dead one is closed.
func KeepaliveServer() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: KeepaliveTime / 2, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 3 * KeepaliveTime, Timeout: 2 * KeepaliveTimeout}),
	}
}

// HTTPClient is a client for the data planes: it trusts the pinned CA and
// the system's roots, and presents the host certificate, since a node
// verifies a client certificate when one is given (§33.5). Its transport
// is made again when the bundle held changes, so a host that runs through
// a CA rollover verifies the nodes' certificates from the new CA.
func (a *Agent) HTTPClient() (*http.Client, error) {
	t := &peerTransport{a: a}
	if _, err := t.current(); err != nil {
		return nil, err
	}

	return &http.Client{Transport: t}, nil
}

// peerTransport is HTTPClient's transport: one http.Transport for the
// bundle held, replaced when it changes.
type peerTransport struct {
	a *Agent

	mu     sync.Mutex
	bundle []byte
	rt     *http.Transport
}

func (t *peerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt, err := t.current()
	if err != nil {
		return nil, err
	}

	return rt.RoundTrip(r)
}

// CloseIdleConnections is what http.Client asks of a transport that has it.
func (t *peerTransport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rt != nil {
		t.rt.CloseIdleConnections()
	}
}

func (t *peerTransport) current() (*http.Transport, error) {
	bundle, err := t.a.Store.Bundle()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rt != nil && bytes.Equal(bundle, t.bundle) {
		return t.rt, nil
	}
	tr := &http.Transport{
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	cfg, err := t.a.PeerTLS()
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		tr.TLSClientConfig = cfg
	}
	if t.rt != nil {
		// What is under way finishes on the old one.
		t.rt.CloseIdleConnections()
	}
	t.rt, t.bundle = tr, bundle

	return tr, nil
}

// Answer is what a kind's Join RPC hands back to the loop.
type Answer struct {
	*api.JoinAnswer
	Keys []*api.KeyEntry
}

// JoinFunc calls the kind's Join RPC with the request.
type JoinFunc func(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (Answer, error)

// Join asks until adopted, storing the certificate and the bundle, and
// answers the final adoption.
func (a *Agent) Join(ctx context.Context, call JoinFunc) (Answer, error) {
	return a.join(ctx, call, a.Dial)
}

// JoinBootstrap opts a role into public same-key recovery after expiry. Existing
// host roles retain their original Join behavior unless they explicitly use it.
func (a *Agent) JoinBootstrap(ctx context.Context, call JoinFunc) (Answer, error) {
	return a.join(ctx, call, a.DialBootstrap)
}

func (a *Agent) join(ctx context.Context, call JoinFunc, dial func(context.Context) (*grpc.ClientConn, error)) (Answer, error) {
	said := false
	for {
		conn, err := dial(ctx)
		if err != nil {
			return Answer{}, err
		}

		hj, err := a.HostJoin()
		if err != nil {
			conn.Close()
			return Answer{}, err
		}

		ans, err := call(ctx, conn, hj)
		conn.Close()
		if err != nil {
			a.Log.Warn("join", "err", err.Error())
		} else if ans.JoinAnswer.GetState() == api.HostState_HOST_STATE_ADOPTED && len(ans.JoinAnswer.GetCertificate()) > 0 {
			id, err := pdid.From(ans.JoinAnswer.GetId())
			if err != nil {
				return Answer{}, err
			}
			leaf, err := a.verifyIssued(ans.JoinAnswer.GetCertificate(), ans.JoinAnswer.GetCaBundle(), id)
			if err != nil {
				// Not stored: what this host holds stays as it was, and it
				// asks again.
				a.Log.Warn("join: the certificate handed back does not verify", "err", err.Error())
			} else {
				if err := a.Store.SetCert(ans.JoinAnswer.GetCertificate(), ans.JoinAnswer.GetCaBundle()); err != nil {
					return Answer{}, err
				}
				a.id = id
				a.Log.Info("adopted", "id", id.String(), "alias", ans.JoinAnswer.GetAlias(),
					"serial", pki.Serial(leaf), "expires", leaf.NotAfter)

				return ans, nil
			}
		} else if !said {
			a.Log.Info("waiting for adoption",
				"alias", ans.JoinAnswer.GetAlias(),
				"hardware", a.hw.Id,
				"key", a.key.Fingerprint(),
				"message", ans.JoinAnswer.GetMessage())
			said = true
		}

		every := a.JoinEvery
		if every <= 0 {
			every = JoinInterval
		}
		select {
		case <-ctx.Done():
			return Answer{}, ctx.Err()
		case <-time.After(every):
		}
	}
}

// verifyIssued checks a certificate the CP handed over before it replaces
// the one held: it is for this host's key, it names row `id`, and it chains
// to the bundle it came with (the pinned one when it came with none) as a
// client certificate. The chain is checked as of now, or of the nearest
// moment the certificate is valid at when this host's clock is outside it:
// a host whose clock is behind still takes what was just issued, and
// whether a certificate has expired is Ready's question, not this one's.
func (a *Agent) verifyIssued(chainPEM, bundlePEM []byte, id pdid.Id) (*x509.Certificate, error) {
	chain, err := pki.ParseCerts(chainPEM)
	if err != nil {
		return nil, err
	}
	if len(chain) == 0 {
		return nil, errors.New("no certificate")
	}
	leaf := chain[0]
	if !a.certifies(leaf) {
		return nil, errors.New("the certificate is not for this host's key")
	}
	if got, ok := pki.IdOf(leaf); !ok || got != id {
		return nil, fmt.Errorf("the certificate names %s, not %s", got, id)
	}
	if len(bundlePEM) == 0 {
		if bundlePEM, err = a.Store.Bundle(); err != nil {
			return nil, err
		}
	}
	roots, err := pki.Pool(bundlePEM)
	if err != nil {
		return nil, err
	}
	at := time.Now()
	if at.Before(leaf.NotBefore) {
		at = leaf.NotBefore
	} else if at.After(leaf.NotAfter) {
		at = leaf.NotAfter
	}
	opts := x509.VerifyOptions{
		Roots:         roots,
		Intermediates: x509.NewCertPool(),
		CurrentTime:   at,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, c := range chain[1:] {
		opts.Intermediates.AddCert(c)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return nil, err
	}

	return leaf, nil
}

// certifies says whether a certificate is for this host's current key.
func (a *Agent) certifies(c *x509.Certificate) bool {
	pub, ok := c.PublicKey.(interface{ Equal(crypto.PublicKey) bool })

	return ok && pub.Equal(&a.key.Key.PublicKey)
}

// Expired says whether the host holds a certificate for its current key
// that has run out (§33.4). Such a host goes back to Join with a new key
// (Rekey) and is adopted again; one already rekeyed holds an expired
// certificate for its old key, and this is false.
func (a *Agent) Expired(now time.Time) bool {
	leaf, err := a.Leaf()
	if err != nil || leaf == nil {
		return false
	}

	return !now.Before(leaf.NotAfter) && a.certifies(leaf)
}

// Rekey replaces the host key (§33.4): a host whose certificate expired
// cannot renew it, and Join with the key the CP knows would hand the same
// expired certificate back. With a new key it appears as "known host, new
// key" under its row, and one adopt puts it back. The expired certificate
// stays until the new one replaces it, so the host still knows its row.
func (a *Agent) Rekey() error {
	k, err := a.Store.Rotate()
	if err != nil {
		return err
	}
	a.key = k

	return nil
}

// Ready says whether the host holds a certificate it can still use.
func (a *Agent) Ready(now time.Time) bool {
	leaf, err := a.Leaf()
	if err != nil || leaf == nil {
		return false
	}

	return now.Before(leaf.NotAfter) && !a.id.IsZero()
}

// NeedsRenewal is two thirds of the way through the certificate (§33.5).
func (a *Agent) NeedsRenewal(now time.Time) bool {
	leaf, err := a.Leaf()
	if err != nil || leaf == nil {
		return false
	}

	return now.After(pki.RenewAt(leaf))
}

// Renew asks the kind's RenewCertificate with a CSR for the current key and
// stores the answer.
func (a *Agent) Renew(ctx context.Context, conn *grpc.ClientConn, call func(ctx context.Context, conn *grpc.ClientConn, csr []byte) (cert, bundle []byte, err error)) error {
	csr, err := a.key.CSR(a.hostname)
	if err != nil {
		return err
	}
	cert, bundle, err := call(ctx, conn, csr)
	if err != nil {
		return err
	}
	if _, err := a.verifyIssued(cert, bundle, a.id); err != nil {
		return fmt.Errorf("the renewed certificate does not verify: %w", err)
	}

	return a.Store.SetCert(cert, bundle)
}

// Interfaces lists the machine's interfaces and addresses, which a node or
// relay reports (§34.10).
func Interfaces() []*api.HostInterface {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []*api.HostInterface
	for _, i := range ifs {
		if i.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		var vs []string
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && !n.IP.IsLinkLocalUnicast() {
				vs = append(vs, n.IP.String())
			}
		}
		if len(vs) > 0 {
			out = append(out, api.HostInterface_builder{Name: i.Name, Addresses: vs}.Build())
		}
	}

	return out
}

// BundleHash is what heartbeats report about the bundle held.
func (a *Agent) BundleHash() string {
	b, err := a.Store.Bundle()
	if err != nil || len(b) == 0 {
		return ""
	}

	return pki.BundleHash(b)
}
