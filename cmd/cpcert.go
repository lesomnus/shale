package cmd

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// The CP's own side of its CA (§33.5): the certificate it presents, and
// what it verifies hosts against, both following the CA set as a rollover
// changes it.

// cpCert is what the CP presents, to a client of its listeners and to a
// node's control API (§35.7). It is the pair the CP was configured with --
// what `shale init` issued, or `control.cert_file` -- while that is not
// from the built-in CA at all, or is from the CA that issues now and short
// of its renewal. Otherwise this process issues itself one from the CA that
// issues now, for the same key and names, and keeps it until that one is
// due in turn: so the CP moves to the new CA when the hosts' certificates
// do, and renews its own certificate, without a file to write. Every CP
// process does it for itself.
type cpCert struct {
	cas   *core.Authorities
	file  *tls.Certificate
	leaf  *x509.Certificate
	names pki.Names
	now   func() time.Time
	log   *slog.Logger

	mu     sync.Mutex
	issued *tls.Certificate
}

func newCpCert(cas *core.Authorities, certFile, keyFile string, extra []string, now func() time.Time, log *slog.Logger) (*cpCert, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	pair.Leaf = leaf
	if _, ok := pair.PrivateKey.(crypto.Signer); !ok {
		return nil, errors.New("control plane key: not a signing key")
	}
	// The names it was issued for, and those this process would give it.
	dns, ips := Hostnames(extra)
	names := pki.Names{DNS: slices.Clone(leaf.DNSNames), IPs: slices.Clone(leaf.IPAddresses)}
	for _, v := range dns {
		if !slices.Contains(names.DNS, v) {
			names.DNS = append(names.DNS, v)
		}
	}
	for _, v := range ips {
		if !slices.ContainsFunc(names.IPs, func(ip net.IP) bool { return ip.Equal(v) }) {
			names.IPs = append(names.IPs, v)
		}
	}
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}

	return &cpCert{cas: cas, file: &pair, leaf: leaf, names: names, now: now, log: log}, nil
}

// Certificate is the pair to present now.
func (c *cpCert) Certificate() *tls.Certificate {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	set, err := c.cas.Current(ctx)
	if err != nil || set == nil || !set.Known(c.leaf) {
		// An external certificate, or nothing to issue from.
		return c.file
	}
	now := c.now()
	if presentable(c.leaf, set, now) {
		return c.file
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.issued != nil && presentable(c.issued.Leaf, set, now) {
		return c.issued
	}
	leaf, err := set.Issuer.IssueServer(c.file.PrivateKey.(crypto.Signer).Public(), pki.CpName, c.names, pki.CpLifetime, now)
	if err != nil {
		c.log.Warn("control plane certificate: issuing one from the active CA", "err", err.Error())
		if c.issued != nil {
			return c.issued
		}

		return c.file
	}
	c.issued = &tls.Certificate{Certificate: [][]byte{leaf.Raw, set.Issuer.Cert.Raw}, PrivateKey: c.file.PrivateKey, Leaf: leaf}
	c.log.Info("control plane certificate: issued from the active CA", "serial", pki.Serial(leaf), "ca", pki.Fingerprint(set.Issuer.Cert), "expires", leaf.NotAfter)

	return c.issued
}

// presentable says whether leaf is still the one to present: from the CA
// that issues now, and short of its renewal.
func presentable(leaf *x509.Certificate, set *core.CaSet, now time.Time) bool {
	return pki.IssuedBy(leaf, set.Issuer.Cert) && now.Before(pki.RenewAt(leaf))
}

// ClientCertificate is what the CP presents to a node's control API: the
// pair it presents now, unless the node asks for certificates from CAs only
// the configured pair is from -- a node of an earlier release, which read
// its bundle when it started and so does not know the new CA until it
// restarts.
func (c *cpCert) ClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	now := c.Certificate()
	if cri.SupportsCertificate(now) == nil {
		return now, nil
	}
	if cri.SupportsCertificate(c.file) == nil && c.now().Before(c.leaf.NotAfter) {
		return c.file, nil
	}

	return now, nil
}

// trustPool is what the CP verifies a host's certificate against: every CA
// in the bundle, the next and the retiring one during a rollover. Init's CA
// alone when the set cannot be read yet, as before there was a set; nil
// where there is no CA.
func (s *Server) trustPool() *x509.CertPool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	set, err := s.Deps.Authorities().Current(ctx)
	if err == nil && set != nil {
		return set.Pool
	}
	if s.CA == nil {
		return nil
	}
	pool := x509.NewCertPool()
	pool.AddCert(s.CA.Cert)

	return pool
}

// trustCurrent has cfg verify client certificates against the CA set as it
// is at each handshake, rather than as it was when the listener started.
func (s *Server) trustCurrent(cfg *tls.Config) {
	cfg.ClientCAs = s.trustPool()
	base := cfg.Clone()
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		pool := s.trustPool()
		if pool == nil {
			return nil, nil
		}
		c := base.Clone()
		c.ClientCAs = pool

		return c, nil
	}
}

// casTransport is an HTTP transport toward the nodes' data planes that
// trusts the system's roots and the CA set as it is now: it is made again
// when the bundle changes, so a node serving a certificate from a new CA is
// verified.
type casTransport struct {
	cas  *core.Authorities
	make func(roots *x509.CertPool) *http.Transport

	mu   sync.Mutex
	hash string
	rt   *http.Transport
}

func (t *casTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.current(r.Context()).RoundTrip(r)
}

func (t *casTransport) current(ctx context.Context) *http.Transport {
	set, _ := t.cas.Current(ctx)
	hash := ""
	if set != nil {
		hash = set.Hash
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rt != nil && hash == t.hash {
		return t.rt
	}
	roots, err := pki.SystemRoots()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	} else {
		roots = roots.Clone()
	}
	if set != nil {
		for _, c := range set.Trusted {
			roots.AddCert(c)
		}
	}
	if t.rt != nil {
		// What is under way finishes on the old one.
		t.rt.CloseIdleConnections()
	}
	t.rt, t.hash = t.make(roots), hash

	return t.rt
}
