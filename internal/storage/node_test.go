package storage

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
)

// The data plane's certificate is a pair: one file without the other is a
// mistake in the configuration, said at start rather than at the first
// browser.
func TestCertConfig(t *testing.T) {
	dir := t.TempDir()
	_, err := New(Config{StateDir: dir, CertFile: "/etc/shale/storage/tls.crt"})
	require.ErrorContains(t, err, "storage.key_file")
	_, err = New(Config{StateDir: dir, KeyFile: "/etc/shale/storage/tls.key"})
	require.ErrorContains(t, err, "storage.cert_file")
	_, err = New(Config{StateDir: dir, CertFile: filepath.Join(dir, "missing.crt"), KeyFile: filepath.Join(dir, "missing.key")})
	require.ErrorContains(t, err, "storage.cert_file")
	_, err = New(Config{StateDir: dir})
	require.NoError(t, err)
	// Development mode is plaintext: the pair is not even read.
	_, err = New(Config{StateDir: dir, Dev: true, CertFile: filepath.Join(dir, "missing.crt"), KeyFile: filepath.Join(dir, "missing.key")})
	require.NoError(t, err)
}

// writeServerPair issues a server certificate for name from ca and writes
// it and its key into dir, as cert-manager's Secret holds them.
func writeServerPair(t *testing.T, ca *pki.CA, dir, name string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert, err := ca.IssueServer(&key.PublicKey, name, pki.Names{DNS: []string{name}}, time.Hour, time.Now())
	require.NoError(t, err)
	kb, err := pki.EncodeKey(key)
	require.NoError(t, err)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certFile, pki.EncodeCerts(cert, ca.Cert), 0o644))
	require.NoError(t, os.WriteFile(keyFile, kb, 0o600))

	return certFile, keyFile
}

// adopted is a node holding a host certificate from the shale CA, as a
// join leaves it.
func adopted(t *testing.T, cfg Config) (*Node, *pki.CA) {
	t.Helper()
	cfg.StateDir = t.TempDir()
	n, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, n.agent.Init())
	ca, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	hj, err := n.agent.HostJoin()
	require.NoError(t, err)
	cert, err := ca.IssueHost(hj.GetCsr(), pdid.New(DomNode), pki.Names{IPs: []net.IP{net.ParseIP("127.0.0.1")}}, time.Now())
	require.NoError(t, err)
	require.NoError(t, n.agent.Store.SetCert(pki.EncodeCerts(cert), ca.Bundle()))

	return n, ca
}

// hostClient is a host certificate from ca, as a producer presents it.
func hostClient(t *testing.T, ca *pki.CA) tls.Certificate {
	t.Helper()
	a := &hostagent.Agent{Kind: 13, Store: pki.Store{Dir: t.TempDir()}}
	require.NoError(t, a.Init())
	hj, err := a.HostJoin()
	require.NoError(t, err)
	cert, err := ca.IssueHost(hj.GetCsr(), pdid.New(13), pki.Names{}, time.Now())
	require.NoError(t, err)
	require.NoError(t, a.Store.SetCert(pki.EncodeCerts(cert), ca.Bundle()))
	c, err := a.Certificate()
	require.NoError(t, err)

	return *c
}

type shook struct {
	served   *x509.Certificate
	verified [][]*x509.Certificate
	err      error
}

// handshake is what a listener with cfg presents to a client that trusts
// roots, dials name and presents client (when it has one), and the client
// chains the listener verified.
func handshake(t *testing.T, cfg *tls.Config, roots *x509.CertPool, name string, client *tls.Certificate) shook {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	srv := make(chan tls.ConnectionState, 1)
	go func() {
		s := tls.Server(a, cfg)
		if s.Handshake() == nil {
			srv <- s.ConnectionState()
		}
		close(srv)
		s.Close()
	}()
	ccfg := &tls.Config{RootCAs: roots, ServerName: name}
	if client != nil {
		// Sent whatever CAs the server names, as a stranger would.
		ccfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return client, nil }
	}
	c := tls.Client(b, ccfg)
	if err := c.Handshake(); err != nil {
		return shook{err: err}
	}
	// TLS 1.3: the server's verdict on the client certificate arrives
	// with the first read.
	go c.Read(make([]byte, 1))
	st, ok := <-srv
	if !ok {
		return shook{err: net.ErrClosed}
	}

	return shook{served: c.ConnectionState().PeerCertificates[0], verified: st.VerifiedChains}
}

// With an external certificate, the data plane serves it to a client that
// asks for its name, a browser for Playback and Export, and still accepts
// a host client certificate from the shale CA; a client that dials an IP
// gets the host certificate, and so does the CP on the control API (§33.5).
func TestDataPlaneServesExternalCertificate(t *testing.T) {
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	certFile, keyFile := writeServerPair(t, public, t.TempDir(), "storage.example.com")
	n, shale := adopted(t, Config{CertFile: certFile, KeyFile: keyFile})

	data, control, err := n.tlsConfigs()
	require.NoError(t, err)
	shalePool, publicPool := x509.NewCertPool(), x509.NewCertPool()
	shalePool.AddCert(shale.Cert)
	publicPool.AddCert(public.Cert)
	producer := hostClient(t, shale)

	// A browser: the public CA, no client certificate.
	got := handshake(t, data, publicPool, "storage.example.com", nil)
	require.NoError(t, got.err)
	require.Equal(t, []string{"storage.example.com"}, got.served.DNSNames)
	require.Empty(t, got.verified)

	// A producer by the name: the same certificate, and its own is verified
	// against the shale CA.
	got = handshake(t, data, publicPool, "storage.example.com", &producer)
	require.NoError(t, got.err)
	require.Equal(t, []string{"storage.example.com"}, got.served.DNSNames)
	require.NotEmpty(t, got.verified)
	_, isHost := pki.IdOf(got.verified[0][0])
	require.True(t, isHost)

	// A client certificate from another CA is refused, as before.
	stranger := hostClient(t, public)
	got = handshake(t, data, publicPool, "storage.example.com", &stranger)
	require.Error(t, got.err)

	// By IP: the host certificate.
	got = handshake(t, data, shalePool, "127.0.0.1", &producer)
	require.NoError(t, got.err)
	_, isHost = pki.IdOf(got.served)
	require.True(t, isHost, "an IP gets the host certificate")

	// The control API keeps the host certificate whatever name is asked.
	got = handshake(t, control, shalePool, "127.0.0.1", &producer)
	require.NoError(t, got.err)
	_, isHost = pki.IdOf(got.served)
	require.True(t, isHost)
	got = handshake(t, control, publicPool, "storage.example.com", &producer)
	require.Error(t, got.err, "the control API does not serve the external certificate")
}

// Without one, both listeners serve the host certificate, as before.
func TestDataPlaneServesHostCertificate(t *testing.T) {
	n, shale := adopted(t, Config{})
	data, control, err := n.tlsConfigs()
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(shale.Cert)
	producer := hostClient(t, shale)
	for _, cfg := range []*tls.Config{data, control} {
		got := handshake(t, cfg, pool, "127.0.0.1", &producer)
		require.NoError(t, got.err)
		_, isHost := pki.IdOf(got.served)
		require.True(t, isHost)
		require.NotEmpty(t, got.verified)
	}
}
