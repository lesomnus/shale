package relay

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/pki"
)

// The WHEP certificate is a pair: one file without the other is a mistake
// in the configuration, said at start rather than at the first browser.
func TestWhepCertConfig(t *testing.T) {
	dir := t.TempDir()
	_, err := New(Config{StateDir: dir, WhepCertFile: "/etc/shale/whep/tls.crt"})
	require.ErrorContains(t, err, "whep_key_file")
	_, err = New(Config{StateDir: dir, WhepKeyFile: "/etc/shale/whep/tls.key"})
	require.ErrorContains(t, err, "whep_cert_file")
	_, err = New(Config{StateDir: dir, WhepCertFile: filepath.Join(dir, "missing.crt"), WhepKeyFile: filepath.Join(dir, "missing.key")})
	require.Error(t, err)
	_, err = New(Config{StateDir: dir})
	require.NoError(t, err)
}

// writeServerPair issues a server certificate for name from ca and writes
// it and its key into dir.
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

// adopted is a relay holding a host certificate from the shale CA, as a
// join leaves it.
func adopted(t *testing.T, cfg Config) (*Relay, *pki.CA) {
	t.Helper()
	cfg.StateDir = t.TempDir()
	r, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, r.agent.Init())
	ca, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	hj, err := r.agent.HostJoin()
	require.NoError(t, err)
	cert, err := ca.IssueHost(hj.GetCsr(), pdid.New(DomRelay), pki.Names{IPs: []net.IP{net.ParseIP("127.0.0.1")}}, time.Now())
	require.NoError(t, err)
	require.NoError(t, r.agent.Store.SetCert(pki.EncodeCerts(cert), ca.Bundle()))

	return r, ca
}

// handshake is the certificate a listener with cfg presents to a client
// that trusts roots and dials name.
func handshake(t *testing.T, cfg *tls.Config, roots *x509.CertPool, name string) *x509.Certificate {
	t.Helper()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go tls.Server(a, cfg).Handshake()
	c := tls.Client(b, &tls.Config{RootCAs: roots, ServerName: name})
	require.NoError(t, c.Handshake())

	return c.ConnectionState().PeerCertificates[0]
}

// With an external certificate, the WHEP listener serves it to browsers
// while ingest keeps the host certificate producers verify against the
// shale CA (§33.5, §39.7).
func TestWhepServesExternalCertificate(t *testing.T) {
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	certFile, keyFile := writeServerPair(t, public, t.TempDir(), "live.example.com")
	r, shale := adopted(t, Config{WhepCertFile: certFile, WhepKeyFile: keyFile})

	ingest, whep, err := r.tlsConfigs()
	require.NoError(t, err)
	shalePool, publicPool := x509.NewCertPool(), x509.NewCertPool()
	shalePool.AddCert(shale.Cert)
	publicPool.AddCert(public.Cert)

	got := handshake(t, ingest, shalePool, "127.0.0.1")
	_, isHost := pki.IdOf(got)
	require.True(t, isHost, "ingest serves the host certificate")
	got = handshake(t, whep, publicPool, "live.example.com")
	require.Equal(t, []string{"live.example.com"}, got.DNSNames)
}

// Without one, both listeners serve the host certificate, as before.
func TestWhepServesHostCertificate(t *testing.T) {
	r, shale := adopted(t, Config{})
	ingest, whep, err := r.tlsConfigs()
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(shale.Cert)
	for _, cfg := range []*tls.Config{ingest, whep} {
		_, isHost := pki.IdOf(handshake(t, cfg, pool, "127.0.0.1"))
		require.True(t, isHost)
	}
}

// What the CP is told for WHEP: `whep_advertise` with its port when it
// has one, else the bound port; `advertise` when it is not set. Ingest
// takes only `advertise`'s host, whatever WHEP does.
func TestAdvertisedWhep(t *testing.T) {
	for _, c := range []struct {
		advertise, whep, want string
	}{
		{"", "", "[::]:7441"},
		{"10.1.2.80", "", "10.1.2.80:7441"},
		{"10.1.2.80:9", "", "10.1.2.80:7441"},
		{"10.1.2.80", "live.example.com", "live.example.com:7441"},
		{"10.1.2.80", "live.example.com:443", "live.example.com:443"},
		{"", "live.example.com:", "live.example.com:7441"},
		{"", "2001:db8::1", "[2001:db8::1]:7441"},
		{"", "[2001:db8::1]", "[2001:db8::1]:7441"},
		{"", "[2001:db8::1]:443", "[2001:db8::1]:443"},
	} {
		r := &Relay{cfg: Config{Advertise: c.advertise, WhepAdvertise: c.whep}}
		require.Equal(t, c.want, r.advertisedWhep("[::]:7441"), "%+v", c)
	}
	r := &Relay{cfg: Config{Advertise: "10.1.2.80", WhepAdvertise: "live.example.com:443"}}
	require.Equal(t, "10.1.2.80:7440", r.advertised("[::]:7440"))
}

// A page on the console's origin reads the session's Location from the
// answer, which a cross-origin fetch sees only when it is exposed.
func TestWhepCors(t *testing.T) {
	r, err := New(Config{StateDir: t.TempDir()})
	require.NoError(t, err)
	w, err := newWhepServer(r)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/whep/x", nil)
	req.Header.Set("Origin", "https://shale.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	w.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "Authorization")

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/whep/x", strings.NewReader("v=0"))
	req.Header.Set("Origin", "https://shale.example.com")
	w.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, rec.Header().Get("Access-Control-Expose-Headers"), "Location")
}
