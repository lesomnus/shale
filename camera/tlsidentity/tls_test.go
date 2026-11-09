package tlsidentity_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera/tlsidentity"
)

func TestNewTLSConfigRejectsMalformedPins(t *testing.T) {
	valid := "sha256:" + strings.Repeat("ab", 32)
	for _, pin := range []string{
		"sha256:", valid[:len(valid)-1], valid + "0", " " + valid,
		valid + "\n", strings.ToUpper(valid), "sha256:" + strings.Repeat("AB", 32),
		"sha256:" + strings.Repeat("g0", 32), "sha256/" + strings.Repeat("ab", 32),
		"sha512:" + strings.Repeat("ab", 32), "sha256:" + strings.Repeat("\xff", 64),
	} {
		cfg, err := tlsidentity.NewTLSConfig(pin)
		if cfg != nil || !errors.Is(err, tlsidentity.ErrInvalidPin) {
			t.Fatal("malformed pin did not fail closed")
		}
		if err.Error() != "camera TLS: invalid SPKI pin" {
			t.Fatal("diagnostic includes untrusted pin input")
		}
	}
}

func TestEmptyPinPreservesPKIVerification(t *testing.T) {
	for _, san := range []bool{false, true} {
		t.Run(map[bool]string{false: "SANless", true: "PKI_valid"}[san], func(t *testing.T) {
			cert := certificate(t, nil, san, 1)
			server, calls := serve(t, cert)
			cfg := config(t, "")
			if cfg.MinVersion != tls.VersionTLS12 || cfg.InsecureSkipVerify || cfg.VerifyConnection != nil {
				t.Fatal("empty pin weakened normal verification")
			}
			cfg.RootCAs = roots(cert.Leaf)
			err := request(t, server.URL, cfg)
			if san {
				if err != nil || calls.Load() != 1 {
					t.Fatalf("valid PKI failed: %v", err)
				}
			} else {
				var hostname x509.HostnameError
				if !errors.As(err, &hostname) || calls.Load() != 0 {
					t.Fatal("un-pinned SANless certificate reached HTTP")
				}
			}
		})
	}
}

func TestPinnedSANlessCameraAndRenewedCertificate(t *testing.T) {
	first := certificate(t, nil, false, 1)
	renewed := certificate(t, first.PrivateKey.(*ecdsa.PrivateKey), false, 2)
	for _, cert := range []tls.Certificate{first, renewed} {
		server, calls := serve(t, cert)
		cfg := config(t, pin(first.Leaf.RawSubjectPublicKeyInfo))
		if cfg.MinVersion != tls.VersionTLS12 || !cfg.InsecureSkipVerify || cfg.VerifyConnection == nil {
			t.Fatal("pin is not paired with mandatory verification")
		}
		if err := request(t, server.URL, cfg); err != nil || calls.Load() != 1 {
			t.Fatalf("approved SANless key failed: %v", err)
		}
	}
}

func TestEmptyPinRejectsUntrustedIssuer(t *testing.T) {
	cert := certificate(t, nil, true, 1)
	server, calls := serve(t, cert)
	err := request(t, server.URL, config(t, ""))
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) || calls.Load() != 0 {
		t.Fatal("unapproved issuer reached HTTP")
	}
}

func TestWrongKeyNeverSendsHTTPCredentialsOrFallsBackToCA(t *testing.T) {
	approved := certificate(t, nil, false, 1)
	for _, san := range []bool{false, true} {
		t.Run(map[bool]string{false: "other_factory_device", true: "CA_valid_other_device"}[san], func(t *testing.T) {
			other := certificate(t, nil, san, 2)
			server, calls := serve(t, other)
			cfg := config(t, pin(approved.Leaf.RawSubjectPublicKeyInfo))
			cfg.RootCAs = roots(other.Leaf)
			err := request(t, server.URL, cfg)
			if !errors.Is(err, tlsidentity.ErrPeerIdentity) || calls.Load() != 0 {
				t.Fatal("wrong key reached HTTP or fell back to normal PKI")
			}
		})
	}
}

func TestPinHashesLeafSPKIRatherThanCertificateDER(t *testing.T) {
	cert := certificate(t, nil, false, 1)
	server, calls := serve(t, cert)
	if err := request(t, server.URL, config(t, pin(cert.Leaf.Raw))); !errors.Is(err, tlsidentity.ErrPeerIdentity) || calls.Load() != 0 {
		t.Fatal("certificate DER digest was accepted as an SPKI digest")
	}
}

func TestPinnedCertificateStillRequiresPrivateKeyPossession(t *testing.T) {
	approved := certificate(t, nil, false, 1)
	other := certificate(t, nil, false, 2)
	forged := approved
	forged.PrivateKey = other.PrivateKey
	server, calls := serve(t, forged)
	if err := request(t, server.URL, config(t, pin(approved.Leaf.RawSubjectPublicKeyInfo))); err == nil || calls.Load() != 0 {
		t.Fatal("certificate without its private key reached HTTP")
	}
}

func TestPeerCertificateRequired(t *testing.T) {
	cfg := config(t, "sha256:"+strings.Repeat("00", 32))
	for _, state := range []tls.ConnectionState{{}, {PeerCertificates: []*x509.Certificate{nil}}, {PeerCertificates: []*x509.Certificate{{}}}} {
		if err := cfg.VerifyConnection(state); !errors.Is(err, tlsidentity.ErrPeerIdentity) {
			t.Fatal("missing peer key accepted")
		}
	}
}

func TestPinCannotNegotiateTLSBefore12(t *testing.T) {
	cert := certificate(t, nil, false, 1)
	server, calls := serve(t, cert, tls.VersionTLS11)
	if err := request(t, server.URL, config(t, pin(cert.Leaf.RawSubjectPublicKeyInfo))); err == nil || calls.Load() != 0 {
		t.Fatal("legacy TLS reached HTTP")
	}
}

func config(t *testing.T, pin string) *tls.Config {
	t.Helper()
	cfg, err := tlsidentity.NewTLSConfig(pin)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func pin(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func roots(cert *x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool
}

func serve(t *testing.T, cert tls.Certificate, maxVersion ...uint16) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if user, password, ok := r.BasicAuth(); !ok || user != "fixture-user" || password != "fixture-password" {
			t.Error("successful request did not carry fixture authentication")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	if len(maxVersion) != 0 {
		s.TLS.MinVersion = tls.VersionTLS10
		s.TLS.MaxVersion = maxVersion[0]
	}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s, calls
}

func request(t *testing.T, url string, cfg *tls.Config) error {
	t.Helper()
	transport := &http.Transport{TLSClientConfig: cfg}
	t.Cleanup(transport.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("fixture-user", "fixture-password")
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	return err
}

func certificate(t *testing.T, key *ecdsa.PrivateKey, san bool, serial int64) tls.Certificate {
	t.Helper()
	if key == nil {
		var err error
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "factory-camera"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if san {
		cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}
