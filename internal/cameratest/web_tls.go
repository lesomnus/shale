package cameratest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// WebKey is a camera's HTTPS web certificate and the SPKI pin naming its key.
// A camera that moves keeps its WebKey; an impostor presents another.
type WebKey struct {
	Certificate tls.Certificate
	SPKI        string
}

// NewWebKey is a self-signed certificate for a new P-256 key.
func NewWebKey(t testing.TB) WebKey {
	t.Helper()
	return issueWebKey(t, nil)
}

// Issue is a certificate for a new key signed by k, as if k were the CA of
// the camera's vendor: the same CA, but not the same camera.
func (k WebKey) Issue(t testing.TB) WebKey {
	t.Helper()
	return issueWebKey(t, &k)
}

func issueWebKey(t testing.TB, issuer *WebKey) WebKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate camera web key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("camera web serial: %v", err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "synthetic-camera"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: issuer == nil}
	parent, signer := template, any(key)
	if issuer != nil {
		parent, signer = issuer.Certificate.Leaf, issuer.Certificate.PrivateKey
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatalf("create camera web certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse camera web certificate: %v", err)
	}
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	return WebKey{Certificate: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, SPKI: "sha256:" + hex.EncodeToString(sum[:])}
}

// AuthAttempt is one request that presented, or tried to set, a credential:
// an ONVIF WS-Security token, or any vendor web call but the anonymous
// activation challenge. Host is the address the camera received it on.
type AuthAttempt struct {
	At   time.Time
	Host string
	Kind string
}

// AuthAttempts lists every authentication attempt the camera received, at
// any address it has served, in order.
func (c *ONVIFCamera) AuthAttempts() []AuthAttempt {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]AuthAttempt(nil), c.attempts...)
}

// WebHandshakes counts completed TLS handshakes on the HTTPS web port.
func (c *ONVIFCamera) WebHandshakes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handshakes
}

func (c *ONVIFCamera) attempt(r *http.Request, kind string) {
	// Caller holds c.mu.
	host, _, _ := net.SplitHostPort(localAddr(r))
	c.attempts = append(c.attempts, AuthAttempt{At: time.Now(), Host: host, Kind: kind})
}

func localAddr(r *http.Request) string {
	if v, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		return v.String()
	}
	return ""
}

// ServeWebTLS adds the vendor web protocol over HTTPS at the camera's current
// host and port, presenting key. Port 0 picks one; a later move keeps it, as
// a camera's configured web port survives a DHCP change.
func (c *ONVIFCamera) ServeWebTLS(t testing.TB, port int, key WebKey) string {
	t.Helper()
	c.mu.Lock()
	c.webKey = key
	host := c.server.Listener.Addr().(*net.TCPAddr).IP.String()
	c.mu.Unlock()
	if err := c.rebindWeb(net.JoinHostPort(host, strconv.Itoa(port))); err != nil {
		t.Fatalf("bind synthetic camera web: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		web := c.web
		c.mu.Unlock()
		if web != nil {
			web.close()
		}
	})
	return c.WebTLSURL()
}

// WebTLSURL is the HTTPS web origin, empty when the camera serves none.
func (c *ONVIFCamera) WebTLSURL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.web == nil {
		return ""
	}
	return c.web.url
}

// SetWebKey changes the key the web port presents from now on, as a factory
// reset or firmware update would.
func (c *ONVIFCamera) SetWebKey(key WebKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.webKey = key
}

// webServer is the HTTPS web port. The key comes from the camera on every
// handshake, so SetWebKey takes effect at once.
type webServer struct {
	server   *http.Server
	listener net.Listener
	url      string
	done     chan struct{}
}

func (w *webServer) close() {
	_ = w.server.Close()
	<-w.done
}

func (c *ONVIFCamera) rebindWeb(address string) error {
	lis, err := net.Listen("tcp4", address)
	if err != nil {
		return err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		key := c.webKey.Certificate
		return &key, nil
	}, VerifyConnection: func(tls.ConnectionState) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.handshakes++
		return nil
	}}
	w := &webServer{server: &http.Server{Handler: http.HandlerFunc(c.serve), TLSConfig: cfg, ReadHeaderTimeout: 5 * time.Second}, listener: lis, url: "https://" + lis.Addr().String(), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		_ = w.server.ServeTLS(lis, "", "")
	}()
	c.mu.Lock()
	old := c.web
	c.web = w
	c.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

// MoveTo models DHCP giving the camera another address: its ONVIF and HTTPS
// web listeners move to ip and keep their ports. Old connections end.
func (c *ONVIFCamera) MoveTo(ip string) error {
	c.mu.Lock()
	httpPort := c.server.Listener.Addr().(*net.TCPAddr).Port
	var webPort int
	if c.web != nil {
		webPort = c.web.listener.Addr().(*net.TCPAddr).Port
	}
	c.mu.Unlock()
	if err := c.rebind(net.JoinHostPort(ip, strconv.Itoa(httpPort))); err != nil {
		return err
	}
	if webPort == 0 {
		return nil
	}
	return c.rebindWeb(net.JoinHostPort(ip, strconv.Itoa(webPort)))
}
