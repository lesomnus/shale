// Package pki is the Control Plane's built-in certificate authority and the
// host-side key handling (§33.5).
//
// A host certificate names its row: a URI SAN `shale:<id>` carrying the
// entity's identifier, whose domain byte says whether it is a node, a
// producer, a reader, or a relay, and which payday's mTLS handler reads as
// the caller. A node or relay certificate also carries every name and IP a
// client may dial it by, because clients verify what they were given.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
)

const (
	// Lifetimes (§33.5).
	HostLifetime = 90 * 24 * time.Hour
	CpLifetime   = 365 * 24 * time.Hour
	CaLifetime   = 10 * 365 * 24 * time.Hour

	// A host renews at two thirds of its certificate's lifetime.
	RenewFraction = 2.0 / 3.0

	// UriScheme is what a host certificate's URI SAN is written under; payday
	// reads the identifier after the last colon whatever the scheme.
	UriScheme = "shale"
)

// CA is one certificate authority with its key.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
}

// NewCA makes a self-signed CA for the cluster.
func NewCA(name string, now time.Time) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name, Organization: []string{"Shale"}},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(CaLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}

	return &CA{Cert: cert, Key: key}, nil
}

// Load reads a CA from PEM files.
func Load(certPath, keyPath string) (*CA, error) {
	cb, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	kb, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	certs, err := ParseCerts(cb)
	if err != nil {
		return nil, err
	}
	if len(certs) == 0 {
		return nil, errors.New("pki: no certificate in " + certPath)
	}

	key, err := ParseKey(kb)
	if err != nil {
		return nil, err
	}

	return &CA{Cert: certs[0], Key: key}, nil
}

// Save writes the CA to PEM files, the key with mode 0600.
func (ca *CA) Save(certPath, keyPath string) error {
	if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(certPath, EncodeCerts(ca.Cert), 0o644); err != nil {
		return err
	}

	kb, err := EncodeKey(ca.Key)
	if err != nil {
		return err
	}

	return os.WriteFile(keyPath, kb, 0o600)
}

// Bundle is the CA certificate as PEM, which is what hosts trust.
func (ca *CA) Bundle() []byte { return EncodeCerts(ca.Cert) }

// Hash is what heartbeats report about the bundle they hold (§33.5).
func BundleHash(pem []byte) string {
	sum := sha256.Sum256(pem)

	return "sha256:" + hex.EncodeToString(sum[:])
}

// Fingerprint is the SHA-256 of a certificate's DER, the way `--ca-hash`
// takes it.
func Fingerprint(c *x509.Certificate) string {
	sum := sha256.Sum256(c.Raw)

	return "sha256:" + hex.EncodeToString(sum[:])
}

// Names are the subject alternative names a certificate carries besides its
// identity.
type Names struct {
	DNS []string
	IPs []net.IP
}

// IssueHost signs a host certificate for the CSR's public key, naming the
// row `id`. `names` may be empty for a producer or a reader, which nobody
// dials.
func (ca *CA) IssueHost(csrDER []byte, id pdid.Id, names Names, now time.Time) (*x509.Certificate, error) {
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, fmt.Errorf("pki: csr: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("pki: csr: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: id.String(), Organization: []string{"Shale"}},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(HostLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{{Scheme: UriScheme, Opaque: id.String()}},
		DNSNames:     names.DNS,
		IPAddresses:  names.IPs,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, csr.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}

	return x509.ParseCertificate(der)
}

// IssueServer signs a plain server certificate, which is what the Control
// Plane serves its APIs with and presents to nodes as their control-API
// peer. It names the CP by a URI so a node can pin it.
func (ca *CA) IssueServer(pub any, cn string, names Names, lifetime time.Duration, now time.Time) (*x509.Certificate, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"Shale"}},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(lifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{{Scheme: UriScheme, Opaque: cn}},
		DNSNames:     names.DNS,
		IPAddresses:  names.IPs,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, pub, ca.Key)
	if err != nil {
		return nil, err
	}

	return x509.ParseCertificate(der)
}

// CpName is the URI a Control Plane certificate carries, which a node's
// control API accepts as its one peer (§35.7).
const CpName = "control-plane"

// IsCp reports whether a verified certificate names the Control Plane.
func IsCp(c *x509.Certificate) bool {
	for _, u := range c.URIs {
		if u.Scheme == UriScheme && u.Opaque == CpName {
			return true
		}
	}

	return c.Subject.CommonName == CpName
}

// IdOf reads the entity identifier a host certificate names.
func IdOf(c *x509.Certificate) (pdid.Id, bool) {
	for _, u := range c.URIs {
		v := u.Opaque
		if i := strings.LastIndexByte(v, ':'); i >= 0 {
			v = v[i+1:]
		}
		if id, err := pdid.Parse(v); err == nil {
			return id, true
		}
	}

	return pdid.Nil, false
}

// VerifyPeer verifies a presented chain against `pool` (the system roots
// when nil) and checks that the leaf names host `id`, whatever address was
// dialed: hosts are named by ID, never by address (§33.2, §34.10).
func VerifyPeer(pool *x509.CertPool, id pdid.Id) func(raw [][]byte, _ [][]*x509.Certificate) error {
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
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
		opts := x509.VerifyOptions{Roots: pool, Intermediates: x509.NewCertPool(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		for _, c := range chain[1:] {
			opts.Intermediates.AddCert(c)
		}
		if _, err := chain[0].Verify(opts); err != nil {
			return err
		}
		got, ok := IdOf(chain[0])
		if !ok || got != id {
			return fmt.Errorf("the peer's certificate names %s, not %s", got, id)
		}

		return nil
	}
}

// Serial writes a serial number the way rows store and compare it.
func Serial(c *x509.Certificate) string {
	return c.SerialNumber.Text(16)
}

// HostKey is a host's key pair with the CSR it joins with.
type HostKey struct {
	Key *ecdsa.PrivateKey
}

// NewHostKey generates a key pair.
func NewHostKey() (*HostKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	return &HostKey{Key: key}, nil
}

// CSR is a signing request for this key; the CA ignores everything in it
// but the public key.
func (k *HostKey) CSR(hostname string) ([]byte, error) {
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: hostname},
	}, k.Key)
}

// Fingerprint is what the host prints in its log and the console shows
// beside a pending host, so an operator can match the two (§33.4).
func (k *HostKey) Fingerprint() string {
	der, err := x509.MarshalPKIXPublicKey(&k.Key.PublicKey)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(der)

	return "sha256:" + hex.EncodeToString(sum[:8])
}

// Store keeps a host's key, certificate, and CA bundle in its state
// directory (§34.3).
type Store struct {
	Dir string
}

func (s Store) keyPath() string    { return filepath.Join(s.Dir, "host.key") }
func (s Store) certPath() string   { return filepath.Join(s.Dir, "host.crt") }
func (s Store) bundlePath() string { return filepath.Join(s.Dir, "ca.crt") }

// Key loads the host key, generating one the first time.
func (s Store) Key() (*HostKey, error) {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return nil, err
	}

	b, err := os.ReadFile(s.keyPath())
	if err == nil {
		key, err := ParseKey(b)
		if err != nil {
			return nil, err
		}

		return &HostKey{Key: key}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	k, err := NewHostKey()
	if err != nil {
		return nil, err
	}
	kb, err := EncodeKey(k.Key)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(s.keyPath(), kb, 0o600); err != nil {
		return nil, err
	}

	return k, nil
}

// Rotate replaces the key, for a renewal that sends a new one.
func (s Store) Rotate() (*HostKey, error) {
	k, err := NewHostKey()
	if err != nil {
		return nil, err
	}
	kb, err := EncodeKey(k.Key)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(s.keyPath(), kb, 0o600); err != nil {
		return nil, err
	}

	return k, nil
}

// Cert loads the host certificate chain, or nil when the host has none yet.
func (s Store) Cert() ([]*x509.Certificate, error) {
	b, err := os.ReadFile(s.certPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return ParseCerts(b)
}

// CertPEM is the chain as stored.
func (s Store) CertPEM() ([]byte, error) {
	b, err := os.ReadFile(s.certPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	return b, err
}

// SetCert stores a chain and the bundle it was issued under.
func (s Store) SetCert(chain, bundle []byte) error {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(s.certPath(), chain, 0o644); err != nil {
		return err
	}
	if len(bundle) > 0 {
		return os.WriteFile(s.bundlePath(), bundle, 0o644)
	}

	return nil
}

// Bundle is the CA bundle the host trusts, or nil before the first join.
func (s Store) Bundle() ([]byte, error) {
	b, err := os.ReadFile(s.bundlePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	return b, err
}

// SetBundle pins a bundle.
func (s Store) SetBundle(bundle []byte) error {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return err
	}

	return os.WriteFile(s.bundlePath(), bundle, 0o644)
}

// BundlePath is where the media server beside a reader finds the CAs (§33.4).
func (s Store) BundlePath() string { return s.bundlePath() }

// Pool is the bundle as a cert pool.
func Pool(bundle []byte) (*x509.CertPool, error) {
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(bundle) {
		return nil, errors.New("pki: no certificate in the bundle")
	}

	return p, nil
}

// EncodeCerts writes certificates as PEM.
func EncodeCerts(cs ...*x509.Certificate) []byte {
	var b []byte
	for _, c := range cs {
		b = append(b, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})...)
	}

	return b
}

// ParseCerts reads every certificate in a PEM bundle.
func ParseCerts(b []byte) ([]*x509.Certificate, error) {
	var cs []*x509.Certificate
	for {
		var blk *pem.Block
		blk, b = pem.Decode(b)
		if blk == nil {
			break
		}
		if blk.Type != "CERTIFICATE" {
			continue
		}

		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, err
		}

		cs = append(cs, c)
	}

	return cs, nil
}

// EncodeKey writes a private key as PKCS#8 PEM.
func EncodeKey(k *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// ParseKey reads a PKCS#8 or EC private key.
func ParseKey(b []byte) (*ecdsa.PrivateKey, error) {
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, errors.New("pki: no key in PEM")
	}

	if k, err := x509.ParsePKCS8PrivateKey(blk.Bytes); err == nil {
		ek, ok := k.(*ecdsa.PrivateKey)
		if !ok {
			return nil, errors.New("pki: not an EC key")
		}

		return ek, nil
	}

	return x509.ParseECPrivateKey(blk.Bytes)
}

// RenewAt is when a host holding this certificate should renew it.
func RenewAt(c *x509.Certificate) time.Time {
	life := c.NotAfter.Sub(c.NotBefore)

	return c.NotBefore.Add(time.Duration(float64(life) * RenewFraction))
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)

	return rand.Int(rand.Reader, limit)
}
