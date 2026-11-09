// Package tlsidentity verifies an explicitly approved camera TLS public key.
// It does not discover, approve, or determine the network scope of a device.
package tlsidentity

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"strings"
)

var (
	ErrInvalidPin   = errors.New("camera TLS: invalid SPKI pin")
	ErrPeerIdentity = errors.New("camera TLS: peer SPKI mismatch")
)

// NewTLSConfig returns a fresh TLS configuration requiring TLS 1.2 or later.
// An empty pin preserves Go's normal certificate chain, hostname, and validity
// verification. The caller may supply its usual RootCAs in that mode.
//
// A nonempty pin must be "sha256:" followed by 64 lowercase hexadecimal digits
// identifying SHA-256 of the leaf certificate's DER SubjectPublicKeyInfo. It is
// an explicit replacement for normal X.509 verification, including certificate
// validity dates, for this camera only. The TLS handshake still requires proof
// of the corresponding private key before HTTP application data can be sent.
// A mismatch never falls back to CA verification.
//
// The caller must bind this configuration to the approved device's current
// authorized endpoints/network, disallow redirects and HTTPS downgrades, and
// preserve VerifyConnection. A pin identifies a key, not a physical device or
// an authorization grant. This helper intentionally provides no user callback
// that can override its verification decision.
func NewTLSConfig(spkiPin string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if spkiPin == "" {
		return cfg, nil
	}
	const prefix = "sha256:"
	if len(spkiPin) != len(prefix)+sha256.Size*2 || !strings.HasPrefix(spkiPin, prefix) {
		return nil, ErrInvalidPin
	}
	encoded := spkiPin[len(prefix):]
	for _, b := range []byte(encoded) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return nil, ErrInvalidPin
		}
	}
	var expected [sha256.Size]byte
	if _, err := hex.Decode(expected[:], []byte(encoded)); err != nil {
		return nil, ErrInvalidPin
	}
	// Pinning deliberately replaces PKI validation. Never detach this flag
	// from the mandatory exact-key verification callback below.
	cfg.InsecureSkipVerify = true
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 || cs.PeerCertificates[0] == nil || len(cs.PeerCertificates[0].RawSubjectPublicKeyInfo) == 0 {
			return ErrPeerIdentity
		}
		actual := sha256.Sum256(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
		if subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			return ErrPeerIdentity
		}
		return nil
	}
	return cfg, nil
}
