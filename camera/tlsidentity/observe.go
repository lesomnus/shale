package tlsidentity

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"time"
)

var ErrObservation = errors.New("camera TLS: public peer observation failed")

// Observe performs only a bounded TLS handshake with an IP-literal HTTPS
// origin. No HTTP request, credential, cookie or caller TLS configuration can
// enter this operation. The public key it returns is an observation, not trust.
// A CP-approved first-enrollment plan must authorize the endpoint separately.
func Observe(ctx context.Context, endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", ErrObservation
	}
	addr, err := netip.ParseAddr(u.Hostname())
	if err != nil || addr.Zone() != "" || addr.IsUnspecified() || addr.IsMulticast() {
		return "", ErrObservation
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var pin string
	// This configuration never escapes the credential-free handshake. The
	// completed handshake proves possession of the observed public key only.
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 || cs.PeerCertificates[0] == nil || len(cs.PeerCertificates[0].RawSubjectPublicKeyInfo) == 0 {
				return ErrObservation
			}
			hash := sha256.Sum256(cs.PeerCertificates[0].RawSubjectPublicKeyInfo)
			pin = "sha256:" + hex.EncodeToString(hash[:])
			return nil
		},
	}
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: cfg}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(addr.String(), port))
	if err != nil {
		if ctx.Err() != nil {
			return "", errors.Join(ErrObservation, ctx.Err())
		}
		return "", ErrObservation
	}
	_ = conn.Close()
	if pin == "" {
		return "", ErrObservation
	}
	return pin, nil
}
