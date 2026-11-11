package provisionhost

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

// Network pins camera control and qualification to an explicitly managed LAN.
// IP literals avoid checking a hostname and then sending credentials to a
// different address following DNS rebinding in a subprocess or redirect.
type Network struct {
	ID           string            `json:"id" yaml:"id"`
	CIDRs        []string          `json:"cidrs" yaml:"cidrs"`
	Interface    string            `json:"interface,omitempty" yaml:"interface,omitempty"`
	LocalAddress string            `json:"local_address,omitempty" yaml:"local_address,omitempty"`
	Seeds        []camera.Endpoint `json:"seeds,omitempty" yaml:"seeds,omitempty"`
	Multicast    bool              `json:"multicast" yaml:"multicast"`
	CAFile       string            `json:"ca_file,omitempty" yaml:"ca_file,omitempty"`
	Worker       ConnectionConfig  `json:"worker,omitempty" yaml:"worker,omitempty"`
}

func (n Network) Validate() error {
	if n.ID == "" || len(n.CIDRs) == 0 {
		return camera.ErrInvalid
	}
	for _, raw := range n.CIDRs {
		if _, err := netip.ParsePrefix(raw); err != nil {
			return camera.ErrInvalid
		}
	}
	for _, seed := range n.Seeds {
		if err := n.AllowURI(seed.URL); err != nil {
			return err
		}
	}
	return nil
}

func (n Network) allowsHost(host string) bool {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	for _, raw := range n.CIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err == nil && prefix.Contains(ip) {
			return true
		}
	}
	return false
}

func (n Network) AllowURI(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Hostname() == "" || u.Fragment != "" || raw != camera.SanitizeURI(raw) {
		return camera.ErrForbidden
	}
	if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return camera.ErrForbidden
	}
	if !n.allowsHost(u.Hostname()) {
		return camera.ErrForbidden
	}
	return nil
}

func (n Network) HTTPClient() (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if n.CAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		data, err := os.ReadFile(n.CAFile)
		if err != nil || !roots.AppendCertsFromPEM(data) {
			return nil, camera.ErrInvalid
		}
		tlsConfig.RootCAs = roots
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConns: 8, IdleConnTimeout: 15 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DisableCompression: true}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil || !n.allowsHost(host) {
			return nil, camera.ErrForbidden
		}
		return (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 15 * time.Second}).DialContext(ctx, network, address)
	}
	return &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return camera.ErrForbidden
		}
		if len(via) > 0 && via[0].URL.Scheme == "https" && request.URL.Scheme != "https" {
			return camera.ErrForbidden
		}
		return n.AllowURI(request.URL.String())
	}}, nil
}

// scopedVerifier validates the actual stream endpoint before the verifier can
// resolve its streaming credential. The profile may originate in a device reply.
type scopedVerifier struct {
	Network  Network
	Verifier camera.MediaVerifier
}

func (v scopedVerifier) Verify(ctx context.Context, r camera.MediaRequest) (camera.VerifiedState, error) {
	if r.NetworkScope != v.Network.ID || r.Scope.NetworkID != v.Network.ID {
		return camera.VerifiedState{}, camera.ErrForbidden
	}
	if err := v.Network.AllowURI(r.Profile.StreamURI); err != nil {
		return camera.VerifiedState{}, err
	}
	return v.Verifier.Verify(ctx, r)
}

func loopbackURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(u.Hostname())
	return err == nil && ip.IsLoopback()
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(strings.Trim(host, "[]"))
	return err == nil && ip.IsLoopback()
}
