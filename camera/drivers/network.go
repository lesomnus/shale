package drivers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/tlsidentity"
)

func networkPrefixes(id string, cidrs []string) ([]netip.Prefix, error) {
	if len(id) == 0 || len(id) > 128 || strings.ContainsFunc(id, unicode.IsControl) || len(cidrs) == 0 || len(cidrs) > 32 {
		return nil, camera.ErrInvalid
	}
	prefixes := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p.Bits() == 0 || p != p.Masked() || p.Addr().IsUnspecified() || p.Addr().IsMulticast() || p.Addr().Is4In6() {
			return nil, camera.ErrInvalid
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, nil
}

func (d *Driver) allowedHost(host string) bool {
	if d.deviceHosts != nil && !d.deviceHosts[host] {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	for _, p := range d.prefixes {
		if p.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}

// Public hints are safe metadata. Internal vendor session paths can carry an
// ephemeral login token; the HTTP route guard below does not publish them.
func publicEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" || camera.SanitizeURI(raw) != raw {
		return nil, camera.ErrForbidden
	}
	switch u.Scheme {
	case "http", "https", "rtsp", "rtsps":
	default:
		return nil, camera.ErrForbidden
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if strings.HasPrefix(strings.ToLower(segment), "stok=") {
			return nil, camera.ErrForbidden
		}
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, camera.ErrForbidden
	}
	for key, values := range q {
		switch strings.ToLower(key) {
		case "channel", "subtype", "stream", "streamid", "profile", "transport", "unicast", "proto":
		default:
			return nil, camera.ErrForbidden
		}
		for _, v := range values {
			if len(v) > 128 || strings.ContainsAny(v, "@:/\\\x00\r\n") {
				return nil, camera.ErrForbidden
			}
		}
	}
	return u, nil
}

type routedTransport struct {
	next     *http.Transport
	allow    func(string) bool
	allowURL func(*url.URL) bool
}

func (r routedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	u := request.URL
	if u == nil || u.User != nil || u.Opaque != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || !r.allow(u.Hostname()) {
		return nil, camera.ErrForbidden
	}
	if r.allowURL != nil && !r.allowURL(u) {
		return nil, camera.ErrForbidden
	}
	return r.next.RoundTrip(request)
}

func (d *Driver) httpClient(roots *x509.CertPool, timeout time.Duration) (*http.Client, *http.Transport) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if roots != nil {
		config.RootCAs = roots.Clone()
	}
	return d.scopedHTTPClient(config, timeout, nil)
}

// The explicit key applies only to the approved camera's HTTPS web port.
// Standard ONVIF operations keep normal PKI validation on their own transport.
func (d *Driver) pinnedWebClient(timeout time.Duration) (*http.Client, *http.Transport, error) {
	config, err := tlsidentity.NewTLSConfig(d.identity.SPKISHA256)
	if err != nil {
		return nil, nil, camera.ErrInvalid
	}
	port := strconv.Itoa(d.bootstrap.WebPort)
	client, transport := d.scopedHTTPClient(config, timeout, func(u *url.URL) bool {
		p := u.Port()
		if p == "" {
			p = "443"
		}
		return u.Scheme == "https" && p == port
	})
	return client, transport, nil
}

func (d *Driver) scopedHTTPClient(config *tls.Config, timeout time.Duration, allowURL func(*url.URL) bool) (*http.Client, *http.Transport) {
	transport := &http.Transport{TLSClientConfig: config, MaxIdleConns: 4, MaxIdleConnsPerHost: 2, IdleConnTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, DisableCompression: true}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil || !d.allowedHost(host) {
			return nil, camera.ErrForbidden
		}
		return (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 10 * time.Second}).DialContext(ctx, network, address)
	}
	return &http.Client{Transport: routedTransport{next: transport, allow: d.allowedHost, allowURL: allowURL}, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, transport
}
