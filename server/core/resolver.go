package core

import (
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
)

// NodeAddresses is what a resolver knows about a node: facts, not policy
// (§34.10).
type NodeAddresses struct {
	Id          pdid.Id
	Alias       string
	Interfaces  []*api.HostInterface
	DataAddress string
	// Dev means plaintext.
	Dev bool
}

// Resolver turns a node and a caller into the endpoints the caller dials.
type Resolver interface {
	Endpoints(n NodeAddresses, caller string, p *api.AddressParams) []*api.Endpoint
}

// Advertised hands out one of the IPs the node reports, chosen by rules on
// the caller's network (§34.10). It is the default, and it needs no DNS.
type Advertised struct{}

func (Advertised) Endpoints(n NodeAddresses, caller string, p *api.AddressParams) []*api.Endpoint {
	if byTemplate(p) {
		return Template{}.Endpoints(n, caller, p)
	}

	host, port := splitAddr(n.DataAddress)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		host = pickAddress(n.Interfaces, caller, p)
	}
	if host == "" {
		return nil
	}

	return []*api.Endpoint{endpoint(host, port, n.Dev)}
}

// Template names a node by a pattern such as `{alias}.nodes.example.com`:
// the `template` resolver, with DNS managed outside Shale, and the `dns`
// one, whose records the leader keeps (§34.10, dns.go).
type Template struct{}

func (Template) Endpoints(n NodeAddresses, _ string, p *api.AddressParams) []*api.Endpoint {
	_, port := splitAddr(n.DataAddress)

	return []*api.Endpoint{endpoint(templateName(p.GetTemplate(), n), port, n.Dev)}
}

// byTemplate says whether the policy hands out names from its template:
// the `template` and `dns` resolvers.
func byTemplate(p *api.AddressParams) bool {
	if p == nil || p.GetTemplate() == "" {
		return false
	}
	switch p.GetResolver() {
	case "template", "dns":
		return true
	}

	return false
}

// templateName is the name a template gives a host.
func templateName(tmpl string, n NodeAddresses) string {
	host := strings.ReplaceAll(tmpl, "{alias}", n.Alias)

	return strings.ReplaceAll(host, "{id}", n.Id.String())
}

// named is the endpoint of an address a host reports by name and port,
// handed out as it is rather than resolved: an operator wrote it, for a
// certificate that names it (§34.10). Nil for an IP, or no host or port.
func named(addr string, dev bool) *api.Endpoint {
	host, port := splitAddr(addr)
	if host == "" || port == 0 || net.ParseIP(host) != nil {
		return nil
	}

	return endpoint(host, port, dev)
}

// endpoint is a data plane over TCP: HTTP/2 over TLS, which a client that
// does not speak it takes as HTTP/1.1, and HTTP/1.1 in the plaintext of
// development mode. A node that serves HTTP/3 does so on the same host and
// port, over UDP, and says so in its answers' Alt-Svc (§35.6), so no
// resolver here names it apart.
func endpoint(host string, port int, dev bool) *api.Endpoint {
	scheme, proto := "https", "h2"
	if dev {
		scheme, proto = "http", "h1"
	}

	return api.Endpoint_builder{Scheme: scheme, Host: host, Port: int32(port), Protocol: proto}.Build()
}

// forProtocol is the endpoints as a caller asked for them by protocol
// (§34.10): every protocol a node speaks is on the one host and port, so
// only what they say changes. Plaintext is HTTP/1.1 whatever is asked, and
// a protocol that is none of the three is not asked.
func forProtocol(eps []*api.Endpoint, proto string) []*api.Endpoint {
	switch proto {
	case "h1", "h2", "h3":
	default:
		return eps
	}
	for _, ep := range eps {
		if ep.GetScheme() == "https" {
			ep.SetProtocol(proto)
		}
	}

	return eps
}

func splitAddr(addr string) (string, int) {
	host, ps, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	port, _ := strconv.Atoi(ps)

	return host, port
}

// pickAddress chooses among the node's addresses: the caller's rule first,
// a loopback caller gets loopback, then the first global IPv4, then IPv6.
func pickAddress(ifs []*api.HostInterface, caller string, p *api.AddressParams) string {
	var all []net.IP
	for _, i := range ifs {
		for _, a := range i.GetAddresses() {
			if ip := net.ParseIP(strings.Split(a, "/")[0]); ip != nil {
				all = append(all, ip)
			}
		}
	}

	from := net.ParseIP(caller)
	if from != nil && p != nil {
		for _, r := range p.GetRules() {
			_, cn, err := net.ParseCIDR(r.GetCallerCidr())
			if err != nil || !cn.Contains(from) {
				continue
			}
			_, pn, err := net.ParseCIDR(r.GetPreferCidr())
			if err != nil {
				continue
			}
			for _, ip := range all {
				if pn.Contains(ip) {
					return ip.String()
				}
			}
		}
	}

	// A loopback caller is on this host: loopback reaches a node on this
	// host and no other, which gets a routable address like any caller.
	if from != nil && from.IsLoopback() && onThisHost(all) {
		for _, ip := range all {
			if ip.IsLoopback() {
				return ip.String()
			}
		}
		if len(all) == 0 {
			return "127.0.0.1"
		}
	}

	// The same subnet as the caller, when any interface shares one; a /24 is
	// a guess, and the rules above are the way to say otherwise.
	if from != nil {
		for _, ip := range all {
			if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			if ip4, f4 := ip.To4(), from.To4(); ip4 != nil && f4 != nil && ip4[0] == f4[0] && ip4[1] == f4[1] && ip4[2] == f4[2] {
				return ip.String()
			}
		}
	}

	for _, ip := range all {
		if ip.To4() != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			return ip.String()
		}
	}
	for _, ip := range all {
		if !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			return ip.String()
		}
	}
	for _, ip := range all {
		return ip.String()
	}

	return ""
}

// NamesFor is what a node's certificate must cover: every name and IP the
// active resolver can hand out (§34.10).
func NamesFor(n NodeAddresses, p *api.AddressParams) (dns []string, ips []net.IP) {
	if p != nil && p.GetTemplate() != "" {
		dns = append(dns, templateName(p.GetTemplate(), n))
	}
	for _, i := range n.Interfaces {
		for _, a := range i.GetAddresses() {
			if ip := net.ParseIP(strings.Split(a, "/")[0]); ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	if host, _ := splitAddr(n.DataAddress); host != "" {
		if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
			ips = append(ips, ip)
		} else if ip == nil {
			dns = append(dns, host)
		}
	}
	if p != nil {
		dns = append(dns, p.GetExtraNames()...)
	}
	ips = append(ips, net.ParseIP("127.0.0.1"))
	dns = append(dns, "localhost")

	return dns, ips
}

// policyNames are the names the active policy hands out for a host by
// itself, which its certificate must carry (§33.5): the template's and the
// extra names. They change with the policy, unlike what the host reports.
func policyNames(n NodeAddresses, p *api.AddressParams) []string {
	var out []string
	if p.GetTemplate() != "" {
		out = append(out, templateName(p.GetTemplate(), n))
	}

	return append(out, p.GetExtraNames()...)
}

// certCovers says whether a host's certificate carries every one of names.
// One that does not parse is not asked about: renewing would not help it.
func certCovers(chain []byte, names []string) bool {
	if len(names) == 0 {
		return true
	}
	certs, err := pki.ParseCerts(chain)
	if err != nil || len(certs) == 0 {
		return true
	}
	for _, name := range names {
		if !slices.ContainsFunc(certs[0].DNSNames, func(v string) bool { return strings.EqualFold(v, name) }) {
			return false
		}
	}

	return true
}

// hostIPs answers the addresses of this host's interfaces, read at most
// once a minute; tests replace it.
var hostIPs = func() []net.IP {
	hostIPsMu.Lock()
	defer hostIPsMu.Unlock()
	if time.Since(hostIPsAt) < time.Minute && hostIPsCache != nil {
		return hostIPsCache
	}
	var ips []net.IP
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				ips = append(ips, n.IP)
			}
		}
	}
	hostIPsCache, hostIPsAt = ips, time.Now()

	return ips
}

var (
	hostIPsMu    sync.Mutex
	hostIPsCache []net.IP
	hostIPsAt    time.Time
)

// onThisHost says whether a node with these addresses runs on the host
// the CP runs on: one of its routable addresses is one of ours.
func onThisHost(all []net.IP) bool {
	mine := hostIPs()
	for _, ip := range all {
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		for _, h := range mine {
			if h.Equal(ip) {
				return true
			}
		}
	}

	return false
}
