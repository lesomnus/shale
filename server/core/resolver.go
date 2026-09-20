package core

import (
	"net"
	"strconv"
	"strings"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
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
	if p != nil && p.GetResolver() == "template" && p.GetTemplate() != "" {
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

// Template names a node by a pattern such as `{alias}.nodes.example.com`,
// with DNS managed outside Shale.
type Template struct{}

func (Template) Endpoints(n NodeAddresses, _ string, p *api.AddressParams) []*api.Endpoint {
	_, port := splitAddr(n.DataAddress)
	host := strings.ReplaceAll(p.GetTemplate(), "{alias}", n.Alias)
	host = strings.ReplaceAll(host, "{id}", n.Id.String())

	return []*api.Endpoint{endpoint(host, port, n.Dev)}
}

func endpoint(host string, port int, dev bool) *api.Endpoint {
	scheme := "https"
	if dev {
		scheme = "http"
	}

	return api.Endpoint_builder{Scheme: scheme, Host: host, Port: int32(port), Protocol: "h1"}.Build()
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

	if from != nil && from.IsLoopback() {
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
		host := strings.ReplaceAll(p.GetTemplate(), "{alias}", n.Alias)
		host = strings.ReplaceAll(host, "{id}", n.Id.String())
		dns = append(dns, host)
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
