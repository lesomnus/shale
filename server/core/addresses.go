package core

import (
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/lesomnus/shale/api"
)

// What a host says about where it listens is built into the URLs every
// client dials (§34.10), so it is checked for shape when it arrives
// (§33.7): an address is a host and a port, a host is an IP or a name, and
// a URL is put together by the URL package rather than by hand.

// hostName is a DNS name: labels of letters, digits and hyphens.
var hostName = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,62}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,62}[A-Za-z0-9])?)*\.?$`)

// validHost says whether v is an IP address or a DNS name.
func validHost(v string) bool {
	if net.ParseIP(v) != nil {
		return true
	}

	return len(v) <= 253 && hostName.MatchString(v)
}

// validateAddr checks a listener address a host reported, `host:port`
// with the host empty for every interface.
func validateAddr(field, v string) error {
	if v == "" {
		return nil
	}
	host, ps, err := net.SplitHostPort(v)
	if err != nil {
		return invalid(field, "host:port expected")
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port < 1 || port > 65535 {
		return invalid(field, "a port between 1 and 65535")
	}
	if host != "" && !validHost(host) {
		return invalid(field, "the host is neither an IP address nor a name")
	}

	return nil
}

// validateInterfaces checks the interfaces a host reported: a short name,
// and addresses that are IPs, with or without a prefix length.
func validateInterfaces(ifs []*api.HostInterface) error {
	if len(ifs) > 64 {
		return invalid("interfaces", "at most 64")
	}
	for _, i := range ifs {
		if n := i.GetName(); len(n) > 64 || strings.ContainsFunc(n, unprintable) {
			return invalid("interfaces.name", "a short printable name")
		}
		if len(i.GetAddresses()) > 64 {
			return invalid("interfaces.addresses", "at most 64 per interface")
		}
		for _, a := range i.GetAddresses() {
			ip, _, _ := strings.Cut(a, "/")
			if net.ParseIP(ip) == nil {
				return invalid("interfaces.addresses", "an IP address, with or without a prefix length")
			}
		}
	}

	return nil
}

// validateNodeAddresses is what a node's join and heartbeat carry.
func validateNodeAddresses(ifs []*api.HostInterface, control, data string) error {
	if err := validateInterfaces(ifs); err != nil {
		return err
	}
	if err := validateAddr("control_address", control); err != nil {
		return err
	}

	return validateAddr("data_address", data)
}

// validateRelayAddresses is what a relay's join and heartbeat carry.
func validateRelayAddresses(ifs []*api.HostInterface, ingest, whep string) error {
	if err := validateInterfaces(ifs); err != nil {
		return err
	}
	if err := validateAddr("ingest_address", ingest); err != nil {
		return err
	}

	return validateAddr("whep_address", whep)
}

// unprintable is a rune that has no place in a name a host reports.
func unprintable(r rune) bool { return r < 0x20 || r == 0x7f }

// endpointURL is a URL on an endpoint: the path's segments are kept as
// they are, the query is encoded, and an IPv6 host is bracketed.
func endpointURL(ep *api.Endpoint, path string, query url.Values) string {
	u := url.URL{
		Scheme: ep.GetScheme(),
		Host:   net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort()))),
		Path:   "/" + strings.TrimPrefix(path, "/"),
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}

	return u.String()
}
