// Package discovery finds camera candidates without authenticating to or
// changing a device. Discovery identities are untrusted hints until claimed.
package discovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"golang.org/x/net/ipv4"
)

const multicastAddress = "239.255.255.250:3702"

// Discoverer implements scoped WS-Discovery and explicit endpoint seeds. The
// injectable packet listener/target also allow unicast fixtures without LAN I/O.
type Discoverer struct {
	ListenPacket func(context.Context, string, string) (net.PacketConn, error)
	Target       net.Addr
	Now          func() time.Time
}

func (d Discoverer) Discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope.ID == "" {
		return nil, camera.ErrInvalid
	}
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	observations := map[string]camera.Observation{}
	if len(scope.Seeds) > 1024 {
		return nil, camera.ErrInvalid
	}
	for _, seed := range scope.Seeds {
		endpoint, err := normalizeEndpoint(seed.URL, scope.ID)
		if err != nil {
			return nil, err
		}
		key := candidateKey(scope.ID, "", endpoint.URL)
		observations[key] = camera.Observation{Scope: scope.ID, ObservedAt: now(), Device: camera.DeviceHint{CandidateID: key, Endpoints: []camera.Endpoint{endpoint}, Evidence: []camera.Evidence{{Source: "address_seed", At: now()}}}}
	}
	if !scope.Multicast {
		return sorted(observations), nil
	}
	wait := scope.Timeout
	if wait <= 0 {
		wait = 2 * time.Second
	}
	if wait > 30*time.Second {
		return nil, camera.ErrInvalid
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	local, err := localAddress(scope)
	if err != nil {
		return nil, err
	}
	listen := d.ListenPacket
	if listen == nil {
		listen = (&net.ListenConfig{}).ListenPacket
	}
	conn, err := listen(ctx, "udp4", net.JoinHostPort(local, "0"))
	if err != nil {
		return nil, fmt.Errorf("camera discovery socket: %w", err)
	}
	defer conn.Close()
	if scope.Interface != "" {
		iface, _ := net.InterfaceByName(scope.Interface) // validated by localAddress
		if _, ok := conn.(*net.UDPConn); ok {
			if err := ipv4.NewPacketConn(conn).SetMulticastInterface(iface); err != nil {
				return nil, fmt.Errorf("camera discovery interface: %w", err)
			}
		}
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	dst := d.Target
	if dst == nil {
		dst, _ = net.ResolveUDPAddr("udp4", multicastAddress)
	}
	probe, err := newProbe()
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	if _, err = conn.WriteTo([]byte(probe), dst); err != nil {
		if ctx.Err() != nil {
			return sorted(observations), parent.Err()
		}
		return nil, fmt.Errorf("camera discovery probe: %w", err)
	}
	buf := make([]byte, 64<<10)
	for {
		n, from, readErr := conn.ReadFrom(buf)
		if readErr != nil {
			if parent.Err() != nil {
				return sorted(observations), parent.Err()
			}
			if ctx.Err() != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return sorted(observations), nil
				}
				return sorted(observations), ctx.Err()
			}
			if e, ok := readErr.(net.Error); ok && e.Timeout() {
				return sorted(observations), nil
			}
			return nil, fmt.Errorf("camera discovery receive: %w", readErr)
		}
		matches, err := ParseMatches(buf[:n])
		if err != nil {
			continue
		} // Unrelated/malformed datagrams are not findings.
		for _, match := range matches {
			var endpoints []camera.Endpoint
			for _, raw := range match.XAddrs {
				endpoint, err := normalizeEndpoint(raw, scope.ID)
				if err == nil {
					endpoints = append(endpoints, endpoint)
				}
			}
			if len(endpoints) == 0 {
				continue
			}
			endpoints = uniqueEndpoints(endpoints)
			// The transport sender disambiguates broken devices advertising the
			// same discovery UUID. It is not a permanent physical identity.
			key := candidateKey(scope.ID, match.Address, hostOnly(from.String()))
			if match.Address == "" {
				key = candidateKey(scope.ID, "", endpoints[0].URL)
			}
			if _, exists := observations[key]; !exists && len(observations) >= 1024 {
				continue
			}
			at := now()
			hint := camera.DeviceHint{CandidateID: key, DiscoveryID: match.Address, Endpoints: endpoints, Evidence: []camera.Evidence{{Source: "ws_discovery", At: at}}}
			if prior, ok := observations[key]; ok {
				hint.Endpoints = uniqueEndpoints(append(prior.Device.Endpoints, endpoints...))
			}
			observations[key] = camera.Observation{Scope: scope.ID, ObservedAt: at, Device: hint}
		}
	}
}

// Match contains only WS-Discovery fields. Neither a UUID nor the XAddr is an
// authenticated device identity.
type Match struct {
	Address string
	XAddrs  []string
	Scopes  []string
}

func ParseMatches(data []byte) ([]Match, error) {
	if len(data) > 64<<10 {
		return nil, camera.ErrInvalid
	}
	var envelope struct {
		Body struct {
			Matches struct {
				Match []struct {
					Endpoint struct {
						Address string `xml:"Address"`
					} `xml:"EndpointReference"`
					XAddrs string `xml:"XAddrs"`
					Scopes string `xml:"Scopes"`
				} `xml:"ProbeMatch"`
			} `xml:"ProbeMatches"`
		} `xml:"Body"`
	}
	if err := xml.Unmarshal(data, &envelope); err != nil {
		return nil, camera.ErrInvalid
	}
	var matches []Match
	for _, m := range envelope.Body.Matches.Match {
		if len(matches) >= 128 {
			break
		}
		if len(m.Endpoint.Address) > 2048 {
			continue
		}
		addresses := strings.Fields(m.XAddrs)
		if len(addresses) > 32 {
			addresses = addresses[:32]
		}
		matches = append(matches, Match{Address: strings.TrimSpace(m.Endpoint.Address), XAddrs: strings.Fields(m.XAddrs), Scopes: strings.Fields(m.Scopes)})
		matches[len(matches)-1].XAddrs = addresses
	}
	return matches, nil
}

// XAddrs preserves the old CLI parser's deliberately namespace-agnostic API.
func XAddrs(data []byte) []string {
	decoder := xml.NewDecoder(strings.NewReader(string(data)))
	var out []string
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == "XAddrs" {
			var text string
			if decoder.DecodeElement(&text, &start) == nil {
				out = append(out, strings.Fields(text)...)
			}
		}
	}
	return out
}

func normalizeEndpoint(raw, scope string) (camera.Endpoint, error) {
	if len(raw) > 4096 {
		return camera.Endpoint{}, camera.ErrInvalid
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "rtsp" && u.Scheme != "rtsps") || u.User != nil || u.Fragment != "" {
		return camera.Endpoint{}, camera.ErrInvalid
	}
	clean, _ := url.Parse(camera.SanitizeURI(u.String()))
	if clean == nil || clean.Query().Encode() != u.Query().Encode() {
		return camera.Endpoint{}, camera.ErrInvalid
	}
	u.Host = strings.ToLower(u.Host)
	return camera.Endpoint{URL: u.String(), NetworkScope: scope}, nil
}

func localAddress(scope camera.DiscoveryScope) (string, error) {
	if scope.Interface == "" {
		if scope.LocalAddress == "" {
			return "0.0.0.0", nil
		}
		ip := net.ParseIP(scope.LocalAddress)
		if ip == nil || ip.To4() == nil {
			return "", camera.ErrInvalid
		}
		return ip.String(), nil
	}
	iface, err := net.InterfaceByName(scope.Interface)
	if err != nil {
		return "", camera.ErrInvalid
	}
	addresses, err := iface.Addrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addresses {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && ip.To4() != nil && (scope.LocalAddress == "" || ip.Equal(net.ParseIP(scope.LocalAddress))) {
			return ip.String(), nil
		}
	}
	return "", camera.ErrInvalid
}

func candidateKey(scope, id, endpoint string) string {
	sum := sha256.Sum256([]byte(scope + "\x00" + id + "\x00" + endpoint))
	return hex.EncodeToString(sum[:16])
}

func hostOnly(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

func uniqueEndpoints(endpoints []camera.Endpoint) []camera.Endpoint {
	seen := map[camera.Endpoint]bool{}
	out := make([]camera.Endpoint, 0, len(endpoints))
	for _, e := range endpoints {
		if !seen[e] {
			seen[e] = true
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out
}

func sorted(observations map[string]camera.Observation) []camera.Observation {
	out := make([]camera.Observation, 0, len(observations))
	for _, o := range observations {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Device.CandidateID < out[j].Device.CandidateID })
	return out
}

func newProbe() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	id := fmt.Sprintf("%x-%x-%x-%x-%x", nonce[0:4], nonce[4:6], nonce[6:8], nonce[8:10], nonce[10:16])
	return `<?xml version="1.0" encoding="UTF-8"?><e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope" xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl"><e:Header><w:MessageID>urn:uuid:` + id + `</w:MessageID><w:To e:mustUnderstand="true">urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To><w:Action e:mustUnderstand="true">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action></e:Header><e:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></e:Body></e:Envelope>`, nil
}
