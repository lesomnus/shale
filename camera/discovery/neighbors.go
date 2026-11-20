package discovery

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

// Neighbor is a read-only link-layer cache entry, never authenticated identity.
type Neighbor struct {
	Address string
	MAC     string
	States  []string
}

type NeighborSource interface {
	Neighbors(context.Context, string) ([]Neighbor, error)
}

// NeighborObservations resolves, on one exact interface, the explicitly
// assigned seed hosts and, through ObserveDiscovered, the hosts that answered
// the regular discoverer inside the network's current CIDRs. It sends no
// packets, scans no prefixes and reads no leases or credentials. Missing or
// unusable entries remain unknown until another normal network operation
// populates the kernel neighbor cache.
type NeighborObservations struct {
	Source NeighborSource
	Now    func() time.Time
}

// maxDiscoveredHosts bounds the discovered hosts one network resolves, as
// many as WithInitial accepts discovered observations.
const maxDiscoveredHosts = 256

var exactInterface = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,14}$`)

func unicastMAC(raw string) string {
	mac, err := net.ParseMAC(raw)
	if err != nil || len(mac) != 6 || mac[0]&1 != 0 {
		return ""
	}
	zero := true
	for _, b := range mac {
		zero = zero && b == 0
	}
	if zero {
		return ""
	}
	return mac.String()
}

func seedHost(ep camera.Endpoint, scope string) (string, error) {
	u, err := url.Parse(ep.URL)
	if err != nil || ep.NetworkScope != scope || u.User != nil || u.Opaque != "" || u.Fragment != "" || len(ep.URL) > 2048 || ep.URL != camera.SanitizeURI(ep.URL) {
		return "", camera.ErrForbidden
	}
	if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return "", camera.ErrForbidden
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return "", camera.ErrForbidden
	}
	return ip.Unmap().String(), nil
}

func usableNeighbor(states []string) bool {
	if len(states) != 1 {
		return false
	}
	switch strings.ToUpper(states[0]) {
	case "REACHABLE", "STALE", "DELAY", "PROBE", "PERMANENT":
		return true
	}
	return false
}

func (p NeighborObservations) ObserveInitial(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	return p.ObserveDiscovered(ctx, scope, nil)
}

// ObserveDiscovered resolves the seed hosts and the hosts of discovered
// observations that name no MAC. A static-address camera has no DHCP lease,
// and one whose WS-Discovery answer carries no durable identity would
// otherwise be recognized only after being made a seed. A discovered host
// counts only inside the scope's current CIDRs, and, as for a seed, only that
// host's existing kernel cache entry is read.
func (p NeighborObservations) ObserveDiscovered(ctx context.Context, scope camera.DiscoveryScope, discovered []camera.Observation) ([]camera.Observation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope.ID == "" || !exactInterface.MatchString(scope.Interface) || scope.Interface == "lo" || len(scope.Seeds) > 32 || p.Source == nil {
		return nil, camera.ErrInvalid
	}
	hosts := map[string][]camera.Endpoint{}
	for _, ep := range scope.Seeds {
		host, err := seedHost(ep, scope.ID)
		if err != nil {
			return nil, err
		}
		hosts[host] = append(hosts[host], ep)
	}
	if err := addDiscovered(hosts, scope, discovered); err != nil {
		return nil, err
	}
	if len(hosts) == 0 {
		return nil, nil
	}
	entries, err := p.Source.Neighbors(ctx, scope.Interface)
	if err != nil {
		return nil, err
	}
	if len(entries) > 1024 {
		return nil, camera.ErrInvalid
	}
	byHost, byMAC, ambiguous := map[string]string{}, map[string]string{}, map[string]bool{}
	for _, entry := range entries {
		ip, err := netip.ParseAddr(entry.Address)
		if err != nil || !usableNeighbor(entry.States) {
			continue
		}
		host, mac := ip.Unmap().String(), unicastMAC(entry.MAC)
		if len(hosts[host]) == 0 || mac == "" {
			continue
		}
		if old := byHost[host]; old != "" && old != mac {
			ambiguous[host] = true
		}
		if old := byMAC[mac]; old != "" && old != host {
			ambiguous[old], ambiguous[host] = true, true
		}
		byHost[host], byMAC[mac] = mac, host
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	var observations []camera.Observation
	for host, mac := range byHost {
		if ambiguous[host] {
			continue
		}
		at := now().UTC()
		observations = append(observations, camera.Observation{Scope: scope.ID, ObservedAt: at, Device: camera.DeviceHint{CandidateID: candidateKey(scope.ID, "mac/"+mac, host), MAC: mac, Endpoints: append([]camera.Endpoint(nil), hosts[host]...), Evidence: []camera.Evidence{{Source: "neighbor_cache", At: at}}}})
	}
	slices.SortFunc(observations, func(a, b camera.Observation) int { return strings.Compare(a.Device.CandidateID, b.Device.CandidateID) })
	return observations, nil
}

// addDiscovered adds to hosts each discovered host inside the scope's CIDRs
// that no discovered observation names a MAC for, with the endpoints it
// advertised. A seed host keeps its seed endpoints. An observation WithInitial
// refuses is left for it to refuse.
func addDiscovered(hosts map[string][]camera.Endpoint, scope camera.DiscoveryScope, discovered []camera.Observation) error {
	if len(discovered) == 0 || len(scope.CIDRs) == 0 {
		return nil
	}
	if len(scope.CIDRs) > 32 || len(discovered) > maxDiscoveredHosts {
		return camera.ErrInvalid
	}
	allowed, err := scopeHosts(scope.CIDRs)
	if err != nil {
		return err
	}
	named, found := map[string]bool{}, map[string][]camera.Endpoint{}
	for _, o := range discovered {
		host, err := observationHost(o, scope.ID)
		if err != nil || !allowed(netip.MustParseAddr(host)) {
			continue
		}
		if o.Device.MAC != "" {
			named[host] = true
		} else if _, seen := found[host]; !seen {
			found[host] = o.Device.Endpoints
		}
	}
	for host, endpoints := range found {
		if _, seed := hosts[host]; !seed && !named[host] {
			hosts[host] = append([]camera.Endpoint(nil), endpoints...)
		}
	}
	return nil
}
