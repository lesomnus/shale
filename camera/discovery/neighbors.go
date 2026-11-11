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

// NeighborObservations resolves only the explicitly assigned seed hosts on one
// exact interface. It sends no packets, scans no prefixes and reads no leases or
// credentials. Missing or unusable entries remain unknown until another normal
// network operation populates the kernel neighbor cache.
type NeighborObservations struct {
	Source NeighborSource
	Now    func() time.Time
}

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope.ID == "" || !exactInterface.MatchString(scope.Interface) || scope.Interface == "lo" || len(scope.Seeds) > 32 || p.Source == nil {
		return nil, camera.ErrInvalid
	}
	if len(scope.Seeds) == 0 {
		return nil, nil
	}
	seeds := map[string][]camera.Endpoint{}
	for _, ep := range scope.Seeds {
		host, err := seedHost(ep, scope.ID)
		if err != nil {
			return nil, err
		}
		seeds[host] = append(seeds[host], ep)
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
		if len(seeds[host]) == 0 || mac == "" {
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
		observations = append(observations, camera.Observation{Scope: scope.ID, ObservedAt: at, Device: camera.DeviceHint{CandidateID: candidateKey(scope.ID, "mac/"+mac, host), MAC: mac, Endpoints: append([]camera.Endpoint(nil), seeds[host]...), Evidence: []camera.Evidence{{Source: "neighbor_cache", At: at}}}})
	}
	slices.SortFunc(observations, func(a, b camera.Observation) int { return strings.Compare(a.Device.CandidateID, b.Device.CandidateID) })
	return observations, nil
}
