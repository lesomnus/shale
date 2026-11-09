package discovery

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

const (
	maxLeaseFileBytes = 1 << 20
	maxLeaseRows      = 4096
	maxLeases         = 1024
	maxLeaseHosts     = 128
)

// Lease is a DHCP server's unauthenticated address/MAC hint. A zero ExpiresAt
// represents the server's explicitly non-expiring lease, not an identity claim.
type Lease struct {
	Address   string
	MAC       string
	ExpiresAt time.Time
}

type LeaseSource interface {
	Leases(context.Context, string) ([]Lease, error)
}

// DNSMasqLeaseFile binds one administrator-selected file to one exact interface.
// Lease contents never supply a path, interface, URL, credential or device ID.
type DNSMasqLeaseFile struct {
	Interface string
	Path      string
}

// DNSMasqLeaseFiles selects only the file bound to the current assigned
// interface. Interfaces without a configured source do not read any files.
type DNSMasqLeaseFiles []DNSMasqLeaseFile

func (s DNSMasqLeaseFiles) Leases(ctx context.Context, iface string) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !exactInterface.MatchString(iface) || iface == "lo" || len(s) == 0 || len(s) > 32 {
		return nil, camera.ErrInvalid
	}
	var path string
	seen := map[string]bool{}
	for _, file := range s {
		if !exactInterface.MatchString(file.Interface) || file.Interface == "lo" || len(file.Path) > 4096 || !filepath.IsAbs(file.Path) || filepath.Clean(file.Path) != file.Path || strings.ContainsAny(file.Path, "\x00\r\n") || seen[file.Interface] {
			return nil, camera.ErrInvalid
		}
		seen[file.Interface] = true
		if file.Interface == iface {
			path = file.Path
		}
	}
	if path == "" {
		return nil, nil
	}
	// Refuse a configured symlink, FIFO or oversized file before opening it.
	// The file and its directory are owned by the local DHCP administrator.
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // The DHCP daemon may not have received its first lease.
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxLeaseFileBytes {
		return nil, camera.ErrInvalid
	}
	f, err := openLeaseFile(path)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() > maxLeaseFileBytes || !os.SameFile(info, opened) {
		return nil, camera.ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, maxLeaseFileBytes+1))
	if err != nil || len(data) > maxLeaseFileBytes {
		return nil, camera.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return parseDNSMasqLeases(ctx, data)
}

func parseDNSMasqLeases(ctx context.Context, data []byte) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 4096), 8192)
	var out []Lease
	rows := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows++
		if rows > maxLeaseRows {
			return nil, camera.ErrInvalid
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) != 5 {
			continue
		}
		expires, err := strconv.ParseInt(fields[0], 10, 64)
		ip, ipErr := netip.ParseAddr(fields[2])
		mac := unicastMAC(fields[1])
		if err != nil || expires < 0 || ipErr != nil || !ip.Is4() || !ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() || mac == "" {
			continue
		}
		var at time.Time
		if expires != 0 {
			at = time.Unix(expires, 0)
			if at.Year() < 1970 || at.Year() > 9999 {
				continue
			}
		}
		// Hostname and client ID are deliberately not returned or used as identity.
		out = append(out, Lease{Address: ip.String(), MAC: mac, ExpiresAt: at})
		if len(out) > maxLeases {
			return nil, camera.ErrInvalid
		}
	}
	if scanner.Err() != nil {
		return nil, camera.ErrInvalid
	}
	return out, nil
}

// DHCPObservations creates only unauthenticated transport candidates. The
// caller supplies today's CIDRs and exact interface; current DHCP hosts reuse
// only the management scheme/port/path templates already named by its seeds.
// It sends no packets and never scans a prefix or reads camera credentials.
type DHCPObservations struct {
	Source LeaseSource
	Now    func() time.Time
}

func (p DHCPObservations) ObserveInitial(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scope.ID == "" || !exactInterface.MatchString(scope.Interface) || scope.Interface == "lo" || len(scope.CIDRs) == 0 || len(scope.CIDRs) > 32 || len(scope.Seeds) > 32 || p.Source == nil {
		return nil, camera.ErrInvalid
	}
	prefixes := make([]netip.Prefix, len(scope.CIDRs))
	for i, raw := range scope.CIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || prefix.Bits() == 0 || prefix != prefix.Masked() || !prefix.Addr().IsGlobalUnicast() || prefix.Addr().IsLinkLocalUnicast() {
			return nil, camera.ErrInvalid
		}
		prefixes[i] = prefix
	}
	allowed := func(ip netip.Addr) bool {
		return slices.ContainsFunc(prefixes, func(prefix netip.Prefix) bool {
			if !prefix.Contains(ip) {
				return false
			}
			if ip.Is4() && prefix.Bits() < 31 {
				addr, network := ip.As4(), prefix.Addr().As4()
				value, base := binary.BigEndian.Uint32(addr[:]), binary.BigEndian.Uint32(network[:])
				broadcast := base | (uint32(1)<<(32-prefix.Bits()) - 1)
				return value != base && value != broadcast
			}
			return true
		})
	}
	var templates []url.URL
	keys := map[string]bool{}
	for _, ep := range scope.Seeds {
		host, err := seedHost(ep, scope.ID)
		if err != nil {
			return nil, err
		}
		u, _ := url.Parse(ep.URL)
		if !allowed(netip.MustParseAddr(host)) {
			return nil, camera.ErrForbidden
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			continue
		}
		if u.RawQuery != "" {
			return nil, camera.ErrForbidden
		}
		// The exact host is replaced only after checking an active, scoped lease.
		key := u.Scheme + ":" + u.Port() + ":" + u.EscapedPath()
		if !keys[key] {
			keys[key] = true
			templates = append(templates, *u)
		}
	}
	if len(templates) == 0 {
		return nil, nil
	}
	entries, err := p.Source.Leases(ctx, scope.Interface)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(entries) > maxLeases {
		return nil, camera.ErrInvalid
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	at := now().UTC()
	byHost, byMAC, ambiguous := map[string]string{}, map[string]string{}, map[string]bool{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ip, err := netip.ParseAddr(entry.Address)
		mac := unicastMAC(entry.MAC)
		if err != nil || !ip.Is4() || !ip.IsGlobalUnicast() || ip.IsLinkLocalUnicast() || !allowed(ip) || mac == "" || !entry.ExpiresAt.IsZero() && (entry.ExpiresAt.Year() < 1970 || entry.ExpiresAt.Year() > 9999 || !entry.ExpiresAt.After(at)) {
			continue
		}
		host := ip.String()
		if old := byHost[host]; old != "" && old != mac {
			ambiguous[host] = true
		}
		if old := byMAC[mac]; old != "" && old != host {
			ambiguous[old], ambiguous[host] = true, true
		}
		byHost[host], byMAC[mac] = mac, host
	}
	if len(byHost) > maxLeaseHosts {
		return nil, camera.ErrInvalid
	}
	var out []camera.Observation
	for host, mac := range byHost {
		if ambiguous[host] {
			continue
		}
		var endpoints []camera.Endpoint
		for _, template := range templates {
			u := template
			u.Host = host
			if port := template.Port(); port != "" {
				u.Host = net.JoinHostPort(host, port)
			}
			endpoints = append(endpoints, camera.Endpoint{URL: u.String(), NetworkScope: scope.ID})
		}
		out = append(out, camera.Observation{Scope: scope.ID, ObservedAt: at, Device: camera.DeviceHint{CandidateID: candidateKey(scope.ID, "mac/"+mac, host), MAC: mac, Endpoints: endpoints, Evidence: []camera.Evidence{{Source: "dhcp_lease", At: at}}}})
	}
	slices.SortFunc(out, func(a, b camera.Observation) int { return strings.Compare(a.Device.CandidateID, b.Device.CandidateID) })
	return out, nil
}

// InitialProviders combines independent read-only hint sources. Contradictory
// mappings remain ambiguous instead of allowing one source to override another.
type InitialProviders []camera.InitialObservationProvider

func (p InitialProviders) ObserveInitial(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	if len(p) == 0 || len(p) > 8 {
		return nil, camera.ErrInvalid
	}
	var out []camera.Observation
	for _, provider := range p {
		if provider == nil {
			return nil, camera.ErrInvalid
		}
		rows, err := provider.ObserveInitial(ctx, scope)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return combineInitial(scope, nil, out)
}
