package discovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

type leaseSourceFunc func(context.Context, string) ([]Lease, error)

func (f leaseSourceFunc) Leases(ctx context.Context, iface string) ([]Lease, error) {
	return f(ctx, iface)
}

func leaseScope() camera.DiscoveryScope {
	s := initialScope()
	s.CIDRs = []string{"192.0.2.0/24"}
	return s
}

func TestDNSMasqLeaseFileBindsInterfaceAndIgnoresNonAddressFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leases")
	require.NoError(t, os.WriteFile(path, []byte("1798761600 02:00:00:00:00:01 192.0.2.42 claimed-device opaque-client-id\n0 02-00-00-00-00-02 192.0.2.43 * *\ninvalid 02:00:00:00:00:03 192.0.2.44 * *\n1798761600 01:00:00:00:00:04 192.0.2.45 * *\n1798761600 02:00:00:00:00:05 camera.example * *\n1798761600 02:00:00:00:00:06 127.0.0.1 * *\n1798761600 02:00:00:00:00:07 ::1 * *\n"), 0600))
	source := DNSMasqLeaseFiles{{Interface: "camera0", Path: path}}
	rows, err := source.Leases(t.Context(), "another0")
	require.NoError(t, err)
	require.Empty(t, rows)
	rows, err = source.Leases(t.Context(), "camera0")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "192.0.2.42", rows[0].Address)
	require.Equal(t, "02:00:00:00:00:02", rows[1].MAC)
	require.Equal(t, int64(1798761600), rows[0].ExpiresAt.Unix())
	require.True(t, rows[1].ExpiresAt.IsZero(), "dnsmasq zero is an explicitly infinite lease")
	// A missing file is normal before the daemon receives its first lease.
	require.NoError(t, os.Remove(path))
	rows, err = source.Leases(t.Context(), "camera0")
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestDNSMasqLeaseFileRejectsUnsafeFilesAndBoundsWithoutLeakingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "leases")
	require.NoError(t, os.WriteFile(path, []byte("private-camera-data"+strings.Repeat("x", maxLeaseFileBytes)), 0600))
	for _, source := range []DNSMasqLeaseFiles{
		{{Interface: "camera0", Path: path}},
		{{Interface: "camera0", Path: dir}},
		{{Interface: "camera0", Path: "relative/leases"}},
		{{Interface: "camera*", Path: path}},
		{{Interface: "camera0", Path: path}, {Interface: "camera0", Path: path}},
	} {
		_, err := source.Leases(t.Context(), "camera0")
		require.ErrorIs(t, err, camera.ErrInvalid)
		require.NotContains(t, err.Error(), "private-camera-data")
	}
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(path, link))
	_, err := (DNSMasqLeaseFiles{{Interface: "camera0", Path: link}}).Leases(t.Context(), "camera0")
	require.ErrorIs(t, err, camera.ErrInvalid)
	for _, data := range []string{
		strings.Repeat("\n", maxLeaseRows+1),
		strings.Repeat("0 02:00:00:00:00:01 192.0.2.1 * *\n", maxLeases+1),
		strings.Repeat("x", 8193),
	} {
		_, err := parseDNSMasqLeases(t.Context(), []byte(data))
		require.ErrorIs(t, err, camera.ErrInvalid)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = (DNSMasqLeaseFiles{{Interface: "camera0", Path: path}}).Leases(ctx, "camera0")
	require.ErrorIs(t, err, context.Canceled)
}

func TestDHCPObservationsUseFreshScopeAndManagementTemplates(t *testing.T) {
	at := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	calls := 0
	p := DHCPObservations{Now: func() time.Time { return at }, Source: leaseSourceFunc(func(_ context.Context, iface string) ([]Lease, error) {
		calls++
		require.Equal(t, "camera0", iface)
		return []Lease{
			{Address: "192.0.2.42", MAC: "02:00:00:00:00:01", ExpiresAt: at.Add(time.Hour)},
			{Address: "192.0.2.43", MAC: "02:00:00:00:00:02"},
			{Address: "192.0.2.44", MAC: "02:00:00:00:00:03", ExpiresAt: at},
			{Address: "198.51.100.42", MAC: "02:00:00:00:00:04", ExpiresAt: at.Add(time.Hour)},
			{Address: "192.0.2.0", MAC: "02:00:00:00:00:05"},
			{Address: "192.0.2.255", MAC: "02:00:00:00:00:06"},
		}, nil
	})}
	scope := leaseScope()
	rows, err := p.ObserveInitial(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, row := range rows {
		require.Equal(t, "poe", row.Scope)
		require.Equal(t, at, row.ObservedAt)
		require.Empty(t, row.Device.ID)
		require.Empty(t, row.Device.DiscoveryID)
		require.Empty(t, row.Device.Manufacturer)
		require.Empty(t, row.Device.Serial)
		require.Len(t, row.Device.Endpoints, 1, "identical seed management templates deduplicate")
		require.Contains(t, row.Device.Endpoints[0].URL, ":2020/onvif/device_service")
		require.Equal(t, "poe", row.Device.Endpoints[0].NetworkScope)
		require.Equal(t, "dhcp_lease", row.Device.Evidence[0].Source)
		require.False(t, row.Device.Evidence[0].Authenticated)
	}
	// The same file cannot expand today's CP network by retaining an old CIDR.
	scope.CIDRs = []string{"192.0.2.0/27"}
	rows, err = p.ObserveInitial(t.Context(), scope)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, 2, calls)
	for _, modify := range []func(*camera.DiscoveryScope){
		func(s *camera.DiscoveryScope) { s.CIDRs = nil },
		func(s *camera.DiscoveryScope) { s.CIDRs = []string{"0.0.0.0/0"} },
		func(s *camera.DiscoveryScope) { s.CIDRs = []string{"192.0.2.1/24"} },
		func(s *camera.DiscoveryScope) { s.Interface = "lo" },
		func(s *camera.DiscoveryScope) { s.Seeds[0].URL = "http://user:secret@192.0.2.10/onvif" },
		func(s *camera.DiscoveryScope) { s.Seeds[0].URL = "http://198.51.100.10/onvif" },
		func(s *camera.DiscoveryScope) { s.Seeds[0].URL = "http://192.0.2.10/onvif?token=secret" },
	} {
		s := leaseScope()
		modify(&s)
		_, err := p.ObserveInitial(t.Context(), s)
		require.Error(t, err)
	}
	require.Equal(t, 2, calls, "invalid scope is refused before accessing leases")
}

func TestDHCPObservationsSuppressConflictsAndRejectExcessOrUnavailableEvidence(t *testing.T) {
	for _, rows := range [][]Lease{
		{{Address: "192.0.2.42", MAC: "02:00:00:00:00:01"}, {Address: "192.0.2.42", MAC: "02:00:00:00:00:02"}},
		{{Address: "192.0.2.42", MAC: "02:00:00:00:00:01"}, {Address: "192.0.2.43", MAC: "02:00:00:00:00:01"}},
		{{Address: "192.0.2.42", MAC: "01:00:00:00:00:01"}},
	} {
		p := DHCPObservations{Source: leaseSourceFunc(func(context.Context, string) ([]Lease, error) { return rows, nil })}
		out, err := p.ObserveInitial(t.Context(), leaseScope())
		require.NoError(t, err)
		require.Empty(t, out)
	}
	for _, count := range []int{maxLeaseHosts + 1, maxLeases + 1} {
		p := DHCPObservations{Source: leaseSourceFunc(func(context.Context, string) ([]Lease, error) {
			rows := make([]Lease, count)
			for i := range rows {
				rows[i] = Lease{Address: fmt.Sprintf("192.0.2.%d", 1+i%250), MAC: fmt.Sprintf("02:00:00:00:%02x:%02x", i/256, i%256)}
			}
			return rows, nil
		})}
		_, err := p.ObserveInitial(t.Context(), leaseScope())
		require.ErrorIs(t, err, camera.ErrInvalid)
	}
	p := DHCPObservations{Source: leaseSourceFunc(func(context.Context, string) ([]Lease, error) { return nil, errors.New("unavailable") })}
	_, err := p.ObserveInitial(t.Context(), leaseScope())
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = p.ObserveInitial(ctx, leaseScope())
	require.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithCancel(t.Context())
	p.Source = leaseSourceFunc(func(context.Context, string) ([]Lease, error) { cancel(); return nil, nil })
	_, err = p.ObserveInitial(ctx, leaseScope())
	require.ErrorIs(t, err, context.Canceled, "an empty late source cannot hide cancellation")
}

func TestInitialProvidersReconcileLeaseWithNeighborAndONVIFEvidence(t *testing.T) {
	scope := leaseScope()
	lease := DHCPObservations{Source: leaseSourceFunc(func(context.Context, string) ([]Lease, error) {
		return []Lease{{Address: "192.0.2.42", MAC: "02:00:00:00:00:01"}}, nil
	})}
	initial := InitialProviders{initialProviderFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) { return nil, nil }), lease}
	p := WithInitial{Provider: initial, Discoverer: primaryDiscovererFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		return []camera.Observation{{Scope: "poe", Device: camera.DeviceHint{CandidateID: "onvif", DiscoveryID: "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248", Endpoints: []camera.Endpoint{{URL: "http://192.0.2.42:2020/onvif/device_service", NetworkScope: "poe"}}}}}, nil
	})}
	out, err := p.Discover(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "02:00:00:00:00:01", out[0].Device.MAC)
	require.NotEmpty(t, out[0].Device.DiscoveryID)
	contradiction := initialProviderFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		return []camera.Observation{{Scope: "poe", Device: camera.DeviceHint{MAC: "02:00:00:00:00:ff", Endpoints: []camera.Endpoint{{URL: "http://192.0.2.42:2020/onvif/device_service", NetworkScope: "poe"}}}}}, nil
	})
	_, err = (InitialProviders{lease, contradiction}).ObserveInitial(t.Context(), scope)
	require.ErrorIs(t, err, camera.ErrAmbiguousIdentity)
}
