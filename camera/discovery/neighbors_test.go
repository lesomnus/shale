package discovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

type neighborSourceFunc func(context.Context, string) ([]Neighbor, error)

func (f neighborSourceFunc) Neighbors(ctx context.Context, iface string) ([]Neighbor, error) {
	return f(ctx, iface)
}

func initialScope() camera.DiscoveryScope {
	return camera.DiscoveryScope{ID: "poe", Interface: "camera0", Seeds: []camera.Endpoint{
		{URL: "http://192.0.2.10:2020/onvif/device_service", NetworkScope: "poe"},
		{URL: "http://192.0.2.20:2020/onvif/device_service", NetworkScope: "poe"},
	}}
}

func TestNeighborObservationsUseOnlyExplicitSeedHostsAndExactInterface(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	calls := 0
	provider := NeighborObservations{Now: func() time.Time { return at }, Source: neighborSourceFunc(func(_ context.Context, iface string) ([]Neighbor, error) {
		calls++
		require.Equal(t, "camera0", iface)
		return []Neighbor{
			{Address: "192.0.2.10", MAC: "02-00-00-00-00-01", States: []string{"REACHABLE"}},
			{Address: "192.0.2.20", MAC: "02:00:00:00:00:02", States: []string{"STALE"}},
			{Address: "192.0.2.99", MAC: "02:00:00:00:00:99", States: []string{"REACHABLE"}},
		}, nil
	})}
	out, err := provider.ObserveInitial(t.Context(), initialScope())
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, 1, calls)
	for _, o := range out {
		require.Equal(t, "poe", o.Scope)
		require.Empty(t, o.Device.ID)
		require.Empty(t, o.Device.DiscoveryID)
		require.Len(t, o.Device.Endpoints, 1)
		require.Equal(t, at, o.ObservedAt)
		require.Equal(t, "neighbor_cache", o.Device.Evidence[0].Source)
		require.False(t, o.Device.Evidence[0].Authenticated)
	}
	scope := initialScope()
	scope.Seeds = nil
	out, err = provider.ObserveInitial(t.Context(), scope)
	require.NoError(t, err)
	require.Empty(t, out)
	require.Equal(t, 1, calls, "no seeds means no system query")
}

func TestNeighborObservationsRejectUnknownFailedAndAmbiguousMappings(t *testing.T) {
	for _, entry := range []Neighbor{
		{Address: "192.0.2.10", MAC: "02:00:00:00:00:01", States: []string{"FAILED"}},
		{Address: "192.0.2.10", MAC: "02:00:00:00:00:01", States: []string{"INCOMPLETE"}},
		{Address: "192.0.2.10", MAC: "02:00:00:00:00:01"},
		{Address: "192.0.2.10", MAC: "01:00:00:00:00:01", States: []string{"REACHABLE"}},
		{Address: "192.0.2.10", MAC: "00:00:00:00:00:00", States: []string{"REACHABLE"}},
		{Address: "not-an-address", MAC: "02:00:00:00:00:01", States: []string{"REACHABLE"}},
	} {
		p := NeighborObservations{Source: neighborSourceFunc(func(context.Context, string) ([]Neighbor, error) { return []Neighbor{entry}, nil })}
		out, err := p.ObserveInitial(t.Context(), initialScope())
		require.NoError(t, err)
		require.Empty(t, out, "%+v", entry)
	}
	for _, entries := range [][]Neighbor{
		{{Address: "192.0.2.10", MAC: "02:00:00:00:00:01", States: []string{"REACHABLE"}}, {Address: "192.0.2.20", MAC: "02:00:00:00:00:01", States: []string{"REACHABLE"}}},
		{{Address: "192.0.2.10", MAC: "02:00:00:00:00:01", States: []string{"REACHABLE"}}, {Address: "192.0.2.10", MAC: "02:00:00:00:00:02", States: []string{"REACHABLE"}}},
	} {
		p := NeighborObservations{Source: neighborSourceFunc(func(context.Context, string) ([]Neighbor, error) { return entries, nil })}
		out, err := p.ObserveInitial(t.Context(), initialScope())
		require.NoError(t, err)
		require.Empty(t, out, "conflicting cache entries do not establish identity")
	}
}

func TestNeighborObservationBoundsAndCancellation(t *testing.T) {
	calls := 0
	p := NeighborObservations{Source: neighborSourceFunc(func(context.Context, string) ([]Neighbor, error) { calls++; return make([]Neighbor, 1025), nil })}
	for _, alter := range []func(*camera.DiscoveryScope){
		func(s *camera.DiscoveryScope) { s.Interface = "" },
		func(s *camera.DiscoveryScope) { s.Interface = "camera*" },
		func(s *camera.DiscoveryScope) { s.Interface = "lo" },
		func(s *camera.DiscoveryScope) { s.Seeds = make([]camera.Endpoint, 33) },
		func(s *camera.DiscoveryScope) { s.Seeds[0].NetworkScope = "other" },
		func(s *camera.DiscoveryScope) { s.Seeds[0].URL = "http://camera.example/onvif" },
		func(s *camera.DiscoveryScope) { s.Seeds[0].URL = "http://admin:private@192.0.2.10/onvif" },
	} {
		scope := initialScope()
		alter(&scope)
		_, err := p.ObserveInitial(t.Context(), scope)
		require.Error(t, err)
	}
	require.Zero(t, calls, "invalid intent is refused before a system query")
	_, err := p.ObserveInitial(t.Context(), initialScope())
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = p.ObserveInitial(ctx, initialScope())
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

type initialProviderFunc func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error)

func (f initialProviderFunc) ObserveInitial(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	return f(ctx, scope)
}

type primaryDiscovererFunc func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error)

func (f primaryDiscovererFunc) Discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	return f(ctx, scope)
}

func TestInitialCompositionKeepsOneCandidateWhenONVIFAppears(t *testing.T) {
	scope := initialScope()
	initial := camera.Observation{Scope: scope.ID, Device: camera.DeviceHint{MAC: "02:00:00:00:00:01", CandidateID: "initial", Endpoints: scope.Seeds[:1]}}
	seed := camera.Observation{Scope: scope.ID, Device: camera.DeviceHint{CandidateID: "seed", Endpoints: scope.Seeds[:1]}}
	epr := seed
	epr.Device.DiscoveryID = "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248"
	epr.Device.CandidateID = "onvif"
	p := WithInitial{Provider: initialProviderFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		return []camera.Observation{initial}, nil
	}), Discoverer: primaryDiscovererFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		return []camera.Observation{seed, epr}, nil
	})}
	out, err := p.Discover(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, epr.Device.DiscoveryID, out[0].Device.DiscoveryID)
	require.Equal(t, initial.Device.MAC, out[0].Device.MAC)
	require.Len(t, out[0].Device.Endpoints, 1)
	conflict := epr
	conflict.Device.DiscoveryID = "urn:uuid:60c1ccab-2b74-4229-84a6-446fb7bc1cc9"
	_, err = combineInitial(scope, []camera.Observation{epr, conflict}, []camera.Observation{initial})
	require.ErrorIs(t, err, camera.ErrAmbiguousIdentity)
	conflict = epr
	conflict.Device.MAC = "02:00:00:00:00:ff"
	_, err = combineInitial(scope, []camera.Observation{conflict}, []camera.Observation{initial})
	require.ErrorIs(t, err, camera.ErrAmbiguousIdentity)
	p.Discoverer = nil
	out, err = p.Discover(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, out, 1, "pre-activation inventory needs no ONVIF response")
	p.Provider = initialProviderFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		return nil, errors.New("provider unavailable")
	})
	_, err = p.Discover(t.Context(), scope)
	require.Error(t, err)
}
