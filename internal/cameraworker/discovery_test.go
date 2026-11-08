package cameraworker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type discovererFunc func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error)

func (f discovererFunc) Discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	return f(ctx, scope)
}

type discoveryWorkerClient struct {
	api.CameraWorkerServiceClient
	poll func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error)
}

func (c discoveryWorkerClient) Poll(ctx context.Context, request *api.CameraWorkerPollRequest, _ ...grpc.CallOption) (*api.CameraWorkerPollResponse, error) {
	return c.poll(ctx, request)
}

type discoveryPolicyClient struct {
	api.CameraPolicyServiceClient
	observe func(context.Context, *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error)
}

func (c discoveryPolicyClient) Observe(ctx context.Context, request *api.CameraPolicyObserveRequest, _ ...grpc.CallOption) (*api.CameraPolicyObserveResponse, error) {
	return c.observe(ctx, request)
}

func discoveryFixture(t *testing.T) (*Worker, *api.CameraWorkerAssignment, *pki.CA) {
	t.Helper()
	id := pdid.New(Domain)
	ca, err := pki.NewCA("discovery-test", time.Now())
	require.NoError(t, err)
	store := pki.Store{Dir: t.TempDir()}
	key, err := store.Key()
	require.NoError(t, err)
	csr, err := key.CSR("discovery-test")
	require.NoError(t, err)
	cert, err := ca.IssueClientHost(csr, id, time.Now())
	require.NoError(t, err)
	require.NoError(t, store.SetCert(pki.EncodeCerts(cert), ca.Bundle()))
	agent := &hostagent.Agent{Kind: Domain, Store: store, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.NoError(t, agent.Init())
	w := &Worker{agent: agent, cfg: Config{DiscoveryInterval: time.Millisecond, DiscoveryTimeout: time.Second, HeartbeatInterval: time.Millisecond, LeaseSeconds: 3, Log: agent.Log}}
	network := api.CameraNetwork_builder{Id: pdid.New(26).Bytes(), Tenant: api.Tenant_builder{Id: pdid.New(1).Bytes()}.Build(), Cidrs: []string{"192.0.2.0/24"}, Seeds: []string{"http://192.0.2.10/onvif"}, Multicast: true, InterfaceHint: "camera0"}.Build()
	a := api.CameraAssignment_builder{Id: pdid.New(28).Bytes(), Tenant: network.GetTenant(), Network: network, WorkerId: id.Bytes(), Generation: 7}.Build()
	return w, api.CameraWorkerAssignment_builder{Assignment: a, Network: network}.Build(), ca
}

func observation(scope, endpoint string) camera.Observation {
	return camera.Observation{Scope: scope, ObservedAt: time.Now().UTC(), Device: camera.DeviceHint{CandidateID: "candidate", DiscoveryID: "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248", Endpoints: []camera.Endpoint{{URL: endpoint, NetworkScope: scope}}}}
}

func TestDiscoveryConfigurationIsOptionalAndBounded(t *testing.T) {
	w, err := New(Config{StateDir: t.TempDir(), Cp: "cp.example"})
	require.NoError(t, err)
	require.Nil(t, w.cfg.Discoverer)
	require.Equal(t, 30*time.Second, w.cfg.DiscoveryInterval)
	require.Equal(t, 2*time.Second, w.cfg.DiscoveryTimeout)
	for _, cfg := range []Config{
		{DiscoveryInterval: time.Millisecond}, {DiscoveryInterval: time.Hour + 1},
		{DiscoveryTimeout: -1}, {DiscoveryTimeout: time.Minute},
	} {
		cfg.StateDir, cfg.Cp = t.TempDir(), "cp.example"
		_, err := New(cfg)
		require.Error(t, err)
	}
}

func TestDiscoveryIdentityNeverUsesAddressOrInventoryClaim(t *testing.T) {
	for _, tc := range []struct {
		hint camera.DeviceHint
		want string
	}{
		{camera.DeviceHint{DiscoveryID: "URN:UUID:9C654A77-3A86-4466-B189-7BBDC38AD248"}, "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248"},
		{camera.DeviceHint{MAC: "02-00-00-00-00-01"}, "mac/02:00:00:00:00:01"},
		{camera.DeviceHint{DiscoveryID: "urn:uuid:00000000-0000-0000-0000-000000000000"}, ""},
		{camera.DeviceHint{DiscoveryID: "http://192.0.2.10/onvif"}, ""},
		{camera.DeviceHint{DiscoveryID: "192.0.2.10"}, ""},
		{camera.DeviceHint{DiscoveryID: "opaque-device"}, ""},
		{camera.DeviceHint{ID: "claimed", CandidateID: "ip-derived-candidate"}, ""},
		{camera.DeviceHint{MAC: "01:00:00:00:00:01"}, ""},
		{camera.DeviceHint{MAC: "00:00:00:00:00:00"}, ""},
		{camera.DeviceHint{MAC: "02:00:00:00:00:01:00:00"}, ""},
	} {
		require.Equal(t, tc.want, discoveryIdentity(tc.hint), "%+v", tc.hint)
	}
}

func TestDiscoveryRequestsDeduplicateAndRejectAmbiguousIdentity(t *testing.T) {
	w, assigned, _ := discoveryFixture(t)
	scope, err := discoveryScope(assigned, w.ID().Bytes(), time.Second)
	require.NoError(t, err)
	first := observation(scope.ID, "http://192.0.2.10/onvif")
	first.Device.ID = "forged-inventory-id"
	first.Device.Evidence = []camera.Evidence{{Source: "forged", Authenticated: true}}
	first.Device.Serial = "untrusted-serial"
	duplicate := first
	duplicate.Device.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.10/media", NetworkScope: scope.ID}}
	requests, ambiguous, err := discoveryRequests(assigned, scope, []camera.Observation{first, duplicate})
	require.NoError(t, err)
	require.False(t, ambiguous)
	require.Len(t, requests, 1)
	require.Len(t, requests[0].GetEndpoints(), 2)
	require.False(t, requests[0].GetIdentity().GetAuthenticated())
	require.Equal(t, "untrusted-serial", requests[0].GetIdentity().GetSerial())
	require.Equal(t, assigned.GetAssignment().GetId(), requests[0].GetAssignment().GetId())
	for _, alter := range []func(*camera.Observation){
		func(o *camera.Observation) { o.Device.CandidateID = "another-candidate" },
		func(o *camera.Observation) {
			o.Device.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.20/onvif", NetworkScope: scope.ID}}
		},
	} {
		collision := first
		alter(&collision)
		requests, ambiguous, err := discoveryRequests(assigned, scope, []camera.Observation{first, collision})
		require.NoError(t, err)
		require.True(t, ambiguous)
		require.Empty(t, requests, "the entire conflicting key must be suppressed")
	}
	// A later scan at a DHCP address keeps the deduplication hint. Physical
	// authentication is left to the existing leased inspection path.
	moved := observation(scope.ID, "http://192.0.2.20/onvif")
	moved.Device.CandidateID = "changed-with-address"
	requests, ambiguous, err = discoveryRequests(assigned, scope, []camera.Observation{moved})
	require.NoError(t, err)
	require.False(t, ambiguous)
	require.Equal(t, discoveryIdentity(first.Device), requests[0].GetDiscoveryId())
	seed := moved
	seed.Device.DiscoveryID = ""
	requests, _, err = discoveryRequests(assigned, scope, []camera.Observation{seed})
	require.NoError(t, err)
	require.Empty(t, requests)
}

func TestDiscoveryRejectsUnsafeNetworkAndObservations(t *testing.T) {
	w, assigned, _ := discoveryFixture(t)
	for _, modify := range []func(*api.CameraWorkerAssignment){
		func(v *api.CameraWorkerAssignment) { v.GetAssignment().SetWorkerId(pdid.New(Domain).Bytes()) },
		func(v *api.CameraWorkerAssignment) { v.GetNetwork().SetId(pdid.New(26).Bytes()) },
		func(v *api.CameraWorkerAssignment) {
			v.GetNetwork().SetTenant(api.Tenant_builder{Id: pdid.New(1).Bytes()}.Build())
		},
		func(v *api.CameraWorkerAssignment) { v.GetNetwork().SetCidrs([]string{"0.0.0.0/0"}) },
		func(v *api.CameraWorkerAssignment) { v.GetNetwork().SetCidrs([]string{"192.0.2.1/24"}) },
		func(v *api.CameraWorkerAssignment) { v.GetNetwork().SetSeeds([]string{"http://198.51.100.1/onvif"}) },
		func(v *api.CameraWorkerAssignment) {
			v.GetNetwork().SetSeeds([]string{"http://user:secret@192.0.2.1/onvif"})
		},
	} {
		v := proto.Clone(assigned).(*api.CameraWorkerAssignment)
		// Break the fixture's shared network edge before mutation.
		v.GetAssignment().SetNetwork(proto.Clone(assigned.GetNetwork()).(*api.CameraNetwork))
		v.GetAssignment().SetTenant(proto.Clone(assigned.GetNetwork().GetTenant()).(*api.Tenant))
		modify(v)
		_, err := discoveryScope(v, w.ID().Bytes(), time.Second)
		require.Error(t, err)
	}
	scope, err := discoveryScope(assigned, w.ID().Bytes(), time.Second)
	require.NoError(t, err)
	for _, endpoint := range []string{
		"http://198.51.100.1/onvif", "http://camera.example/onvif", "http://192.0.2.1/onvif?password=secret",
		"http://user:secret@192.0.2.1/onvif", "http://192.0.2.1/onvif#fragment", "ftp://192.0.2.1/onvif",
	} {
		_, _, err := discoveryRequests(assigned, scope, []camera.Observation{observation(scope.ID, endpoint)})
		require.Error(t, err, endpoint)
	}
	other := observation("another-network", "http://192.0.2.10/onvif")
	_, _, err = discoveryRequests(assigned, scope, []camera.Observation{other})
	require.Error(t, err)
	_, _, err = discoveryRequests(assigned, scope, make([]camera.Observation, 257))
	require.Error(t, err)
}

func TestDiscoveryRefreshesAssignmentAndNetworkImmediatelyBeforeLAN(t *testing.T) {
	w, assigned, _ := discoveryFixture(t)
	fresh := proto.Clone(assigned).(*api.CameraWorkerAssignment)
	fresh.GetAssignment().SetGeneration(8)
	fresh.GetNetwork().SetCidrs([]string{"198.51.100.0/24"})
	fresh.GetNetwork().SetSeeds([]string{"http://198.51.100.10/onvif"})
	fresh.GetNetwork().SetInterfaceHint("camera-new")
	polls, scans, observes := 0, 0, 0
	workers := discoveryWorkerClient{poll: func(ctx context.Context, r *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		require.Zero(t, r.GetWaitSeconds())
		require.Empty(t, r.GetAfterRevision())
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), discoveryRPCTimeout)
		polls++
		v := fresh
		if polls == 1 {
			v = assigned
		}
		return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{v}}.Build(), nil
	}}
	w.cfg.Discoverer = discovererFunc(func(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
		scans++
		require.Equal(t, 2, polls)
		require.Equal(t, "camera-new", scope.Interface)
		require.Equal(t, "http://198.51.100.10/onvif", scope.Seeds[0].URL)
		require.Less(t, scope.Timeout, w.cfg.DiscoveryTimeout)
		_, ok := ctx.Deadline()
		require.True(t, ok)
		return []camera.Observation{observation(scope.ID, scope.Seeds[0].URL)}, nil
	})
	policies := discoveryPolicyClient{observe: func(ctx context.Context, r *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error) {
		observes++
		require.Equal(t, int64(8), r.GetAssignmentGeneration())
		require.Equal(t, fresh.GetAssignment().GetId(), r.GetAssignment().GetId())
		_, ok := ctx.Deadline()
		require.True(t, ok)
		return &api.CameraPolicyObserveResponse{}, nil
	}}
	w.discoverAssigned(t.Context(), workers, policies)
	require.Equal(t, 1, scans)
	require.Equal(t, 1, observes)
	// A removed assignment in that fresh read never reaches a LAN transport.
	polls = 0
	workers.poll = func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		polls++
		if polls == 1 {
			return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{assigned}}.Build(), nil
		}
		return &api.CameraWorkerPollResponse{}, nil
	}
	w.discoverAssigned(t.Context(), workers, policies)
	require.Equal(t, 1, scans)
	require.Equal(t, 1, observes)
}

func TestDiscoveryTimeoutAndObserveDenialRetryWithoutLeakingErrors(t *testing.T) {
	w, assigned, _ := discoveryFixture(t)
	var logs bytes.Buffer
	w.cfg.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	w.cfg.DiscoveryTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	workers := discoveryWorkerClient{poll: func(context.Context, *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
		return api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{assigned}}.Build(), nil
	}}
	scans, observes := 0, 0
	w.cfg.Discoverer = discovererFunc(func(call context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
		scans++
		if scans == 1 {
			<-call.Done()
			return nil, errors.New("rtsp://admin:camera-secret@192.0.2.10/stream")
		}
		return []camera.Observation{observation(scope.ID, "http://192.0.2.10/onvif")}, nil
	})
	policies := discoveryPolicyClient{observe: func(_ context.Context, r *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error) {
		observes++
		require.Equal(t, int64(7), r.GetAssignmentGeneration(), "retain the generation used by the LAN call")
		if observes == 2 {
			cancel()
		}
		return nil, status.Error(codes.PermissionDenied, "old assignment; camera-secret")
	}}
	require.ErrorIs(t, w.discover(ctx, workers, policies), context.Canceled)
	require.Equal(t, 3, scans)
	require.Equal(t, 2, observes)
	require.Contains(t, logs.String(), "camera worker discovery failed; retrying")
	require.Contains(t, logs.String(), `"code":"PermissionDenied"`)
	for _, private := range []string{"camera-secret", "admin", "192.0.2.10", "old assignment"} {
		require.NotContains(t, logs.String(), private)
	}
}
