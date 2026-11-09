package cameraworker

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const discoveryRPCTimeout = 10 * time.Second

// Discovery failures do not cancel an executing job. The session owns this loop
// alongside heartbeat and job polling, and joins it before closing the connection
// on cancellation or certificate renewal. Discoverers must honor their context.
func (w *Worker) discover(ctx context.Context, workers api.CameraWorkerServiceClient, policies api.CameraPolicyServiceClient) error {
	for ctx.Err() == nil {
		w.discoverAssigned(ctx, workers, policies)
		timer := time.NewTimer(w.cfg.DiscoveryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func discoveryPoll(ctx context.Context, client api.CameraWorkerServiceClient) (*api.CameraWorkerPollResponse, error) {
	call, cancel := context.WithTimeout(ctx, discoveryRPCTimeout)
	defer cancel()
	snapshot, err := client.Poll(call, api.CameraWorkerPollRequest_builder{}.Build())
	if err != nil {
		return nil, err
	}
	if snapshot == nil || len(snapshot.GetAssignments()) > 100 {
		return nil, camera.ErrInvalid
	}
	return snapshot, nil
}

func (w *Worker) discoverAssigned(ctx context.Context, workers api.CameraWorkerServiceClient, policies api.CameraPolicyServiceClient) {
	snapshot, err := discoveryPoll(ctx, workers)
	if err != nil {
		w.logDiscoveryFailure(ctx, "poll", err)
		return
	}
	for _, selected := range snapshot.GetAssignments() {
		if ctx.Err() != nil {
			return
		}
		// The first snapshot selects IDs only. Each LAN operation needs today's
		// assignment and network, including changes while another LAN was scanned.
		fresh, err := discoveryPoll(ctx, workers)
		if err != nil {
			w.logDiscoveryFailure(ctx, "poll", err)
			continue
		}
		var assigned *api.CameraWorkerAssignment
		for _, candidate := range fresh.GetAssignments() {
			if bytes.Equal(candidate.GetAssignment().GetId(), selected.GetAssignment().GetId()) {
				assigned = candidate
				break
			}
		}
		if assigned == nil {
			continue
		}
		scope, err := discoveryScope(assigned, w.ID().Bytes(), w.cfg.DiscoveryTimeout)
		if err != nil {
			w.logDiscoveryFailure(ctx, "scope", err)
			continue
		}
		call, cancel := context.WithTimeout(ctx, w.cfg.DiscoveryTimeout)
		observations, err := w.cfg.Discoverer.Discover(call, scope)
		if err == nil {
			err = call.Err()
		}
		cancel()
		if err != nil {
			w.logDiscoveryFailure(ctx, "scan", err)
			continue
		}
		requests, ambiguous, err := discoveryRequests(assigned, scope, observations)
		if err != nil {
			w.logDiscoveryFailure(ctx, "observations", err)
			continue
		}
		if ambiguous {
			w.cfg.Log.Warn("camera worker discovery identity is ambiguous; candidates skipped")
		}
		for _, request := range requests {
			if ctx.Err() != nil {
				return
			}
			call, cancel := context.WithTimeout(ctx, discoveryRPCTimeout)
			// Retain the generation used for this scan. Observe rechecks current
			// CP authority and rejects handover/revocation during the LAN call.
			_, err := policies.Observe(call, request)
			cancel()
			if err != nil {
				w.logDiscoveryFailure(ctx, "observe", err)
			}
		}
	}
}

// Do not log device responses, endpoints, identities or error text. Injected
// transports can include secrets in any error, including a gRPC status message.
func (w *Worker) logDiscoveryFailure(ctx context.Context, phase string, err error) {
	if ctx.Err() != nil {
		return
	}
	code := status.Code(err)
	if err == context.DeadlineExceeded {
		code = codes.DeadlineExceeded
	}
	w.cfg.Log.Warn("camera worker discovery failed; retrying", "phase", phase, "code", code.String())
}

func discoveryScope(v *api.CameraWorkerAssignment, worker []byte, timeout time.Duration) (camera.DiscoveryScope, error) {
	a, n := v.GetAssignment(), v.GetNetwork()
	id := cameraID(n.GetId())
	if cameraID(a.GetId()) == "" || id == "" || cameraID(worker) == "" || !bytes.Equal(a.GetWorkerId(), worker) || a.GetGeneration() < 1 || !bytes.Equal(a.GetNetwork().GetId(), n.GetId()) || !bytes.Equal(a.GetTenant().GetId(), n.GetTenant().GetId()) || !bytes.Equal(a.GetSite().GetId(), n.GetSite().GetId()) {
		return camera.DiscoveryScope{}, camera.ErrForbidden
	}
	if len(n.GetCidrs()) == 0 || len(n.GetCidrs()) > 32 || len(n.GetSeeds()) > 32 || len(n.GetInterfaceHint()) > 128 || strings.ContainsAny(n.GetInterfaceHint(), "\x00\r\n") {
		return camera.DiscoveryScope{}, camera.ErrInvalid
	}
	for _, raw := range n.GetCidrs() {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p.Bits() == 0 || p != p.Masked() || p.Addr().IsUnspecified() || p.Addr().IsMulticast() {
			return camera.DiscoveryScope{}, camera.ErrInvalid
		}
	}
	// Leave room for a discoverer's normal observation-window completion before
	// the hard parent deadline, which must remain a cancellation/error boundary.
	scope := camera.DiscoveryScope{ID: id, Interface: n.GetInterfaceHint(), CIDRs: slices.Clone(n.GetCidrs()), Multicast: n.GetMulticast(), Timeout: timeout - timeout/10}
	for _, raw := range n.GetSeeds() {
		if _, err := discoveryEndpoint(raw, n.GetCidrs()); err != nil {
			return camera.DiscoveryScope{}, err
		}
		scope.Seeds = append(scope.Seeds, camera.Endpoint{URL: raw, NetworkScope: id})
	}
	return scope, nil
}

// CandidateID includes transport addresses in the concrete discoverer and must
// never be promoted to a durable key. UUID EPRs and scoped unicast MACs are only
// rediscovery hints; neither authenticates a physical inventory claim.
func discoveryIdentity(h camera.DeviceHint) string {
	if strings.HasPrefix(strings.ToLower(h.DiscoveryID), "urn:uuid:") {
		id, err := uuid.Parse(h.DiscoveryID[len("urn:uuid:"):])
		if err == nil && id != uuid.Nil() {
			return "urn:uuid:" + id.String()
		}
	}
	mac, err := net.ParseMAC(h.MAC)
	if err == nil && len(mac) == 6 && mac[0]&1 == 0 && !bytes.Equal(mac, make([]byte, 6)) {
		return "mac/" + mac.String()
	}
	return ""
}

func discoveryRequests(v *api.CameraWorkerAssignment, scope camera.DiscoveryScope, observations []camera.Observation) ([]*api.CameraPolicyObserveRequest, bool, error) {
	if len(observations) > 256 {
		return nil, false, camera.ErrInvalid
	}
	type candidate struct {
		request     *api.CameraPolicyObserveRequest
		candidateID string
		host        string
		ambiguous   bool
	}
	byID := map[string]*candidate{}
	var order []string
	for _, observation := range observations {
		if observation.Scope != scope.ID || len(observation.Device.Endpoints) == 0 || len(observation.Device.Endpoints) > 32 {
			return nil, false, camera.ErrForbidden
		}
		var endpoints []*api.CameraObservedEndpoint
		var host string
		for _, endpoint := range observation.Device.Endpoints {
			if endpoint.NetworkScope != scope.ID {
				return nil, false, camera.ErrForbidden
			}
			currentHost, err := discoveryEndpoint(endpoint.URL, v.GetNetwork().GetCidrs())
			if err != nil {
				return nil, false, err
			}
			if host != "" && host != currentHost {
				host = "ambiguous"
			} else {
				host = currentHost
			}
			var at *timestamppb.Timestamp
			if !observation.ObservedAt.IsZero() {
				at = timestamppb.New(observation.ObservedAt)
				if at.CheckValid() != nil {
					return nil, false, camera.ErrInvalid
				}
			}
			endpoints = append(endpoints, api.CameraObservedEndpoint_builder{Uri: endpoint.URL, Provenance: "discovery", ObservedAt: at}.Build())
		}
		key := discoveryIdentity(observation.Device)
		if key == "" {
			continue // An address seed alone cannot select a durable identity.
		}
		prior, exists := byID[key]
		if exists {
			if prior.candidateID != observation.Device.CandidateID || prior.host != host || host == "ambiguous" {
				prior.ambiguous = true
			}
			for _, endpoint := range endpoints {
				if !slices.ContainsFunc(prior.request.GetEndpoints(), func(old *api.CameraObservedEndpoint) bool { return old.GetUri() == endpoint.GetUri() }) {
					prior.request.SetEndpoints(append(prior.request.GetEndpoints(), endpoint))
				}
			}
			if len(prior.request.GetEndpoints()) > 32 {
				return nil, false, camera.ErrInvalid
			}
			continue
		}
		identity := identityEvidence(observation.Device)
		identity.SetAuthenticated(false)
		request := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: v.GetAssignment().GetId()}.Build(), AssignmentGeneration: v.GetAssignment().GetGeneration(), DiscoveryId: key, Identity: identity, Endpoints: endpoints}.Build()
		byID[key] = &candidate{request: request, candidateID: observation.Device.CandidateID, host: host, ambiguous: host == "ambiguous"}
		order = append(order, key)
	}
	var requests []*api.CameraPolicyObserveRequest
	ambiguous := false
	for _, key := range order {
		if byID[key].ambiguous {
			ambiguous = true
			continue
		}
		requests = append(requests, byID[key].request)
	}
	return requests, ambiguous, nil
}

// Match the CP's endpoint restrictions before passing any seed to a LAN driver
// or any observation back to Observe. DNS, credentials and opaque queries are
// excluded even when an injected discoverer supplies them.
func discoveryEndpoint(raw string, cidrs []string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.User != nil || u.Fragment != "" || u.Opaque != "" || raw != camera.SanitizeURI(raw) || u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return "", camera.ErrForbidden
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() {
		return "", camera.ErrForbidden
	}
	ip = ip.Unmap()
	allowed := false
	for _, raw := range cidrs {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p.Bits() == 0 || p != p.Masked() {
			return "", camera.ErrForbidden
		}
		allowed = allowed || p.Contains(ip)
	}
	if !allowed {
		return "", camera.ErrForbidden
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", camera.ErrForbidden
	}
	for key, values := range query {
		switch strings.ToLower(key) {
		case "channel", "subtype", "stream", "streamid", "profile", "transport", "unicast", "proto":
		default:
			return "", camera.ErrForbidden
		}
		for _, value := range values {
			if len(value) > 128 || strings.ContainsAny(value, "@:/\\\x00\r\n") {
				return "", camera.ErrForbidden
			}
		}
	}
	return ip.String(), nil
}
