package cameraworker

import (
	"bytes"
	"context"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/tlsidentity"
	"google.golang.org/protobuf/proto"
)

const transportProbeTimeout = 20 * time.Second

// transportGrantScope admits a CP transport grant only for the host this scan
// observed, in today's assignment, network and device, and within its short
// lifetime. The CP repeats every check after the handshake.
func transportGrantScope(v *api.CameraWorkerAssignment, worker []byte, request *api.CameraPolicyObserveRequest, observed *api.CameraPolicyObserveResponse, now time.Time) error {
	g, d := observed.GetTransportGrant(), observed.GetDevice()
	if g == nil || d == nil || d.GetDateErased() != nil {
		return camera.ErrForbidden
	}
	var candidate *api.CameraTransportCandidate
	for _, c := range d.GetTransport().GetCandidates() {
		if c.GetId() == g.GetCandidateId() {
			candidate = c
		}
	}
	if candidate == nil || candidate.GetState() != "pending" || !proto.Equal(candidate.GetGrant(), g) {
		return camera.ErrForbidden
	}
	if _, err := discoveryScope(v, worker, time.Second); err != nil {
		return err
	}
	a, n := v.GetAssignment(), v.GetNetwork()
	if !bytes.Equal(g.GetAssignmentId(), a.GetId()) || g.GetAssignmentGeneration() != a.GetGeneration() || !bytes.Equal(request.GetAssignment().GetId(), a.GetId()) || request.GetAssignmentGeneration() != a.GetGeneration() {
		return camera.ErrForbidden
	}
	if !bytes.Equal(g.GetDeviceId(), d.GetId()) || !bytes.Equal(g.GetNetworkId(), n.GetId()) || !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) || !bytes.Equal(d.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(d.GetSite().GetId(), a.GetSite().GetId()) || !proto.Equal(n.GetDateUpdated(), g.GetNetworkVersion()) || g.GetObservationRevision() != d.GetObservationRevision() {
		return camera.ErrForbidden
	}
	if g.GetDateIssued() == nil || g.GetDateExpires() == nil || g.GetDateIssued().CheckValid() != nil || g.GetDateExpires().CheckValid() != nil || !now.Before(g.GetDateExpires().AsTime()) || g.GetDateIssued().AsTime().After(now.Add(time.Second)) || g.GetDateExpires().AsTime().Sub(g.GetDateIssued().AsTime()) > 2*time.Minute {
		return camera.ErrForbidden
	}
	if pin := g.GetTrust(); pin == nil || pin.GetDeviceId() != cameraID(d.GetId()) || (camera.DeviceTLSIdentity{DeviceID: pin.GetDeviceId(), SPKISHA256: pin.GetSpkiSha256()}).Validate() != nil {
		return camera.ErrForbidden
	}
	// The handshake goes only to the one host this scan saw for the device,
	// inside the assigned prefixes, on an explicit HTTPS port.
	host := ""
	for _, ep := range request.GetEndpoints() {
		h, err := discoveryEndpoint(ep.GetUri(), n.GetCidrs())
		if err != nil || host != "" && h != host {
			return camera.ErrForbidden
		}
		host = h
	}
	u, err := url.Parse(g.GetEndpoint())
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Port() == "" {
		return camera.ErrForbidden
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return camera.ErrForbidden
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || host == "" || ip.Unmap().String() != host {
		return camera.ErrForbidden
	}
	if _, err := discoveryEndpoint(g.GetEndpoint(), n.GetCidrs()); err != nil {
		return camera.ErrForbidden
	}
	return nil
}

// probeTransport is one credential-free TLS handshake with the candidate: no
// HTTP request, credential, cookie or caller TLS configuration. The key it
// returns is an observation for the CP to compare, never trust by itself.
func probeTransport(ctx context.Context, g *api.CameraTransportGrant) (string, error) {
	return tlsidentity.Observe(ctx, g.GetEndpoint())
}

// verifyTransport performs a CP transport grant: a fresh assignment, one
// handshake bounded by the grant's lifetime, and the presented key to the CP.
// Old CPs send no grant; then nothing is done and the operator path remains.
func (w *Worker) verifyTransport(ctx context.Context, workers api.CameraWorkerServiceClient, policies api.CameraPolicyServiceClient, request *api.CameraPolicyObserveRequest, observed *api.CameraPolicyObserveResponse, probe func(context.Context, *api.CameraTransportGrant) (string, error)) error {
	g := observed.GetTransportGrant()
	if g == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, transportProbeTimeout)
	defer cancel()
	fresh, err := discoveryPoll(ctx, workers)
	if err != nil {
		return err
	}
	var selected *api.CameraWorkerAssignment
	for _, a := range fresh.GetAssignments() {
		if bytes.Equal(a.GetAssignment().GetId(), g.GetAssignmentId()) {
			selected = a
			break
		}
	}
	if selected == nil || transportGrantScope(selected, w.ID().Bytes(), request, observed, time.Now()) != nil {
		return camera.ErrForbidden
	}
	ctx, expire := context.WithDeadline(ctx, g.GetDateExpires().AsTime())
	defer expire()
	key, err := probe(ctx, proto.Clone(g).(*api.CameraTransportGrant))
	if err != nil {
		// Unreachable or not TLS: the candidate stays pending and expires.
		return err
	}
	call, done := context.WithTimeout(ctx, discoveryRPCTimeout)
	defer done()
	out, err := policies.VerifyTransport(call, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: key}.Build())
	if err != nil {
		return err
	}
	// The CP's outcome is a fixed code; it carries no device text.
	w.cfg.Log.Info("camera worker transport candidate decided", "code", out.GetCode())
	return nil
}
