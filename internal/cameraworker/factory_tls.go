package cameraworker

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/tlsidentity"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"google.golang.org/protobuf/proto"
)

const factoryTLSProbeTimeout = 20 * time.Second

func factoryTLSPlanScope(v *api.CameraWorkerAssignment, worker []byte, observed *api.CameraPolicyObserveResponse, now time.Time) error {
	p, d := observed.GetFactoryTlsPlan(), observed.GetDevice()
	if p == nil || d == nil || d.GetDateErased() != nil || d.GetIdentity().GetAuthenticated() || d.GetFactoryTls().GetIdentity() != nil || !proto.Equal(d.GetFactoryTls().GetPlan(), p) {
		return camera.ErrForbidden
	}
	if _, err := discoveryScope(v, worker, time.Second); err != nil {
		return err
	}
	a, n := v.GetAssignment(), v.GetNetwork()
	if !bytes.Equal(p.GetAssignmentId(), a.GetId()) || p.GetAssignmentGeneration() != a.GetGeneration() || !bytes.Equal(p.GetDeviceId(), d.GetId()) || p.GetObservationRevision() != d.GetObservationRevision() || !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) || !bytes.Equal(d.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(d.GetSite().GetId(), a.GetSite().GetId()) || !proto.Equal(n.GetDateUpdated(), p.GetNetworkVersion()) || cameraID(n.GetActivePolicyId()) != p.GetPolicy().GetId() || p.GetPolicy().GetRevision() == "" || p.GetOperationKey() == "" {
		return camera.ErrForbidden
	}
	if p.GetDateIssued() == nil || p.GetDateExpires() == nil || p.GetDateIssued().CheckValid() != nil || p.GetDateExpires().CheckValid() != nil || !now.Before(p.GetDateExpires().AsTime()) || p.GetDateIssued().AsTime().After(now) || p.GetDateExpires().AsTime().Sub(p.GetDateIssued().AsTime()) > 2*time.Minute {
		return camera.ErrForbidden
	}
	b := p.GetBootstrap()
	if b.GetWebScheme() != "https" || b.GetWebPort() != 443 {
		return camera.ErrForbidden
	}
	switch b.GetCompatibilityProfile() {
	case tplink.ProfileC420Known, tplink.ProfileC420V120, tplink.ProfileC420V122:
	default:
		return camera.ErrUnsupported
	}
	if len(d.GetEndpoints()) == 0 {
		return camera.ErrForbidden
	}
	var host string
	for _, e := range d.GetEndpoints() {
		h, err := discoveryEndpoint(e.GetUri(), n.GetCidrs())
		if err != nil || host != "" && h != host {
			return camera.ErrForbidden
		}
		host = h
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsPrivate() || ip.Zone() != "" || p.GetEndpoint() != "https://"+net.JoinHostPort(ip.String(), "443") {
		return camera.ErrForbidden
	}
	return nil
}

// Only public TLS and the known anonymous factory challenge are used here.
// No AuthProvider, credential lease, caller headers or cookie jar are accepted.
// The second connection must prove the same key before HTTP can be sent.
func probeFactoryTLS(ctx context.Context, p *api.CameraFactoryTLSPlan) (string, error) {
	pin, err := tlsidentity.Observe(ctx, p.GetEndpoint())
	if err != nil {
		return "", err
	}
	tlsConfig, err := tlsidentity.NewTLSConfig(pin)
	if err != nil {
		return "", camera.ErrInvalid
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	driver := &tplink.Driver{HTTPClient: client, WebEndpoint: p.GetEndpoint(), CompatibilityProfile: p.GetBootstrap().GetCompatibilityProfile()}
	u, err := url.Parse(p.GetEndpoint())
	if err != nil || u.User != nil {
		return "", camera.ErrForbidden
	}
	activation, err := driver.Activation(ctx, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: p.GetEndpoint()}}})
	if err != nil {
		return "", err
	}
	if activation != camera.ActivationRequired {
		return "", camera.ErrManualActivation
	}
	return pin, nil
}

func (w *Worker) completeFactoryTLS(ctx context.Context, workers api.CameraWorkerServiceClient, policies api.CameraPolicyServiceClient, observed *api.CameraPolicyObserveResponse, probe func(context.Context, *api.CameraFactoryTLSPlan) (string, error)) error {
	p := observed.GetFactoryTlsPlan()
	if p == nil {
		return nil
	}
	// A probe has its own bounded lifetime, independent of the completed short
	// multicast scan. The CP repeats these checks after all LAN I/O.
	ctx, cancel := context.WithTimeout(ctx, factoryTLSProbeTimeout)
	defer cancel()
	fresh, err := discoveryPoll(ctx, workers)
	if err != nil {
		return err
	}
	var selected *api.CameraWorkerAssignment
	for _, a := range fresh.GetAssignments() {
		if bytes.Equal(a.GetAssignment().GetId(), p.GetAssignmentId()) {
			selected = a
			break
		}
	}
	if selected == nil || factoryTLSPlanScope(selected, w.ID().Bytes(), observed, time.Now()) != nil {
		return camera.ErrForbidden
	}
	ctx, expire := context.WithDeadline(ctx, p.GetDateExpires().AsTime())
	defer expire()
	pin, err := probe(ctx, proto.Clone(p).(*api.CameraFactoryTLSPlan))
	if err != nil {
		return err
	}
	_, err = policies.TrustFactoryTLS(ctx, api.CameraPolicyTrustFactoryTLSRequest_builder{Plan: p, Identity: api.CameraDeviceTLSIdentity_builder{DeviceId: cameraID(p.GetDeviceId()), SpkiSha256: pin}.Build(), ActivationRequired: true}.Build())
	return err
}
