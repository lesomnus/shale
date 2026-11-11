package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/netip"
	"net/url"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const cameraFactoryTLSLifetime = 2 * time.Minute

func cameraFactoryTLSPolicy(n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) error {
	if !p.GetTrustNewFactoryTls() {
		return nil
	}
	b := p.GetBootstrap()
	if !p.GetAutomatic() || !p.GetAllowActivation() || p.GetMaxDevices() < 1 || b.GetWebScheme() != "https" || b.GetWebPort() != 443 {
		return invalid("trust_new_factory_tls", "automatic activation with an explicit HTTPS port 443 bootstrap is required")
	}
	switch b.GetCompatibilityProfile() {
	case tplink.ProfileC420Known, tplink.ProfileC420V120, tplink.ProfileC420V122:
	default:
		return invalid("trust_new_factory_tls", "a supported factory activation protocol is required")
	}
	if n == nil || n.GetInterfaceHint() == "" || len(n.GetCidrs()) == 0 {
		return invalid("trust_new_factory_tls", "an explicit isolated private camera interface and network are required")
	}
	for _, raw := range n.GetCidrs() {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p != p.Masked() || !cameraFactoryPrivatePrefix(p) {
			return invalid("trust_new_factory_tls", "factory trust is restricted to explicit private camera networks")
		}
	}
	return nil
}

func cameraFactoryPrivatePrefix(p netip.Prefix) bool {
	for _, raw := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"} {
		private := netip.MustParsePrefix(raw)
		if p.Bits() >= private.Bits() && private.Contains(p.Addr()) {
			return true
		}
	}
	return false
}

// A discovery endpoint is a candidate, never proof of identity. The returned
// origin is scoped by both the CP network and the current inventory snapshot.
func cameraFactoryTLSOrigin(n *api.CameraNetwork, d *api.CameraDevice) (string, error) {
	if len(d.GetEndpoints()) == 0 || cameraEndpoints(d.GetEndpoints(), n.GetCidrs()) != nil {
		return "", denied("factory TLS", "a current scoped camera endpoint is required")
	}
	var host string
	for _, e := range d.GetEndpoints() {
		u, err := url.Parse(e.GetUri())
		if err != nil {
			return "", denied("factory TLS", "a current scoped camera endpoint is required")
		}
		ip, err := netip.ParseAddr(u.Hostname())
		if err != nil || !ip.IsPrivate() || ip.Zone() != "" || host != "" && host != ip.String() {
			return "", denied("factory TLS", "one exact private camera address is required")
		}
		host = ip.String()
	}
	return "https://" + net.JoinHostPort(host, "443"), nil
}

// Erased bindings or revoked credentials are still evidence of prior ownership.
// A reset, replacement or imported initialized account requires operator trust.
func (s Core) cameraFactoryTLSFresh(ctx context.Context, d *api.CameraDevice) error {
	if d.GetDateErased() != nil || d.GetIdentity().GetAuthenticated() {
		return denied("factory TLS", "an existing camera identity cannot acquire first-use trust")
	}
	id := mustId(d.GetId()).Uuid()
	bound, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.DeviceIdEQ(id)).Exist(ctx)
	if err != nil {
		return err
	}
	secret, err := s.ent(ctx).CameraCredential.Query().Where(cameracredential.DeviceIdEQ(id)).Exist(ctx)
	if err != nil {
		return err
	}
	if bound || secret {
		return denied("factory TLS", "an existing camera account or binding cannot acquire first-use trust")
	}
	return nil
}

// Operator observations are independent inventory writes. Under the same
// Network lock, recheck every canonical host owner after the public LAN probe;
// a plan's own Device revision cannot detect a different Device taking its IP.
func (s Core) cameraFactoryTLSOwnership(ctx context.Context, n *api.CameraNetwork, d *api.CameraDevice) error {
	mac := ""
	if len(d.GetIdentity().GetMacs()) == 1 {
		mac = cameraObservationMAC(d.GetIdentity().GetMacs()[0])
	}
	r := api.CameraPolicyObserveRequest_builder{DiscoveryId: d.GetEnrollment().GetDiscoveryId(), Identity: d.GetIdentity(), Endpoints: d.GetEndpoints()}.Build()
	rows, err := s.cameraObservationInventory(ctx, n, r, []string{d.GetDeviceKey()}, mac)
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0].Id != mustId(d.GetId()).Uuid() {
		return status.Error(codes.Aborted, "factory camera endpoint ownership changed")
	}
	return nil
}

func (s Core) cameraPlanFactoryTLS(ctx context.Context, a *api.CameraAssignment, n *api.CameraNetwork, d *api.CameraDevice, p *api.CameraPolicy) error {
	if err := cameraFactoryTLSPolicy(n, p.GetEnrollment()); err != nil {
		return err
	}
	if err := s.cameraLockDevice(ctx, d.GetId()); err != nil {
		return err
	}
	if err := s.cameraFactoryTLSFresh(ctx, d); err != nil {
		return err
	}
	if err := s.cameraFactoryTLSOwnership(ctx, n, d); err != nil {
		return err
	}
	origin, err := cameraFactoryTLSOrigin(n, d)
	if err != nil {
		return err
	}
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return status.Error(codes.Unavailable, "factory trust plan is unavailable")
	}
	now := s.d.now()
	plan := api.CameraFactoryTLSPlan_builder{
		AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), DeviceId: d.GetId(), ObservationRevision: d.GetObservationRevision(),
		Policy:    api.CameraRevisionReference_builder{Id: mustId(p.GetId()).String(), Revision: p.GetRevision()}.Build(),
		Bootstrap: proto.Clone(p.GetEnrollment().GetBootstrap()).(*api.CameraBootstrapProfile), Endpoint: origin,
		DateIssued: timestamppb.New(now), DateExpires: timestamppb.New(now.Add(cameraFactoryTLSLifetime)), NetworkVersion: n.GetDateUpdated(), OperationKey: hex.EncodeToString(key),
	}.Build()
	_, err = s.Next().CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), FactoryTls: api.CameraFactoryTLSState_builder{Plan: plan}.Build(), DateUpdated: d.GetDateUpdated()}.Build())
	return err
}

func (s coreCameraPolicy) TrustFactoryTLS(ctx context.Context, r *api.CameraPolicyTrustFactoryTLSRequest) (*api.CameraPolicyObserveResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	plan := r.GetPlan()
	if plan == nil || !r.GetActivationRequired() || r.GetIdentity() == nil || cameraTLSIdentity(r.GetIdentity()) != nil {
		return nil, invalid("factory TLS", "an exact plan and credential-free factory activation evidence are required")
	}
	deviceID, err := cameraPresetID(r.GetIdentity().GetDeviceId(), DomCameraDevice)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(plan.GetDeviceId(), deviceID) {
		return nil, invalid("factory TLS", "the plan and identity must name the same device")
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraPolicyObserveResponse
	err = s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, n, err := c.cameraObservationAssignment(ctx, api.CameraAssignmentRef_builder{Id: plan.GetAssignmentId()}.Build(), plan.GetAssignmentGeneration())
		if err != nil {
			return err
		}
		if err := c.cameraLockNetwork(ctx, n.GetId()); err != nil {
			return err
		}
		n, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := c.cameraLockDevice(ctx, plan.GetDeviceId()); err != nil {
			return err
		}
		d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: plan.GetDeviceId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) || cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()) != nil || r.GetIdentity().GetDeviceId() != mustId(d.GetId()).String() {
			return denied("factory TLS", "device belongs to another assigned scope")
		}
		stored := d.GetFactoryTls()
		if !proto.Equal(stored.GetPlan(), plan) {
			return status.Error(codes.Aborted, "factory trust plan changed")
		}
		p, err := nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: n.GetActivePolicyId()}.Build()}.Build())
		if err != nil {
			return err
		}
		origin, err := cameraFactoryTLSOrigin(n, d)
		if err != nil {
			return err
		}
		if err := c.cameraFactoryTLSOwnership(ctx, n, d); err != nil {
			return err
		}
		if !p.GetEnrollment().GetTrustNewFactoryTls() || cameraFactoryTLSPolicy(n, p.GetEnrollment()) != nil || plan.GetPolicy().GetId() != mustId(p.GetId()).String() || plan.GetPolicy().GetRevision() != p.GetRevision() || !proto.Equal(plan.GetBootstrap(), p.GetEnrollment().GetBootstrap()) || !proto.Equal(plan.GetNetworkVersion(), n.GetDateUpdated()) || plan.GetEndpoint() != origin {
			return status.Error(codes.Aborted, "factory trust authority changed")
		}
		if stored.GetIdentity() != nil {
			// A lost success response can be replayed after another observation or
			// expiry. It cannot change the accepted key or today's authority.
			if !proto.Equal(stored.GetIdentity(), r.GetIdentity()) || stored.GetDateTrusted() == nil {
				return denied("factory TLS", "the accepted device TLS identity cannot be replaced")
			}
		} else {
			if d.GetObservationRevision() != plan.GetObservationRevision() || plan.GetDateIssued() == nil || plan.GetDateExpires() == nil || plan.GetDateIssued().CheckValid() != nil || plan.GetDateExpires().CheckValid() != nil || !s.d.now().Before(plan.GetDateExpires().AsTime()) || plan.GetDateIssued().AsTime().After(s.d.now()) {
				return status.Error(codes.Aborted, "factory trust observation expired or changed")
			}
			if err := c.cameraFactoryTLSFresh(ctx, d); err != nil {
				return err
			}
			state := proto.Clone(stored).(*api.CameraFactoryTLSState)
			state.SetIdentity(proto.Clone(r.GetIdentity()).(*api.CameraDeviceTLSIdentity))
			state.SetDateTrusted(timestamppb.New(s.d.now()))
			_, err = nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), FactoryTls: state, DateUpdated: d.GetDateUpdated()}.Build())
			if err != nil {
				return err
			}
			d, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
			if err != nil {
				return err
			}
		}
		b, job, code, err := c.cameraEnrollObservation(ctx, a, n, d)
		if err != nil {
			return err
		}
		_, err = nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Enrollment: api.CameraEnrollmentObservation_builder{Code: code, ObservedAt: timestamppb.New(s.d.now()), DiscoveryId: d.GetEnrollment().GetDiscoveryId()}.Build(), DateUpdated: d.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		d, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		out = api.CameraPolicyObserveResponse_builder{Device: d, Binding: b, Job: job, Code: code}.Build()
		return nil
	})
	return out, cameraConflict(err)
}
