package core

import (
	"bytes"
	"context"
	"regexp"

	"github.com/lesomnus/shale/api"
	"google.golang.org/protobuf/proto"
)

var cameraSPKISHA256 = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Syntax never grants trust: only a policy approved for this exact device may
// supply the identity. An absent identity retains ordinary PKI verification.
func cameraTLSIdentity(v *api.CameraDeviceTLSIdentity) error {
	if v == nil {
		return nil
	}
	if _, err := cameraPresetID(v.GetDeviceId(), DomCameraDevice); err != nil {
		return invalid("device_tls_identity.device_id", "an exact canonical camera device ID is required")
	}
	if !cameraSPKISHA256.MatchString(v.GetSpkiSha256()) {
		return invalid("device_tls_identity.spki_sha256", "an explicit canonical SHA256 SPKI digest is required")
	}
	return nil
}
func cameraTLSBootstrap(v *api.CameraBootstrapProfile) error {
	if v == nil || v.GetWebScheme() != "https" {
		return invalid("device_tls_identity", "an explicit HTTPS bootstrap destination is required")
	}
	return nil
}
func (s Core) cameraPolicyTLSIdentities(ctx context.Context, n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) error {
	identities := p.GetDeviceTlsIdentities()
	if len(identities) == 0 {
		return nil
	}
	if len(identities) > 128 {
		return invalid("device_tls_identities", "too many device TLS identities")
	}
	if err := cameraTLSBootstrap(p.GetBootstrap()); err != nil {
		return err
	}
	if n == nil {
		return invalid("device_tls_identities", "an exact camera network is required")
	}
	seen := map[string]bool{}
	for _, v := range identities {
		if v == nil {
			return invalid("device_tls_identities", "empty device TLS identity")
		}
		if err := cameraTLSIdentity(v); err != nil {
			return err
		}
		if seen[v.GetDeviceId()] {
			return invalid("device_tls_identities", "a device may have only one approved TLS identity per revision")
		}
		seen[v.GetDeviceId()] = true
		id, _ := cameraPresetID(v.GetDeviceId(), DomCameraDevice)
		d, err := s.Next().CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(d.GetId(), id) || d.GetDateErased() != nil || !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) {
			return denied("device TLS identity", "device belongs to another camera network")
		}
		if err := cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
			return err
		}
	}
	return nil
}

// A job never receives the policy's other devices' identities or a mutable
// pointer to its approved pin. Device IDs remain stable through DHCP changes.
func cameraSelectedTLSIdentity(p *api.CameraPolicy, device []byte) *api.CameraDeviceTLSIdentity {
	if len(device) == 0 {
		return nil
	}
	id := mustId(device).String()
	for _, v := range p.GetEnrollment().GetDeviceTlsIdentities() {
		if v.GetDeviceId() == id {
			return proto.Clone(v).(*api.CameraDeviceTLSIdentity)
		}
	}
	return nil
}

func cameraDeviceTLSIdentity(p *api.CameraPolicy, d *api.CameraDevice) *api.CameraDeviceTLSIdentity {
	if pin := cameraSelectedTLSIdentity(p, d.GetId()); pin != nil {
		return pin
	}
	state := d.GetFactoryTls()
	if state.GetDateTrusted() != nil && state.GetIdentity() != nil && state.GetIdentity().GetDeviceId() == mustId(d.GetId()).String() && bytes.Equal(state.GetPlan().GetDeviceId(), d.GetId()) {
		return proto.Clone(state.GetIdentity()).(*api.CameraDeviceTLSIdentity)
	}
	return nil
}
func cameraExactTLSIdentity(p *api.CameraPolicy, a *api.CameraAssignment, d *api.CameraDevice, input *api.CameraJobInputs) error {
	if p == nil {
		if input.GetDeviceTlsIdentity() != nil {
			return denied("device TLS identity", "a pin requires an operator-approved policy revision")
		}
		return nil
	}
	if d == nil || d.GetDateErased() != nil || !bytes.Equal(d.GetNetwork().GetId(), a.GetNetwork().GetId()) {
		return denied("device TLS identity", "job device belongs to another camera network")
	}
	if err := cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
		return err
	}
	want := cameraDeviceTLSIdentity(p, d)
	if p.GetEnrollment().GetTrustNewFactoryTls() && want == nil {
		return denied("device TLS identity", "first factory TLS trust must commit before a job is created")
	}
	if !proto.Equal(input.GetDeviceTlsIdentity(), want) {
		return denied("device TLS identity", "job must retain its device's exact policy-approved TLS identity")
	}
	return nil
}
