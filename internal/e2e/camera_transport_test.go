package e2e_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// transportVerified is what a candidate presenting its device's approved key
// comes to.
const transportVerified = "transport_key_verified"

const (
	transportEPR    = "urn:uuid:7d6c4b9e-5a3f-4c1e-9b2d-0e8f6a4c2d10"
	transportMAC    = "02:00:00:00:22:23"
	transportSerial = "transport-physical-unit"
)

type transportOptions struct {
	pin    bool   // a per-device SPKI approval; otherwise ordinary PKI
	scheme string // the bootstrap web scheme
	skip   bool   // tls_verification: skip
}

// transportFixture is a recording camera's CP state: an authenticated
// identity, a published binding and a settled job pinned under an active
// policy, and a real fake camera at 127.0.0.2 presenting its web key.
type transportFixture struct {
	server              *cmd.Server
	cp                  string
	admin, workerConn   *grpc.ClientConn
	agent               *hostagent.Agent
	ctx                 context.Context
	worker              *api.CameraWorker
	network             *api.CameraNetwork
	assignment          *api.CameraAssignment
	device              *api.CameraDevice
	binding             *api.CameraBinding
	job                 *api.CameraJob
	policy              *api.CameraPolicy
	preset              *api.CameraPreset
	key                 cameratest.WebKey
	port                int
	camera              *cameratest.ONVIFCamera
	request             *api.CameraPolicyObserveRequest
	devices             api.CameraDeviceServiceClient
	policies, machine   api.CameraPolicyServiceClient
	sourceID, bindingID []byte
}

func newTransportFixture(t *testing.T, o transportOptions) *transportFixture {
	t.Helper()
	f := &transportFixture{}
	f.server, f.cp, f.admin, f.ctx = cameraPolicyControl(t)
	var err error
	f.worker, f.workerConn, f.agent = cameraWorkerMachine(t, f.ctx, f.server, f.cp, f.admin, "transport-worker", nil)
	producer, _, _ := cameraWorkerProducer(t, f.ctx, f.server, f.cp, f.admin, "transport-producer", nil)
	f.network, err = api.NewCameraNetworkServiceClient(f.admin).Configure(f.ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: ptr("acme")}.Build(), Alias: "transport-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo"}.Build())
	require.NoError(t, err)
	nref := api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build()
	f.assignment, err = api.NewCameraAssignmentServiceClient(f.admin).Assign(f.ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "transport", WorkerId: f.worker.GetId()}.Build())
	require.NoError(t, err)

	f.key = cameratest.NewWebKey(t)
	f.camera = cameratest.ServeONVIFWithOptions(t, cameramodel.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10}, cameratest.ONVIFOptions{ListenAddress: "127.0.0.2:0", Identity: cameratest.CameraIdentity{Serial: transportSerial, MAC: transportMAC, DiscoveryID: transportEPR}})
	web, err := url.Parse(f.camera.ServeWebTLS(t, 0, f.key))
	require.NoError(t, err)
	f.port, err = strconv.Atoi(web.Port())
	require.NoError(t, err)

	f.devices = api.NewCameraDeviceServiceClient(f.admin)
	digest := sha256.Sum256([]byte(transportEPR))
	eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: f.camera.CurrentURL()}.Build()}
	d, err := f.devices.Claim(f.ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: "discovery/" + hex.EncodeToString(digest[:]), Endpoints: eps}.Build())
	require.NoError(t, err)
	f.device, err = f.devices.Observe(f.ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), ExpectedVersion: d.GetDateUpdated(), Identity: api.CameraIdentityEvidence_builder{Manufacturer: "TP-Link", Model: "EasyCam C420", Serial: transportSerial, Macs: []string{transportMAC}, Authenticated: true}.Build(), Endpoints: eps}.Build())
	require.NoError(t, err)

	f.preset, err = api.NewCameraPresetServiceClient(f.admin).Create(f.ctx, cameraPolicyPresetRequest("transport-preset", "1", cameraPolicyRule("base", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, f.network.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	scheme := o.scheme
	if scheme == "" {
		scheme = "https"
	}
	enrollment := api.CameraEnrollmentPolicy_builder{Automatic: true, AllowAny: true, MaxDevices: 4, Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-first-password-v1", WebScheme: scheme, WebPort: int32(f.port)}.Build()}.Build()
	if o.pin {
		enrollment.SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{f.pin(t, f.key.SPKI)})
	}
	if o.skip {
		enrollment.SetTlsVerification(cameramodel.TLSVerificationSkip)
	}
	request := cameraPolicyCreateRequest(f.network, f.preset, "transport-policy", "1")
	request.SetEnrollment(enrollment)
	f.policies = api.NewCameraPolicyServiceClient(f.admin)
	f.policy, err = f.policies.Create(f.ctx, request)
	require.NoError(t, err)
	f.network, err = f.policies.Activate(f.ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: f.policy.GetId()}.Build(), ExpectedNetworkVersion: f.network.GetDateUpdated()}.Build())
	require.NoError(t, err)

	f.binding, err = api.NewCameraBindingServiceClient(f.admin).Bind(f.ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: f.device.GetId()}.Build(), LogicalKey: "transport-camera", SourceAlias: "transport-camera"}.Build())
	require.NoError(t, err)
	credential := func(purpose string) *api.CameraCredentialReference {
		return api.CameraCredentialReference_builder{Provider: "cp", Id: "transport-" + purpose, Version: "1", Purpose: purpose}.Build()
	}
	inputs := api.CameraJobInputs_builder{Policy: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, f.policy.GetId()), Revision: f.policy.GetRevision()}.Build(), Bootstrap: proto.Clone(enrollment.GetBootstrap()).(*api.CameraBootstrapProfile), ManagementCredential: credential("management"), StreamingCredential: credential("streaming"), TlsVerification: enrollment.GetTlsVerification()}.Build()
	if o.pin {
		inputs.SetDeviceTlsIdentity(f.pin(t, f.key.SPKI))
	}
	f.job, err = api.NewCameraJobServiceClient(f.admin).Create(f.ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: f.binding.GetId()}.Build(), RequestKey: "transport-enrollment", Inputs: inputs}.Build())
	require.NoError(t, err)
	// The job's run to ACTIVE and its publication are other tests'. Here the
	// camera records: its last job settled and its binding has an input.
	f.settle(t, f.job)
	_, err = f.server.Ent.CameraBinding.Update().Where(camerabinding.IdEQ(uuidOf(t, f.binding.GetId()))).SetDesired(api.CameraCaptureSpec_builder{BindingId: f.binding.GetId(), SourceId: f.binding.GetSource().GetId(), Revision: 1, Input: "rtsp://127.0.0.2:8554/stream"}.Build()).Save(f.ctx)
	require.NoError(t, err)
	f.binding = f.getBinding(t)
	f.machine = api.NewCameraPolicyServiceClient(f.workerConn)
	f.request = api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build(), AssignmentGeneration: f.assignment.GetGeneration(), DiscoveryId: transportEPR, Endpoints: eps}.Build()
	// Discovery has seen the camera where it is: its EPR is known.
	require.Equal(t, "already_enrolled", f.observe(t, f.request).GetCode())
	f.device = f.get(t)
	require.Equal(t, transportEPR, f.device.GetEnrollment().GetDiscoveryId())
	return f
}

func ptr[T any](v T) *T { return &v }

func uuidOf(t *testing.T, raw []byte) [16]byte {
	t.Helper()
	require.Len(t, raw, 16)
	return [16]byte(raw)
}

func (f *transportFixture) pin(t *testing.T, spki string) *api.CameraDeviceTLSIdentity {
	t.Helper()
	return api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, f.device.GetId()), SpkiSha256: spki}.Build()
}

func (f *transportFixture) settle(t *testing.T, j *api.CameraJob) {
	t.Helper()
	_, err := f.server.Ent.CameraJob.Update().Where(camerajob.IdEQ(uuidOf(t, j.GetId()))).SetStage(int32(api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE)).Save(f.ctx)
	require.NoError(t, err)
}

func (f *transportFixture) get(t *testing.T) *api.CameraDevice {
	t.Helper()
	d, err := f.devices.Get(f.ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: f.device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	return d
}

func (f *transportFixture) getBinding(t *testing.T) *api.CameraBinding {
	t.Helper()
	b, err := api.NewCameraBindingServiceClient(f.admin).Get(f.ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: f.binding.GetId()}.Build()}.Build())
	require.NoError(t, err)
	return b
}

// moved is the fixture's observation at another host.
func (f *transportFixture) moved(host string) *api.CameraPolicyObserveRequest {
	r := proto.Clone(f.request).(*api.CameraPolicyObserveRequest)
	r.SetEndpoints([]*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://" + host + ":2020/onvif/device_service"}.Build()})
	return r
}

func (f *transportFixture) observe(t *testing.T, r *api.CameraPolicyObserveRequest) *api.CameraPolicyObserveResponse {
	t.Helper()
	v, err := f.machine.Observe(f.ctx, r)
	require.NoError(t, err)
	return v
}

func (f *transportFixture) verify(t *testing.T, g *api.CameraTransportGrant, spki string) *api.CameraPolicyObserveResponse {
	t.Helper()
	v, err := f.machine.VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: spki}.Build())
	require.NoError(t, err)
	return v
}

// unchanged asserts the device still has its authorized endpoints, identity
// and endpoint revision, and that no job, binding or Source was added.
func (f *transportFixture) unchanged(t *testing.T) {
	t.Helper()
	d := f.get(t)
	require.True(t, proto.Equal(f.device.GetEndpoints()[0], d.GetEndpoints()[0]), "authorized endpoints are unchanged")
	require.Len(t, d.GetEndpoints(), len(f.device.GetEndpoints()))
	require.True(t, proto.Equal(f.device.GetIdentity(), d.GetIdentity()), "identity is unchanged")
	require.Equal(t, f.device.GetObservationRevision(), d.GetObservationRevision())
	require.Equal(t, f.device.GetEnrollment().GetDiscoveryId(), d.GetEnrollment().GetDiscoveryId())
	jobs, err := f.server.Ent.CameraJob.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, jobs, "no recovery job")
	require.Equal(t, 1, mustCameraBindingCount(t, f.ctx, f.server))
	sources, err := f.server.Ent.Source.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, sources)
	b := f.getBinding(t)
	require.True(t, proto.Equal(f.binding.GetDesired(), b.GetDesired()), "the binding's input is unchanged")
}

// transportDiscoverer reports whatever cameras the test says are on the LAN.
type transportDiscoverer struct {
	mu      sync.Mutex
	cameras []*cameratest.ONVIFCamera
}

func (d *transportDiscoverer) set(cameras ...*cameratest.ONVIFCamera) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cameras = cameras
}

func (d *transportDiscoverer) Discover(_ context.Context, scope cameramodel.DiscoveryScope) ([]cameramodel.Observation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []cameramodel.Observation
	for _, c := range d.cameras {
		h := c.Hint("")
		out = append(out, cameramodel.Observation{Scope: scope.ID, ObservedAt: time.Now(), Device: cameramodel.DeviceHint{CandidateID: c.CurrentURL(), DiscoveryID: h.DiscoveryID, Endpoints: []cameramodel.Endpoint{{URL: c.CurrentURL(), NetworkScope: scope.ID}}}})
	}
	return out, nil
}

// run starts the real Worker runtime with this discoverer. It executes no job.
func (f *transportFixture) run(t *testing.T, discoverer cameramodel.Discoverer) {
	t.Helper()
	runtime, err := cameraworker.New(cameraworker.Config{StateDir: f.agent.Store.Dir, Cp: f.cp, Tenant: "acme", HardwareID: "transport-worker", CaHash: pki.Fingerprint(f.server.CA.Cert), HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 30, DiscoveryInterval: time.Second, DiscoveryTimeout: 500 * time.Millisecond, Discoverer: discoverer, Log: f.agent.Log, Executor: cameraworker.ExecutorFunc(func(context.Context, *cameraworker.Execution) error {
		t.Error("a staged transport candidate created a job")
		return nil
	})})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Worker did not stop")
		}
	})
}

// candidate waits for the device's candidate at host to be decided.
func (f *transportFixture) decided(t *testing.T, host string) *api.CameraTransportCandidate {
	t.Helper()
	var out *api.CameraTransportCandidate
	require.Eventually(t, func() bool {
		for _, c := range f.get(t).GetTransport().GetCandidates() {
			u, err := url.Parse(c.GetEndpoints()[0].GetUri())
			if err == nil && u.Hostname() == host && c.GetState() != "pending" {
				out = c
				return true
			}
		}
		return false
	}, 15*time.Second, 50*time.Millisecond)
	return out
}

// The same camera at an unowned new address presents its approved key in a
// credential-free handshake by the real Worker. Nothing else reaches it.
func TestCameraTransportCandidateVerifiesTheApprovedKeyCredentialFree(t *testing.T) {
	f := newTransportFixture(t, transportOptions{pin: true})
	require.NoError(t, f.camera.MoveTo("127.0.0.3"))
	discoverer := &transportDiscoverer{}
	discoverer.set(f.camera)
	f.run(t, discoverer)
	c := f.decided(t, "127.0.0.3")
	require.Equal(t, "verified", c.GetState())
	require.Equal(t, "key_verified", c.GetCode())
	require.Equal(t, f.key.SPKI, c.GetPresentedSpkiSha256())
	require.True(t, proto.Equal(f.pin(t, f.key.SPKI), c.GetGrant().GetTrust()))
	require.Equal(t, "https://127.0.0.3:"+strconv.Itoa(f.port), c.GetGrant().GetEndpoint())
	require.Equal(t, f.job.GetId(), c.GetGrant().GetJobId())
	require.Positive(t, f.camera.WebHandshakes())
	require.Empty(t, f.camera.AuthAttempts(), "verification presents no credential")
	f.unchanged(t)
	require.Equal(t, "transport_key_verified", f.get(t).GetEnrollment().GetCode())
}

// A device copying the camera's EPR and MAC at a new address, with another
// key from the same CA, receives one TLS handshake and nothing else.
func TestCameraTransportCandidateImpostorReceivesNoCredential(t *testing.T) {
	f := newTransportFixture(t, transportOptions{pin: true})
	impostor := cameratest.ServeCamera(t, cameratest.CameraOptions{IP: "127.0.0.4", Profile: cameramodel.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}, Identity: cameratest.CameraIdentity{Serial: transportSerial, MAC: transportMAC, DiscoveryID: transportEPR}, Web: ptr(f.key.Issue(t)), WebPort: f.port})
	discoverer := &transportDiscoverer{}
	discoverer.set(impostor.ONVIF)
	f.run(t, discoverer)
	c := f.decided(t, "127.0.0.4")
	require.Equal(t, "operator_required", c.GetState())
	require.Equal(t, "key_mismatch", c.GetCode())
	require.NotEqual(t, f.key.SPKI, c.GetPresentedSpkiSha256())
	require.Positive(t, impostor.ONVIF.WebHandshakes())
	require.Empty(t, impostor.ONVIF.AuthAttempts(), "no credential is presented to a copied EPR or MAC")
	require.Zero(t, impostor.Media.Connections())
	require.Empty(t, f.camera.AuthAttempts())
	f.unchanged(t)
	// The fixed status stays until the candidate expires; a later scan does
	// not try again with it.
	time.Sleep(1500 * time.Millisecond)
	again := f.get(t).GetTransport().GetCandidates()
	require.Len(t, again, 1)
	require.Equal(t, "key_mismatch", again[0].GetCode())
}

// Every case an operator must decide fails closed with a fixed status, and
// leaves the authorized endpoints, identity, binding and jobs as they were.
func TestCameraTransportCandidateFailsClosed(t *testing.T) {
	staged := func(t *testing.T, f *transportFixture, host, code string) *api.CameraPolicyObserveResponse {
		t.Helper()
		v := f.observe(t, f.moved(host))
		require.Equal(t, "transport_"+code, v.GetCode())
		require.Nil(t, v.GetTransportGrant(), "nothing to verify")
		f.unchanged(t)
		return v
	}
	granted := func(t *testing.T, f *transportFixture, host string) *api.CameraTransportGrant {
		t.Helper()
		v := f.observe(t, f.moved(host))
		require.Equal(t, "transport_verification_required", v.GetCode())
		require.NotNil(t, v.GetTransportGrant())
		return v.GetTransportGrant()
	}
	refused := func(t *testing.T, f *transportFixture, g *api.CameraTransportGrant, code string) {
		t.Helper()
		v := f.verify(t, g, f.key.SPKI)
		require.Equal(t, "transport_"+code, v.GetCode())
		c := v.GetDevice().GetTransport().GetCandidates()
		require.Equal(t, "operator_required", c[len(c)-1].GetState())
		f.unchanged(t)
	}
	t.Run("ordinary_pki", func(t *testing.T) {
		// "A certificate from the same CA" names no key of this camera.
		staged(t, newTransportFixture(t, transportOptions{}), "127.0.0.3", "trust_missing")
	})
	t.Run("http_only", func(t *testing.T) {
		staged(t, newTransportFixture(t, transportOptions{scheme: "http"}), "127.0.0.3", "insecure_http")
	})
	t.Run("tls_verification_skip", func(t *testing.T) {
		staged(t, newTransportFixture(t, transportOptions{skip: true}), "127.0.0.3", "tls_verification_skip")
	})
	t.Run("contradictory_mac", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		// The device's EPR beside another MAC, then its MAC beside another EPR.
		r := f.moved("127.0.0.3")
		r.SetIdentity(api.CameraIdentityEvidence_builder{Macs: []string{"02:00:00:00:99:99"}}.Build())
		v := f.observe(t, r)
		require.Equal(t, "transport_evidence_conflict", v.GetCode())
		require.Nil(t, v.GetTransportGrant())
		f.unchanged(t)
		r = f.moved("127.0.0.4")
		r.SetIdentity(api.CameraIdentityEvidence_builder{Macs: []string{transportMAC}}.Build())
		r.SetDiscoveryId("urn:uuid:0b3c9f0e-2d64-4b8a-a7d2-8f1e3c5a9b70")
		v = f.observe(t, r)
		require.Equal(t, "transport_evidence_conflict", v.GetCode())
		require.Nil(t, v.GetTransportGrant())
		f.unchanged(t)
	})
	t.Run("owned_host", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		_, err := f.devices.Claim(f.ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), DeviceKey: "transport-other-owner", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://[::ffff:127.0.0.3]:80/other"}.Build()}}.Build())
		require.NoError(t, err)
		staged(t, f, "127.0.0.3", "host_owned")
	})
	t.Run("owned_host_after_grant", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		_, err := f.devices.Claim(f.ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), DeviceKey: "transport-other-owner", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://127.0.0.3:8080/other"}.Build()}}.Build())
		require.NoError(t, err)
		refused(t, f, g, "host_owned")
	})
	t.Run("several_candidates", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		v := f.observe(t, f.moved("127.0.0.4"))
		require.Equal(t, "transport_ambiguous", v.GetCode())
		require.Nil(t, v.GetTransportGrant())
		for _, c := range v.GetDevice().GetTransport().GetCandidates() {
			require.Equal(t, "ambiguous", c.GetCode())
		}
		refused(t, f, g, "ambiguous")
	})
	t.Run("expired_evidence", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		// Age the stored evidence; the request names exactly what is stored.
		state := proto.Clone(f.get(t).GetTransport()).(*api.CameraTransportState)
		c := state.GetCandidates()[0]
		g = proto.Clone(g).(*api.CameraTransportGrant)
		g.SetDateIssued(timestamppb.New(time.Now().Add(-3 * time.Minute)))
		g.SetDateExpires(timestamppb.New(time.Now().Add(-time.Minute)))
		c.SetGrant(g)
		_, err := f.server.Ent.CameraDevice.Update().Where(cameradevice.IdEQ(uuidOf(t, f.device.GetId()))).SetTransport(state).Save(f.ctx)
		require.NoError(t, err)
		refused(t, f, g, "expired")
	})
	t.Run("expired_candidate_observed_again", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		state := proto.Clone(f.get(t).GetTransport()).(*api.CameraTransportState)
		state.GetCandidates()[0].SetDateExpires(timestamppb.New(time.Now().Add(-time.Second)))
		_, err := f.server.Ent.CameraDevice.Update().Where(cameradevice.IdEQ(uuidOf(t, f.device.GetId()))).SetTransport(state).Save(f.ctx)
		require.NoError(t, err)
		fresh := granted(t, f, "127.0.0.3")
		require.NotEqual(t, g.GetCandidateId(), fresh.GetCandidateId(), "expired evidence never approves a later observation")
		v := f.verify(t, g, f.key.SPKI)
		require.Equal(t, "transport_expired", v.GetCode(), "the old grant answers with its fixed outcome")
		require.Equal(t, "expired", v.GetDevice().GetTransport().GetCandidates()[0].GetCode())
		require.Equal(t, "pending", v.GetDevice().GetTransport().GetCandidates()[1].GetState())
		f.unchanged(t)
	})
	t.Run("changed_generation", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		assignments := api.NewCameraAssignmentServiceClient(f.admin)
		a, err := assignments.Get(f.ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: f.assignment.GetId()}.Build()}.Build())
		require.NoError(t, err)
		_, err = assignments.Assign(f.ctx, api.CameraAssignmentAssignRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), ExpectedVersion: a.GetDateUpdated(), Network: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build(), WorkerSlot: "transport-moved", WorkerId: f.worker.GetId()}.Build())
		require.NoError(t, err)
		refused(t, f, g, "assignment_changed")
	})
	t.Run("changed_trust", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		_, err := api.NewCameraNetworkServiceClient(f.admin).Configure(f.ctx, api.CameraNetworkConfigureRequest_builder{Ref: api.CameraNetworkRef_builder{Id: f.network.GetId()}.Build(), ExpectedVersion: f.network.GetDateUpdated(), Alias: f.network.GetAlias(), Name: "edited while verifying", Cidrs: f.network.GetCidrs(), InterfaceHint: f.network.GetInterfaceHint()}.Build())
		require.NoError(t, err)
		refused(t, f, g, "trust_changed")
	})
	t.Run("mismatched_trust", func(t *testing.T) {
		// The operator's active revision approves another key than the one
		// the device's last job pinned: no automatic choice between them.
		f := newTransportFixture(t, transportOptions{pin: true})
		request := cameraPolicyCreateRequest(f.network, f.preset, "transport-policy", "2")
		request.SetEnrollment(proto.Clone(f.policy.GetEnrollment()).(*api.CameraEnrollmentPolicy))
		request.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{f.pin(t, cameratest.NewWebKey(t).SPKI)})
		next, err := f.policies.Create(f.ctx, request)
		require.NoError(t, err)
		_, err = f.policies.Activate(f.ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: next.GetId()}.Build(), ExpectedNetworkVersion: f.network.GetDateUpdated()}.Build())
		require.NoError(t, err)
		staged(t, f, "127.0.0.3", "trust_mismatch")
	})
	t.Run("unfinished_job", func(t *testing.T) {
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		// An operator's reapply is an unfinished mutation of the camera.
		_, err := api.NewCameraJobServiceClient(f.admin).Create(f.ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: f.binding.GetId()}.Build(), RequestKey: "transport-reapply", Inputs: f.job.GetInputs()}.Build())
		require.NoError(t, err)
		v := f.verify(t, g, f.key.SPKI)
		require.Equal(t, "transport_mutation_active", v.GetCode())
		require.True(t, proto.Equal(f.device.GetEndpoints()[0], v.GetDevice().GetEndpoints()[0]))
	})
	t.Run("replacement_job", func(t *testing.T) {
		// A grant bound to the device's last job cannot approve anything for
		// the job that settled after it.
		f := newTransportFixture(t, transportOptions{pin: true})
		g := granted(t, f, "127.0.0.3")
		next, err := api.NewCameraJobServiceClient(f.admin).Create(f.ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: f.binding.GetId()}.Build(), RequestKey: "transport-replacement", Inputs: f.job.GetInputs()}.Build())
		require.NoError(t, err)
		f.settle(t, next)
		v := f.verify(t, g, f.key.SPKI)
		require.Equal(t, "transport_job_changed", v.GetCode())
		require.True(t, proto.Equal(f.device.GetEndpoints()[0], v.GetDevice().GetEndpoints()[0]))
	})
}

// Concurrent verifications of one grant, a scan repeating it, lost responses
// and cancellation all come to one decision for that exact candidate.
func TestCameraTransportCandidateRacesAndReplays(t *testing.T) {
	f := newTransportFixture(t, transportOptions{pin: true})
	first := f.observe(t, f.moved("127.0.0.3"))
	g := first.GetTransportGrant()
	require.NotNil(t, g)
	// A repeated scan reissues the same grant rather than a second candidate.
	again := f.observe(t, f.moved("127.0.0.3"))
	require.True(t, proto.Equal(g, again.GetTransportGrant()))
	require.Len(t, again.GetDevice().GetTransport().GetCandidates(), 1)
	// Only the assigned Worker verifies, and only the exact stored grant.
	_, err := f.policies.VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: f.key.SPKI}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an operator cannot impersonate the LAN observer")
	_, otherConn, _ := cameraWorkerMachine(t, f.ctx, f.server, f.cp, f.admin, "transport-other-worker", nil)
	_, err = api.NewCameraPolicyServiceClient(otherConn).VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: f.key.SPKI}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	for _, edit := range []func(*api.CameraTransportGrant){
		func(g *api.CameraTransportGrant) { g.SetEndpoint("https://127.0.0.5:" + strconv.Itoa(f.port)) },
		func(g *api.CameraTransportGrant) { g.SetObservationRevision(g.GetObservationRevision() + 1) },
		func(g *api.CameraTransportGrant) { g.GetTrust().SetSpkiSha256(cameratest.NewWebKey(t).SPKI) },
		func(g *api.CameraTransportGrant) { g.SetDateExpires(timestamppb.New(time.Now().Add(time.Hour))) },
	} {
		changed := proto.Clone(g).(*api.CameraTransportGrant)
		edit(changed)
		_, err := f.machine.VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: changed, SpkiSha256: f.key.SPKI}.Build())
		require.Equal(t, codes.Aborted, status.Code(err))
	}
	// A call whose context ends first may or may not land; either way the
	// replay below decides the candidate exactly once.
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	_, _ = f.machine.VerifyTransport(canceled, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: f.key.SPKI}.Build())
	var wg sync.WaitGroup
	results := make([]*api.CameraPolicyObserveResponse, 4)
	errs := make([]error, 4)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A test CP on SQLite has no row locks: a transaction that lost
			// the race can fail whole, writing nothing, and its caller
			// retries. Every answer that lands must be the one decision.
			for range 5 {
				results[i], errs[i] = f.machine.VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: f.key.SPKI}.Build())
				if errs[i] == nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err)
		require.Equal(t, transportVerified, results[i].GetCode())
	}
	decided := f.get(t)
	c := decided.GetTransport().GetCandidates()[0]
	require.Equal(t, "verified", c.GetState())
	// A lost response is answered again without another write.
	replay := f.verify(t, g, f.key.SPKI)
	require.Equal(t, transportVerified, replay.GetCode())
	require.True(t, proto.Equal(decided.GetDateUpdated(), f.get(t).GetDateUpdated()))
	// The decided grant cannot be decided again with another key.
	_, err = f.machine.VerifyTransport(f.ctx, api.CameraPolicyVerifyTransportRequest_builder{Grant: g, SpkiSha256: cameratest.NewWebKey(t).SPKI}.Build())
	require.Equal(t, codes.Aborted, status.Code(err))
	f.unchanged(t)
}
