package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// keyedWeb is a camera whose web port presents key. A driver pinning another
// key is refused in the handshake, before any request or credential.
type keyedWeb struct {
	pin *cameramodel.DeviceTLSIdentity
	key string
}

func (c keyedWeb) web() error {
	if c.pin == nil || c.pin.SPKISHA256 != c.key {
		return cameramodel.ErrTLSIdentityChanged
	}
	return nil
}
func (c keyedWeb) Activation(context.Context, cameramodel.DeviceHint) (cameramodel.ActivationState, error) {
	return cameramodel.ActivationReady, c.web()
}
func (c keyedWeb) Activate(context.Context, cameramodel.DeviceHint, cameramodel.CredentialLease) error {
	return c.web()
}
func (c keyedWeb) Inspect(context.Context, cameramodel.DeviceHint, cameramodel.CredentialLease) (cameramodel.Inspection, error) {
	return cameramodel.Inspection{}, cameramodel.ErrAuthRequired
}
func (c keyedWeb) Plan(context.Context, cameramodel.Inspection, cameramodel.DesiredState) (cameramodel.ChangePlan, error) {
	return cameramodel.ChangePlan{}, cameramodel.ErrUnsupported
}
func (c keyedWeb) Apply(context.Context, cameramodel.DeviceHint, cameramodel.CredentialLease, cameramodel.ChangePlan) (cameramodel.CurrentState, error) {
	return cameramodel.CurrentState{}, cameramodel.ErrUnsupported
}
func (c keyedWeb) ObserveWebIdentity(_ context.Context, h cameramodel.DeviceHint) (cameramodel.DeviceTLSIdentity, error) {
	return cameramodel.DeviceTLSIdentity{DeviceID: string(h.ID), SPKISHA256: c.key}, nil
}

// A job meeting a key its pin does not name stops with that key, through the
// real Worker, leased checkpoint and CP validation. One operator request then
// approves exactly that key for exactly that device.
func TestCameraPolicyApproveChangedTLSKey(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	producer, _, _ := cameraWorkerProducer(t, ctx, server, cp, admin, "tls-approve-producer", nil)
	worker, workerConn, agent := cameraWorkerMachine(t, ctx, server, cp, admin, "tls-approve-worker", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "tls-approve-lan", nil)
	nref := api.CameraNetworkRef_builder{Id: n.GetId()}.Build()
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: nref, Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "tls-approve", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	devices := api.NewCameraDeviceServiceClient(admin)
	claim := func(key, host string) *api.CameraDevice {
		t.Helper()
		v, e := devices.Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: nref, DeviceKey: key, Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://" + host + "/onvif/device_service"}.Build()}}.Build())
		require.NoError(t, e)
		return v
	}
	d1, d2 := claim("tls-approve-a", "192.0.2.21"), claim("tls-approve-b", "192.0.2.22")
	key := func(digit string) string { return "sha256:" + strings.Repeat(digit, 64) }
	pin := func(d *api.CameraDevice, digit string) *api.CameraDeviceTLSIdentity {
		return api.CameraDeviceTLSIdentity_builder{DeviceId: cameraPolicyID(t, d.GetId()), SpkiSha256: key(digit)}.Build()
	}
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("tls-approve-preset", "1", cameraPolicyRule("base", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, n.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	policies := api.NewCameraPolicyServiceClient(admin)
	request := cameraPolicyCreateRequest(n, preset, "tls-approve-policy", "1")
	request.SetEnrollment(api.CameraEnrollmentPolicy_builder{Bootstrap: api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 443}.Build(), DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{pin(d1, "a"), pin(d2, "d")}}.Build())
	original, err := policies.Create(ctx, request)
	require.NoError(t, err)
	n, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: original.GetId()}.Build(), ExpectedNetworkVersion: n.GetDateUpdated()}.Build())
	require.NoError(t, err)

	bindings := api.NewCameraBindingServiceClient(admin)
	jobs := api.NewCameraJobServiceClient(admin)
	enroll := func(d *api.CameraDevice, alias, digit string) *api.CameraJob {
		t.Helper()
		b, e := bindings.Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: alias, SourceAlias: alias}.Build())
		require.NoError(t, e)
		credential := func(purpose string) *api.CameraCredentialReference {
			return api.CameraCredentialReference_builder{Provider: "cp", Id: alias + "-" + purpose, Version: "1", Purpose: purpose}.Build()
		}
		j, e := jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: alias + "-intent", Inputs: api.CameraJobInputs_builder{
			Policy: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, original.GetId()), Revision: original.GetRevision()}.Build(), Bootstrap: proto.Clone(original.GetEnrollment().GetBootstrap()).(*api.CameraBootstrapProfile), DeviceTlsIdentity: pin(d, digit),
			ManagementCredential: credential("management"), StreamingCredential: credential("streaming"),
		}.Build()}.Build())
		require.NoError(t, e)
		return j
	}
	job1, job2 := enroll(d1, "tls-approve-a", "a"), enroll(d2, "tls-approve-b", "d")

	// The first camera now presents key b; its job pins key a.
	var mu sync.Mutex
	presented := map[string]string{string(d1.GetId()): key("b"), string(d2.GetId()): key("d")}
	present := func(d *api.CameraDevice, v string) {
		mu.Lock()
		defer mu.Unlock()
		presented[string(d.GetId())] = v
	}
	runtime, err := cameraworker.New(cameraworker.Config{StateDir: agent.Store.Dir, Cp: cp, Tenant: "acme", HardwareID: "tls-approve-worker", CaHash: pki.Fingerprint(server.CA.Cert), HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 30, Log: agent.Log, Executor: cameraworker.ExecutorFunc(func(ctx context.Context, e *cameraworker.Execution) error {
		if !bytes.Equal(e.Snapshot().GetDevice().GetId(), d1.GetId()) {
			return nil
		}
		p := cameraworker.Provisioner{
			Devices: cameraworker.LeasedDevices{Factory: func(_ context.Context, w *api.CameraWorkerWork) (cameraworker.DeviceAdapters, error) {
				mu.Lock()
				c := keyedWeb{pin: cameraworker.DeviceTLSIdentity(w), key: presented[string(w.GetDevice().GetId())]}
				mu.Unlock()
				return cameraworker.DeviceAdapters{Inspector: c, Configurator: c, Bootstrapper: c, Observer: c}, nil
			}},
			Auth:      func(*cameraworker.Execution) cameramodel.AuthProvider { return nil },
			Publisher: func(*cameraworker.Execution) provision.SourcePublisher { return nil },
		}
		err := p.Execute(ctx, e)
		if errors.Is(err, cameramodel.ErrTLSIdentityChanged) {
			return nil // stopped for an operator; not retried in this session
		}
		return err
	})})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Worker did not stop")
		}
	})
	stoppedOn := func(j *api.CameraJob, digit string) *api.CameraJob {
		t.Helper()
		var v *api.CameraJob
		require.Eventually(t, func() bool {
			var e error
			v, e = jobs.Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build()}.Build())
			require.NoError(t, e)
			return v.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_WAITING && v.GetObserved().GetCode() == "tls_key_changed" && v.GetObserved().GetProgress().GetChangedTlsIdentity().GetSpkiSha256() == key(digit)
		}, 15*time.Second, 50*time.Millisecond)
		return v
	}
	stopped := stoppedOn(job1, "b")
	require.True(t, proto.Equal(pin(d1, "b"), stopped.GetObserved().GetProgress().GetChangedTlsIdentity()))
	require.True(t, proto.Equal(pin(d1, "a"), stopped.GetInputs().GetDeviceTlsIdentity()), "an observed key never becomes the job's pin")

	// Without an approval, the changed key is refused: no job can pin it.
	b1, err := bindings.Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: job1.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	unapproved := proto.Clone(job1.GetInputs()).(*api.CameraJobInputs)
	unapproved.SetDeviceTlsIdentity(pin(d1, "b"))
	_, err = jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b1.GetId()}.Build(), RequestKey: "tls-approve-unapproved", Inputs: unapproved}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	approve := func(conn api.CameraPolicyServiceClient, j *api.CameraJob, digit string, version *api.CameraNetwork) (*api.CameraPolicyApproveTLSResponse, error) {
		return conn.ApproveTLS(ctx, api.CameraPolicyApproveTLSRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), SpkiSha256: key(digit), ExpectedNetworkVersion: version.GetDateUpdated()}.Build())
	}
	_, err = approve(api.NewCameraPolicyServiceClient(workerConn), job1, "b", n)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the Worker that saw the key cannot approve it")
	_, err = approve(policies, job1, "c", n)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "an approval names exactly the key the job recorded")
	_, err = policies.ApproveTLS(ctx, api.CameraPolicyApproveTLSRequest_builder{Ref: api.CameraJobRef_builder{Id: job1.GetId()}.Build(), SpkiSha256: strings.ToUpper(key("b")), ExpectedNetworkVersion: n.GetDateUpdated()}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = approve(policies, job2, "b", n)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the key cannot be approved onto another device's job")
	stale := proto.Clone(n).(*api.CameraNetwork)
	stale.SetDateUpdated(original.GetDateCreated())
	_, err = approve(policies, job1, "b", stale)
	require.Equal(t, codes.Aborted, status.Code(err), "the approval activates under the network's CAS")
	current, err := api.NewCameraNetworkServiceClient(admin).Get(ctx, api.CameraNetworkGetRequest_builder{Ref: nref}.Build())
	require.NoError(t, err)
	require.Equal(t, original.GetId(), current.GetActivePolicyId(), "refused approvals change nothing")

	// After the approval the camera presents yet another key.
	present(d1, key("c"))
	approved, err := approve(policies, job1, "b", n)
	require.NoError(t, err)
	p := approved.GetPolicy()
	require.NotEqual(t, original.GetId(), p.GetId())
	require.Equal(t, original.GetPolicyKey(), p.GetPolicyKey())
	require.True(t, strings.HasPrefix(p.GetRevision(), "tls-"))
	require.Equal(t, p.GetId(), approved.GetNetwork().GetActivePolicyId())
	require.Len(t, p.GetEnrollment().GetDeviceTlsIdentities(), 2)
	require.True(t, proto.Equal(pin(d2, "d"), p.GetEnrollment().GetDeviceTlsIdentities()[0]), "other devices keep their approval")
	require.True(t, proto.Equal(pin(d1, "b"), p.GetEnrollment().GetDeviceTlsIdentities()[1]))
	expected := proto.Clone(original.GetEnrollment()).(*api.CameraEnrollmentPolicy)
	expected.SetDeviceTlsIdentities(p.GetEnrollment().GetDeviceTlsIdentities())
	require.True(t, proto.Equal(expected, p.GetEnrollment()), "only the device's key changes")
	successor := approved.GetJob()
	require.True(t, proto.Equal(pin(d1, "b"), successor.GetInputs().GetDeviceTlsIdentity()))
	require.Equal(t, job1.GetBinding().GetId(), successor.GetBinding().GetId())
	require.Equal(t, cameraPolicyID(t, p.GetId()), successor.GetInputs().GetPolicy().GetId())
	require.True(t, proto.Equal(job1.GetInputs().GetManagementCredential(), successor.GetInputs().GetManagementCredential()))
	require.True(t, proto.Equal(job1.GetInputs().GetStreamingCredential(), successor.GetInputs().GetStreamingCredential()))
	settled, err := jobs.Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job1.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, settled.GetStage())
	other, err := jobs.Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job2.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotEqual(t, api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, other.GetStage())
	require.True(t, proto.Equal(pin(d2, "d"), other.GetInputs().GetDeviceTlsIdentity()))

	// The approval is spent: the same request is refused, even at the current
	// network version, and so is the approved key on the job that now stopped
	// on yet another key.
	_, err = approve(policies, job1, "b", approved.GetNetwork())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	again := stoppedOn(successor, "c")
	require.True(t, proto.Equal(pin(d1, "b"), again.GetInputs().GetDeviceTlsIdentity()))
	_, err = approve(policies, successor, "b", approved.GetNetwork())
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a different key after the approval needs its own approval")

	// A network whose active policy checks no key has no key to approve.
	skip := cameraPolicyCreateRequest(n, preset, "tls-approve-policy", "skip")
	skip.SetEnrollment(api.CameraEnrollmentPolicy_builder{Bootstrap: proto.Clone(original.GetEnrollment().GetBootstrap()).(*api.CameraBootstrapProfile), TlsVerification: cameramodel.TLSVerificationSkip}.Build())
	skipping, err := policies.Create(ctx, skip)
	require.NoError(t, err)
	n, err = policies.Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: skipping.GetId()}.Build(), ExpectedNetworkVersion: approved.GetNetwork().GetDateUpdated()}.Build())
	require.NoError(t, err)
	_, err = approve(policies, successor, "c", n)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
