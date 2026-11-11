package e2e_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This is a portable-engine/real-TLS-CP contract test with a scripted device.
// Actual ONVIF/RTSP, independent decoded live, and committed media are qualified
// by the separate edge media suite, never inferred from this fake's counters.
type checkpointDevice struct {
	mu                                                 sync.Mutex
	profile                                            cameramodel.MediaProfile
	frequency                                          int
	videoWrites, initializationWrites, primaryAttempts int
	omitRateFacts                                      bool
	probes                                             int
}

func (d *checkpointDevice) Inspect(_ context.Context, h cameramodel.DeviceHint, _ cameramodel.CredentialLease) (cameramodel.Inspection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h.Manufacturer = "fixture"
	h.Model = "TestCam"
	h.Serial = "workflow-unit"
	h.Evidence = []cameramodel.Evidence{{Source: "scripted", Authenticated: true}}
	profile := d.profile
	if d.omitRateFacts {
		profile.FPS = 0
		profile.Bitrate = 0
	}
	return cameramodel.Inspection{Device: h, Activation: cameramodel.ActivationReady, Current: cameramodel.CurrentState{Profiles: []cameramodel.MediaProfile{profile}, Firmware: "scripted-1"}}, nil
}
func (d *checkpointDevice) Plan(_ context.Context, i cameramodel.Inspection, w cameramodel.DesiredState) (cameramodel.ChangePlan, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w.Width == 1280 {
		d.primaryAttempts++
		return cameramodel.ChangePlan{}, cameramodel.ErrUnsupported
	}
	before := i.Current.Profiles[0]
	after := before
	if w.Codec != "" {
		after.Codec = w.Codec
	}
	if w.Width != 0 {
		after.Width = w.Width
	}
	if w.Height != 0 {
		after.Height = w.Height
	}
	if w.FPS != 0 {
		after.FPS = w.FPS
	}
	if w.Bitrate != 0 {
		after.Bitrate = w.Bitrate
	}
	if w.GOP != 0 {
		after.GOP = w.GOP
	}
	var changes []string
	if before.FPS != after.FPS {
		changes = append(changes, "fps")
	}
	return cameramodel.ChangePlan{ProfileToken: before.Token, Before: before, After: after, Revision: w.Revision, Changes: changes}, nil
}
func (d *checkpointDevice) Apply(_ context.Context, _ cameramodel.DeviceHint, _ cameramodel.CredentialLease, p cameramodel.ChangePlan) (cameramodel.CurrentState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.videoWrites++
	d.profile = p.After
	return cameramodel.CurrentState{Profiles: []cameramodel.MediaProfile{d.profile}, Firmware: "scripted-1"}, nil
}
func (d *checkpointDevice) PlanInitialization(_ context.Context, _ cameramodel.DeviceHint, _ cameramodel.CredentialLease, s cameramodel.DeviceSettings) (cameramodel.InitializationPlan, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var changes []string
	if d.frequency != s.PowerLineFrequency {
		changes = []string{cameramodel.PresetFieldFrequency}
	}
	return cameramodel.InitializationPlan{Before: cameramodel.DeviceSettings{PowerLineFrequency: d.frequency}, After: s, Changes: changes, Revision: "frequency-v1"}, nil
}
func (d *checkpointDevice) ApplyInitialization(_ context.Context, _ cameramodel.DeviceHint, _ cameramodel.CredentialLease, p cameramodel.InitializationPlan) (cameramodel.DeviceSettings, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initializationWrites++
	d.frequency = p.After.PowerLineFrequency
	return p.After, nil
}
func (d *checkpointDevice) Verify(_ context.Context, r cameramodel.MediaRequest) (cameramodel.VerifiedState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := d.profile
	d.probes++
	p.FPS -= 0.01 * float64(d.probes)
	p.Bitrate -= 100_000 * int64(d.probes)
	return cameramodel.VerifiedState{Profile: p, Complete: true, Decoded: true, DecodedFrames: 75, Duration: r.Duration, ObservedAt: time.Now().UTC(), Firmware: "scripted-1", ProfileRevision: r.Desired.Revision}, nil
}

type checkpointAuth struct{}

func (checkpointAuth) Acquire(_ context.Context, r cameramodel.AuthRequest) (cameramodel.CredentialLease, error) {
	if r.ExistingRef == nil {
		return nil, cameramodel.ErrAuthRequired
	}
	return cameramodel.NewCredentialLease(*r.ExistingRef, cameramodel.CredentialMaterial{Username: "fixture-user", Password: "fixture-password"}, time.Now().Add(time.Minute)), nil
}

type checkpointNotReady struct{}

func (checkpointNotReady) Observe(context.Context, provision.Deployment, provision.BindingState) (provision.BindingState, error) {
	return provision.BindingState{}, provision.ErrNotReady
}

func TestCameraPolicyPortableWorkflowRetainsFallbackAndDurablePublication(t *testing.T) {
	cameraPolicyPortableWorkflow(t, false)
}
func TestCameraPolicyUnknownRateFactsRecoverDurablePublication(t *testing.T) {
	cameraPolicyPortableWorkflow(t, true)
}
func cameraPolicyPortableWorkflow(t *testing.T, omitRateFacts bool) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	worker, workerConn, agent := cameraWorkerMachine(t, ctx, server, cp, admin, "workflow-worker", nil)
	presetRequest := cameraPolicyPresetRequest("workflow-preset", "1", cameraPolicyRule("defaults", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, worker.GetTenant().GetId())}.Build()))
	if omitRateFacts {
		rule := presetRequest.GetRules()[0]
		rule.GetSettings().GetVideo().SetFps(0)
		rule.GetSettings().GetVideo().SetBitrateBps(0)
		rule.GetAlternatives().GetCandidates()[0].GetVideo().SetFps(0)
	}
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, presetRequest)
	require.NoError(t, err)
	ref := func(purpose string) *api.CameraCredentialReference {
		return api.CameraCredentialReference_builder{Provider: "fixture", Id: "workflow-" + purpose, Version: "original", Purpose: purpose}.Build()
	}
	job := cameraWorkerJob(t, ctx, server, cp, admin, worker, "workflow", nil, api.CameraJobInputs_builder{Preset: api.CameraRevisionReference_builder{Id: cameraPolicyID(t, preset.GetId()), Revision: preset.GetRevision()}.Build(), ManagementCredential: ref("management"), StreamingCredential: ref("streaming")}.Build())
	binding, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: job.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err)
	device, err := api.NewCameraDeviceServiceClient(admin).Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: binding.GetDevice().GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraDeviceServiceClient(admin).Observe(ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), ExpectedVersion: device.GetDateUpdated(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20/onvif/device_service"}.Build()}}.Build())
	require.NoError(t, err)
	d := &checkpointDevice{omitRateFacts: omitRateFacts, frequency: 50, profile: cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 640, Height: 360, FPS: 30, Bitrate: 2_000_000, GOP: 30, StreamURI: "rtsp://192.0.2.20/stream"}}
	var operationIDs []string
	forgedDeclarations := 0
	p := cameraworker.Provisioner{Devices: cameraworker.LeasedDevices{Inspector: d, Configurator: d, Initializer: d, Verifier: d}, Auth: func(*cameraworker.Execution) cameramodel.AuthProvider { return checkpointAuth{} }, Publisher: func(e *cameraworker.Execution) provision.SourcePublisher {
		w := e.Snapshot()
		a := w.GetAssignment()
		return &publication.Publisher{AssignmentID: cameraPolicyID(t, a.GetId()), Scope: source.Scope{TenantID: cameraPolicyID(t, a.GetTenant().GetId()), SetID: cameraPolicyID(t, a.GetSet().GetId()), ProducerID: cameraPolicyID(t, a.GetProducer().GetId()), NetworkID: cameraPolicyID(t, a.GetNetwork().GetId())}, Committer: sourceCatalogCommit(func(ctx context.Context, dep provision.Deployment) (publication.Receipt, error) {
			operationIDs = append(operationIDs, dep.OperationID)
			if j := e.Snapshot().GetJob(); omitRateFacts && j.GetDatePublished() == nil {
				// The full Deploying checkpoint is already durable. Keeping its
				// bytes unchanged must not permit a different outer declaration.
				forged := proto.Clone(j.GetObserved()).(*api.CameraJobObservation)
				forged.GetEffective().SetMaxBitrateBps(forged.GetEffective().GetMaxBitrateBps() + 100_000)
				_, err := api.NewCameraJobServiceClient(workerConn).Publish(ctx, api.CameraJobPublishRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: "rewrite-unchanged-publication", LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Observed: forged, Spec: api.CameraCaptureSpec_builder{Input: dep.Profile.StreamURI, Profile: forged.GetEffective(), Credential: j.GetInputs().GetStreamingCredential()}.Build()}.Build())
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				forgedDeclarations++
			}
			return (cameraworker.JobCommitter{Execution: e}).Commit(ctx, dep)
		}), Observer: checkpointNotReady{}}
	}}
	results := make(chan error, 1)
	runtime, err := cameraworker.New(cameraworker.Config{StateDir: agent.Store.Dir, Cp: cp, Tenant: "acme", HardwareID: "workflow-worker", CaHash: pki.Fingerprint(server.CA.Cert), HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 30, Log: agent.Log, Executor: cameraworker.ExecutorFunc(func(ctx context.Context, e *cameraworker.Execution) error {
		err := p.Execute(ctx, e)
		if !errors.Is(err, provision.ErrNotReady) {
			results <- err
			return err
		}
		first := e.Snapshot().GetJob().GetPublishedRevision()
		if first <= 0 {
			results <- errors.New("workflow did not publish")
			return nil
		}
		// A new engine and Store recover entirely from CP, including candidate
		// selection. A new probe must reuse the previous durable publication.
		err = p.Execute(ctx, e)
		if !errors.Is(err, provision.ErrNotReady) {
			results <- err
			return err
		}
		if e.Snapshot().GetJob().GetPublishedRevision() != first {
			results <- errors.New("resume replaced the capture publication")
			return nil
		}
		results <- nil
		<-ctx.Done()
		return ctx.Err()
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
			t.Error("workflow Worker did not stop")
		}
	})
	select {
	case err := <-results:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("portable workflow did not finish its contract checks")
	}
	stored, err := api.NewCameraJobServiceClient(admin).Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, stored.GetStage())
	require.Equal(t, int32(1), stored.GetObserved().GetProgress().GetCandidateIndex())
	require.NotEmpty(t, stored.GetObserved().GetWarnings())
	require.Equal(t, int32(60), stored.GetObserved().GetProgress().GetInitialized().GetPowerLineFrequency())
	require.Equal(t, "original", stored.GetObserved().GetProgress().GetStreamingRef().GetVersion())
	d.mu.Lock()
	writes, inits, primary := d.videoWrites, d.initializationWrites, d.primaryAttempts
	d.mu.Unlock()
	if omitRateFacts {
		require.Zero(t, writes)
	} else {
		require.Equal(t, 1, writes)
	}
	require.Len(t, operationIDs, 2)
	require.Equal(t, operationIDs[0], operationIDs[1], "recovery retains the publication intent before a new measurement")
	if omitRateFacts {
		require.Equal(t, 1, forgedDeclarations)
	}
	require.Equal(t, 1, inits)
	require.Equal(t, 1, primary, "resume never retries the rejected preferred candidate")
	sources, err := server.Ent.Source.Query().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, sources)
}
