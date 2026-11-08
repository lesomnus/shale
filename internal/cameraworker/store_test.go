package cameraworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func checkpointFixture(t *testing.T) (*Execution, *api.CameraWorkerWork) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	id := func(domain pdid.Domain) []byte { return pdid.New(domain).Bytes() }
	a := api.CameraAssignment_builder{Id: id(28), Tenant: api.Tenant_builder{Id: id(1)}.Build(), Set: api.Set_builder{Id: id(7)}.Build(), Producer: api.Producer_builder{Id: id(22)}.Build(), Network: api.CameraNetwork_builder{Id: id(26)}.Build(), Generation: 2}.Build()
	d := api.CameraDevice_builder{Id: id(27), Network: a.GetNetwork(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20/onvif/device_service"}.Build()}}.Build()
	b := api.CameraBinding_builder{Id: id(29), Assignment: a, Device: d, Source: api.Source_builder{Id: id(8)}.Build(), LogicalKey: "main", SourceAlias: "main-camera"}.Build()
	ref := func(purpose string) *api.CameraCredentialReference {
		return api.CameraCredentialReference_builder{Provider: "cp", Id: "credential-" + purpose, Version: "version-1", Purpose: purpose}.Build()
	}
	j := api.CameraJob_builder{Id: id(30), Assignment: a, Binding: b, Inputs: api.CameraJobInputs_builder{AllowActivation: true, InitialCredential: ref("initial_admin"), ManagementCredential: ref("management"), StreamingCredential: ref("streaming")}.Build(), LeaseGeneration: 3, AssignmentGeneration: 2, LeaseExpires: timestamppb.New(time.Now().Add(time.Minute)), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_CLAIMED}.Build()
	w := api.CameraWorkerWork_builder{Job: j, Assignment: a, Device: d, Binding: b, Network: a.GetNetwork()}.Build()
	e := &Execution{life: ctx, cancel: cancel, work: proto.Clone(w).(*api.CameraWorkerWork)}
	e.workers = workerClient{check: func(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
		return proto.Clone(w).(*api.CameraWorkerWork), nil
	}}
	e.jobs = jobClient{report: func(_ context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
		require.Equal(t, w.GetJob().GetLeaseGeneration(), r.GetLeaseGeneration())
		require.Equal(t, w.GetJob().GetAssignmentGeneration(), r.GetAssignmentGeneration())
		_, err := cameraprogress.Decode(r.GetObserved().GetProgress())
		require.NoError(t, err)
		w.GetJob().SetObserved(proto.Clone(r.GetObserved()).(*api.CameraJobObservation))
		w.GetJob().SetStage(r.GetStage())
		return proto.Clone(w.GetJob()).(*api.CameraJob), nil
	}}
	return e, w
}

func checkpointRun(w *api.CameraWorkerWork) provision.Run {
	q := jobRequest(w)
	now := time.Now().UTC()
	return provision.Run{Request: q, Stage: provision.Pending, Version: 1, CreatedAt: now, UpdatedAt: now, InitialRef: q.InitialRef, ManagementRef: q.ManagementRef, StreamingRef: q.StreamingRef, Binding: provision.BindingState{BindingID: q.BindingID, Alias: q.Alias}}
}

func TestJobStoreRetainsPinnedStateAndRebindsOnlyDeliveryAndTransport(t *testing.T) {
	e, w := checkpointFixture(t)
	w.GetJob().GetInputs().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-v1", WebScheme: "https", WebPort: 8443}.Build())
	s := &JobStore{Execution: e}
	r := checkpointRun(w)
	r.ActivationAttempted = true
	r.CandidateIndex = 2
	r.Events = []provision.Event{{At: r.UpdatedAt, Stage: provision.Activating, Code: "credential_prepared"}}
	require.NoError(t, s.Save(t.Context(), r))
	oldScope := r.Request.Scope
	w.GetAssignment().SetProducer(api.Producer_builder{Id: pdid.New(22).Bytes()}.Build())
	w.GetDevice().GetEndpoints()[0].SetUri("http://192.0.2.77/onvif/device_service")
	got, err := s.Load(t.Context(), r.Request.ID)
	require.NoError(t, err)
	require.True(t, got.ActivationAttempted)
	require.Equal(t, 2, got.CandidateIndex)
	require.Equal(t, r.Version, got.Version)
	require.Equal(t, r.InitialRef.ID, got.InitialRef.ID)
	require.Equal(t, r.InitialRef.Version, got.InitialRef.Version)
	require.Equal(t, r.Request.Bootstrap, got.Request.Bootstrap)
	require.NotSame(t, r.Request.Bootstrap, got.Request.Bootstrap)
	require.NotEqual(t, oldScope.ProducerID, got.InitialRef.Scope.ProducerID)
	require.Equal(t, enrollmentScope(w.GetAssignment()), got.Request.Scope)
	require.Equal(t, "http://192.0.2.77/onvif/device_service", got.Request.Device.Endpoints[0].URL)
	require.Equal(t, "http://192.0.2.20/onvif/device_service", r.Request.Device.Endpoints[0].URL, "state is detached")
	_, err = s.Load(t.Context(), pdid.New(30).String())
	require.ErrorIs(t, err, camera.ErrForbidden)
}

func TestJobStoreBootstrapCannotChangeOrOverrideActivation(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "pinned"}[pinned], func(t *testing.T) {
			e, w := checkpointFixture(t)
			w.GetJob().GetInputs().SetAllowActivation(false)
			if pinned {
				w.GetJob().GetInputs().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: "fixture-v1", WebScheme: "https", WebPort: 8443}.Build())
			}
			s := &JobStore{Execution: e}
			q := jobRequest(w)
			require.False(t, q.AllowActivation)
			require.NoError(t, s.Reserve(t.Context(), q))
			r := checkpointRun(w)
			require.NoError(t, s.Save(t.Context(), r))
			if pinned {
				q.Bootstrap.WebPort = 443
				require.Equal(t, int32(8443), w.GetJob().GetInputs().GetBootstrap().GetWebPort(), "request owns its pointer")
			} else {
				q.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-v1", WebScheme: "https", WebPort: 8443}
			}
			require.ErrorIs(t, s.Reserve(t.Context(), q), camera.ErrForbidden)
			r.Request.Bootstrap = q.Bootstrap
			require.ErrorIs(t, s.Save(t.Context(), r), camera.ErrForbidden)
			if pinned {
				q.Bootstrap = nil
				require.ErrorIs(t, s.Reserve(t.Context(), q), camera.ErrForbidden)
			}
			// An inconsistent saved pin cannot become a fresh request on resume.
			w.GetJob().GetObserved().GetProgress().GetRequest().SetBootstrap(api.CameraBootstrapProfile_builder{CompatibilityProfile: "other", WebScheme: "http", WebPort: 80}.Build())
			_, err := s.Load(t.Context(), q.ID)
			require.ErrorIs(t, err, camera.ErrForbidden)
		})
	}
}

func TestJobStoreLostCheckpointStopsFollowingDeviceWrites(t *testing.T) {
	e, w := checkpointFixture(t)
	s := &JobStore{Execution: e}
	r := checkpointRun(w)
	e.jobs = jobClient{report: func(context.Context, *api.CameraJobReportRequest) (*api.CameraJob, error) {
		return nil, errors.New("lost response with password")
	}}
	require.Error(t, s.Save(t.Context(), r))
	calls := 0
	err := e.Step(context.Background(), func(context.Context, *api.CameraWorkerWork) error { calls++; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

type resolveClient struct {
	api.CameraPresetServiceClient
	resolve func(context.Context, *api.CameraPresetResolveForJobRequest) (*api.CameraResolvedPreset, error)
}

func (c resolveClient) ResolveForJob(ctx context.Context, r *api.CameraPresetResolveForJobRequest, _ ...grpc.CallOption) (*api.CameraResolvedPreset, error) {
	return c.resolve(ctx, r)
}

func TestJobPresetsUseCurrentLeaseAndNeverInferScopeFromHints(t *testing.T) {
	e, w := checkpointFixture(t)
	calls := 0
	e.presets = resolveClient{resolve: func(_ context.Context, r *api.CameraPresetResolveForJobRequest) (*api.CameraResolvedPreset, error) {
		calls++
		require.Equal(t, w.GetJob().GetId(), r.GetJob().GetId())
		require.Equal(t, int64(3), r.GetLeaseGeneration())
		require.Equal(t, int64(2), r.GetAssignmentGeneration())
		return api.CameraResolvedPreset_builder{Id: "fixed-preset", Revision: "1", Requested: api.CameraPresetSettings_builder{Video: api.CameraPresetVideo_builder{Codec: "h264"}.Build()}.Build()}.Build(), nil
	}}
	p := JobPresets{Execution: e}
	h := deviceHint(w)
	scope := enrollmentScope(w.GetAssignment())
	got, err := p.Resolve(t.Context(), scope, h)
	require.NoError(t, err)
	require.Equal(t, "fixed-preset", got.Ref.ID)
	scope.NetworkID = pdid.New(26).String()
	_, err = p.Resolve(t.Context(), scope, h)
	require.ErrorIs(t, err, camera.ErrForbidden)
	require.Equal(t, 1, calls)
}

type fixtureInspector func(context.Context, camera.DeviceHint, camera.CredentialLease) (camera.Inspection, error)

func (f fixtureInspector) Inspect(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
	return f(ctx, h, l)
}

func TestLeasedDevicesRefreshTransportAndHideDriverError(t *testing.T) {
	e, w := checkpointFixture(t)
	h := deviceHint(w)
	h.Endpoints[0].URL = "http://192.0.2.1/old"
	w.GetDevice().GetEndpoints()[0].SetUri("http://192.0.2.99/onvif/device_service")
	secret := errors.New("fixture vendor password")
	d := LeasedDevices{Execution: e, Inspector: fixtureInspector(func(ctx context.Context, actual camera.DeviceHint, _ camera.CredentialLease) (camera.Inspection, error) {
		require.Equal(t, h.ID, actual.ID)
		require.Equal(t, "http://192.0.2.99/onvif/device_service", actual.Endpoints[0].URL)
		_, ok := ctx.Deadline()
		require.True(t, ok)
		return camera.Inspection{}, secret
	})}
	_, err := d.Inspect(t.Context(), h, nil)
	require.ErrorIs(t, err, secret)
	require.NotContains(t, err.Error(), "password")
}

func TestJobCommitterRecoversPublicationWithoutReplayingNewProbeTimes(t *testing.T) {
	e, w := checkpointFixture(t)
	r := checkpointRun(w)
	p := camera.MediaProfile{Token: "main", Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: 2_000_000, GOP: 30, StreamURI: "rtsp://192.0.2.20/stream"}
	r.Stage = provision.Deploying
	r.Plan = &camera.ChangePlan{ProfileToken: "main", Before: p, After: p, Revision: "1"}
	r.Verified = &camera.VerifiedState{Profile: p, Complete: true, Decoded: true, DecodedFrames: 150, Duration: 5 * time.Second, ObservedAt: r.UpdatedAt, ProfileRevision: "1"}
	// The new actual decoder sample has a slightly different cadence. A lost
	// durable response must not turn this into another capture publication.
	r.Verified.Profile.FPS = 29.97
	o, err := runObservation(r, nil)
	require.NoError(t, err)
	w.GetJob().SetObserved(o)
	w.GetJob().SetPublishedRevision(9)
	w.GetJob().SetDatePublished(timestamppb.New(r.UpdatedAt.Add(-time.Minute)))
	w.GetJob().SetStage(api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY)
	old := captureProfile(p)
	old.SetKeyframeIntervalMs(1000)
	w.GetBinding().SetCollect(true)
	w.GetBinding().SetDesired(api.CameraCaptureSpec_builder{Profile: old, Input: p.StreamURI, Credential: w.GetJob().GetInputs().GetStreamingCredential(), Revision: 9}.Build())
	d := provision.Deployment{OperationID: "stable-operation", Request: r.Request, Profile: p, Credential: *r.StreamingRef, Previous: r.Binding}
	receipt, err := (JobCommitter{Execution: e}).Commit(t.Context(), d)
	require.NoError(t, err)
	require.Equal(t, int64(9), receipt.Revision.Catalog)
	require.Equal(t, cameraID(w.GetBinding().GetSource().GetId()), receipt.Binding.SourceID)
	require.Equal(t, "stable-operation", receipt.Binding.DeploymentID)
	require.Equal(t, int64(1000), w.GetBinding().GetDesired().GetProfile().GetKeyframeIntervalMs(), "a new probe does not replace durable capture intent")
	w.GetBinding().GetDesired().SetInput("rtsp://192.0.2.21/another")
	_, err = (JobCommitter{Execution: e}).Commit(t.Context(), d)
	require.ErrorIs(t, err, camera.ErrConflict)
}

type fixtureVerifier func(context.Context, camera.MediaRequest) (camera.VerifiedState, error)

func (f fixtureVerifier) Verify(ctx context.Context, r camera.MediaRequest) (camera.VerifiedState, error) {
	return f(ctx, r)
}

func TestLeasedVerifierRechecksPendingMediaPrefixAfterPolicyChange(t *testing.T) {
	e, w := checkpointFixture(t)
	w.GetNetwork().SetCidrs([]string{"192.0.2.0/24", "198.51.100.0/24"})
	q := jobRequest(w)
	calls := 0
	d := LeasedDevices{Execution: e, Verifier: fixtureVerifier(func(context.Context, camera.MediaRequest) (camera.VerifiedState, error) {
		calls++
		return camera.VerifiedState{}, nil
	})}
	r := camera.MediaRequest{Scope: q.Scope, NetworkScope: q.Scope.NetworkID, Device: q.Device, Profile: camera.MediaProfile{StreamURI: "rtsp://198.51.100.20/main"}}
	_, err := d.Verify(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	// Management remains inside its allowed prefix. The pending unpublished
	// RTSP address alone no longer belongs to this fresh network grant.
	w.GetNetwork().SetCidrs([]string{"192.0.2.0/24"})
	_, err = d.Verify(t.Context(), r)
	require.ErrorIs(t, err, camera.ErrForbidden)
	require.Equal(t, 1, calls)
}

func TestJobCommitterRecoversUnknownRateFactsWithoutReplacingPublishedSpec(t *testing.T) {
	e, w := checkpointFixture(t)
	r := checkpointRun(w)
	configured := camera.MediaProfile{Token: "main", Codec: "h264", Width: 1280, Height: 720, GOP: 30, StreamURI: "rtsp://192.0.2.20/main"}
	actual := configured
	actual.FPS = 29.99
	actual.Bitrate = 1_800_000
	r.Stage = provision.Deploying
	r.Plan = &camera.ChangePlan{ProfileToken: "main", Before: configured, After: configured, Revision: "1"}
	r.Verified = &camera.VerifiedState{Profile: actual, Complete: true, Decoded: true, DecodedFrames: 150, Duration: 5 * time.Second, ObservedAt: r.UpdatedAt, ProfileRevision: "1"}
	o, err := runObservation(r, nil)
	require.NoError(t, err)
	w.GetJob().SetObserved(o)
	w.GetJob().SetPublishedRevision(9)
	w.GetJob().SetDatePublished(timestamppb.New(r.UpdatedAt.Add(-time.Minute)))
	w.GetJob().SetStage(api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY)
	old := captureProfile(actual)
	old.SetFps(29.97)
	old.SetMaxBitrateBps(1_700_000)
	w.GetBinding().SetCollect(true)
	w.GetBinding().SetDesired(api.CameraCaptureSpec_builder{Profile: old, Input: actual.StreamURI, Credential: w.GetJob().GetInputs().GetStreamingCredential(), Revision: 9}.Build())
	d := provision.Deployment{OperationID: "stable-unknown-operation", Request: r.Request, Profile: actual, Credential: *r.StreamingRef, Previous: r.Binding}
	receipt, err := (JobCommitter{Execution: e}).Commit(t.Context(), d)
	require.NoError(t, err)
	require.Equal(t, int64(9), receipt.Revision.Catalog)
	require.Equal(t, 29.97, w.GetBinding().GetDesired().GetProfile().GetFps())
	require.Equal(t, int64(1_700_000), w.GetBinding().GetDesired().GetProfile().GetMaxBitrateBps())
}
