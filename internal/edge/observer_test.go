package edge

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/mediaprobe"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type observerExecutionFixture struct {
	observations []*api.CameraWorkerObservation
	reads        int
	checkpoints  []*api.CameraJobObservation
	stage        api.CameraJobStage
	observeError error
	reportError  error
	step         func(context.Context, func(context.Context, *api.CameraWorkerWork) error) error
}

func (f *observerExecutionFixture) Observe(ctx context.Context) (*api.CameraWorkerObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.observeError != nil {
		return nil, f.observeError
	}
	i := min(f.reads, len(f.observations)-1)
	f.reads++
	return proto.Clone(f.observations[i]).(*api.CameraWorkerObservation), nil
}

func (f *observerExecutionFixture) Step(ctx context.Context, run func(context.Context, *api.CameraWorkerWork) error) error {
	if f.step != nil {
		return f.step(ctx, run)
	}
	i := min(f.reads-1, len(f.observations)-1)
	w := proto.Clone(f.observations[i].GetWork()).(*api.CameraWorkerWork)
	ctx, cancel := context.WithDeadline(ctx, w.GetJob().GetLeaseExpires().AsTime())
	defer cancel()
	return run(ctx, w)
}

func (f *observerExecutionFixture) Checkpoint(_ context.Context, key string, stage api.CameraJobStage, observed *api.CameraJobObservation) (*api.CameraJob, error) {
	if f.reportError != nil {
		return nil, f.reportError
	}
	if len(key) != 64 {
		return nil, camera.ErrInvalid
	}
	f.stage = stage
	f.checkpoints = append(f.checkpoints, proto.Clone(observed).(*api.CameraJobObservation))
	return &api.CameraJob{}, nil
}

func nativeObserverFixture(t *testing.T) (*jobObserver, *observerExecutionFixture, provision.Deployment, provision.BindingState, mediaprobe.Evidence) {
	t.Helper()
	now := time.Now().UTC()
	id := func(domain pdid.Domain) []byte { return pdid.New(domain).Bytes() }
	a := api.CameraAssignment_builder{Id: id(28), Generation: 2, CatalogRevision: 7, Producer: api.Producer_builder{Id: id(22)}.Build()}.Build()
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 640, Height: 360, Fps: 30, MaxBitrateBps: 2_000_000}.Build()
	b := api.CameraBinding_builder{Id: id(29), Assignment: a, Source: api.Source_builder{Id: id(8)}.Build(), LogicalKey: "camera-main", SourceAlias: "main", Collect: true, AppliedRevision: 7}.Build()
	b.SetDesired(api.CameraCaptureSpec_builder{BindingId: b.GetId(), SourceId: b.GetSource().GetId(), Revision: 7, Profile: profile, Input: "rtsp://192.0.2.1/main"}.Build())
	app := api.CameraApplication_builder{Target: api.CameraApplyTarget_builder{AssignmentId: a.GetId(), BindingId: b.GetId(), SourceId: b.GetSource().GetId(), AssignmentGeneration: 2, CatalogRevision: 7, SpecRevision: 7}.Build(), Sequence: 3, CaptureEpoch: "capture-current", ProducerId: a.GetProducer().GetId(), DateStarted: timestamppb.New(now.Add(-10 * time.Second)), DateApplied: timestamppb.New(now.Add(-8 * time.Second)), Media: api.CameraRuntimeMedia_builder{Profile: profile, Frames: 60, Keyframes: 2, FirstMediaAt: timestamppb.New(now.Add(-9 * time.Second)), LastMediaAt: timestamppb.New(now.Add(-8 * time.Second))}.Build()}.Build()
	b.SetStartingApplication(proto.Clone(app).(*api.CameraApplication))
	b.SetAppliedApplication(app)
	progress := api.CameraProvisionState_builder{SchemaVersion: 1, Stage: "binding", Version: 23, CandidateIndex: 2}.Build()
	prior := api.CameraJobObservation_builder{Effective: profile, Progress: progress, Code: "preserve-code", Warnings: []*api.CameraWarning{api.CameraWarning_builder{Code: "preserve-warning"}.Build()}, Verified: api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DecodedFrames: 8}.Build()}.Build()
	j := api.CameraJob_builder{Id: id(30), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, AssignmentGeneration: 2, LeaseGeneration: 4, LeaseExpires: timestamppb.New(now.Add(time.Minute)), DatePublished: timestamppb.New(now.Add(-20 * time.Second)), PublishedRevision: 7, Observed: prior}.Build()
	w := api.CameraWorkerWork_builder{Job: j, Assignment: a, Binding: b}.Build()
	obs := api.CameraWorkerObservation_builder{Work: w, ProducerStatus: api.ProducerStatus_builder{DateReported: timestamppb.New(now), Sources: []*api.SourceReport{api.SourceReport_builder{SourceId: b.GetSource().GetId(), CaptureEpoch: app.GetCaptureEpoch(), InputUp: true, FrameRate: 30}.Build()}}.Build(), Live: api.LiveSource_builder{SourceId: b.GetSource().GetId(), CaptureEpoch: app.GetCaptureEpoch(), WhepUrl: "https://relay.example/private/whep", ViewToken: "private-capability-token", DateExpires: timestamppb.New(now.Add(30 * time.Second))}.Build(), RecordingComplete: true}.Build()
	d := provision.Deployment{OperationID: "deployment-operation", Request: provision.Request{ID: nativeID(j.GetId()), BindingID: b.GetLogicalKey()}}
	binding := provision.BindingState{BindingID: b.GetLogicalKey(), Alias: b.GetSourceAlias(), SourceID: nativeID(b.GetSource().GetId()), DeploymentID: d.OperationID, DeployedAt: j.GetDatePublished().AsTime()}
	f := &observerExecutionFixture{observations: []*api.CameraWorkerObservation{obs}}
	evidence := mediaprobe.Evidence{CaptureEpoch: app.GetCaptureEpoch(), AccessUnits: 8, DecodedFrames: 8, RTPAdvances: 7, Width: 640, Height: 360, FirstReceivedAt: now.Add(-5 * time.Second), LastReceivedAt: now.Add(-4 * time.Second), FirstDecodedAt: now.Add(-2 * time.Second), LastDecodedAt: now.Add(-time.Second)}
	o := &jobObserver{execution: f, client: func() *http.Client { return http.DefaultClient }, probe: func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
		return evidence, nil
	}, ffmpeg: "configured-ffmpeg", timeout: 50 * time.Millisecond, interval: time.Millisecond}
	return o, f, d, binding, evidence
}

func TestJobObserverWaitsForExactNativeApplicationAndPreservesProgress(t *testing.T) {
	o, f, d, b, evidence := nativeObserverFixture(t)
	ready := f.observations[0]
	pending := proto.Clone(ready).(*api.CameraWorkerObservation)
	pending.GetWork().GetBinding().GetAppliedApplication().GetTarget().SetSpecRevision(6)
	f.observations = []*api.CameraWorkerObservation{pending, ready}
	client := &http.Client{}
	o.client = func() *http.Client { return client }
	probes := 0
	o.probe = func(ctx context.Context, options mediaprobe.Options, endpoint mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
		probes++
		require.Equal(t, 2, f.reads, "stale application must not be probed")
		require.Equal(t, "capture-current", endpoint.ExpectedEpoch)
		require.Equal(t, ready.GetLive().GetWhepUrl(), endpoint.URL)
		require.Equal(t, ready.GetLive().GetViewToken(), endpoint.Token)
		require.Equal(t, "configured-ffmpeg", options.FFmpeg)
		require.Same(t, client, options.HTTPClient)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, !deadline.After(ready.GetLive().GetDateExpires().AsTime()))
		return evidence, nil
	}
	out, err := o.Observe(t.Context(), d, b)
	require.NoError(t, err)
	require.True(t, out.Ingest && out.Live && out.Committed)
	require.False(t, out.CommittedAt.Before(out.DeployedAt))
	require.Equal(t, 1, probes)
	require.Equal(t, 3, f.reads, "successful decode must be followed by a fresh CP observation")
	require.Len(t, f.checkpoints, 1)
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, f.stage, "the portable Store reports Active after this checkpoint")
	checkpoint := f.checkpoints[0]
	prior := ready.GetWork().GetJob().GetObserved()
	require.True(t, proto.Equal(prior.GetProgress(), checkpoint.GetProgress()))
	require.True(t, proto.Equal(prior.GetVerified(), checkpoint.GetVerified()))
	require.True(t, proto.Equal(prior.GetEffective(), checkpoint.GetEffective()))
	require.Equal(t, prior.GetCode(), checkpoint.GetCode())
	require.Equal(t, prior.GetWarnings()[0].GetCode(), checkpoint.GetWarnings()[0].GetCode())
	require.Equal(t, int64(7), checkpoint.GetAppliedRevision())
	require.Equal(t, evidence.CaptureEpoch, checkpoint.GetLiveCaptureEpoch())
	require.Equal(t, int64(evidence.DecodedFrames), checkpoint.GetLiveDecodedFrames())
	require.Equal(t, evidence.FirstDecodedAt, checkpoint.GetLiveFirstFrameAt().AsTime())
	require.Equal(t, evidence.LastDecodedAt, checkpoint.GetLiveLastFrameAt().AsTime())
	require.NotEqual(t, evidence.FirstReceivedAt, checkpoint.GetLiveFirstFrameAt().AsTime())
	require.Zero(t, prior.GetLiveDecodedFrames(), "observation inputs stay detached")
	encoded, err := proto.Marshal(checkpoint)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), ready.GetLive().GetViewToken())
	require.NotContains(t, string(encoded), ready.GetLive().GetWhepUrl())
}

func TestJobObserverRejectsIncompleteOrMismatchedNativeFactsWithoutProbe(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*api.CameraWorkerObservation)
	}{
		{"recording", func(o *api.CameraWorkerObservation) { o.SetRecordingComplete(false) }},
		{"starting-epoch", func(o *api.CameraWorkerObservation) {
			o.GetWork().GetBinding().GetStartingApplication().SetCaptureEpoch("replacement")
		}},
		{"generation", func(o *api.CameraWorkerObservation) { o.GetWork().GetAssignment().SetGeneration(3) }},
		{"applied-revision", func(o *api.CameraWorkerObservation) { o.GetWork().GetBinding().SetAppliedRevision(6) }},
		{"wrong-target-source", func(o *api.CameraWorkerObservation) {
			o.GetWork().GetBinding().GetAppliedApplication().GetTarget().SetSourceId(pdid.New(8).Bytes())
		}},
		{"live-epoch", func(o *api.CameraWorkerObservation) { o.GetLive().SetCaptureEpoch("old") }},
		{"expired-live", func(o *api.CameraWorkerObservation) {
			o.GetLive().SetDateExpires(timestamppb.New(time.Now().Add(-time.Second)))
		}},
		{"old-producer", func(o *api.CameraWorkerObservation) {
			o.GetProducerStatus().SetDateReported(timestamppb.New(time.Now().Add(-3 * time.Minute)))
		}},
		{"wrong-producer-epoch", func(o *api.CameraWorkerObservation) { o.GetProducerStatus().GetSources()[0].SetCaptureEpoch("old") }},
		{"no-input", func(o *api.CameraWorkerObservation) { o.GetProducerStatus().GetSources()[0].SetInputUp(false) }},
		{"no-frame-progress", func(o *api.CameraWorkerObservation) { o.GetProducerStatus().GetSources()[0].SetFrameRate(0) }},
		{"unapplied", func(o *api.CameraWorkerObservation) {
			o.GetWork().GetBinding().GetAppliedApplication().ClearDateApplied()
		}},
		{"no-ack-frames", func(o *api.CameraWorkerObservation) {
			o.GetWork().GetBinding().GetAppliedApplication().GetMedia().SetFrames(0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, f, d, b, _ := nativeObserverFixture(t)
			o.timeout = 4 * time.Millisecond
			tc.edit(f.observations[0])
			o.probe = func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
				t.Fatal("invalid native facts were probed")
				return mediaprobe.Evidence{}, nil
			}
			b.Ingest, b.Live, b.Committed = true, true, true
			out, err := o.Observe(t.Context(), d, b)
			require.ErrorIs(t, err, provision.ErrNotReady)
			require.False(t, out.Ingest || out.Live || out.Committed)
			require.Empty(t, f.checkpoints)
		})
	}
}

func TestJobObserverDiscardsDecodeWhenCaptureChanges(t *testing.T) {
	o, f, d, b, evidence := nativeObserverFixture(t)
	old := f.observations[0]
	next := proto.Clone(old).(*api.CameraWorkerObservation)
	for _, app := range []*api.CameraApplication{next.GetWork().GetBinding().GetStartingApplication(), next.GetWork().GetBinding().GetAppliedApplication()} {
		app.SetCaptureEpoch("capture-replacement")
		app.SetSequence(4)
	}
	next.GetLive().SetCaptureEpoch("capture-replacement")
	next.GetProducerStatus().GetSources()[0].SetCaptureEpoch("capture-replacement")
	f.observations = []*api.CameraWorkerObservation{old, next}
	probes := 0
	o.probe = func(_ context.Context, _ mediaprobe.Options, endpoint mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
		probes++
		require.Empty(t, f.checkpoints)
		e := evidence
		e.CaptureEpoch = endpoint.ExpectedEpoch
		return e, nil
	}
	_, err := o.Observe(t.Context(), d, b)
	require.NoError(t, err)
	require.Equal(t, 2, probes)
	require.Len(t, f.checkpoints, 1)
	require.Equal(t, "capture-replacement", f.checkpoints[0].GetLiveCaptureEpoch())
}

func TestJobObserverRejectsInvalidDecodeAndBindsCancellation(t *testing.T) {
	t.Run("wrong-decoded-epoch", func(t *testing.T) {
		o, f, d, b, evidence := nativeObserverFixture(t)
		o.timeout = 4 * time.Millisecond
		evidence.CaptureEpoch = "old"
		o.probe = func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
			return evidence, nil
		}
		_, err := o.Observe(t.Context(), d, b)
		require.ErrorIs(t, err, provision.ErrNotReady)
		require.Empty(t, f.checkpoints)
	})
	t.Run("probe-canceled", func(t *testing.T) {
		o, f, d, b, _ := nativeObserverFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		o.probe = func(ctx context.Context, _ mediaprobe.Options, _ mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
			cancel()
			<-ctx.Done()
			return mediaprobe.Evidence{}, ctx.Err()
		}
		_, err := o.Observe(ctx, d, b)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, f.checkpoints)
	})
	t.Run("observation-timeout", func(t *testing.T) {
		o, f, d, b, _ := nativeObserverFixture(t)
		o.timeout = 4 * time.Millisecond
		o.probe = func(ctx context.Context, _ mediaprobe.Options, _ mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
			<-ctx.Done()
			return mediaprobe.Evidence{}, ctx.Err()
		}
		_, err := o.Observe(t.Context(), d, b)
		require.ErrorIs(t, err, provision.ErrNotReady)
		require.Empty(t, f.checkpoints)
	})
}

func TestJobObserverPreservesAuthorizationErrorsAndRedactsDiagnostics(t *testing.T) {
	for _, phase := range []string{"observe", "checkpoint", "probe"} {
		t.Run(phase, func(t *testing.T) {
			o, f, d, b, _ := nativeObserverFixture(t)
			injected := status.Error(codes.PermissionDenied, "https://operator:private-secret@camera.example/private-token")
			switch phase {
			case "observe":
				f.observeError = injected
			case "checkpoint":
				f.reportError = injected
			case "probe":
				o.probe = func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
					return mediaprobe.Evidence{}, injected
				}
			}
			_, err := o.Observe(t.Context(), d, b)
			require.ErrorIs(t, err, injected)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.NotContains(t, err.Error(), "private")
			require.NotContains(t, status.Convert(err).Message(), "private")
			require.Nil(t, errors.Unwrap(err))
			require.Empty(t, f.checkpoints)
		})
	}
}

func TestJobObserverDoesNotProbeUnsupportedCodecOrForeignDeployment(t *testing.T) {
	for _, kind := range []string{"codec", "deployment"} {
		t.Run(kind, func(t *testing.T) {
			o, f, d, b, _ := nativeObserverFixture(t)
			want := camera.ErrUnsupported
			if kind == "codec" {
				f.observations[0].GetWork().GetBinding().GetDesired().GetProfile().SetCodec("h265")
			} else {
				d.OperationID = "foreign"
				want = provision.ErrConflict
			}
			o.probe = func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error) {
				t.Fatal("unsupported target was probed")
				return mediaprobe.Evidence{}, nil
			}
			_, err := o.Observe(t.Context(), d, b)
			require.ErrorIs(t, err, want)
			require.False(t, strings.Contains(err.Error(), "private"))
			require.Empty(t, f.checkpoints)
		})
	}
}
