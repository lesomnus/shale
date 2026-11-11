package edge

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/mediaprobe"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type observationExecution interface {
	Observe(context.Context) (*api.CameraWorkerObservation, error)
	Step(context.Context, func(context.Context, *api.CameraWorkerWork) error) error
	Checkpoint(context.Context, string, api.CameraJobStage, *api.CameraJobObservation) (*api.CameraJob, error)
}

type jobObserver struct {
	execution observationExecution
	client    func() *http.Client
	probe     func(context.Context, mediaprobe.Options, mediaprobe.Endpoint) (mediaprobe.Evidence, error)
	ffmpeg    string
	timeout   time.Duration
	interval  time.Duration
}

func newJobObserver(e *cameraworker.Execution, client func() *http.Client, options CameraOptions) publication.Observer {
	if e == nil {
		return &jobObserver{}
	}
	return &jobObserver{execution: e, client: client, probe: mediaprobe.Probe, ffmpeg: options.FFmpeg, timeout: time.Minute, interval: time.Second}
}

func (o *jobObserver) Observe(ctx context.Context, d provision.Deployment, binding provision.BindingState) (provision.BindingState, error) {
	// A durable publication and previous readiness bits never qualify a new
	// runtime incarnation. Every successful call collects fresh native evidence.
	binding.Ingest, binding.Live, binding.Committed, binding.CommittedAt = false, false, false, time.Time{}
	if o == nil || o.execution == nil || o.client == nil || o.probe == nil || o.timeout <= 0 || o.interval <= 0 {
		return binding, camera.ErrInvalid
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	for ctx.Err() == nil {
		observed, err := o.execution.Observe(ctx)
		if err != nil {
			return binding, observationFailure(parent, ctx, err)
		}
		app, err := readyObservation(observed, d, binding, time.Now())
		if err != nil && !errors.Is(err, provision.ErrNotReady) {
			return binding, err
		}
		if err == nil {
			live := observed.GetLive()
			var evidence mediaprobe.Evidence
			err = o.execution.Step(ctx, func(step context.Context, current *api.CameraWorkerWork) error {
				fresh, err := appliedCapture(current, d, binding, time.Now())
				if err != nil {
					return err
				}
				if !sameCapture(app, fresh) {
					return provision.ErrNotReady
				}
				// Execution.Step binds session revocation and the fresh job lease.
				// The signed live capability may expire sooner than that lease.
				probe, done := context.WithDeadline(step, live.GetDateExpires().AsTime())
				defer done()
				client := o.client()
				if client == nil {
					return provision.ErrNotReady
				}
				evidence, err = o.probe(probe, mediaprobe.Options{FFmpeg: o.ffmpeg, HTTPClient: client, Timeout: 15 * time.Second}, mediaprobe.Endpoint{URL: live.GetWhepUrl(), Token: live.GetViewToken(), ExpectedEpoch: app.GetCaptureEpoch()})
				return err
			})
			if ctx.Err() != nil {
				break
			}
			if err == nil && validDecode(evidence, app, observed.GetWork().GetBinding().GetDesired().GetProfile(), time.Now()) {
				// Applying can change while packets are being received or decoded.
				// Old advancing media cannot attest the replacement incarnation.
				fresh, err := o.execution.Observe(ctx)
				if err != nil {
					return binding, observationFailure(parent, ctx, err)
				}
				current, err := readyObservation(fresh, d, binding, time.Now())
				if err != nil && !errors.Is(err, provision.ErrNotReady) {
					return binding, err
				}
				if err == nil && sameCapture(app, current) {
					checkpoint := fresh.GetWork().GetJob().GetObserved()
					if checkpoint == nil {
						return binding, camera.ErrInvalid
					}
					checkpoint = proto.Clone(checkpoint).(*api.CameraJobObservation)
					checkpoint.SetAppliedRevision(fresh.GetWork().GetJob().GetPublishedRevision())
					checkpoint.SetLiveCaptureEpoch(evidence.CaptureEpoch)
					checkpoint.SetLiveDecodedFrames(int64(evidence.DecodedFrames))
					checkpoint.SetLiveFirstFrameAt(timestamppb.New(evidence.FirstDecodedAt))
					checkpoint.SetLiveLastFrameAt(timestamppb.New(evidence.LastDecodedAt))
					encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(checkpoint)
					if err != nil {
						return binding, camera.ErrInvalid
					}
					key := provision.Key("native-live-observed", string(fresh.GetWork().GetJob().GetId()), string(encoded))
					if _, err := o.execution.Checkpoint(ctx, key, api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, checkpoint); err != nil {
						return binding, observationFailure(parent, ctx, err)
					}
					binding.Ingest, binding.Live, binding.Committed = true, true, true
					// ObserveJob supplies an authoritative completion fact, rather
					// than a Lamina commit timestamp. Record when it was observed.
					binding.CommittedAt = time.Now().UTC()
					return binding, nil
				}
			} else if err != nil && !retryableProbe(err) {
				return binding, observationError{err}
			}
		}
		timer := time.NewTimer(o.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	if parent.Err() != nil {
		return binding, parent.Err()
	}
	return binding, provision.ErrNotReady
}

// appliedCapture duplicates only the native correlation predicates needed
// before probing. CP checks them again when the portable workflow reports Active.
func appliedCapture(w *api.CameraWorkerWork, d provision.Deployment, binding provision.BindingState, now time.Time) (*api.CameraApplication, error) {
	j, b, a := w.GetJob(), w.GetBinding(), w.GetAssignment()
	if d.OperationID == "" || d.OperationID != binding.DeploymentID || binding.BindingID != d.Request.BindingID || b.GetLogicalKey() != binding.BindingID || b.GetSourceAlias() != binding.Alias || binding.SourceID == "" || nativeID(b.GetSource().GetId()) != binding.SourceID || nativeID(j.GetId()) != d.Request.ID {
		return nil, provision.ErrConflict
	}
	desired, app, starting := b.GetDesired(), b.GetAppliedApplication(), b.GetStartingApplication()
	target := app.GetTarget()
	if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY || !validObservationTime(j.GetDatePublished()) || j.GetPublishedRevision() < 1 || !b.GetCollect() || desired.GetRevision() != j.GetPublishedRevision() || b.GetAppliedRevision() != j.GetPublishedRevision() ||
		!validObservationTime(j.GetLeaseExpires()) || !j.GetLeaseExpires().AsTime().After(now) || j.GetAssignmentGeneration() != a.GetGeneration() ||
		app.GetSequence() < 1 || app.GetSequence() != starting.GetSequence() || app.GetCaptureEpoch() == "" || app.GetCaptureEpoch() != starting.GetCaptureEpoch() || app.GetDrainCutoff() != nil || !proto.Equal(target, starting.GetTarget()) ||
		!validObservationTime(app.GetDateStarted()) || !validObservationTime(app.GetDateApplied()) || app.GetDateApplied().AsTime().Before(app.GetDateStarted().AsTime().Add(-5*time.Second)) ||
		target.GetAssignmentGeneration() != a.GetGeneration() || target.GetSpecRevision() != j.GetPublishedRevision() ||
		!bytes.Equal(target.GetAssignmentId(), a.GetId()) || !bytes.Equal(target.GetBindingId(), b.GetId()) || !bytes.Equal(target.GetSourceId(), b.GetSource().GetId()) || !bytes.Equal(app.GetProducerId(), a.GetProducer().GetId()) ||
		!bytes.Equal(desired.GetBindingId(), b.GetId()) || !bytes.Equal(desired.GetSourceId(), b.GetSource().GetId()) {
		return nil, provision.ErrNotReady
	}
	if !strings.EqualFold(desired.GetProfile().GetCodec(), "h264") {
		return nil, camera.ErrUnsupported
	}
	media := app.GetMedia()
	if media.GetFrames() < 2 || media.GetKeyframes() < 1 || !validObservationTime(media.GetFirstMediaAt()) || !validObservationTime(media.GetLastMediaAt()) || !media.GetLastMediaAt().AsTime().After(media.GetFirstMediaAt().AsTime()) {
		return nil, provision.ErrNotReady
	}
	return app, nil
}

func readyObservation(o *api.CameraWorkerObservation, d provision.Deployment, binding provision.BindingState, now time.Time) (*api.CameraApplication, error) {
	app, err := appliedCapture(o.GetWork(), d, binding, now)
	if err != nil {
		return nil, err
	}
	live := o.GetLive()
	if !o.GetRecordingComplete() || !bytes.Equal(live.GetSourceId(), o.GetWork().GetBinding().GetSource().GetId()) || live.GetCaptureEpoch() != app.GetCaptureEpoch() || live.GetWhepUrl() == "" || live.GetViewToken() == "" || !validObservationTime(live.GetDateExpires()) || !live.GetDateExpires().AsTime().After(now) {
		return nil, provision.ErrNotReady
	}
	reported := o.GetProducerStatus().GetDateReported()
	if !validObservationTime(reported) || reported.AsTime().Before(app.GetDateStarted().AsTime().Add(-5*time.Second)) || now.Sub(reported.AsTime()) > 2*time.Minute || reported.AsTime().After(now.Add(5*time.Second)) {
		return nil, provision.ErrNotReady
	}
	for _, report := range o.GetProducerStatus().GetSources() {
		if bytes.Equal(report.GetSourceId(), live.GetSourceId()) && report.GetCaptureEpoch() == app.GetCaptureEpoch() && report.GetInputUp() && report.GetFrameRate() > 0 && !math.IsInf(report.GetFrameRate(), 0) {
			return app, nil
		}
	}
	return nil, provision.ErrNotReady
}

func sameCapture(a, b *api.CameraApplication) bool {
	return a.GetCaptureEpoch() == b.GetCaptureEpoch() && a.GetSequence() == b.GetSequence() && proto.Equal(a.GetTarget(), b.GetTarget())
}

func validDecode(e mediaprobe.Evidence, app *api.CameraApplication, profile *api.CameraCaptureProfile, now time.Time) bool {
	return e.CaptureEpoch == app.GetCaptureEpoch() && e.DecodedFrames >= 2 && e.DecodedFrames <= e.AccessUnits && e.RTPAdvances > 0 && e.Width > 0 && e.Height > 0 && e.Width == int(profile.GetWidth()) && e.Height == int(profile.GetHeight()) &&
		!e.FirstDecodedAt.IsZero() && e.LastDecodedAt.After(e.FirstDecodedAt) && !e.FirstDecodedAt.Before(app.GetDateApplied().AsTime().Add(-5*time.Second)) && !e.LastDecodedAt.After(now.Add(5*time.Second))
}

func validObservationTime(t *timestamppb.Timestamp) bool { return t != nil && t.CheckValid() == nil }

func nativeID(raw []byte) string {
	id, err := pdid.From(raw)
	if err != nil || id.IsZero() {
		return ""
	}
	return id.String()
}

func retryableProbe(err error) bool {
	return errors.Is(err, provision.ErrNotReady) || errors.Is(err, mediaprobe.ErrEpoch) || errors.Is(err, mediaprobe.ErrNegotiation) || errors.Is(err, mediaprobe.ErrMedia) || errors.Is(err, mediaprobe.ErrDecode) || errors.Is(err, context.DeadlineExceeded)
}

func observationFailure(parent, bounded context.Context, err error) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	if bounded.Err() != nil {
		return provision.ErrNotReady
	}
	return observationError{err}
}

// Preserve retry/authorization classification without exposing transport URLs,
// capability tokens or injected decoder diagnostics through formatting/unwrapping.
type observationError struct{ cause error }

func (observationError) Error() string          { return "camera media observation failed" }
func (e observationError) GoString() string     { return e.Error() }
func (e observationError) Is(target error) bool { return errors.Is(e.cause, target) }
func (e observationError) GRPCStatus() *status.Status {
	return status.New(status.Code(e.cause), "camera media observation failed")
}
