package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	catalog "github.com/lesomnus/shale/source"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// producerApplicationTx is deliberately private to these custom operations.
// Generic inventory/job RPCs gain no Producer rights, including at Site=NULL.
func (s Core) producerApplicationTx(ctx context.Context, target *api.CameraApplyTarget, fn func(context.Context, api.Server, *api.CameraAssignment, *api.CameraBinding) error) error {
	f, err := actor(ctx)
	if err != nil {
		return err
	}
	if kindOf(f.Actor) != DomProducer {
		return denied("camera application", "only the assigned Producer may acknowledge capture")
	}
	if err := cameraApplyTarget(target); err != nil {
		return err
	}
	return s.ownTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, err := c.producerCatalogRow(ctx, api.CameraAssignmentRef_builder{Id: target.GetAssignmentId()}.Build(), f.Actor, f.Tenant)
		if err != nil {
			return err
		}
		p, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !s.d.Dev {
			prior, ok := f.Row.(*api.Producer)
			if !ok || prior.GetCertSerial() == "" || prior.GetCertSerial() != p.GetCertSerial() || p.GetDateCertExpires() == nil || !p.GetDateCertExpires().AsTime().After(s.d.now()) {
				return denied("camera application", "Producer certificate expired or was revoked")
			}
		}
		b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: target.GetBindingId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(b.GetAssignment().GetId(), a.GetId()) || !bytes.Equal(b.GetSource().GetId(), target.GetSourceId()) || !bytes.Equal(b.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(b.GetSite().GetId(), a.GetSite().GetId()) {
			return denied("camera application", "binding does not belong to this assignment")
		}
		if target.GetAssignmentGeneration() != a.GetGeneration() || target.GetCatalogRevision() > a.GetCatalogRevision() || !b.GetCollect() || b.GetDesired() == nil || target.GetSpecRevision() != b.GetDesired().GetRevision() {
			return status.Error(codes.Aborted, "camera capture intent changed")
		}
		if !bytes.Equal(b.GetDesired().GetBindingId(), b.GetId()) || !bytes.Equal(b.GetDesired().GetSourceId(), b.GetSource().GetId()) {
			return failed("published capture identity is inconsistent")
		}
		return fn(ctx, nx, a, b)
	})
}

func cameraApplyTarget(t *api.CameraApplyTarget) error {
	if t == nil || t.GetAssignmentGeneration() < 1 || t.GetSpecRevision() < 1 || t.GetCatalogRevision() < t.GetSpecRevision() {
		return invalid("application", "complete current capture identity is required")
	}
	for _, field := range []struct {
		raw    []byte
		domain pdid.Domain
	}{{t.GetAssignmentId(), DomCameraAssignment}, {t.GetBindingId(), DomCameraBinding}, {t.GetSourceId(), DomSource}} {
		id, err := pdid.From(field.raw)
		if err != nil || id.Domain() != field.domain {
			return invalid("application", "invalid capture identity")
		}
	}
	return nil
}

func (s coreCameraAssignment) Application(ctx context.Context, r *api.CameraApplicationRequest) (*api.CameraApplyState, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	var out *api.CameraApplyState
	err := s.producerApplicationTx(ctx, r.GetTarget(), func(ctx context.Context, nx api.Server, a *api.CameraAssignment, b *api.CameraBinding) error {
		out = api.CameraApplyState_builder{Starting: b.GetStartingApplication(), Applied: b.GetAppliedApplication()}.Build()
		return nil
	})
	return out, err
}

func (s coreCameraAssignment) BeginApply(ctx context.Context, r *api.CameraBeginApplyRequest) (*api.CameraApplication, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if err := cameraToken("intent_key", r.GetIntentKey()); err != nil {
		return nil, err
	}
	if r.GetExpectedSequence() < 0 || r.GetExpectedSequence() == math.MaxInt64 {
		return nil, invalid("expected_sequence", "invalid capture sequence")
	}
	var out *api.CameraApplication
	err := s.producerApplicationTx(ctx, r.GetTarget(), func(ctx context.Context, nx api.Server, a *api.CameraAssignment, b *api.CameraBinding) error {
		prior := b.GetStartingApplication()
		if prior.GetIntentKey() == r.GetIntentKey() {
			if prior.GetDrainCutoff() != nil {
				return status.Error(codes.Aborted, "capture starting intent has ended")
			}
			if !proto.Equal(prior.GetTarget(), r.GetTarget()) {
				return status.Error(codes.Aborted, "capture intent key was reused")
			}
			out = prior
			return nil
		}
		if r.GetExpectedSequence() != prior.GetSequence() || r.GetTarget().GetCatalogRevision() != a.GetCatalogRevision() {
			return status.Error(codes.Aborted, "capture starting sequence or catalog changed")
		}
		for _, old := range b.GetPreviousApplications() {
			if old.GetIntentKey() == r.GetIntentKey() {
				return status.Error(codes.Aborted, "capture starting intent was superseded")
			}
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return status.Error(codes.Internal, "cannot allocate capture epoch")
		}
		source, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if source.GetProfile().GetMaxBitrate() <= 0 || source.GetProfile().GetDurationSeconds() <= 0 {
			return failed("managed capture must negotiate its storage profile before BeginApply")
		}
		app := api.CameraApplication_builder{Target: r.GetTarget(), Sequence: prior.GetSequence() + 1, CaptureEpoch: hex.EncodeToString(nonce[:]), IntentKey: r.GetIntentKey(), DateStarted: timestamppb.New(s.d.now()), ProducerId: a.GetProducer().GetId(), SegmentProfile: source.GetProfile()}.Build()
		next, err := nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), StartingApplication: app, AppliedApplication: cameraDrainApplication(b.GetAppliedApplication(), s.d.now()), PreviousApplications: cameraDrainHistory(b, s.d.now()), DateUpdated: b.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		out = next.GetStartingApplication()
		return nil
	})
	return out, err
}

func cameraRuntimeProgress(m *api.CameraRuntimeMedia) catalog.MediaProgress {
	p := m.GetProfile()
	return catalog.MediaProgress{Codec: p.GetCodec(), Width: int(p.GetWidth()), Height: int(p.GetHeight()), FPS: p.GetFps(), Bitrate: p.GetMaxBitrateBps(), KeyframeInterval: time.Duration(p.GetKeyframeIntervalMs()) * time.Millisecond, FirstMediaAt: m.GetFirstMediaAt().AsTime(), LastMediaAt: m.GetLastMediaAt().AsTime(), Frames: m.GetFrames(), Keyframes: m.GetKeyframes()}
}
func cameraCatalogProfile(p *api.CameraCaptureProfile) catalog.Profile {
	return catalog.Profile{Codec: p.GetCodec(), Width: int(p.GetWidth()), Height: int(p.GetHeight()), FPS: p.GetFps(), MaxBitrate: p.GetMaxBitrateBps(), KeyframeInterval: time.Duration(p.GetKeyframeIntervalMs()) * time.Millisecond}
}

func (s coreCameraAssignment) Applied(ctx context.Context, r *api.CameraAppliedRequest) (*api.CameraApplication, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if r.GetSequence() < 1 || len(r.GetCaptureEpoch()) != 32 {
		return nil, invalid("application", "valid capture ticket is required")
	}
	if _, err := hex.DecodeString(r.GetCaptureEpoch()); err != nil {
		return nil, invalid("application", "invalid capture epoch")
	}
	if err := cameraProfile(r.GetMedia().GetProfile(), true); err != nil {
		return nil, err
	}
	if r.GetMedia().GetProfile().GetKeyframeIntervalMs() > time.Minute.Milliseconds() {
		return nil, invalid("media", "keyframe observation exceeds bounded capture window")
	}
	if r.GetMedia().GetFirstMediaAt() == nil || r.GetMedia().GetLastMediaAt() == nil || r.GetMedia().GetFirstMediaAt().CheckValid() != nil || r.GetMedia().GetLastMediaAt().CheckValid() != nil {
		return nil, invalid("media", "valid capture progress dates are required")
	}
	if err := cameraCredential(r.GetUsedCredential(), "streaming"); err != nil {
		return nil, err
	}
	var out *api.CameraApplication
	err := s.producerApplicationTx(ctx, r.GetTarget(), func(ctx context.Context, nx api.Server, a *api.CameraAssignment, b *api.CameraBinding) error {
		starting := b.GetStartingApplication()
		if starting.GetDrainCutoff() != nil {
			return status.Error(codes.Aborted, "capture starting intent has ended")
		}
		if starting.GetSequence() != r.GetSequence() || starting.GetCaptureEpoch() != r.GetCaptureEpoch() || !proto.Equal(starting.GetTarget(), r.GetTarget()) || !bytes.Equal(starting.GetProducerId(), a.GetProducer().GetId()) {
			return status.Error(codes.Aborted, "capture starting ticket was superseded")
		}
		if applied := b.GetAppliedApplication(); applied.GetSequence() == r.GetSequence() && applied.GetCaptureEpoch() == r.GetCaptureEpoch() {
			out = applied
			return nil
		}
		if !proto.Equal(b.GetDesired().GetCredential(), r.GetUsedCredential()) {
			return denied("capture credential", "acknowledgment does not name the credential used by this intent")
		}
		progress := cameraRuntimeProgress(r.GetMedia())
		now := s.d.now()
		if progress.FirstMediaAt.Before(starting.GetDateStarted().AsTime().Add(-5*time.Second)) || progress.LastMediaAt.After(now.Add(5*time.Second)) || progress.LastMediaAt.Before(now.Add(-time.Minute)) {
			return invalid("media", "capture evidence is outside its current starting window")
		}
		if err := catalog.CheckMedia(cameraCatalogProfile(b.GetDesired().GetProfile()), progress); err != nil {
			return failed("observed capture does not satisfy its exact published profile")
		}
		applied := proto.Clone(starting).(*api.CameraApplication)
		applied.SetDateApplied(timestamppb.New(now))
		applied.SetMedia(r.GetMedia())
		applied.SetUsedCredential(r.GetUsedCredential())
		rev := r.GetTarget().GetSpecRevision()
		next, err := nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), AppliedApplication: applied, AppliedRevision: &rev, DateUpdated: b.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		out = next.GetAppliedApplication()
		return nil
	})
	return out, err
}
