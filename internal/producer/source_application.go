package producer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/fmp4"
	catalog "github.com/lesomnus/shale/source"
	catalogcp "github.com/lesomnus/shale/source/cp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// currentEpoch is read by the synchronous live tee. Asynchronous helper work
// must retain this value in its own immutable tap and reject stale callbacks.
func (s *source) currentEpoch() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ticket.Epoch
}

func (p *Producer) applicationController(scope catalog.Scope) catalog.ApplyController {
	if p.cfg.ApplyController != nil {
		return p.cfg.ApplyController
	}
	return catalogcp.NewApplications(p.cp(), scope)
}

func (p *Producer) beginSourceAttempt(ctx context.Context, s *source) error {
	if s.catalogRevision.AssignmentID == "" {
		return nil
	}
	v := p.acceptedCatalog()
	current := false
	for _, spec := range v.Sources {
		if spec.SourceID == s.spec.SourceID && sameCapture(s, spec, v.Revision) {
			current = true
			break
		}
	}
	if !current || p.sourceByAlias(s.cfg.Alias) != s {
		return catalog.ErrStale
	}
	var intent [16]byte
	if _, err := rand.Read(intent[:]); err != nil {
		return catalog.ErrUnavailable
	}
	app := catalog.Application{Scope: v.Scope, Revision: v.Revision, BindingID: s.spec.BindingID, SourceID: s.spec.SourceID, SpecRevision: s.spec.Revision}
	s.mu.Lock()
	if s.pendingIntent == "" {
		s.pendingIntent = hex.EncodeToString(intent[:])
		s.pendingApplication = app
	}
	intentKey := s.pendingIntent
	app = s.pendingApplication
	s.mu.Unlock()
	ticket, err := p.applicationController(v.Scope).Begin(ctx, app, intentKey)
	if err != nil {
		if status.Code(err) == codes.Aborted {
			s.mu.Lock()
			if s.pendingIntent == intentKey {
				s.pendingIntent = ""
			}
			s.mu.Unlock()
		}
		return providerError(err)
	}
	if ticket.Application != app || ticket.Sequence < 1 || len(ticket.Epoch) != 32 {
		return catalog.ErrInvalid
	}
	if _, err := hex.DecodeString(ticket.Epoch); err != nil {
		return catalog.ErrInvalid
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.sourceByAlias(s.cfg.Alias) != s {
		return catalog.ErrStale
	}
	s.applicationMu.Lock()
	s.mu.Lock()
	s.ticket = ticket
	s.pendingIntent = ""
	s.usedCredential = nil
	s.proof = nil
	s.proofNotified = false
	s.acknowledged = false
	s.attemptEnded = false
	s.stopReason = ""
	s.secBytes = [60]int64{}
	s.secFrames = [60]int64{}
	s.lastTick = time.Time{}
	s.mu.Unlock()
	s.applicationMu.Unlock()
	// Refresh signed per-source grants. Relay outage never prevents archive
	// capture; reliable relay control waits for a matching authenticated grant.
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	if ra, err := api.NewProducerServiceClient(p.cp()).Relay(call, &api.ProducerRelayRequest{}); err == nil {
		p.relay.set(ra.GetRelay())
		p.relay.sourcesChanged()
	}
	cancel()
	p.relay.resetSource(mustId(s.row.GetId()), ticket.Epoch, false)
	return nil
}

func (p *Producer) sourceCredentialUsed(s *source, ref *camera.CredentialRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usedCredential = nil
	if ref != nil {
		s.usedCredential = &catalog.CredentialRef{Provider: ref.Provider, ID: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}
	}
}

func (p *Producer) endSourceAttempt(ctx context.Context, s *source) {
	if s.catalogRevision.AssignmentID == "" {
		return
	}
	s.applicationMu.Lock()
	defer s.applicationMu.Unlock()
	s.mu.Lock()
	ticket := s.ticket
	reason := s.stopReason
	s.attemptEnded = true
	s.proof = nil
	s.mu.Unlock()
	if ticket.Epoch == "" {
		return
	}
	if reason == "" {
		reason = catalog.StopDisconnected
		if ctx.Err() != nil {
			reason = catalog.StopShutdown
		}
	}
	p.relay.resetSource(mustId(s.row.GetId()), ticket.Epoch, true)
	if p.cfg.ApplyObserver != nil {
		call, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		p.cfg.ApplyObserver.Stopped(call, catalog.StoppedProof{ApplyTicket: ticket, Reason: reason})
	}
}

func (p *Producer) appliedProofs(ctx context.Context, s *source) {
	if s.catalogRevision.AssignmentID == "" {
		return
	}
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.proofWake:
		case <-retry.C:
		}
		if ctx.Err() != nil {
			return
		}
		if p.sourceByAlias(s.cfg.Alias) != s {
			continue
		}
		s.applicationMu.Lock()
		s.mu.Lock()
		if s.proof == nil || s.attemptEnded || s.acknowledged {
			s.mu.Unlock()
			s.applicationMu.Unlock()
			continue
		}
		proof := *s.proof
		if proof.UsedCredentialRef != nil {
			ref := *proof.UsedCredentialRef
			proof.UsedCredentialRef = &ref
		}
		notify := !s.proofNotified
		s.proofNotified = true
		s.mu.Unlock()
		if notify && p.cfg.ApplyObserver != nil {
			call, cancel := context.WithTimeout(ctx, 5*time.Second)
			p.cfg.ApplyObserver.Applied(call, proof)
			cancel()
		}
		s.applicationMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		err := p.applicationController(proof.Scope).Acknowledge(ctx, proof)
		if err != nil {
			p.log.Warn("capture acknowledgment waiting", "source", s.cfg.Alias, "code", status.Code(err).String())
			continue
		}
		s.mu.Lock()
		if s.ticket.Epoch == proof.Epoch && !s.attemptEnded {
			s.acknowledged = true
			select {
			case p.heartbeatWake <- struct{}{}:
			default:
			}
		}
		s.mu.Unlock()
	}
}

type captureProgress struct {
	ticket                       catalog.ApplyTicket
	first, last                  time.Time
	firstTick, nextTick, lastKey int64
	ticks, frames, bytes, keys   int64
	keyInterval                  time.Duration
	armed                        bool
}

// observeApplication consumes only this read loop's parsed video. It never
// reads a desired profile as observed dimensions or uses a previous loop's
// counters. Unknown/mismatched samples remain unacknowledged.
func (p *Producer) observeApplication(s *source, progress *captureProgress, f *Frame, init *fmp4.Init) {
	if progress.ticket.Epoch == "" || init == nil || s.spec == nil {
		return
	}
	v := init.Video()
	if f.Kind == FramePrefix || v == nil || v.Timescale == 0 || !f.VideoValid {
		if f.Kind == FramePrefix {
			*progress = captureProgress{ticket: progress.ticket}
		}
		return
	}
	if !progress.armed || f.Ticks != progress.nextTick {
		if !f.IDR {
			return
		}
		*progress = captureProgress{ticket: progress.ticket, armed: true, first: time.Now(), firstTick: f.Ticks, lastKey: f.Ticks}
	}
	if f.IDR {
		if progress.keys > 0 {
			gap := time.Duration(f.Ticks-progress.lastKey) * time.Second / time.Duration(v.Timescale)
			progress.keyInterval = max(progress.keyInterval, gap)
		}
		progress.lastKey = f.Ticks
		progress.keys++
	}
	progress.nextTick = f.Ticks + f.VideoTicks
	progress.ticks += f.VideoTicks
	progress.frames += int64(f.Frames)
	progress.bytes += f.VideoBytes
	progress.last = time.Now()
	seconds := float64(progress.ticks) / float64(v.Timescale)
	if seconds < 2 || progress.last.Sub(progress.first) < 500*time.Millisecond {
		return
	}
	codec := ""
	switch v.Entry {
	case "avc1", "avc3":
		codec = "h264"
	case "hvc1", "hev1":
		codec = "h265"
	}
	media := catalog.MediaProgress{Codec: codec, Width: v.Width, Height: v.Height, FPS: float64(progress.frames) / seconds, Bitrate: int64(float64(progress.bytes) * 8 / seconds), FirstMediaAt: progress.first, LastMediaAt: progress.last, Frames: progress.frames, Keyframes: progress.keys, KeyframeInterval: progress.keyInterval}
	if len(v.ParamSets()) == 0 || catalog.CheckMedia(s.spec.Profile, media) != nil {
		return
	}
	s.mu.Lock()
	if s.ticket.Epoch == progress.ticket.Epoch && !s.attemptEnded && !s.acknowledged {
		proof := catalog.AppliedProof{ApplyTicket: progress.ticket, Media: media, UsedCredentialRef: s.usedCredential}
		if proof.UsedCredentialRef != nil {
			ref := *proof.UsedCredentialRef
			proof.UsedCredentialRef = &ref
		}
		s.proof = &proof
		select {
		case s.proofWake <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
	// Keep a fresh bounded window when the CP has not received an ACK yet.
	if seconds > 5 && f.IDR {
		*progress = captureProgress{ticket: progress.ticket}
	}
}
