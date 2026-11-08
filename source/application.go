package source

import (
	"context"
	"math"
	"time"

	"github.com/lesomnus/payday/pdid"
)

// Application identifies one desired source at one authorized assignment
// generation. Catalog is diagnostic: an unrelated source edit does not replace
// an already running source's incarnation.
type Application struct {
	Scope               Scope
	Revision            Revision
	BindingID, SourceID string
	SpecRevision        int64
}

// ApplyTicket is a server-fenced capture incarnation. Starting an application
// never acknowledges media: only a later proof from that exact new read loop
// may apply it. A process restart always obtains a new ticket.
type ApplyTicket struct {
	Application
	Sequence int64
	Epoch    string
}

func ValidateApplication(a Application) error {
	r, err := normalizeRevision(a.Revision)
	if err != nil || r.AssignmentID == "" || r != a.Revision || a.SpecRevision < 1 || a.SpecRevision > r.Catalog {
		return ErrInvalid
	}
	for _, field := range []struct {
		value  string
		domain pdid.Domain
	}{
		{a.Scope.TenantID, domainTenant}, {a.Scope.SetID, domainSet}, {a.Scope.ProducerID, domainProducer}, {a.Scope.NetworkID, domainNetwork}, {a.BindingID, domainBinding}, {a.SourceID, domainSource},
	} {
		id, err := canonicalID(field.value, field.domain)
		if err != nil || id != field.value {
			return ErrInvalid
		}
	}
	if a.Scope.SiteID != "" {
		id, err := canonicalID(a.Scope.SiteID, domainSite)
		if err != nil || id != a.Scope.SiteID {
			return ErrInvalid
		}
	}
	return nil
}

// MediaProgress describes observed video. Arrival time and media cadence remain
// separate; neither the configured profile nor a running process is evidence.
type MediaProgress struct {
	Codec                     string
	Width, Height             int
	FPS                       float64
	Bitrate                   int64
	KeyframeInterval          time.Duration
	FirstMediaAt, LastMediaAt time.Time
	Frames, Keyframes         int64
}

// AppliedProof contains no endpoint, credential material or worker dependency.
// UsedCredentialRef is the exact lease used by this capture, never a grant.
type AppliedProof struct {
	ApplyTicket
	Media             MediaProgress
	UsedCredentialRef *CredentialRef
}

// ApplyController is the Producer's narrowly authorized execution path, separate
// from provisioning publication. Begin is idempotent for an intent within the
// current starting ticket; old/superseded tickets cannot acknowledge a new run.
type ApplyController interface {
	Begin(context.Context, Application, string) (ApplyTicket, error)
	Acknowledge(context.Context, AppliedProof) error
}

type StopReason string

const (
	StopRemoved      StopReason = "removed"
	StopSuperseded   StopReason = "superseded"
	StopRevoked      StopReason = "revoked"
	StopShutdown     StopReason = "shutdown"
	StopDisconnected StopReason = "disconnected"
)

type StoppedProof struct {
	ApplyTicket
	Reason StopReason
}

// ApplyObserver supports a local, explicitly enabled credential cache without
// tying capture to a concrete provider. Notifications are emitted only by the
// current run owner; a stopped/superseded run cannot re-enable its cache.
type ApplyObserver interface {
	Applied(context.Context, AppliedProof)
	Stopped(context.Context, StoppedProof)
}

// CheckMedia requires observed facts for every declared capture constraint.
// A bitrate is a ceiling, not a target. A bounded burst allowance accounts for
// startup IDRs; a zero/unknown measurement never proves a declared constraint.
func CheckMedia(want Profile, got MediaProgress) error {
	if got.Frames < 2 || got.Frames > 120000 || got.Keyframes < 1 || got.Keyframes > got.Frames || got.FirstMediaAt.IsZero() || got.LastMediaAt.Sub(got.FirstMediaAt) < 500*time.Millisecond || got.LastMediaAt.Sub(got.FirstMediaAt) > time.Minute {
		return ErrInvalid
	}
	if got.Codec == "" || got.Width <= 0 || got.Height <= 0 || math.IsNaN(got.FPS) || math.IsInf(got.FPS, 0) || got.FPS <= 0 || got.Bitrate <= 0 {
		return ErrInvalid
	}
	if want.Codec != "" && want.Codec != got.Codec || want.Width > 0 && want.Width != got.Width || want.Height > 0 && want.Height != got.Height {
		return ErrConflict
	}
	if want.FPS > 0 && math.Abs(got.FPS-want.FPS) > math.Max(0.5, want.FPS*0.05) {
		return ErrConflict
	}
	if want.MaxBitrate > 0 && float64(got.Bitrate) > float64(want.MaxBitrate)*1.25 {
		return ErrConflict
	}
	if want.KeyframeInterval > 0 && (got.KeyframeInterval <= 0 || got.KeyframeInterval > want.KeyframeInterval+want.KeyframeInterval/4+50*time.Millisecond) {
		return ErrConflict
	}
	return nil
}
