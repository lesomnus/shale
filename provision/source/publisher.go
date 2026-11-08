// Package source connects durable camera publication to a Producer's optional
// in-process source catalog. The catalog never becomes the publication authority.
package source

import (
	"context"
	"errors"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	catalog "github.com/lesomnus/shale/source"
)

// Receipt acknowledges a committed, replayable publication. Binding.BindingID
// is the workflow's logical key, whereas SourceID is the real CP Source PDID.
// A Committer must durably retain OperationID before returning success; a lost
// response is retried with the same Deployment.OperationID, never a new mutation.
type Receipt struct {
	Binding  provision.BindingState
	Revision catalog.Revision
}

type Committer interface {
	Commit(context.Context, provision.Deployment) (Receipt, error)
}

type Observer interface {
	Observe(context.Context, provision.Deployment, provision.BindingState) (provision.BindingState, error)
}

// RefreshFailure contains only a fixed classification, never a transport error,
// endpoint, credential or deployment. Hooks may be called concurrently.
type RefreshFailure string

const (
	RefreshUnavailable RefreshFailure = "unavailable"
	RefreshCanceled    RefreshFailure = "canceled"
	RefreshInvalid     RefreshFailure = "invalid"
	RefreshConflict    RefreshFailure = "conflict"
	RefreshStale       RefreshFailure = "stale"
)

type Publisher struct {
	AssignmentID string
	Scope        catalog.Scope
	Committer    Committer
	// Provider uses a separate Producer-authenticated capability. A Worker
	// receipt does not authorize reading or constructing the Producer's catalog.
	// Provider and Memory may both be nil for independent commit-only workers.
	Provider catalog.Provider
	Memory   *catalog.Memory
	Observer Observer
	// OnRefreshFailure reports a committed publication whose local notification
	// needs retrying. It must not be used as evidence of a failed camera mutation.
	OnRefreshFailure func(RefreshFailure)
}

var _ provision.SourcePublisher = (*Publisher)(nil)

func (p *Publisher) expected() (catalog.Snapshot, error) {
	if p == nil || p.AssignmentID == "" || (p.Provider == nil) != (p.Memory == nil) {
		return catalog.Snapshot{}, catalog.ErrInvalid
	}
	return catalog.Normalize(catalog.Snapshot{
		Revision: catalog.Revision{AssignmentID: p.AssignmentID, Generation: 1}, Scope: p.Scope,
	})
}

func (p *Publisher) deployment(d provision.Deployment) (catalog.Snapshot, string, error) {
	expected, err := p.expected()
	if err != nil {
		return catalog.Snapshot{}, "", err
	}
	if d.OperationID == "" || d.Request.BindingID == "" {
		return catalog.Snapshot{}, "", catalog.ErrInvalid
	}
	actual, err := catalog.Normalize(catalog.Snapshot{Revision: expected.Revision, Scope: catalog.Scope{
		TenantID: d.Request.Scope.TenantID, SiteID: d.Request.Scope.SiteID,
		SetID: d.Request.Scope.SetID, ProducerID: d.Request.Scope.ProducerID, NetworkID: d.Request.Scope.NetworkID,
	}})
	if err != nil {
		return catalog.Snapshot{}, "", err
	}
	if actual.Scope != expected.Scope {
		return catalog.Snapshot{}, "", camera.ErrForbidden
	}
	alias := d.Previous.Alias
	if alias == "" {
		alias = d.Request.Alias
	}
	if alias == "" || (d.Previous.BindingID != "" && d.Previous.BindingID != d.Request.BindingID) {
		return catalog.Snapshot{}, "", catalog.ErrInvalid
	}
	return expected, alias, nil
}

func binding(d provision.Deployment, alias string, b provision.BindingState) error {
	id, err := pdid.Parse(b.SourceID)
	if err != nil || id.IsZero() || id.Domain() != 8 || id.String() != b.SourceID {
		return catalog.ErrInvalid
	}
	if b.BindingID != d.Request.BindingID || b.Alias != alias || b.DeploymentID != d.OperationID || b.DeployedAt.IsZero() {
		return catalog.ErrConflict
	}
	if d.Previous.SourceID != "" && b.SourceID != d.Previous.SourceID {
		return catalog.ErrConflict
	}
	return nil
}

// Publish commits first. Refreshing local memory is only an optimization: its
// failure, cancellation or stale read cannot turn a durable success into a
// camera-operation retry. Refresh or periodic resnapshot repairs a lost hint.
func (p *Publisher) Publish(ctx context.Context, d provision.Deployment) (provision.BindingState, error) {
	if err := ctx.Err(); err != nil {
		return provision.BindingState{}, err
	}
	expected, alias, err := p.deployment(d)
	if err != nil {
		return provision.BindingState{}, err
	}
	if p.Committer == nil {
		return provision.BindingState{}, catalog.ErrInvalid
	}
	receipt, err := p.Committer.Commit(ctx, d)
	if err != nil {
		return provision.BindingState{}, redactedError{cause: err}
	}
	if err = binding(d, alias, receipt.Binding); err != nil {
		return provision.BindingState{}, err
	}
	version, err := catalog.Normalize(catalog.Snapshot{Revision: receipt.Revision, Scope: expected.Scope})
	if err != nil || version.Revision.AssignmentID != expected.Revision.AssignmentID || version.Revision.Catalog < 1 {
		return provision.BindingState{}, catalog.ErrInvalid
	}
	receipt.Revision = version.Revision
	out := receipt.Binding
	// Only the Observer can attest current media. A commit acknowledgment is
	// neither an applied configuration nor fresh ingest/live/recording evidence.
	out.Ingest, out.Live, out.Committed, out.CommittedAt = false, false, false, time.Time{}
	if p.Provider != nil {
		if err = p.refresh(ctx, expected, &receipt); err != nil {
			p.refreshFailed(err)
		}
	}
	return out, nil
}

// Refresh reads and validates the complete authorized Producer catalog. It does
// not repeat a publication or a device write, and errors preserve accepted memory.
func (p *Publisher) Refresh(ctx context.Context) error {
	expected, err := p.expected()
	if err == nil {
		err = p.refresh(ctx, expected, nil)
	}
	if err != nil && p != nil {
		p.refreshFailed(err)
	}
	return err
}

func (p *Publisher) refresh(ctx context.Context, expected catalog.Snapshot, receipt *Receipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Provider == nil || p.Memory == nil {
		return catalog.ErrUnavailable
	}
	next, err := p.Provider.Snapshot(ctx)
	if err != nil {
		return redactedError{cause: errors.Join(catalog.ErrUnavailable, err)}
	}
	next, err = catalog.Normalize(next)
	if err != nil {
		return err
	}
	if next.Scope != expected.Scope || next.Revision.AssignmentID != expected.Revision.AssignmentID {
		return catalog.ErrConflict
	}
	if receipt != nil {
		if next.Revision.Generation < receipt.Revision.Generation || next.Revision.Catalog < receipt.Revision.Catalog {
			return catalog.ErrStale
		}
		// At this exact revision the committed Source must be present. A later
		// authoritative catalog may already have changed or stopped that Source.
		if next.Revision.Catalog == receipt.Revision.Catalog {
			found := false
			for _, spec := range next.Sources {
				if spec.SourceID == receipt.Binding.SourceID && spec.Alias == receipt.Binding.Alias && spec.Revision == receipt.Revision.Catalog {
					found = true
					break
				}
			}
			if !found {
				return catalog.ErrConflict
			}
		}
	}
	return p.Memory.Publish(ctx, next)
}

func (p *Publisher) refreshFailed(err error) {
	if p.OnRefreshFailure == nil {
		return
	}
	code := RefreshUnavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = RefreshCanceled
	case errors.Is(err, catalog.ErrInvalid):
		code = RefreshInvalid
	case errors.Is(err, catalog.ErrConflict):
		code = RefreshConflict
	case errors.Is(err, catalog.ErrStale):
		code = RefreshStale
	}
	p.OnRefreshFailure(code)
}

func (p *Publisher) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	_, alias, err := p.deployment(d)
	if err != nil {
		return b, err
	}
	if err = binding(d, alias, b); err != nil {
		return b, err
	}
	if p.Observer == nil {
		return b, provision.ErrNotReady
	}
	out, err := p.Observer.Observe(ctx, d, b)
	if err != nil {
		// Readiness observers may report partial progress together with
		// ErrNotReady. Keep it only when it still names this deployment.
		if binding(d, alias, out) == nil && out.SourceID == b.SourceID {
			return out, redactedError{cause: err}
		}
		return b, redactedError{cause: err}
	}
	if err = binding(d, alias, out); err != nil || out.SourceID != b.SourceID {
		return b, catalog.ErrConflict
	}
	return out, nil
}

// Preserve errors.Is classification without exposing a wrapped diagnostic to
// Unwrap, logs or formatting. Injected transports may include authenticated URIs.
type redactedError struct{ cause error }

func (e redactedError) Error() string        { return "source publication unavailable" }
func (e redactedError) GoString() string     { return e.Error() }
func (e redactedError) Is(target error) bool { return errors.Is(e.cause, target) }
