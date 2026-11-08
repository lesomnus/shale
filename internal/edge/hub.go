package edge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	"github.com/lesomnus/shale/source"
	sourcecp "github.com/lesomnus/shale/source/cp"
)

// sourceBroker carries hints, never machine authority. Only an authenticated
// Producer factory can attach a provider; a Worker receipt cannot seed it.
type sourceBroker struct {
	mu      sync.Mutex
	current *replicaProvider
}

func newSourceBroker() *sourceBroker { return &sourceBroker{} }

func (b *sourceBroker) bind(ctx context.Context, session producer.SourceSession, factory func(context.Context, producer.SourceSession) (source.Provider, error)) (source.Provider, error) {
	var upstream source.Provider
	var err error
	if factory != nil {
		upstream, err = factory(ctx, session)
	} else {
		upstream, err = sourcecp.New(session.Conn, sourcecp.Options{ProducerID: session.ProducerID, TenantID: session.TenantID, SiteID: session.SiteID, SetID: session.SetID})
	}
	if err != nil {
		return nil, err
	}
	if upstream == nil {
		return nil, source.ErrInvalid
	}
	r := &replicaProvider{upstream: upstream, memory: &source.Memory{}, session: session}
	b.mu.Lock()
	b.current = r
	b.mu.Unlock()
	return r, nil
}

type replicaProvider struct {
	upstream source.Provider
	memory   *source.Memory
	session  producer.SourceSession
}

func (r *replicaProvider) Snapshot(ctx context.Context) (source.Snapshot, error) {
	v, err := r.upstream.Snapshot(ctx)
	if err != nil {
		return source.Snapshot{}, err
	}
	v, err = source.Normalize(v)
	if err != nil {
		return source.Snapshot{}, err
	}
	s := r.session
	if v.Revision.AssignmentID == "" || v.Scope.ProducerID != s.ProducerID || v.Scope.TenantID != s.TenantID || v.Scope.SiteID != s.SiteID || v.Scope.SetID != s.SetID {
		return source.Snapshot{}, source.ErrConflict
	}
	if err = r.memory.Publish(ctx, v); err != nil {
		return source.Snapshot{}, err
	}
	return v, nil
}

// Full CP reads repair lost hints. The bounded memory wait makes local commits
// wake the reader immediately without a per-source queue or background waiter.
func (r *replicaProvider) WaitForChange(ctx context.Context, after source.Revision) (source.Revision, error) {
	for {
		if _, err := r.Snapshot(ctx); err != nil {
			return source.Revision{}, err
		}
		wait, cancel := context.WithTimeout(ctx, time.Second)
		rev, err := r.memory.WaitForChange(wait, after)
		cancel()
		if err == nil {
			return rev, nil
		}
		if ctx.Err() != nil {
			return source.Revision{}, ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return source.Revision{}, err
		}
	}
}

func (b *sourceBroker) refresh(ctx context.Context, d provision.Deployment, receipt publication.Receipt) error {
	b.mu.Lock()
	r := b.current
	b.mu.Unlock()
	if r == nil {
		return source.ErrUnavailable
	}
	next, err := r.Snapshot(ctx)
	if err != nil {
		return err
	}
	s := d.Request.Scope
	if next.Scope != (source.Scope{TenantID: s.TenantID, SiteID: s.SiteID, SetID: s.SetID, ProducerID: s.ProducerID, NetworkID: s.NetworkID}) || next.Revision.AssignmentID != receipt.Revision.AssignmentID {
		return source.ErrConflict
	}
	if next.Revision.Generation < receipt.Revision.Generation || next.Revision.Catalog < receipt.Revision.Catalog {
		return source.ErrStale
	}
	if next.Revision.Catalog == receipt.Revision.Catalog {
		for _, spec := range next.Sources {
			if spec.SourceID == receipt.Binding.SourceID && spec.Alias == receipt.Binding.Alias && spec.Revision == receipt.Revision.Catalog {
				return nil
			}
		}
		return source.ErrConflict
	}
	return nil
}

type notifyingCommitter struct {
	commit publication.Committer
	broker *sourceBroker
}

func (c notifyingCommitter) Commit(ctx context.Context, d provision.Deployment) (publication.Receipt, error) {
	receipt, err := c.commit.Commit(ctx, d)
	if err != nil {
		return receipt, err
	}
	// Publication has committed. A missing colocated Producer or failed refresh
	// must not turn that success into a repeated physical camera operation.
	if c.broker != nil {
		_ = c.broker.refresh(ctx, d, receipt)
	}
	return receipt, nil
}
