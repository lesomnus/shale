package source

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
)

// Memory is an atomic in-process catalog. It is not durable authority: a
// CP-managed publisher must commit centrally before accepting that snapshot
// here. A process restart reconstructs it from the durable provider.
// Its zero value is unavailable, never an implicitly successful empty catalog.
type Memory struct {
	mu       sync.Mutex
	snapshot *Snapshot
	payload  [32]byte
	changed  chan struct{}
}

var _ Provider = (*Memory)(nil)

func NewMemory(initial Snapshot) (*Memory, error) {
	p := &Memory{}
	if err := p.Publish(context.Background(), initial); err != nil {
		return nil, err
	}
	return p, nil
}

// Publish atomically accepts a full snapshot. Notifications coalesce; slow
// consumers never block the publisher or retain a queue of obsolete snapshots.
// Rejected input leaves both accepted state and its notification unchanged.
func (p *Memory) Publish(ctx context.Context, next Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v, err := Normalize(next)
	if err != nil {
		return err
	}
	// Exclude assignment-generation counters from the payload comparison. A
	// fenced assignment may advance without changing an existing source list.
	b, err := json.Marshal(struct {
		Scope   Scope
		Sources []Spec
	}{v.Scope, v.Sources})
	if err != nil {
		return invalid("capture content")
	}
	hash := sha256.Sum256(b)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if old := p.snapshot; old != nil {
		if old.Scope != v.Scope {
			return ErrConflict
		}
		if err := compare(old.Revision, v.Revision); err != nil {
			return err
		}
		if old.Revision.Catalog == v.Revision.Catalog && p.payload != hash {
			return ErrConflict
		}
		if old.Revision == v.Revision {
			return nil
		}
	}
	p.snapshot, p.payload = &v, hash
	if p.changed != nil {
		close(p.changed)
	}
	p.changed = make(chan struct{})
	return nil
}

func (p *Memory) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if p.snapshot == nil {
		return Snapshot{}, ErrUnavailable
	}
	return p.snapshot.Clone(), nil
}

func (p *Memory) WaitForChange(ctx context.Context, after Revision) (Revision, error) {
	if err := ctx.Err(); err != nil {
		return Revision{}, err
	}
	var err error
	after, err = normalizeRevision(after)
	if err != nil {
		return Revision{}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return Revision{}, err
		}
		p.mu.Lock()
		if p.snapshot == nil {
			p.mu.Unlock()
			return Revision{}, ErrUnavailable
		}
		current, changed := p.snapshot.Revision, p.changed
		p.mu.Unlock()
		if err := compare(after, current); err != nil {
			return Revision{}, err
		}
		if current != after {
			return current, nil
		}
		select {
		case <-ctx.Done():
			return Revision{}, ctx.Err()
		case <-changed:
		}
	}
}
