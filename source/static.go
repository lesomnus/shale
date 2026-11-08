package source

import "context"

// Static exposes normalized file/manual intent without sharing the private
// capture settings or AuthProvider owned by the compatibility adapter.
type Static struct{ snapshot *Snapshot }

var _ Provider = (*Static)(nil)

// NewStatic preserves source order and aliases. It does not invent Source IDs:
// the normal Producer registration path still assigns legacy Sources.
func NewStatic(specs []Spec) (*Static, error) {
	snapshot := (Snapshot{Revision: Revision{Catalog: 1}, Sources: specs}).Clone()
	for i := range snapshot.Sources {
		snapshot.Sources[i].Revision = 1
	}
	v, err := Normalize(snapshot)
	if err != nil {
		return nil, err
	}
	return &Static{snapshot: &v}, nil
}

func (p *Static) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if p.snapshot == nil {
		return Snapshot{}, ErrUnavailable
	}
	return p.snapshot.Clone(), nil
}

func (p *Static) WaitForChange(ctx context.Context, after Revision) (Revision, error) {
	if err := ctx.Err(); err != nil {
		return Revision{}, err
	}
	if p.snapshot == nil {
		return Revision{}, ErrUnavailable
	}
	var err error
	after, err = normalizeRevision(after)
	if err != nil {
		return Revision{}, err
	}
	if err := compare(after, p.snapshot.Revision); err != nil {
		return Revision{}, err
	}
	if after != p.snapshot.Revision {
		return p.snapshot.Revision, nil
	}
	<-ctx.Done()
	return Revision{}, ctx.Err()
}
