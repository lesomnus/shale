package producer

import (
	"slices"

	catalog "github.com/lesomnus/shale/source"
)

// installSource publishes only a fully constructed incarnation. It does not
// replace an existing capture: that capture must first stop and be retired.
func (p *Producer) installSource(s *source) error {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()
	if _, exists := p.sources[s.cfg.Alias]; exists {
		return catalog.ErrConflict
	}
	for _, other := range p.order {
		if len(s.row.GetId()) > 0 && string(s.row.GetId()) == string(other.row.GetId()) {
			return catalog.ErrConflict
		}
	}
	if p.sources == nil {
		p.sources = make(map[string]*source)
	}
	p.sources[s.cfg.Alias] = s
	p.order = append(p.order, s)
	return nil
}

// activeSources returns a stable list without keeping the registry lock across
// source locks, network calls or process shutdown. A source's immutable capture
// configuration belongs to that incarnation; replacement never rewrites it.
func (p *Producer) activeSources() []*source {
	if p.loading.Load() {
		return nil
	}
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	return slices.Clone(p.order)
}

// retainedSources includes stopped incarnations whose uploader still owns RAM.
// Removing desired collection never hides their bytes from the host budget.
func (p *Producer) retainedSources() []*source {
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	out := slices.Clone(p.order)
	for s := range p.draining {
		out = append(out, s)
	}
	return out
}

func (p *Producer) sourceByAlias(alias string) *source {
	if p.loading.Load() {
		return nil
	}
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	return p.sources[alias]
}

// accountedSources snapshots retained sources and already reclaimed counters
// atomically, so a source finishing its drain is counted once.
func (p *Producer) accountedSources() ([]*source, Stats) {
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	out := slices.Clone(p.order)
	for s := range p.draining {
		out = append(out, s)
	}
	return out, p.retired
}

func (p *Producer) memberCount() int {
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	return p.members
}

// retireSource keeps the entry available to its FIFO uploader and RAM counters,
// while heartbeat, allocation prefetch and live producers stop advertising it.
func (p *Producer) retireSource(s *source) {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()
	if p.sources[s.cfg.Alias] != s {
		return
	}
	delete(p.sources, s.cfg.Alias)
	p.order = slices.DeleteFunc(p.order, func(v *source) bool { return v == s })
	if p.draining == nil {
		p.draining = map[*source]bool{}
	}
	p.draining[s] = true
}

func (p *Producer) forgetDrainedSource(s *source) {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()
	if !p.draining[s] {
		return
	}
	// Only the lifecycle owner calls this after capture and uploader join.
	// Keep lifetime counters while letting the drained incarnation be collected.
	s.mu.Lock()
	p.retired.Stored += s.stored
	p.retired.Lost += s.lost
	p.retired.LostBytes += s.lostBytes
	p.retired.Cut += s.cut
	s.mu.Unlock()
	delete(p.draining, s)
}

func (p *Producer) acceptedCatalog() catalog.Snapshot {
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	return p.initialCatalog.Clone()
}
func (p *Producer) acceptCatalog(v catalog.Snapshot) {
	p.sourcesMu.Lock()
	defer p.sourcesMu.Unlock()
	p.initialCatalog = v.Clone()
}

// stagedSources is used only by initial registration while loading hides the
// not-yet-resolved rows from heartbeat, live and allocation consumers.
func (p *Producer) stagedSources() []*source {
	p.sourcesMu.RLock()
	defer p.sourcesMu.RUnlock()
	return slices.Clone(p.order)
}
