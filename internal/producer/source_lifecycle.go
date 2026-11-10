package producer

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/server/core"
	catalog "github.com/lesomnus/shale/source"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sourceRuntime struct {
	stopCapture, stopUpload context.CancelFunc
	captureDone, uploadDone chan struct{}
	stopProof               context.CancelFunc
	proofDone               chan struct{}
}

func (p *Producer) startSource(ctx context.Context, s *source) {
	capture, stopCapture := context.WithCancel(ctx)
	upload, stopUpload := context.WithCancel(ctx)
	proof, stopProof := context.WithCancel(ctx)
	r := &sourceRuntime{stopCapture: stopCapture, stopUpload: stopUpload, stopProof: stopProof, captureDone: make(chan struct{}), uploadDone: make(chan struct{}), proofDone: make(chan struct{})}
	s.runtime = r
	go func() {
		defer close(r.captureDone)
		if err := p.capture(capture, s); err != nil && capture.Err() == nil {
			p.log.Warn("capture ended", "source", s.cfg.Alias, "code", "capture_failed")
		}
	}()
	go func() { defer close(r.uploadDone); _ = p.uploads(upload, s) }()
	go func() { defer close(r.proofDone); p.appliedProofs(proof, s) }()
}

// stopSource always joins the read owner before its final segment is drained.
// The manager never calls Cutter.Stop concurrently with Feed. Upload remains a
// single FIFO for this Source until either its queue empties or the bound wins.
func (p *Producer) stopSource(ctx context.Context, s *source, reason catalog.StopReason) {
	r := s.runtime
	if r == nil {
		return
	}
	p.retireSource(s)
	s.mu.Lock()
	s.stopReason = reason
	s.mu.Unlock()
	r.stopProof()
	r.stopCapture()
	<-r.captureDone
	<-r.proofDone
	deadline := time.NewTimer(p.cfg.DrainTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	drained := false
wait:
	for {
		s.mu.Lock()
		empty := len(s.pending) == 0
		s.mu.Unlock()
		if empty {
			drained = true
			break
		}
		select {
		case <-ctx.Done():
			break wait
		case <-deadline.C:
			break wait
		case <-tick.C:
		}
	}
	r.stopUpload()
	<-r.uploadDone
	if !drained {
		// No capture/uploader can now mutate the FIFO. A single bounded report
		// window covers all old segments rather than adding one timeout per row.
		report, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for {
			s.mu.Lock()
			var seg *Segment
			if len(s.pending) > 0 {
				seg = s.pending[0]
			}
			s.mu.Unlock()
			if seg == nil {
				break
			}
			p.drop(report, s, seg, "source "+string(reason)+": bounded upload drain expired")
		}
	}
	p.log.Info("source stopped", "source", s.cfg.Alias, "reason", string(reason), "drained", drained)
}

// sourceLifecycles is the only writer of capture membership. A failed full read
// is never interpreted as removal. Errors retry against the complete accepted
// desired snapshot; unchanged streams and their upload loops continue running.
func (p *Producer) sourceLifecycles(ctx context.Context) error {
	for p.loading.Load() {
		if ctx.Err() != nil {
			return nil
		}
		if err := p.initializeSources(ctx); err != nil {
			p.log.Warn("source catalog awaiting initial authorized state", "code", status.Code(err).String())
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
	}
	for _, s := range p.activeSources() {
		p.startSource(ctx, s)
	}
	defer func() { p.stopSources(ctx, p.activeSources(), catalog.StopShutdown) }()
	initial := p.acceptedCatalog()
	if initial.Revision.AssignmentID == "" {
		<-ctx.Done()
		return nil
	}
	accepted, err := catalog.NewMemory(initial)
	if err != nil {
		return err
	}
	// reconciled is the snapshot the captures last matched, nil when they
	// may not: before the first reconciliation, after one failed, and after
	// they were stopped outside it. The loop wakes every second; reconciling
	// an unchanged snapshot negotiates with the CP twice and re-authorizes
	// with the relay, once a second for as long as the producer runs.
	var reconciled *catalog.Snapshot
	for {
		wait, cancel := context.WithTimeout(ctx, time.Second)
		_, _ = p.catalog.WaitForChange(wait, p.acceptedCatalog().Revision)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		readConn := p.conn.Load()
		read, cancel := context.WithTimeout(ctx, 10*time.Second)
		next, err := p.catalog.Snapshot(read)
		cancel()
		if err != nil {
			if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unauthenticated {
				if p.renewing.Load() || p.conn.Load() != readConn {
					if !sleep(ctx, 100*time.Millisecond) {
						return nil
					}
					continue
				}
				// Explicit authorization rejection fences capture without pretending
				// the last authorized desired snapshot was successfully empty.
				p.stopSources(ctx, p.activeSources(), catalog.StopRevoked)
				reconciled = nil
			}
			p.log.Warn("source catalog unavailable; retained accepted state", "code", status.Code(err).String())
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		if err := accepted.Publish(ctx, next); err != nil {
			p.log.Warn("source catalog rejected", "code", "catalog_conflict")
			continue
		}
		next, _ = accepted.Snapshot(ctx)
		p.acceptCatalog(next)
		if reconciled != nil && reflect.DeepEqual(*reconciled, next) {
			continue
		}
		if err := p.reconcileSources(ctx, next); err != nil {
			p.log.Warn("source reconciliation waiting", "code", status.Code(err).String())
			reconciled = nil
			continue
		}
		reconciled = &next
	}
}

func sameCapture(s *source, v catalog.Spec, rev catalog.Revision) bool {
	return s.spec != nil && s.catalogRevision.AssignmentID == rev.AssignmentID && s.catalogRevision.Generation == rev.Generation && reflect.DeepEqual(*s.spec, v)
}

func (p *Producer) managedSourceRow(ctx context.Context, s *source) error {
	id, err := identifierSource(s.spec.SourceID)
	if err != nil {
		return err
	}
	row, err := api.NewSourceServiceClient(p.cp()).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: id}.Build()}.Build())
	if err != nil {
		return err
	}
	if err := p.validateCatalogSource(s, row.GetId(), row.GetTenant().GetId(), row.GetSet().GetId(), row.GetAlias()); err != nil {
		return err
	}
	for key, want := range s.cfg.Labels {
		if row.GetLabels()[key] != want {
			return catalog.ErrConflict
		}
	}
	s.row = row
	p.sourcesMu.Lock()
	p.members = max(p.members, int(row.GetOrdinal())+1)
	p.sourcesMu.Unlock()
	return nil
}

func (p *Producer) reconcileSources(ctx context.Context, next catalog.Snapshot) error {
	desired := make(map[string]catalog.Spec, len(next.Sources))
	for _, v := range next.Sources {
		desired[v.SourceID] = v
	}
	active := map[string]*source{}
	var removed []*source
	for _, s := range p.activeSources() {
		if s.spec == nil {
			continue
		}
		if _, ok := desired[s.spec.SourceID]; !ok {
			removed = append(removed, s)
		} else {
			active[s.spec.SourceID] = s
		}
	}
	p.stopSources(ctx, removed, catalog.StopRemoved)
	var proposed []*source
	startable := map[*source]bool{}
	for _, v := range next.Sources {
		if old := active[v.SourceID]; old != nil && sameCapture(old, v, next.Revision) {
			proposed = append(proposed, old)
			continue
		}
		cfg := p.configForSpec(next, v)
		s, err := p.sourceFor(cfg)
		if err != nil {
			if old := active[v.SourceID]; old != nil {
				proposed = append(proposed, old)
				continue
			}
			// Reserve the desired budget but never publish/start an invalid input.
			s = &source{cfg: cfg}
		} else {
			startable[s] = true
		}
		spec := v.Clone()
		s.spec = &spec
		s.catalogRevision = next.Revision
		if err := p.managedSourceRow(ctx, s); err != nil {
			return err
		}
		proposed = append(proposed, s)
	}
	if err := p.negotiateSources(ctx, proposed, next, true); err != nil {
		return err
	}
	// The strict commit can fail without changing any existing capture/peer.
	// Frozen old ticket limits continue governing its allocated/unallocated FIFO.
	if err := p.negotiateSources(ctx, proposed, next, false); err != nil {
		return err
	}
	var replaced []*source
	for _, s := range proposed {
		if startable[s] {
			if old := active[s.spec.SourceID]; old != nil {
				replaced = append(replaced, old)
			}
		}
	}
	p.stopSources(ctx, replaced, catalog.StopSuperseded)
	for _, s := range proposed {
		if startable[s] {
			if old := active[s.spec.SourceID]; old != nil {
				old.mu.Lock()
				s.lastObj = bytes.Clone(old.lastObj)
				old.mu.Unlock()
			}
		}
	}

	// Existing captures can grow their queues during the CP commit and another
	// source's drain. Recheck the entire pending start batch before publishing
	// any new incarnation; merely having one free byte is not segment capacity.
	var reserve int64
	for _, s := range proposed {
		if startable[s] {
			reserve += core.MaxLength(s.profile)
		}
	}
	if reserve > p.cfg.Buffer-p.heldBytes() {
		return status.Error(codes.ResourceExhausted, "managed capture awaits retained RAM drain")
	}
	for _, s := range proposed {
		if !startable[s] {
			continue
		}
		if err := p.installSource(s); err != nil {
			return err
		}
		p.startSource(ctx, s)
	}
	return nil
}

func (p *Producer) initializeSources(ctx context.Context) error {
	p.loading.Store(true)
	if err := p.loadSourceCatalog(ctx); err != nil {
		return err
	}
	if err := p.validateCameraAssignmentSources(p.stagedSources()); err != nil {
		return err
	}
	if err := p.register(ctx); err != nil {
		return err
	}
	if err := p.negotiateSources(ctx, p.stagedSources(), p.acceptedCatalog(), false); err != nil {
		return err
	}
	p.loading.Store(false)
	return nil
}

// Stop all changed captures promptly; their independent FIFO drains proceed
// concurrently, so a stalled camera cannot delay cancellation of every other
// changed source by another full drain timeout.
func (p *Producer) stopSources(ctx context.Context, sources []*source, reason catalog.StopReason) {
	var stops sync.WaitGroup
	for _, s := range sources {
		stops.Go(func() { p.stopSource(ctx, s, reason); p.forgetDrainedSource(s) })
	}
	stops.Wait()
}
