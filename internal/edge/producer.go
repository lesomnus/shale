package edge

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/lesomnus/shale/camera"
	credentialcp "github.com/lesomnus/shale/camera/credential/cp"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/source"
)

func buildProducer(cfg producer.Config, broker *sourceBroker, options *CacheOptions) (runner, error) {
	if broker == nil || len(cfg.Sources) != 0 || cfg.SourceProvider != nil || cfg.AuthProvider != nil || cfg.AuthProviderFactory != nil {
		return nil, ErrInvalidConfig
	}
	factory := cfg.SourceProviderFactory
	cfg.SourceProviderFactory = func(ctx context.Context, s producer.SourceSession) (source.Provider, error) {
		return broker.bind(ctx, s, factory)
	}
	cache := &cacheObserver{next: cfg.ApplyObserver}
	if options != nil {
		cfg.ApplyObserver = cache
	}
	cfg.AuthProviderFactory = func(_ context.Context, s producer.SourceSession) (camera.AuthProvider, error) {
		var streaming *credentialcp.StreamingCache
		if options != nil {
			// This factory runs only after Producer authentication. State belongs
			// to that role, and never uses the Worker's key or the CP's KEK.
			key, err := (pki.Store{Dir: cfg.StateDir}).Key()
			if err != nil {
				return nil, credentialcp.ErrUnavailable
			}
			material, err := pki.EncodeKey(key.Key)
			if err != nil {
				return nil, credentialcp.ErrUnavailable
			}
			defer clear(material)
			directory, err := filepath.Abs(filepath.Join(cfg.StateDir, "streaming-cache"))
			if err != nil {
				return nil, credentialcp.ErrUnavailable
			}
			streaming, err = credentialcp.NewStreamingCache(credentialcp.CacheOptions{Directory: directory, KeyMaterial: material, MaxAge: options.MaxAge, MaxEntries: options.MaxEntries})
			if err != nil {
				return nil, err
			}
			cache.set(streaming)
		}
		return credentialcp.New(s.Conn, credentialcp.Options{Role: credentialcp.Producer, Scope: camera.EnrollmentScope{TenantID: s.TenantID, SiteID: s.SiteID, SetID: s.SetID, ProducerID: s.ProducerID}, Cache: streaming})
	}
	return producer.New(cfg)
}

// The Producer's run owner fences callbacks across incarnations. Cache entries
// additionally require an online lease and this exact immutable source context.
type cacheObserver struct {
	mu    sync.Mutex
	cache *credentialcp.StreamingCache
	next  source.ApplyObserver
}

func (o *cacheObserver) set(c *credentialcp.StreamingCache) { o.mu.Lock(); o.cache = c; o.mu.Unlock() }
func (o *cacheObserver) get() *credentialcp.StreamingCache {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.cache
}
func (o *cacheObserver) Applied(ctx context.Context, p source.AppliedProof) {
	if cache := o.get(); cache != nil && p.UsedCredentialRef != nil {
		s := p.Scope
		scope := camera.EnrollmentScope{TenantID: s.TenantID, SiteID: s.SiteID, SetID: s.SetID, ProducerID: s.ProducerID, NetworkID: s.NetworkID}
		r := p.UsedCredentialRef
		ref := camera.CredentialRef{Provider: r.Provider, ID: r.ID, Version: r.Version, Purpose: camera.AuthPurpose(r.Purpose), Scope: scope, Recoverable: true}
		_ = cache.RecordApplied(ctx, credentialcp.AppliedProof{Source: camera.SourceAuthContext{SourceID: p.SourceID, Revision: p.SpecRevision, Scope: scope, AssignmentID: p.Revision.AssignmentID, AssignmentGeneration: p.Revision.Generation}, Ref: ref, AssignmentID: p.Revision.AssignmentID, AssignmentGeneration: p.Revision.Generation, CaptureEpoch: p.Epoch, FirstMediaAt: p.Media.FirstMediaAt, LastMediaAt: p.Media.LastMediaAt, Frames: p.Media.Frames, Keyframes: p.Media.Keyframes})
	}
	if o.next != nil {
		o.next.Applied(ctx, p)
	}
}
func (o *cacheObserver) Stopped(ctx context.Context, p source.StoppedProof) {
	if cache := o.get(); cache != nil {
		_ = cache.InvalidateSource(p.SourceID)
	}
	if o.next != nil {
		o.next.Stopped(ctx, p)
	}
}
