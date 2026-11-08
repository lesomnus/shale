package producer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	catalog "github.com/lesomnus/shale/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SourceSession is supplied only after host authentication and Set assignment.
// Conn follows certificate-renewal replacements for the lifetime of the Producer.
type SourceSession struct {
	Conn                                grpc.ClientConnInterface
	ProducerID, TenantID, SiteID, SetID string
}

// ErrInitialCatalogEmpty is retained for source compatibility. Managed catalogs
// may now be empty while an approved Producer waits for enrollment.
var ErrInitialCatalogEmpty = errors.New("initial source catalog is empty; idle source reconciliation is not enabled")

func identifierSource(value string) ([]byte, error) {
	id, err := pdid.Parse(value)
	if err != nil || id.Domain() != 8 {
		return nil, catalog.ErrInvalid
	}
	return id.Bytes(), nil
}

func (p *Producer) sourceSession() SourceSession {
	s := SourceSession{Conn: p.cp(), ProducerID: p.id.String(), TenantID: mustId(p.set.GetTenant().GetId()).String(), SetID: mustId(p.set.GetId()).String()}
	if len(p.set.GetSite().GetId()) > 0 {
		s.SiteID = mustId(p.set.GetSite().GetId()).String()
	}
	return s
}

func (p *Producer) loadSourceCatalog(ctx context.Context) error {
	session := p.sourceSession()
	if p.cfg.AuthProviderFactory != nil {
		provider, err := p.cfg.AuthProviderFactory(ctx, session)
		if err != nil {
			return providerError(err)
		}
		if provider == nil {
			return camera.ErrAuthRequired
		}
		p.cfg.AuthProvider = provider
		p.cfg.AuthProviderFactory = nil
	}
	provider := p.cfg.SourceProvider
	if p.cfg.SourceProviderFactory != nil {
		provider = p.factoryProvider
		if provider == nil {
			var err error
			provider, err = p.cfg.SourceProviderFactory(ctx, session)
			if err != nil {
				return providerError(err)
			}
		}
		if provider == nil {
			return catalog.ErrInvalid
		}
		p.factoryProvider = provider
	}
	var local map[string]SourceConfig
	if provider == nil {
		var err error
		provider, local, p.cfg.AuthProvider, err = legacyCatalog(p.cfg.Sources, session, p.cfg.AuthProvider)
		if err != nil {
			return err
		}
	}
	v, err := provider.Snapshot(ctx)
	if err != nil {
		return providerError(err)
	}
	v, err = catalog.Normalize(v)
	if err != nil {
		return err
	}
	managed := v.Revision.AssignmentID != ""
	if managed && (v.Scope.ProducerID != session.ProducerID || v.Scope.TenantID != session.TenantID || v.Scope.SetID != session.SetID || v.Scope.SiteID != session.SiteID) {
		return catalog.ErrConflict
	}
	if len(v.Sources) == 0 && !managed {
		return ErrInitialCatalogEmpty
	}
	configs := make([]SourceConfig, 0, len(v.Sources))
	p.initialSpecs = make(map[string]catalog.Spec, len(v.Sources))
	for _, spec := range v.Sources {
		cfg, ok := local[spec.Alias]
		if !ok {
			cfg = p.configForSpec(v, spec)
		}
		configs = append(configs, cfg)
		p.initialSpecs[spec.Alias] = spec.Clone()
	}
	p.catalog = provider
	p.acceptCatalog(v)
	// The original local URI material is now held only by the private provider;
	// local commands/USB settings remain a private compatibility sidecar.
	p.cfg.Sources = configs
	if err := p.configureSources(configs); err != nil {
		return err
	}
	for _, s := range p.stagedSources() {
		spec := p.initialSpecs[s.cfg.Alias].Clone()
		s.spec = &spec
		s.catalogRevision = v.Revision
	}
	return nil
}

// configForSpec preserves the exact managed profile privately for validation.
// ffmpeg copy never receives a rounded frame-rate override.
func (p *Producer) configForSpec(v catalog.Snapshot, spec catalog.Spec) SourceConfig {
	session := p.sourceSession()
	cfg := SourceConfig{Alias: spec.Alias, Name: spec.Name, Input: spec.Input, Format: spec.Profile.Codec, Zone: spec.Zone, Labels: spec.Labels, KeyframeInterval: spec.Profile.KeyframeInterval}
	if spec.Profile.Width > 0 && spec.Profile.Height > 0 {
		cfg.Size = fmt.Sprintf("%dx%d", spec.Profile.Width, spec.Profile.Height)
	}
	if v.Revision.AssignmentID != "" {
		cfg.Encoder = "copy"
		// Copy preserves fractional input cadence. The complete exact
		// profile stays in initialSpecs; do not truncate it into Fps.
		cfg.MaxBitrate = spec.Profile.MaxBitrate + cfg.audioShare()
	} else {
		if spec.Profile.FPS > 0 && math.Trunc(spec.Profile.FPS) == spec.Profile.FPS {
			cfg.Fps = int(spec.Profile.FPS)
		}
		cfg.MaxBitrate = spec.Profile.MaxBitrate
	}
	cfg.Scope = camera.EnrollmentScope{TenantID: session.TenantID, SiteID: session.SiteID, SetID: session.SetID, ProducerID: session.ProducerID, NetworkID: v.Scope.NetworkID}
	if cfg.Scope.NetworkID == "" {
		cfg.Scope.NetworkID = "local"
	}
	if ref := spec.Credential; ref != nil {
		cfg.InputCredential = &camera.CredentialRef{Provider: ref.Provider, ID: ref.ID, Version: ref.Version, Scope: cfg.Scope, Purpose: camera.AuthPurpose(ref.Purpose), Recoverable: true}
		// The AuthProvider still proves exact version/scope and recovery.
		// Merely calling a provider "cp" never authorizes this reference.
	}
	return cfg
}

func (p *Producer) sourceAuth(s *source) camera.AuthProvider {
	provider := p.cfg.AuthProvider
	if scoped, ok := provider.(camera.SourceAuthProvider); ok {
		value := camera.SourceAuthContext{Scope: s.cfg.Scope}
		if s.spec != nil {
			// The startup alias map may describe a different source/revision
			// after a live update. Each incarnation owns its immutable intent.
			value.SourceID, value.Revision = s.spec.SourceID, s.spec.Revision
			value.AssignmentID, value.AssignmentGeneration = s.catalogRevision.AssignmentID, s.catalogRevision.Generation
		} else {
			initial := p.acceptedCatalog()
			value.AssignmentID, value.AssignmentGeneration = initial.Revision.AssignmentID, initial.Revision.Generation
			if spec, ok := p.initialSpecs[s.cfg.Alias]; ok {
				value.SourceID, value.Revision = spec.SourceID, spec.Revision
			}
		}
		if value.SourceID == "" && s.row != nil {
			value.SourceID = mustId(s.row.GetId()).String()
		}
		return scoped.ForSource(value)
	}
	return provider
}

func providerError(err error) error {
	for _, safe := range []error{context.Canceled, context.DeadlineExceeded, catalog.ErrInvalid, catalog.ErrConflict, catalog.ErrStale, catalog.ErrUnavailable} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	if code := status.Code(err); code != codes.Unknown {
		return status.Error(code, "source catalog request failed")
	}
	return catalog.ErrUnavailable
}

func (p *Producer) validateCatalogSource(s *source, rowID, tenantID, setID []byte, alias string) error {
	var spec catalog.Spec
	ok := s.spec != nil
	if ok {
		spec = *s.spec
	} else {
		spec, ok = p.initialSpecs[s.cfg.Alias]
	}
	if !ok || spec.SourceID == "" {
		return nil
	}
	id, err := identifierSource(spec.SourceID)
	if err != nil {
		return err
	}
	if !bytes.Equal(id, rowID) || !bytes.Equal(tenantID, p.set.GetTenant().GetId()) || !bytes.Equal(setID, p.set.GetId()) || alias != s.cfg.Alias {
		return catalog.ErrConflict
	}
	return nil
}
