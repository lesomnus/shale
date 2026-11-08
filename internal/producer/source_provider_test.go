package producer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	catalog "github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testCatalog struct {
	value catalog.Snapshot
	err   error
}

func (p testCatalog) Snapshot(context.Context) (catalog.Snapshot, error) {
	return p.value.Clone(), p.err
}
func (p testCatalog) WaitForChange(ctx context.Context, _ catalog.Revision) (catalog.Revision, error) {
	<-ctx.Done()
	return catalog.Revision{}, ctx.Err()
}

func catalogRuntime(t *testing.T, cfg Config) (*Producer, catalog.Snapshot) {
	t.Helper()
	cfg.Log = slog.New(slog.DiscardHandler)
	p, err := New(cfg)
	require.NoError(t, err)
	p.id = pdid.New(22)
	tenant, set, site := pdid.New(1), pdid.New(7), pdid.New(19)
	p.set = api.Set_builder{Id: set.Bytes(), Tenant: api.Tenant_builder{Id: tenant.Bytes()}.Build(), Site: api.Site_builder{Id: site.Bytes()}.Build()}.Build()
	v := catalog.Snapshot{
		Revision: catalog.Revision{AssignmentID: pdid.New(28).String(), Generation: 1, Catalog: 1},
		Scope:    catalog.Scope{TenantID: tenant.String(), SiteID: site.String(), SetID: set.String(), ProducerID: p.id.String(), NetworkID: pdid.New(26).String()},
		Sources:  []catalog.Spec{{BindingID: pdid.New(29).String(), SourceID: pdid.New(8).String(), Revision: 1, Alias: "door", Input: "rtsp://192.0.2.10/main", Profile: catalog.Profile{Codec: "h264", Width: 1280, Height: 720, FPS: 29.97, MaxBitrate: 2_000_000}, Labels: map[string]string{"room": "front"}}},
	}
	return p, v
}

func TestSourceCatalogFactoryDeferredAndFractionalCopy(t *testing.T) {
	called := 0
	var value catalog.Snapshot
	p, snapshot := catalogRuntime(t, Config{SourceProviderFactory: func(_ context.Context, session SourceSession) (catalog.Provider, error) {
		called++
		require.NotNil(t, session.Conn)
		require.Equal(t, value.Scope.ProducerID, session.ProducerID)
		require.Equal(t, value.Scope.TenantID, session.TenantID)
		require.Equal(t, value.Scope.SetID, session.SetID)
		require.Equal(t, value.Scope.SiteID, session.SiteID)
		return testCatalog{value: value}, nil
	}})
	value = snapshot
	require.Zero(t, called, "construction cannot resolve an operator or unadopted machine catalog")
	require.Empty(t, p.order)
	require.NoError(t, p.loadSourceCatalog(context.Background()))
	require.Equal(t, 1, called)
	require.Len(t, p.order, 1)
	s := p.order[0]
	require.Equal(t, "copy", s.cfg.Encoder)
	require.Zero(t, s.cfg.Fps, "copy capture must not turn 29.97 into an integer override")
	require.Equal(t, 29.97, p.initialSpecs["door"].Profile.FPS)
	require.Equal(t, int64(2_000_000)+s.cfg.audioShare(), p.startingCeiling(s))
	require.Equal(t, "1280x720", s.cfg.Size)
	require.NoError(t, p.validateCameraAssignment())
	value.Sources[0].Labels["room"] = "caller mutation"
	require.Equal(t, "front", p.initialCatalog.Sources[0].Labels["room"])
	require.Equal(t, "front", s.cfg.Labels["room"])
	rowID, err := identifierSource(snapshot.Sources[0].SourceID)
	require.NoError(t, err)
	require.NoError(t, p.validateCatalogSource(s, rowID, p.set.GetTenant().GetId(), p.set.GetId(), "door"))
	require.ErrorIs(t, p.validateCatalogSource(s, pdid.New(8).Bytes(), p.set.GetTenant().GetId(), p.set.GetId(), "door"), catalog.ErrConflict)
	require.ErrorIs(t, p.validateCatalogSource(s, rowID, pdid.New(1).Bytes(), p.set.GetId(), "door"), catalog.ErrConflict)
	require.ErrorIs(t, p.validateCatalogSource(s, rowID, p.set.GetTenant().GetId(), pdid.New(7).Bytes(), "door"), catalog.ErrConflict)
	require.ErrorIs(t, p.validateCatalogSource(s, rowID, p.set.GetTenant().GetId(), p.set.GetId(), "other"), catalog.ErrConflict)
}

func TestSourceCatalogInitialBoundaries(t *testing.T) {
	for _, field := range []string{"producer", "tenant", "set", "site"} {
		t.Run(field, func(t *testing.T) {
			p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
			switch field {
			case "producer":
				v.Scope.ProducerID = pdid.New(22).String()
			case "tenant":
				v.Scope.TenantID = pdid.New(1).String()
			case "set":
				v.Scope.SetID = pdid.New(7).String()
			case "site":
				v.Scope.SiteID = pdid.New(19).String()
			}
			p.cfg.SourceProvider = testCatalog{value: v}
			require.ErrorIs(t, p.loadSourceCatalog(context.Background()), catalog.ErrConflict)
			require.Empty(t, p.order)
		})
	}
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
	v.Sources, v.Revision.Catalog = nil, 0
	p.cfg.SourceProvider = testCatalog{value: v}
	require.NoError(t, p.loadSourceCatalog(context.Background()))
	require.Empty(t, p.activeSources(), "approved empty catalogs wait without a synthetic camera")
	p.cfg.SourceProvider = testCatalog{err: errors.New("private credential endpoint")}
	err := p.loadSourceCatalog(context.Background())
	require.ErrorIs(t, err, catalog.ErrUnavailable)
	require.NotContains(t, err.Error(), "private credential")
	p.cfg.SourceProvider = testCatalog{err: context.Canceled}
	require.ErrorIs(t, p.loadSourceCatalog(context.Background()), context.Canceled)
	p.cfg.SourceProvider = testCatalog{err: status.Error(codes.PermissionDenied, "private credential endpoint")}
	err = p.loadSourceCatalog(context.Background())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "authorization failures retain a safe actionable status")
	require.NotContains(t, err.Error(), "private credential")
	p.cfg.SourceProvider = nil
	p.cfg.SourceProviderFactory = func(context.Context, SourceSession) (catalog.Provider, error) { return nil, nil }
	require.ErrorIs(t, p.loadSourceCatalog(context.Background()), catalog.ErrInvalid)
}

func TestSourceCatalogConfigurationIsExplicit(t *testing.T) {
	provider, err := catalog.NewStatic([]catalog.Spec{{Alias: "door", Input: "rtsp://192.0.2.10/main"}})
	require.NoError(t, err)
	factory := func(context.Context, SourceSession) (catalog.Provider, error) { return provider, nil }
	for _, cfg := range []Config{
		{SourceProvider: provider, SourceProviderFactory: factory},
		{SourceProvider: provider, Sources: []SourceConfig{{Alias: "local", Input: "v4l2:/dev/video0"}}},
		{SourceProviderFactory: factory, Sources: []SourceConfig{{Alias: "local", Input: "v4l2:/dev/video0"}}},
	} {
		_, err := New(cfg)
		require.Error(t, err)
	}
}

func TestSourceCatalogReferenceIsNotAuthority(t *testing.T) {
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
	v.Sources[0].Credential = &catalog.CredentialRef{Provider: "cp", ID: "stream", Version: "v1", Purpose: "streaming"}
	p.cfg.SourceProvider = testCatalog{value: v}
	require.ErrorIs(t, p.loadSourceCatalog(context.Background()), camera.ErrAuthRequired, "a reference cannot create an AuthProvider")
}

type sourceAuthProbe struct {
	seen   []camera.SourceAuthContext
	events []string
}

func (p *sourceAuthProbe) Acquire(context.Context, camera.AuthRequest) (camera.CredentialLease, error) {
	p.events = append(p.events, "acquire")
	return nil, camera.ErrAuthRequired
}
func (p *sourceAuthProbe) ForSource(s camera.SourceAuthContext) camera.AuthProvider {
	p.seen = append(p.seen, s)
	p.events = append(p.events, "scope")
	return p
}

func TestCredentialFactoryOpensAfterAdoptionAndBindsExactSource(t *testing.T) {
	opened := 0
	auth := &sourceAuthProbe{}
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}, AuthProviderFactory: func(_ context.Context, session SourceSession) (camera.AuthProvider, error) {
		opened++
		require.NotEmpty(t, session.ProducerID)
		require.NotEmpty(t, session.SetID)
		require.NotNil(t, session.Conn)
		return auth, nil
	}})
	require.Zero(t, opened)
	v.Sources[0].Credential = &catalog.CredentialRef{Provider: "cp", ID: pdid.New(32).String(), Version: "v1", Purpose: "streaming"}
	p.cfg.SourceProvider = testCatalog{value: v}
	require.NoError(t, p.loadSourceCatalog(t.Context()))
	require.Equal(t, 1, opened)
	require.Empty(t, auth.seen, "construction does not bind a still-incomplete capture context")
	_ = p.sourceAuth(p.stagedSources()[0])
	require.NotEmpty(t, auth.seen)
	require.Equal(t, v.Sources[0].SourceID, auth.seen[len(auth.seen)-1].SourceID)
	require.Equal(t, v.Sources[0].Revision, auth.seen[len(auth.seen)-1].Revision)
	require.Equal(t, p.order[0].cfg.Scope, auth.seen[len(auth.seen)-1].Scope)
	_, err := New(Config{Sources: []SourceConfig{{Alias: "usb", Input: "v4l2:/dev/video0"}}, AuthProvider: auth, AuthProviderFactory: func(context.Context, SourceSession) (camera.AuthProvider, error) { return auth, nil }})
	require.ErrorIs(t, err, camera.ErrInvalid)
}

func TestSourceCatalogHiddenInitialRowsRetainManagedSpec(t *testing.T) {
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
	p.cfg.SourceProvider = testCatalog{value: v}
	p.loading.Store(true)
	require.NoError(t, p.loadSourceCatalog(context.Background()))
	require.Empty(t, p.activeSources(), "incomplete rows stay hidden from heartbeat and live")
	staged := p.stagedSources()
	require.Len(t, staged, 1)
	require.NotNil(t, staged[0].spec, "hidden initial capture still carries managed execution identity")
	require.Equal(t, v.Revision, staged[0].catalogRevision)
	require.Equal(t, v.Sources[0], *staged[0].spec)
	p.loading.Store(false)
	require.True(t, sameCapture(p.activeSources()[0], v.Sources[0], v.Revision), "first reconciliation reuses the initialized incarnation instead of adding a duplicate alias")
}

func TestCredentialScopeUsesImmutableIncarnationInsteadOfInitialAlias(t *testing.T) {
	auth := &sourceAuthProbe{}
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}, AuthProvider: auth})
	p.cfg.SourceProvider = testCatalog{value: v}
	require.NoError(t, p.loadSourceCatalog(t.Context()))
	next := v.Clone()
	next.Revision.Generation++
	next.Revision.Catalog++
	next.Sources[0].SourceID = pdid.New(8).String()
	next.Sources[0].Revision++
	s, err := p.sourceFor(p.configForSpec(next, next.Sources[0]))
	require.NoError(t, err)
	require.Empty(t, auth.seen)
	s.spec = &next.Sources[0]
	s.catalogRevision = next.Revision
	_ = p.sourceAuth(s)
	require.Len(t, auth.seen, 1)
	require.Equal(t, next.Sources[0].SourceID, auth.seen[0].SourceID)
	require.Equal(t, next.Sources[0].Revision, auth.seen[0].Revision)
	require.Equal(t, next.Revision.AssignmentID, auth.seen[0].AssignmentID)
	require.Equal(t, next.Revision.Generation, auth.seen[0].AssignmentGeneration)
	require.Equal(t, s.cfg.Scope, auth.seen[0].Scope)
}

func TestCredentialScopeIsOpenedAfterBeginBeforeEveryAcquire(t *testing.T) {
	auth := &sourceAuthProbe{}
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}, AuthProvider: auth})
	v.Sources[0].Credential = &catalog.CredentialRef{Provider: "cp", ID: pdid.New(32).String(), Version: "v1", Purpose: "streaming"}
	p.cfg.SourceProvider = testCatalog{value: v}
	require.NoError(t, p.loadSourceCatalog(t.Context()))
	s := p.stagedSources()[0]
	s.capture.BeforeAttempt = func(context.Context) error {
		auth.events = append(auth.events, "begin")
		return nil
	}
	for range 2 {
		err := s.capture.once(t.Context(), func(_ io.Reader) { t.Error("unauthorized input opened") })
		require.ErrorIs(t, err, camera.ErrAuthRequired)
	}
	require.Equal(t, []string{"begin", "scope", "acquire", "begin", "scope", "acquire"}, auth.events)
	require.Len(t, auth.seen, 2)
}
