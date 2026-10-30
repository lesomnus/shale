package e2e_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/sso/ssotest"
	"github.com/lesomnus/shale/internal/token"
)

// exportRecording stores the small real capture through the ordinary
// allocation/upload/commit path. No producer loop or ffmpeg is needed.
func exportRecording(t *testing.T, c *cluster, admin *grpc.ClientConn) (*api.Source, *api.LaminaExportRequest) {
	t.Helper()
	ctx := context.Background()
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "export-recordings"}.Build())
	require.NoError(t, err)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Site: api.SiteRef_builder{Id: site.GetId()}.Build(), Alias: "export-camera",
	}.Build())
	require.NoError(t, err)
	src, err := api.NewSourceServiceClient(admin).Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: "door",
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)
	laminae := api.NewLaminaServiceClient(admin)
	al, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-time.Hour).Truncate(time.Second)),
	}.Build())
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join("..", "fmp4", "testdata", "av.mp4"))
	require.NoError(t, err)
	from := al.GetDateStarted().AsTime()
	to := from.Add(6 * time.Second)
	require.Equal(t, http.StatusCreated, put(t, al, body, to))
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		return err == nil && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 10*time.Second, 100*time.Millisecond, "the recording committed")

	return src, api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build(), From: timestamppb.New(from), To: timestamppb.New(to)}.Build()
}

func exportHTTPBase(t *testing.T, c *cluster, surface cmd.Surface) string {
	t.Helper()
	var addr string
	require.Eventually(t, func() bool {
		addr = c.running.CP.HttpAddr(surface)
		return addr != ""
	}, 10*time.Second, 50*time.Millisecond)

	return "http://" + addr
}

func exportDownload(t *testing.T, base string, out *api.LaminaExportResponse) []byte {
	t.Helper()
	u, err := url.Parse(out.GetUrl())
	require.NoError(t, err)
	require.Equal(t, "/exports/recording.mp4", u.Path)
	// A native download has only the short-lived URL: no app credential,
	// session cookie, reader certificate or JavaScript media buffer.
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(base + out.GetUrl())
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	require.Equal(t, "video/mp4", resp.Header.Get("Content-Type"))
	require.Contains(t, resp.Header.Get("Content-Disposition"), "attachment;")
	require.Equal(t, int64(len(body)), resp.ContentLength)
	boxes, err := fmp4.Children(body)
	require.NoError(t, err)
	require.Len(t, boxes, 3)
	require.Equal(t, "ftyp", boxes[0].Type)
	require.Equal(t, "mdat", boxes[1].Type)
	require.Equal(t, "moov", boxes[2].Type, "one normal MP4, its completed sample tables at the tail")

	return body
}

func TestNativeExportDownloadAndWall(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	src, ex := exportRecording(t, c, admin)
	base := exportHTTPBase(t, c, cmd.SurfaceTenant)
	viewer, err := c.running.CP.Identity.AddPerson(ctx, "acme", "export-viewer", "Export viewer")
	require.NoError(t, err)
	// Provision its Shale row before adding the site's explicit member.
	_, err = api.NewHolderServiceClient(c.dial("@acme/export-viewer")).Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: viewer.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Site: api.SiteRef_builder{Id: src.GetSite().GetId()}.Build(), Holder: api.HolderRef_builder{Id: viewer.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	out, err := api.NewLaminaServiceClient(c.dial("@acme/export-viewer")).Export(ctx, ex)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(5*time.Minute), out.GetDateExpires().AsTime(), 5*time.Second)
	exportDownload(t, base, out)

	// A joined/adopted reader may issue a browser capability too.
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR("export-reader")
	require.NoError(t, err)
	anonymous, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer anonymous.Close()
	joined, err := api.NewReaderServiceClient(anonymous).Join(ctx, api.ReaderJoinRequest_builder{
		Tenant: "acme", Host: api.HostJoin_builder{HardwareId: "export-reader-id", Hostname: "export-reader", Csr: csr, KeyFingerprint: key.Fingerprint()}.Build(),
	}.Build())
	require.NoError(t, err)
	reader, err := api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: joined.GetAnswer().GetId()}.Build()}.Build())
	require.NoError(t, err)
	readerID, err := pdid.From(reader.GetId())
	require.NoError(t, err)
	out, err = api.NewLaminaServiceClient(c.dial(readerID.String())).Export(ctx, ex)
	require.NoError(t, err)
	exportDownload(t, base, out)

	// The site's second wall applies as well as the tenant's. This
	// ordinary viewer has no membership in the private site.
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "private-export"}.Build())
	require.NoError(t, err)
	privateSet, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Site: api.SiteRef_builder{Id: site.GetId()}.Build(), Alias: "private-export"}.Build())
	require.NoError(t, err)
	privateSource, err := api.NewSourceServiceClient(admin).Add(ctx, api.SourceAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Set: api.SetRef_builder{Id: privateSet.GetId()}.Build(), Alias: "private-camera"}.Build())
	require.NoError(t, err)
	private := proto.CloneOf(ex)
	private.SetSource(api.SourceRef_builder{Id: privateSource.GetId()}.Build())
	_, err = api.NewLaminaServiceClient(c.dial("@acme/export-viewer")).Export(ctx, private)
	require.Equal(t, codes.NotFound, status.Code(err))

	// Another tenant's actual source exists but is invisible to this
	// tenant API even when its identifier is known.
	foreignConn := c.dial("@cluster/ops")
	foreignSet, err := api.NewSetServiceClient(foreignConn).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("cluster")}.Build(), Alias: "foreign-export"}.Build())
	require.NoError(t, err)
	foreign, err := api.NewSourceServiceClient(foreignConn).Add(ctx, api.SourceAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("cluster")}.Build(), Set: api.SetRef_builder{Id: foreignSet.GetId()}.Build(), Alias: "foreign-camera"}.Build())
	require.NoError(t, err)
	bad := proto.CloneOf(ex)
	bad.SetSource(api.SourceRef_builder{Id: foreign.GetId()}.Build())
	_, err = api.NewLaminaServiceClient(admin).Export(ctx, bad)
	require.Equal(t, codes.NotFound, status.Code(err))

	// Export media is absent on the separate operator HTTP surface.
	resp, err := http.Get(exportHTTPBase(t, c, cmd.SurfaceCluster) + out.GetUrl())
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	resp, err = http.Get(base + out.GetUrl() + "x")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "a tampered capability cannot download")
	require.NotEmpty(t, src.GetId())
}

// A finalized partial upload can have no stored end timestamp. Timeline
// selects it via IS NULL even after its estimated end falls outside the query.
// Exercise the real SQL, layer stack and HTTP route beside a complete upload.
func TestNativeExportIgnoresStaleIncompleteRecording(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	src, ex := exportRecording(t, c, admin)
	laminae := api.NewLaminaServiceClient(admin)
	oldStart := ex.GetFrom().AsTime().Add(-time.Hour)
	al, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(oldStart),
	}.Build())
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join("..", "fmp4", "testdata", "av.mp4"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, put(t, al, body, oldStart.Add(6*time.Second)))
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		return err == nil && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 10*time.Second, 100*time.Millisecond)
	// Model a historical incomplete finalization directly, rather than
	// introducing a timed producer crash into this download regression.
	id, err := pdid.From(al.GetLaminaId())
	require.NoError(t, err)
	_, err = c.running.CP.Deps.Ent.Lamina.UpdateOneId(id.Uuid()).ClearDateEnded().SetIncomplete(true).Save(ctx)
	require.NoError(t, err)
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{Source: ex.GetSource(), From: ex.GetFrom(), To: ex.GetTo()}.Build())
	require.NoError(t, err)
	stale := false
	complete := false
	for _, source := range tl.GetSources() {
		for _, row := range source.GetLaminae() {
			if string(row.GetLaminaId()) == string(al.GetLaminaId()) {
				stale = true
				require.True(t, row.GetIncomplete())
				require.True(t, row.GetEndedEstimated())
				require.True(t, row.GetDateEnded().AsTime().Before(ex.GetFrom().AsTime()))
			} else if !row.GetIncomplete() {
				complete = true
			}
		}
	}
	require.True(t, stale, "the real Timeline includes the historical incomplete row")
	require.True(t, complete, "a complete recording exists inside the requested window")
	out, err := laminae.Export(ctx, ex)
	require.NoError(t, err)
	exportDownload(t, exportHTTPBase(t, c, cmd.SurfaceTenant), out)
}

func TestNativeExportDisabled(t *testing.T) {
	c := start(t)
	_, err := api.NewLaminaServiceClient(c.dial("@acme/admin")).Export(context.Background(), api.LaminaExportRequest_builder{}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	resp, err := http.Get(exportHTTPBase(t, c, cmd.SurfaceTenant) + "/exports/recording.mp4?token=anything")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDelegatedNativeExport(t *testing.T) {
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	c := start(t, func(c *cmd.Config) {
		c.Export.Enabled = true
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second, Reads: identity.ReadsGranted}
		c.Auth.Oidc.Issuer, c.Auth.Oidc.ClientId, c.Auth.Oidc.ClientSecret = idp.URL, "shale", "s3cret"
		c.Auth.Delegation = cmd.DelegationConfig{Audience: shaleApi, Clients: map[string]string{portalClient: "portal"}, Persons: cmd.DelegationPersonsTenant, TokenTtl: 2 * time.Minute}
	})
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	src, ex := exportRecording(t, c, admin)
	base := exportHTTPBase(t, c, cmd.SurfaceTenant)
	id := c.running.CP.Identity
	bob, err := id.AddPerson(ctx, "acme", "export-bob", "Bob")
	require.NoError(t, err)
	portal, err := id.AddPerson(ctx, "acme", "portal", "Portal")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "export-app", []string{"/roster.DelegationService/Exchange"}, portal.Id)
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "record-through-app", []string{api.DelegationService_Live_FullMethodName, api.DelegationService_Timeline_FullMethodName}, portal.Id)
	require.NoError(t, err)
	personToken := func() string {
		now := time.Now()
		return idp.Sign(t, map[string]any{"iss": idp.URL, "sub": bob.Id.String(), "aud": []string{shaleApi}, "client_id": portalClient, "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix()})
	}
	exchange := func() context.Context {
		rd, err := id.Exchange(ctx, "acme", "portal", []string{"/shale.DelegationService/*"})
		require.NoError(t, err)
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+rd)
	}
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ds := api.NewDelegationServiceClient(conn)
	var started *api.DelegationStartResponse
	windowFrom, windowTo := ex.GetFrom().AsTime().Add(2*time.Second), ex.GetTo().AsTime().Add(-time.Second)
	require.Eventually(t, func() bool {
		started, err = ds.Start(exchange(), api.DelegationStartRequest_builder{
			AccessToken: personToken(), Recordings: true, Sources: []*api.SourceRef{ex.GetSource()},
			RecordingsFrom: timestamppb.New(windowFrom), RecordingsTo: timestamppb.New(windowTo),
		}.Build())
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "the app is granted recordings")
	_, err = api.NewLaminaServiceClient(c.dial("@acme/export-bob")).Export(ctx, ex)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "Bob has no direct read or export grant")
	out, err := ds.Export(exchange(), api.DelegationExportRequest_builder{Handle: started.GetHandle(), Export: ex}.Build())
	require.NoError(t, err, "EveryPerson can export through an app without a direct read grant")
	u, err := url.Parse(out.GetUrl())
	require.NoError(t, err)
	claims, err := token.Parse(u.Query().Get("token"))
	require.NoError(t, err)
	require.Equal(t, bob.Id.Bytes(), claims.GetActor())
	require.Equal(t, portal.Id.Bytes(), claims.GetDelegator())
	require.Equal(t, src.GetId(), claims.GetSource())
	require.Equal(t, windowFrom, claims.GetExportFrom().AsTime())
	require.Equal(t, windowTo, claims.GetExportTo().AsTime(), "the capability contains the narrowed recording window")
	exportDownload(t, base, out)

	liveOnly, err := ds.Start(exchange(), api.DelegationStartRequest_builder{AccessToken: personToken(), Live: true}.Build())
	require.NoError(t, err)
	_, err = ds.Export(exchange(), api.DelegationExportRequest_builder{Handle: liveOnly.GetHandle(), Export: ex}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	outOfWindow := proto.CloneOf(ex)
	outOfWindow.SetFrom(timestamppb.New(windowTo.Add(time.Minute)))
	outOfWindow.SetTo(timestamppb.New(windowTo.Add(2 * time.Minute)))
	_, err = ds.Export(exchange(), api.DelegationExportRequest_builder{Handle: started.GetHandle(), Export: outOfWindow}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	other, err := api.NewSourceServiceClient(admin).Add(ctx, api.SourceAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Set: api.SetRef_builder{Id: src.GetSet().GetId()}.Build(), Alias: "another-camera"}.Build())
	require.NoError(t, err)
	wrongSource := proto.CloneOf(ex)
	wrongSource.SetSource(api.SourceRef_builder{Id: other.GetId()}.Build())
	_, err = ds.Export(exchange(), api.DelegationExportRequest_builder{Handle: started.GetHandle(), Export: wrongSource}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another camera is outside the handle's scope")
	_, err = ds.Export(ctx, api.DelegationExportRequest_builder{Handle: started.GetHandle(), Export: ex}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "the handle alone cannot issue a ticket")
	_, err = ds.Revoke(exchange(), api.DelegationRevokeRequest_builder{Handle: started.GetHandle()}.Build())
	require.NoError(t, err)
	_, err = ds.Export(exchange(), api.DelegationExportRequest_builder{Handle: started.GetHandle(), Export: ex}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "revocation stops new download capabilities")
}
