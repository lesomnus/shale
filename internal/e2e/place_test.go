package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/protobuf-patch/patchpb"
	"github.com/lesomnus/protobuf-patch/patchproto"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/server/core"
)

// looseRecording is a set in no site with one camera and a recording of it
// stored through the ordinary allocation, upload and commit.
func looseRecording(t *testing.T, admin *grpc.ClientConn, alias string) (*api.SetRef, *api.Source, *api.LaminaExportRequest) {
	t.Helper()
	ctx := context.Background()
	set := setIn(t, ctx, admin, alias, nil)
	src, err := api.NewSourceServiceClient(admin).Add(ctx, api.SourceAddRequest_builder{
		Set: set, Alias: alias + "-cam", Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
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
	require.Equal(t, 201, put(t, al, body, to))
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		return err == nil && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 10*time.Second, 100*time.Millisecond, "the recording committed")

	return set, src, api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build(), From: timestamppb.New(from), To: timestamppb.New(to)}.Build()
}

func placeSet(ctx context.Context, conn *grpc.ClientConn, set *api.SetRef, site *api.SiteRef, dry bool) (*api.SetPlaceResponse, error) {
	return api.NewSetServiceClient(conn).Place(ctx, api.SetPlaceRequest_builder{Set: set, Site: site, DryRun: dry}.Build())
}

// rowSites is the site of the set's source and of each of its laminae, as
// whoever sees every site reads them.
func rowSites(t *testing.T, ctx context.Context, admin *grpc.ClientConn, src *api.Source) []string {
	t.Helper()
	v, err := api.NewSourceServiceClient(admin).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.NoError(t, err)
	out := []string{string(v.GetSite().GetId())}
	ls, err := api.NewLaminaServiceClient(admin).List(ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{
		api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build(),
	}}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, ls.GetItems())
	for _, l := range ls.GetItems() {
		out = append(out, string(l.GetSite().GetId()))
	}

	return out
}

func allAre(vs []string, v string) bool {
	return !slices.ContainsFunc(vs, func(s string) bool { return s != v })
}

// A set that exists is put into a site with SetService/Place (§7), and
// with it everything it ever recorded: whoever sees the site sees the set,
// its camera, its timeline, Live and an export of a recording made before
// the move, and a set beside it is untouched. Moved on into a MEMBERS
// site, they no longer do and a member does; a key's list that named the
// site it left no longer reaches it, and one that named its camera still
// does. Only whoever sees every site may; no site, another tenant's and an
// erased one are refused; a set placed where it is writes nothing; a dry
// run counts what moves and moves nothing; the trail has the call; and
// the command line says what moved.
func TestPlaceASet(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Export.Enabled = true
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	admin := c.dial("@acme/admin")
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()
	sites := api.NewSiteServiceClient(admin)

	yard, yardCam, ex := looseRecording(t, admin, "yard")
	barn, barnCam, _ := looseRecording(t, admin, "barn")
	hall := siteOf(t, ctx, admin, "hall", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	vault := siteOf(t, ctx, admin, "vault", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	gone := siteOf(t, ctx, admin, "gone", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	_, err := sites.Erase(ctx, gone)
	require.NoError(t, err)

	// Bob may write sets and sees the sites everybody sees; that is not
	// every site.
	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "placer", []string{api.SetService_Place_FullMethodName, "/shale.SetService/*"}, bob.Id)
	require.NoError(t, err)
	asBob := c.dial("@acme/bob")
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, yard)), "in no site, only whoever sees every site sees it")

	// Refused, each writing nothing.
	_, err = placeSet(ctx, asBob, yard, hall, false)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "somebody who sees only some sites: %v", err)
	require.Contains(t, status.Convert(err).Message(), "every site")
	_, err = placeSet(ctx, admin, yard, nil, false)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "no site: %v", err)
	_, err = placeSet(ctx, admin, yard, gone, false)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "an erased site: %v", err)
	boss, _, err := id.Seed(ctx, "globex", "boss")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "globex", identity.OperatorRole, []string{identity.Everything}, boss.Id)
	require.NoError(t, err)
	theirs, err := api.NewSiteServiceClient(c.dial("@globex/boss")).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("globex")}.Build(), Alias: "hq", Visibility: z.Ptr(api.SiteVisibility_SITE_VISIBILITY_EVERYBODY),
	}.Build())
	require.NoError(t, err)
	_, err = placeSet(ctx, admin, yard, api.SiteRef_builder{Id: theirs.GetId()}.Build(), false)
	require.Equal(t, codes.NotFound, status.Code(err), "another tenant's site: %v", err)
	require.True(t, allAre(rowSites(t, ctx, admin, yardCam), ""), "nothing moved")

	// A dry run counts and moves nothing.
	dry, err := placeSet(ctx, admin, yard, hall, true)
	require.NoError(t, err)
	require.True(t, dry.GetDryRun())
	require.Equal(t, int64(1), dry.GetMoved().GetSources())
	require.Equal(t, int64(1), dry.GetMoved().GetLaminae())
	require.Positive(t, dry.GetMoved().GetAttempts())
	require.Empty(t, dry.GetSet().GetSite().GetId())
	require.True(t, allAre(rowSites(t, ctx, admin, yardCam), ""), "a dry run moves nothing")

	// Placed: every row of the set carries the site, and the set beside it
	// none.
	got, err := placeSet(ctx, admin, yard, hall, false)
	require.NoError(t, err)
	require.False(t, got.GetDryRun())
	require.Equal(t, dry.GetMoved().GetSources(), got.GetMoved().GetSources())
	require.Equal(t, dry.GetMoved().GetLaminae(), got.GetMoved().GetLaminae())
	require.Equal(t, dry.GetMoved().GetAttempts(), got.GetMoved().GetAttempts())
	require.Equal(t, hall.GetId(), got.GetSet().GetSite().GetId())
	require.Empty(t, got.GetFromSiteId())
	require.True(t, allAre(rowSites(t, ctx, admin, yardCam), string(hall.GetId())))
	require.True(t, allAre(rowSites(t, ctx, admin, barnCam), ""), "another set's rows are untouched")

	// Bob sees all of it, the recording made before the move among it.
	require.Eventually(t, func() bool { return setSeen(ctx, asBob, yard) == nil }, sitesSeenWithin, 200*time.Millisecond, "the set")
	_, err = api.NewSourceServiceClient(asBob).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: yardCam.GetId()}.Build()}.Build())
	require.NoError(t, err, "its camera")
	tl, err := api.NewLaminaServiceClient(asBob).Timeline(ctx, api.LaminaTimelineRequest_builder{Source: ex.GetSource(), From: ex.GetFrom(), To: ex.GetTo(), Size: 10}.Build())
	require.NoError(t, err, "its timeline")
	require.NotEmpty(t, tl.GetSources())
	_, err = api.NewSetServiceClient(asBob).Live(ctx, api.SetLiveRequest_builder{Ref: yard}.Build())
	require.NotEqual(t, codes.NotFound, status.Code(err), "Live: %v", err)
	_, err = api.NewLaminaServiceClient(asBob).Export(ctx, ex)
	require.NoError(t, err, "an export of what it recorded before the move")
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, barn)), "the set beside it")

	// Where it is already: nothing written.
	again, err := placeSet(ctx, admin, yard, hall, false)
	require.NoError(t, err)
	require.Zero(t, again.GetMoved().GetSources()+again.GetMoved().GetLaminae()+again.GetMoved().GetAttempts()+again.GetMoved().GetProducers())
	require.Equal(t, got.GetSet().GetDateUpdated().AsTime(), again.GetSet().GetDateUpdated().AsTime())

	// The trail has the call: who, the set, and its site.
	adminRow, err := api.NewHolderServiceClient(admin).Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
		Alias: z.Ptr("admin"), Tenant: acme,
	}.Build()}.Build()}.Build())
	require.NoError(t, err)
	rows, err := api.NewAuditServiceClient(c.dialCluster(c.operator())).List(ctx, api.AuditListRequest_builder{
		Filters: []*api.AuditFilter{api.AuditFilter_builder{ObjectId: got.GetSet().GetId()}.Build()}, Size: 500,
	}.Build())
	require.NoError(t, err)
	var line *api.Audit
	for _, r := range rows.GetItems() {
		if r.GetAction() == api.SetService_Place_FullMethodName && bytes.Equal(r.GetObjectId(), got.GetSet().GetId()) {
			require.Nil(t, line, "one line for one move")
			line = r
		}
	}
	require.NotNil(t, line, "the move is on the trail")
	require.Equal(t, adminRow.GetId(), line.GetActorId())
	require.Contains(t, line.GetFields(), uint32(3), "the site")
	require.NotEmpty(t, line.GetPatch())
	doc := &patchpb.Patch{}
	require.NoError(t, proto.Unmarshal(line.GetPatch(), doc))
	placed, err := patchproto.Apply(&api.Set{}, doc)
	require.NoError(t, err, "the patch applies to the set as it was, in no site")
	require.Equal(t, hall.GetId(), placed.GetSite().GetId(), "and puts it in the hall")
	_, err = patchproto.Apply(api.Set_builder{Site: api.Site_builder{Id: vault.GetId()}.Build()}.Build(), doc)
	require.Error(t, err, "and says where it was: not in the vault")

	// Carol, a member of the vault, with two lists of her own made while
	// the set is in the hall: one of the hall, one of its camera.
	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "viewer", core.KeyScopeMethods(), carol.Id)
	require.NoError(t, err)
	_, err = c.running.CP.Provision(ctx, carol)
	require.NoError(t, err)
	_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{
		Tenant: acme, Site: vault, Holder: api.HolderRef_builder{Id: carol.Id.Bytes()}.Build(),
	}.Build())
	require.NoError(t, err)
	asCarol := c.dial("@acme/carol")
	require.Eventually(t, func() bool { return setSeen(ctx, asCarol, yard) == nil }, sitesSeenWithin, 200*time.Millisecond)
	reaches := api.NewReachServiceClient(asCarol)
	ofHall, err := reaches.Add(ctx, api.ReachAddRequest_builder{Name: "hall", Cameras: api.ReachCameras_builder{Sites: [][]byte{hall.GetId()}}.Build()}.Build())
	require.NoError(t, err)
	ofCam, err := reaches.Add(ctx, api.ReachAddRequest_builder{Name: "yard", Cameras: api.ReachCameras_builder{Sources: [][]byte{yardCam.GetId()}}.Build()}.Build())
	require.NoError(t, err)
	key := func(name string, reach *api.Reach) context.Context {
		v, err := api.NewHolderServiceClient(asCarol).BeginKey(ctx, api.HolderBeginKeyRequest_builder{
			Name: name, Methods: core.KeyScopeMethods(), Scope: core.ReachScope(mustPdid(t, reach.GetId())),
		}.Build())
		require.NoError(t, err)
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+v.GetToken())
	}
	byHall, byCam := key("hall", ofHall), key("yard", ofCam)
	conn := plainConn(t, c.running.TenantAddr)
	require.NoError(t, setSeen(byHall, conn, yard))
	require.NoError(t, setSeen(byCam, conn, yard))

	// Moved on into the vault: Bob no longer sees it, Carol does; her list
	// of the hall no longer reaches it, her list of its camera does.
	moved, err := placeSet(ctx, admin, yard, vault, false)
	require.NoError(t, err)
	require.Equal(t, hall.GetId(), moved.GetFromSiteId())
	require.Equal(t, got.GetMoved().GetLaminae(), moved.GetMoved().GetLaminae())
	require.True(t, allAre(rowSites(t, ctx, admin, yardCam), string(vault.GetId())))
	require.Eventually(t, func() bool { return status.Code(setSeen(ctx, asBob, yard)) == codes.NotFound }, sitesSeenWithin, 200*time.Millisecond, "Bob")
	_, err = api.NewLaminaServiceClient(asBob).Export(ctx, ex)
	require.Equal(t, codes.NotFound, status.Code(err), "Bob's export")
	require.NoError(t, setSeen(ctx, asCarol, yard), "a member")
	_, err = api.NewLaminaServiceClient(asCarol).Export(ctx, ex)
	require.NoError(t, err, "a member's export")
	require.Eventually(t, func() bool { return status.Code(setSeen(byHall, conn, yard)) == codes.NotFound }, sitesSeenWithin, 200*time.Millisecond,
		"a list that named the site it left")
	require.NoError(t, setSeen(byCam, conn, yard), "a list that named its camera")

	// The command line, which says what moved.
	want, err := placeSet(ctx, admin, barn, hall, true)
	require.NoError(t, err)
	var out bytes.Buffer
	x := cli.Cmd(&cmd.Config{})
	x.Writer, x.ErrWriter = &out, &out
	require.NoError(t, x.Run(ctx, []string{"--addr", "http://" + c.running.TenantAddr, "--as", "@acme/admin", "set", "place", "@acme/barn", "@acme/hall"}))
	require.Contains(t, out.String(), "barn: in no site, now in hall")
	require.Contains(t, out.String(), fmt.Sprintf("moved %d source(s), %d lamina(s), %d attempt(s), %d producer(s)",
		want.GetMoved().GetSources(), want.GetMoved().GetLaminae(), want.GetMoved().GetAttempts(), want.GetMoved().GetProducers()))
	require.True(t, allAre(rowSites(t, ctx, admin, barnCam), string(hall.GetId())))
	out.Reset()
	require.NoError(t, x.Run(ctx, []string{"--addr", "http://" + c.running.TenantAddr, "--as", "@acme/admin", "set", "place", "--dry-run", "@acme/barn", "@acme/vault"}))
	require.Contains(t, out.String(), "barn: in hall, would be in vault")
	require.Contains(t, out.String(), "would move")
	require.True(t, allAre(rowSites(t, ctx, admin, barnCam), string(hall.GetId())), "a dry run from the command line moves nothing")
}
