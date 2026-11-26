package e2e_test

import (
	"bytes"
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
)

// A key narrowed to some cameras (§33.1), end to end, with the roster in
// this process: two sites, a set of two cameras recording and live in one
// of them and a set of two more in the other, and a person who sees both.
//
// A key whose scope is a list of one camera sees that camera and nothing
// of the others, on every path: the generated reads and the stream, the
// set it is in, the laminae and their attempts, the timeline, the live
// view's tokens and the export's ticket. A list of a site reaches its
// cameras; a list of a site and a camera of the other site reaches both.
// A list changed reaches the new within the remembering; a list erased,
// or another person's, reaches nothing, and a call naming a camera says
// the list is gone. A key that carries a list carries only what a list
// confines, at BeginKey and at the door; a list names only what its person
// sees; a key that carries none is as before; and the command line, with
// SHALE_CLIENT_TOKEN set to a narrowed key, lists within the list.
func TestKeyNarrowedToCameras(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: standingTtl}
		c.Export.Enabled = true
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := c.running.CP.Identity
	admin := c.dial("@acme/admin")
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()

	sites := api.NewSiteServiceClient(admin)
	site := func(alias string) *api.Site {
		v, err := sites.Add(ctx, api.SiteAddRequest_builder{Tenant: acme, Alias: alias}.Build())
		require.NoError(t, err)
		return v
	}
	north, south, west := site("north"), site("south"), site("west")
	siteRef := func(v *api.Site) *api.SiteRef { return api.SiteRef_builder{Id: v.GetId()}.Build() }

	// North: a set of two cameras, recording and live.
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	cam := func(alias string) producer.SourceConfig {
		return producer.SourceConfig{Alias: alias, Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}
	}
	yardSet, _, _ := liveProducerAt(t, ctx, c, admin, siteRef(north), cam("door"), cam("yard"))
	adminSources := api.NewSourceServiceClient(admin)
	require.Eventually(t, func() bool {
		v, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: yardSet.GetId()}.Build()}.Build())
		return err == nil && len(v.GetSources()) == 2
	}, 30*time.Second, 300*time.Millisecond, "both cameras live")
	members, err := adminSources.List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{
		api.SourceFilter_builder{Set: api.SetRef_builder{Id: yardSet.GetId()}.Build()}.Build(),
	}}.Build())
	require.NoError(t, err)
	require.Len(t, members.GetItems(), 2)
	slices.SortFunc(members.GetItems(), func(a, b *api.Source) int { return int(a.GetOrdinal() - b.GetOrdinal()) })
	door, yard := members.GetItems()[0], members.GetItems()[1]

	// South: a set of two cameras that record nothing yet; west: one Bob
	// does not see.
	addSet := func(alias string, at *api.Site, cams ...string) (*api.Set, []*api.Source) {
		set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: acme, Alias: alias, Site: siteRef(at)}.Build())
		require.NoError(t, err)
		var out []*api.Source
		for _, a := range cams {
			v, err := adminSources.Add(ctx, api.SourceAddRequest_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: a}.Build())
			require.NoError(t, err)
			out = append(out, v)
		}
		return set, out
	}
	gateSet, gates := addSet("gate", south, "gate-a", "gate-b")
	_, wests := addSet("barn", west, "barn")

	// Bob sees north and south, and holds the reads at Shale.
	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	table := core.KeyScopeMethods()
	_, err = id.Grant(ctx, "acme", "viewer", append(slices.Clone(table),
		api.SourceService_Patch_FullMethodName, api.ProducerService_List_FullMethodName, api.CameraBindingService_List_FullMethodName,
	), bob.Id)
	require.NoError(t, err)
	// His row, made as at his first arrival -- seeing every site, as
	// everybody arrives where roster says who operates -- and then held to
	// two of them before he calls anything.
	_, err = c.running.CP.Provision(ctx, bob)
	require.NoError(t, err)
	_, err = api.NewHolderServiceClient(admin).Patch(ctx, api.HolderPatchRequest_builder{
		Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build(), AllSites: z.Ptr(false), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	asBob := c.dial("@acme/bob")
	for _, at := range []*api.Site{north, south} {
		_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{
			Tenant: acme, Site: siteRef(at), Holder: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build(),
		}.Build())
		require.NoError(t, err)
	}

	// His lists, of what he sees and nothing else.
	reaches := api.NewReachServiceClient(asBob)
	list := func(name string, sites []*api.Site, sources ...*api.Source) (*api.Reach, error) {
		b := api.ReachCameras_builder{}
		for _, v := range sites {
			b.Sites = append(b.Sites, v.GetId())
		}
		for _, v := range sources {
			b.Sources = append(b.Sources, v.GetId())
		}
		return reaches.Add(ctx, api.ReachAddRequest_builder{Name: name, Cameras: b.Build()}.Build())
	}
	one, err := list("door", nil, door)
	require.NoError(t, err)
	southern, err := list("south", []*api.Site{south})
	require.NoError(t, err)
	mixed, err := list("south and door", []*api.Site{south}, door)
	require.NoError(t, err)
	_, err = list("west", []*api.Site{west})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a site he does not see: %v", err)
	_, err = list("barn", nil, wests[0])
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a camera he does not see: %v", err)

	// Carol has a list of her own, which Bob neither reads nor carries.
	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	asCarol := c.dial("@acme/carol")
	_, err = api.NewSetServiceClient(asCarol).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	hers, err := api.NewReachServiceClient(asCarol).Add(ctx, api.ReachAddRequest_builder{Name: "hers", Cameras: api.ReachCameras_builder{}.Build()}.Build())
	require.NoError(t, err)
	_, err = reaches.Add(ctx, api.ReachAddRequest_builder{Name: "x", Holder: api.HolderRef_builder{Id: carol.Id.Bytes()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "one for somebody else: %v", err)
	theirs, err := reaches.List(ctx, api.ReachListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, theirs.GetItems(), 3, "his own and not hers")
	_, err = reaches.Get(ctx, api.ReachGetRequest_builder{Ref: api.ReachRef_builder{Id: hers.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))

	// His keys, made as the Tokens page makes them.
	holders := api.NewHolderServiceClient(asBob)
	key := func(name string, reach *api.Reach, methods []string) (string, error) {
		scope := ""
		if reach != nil {
			scope = core.ReachScope((mustPdid(t, reach.GetId())))
		}
		v, err := holders.BeginKey(ctx, api.HolderBeginKeyRequest_builder{Name: name, Methods: methods, Scope: scope}.Build())
		return v.GetToken(), err
	}
	oneKey, err := key("door", one, table)
	require.NoError(t, err)
	southKey, err := key("south", southern, table)
	require.NoError(t, err)
	mixedKey, err := key("mixed", mixed, table)
	require.NoError(t, err)
	allKey, err := key("all", nil, table)
	require.NoError(t, err)
	listed, err := holders.ListKeys(ctx, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.ElementsMatch(t, table, listed.GetNarrowed(), "what a narrowed key of his may be given")

	conn := plainConn(t, c.running.TenantAddr)
	bearer := func(token string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	sources := api.NewSourceServiceClient(conn)
	called := map[string]string{string(door.GetId()): "door", string(yard.GetId()): "yard", string(gates[0].GetId()): "gate-a", string(gates[1].GetId()): "gate-b"}
	aliases := func(token string) []string {
		t.Helper()
		vs, err := sources.List(bearer(token), api.SourceListRequest_builder{}.Build())
		require.NoError(t, err)
		var out []string
		for _, v := range vs.GetItems() {
			n, ok := called[string(v.GetId())]
			require.True(t, ok, "a camera of west: %v", v)
			out = append(out, n)
		}
		slices.Sort(out)
		return out
	}
	getSource := func(token string, v *api.Source) error {
		_, err := sources.Get(bearer(token), api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: v.GetId()}.Build()}.Build())
		return err
	}

	// The key of one camera: the camera, and nothing of the others.
	require.Equal(t, []string{"door"}, aliases(oneKey))
	require.NoError(t, getSource(oneKey, door))
	require.Equal(t, codes.NotFound, status.Code(getSource(oneKey, yard)), "as a row outside the wall")
	require.Equal(t, codes.NotFound, status.Code(getSource(oneKey, gates[0])))

	sets, err := api.NewSetServiceClient(conn).List(bearer(oneKey), api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, sets.GetItems(), 1, "its set alone")
	require.Equal(t, yardSet.GetId(), sets.GetItems()[0].GetId())

	// Its laminae, once both cameras have some, and their attempts.
	laminae := api.NewLaminaServiceClient(conn)
	require.Eventually(t, func() bool {
		return anyLamina(ctx, admin, door.GetId(), func(*api.Lamina) bool { return true }) &&
			anyLamina(ctx, admin, yard.GetId(), func(*api.Lamina) bool { return true })
	}, 60*time.Second, 500*time.Millisecond, "both cameras record")
	ls, err := laminae.List(bearer(oneKey), api.LaminaListRequest_builder{Size: 500}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, ls.GetItems())
	doors := map[string]bool{}
	for _, v := range ls.GetItems() {
		require.Equal(t, door.GetId(), v.GetSource().GetId(), "a lamina of another camera")
		doors[string(v.GetId())] = true
	}
	var yards *api.Lamina
	require.True(t, anyLamina(ctx, admin, yard.GetId(), func(v *api.Lamina) bool { yards = v; return true }))
	_, err = laminae.Get(bearer(oneKey), api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: yards.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "another camera's lamina: %v", err)
	_, err = api.NewSetServiceClient(conn).Get(bearer(oneKey), api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: gateSet.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "a set with no camera of the list: %v", err)
	attempts, err := api.NewAttemptServiceClient(conn).List(bearer(oneKey), api.AttemptListRequest_builder{Size: 500}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, attempts.GetItems())
	for _, v := range attempts.GetItems() {
		require.True(t, doors[string(v.GetLamina().GetId())], "an attempt of another camera's lamina")
	}
	all, err := api.NewAttemptServiceClient(admin).List(ctx, api.AttemptListRequest_builder{Size: 500}.Build())
	require.NoError(t, err)
	require.Greater(t, len(all.GetItems()), len(attempts.GetItems()), "the other camera's are there")

	tl, err := laminae.Timeline(bearer(oneKey), api.LaminaTimelineRequest_builder{
		Set: api.SetRef_builder{Id: yardSet.GetId()}.Build(), From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.Now(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1, "the set's timeline, of its camera alone")
	require.Equal(t, door.GetId(), tl.GetSources()[0].GetSourceId())
	for _, v := range tl.GetSources()[0].GetLaminae() {
		require.True(t, doors[string(v.GetLaminaId())])
	}
	_, err = laminae.Timeline(bearer(oneKey), api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: yard.GetId()}.Build(), From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.Now(),
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))

	// Live: its set's view tokens are for it alone, and another camera's
	// live view is refused.
	live, err := api.NewSetServiceClient(conn).Live(bearer(oneKey), api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: yardSet.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, live.GetSources(), 1)
	claims, err := token.Parse(live.GetSources()[0].GetViewToken())
	require.NoError(t, err)
	require.Equal(t, door.GetId(), claims.GetSource())
	_, err = sources.Live(bearer(oneKey), api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: yard.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	_, err = sources.Live(bearer(oneKey), api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: door.GetId()}.Build()}.Build())
	require.NoError(t, err)

	// Export: a ticket for its camera, and none for another.
	export := func(token string, v *api.Source) error {
		_, err := laminae.Export(bearer(token), api.LaminaExportRequest_builder{
			Source: api.SourceRef_builder{Id: v.GetId()}.Build(), From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.Now(),
		}.Build())
		return err
	}
	require.NoError(t, export(oneKey, door))
	require.Equal(t, codes.NotFound, status.Code(export(oneKey, yard)))

	// A stream: another camera cannot be watched, and its writes are
	// never carried.
	_, err = recvWatch(bearer(oneKey), sources, yard).Recv()
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	stream := recvWatch(bearer(oneKey), sources, door)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, first.GetItems(), 1)
	require.Equal(t, door.GetId(), first.GetItems()[0].GetId())
	rename := func(v *api.Source, name string) {
		_, err := adminSources.Patch(ctx, api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: v.GetId()}.Build(), Name: z.Ptr(name), DateUpdatedForce: z.Ptr(true)}.Build())
		require.NoError(t, err)
	}
	// until reads the stream until an item says `done`; every item it
	// carries is the camera's.
	until := func(done func(*api.SourceWatchItem) bool) {
		t.Helper()
		for {
			v, err := stream.Recv()
			require.NoError(t, err)
			for _, it := range v.GetItems() {
				require.Equal(t, door.GetId(), it.GetId(), "another camera's row")
				if done(it) {
					return
				}
			}
		}
	}
	rename(yard, "the yard")
	rename(door, "the door")
	until(func(it *api.SourceWatchItem) bool { return it.GetValue().GetName() == "the door" })

	// A list of a site: its cameras; of a site and a camera of the other:
	// both.
	require.Equal(t, []string{"gate-a", "gate-b"}, aliases(southKey))
	sets, err = api.NewSetServiceClient(conn).List(bearer(southKey), api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, sets.GetItems(), 1)
	require.Equal(t, gateSet.GetId(), sets.GetItems()[0].GetId())
	require.Equal(t, []string{"door", "gate-a", "gate-b"}, aliases(mixedKey))
	require.Equal(t, []string{"door", "gate-a", "gate-b", "yard"}, aliases(allKey), "a key that carries none is as before")

	// From a script.
	t.Setenv("SHALE_CLIENT_TOKEN", mixedKey)
	var out bytes.Buffer
	x := cli.Cmd(&cmd.Config{})
	x.Writer, x.ErrWriter = &out, &out
	require.NoError(t, x.Run(ctx, []string{"--addr", "http://" + c.running.TenantAddr, "source", "ls"}))
	require.Contains(t, out.String(), gates[0].GetAlias())
	require.Contains(t, out.String(), door.GetAlias())
	require.NotContains(t, out.String(), yard.GetAlias())
	out.Reset()
	require.NoError(t, x.Run(ctx, []string{"--addr", "http://" + c.running.TenantAddr, "source", "get", "@acme/" + door.GetAlias()}))
	require.Contains(t, out.String(), door.GetAlias())
	require.Error(t, x.Run(ctx, []string{"--addr", "http://" + c.running.TenantAddr, "source", "get", "@acme/" + yard.GetAlias()}))

	// The list changed: the key reaches the new within the remembering. The
	// stream that watched the camera it no longer reaches is told, at the
	// camera's next write, that the row left its sight, and no more.
	_, err = reaches.Patch(ctx, api.ReachPatchRequest_builder{
		Ref: api.ReachRef_builder{Id: one.GetId()}.Build(), Cameras: api.ReachCameras_builder{Sources: [][]byte{yard.GetId()}}.Build(), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return slices.Equal([]string{"yard"}, aliases(oneKey)) }, standingTtl+standingSlack, 50*time.Millisecond)
	rename(door, "the front door")
	until(func(it *api.SourceWatchItem) bool { return it.GetValue() == nil })

	// Erased: it reaches nothing, and a call naming a camera says why.
	_, err = reaches.Erase(ctx, api.ReachRef_builder{Id: one.GetId()}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(aliases(oneKey)) == 0 }, standingTtl+standingSlack, 50*time.Millisecond)
	err = getSource(oneKey, yard)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "list of cameras is gone")
	require.Equal(t, codes.FailedPrecondition, status.Code(export(oneKey, yard)))
	sets, err = api.NewSetServiceClient(conn).List(bearer(oneKey), api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, sets.GetItems())

	// What a narrowed key carries: refused at BeginKey whatever else it
	// names, and at the door whatever its methods say.
	_, err = key("binding", southern, append(slices.Clone(table), api.CameraBindingService_List_FullMethodName))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "cannot carry "+api.CameraBindingService_List_FullMethodName)
	_, err = key("wide", southern, append(slices.Clone(table), api.SourceService_Patch_FullMethodName))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "cannot carry "+api.SourceService_Patch_FullMethodName)
	tenant := (mustPdid(t, door.GetTenant().GetId()))
	wide, _, err := id.IssueKeyAs(ctx, tenant, bob.Id, identity.TerminalKey{
		Name: "wide", Methods: []string{
			api.SourceService_List_FullMethodName, api.SourceService_Patch_FullMethodName, api.ProducerService_List_FullMethodName,
			api.CameraBindingService_List_FullMethodName,
		},
		Scope: core.ReachScope((mustPdid(t, southern.GetId()))),
	})
	require.NoError(t, err, "roster keeps what it is given")
	require.Equal(t, []string{"gate-a", "gate-b"}, aliases(wide))
	_, err = api.NewProducerServiceClient(conn).List(bearer(wide), api.ProducerListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a producer's inventory: %v", err)
	require.Contains(t, status.Convert(err).Message(), "carries a list of cameras")
	_, err = api.NewCameraBindingServiceClient(conn).List(bearer(wide), api.CameraBindingListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a camera's binding, which the table does not offer: %v", err)
	require.Contains(t, status.Convert(err).Message(), "carries a list of cameras")
	_, err = sources.Patch(bearer(wide), api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: gates[0].GetId()}.Build(), Name: z.Ptr("x"), DateUpdatedForce: z.Ptr(true)}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a write: %v", err)

	// Somebody else's list: refused at BeginKey, and nothing at the door.
	_, err = key("hers", hers, table)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "no list of cameras of yours")
	borrowed, _, err := id.IssueKeyAs(ctx, tenant, bob.Id, identity.TerminalKey{
		Name: "borrowed", Methods: table, Scope: core.ReachScope((mustPdid(t, hers.GetId()))),
	})
	require.NoError(t, err)
	require.Empty(t, aliases(borrowed))
	require.Equal(t, codes.FailedPrecondition, status.Code(getSource(borrowed, door)))
}

// recvWatch opens a watch of one camera; a watch that is refused is
// refused at its first receive.
func recvWatch(ctx context.Context, sources api.SourceServiceClient, v *api.Source) grpc.ServerStreamingClient[api.SourceWatchResponse] {
	s, err := sources.Watch(ctx, api.SourceWatchRequest_builder{Filters: []*api.SourceFilter{
		api.SourceFilter_builder{Ref: api.SourceRef_builder{Id: v.GetId()}.Build()}.Build(),
	}}.Build())
	if err != nil {
		panic(err)
	}

	return s
}
