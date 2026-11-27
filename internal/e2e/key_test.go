package e2e_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// A person's key for Shale (§33.1), minted at the roster in this process
// by roster's own `ApiKeyService/Issue`, as its account page does: it
// reads as them with the methods it names and with no other, a person
// arriving with one for the first time gets their rows, two keys of one
// person are each held to their own, a key of roster's own and a key for
// another app are no keys here, a bearer that is no key goes the way it
// went, and a key ended at roster, or of somebody suspended there, is
// refused within the TTL.
func TestPersonsKey(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: standingTtl}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)
	bearer := func(token string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	listSets := func(ctx context.Context) error {
		_, err := api.NewSetServiceClient(conn).List(ctx, api.SetListRequest_builder{}.Build())
		return err
	}
	listSources := func(ctx context.Context) error {
		_, err := api.NewSourceServiceClient(conn).List(ctx, api.SourceListRequest_builder{}.Build())
		return err
	}
	listLaminae := func(ctx context.Context) error {
		_, err := api.NewLaminaServiceClient(conn).List(ctx, api.LaminaListRequest_builder{}.Build())
		return err
	}
	ends := func(call func() error, why string) {
		t.Helper()
		began := time.Now()
		require.Eventually(t, func() bool { return status.Code(call()) == codes.Unauthenticated }, standingTtl+standingSlack, 50*time.Millisecond, why)
		require.Less(t, time.Since(began), standingTtl+standingSlack, why)
	}
	reads := []string{
		api.SetService_List_FullMethodName,
		api.SourceService_List_FullMethodName,
		api.LaminaService_List_FullMethodName,
	}

	// Bob is at roster and has never been here: no row of his.
	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "reader", reads, bob.Id)
	require.NoError(t, err)
	_, err = c.running.CP.Ungated.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))

	// A key naming two of the three: it reads with them, and is refused the
	// third, which Bob holds.
	two, twoId, err := id.IssueKey(ctx, "acme", "bob", "two", identity.Agent, reads[:2], time.Time{})
	require.NoError(t, err)
	require.NoError(t, listSets(bearer(two)), "a first arrival, by a key")
	require.NoError(t, listSources(bearer(two)))
	require.Equal(t, codes.PermissionDenied, status.Code(listLaminae(bearer(two))), "a method the key does not name")
	h, err := c.running.CP.Ungated.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err, "made at the first arrival, as at a first sign-in")
	require.Equal(t, "bob", h.GetAlias())

	// And from a script: the command line sends `client.token` as the
	// bearer, here from the environment.
	shale := func(args ...string) error {
		x := cli.Cmd(&cmd.Config{})
		x.Writer, x.ErrWriter = io.Discard, io.Discard
		return x.Run(ctx, append([]string{"--addr", "http://" + c.running.TenantAddr}, args...))
	}
	t.Setenv("SHALE_CLIENT_TOKEN", two)
	require.NoError(t, shale("set", "ls"))
	require.Equal(t, codes.PermissionDenied, status.Code(shale("lamina", "ls")))

	// A second key of his, naming the third alone: each is held to its own,
	// whichever was asked about last.
	one, _, err := id.IssueKey(ctx, "acme", "bob", "one", identity.Agent, reads[2:], time.Time{})
	require.NoError(t, err)
	require.NoError(t, listLaminae(bearer(one)))
	require.Equal(t, codes.PermissionDenied, status.Code(listSets(bearer(one))))
	require.NoError(t, listSets(bearer(two)))
	require.Equal(t, codes.PermissionDenied, status.Code(listLaminae(bearer(two))))
	require.Equal(t, codes.PermissionDenied, status.Code(listSources(bearer(one))))

	// A key of roster's own, and a key for another app, are no keys here.
	_, err = id.Grant(ctx, "acme", "me", []string{"/roster.MeService/Get"}, bob.Id)
	require.NoError(t, err)
	own, _, err := id.IssueKey(ctx, "acme", "bob", "own", "", []string{"/roster.MeService/Get"}, time.Time{})
	require.NoError(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(listSets(bearer(own))), "a key of roster's")
	require.NoError(t, id.InstallApp(ctx, "acme", "loft", []string{"/loft.*/*"}, []string{"bob"}))
	loft, _, err := id.IssueKey(ctx, "acme", "bob", "loft", "loft", []string{"/loft.AssetService/Get"}, time.Time{})
	require.NoError(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(listSets(bearer(loft))), "a key for another app")
	require.Equal(t, codes.Unauthenticated, status.Code(listSets(bearer("rt_nothing"))), "a key nobody minted")

	// A bearer that is no key of roster's is not one of these: alone it is
	// nobody, as before, and beside a session the session is who calls.
	require.Equal(t, codes.Unauthenticated, status.Code(listSets(bearer("pd_nokey"))))
	require.NoError(t, id.SetPassword(ctx, "acme", "bob", "bob's password"))
	tenantBase, _ := httpBases(t, c)
	code, cookie := signIn(t, tenantBase, "acme", "bob", "bob's password")
	require.Equal(t, http.StatusNoContent, code)
	require.NoError(t, listSets(metadata.AppendToOutgoingContext(ctx, "cookie", "shale_tenant="+cookie, "authorization", "Bearer pd_nokey")))
	require.NoError(t, listLaminae(metadata.AppendToOutgoingContext(ctx, "cookie", "shale_tenant="+cookie)), "a session is all of the person")

	// Ended at roster, as on its account page: refused within the TTL; the
	// other key goes on.
	require.NoError(t, id.EndKey(ctx, "acme", "bob", twoId))
	ends(func() error { return listSets(bearer(two)) }, "a key ended at roster")
	require.NoError(t, listLaminae(bearer(one)))

	// Somebody suspended at roster: their key is refused within the TTL.
	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "reader", reads, carol.Id)
	require.NoError(t, err)
	theirs, _, err := id.IssueKey(ctx, "acme", "carol", "script", identity.Agent, reads[:1], time.Time{})
	require.NoError(t, err)
	require.NoError(t, listSets(bearer(theirs)))
	require.NoError(t, id.Suspend(ctx, carol.Id))
	ends(func() error { return listSets(bearer(theirs)) }, "a suspended person's key")
	require.NoError(t, listLaminae(bearer(one)), "nobody else's")
}

// A key that has an end ends what was opened with it there (§33.1): a
// Watch is cut at its end by payday, Unauthenticated, and a Live answers
// view tokens that end no later than it, at whose end the relay ends the
// view.
func TestPersonsKeyEnds(t *testing.T) {
	// Dave sees the set as everybody does: its site is said EVERYBODY,
	// and he is no member of it.
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme"}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	set, _ := liveSourceAt(t, ctx, c, admin, "av.mp4", everybody(t, ctx, admin, "lobby"))
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)

	dave, err := id.AddPerson(ctx, "acme", "dave", "Dave")
	require.NoError(t, err)
	methods := []string{api.SetService_Watch_FullMethodName, api.SetService_Get_FullMethodName, api.SetService_Live_FullMethodName}
	_, err = id.Grant(ctx, "acme", "viewer", methods, dave.Id)
	require.NoError(t, err)
	end := time.Now().Add(10 * time.Second)
	key, _, err := id.IssueKey(ctx, "acme", "dave", "watching", identity.Agent, methods, end)
	require.NoError(t, err)
	as := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	ref := api.SetRef_builder{Id: set.GetId()}.Build()

	live, err := api.NewSetServiceClient(conn).Live(as, api.SetLiveRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, live.GetSources())
	src := live.GetSources()[0]
	require.False(t, src.GetDateExpires().AsTime().After(end), "a view token outlives no key: %s, the key ends %s", src.GetDateExpires().AsTime(), end)
	v := watch(t, src)
	defer v.close()
	v.video(t, 20, 10*time.Second)

	st, err := api.NewSetServiceClient(conn).Watch(as, api.SetWatchRequest_builder{
		Filters: []*api.SetFilter{api.SetFilter_builder{Ref: ref}.Build()},
	}.Build())
	require.NoError(t, err)
	for {
		_, err = st.Recv()
		if err != nil {
			break
		}
	}
	require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
	require.Contains(t, err.Error(), "expired")
	require.False(t, time.Now().Before(end.Add(-time.Second)), "cut at the key's end and not before it")
	require.Less(t, time.Since(end), 3*time.Second, "cut at the key's end")

	// And the view: the relay ended it at its token's end.
	time.Sleep(time.Until(end.Add(1500 * time.Millisecond)))
	for len(v.packets) > 0 {
		<-v.packets
	}
	select {
	case <-v.packets:
		t.Fatal("video after the key ended")
	case <-time.After(2 * time.Second):
	}

	// Nor is anything served with it afterwards.
	_, err = api.NewSetServiceClient(conn).Get(as, api.SetGetRequest_builder{Ref: ref}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

// A stream opened with a key is cut when the key is ended at roster
// (§33.1), within the TTL, as for a person stopped: what roster said of a
// key with a stream open is asked again each time it runs out. Another
// key's stream of the same person goes on.
func TestPersonsKeyEndedAtRosterEndsItsStreams(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: standingTtl}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "watched", Site: everybody(t, ctx, admin, "lobby"),
	}.Build())
	require.NoError(t, err)
	ref := api.SetRef_builder{Id: set.GetId()}.Build()

	erin, err := id.AddPerson(ctx, "acme", "erin", "Erin")
	require.NoError(t, err)
	methods := []string{api.SetService_Watch_FullMethodName, api.SetService_Get_FullMethodName}
	_, err = id.Grant(ctx, "acme", "watcher", methods, erin.Id)
	require.NoError(t, err)
	ended, endedId, err := id.IssueKey(ctx, "acme", "erin", "ended", identity.Agent, methods, time.Time{})
	require.NoError(t, err)
	kept, _, err := id.IssueKey(ctx, "acme", "erin", "kept", identity.Agent, methods, time.Time{})
	require.NoError(t, err)

	open := func(token string) <-chan error {
		as := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		st, err := api.NewSetServiceClient(conn).Watch(as, api.SetWatchRequest_builder{
			Filters: []*api.SetFilter{api.SetFilter_builder{Ref: ref}.Build()},
		}.Build())
		require.NoError(t, err)
		_, err = st.Recv()
		require.NoError(t, err, "the first message, what is there now")
		done := make(chan error, 1)
		go func() {
			for {
				if _, err := st.Recv(); err != nil {
					done <- err
					return
				}
			}
		}()
		return done
	}
	endedWatch, keptWatch := open(ended), open(kept)

	require.NoError(t, id.EndKey(ctx, "acme", "erin", endedId))
	began := time.Now()
	select {
	case err := <-endedWatch:
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
		require.Less(t, time.Since(began), standingTtl+standingSlack)
	case <-time.After(standingTtl + standingSlack):
		t.Fatal("a Watch outlived its key, ended at roster")
	}

	select {
	case err := <-keptWatch:
		t.Fatalf("another key's Watch was cut: %v", err)
	case <-time.After(2 * standingTtl):
	}
}
