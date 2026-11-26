package e2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// A person makes, sees and ends their keys for Shale from the console
// (§33.1), with the roster in this process, where the console is roster's
// front door: the key is made at once, as the person, and answered once;
// it is taken at the door with the methods it names; it is listed for its
// person with its name, methods, end, when it was made and used, and for
// nobody else; it is ended by its person and by nobody else, and refused
// within the remembering after that. A key of roster's own and a key for
// another app are not among a person's keys for Shale. A key that names
// BeginKey does not make one here; one that names ListKeys and EndKey
// lists and ends. A person with no role of Shale's lists their keys, and
// is refused one by roster's rule. The cluster API serves none of it.
func TestPersonsOwnKeys(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: standingTtl, Reads: "granted"}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)
	holders := api.NewHolderServiceClient(conn)
	tenantBase, _ := httpBases(t, c)
	session := func(alias string) context.Context {
		t.Helper()
		require.NoError(t, id.SetPassword(ctx, "acme", alias, alias+"'s password"))
		code, cookie := signIn(t, tenantBase, "acme", alias, alias+"'s password")
		require.Equal(t, http.StatusNoContent, code)
		return metadata.AppendToOutgoingContext(ctx, "cookie", "shale_tenant="+cookie)
	}
	bearer := func(token string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	listSets := func(ctx context.Context) error {
		_, err := api.NewSetServiceClient(conn).List(ctx, api.SetListRequest_builder{}.Build())
		return err
	}
	own := []string{
		api.HolderService_BeginKey_FullMethodName, api.HolderService_PollKey_FullMethodName,
		api.HolderService_ListKeys_FullMethodName, api.HolderService_EndKey_FullMethodName,
	}
	reads := []string{api.SetService_List_FullMethodName, api.SetService_Get_FullMethodName}

	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "reader", append(append([]string{}, reads...), own...), bob.Id)
	require.NoError(t, err)
	asBob := session("bob")

	// Made at once, and answered once: nothing to say yes to elsewhere.
	ends := time.Now().Add(90 * 24 * time.Hour).Truncate(time.Second)
	made, err := holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{
		Name: "script", Methods: reads[:1], DateExpired: timestamppb.New(ends),
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, made.GetToken())
	require.Empty(t, made.GetHandle(), "nothing to poll where roster runs here")
	require.Empty(t, made.GetApproveAt())
	require.Equal(t, "script", made.GetKey().GetName())
	require.Equal(t, reads[:1], made.GetKey().GetMethods())
	require.True(t, made.GetKey().GetDateExpired().AsTime().Equal(ends))

	// Taken at the door with the methods it names, and no other.
	require.NoError(t, listSets(bearer(made.GetToken())))
	_, err = api.NewSourceServiceClient(conn).List(bearer(made.GetToken()), api.SourceListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a method the key does not name: %v", err)

	// Listed for Bob, with what roster holds of it, and nothing that is
	// not his for Shale: a key of roster's own, one for another app.
	_, err = id.Grant(ctx, "acme", "me", []string{"/roster.MeService/Get"}, bob.Id)
	require.NoError(t, err)
	_, _, err = id.IssueKey(ctx, "acme", "bob", "rosters", "", []string{"/roster.MeService/Get"}, time.Time{})
	require.NoError(t, err)
	require.NoError(t, id.InstallApp(ctx, "acme", "loft", []string{"/loft.*/*"}, []string{"bob"}))
	_, _, err = id.IssueKey(ctx, "acme", "bob", "lofts", "loft", []string{"/loft.AssetService/Get"}, time.Time{})
	require.NoError(t, err)
	listed, err := holders.ListKeys(asBob, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, listed.GetKeys(), 1, "%v", listed.GetKeys())
	k := listed.GetKeys()[0]
	require.Equal(t, made.GetKey().GetId(), k.GetId())
	require.Equal(t, "script", k.GetName())
	require.Equal(t, reads[:1], k.GetMethods())
	require.True(t, k.GetDateExpired().AsTime().Equal(ends))
	require.NotNil(t, k.GetDateCreated())
	require.NotNil(t, k.GetDateUsed(), "used at the door above")
	require.ElementsMatch(t, append(append([]string{}, reads...), own...), listed.GetHolds(), "what a key of his may be given")
	require.ElementsMatch(t, reads, listed.GetReads(), "the console's reads he holds")
	require.Equal(t, standingTtl, listed.GetRemembered().AsDuration())

	// roster's rules, in roster's words: a method Bob does not hold, a name
	// he has used, a name not spelled as a name is, an end that has passed.
	_, err = holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{Name: "wider", Methods: []string{api.SetService_Add_FullMethodName}}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "you do not hold")
	_, err = holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{Name: "script", Methods: reads[:1]}.Build())
	require.Equal(t, codes.AlreadyExists, status.Code(err), "%v", err)
	_, err = holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{Name: "My Script", Methods: reads[:1]}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	_, err = holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{Name: "late", Methods: reads[:1], DateExpired: timestamppb.New(time.Now().Add(-time.Minute))}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	// A key that names BeginKey does not make one here, and says why; one
	// that names ListKeys lists, as its person does.
	keyed, err := holders.BeginKey(asBob, api.HolderBeginKeyRequest_builder{Name: "keys", Methods: own}.Build())
	require.NoError(t, err)
	_, err = holders.BeginKey(bearer(keyed.GetToken()), api.HolderBeginKeyRequest_builder{Name: "minted", Methods: reads[:1]}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "a key does not make keys here")
	byKey, err := holders.ListKeys(bearer(keyed.GetToken()), api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, byKey.GetKeys(), 2)
	_, err = holders.ListKeys(bearer(made.GetToken()), api.HolderListKeysRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a key that does not name ListKeys")

	// Carol, who holds no role of Shale's at all: her own list, empty and
	// not refused even with reads granted, and no key, by roster's rule.
	// Bob's key is not hers to see or end.
	_, err = id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	asCarol := session("carol")
	theirs, err := holders.ListKeys(asCarol, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, theirs.GetKeys())
	require.Empty(t, theirs.GetHolds(), "she can be given no key")
	require.Empty(t, theirs.GetNoneHeld(), "an administrator can give her a role of Shale's here")
	_, err = holders.BeginKey(asCarol, api.HolderBeginKeyRequest_builder{Name: "mine", Methods: reads[:1]}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	_, err = holders.EndKey(asCarol, api.HolderEndKeyRequest_builder{Id: k.GetId()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "somebody else's key: %v", err)
	require.NoError(t, listSets(bearer(made.GetToken())), "and it goes on")

	// The cluster API serves none of it, an operator's session included.
	_, err = api.NewHolderServiceClient(c.dialCluster("@acme/admin")).ListKeys(ctx, api.HolderListKeysRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)

	// Ended by Bob: the answer says how long it is remembered, and it is
	// refused within that.
	ended, err := holders.EndKey(asBob, api.HolderEndKeyRequest_builder{Id: k.GetId()}.Build())
	require.NoError(t, err)
	require.Equal(t, standingTtl, ended.GetRemembered().AsDuration())
	began := time.Now()
	require.Eventually(t, func() bool { return status.Code(listSets(bearer(made.GetToken()))) == codes.Unauthenticated }, standingTtl+standingSlack, 50*time.Millisecond)
	require.Less(t, time.Since(began), standingTtl+standingSlack)
	after, err := holders.ListKeys(asBob, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, after.GetKeys(), 1, "the key that lists, and not the one ended")
	_, err = holders.EndKey(asBob, api.HolderEndKeyRequest_builder{Id: k.GetId()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "ended already")
}

// Where roster runs in this process and no operators are named, nobody can
// hold a role of Shale's, so nobody can make a token (§33.1): ListKeys says
// why, for the page to say it instead of sending the person to an
// administrator who has nothing to give.
func TestNoOperatorsNoTokens(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)
	tenantBase, _ := httpBases(t, c)
	require.NoError(t, id.SetPassword(ctx, "acme", "admin", "admin's password"))
	code, cookie := signIn(t, tenantBase, "acme", "admin", "admin's password")
	require.Equal(t, http.StatusNoContent, code)
	asAdmin := metadata.AppendToOutgoingContext(ctx, "cookie", "shale_tenant="+cookie)

	listed, err := api.NewHolderServiceClient(conn).ListKeys(asAdmin, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, listed.GetHolds())
	require.Contains(t, listed.GetNoneHeld(), "auth.operators")
}
