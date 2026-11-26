package cmd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/identity"
)

// A person's key for Shale at the door (§33.1): told apart by its prefix
// before roster is asked; what roster said kept per key for the TTL, and
// never beyond the key's end; two keys of one person each their own; a
// person roster says stopped dropped at once, with their streams; a key
// that is none one answer; roster down Unavailable, asked again only after
// a short while.
func TestPersonsKeyAtTheDoor(t *testing.T) {
	ctx := context.Background()
	tenant, bob, carol := pdid.New(1), pdid.New(2), pdid.New(2)
	now := time.Unix(1_800_000_000, 0)
	reads := frame.Whole().To("/shale.SetService/List", "/shale.SourceService/List")
	lamina := frame.Whole().To("/shale.LaminaService/List")
	said := map[string]auth.Identity{
		"rt_two":   {Id: bob.String(), TenantId: tenant.String(), Grant: reads},
		"rt_one":   {Id: bob.String(), TenantId: tenant.String(), Grant: lamina},
		"rt_carol": {Id: carol.String(), TenantId: tenant.String(), Grant: reads},
		"rt_ends":  {Id: bob.String(), TenantId: tenant.String(), Grant: reads, DateExpired: now.Add(10 * time.Second)},
	}
	var down error
	asked := map[string]int{}
	k := &keys{ttl: 30 * time.Second, now: func() time.Time { return now }, known: map[[32]byte]keyEntry{}, stops: map[pdid.Id]chan struct{}{}, open: map[[32]byte]*keyStreams{}}
	k.ask = func(_ context.Context, token string) (auth.Identity, error) {
		asked[token]++
		if down != nil {
			return auth.Identity{}, down
		}
		v, ok := said[token]
		if !ok {
			return auth.Identity{}, identity.ErrNoKey
		}
		return v, nil
	}
	h := k.handler()
	call := func(header ...string) (auth.Identity, error) {
		md := metadata.MD{}
		for _, v := range header {
			md.Append(auth.Header, v)
		}
		return h.Handle(metadata.NewIncomingContext(ctx, md))
	}

	// Nothing that is not a person's key is asked about, and it goes on to
	// the next handler: a session, a certificate, the plain header.
	for _, v := range [][]string{nil, {"Bearer pd_session"}, {"Basic cnQ="}, {"Bearer "}} {
		_, err := call(v...)
		require.ErrorIs(t, err, auth.ErrNoCredential, "%q", v)
	}
	require.Empty(t, asked)

	// A key: the person, its own methods, by the key's handler.
	id, err := call("Bearer rt_two")
	require.NoError(t, err)
	require.Equal(t, bob.String(), id.Id)
	require.Equal(t, auth.MethodBearer, id.Method)
	require.Equal(t, authorityKey, id.Authority)
	require.True(t, id.Grant.Allows("/shale.SetService/List"))
	require.False(t, id.Grant.Allows("/shale.LaminaService/List"))
	_, err = call("Bearer pd_other", "Bearer rt_two")
	require.NoError(t, err, "the key among other bearers")

	// Two keys of one person: each its own, however the two are asked.
	id, err = call("Bearer rt_one")
	require.NoError(t, err)
	require.True(t, id.Grant.Allows("/shale.LaminaService/List"))
	require.False(t, id.Grant.Allows("/shale.SetService/List"))
	id, err = call("Bearer rt_two")
	require.NoError(t, err)
	require.False(t, id.Grant.Allows("/shale.LaminaService/List"))
	require.Equal(t, 1, asked["rt_two"], "kept for the TTL")
	require.Equal(t, 1, asked["rt_one"])

	// A key that is none: one answer, and kept too.
	_, err = call("Bearer rt_nobody")
	require.ErrorIs(t, err, auth.ErrUnknownToken)
	require.NotErrorIs(t, err, auth.ErrUnavailable)
	_, err = call("Bearer rt_nobody")
	require.ErrorIs(t, err, auth.ErrUnknownToken)
	require.Equal(t, 1, asked["rt_nobody"])

	// Ended at roster: still believed within the TTL, refused after it.
	delete(said, "rt_two")
	_, err = call("Bearer rt_two")
	require.NoError(t, err, "within the TTL")
	now = now.Add(31 * time.Second)
	_, err = call("Bearer rt_two")
	require.ErrorIs(t, err, auth.ErrUnknownToken)
	require.Equal(t, 2, asked["rt_two"])

	// A key with an end is believed until it and never after, whatever was
	// said: here a roster that would still answer it.
	e := said["rt_ends"]
	e.DateExpired = now.Add(10 * time.Second)
	said["rt_ends"] = e
	id, err = call("Bearer rt_ends")
	require.NoError(t, err)
	require.Equal(t, e.DateExpired, id.DateExpired, "a stream opened with it is cut at its end")
	now = now.Add(10 * time.Second)
	_, err = call("Bearer rt_ends")
	require.ErrorIs(t, err, auth.ErrUnknownToken)
	require.Equal(t, 2, asked["rt_ends"], "asked again at its end, and refused whatever roster said")

	// A person roster says stopped: forgotten at once, and the streams
	// their keys opened told; nobody else's.
	bobs, err := call("Bearer rt_one")
	require.NoError(t, err)
	carols, err := call("Bearer rt_carol")
	require.NoError(t, err)
	n := asked["rt_one"]
	k.drop(bob)
	select {
	case <-bobs.Changed:
	default:
		t.Fatal("a stream opened with a key of somebody stopped goes on")
	}
	select {
	case <-carols.Changed:
		t.Fatal("somebody else's stream was cut")
	default:
	}
	_, err = call("Bearer rt_one")
	require.NoError(t, err)
	require.Equal(t, n+1, asked["rt_one"], "asked again after the drop")
	_, err = call("Bearer rt_carol")
	require.NoError(t, err)
	require.Equal(t, 1, asked["rt_carol"], "nobody else's dropped")

	// Roster down: Unavailable, not a bad key, and asked again only after
	// a few seconds.
	now = now.Add(time.Minute)
	down = fmt.Errorf("%w: connection refused", auth.ErrUnavailable)
	_, err = call("Bearer rt_carol")
	require.ErrorIs(t, err, auth.ErrUnavailable)
	down = nil
	_, err = call("Bearer rt_carol")
	require.ErrorIs(t, err, auth.ErrUnavailable, "remembered for a moment")
	now = now.Add(keysRetry)
	_, err = call("Bearer rt_carol")
	require.NoError(t, err)
}

// A key with a stream open is asked about again each time what roster
// said runs out: `no such token` cuts that key's streams and no other
// key's, a roster that cannot be asked cuts nothing, and once the last
// stream closes nothing is asked.
func TestPersonsKeyStreamsFollowTheKey(t *testing.T) {
	bob := pdid.New(2)
	ttl := 50 * time.Millisecond
	answer := map[string]error{"rt_a": nil, "rt_b": nil}
	asked := map[string]int{}
	var mu sync.Mutex
	k := &keys{ttl: ttl, now: time.Now, known: map[[32]byte]keyEntry{}, stops: map[pdid.Id]chan struct{}{}, open: map[[32]byte]*keyStreams{}}
	k.ask = func(_ context.Context, token string) (auth.Identity, error) {
		mu.Lock()
		defer mu.Unlock()
		asked[token]++
		if err := answer[token]; err != nil {
			return auth.Identity{}, err
		}
		return auth.Identity{Id: bob.String(), Grant: frame.Whole()}, nil
	}
	set := func(token string, err error) { mu.Lock(); answer[token] = err; mu.Unlock() }
	count := func(token string) int { mu.Lock(); defer mu.Unlock(); return asked[token] }

	goneA, releaseA := k.opened("rt_a")
	goneB, releaseB := k.opened("rt_b")
	require.Eventually(t, func() bool { return count("rt_a") >= 2 && count("rt_b") >= 2 }, time.Second, 10*time.Millisecond, "asked again as it runs out")

	// Roster down: nothing is cut.
	set("rt_a", fmt.Errorf("%w: connection refused", auth.ErrUnavailable))
	n := count("rt_a")
	require.Eventually(t, func() bool { return count("rt_a") > n }, 7*time.Second, 10*time.Millisecond)
	select {
	case <-goneA:
		t.Fatal("cut while roster could not be asked")
	default:
	}

	// Ended at roster: that key's streams, and not the other's.
	set("rt_a", identity.ErrNoKey)
	select {
	case <-goneA:
	case <-time.After(7 * time.Second):
		t.Fatal("a key's stream outlived the key")
	}
	select {
	case <-goneB:
		t.Fatal("another key's stream was cut")
	default:
	}
	releaseA()

	// The last stream closed: nothing more is asked.
	releaseB()
	time.Sleep(3 * ttl)
	n = count("rt_b")
	time.Sleep(3 * ttl)
	require.Equal(t, n, count("rt_b"))
}
