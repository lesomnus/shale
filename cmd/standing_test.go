package cmd

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/auth/authsession"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/identity"
)

// A session is held to what roster says of its person (§33.7): refused once
// they are suspended, erased, gone, or signed out everywhere at or after it
// began; roster's answer kept for the TTL; and while roster cannot be
// asked, refused as Unavailable and asked again only after a short while.
func TestSessionStanding(t *testing.T) {
	ctx := context.Background()
	tenant, holder := pdid.New(1), pdid.New(2)
	now := time.Unix(1_800_000_000, 0)
	var st identity.Standing
	var down error
	asked := 0
	s := newStanding(nil, 30*time.Second)
	s.ask = func(context.Context, pdid.Id, pdid.Id) (identity.Standing, error) {
		asked++
		return st, down
	}
	s.now = func() time.Time { return now }
	began := now.Add(-time.Minute)
	session := authsession.Session{Id: holder.String(), TenantId: tenant.String(), Held: map[string]string{heldIssued: strconv.FormatInt(began.UnixNano(), 10)}}
	forget := func() { s.known = map[pdid.Id]standingEntry{} }

	require.NoError(t, s.check(ctx, session))
	require.NoError(t, s.check(ctx, session))
	require.Equal(t, 1, asked, "kept for the TTL")

	// Signed out everywhere before the session began is nothing to it; as
	// it began, or after, ends it.
	st.Invalidated = began.Add(-time.Millisecond)
	forget()
	require.NoError(t, s.check(ctx, session))
	st.Invalidated = began
	forget()
	require.ErrorIs(t, s.check(ctx, session), errStanding)
	st.Invalidated = began.Add(time.Millisecond)
	forget()
	require.ErrorIs(t, s.check(ctx, session), errStanding)
	st.Invalidated = time.Time{}

	// Known for the TTL, then asked again.
	forget()
	require.NoError(t, s.check(ctx, session))
	st.Disabled = now
	require.NoError(t, s.check(ctx, session), "within the TTL")
	now = now.Add(31 * time.Second)
	require.ErrorIs(t, s.check(ctx, session), errStanding, "suspended")
	st.Disabled = time.Time{}

	st.Erased = now
	forget()
	require.ErrorIs(t, s.check(ctx, session), errStanding, "erased")
	st.Erased = time.Time{}

	down = identity.ErrNoPerson
	forget()
	require.ErrorIs(t, s.check(ctx, session), errStanding, "gone")

	// Roster down: refused as unavailable, remembered for a few seconds.
	down = errors.New("connection refused")
	forget()
	asked = 0
	err := s.check(ctx, session)
	require.ErrorIs(t, err, auth.ErrUnavailable)
	require.NotErrorIs(t, err, errStanding, "not a reason to sign in again")
	require.ErrorIs(t, s.check(ctx, session), auth.ErrUnavailable)
	require.Equal(t, 1, asked)
	down = nil
	now = now.Add(standingRetry)
	require.NoError(t, s.check(ctx, session), "asked again after the retry")

	// A session that does not say when it began is not served.
	require.ErrorIs(t, s.check(ctx, authsession.Session{Id: holder.String(), TenantId: tenant.String()}), errStanding)
}
