package cmd

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/auth/authsession"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/identity"
)

// A session is sealed into its cookie, so nothing here can end one before
// its clock does (payday's `authsession.Sealed`). What can be ended is the
// person it names: roster says when somebody was signed out everywhere,
// suspended or erased, and every call a session makes is held to that,
// on both surfaces and however the session was minted (§33.7).

// heldIssued is where a session keeps when it was minted, in Unix
// nanoseconds: what roster's word that somebody was signed out everywhere
// is compared with. Kept in the session's Held since the sealed form has no field for
// it; nothing about the person is.
const heldIssued = "issued"

// issued stamps a session with when it is minted.
func issued(v authsession.Session) authsession.Session {
	held := maps.Clone(v.Held)
	if held == nil {
		held = map[string]string{}
	}
	held[heldIssued] = strconv.FormatInt(time.Now().UnixNano(), 10)
	v.Held = held

	return v
}

// standingRetry is how long a failure to ask roster is remembered: the
// answer in the meantime is no, without asking again, as for what roster
// grants (`identity.Operators`).
const standingRetry = 5 * time.Second

// standing is what roster says of the people sessions name, kept for the
// TTL of what it grants them (`auth.operators.ttl`, 30 s by default), so a
// session ends within that of its person being signed out everywhere,
// suspended or erased. It fails closed: while roster cannot be asked, no
// session is served, and the call is told Unavailable rather than to sign
// in again.
type standing struct {
	// ask is roster's answer about a person: `identity.Store.StandingOf`.
	ask func(ctx context.Context, tenant, holder pdid.Id) (identity.Standing, error)
	ttl time.Duration
	now func() time.Time

	mu    sync.Mutex
	known map[pdid.Id]standingEntry
}

type standingEntry struct {
	at  time.Time
	v   identity.Standing
	err error
}

func newStanding(id *identity.Store, ttl time.Duration) *standing {
	if ttl <= 0 {
		ttl = identity.OperatorsTtl
	}

	return &standing{ask: id.StandingOf, ttl: ttl, now: time.Now, known: map[pdid.Id]standingEntry{}}
}

// of is what roster says of a person, from what is known while it is
// fresh. That roster does not have them, or does not serve their tenant,
// is an answer and kept as long as one; a failure to ask is kept for
// standingRetry.
func (s *standing) of(ctx context.Context, tenant, holder pdid.Id) (identity.Standing, error) {
	now := s.now()
	s.mu.Lock()
	e, ok := s.known[holder]
	s.mu.Unlock()
	if ok {
		keep := s.ttl
		if e.err != nil && !refusedByRoster(e.err) {
			keep = standingRetry
		}
		if now.Sub(e.at) < keep {
			return e.v, e.err
		}
	}

	v, err := s.ask(ctx, tenant, holder)
	s.mu.Lock()
	s.known[holder] = standingEntry{at: now, v: v, err: err}
	s.mu.Unlock()

	return v, err
}

func refusedByRoster(err error) bool {
	return errors.Is(err, identity.ErrNoPerson) || errors.Is(err, identity.ErrNoTenant)
}

// errStanding is a session whose person roster says is not in good
// standing: no good, as a credential, so the caller is told to sign in.
var errStanding = errors.New("this session's person is not in good standing at roster")

// check says whether a session may still be served: its person is there
// at roster, not suspended or erased, and was not signed out everywhere
// since it was minted: at or after, as roster holds what it issued itself
// (its `server/keys`).
func (s *standing) check(ctx context.Context, v authsession.Session) error {
	holder, err := pdid.Parse(v.Id)
	if err != nil {
		return fmt.Errorf("%w: %w", errStanding, err)
	}
	tenant, err := pdid.Parse(v.TenantId)
	if err != nil {
		return fmt.Errorf("%w: %w", errStanding, err)
	}
	ns, err := strconv.ParseInt(v.Held[heldIssued], 10, 64)
	if err != nil {
		return fmt.Errorf("%w: the session does not say when it began", errStanding)
	}

	st, err := s.of(ctx, tenant, holder)
	switch {
	case refusedByRoster(err):
		return fmt.Errorf("%w: %w", errStanding, err)
	case err != nil:
		return fmt.Errorf("%w: roster: %w", auth.ErrUnavailable, err)
	case !st.Erased.IsZero():
		return fmt.Errorf("%w: erased", errStanding)
	case !st.Disabled.IsZero():
		return fmt.Errorf("%w: suspended", errStanding)
	case !st.Invalidated.IsZero() && !st.Invalidated.Before(time.Unix(0, ns)):
		return fmt.Errorf("%w: signed out everywhere since this session began", errStanding)
	}

	return nil
}

// sessionHandler reads a surface's session cookie as payday's handler
// does, and then holds the session to its person's standing. The session
// is opened a second time for what it holds, which the identity the
// handler answers does not carry.
func (s *Server) sessionHandler(ss *authsession.Sessions) auth.Handler {
	base := ss.Handler()

	return auth.HandlerFunc(func(ctx context.Context) (auth.Identity, error) {
		id, err := base.Handle(ctx)
		if err != nil {
			return id, err
		}
		md, _ := metadata.FromIncomingContext(ctx)
		v, err := ss.Read(ctx, ss.KeyOf(md.Get("cookie")))
		if err != nil {
			return auth.Identity{}, err
		}
		if err := s.standing.check(ctx, v); err != nil {
			if errors.Is(err, errStanding) {
				// The browser is told to drop it, as for a session that
				// ran out: it will not open again.
				_ = grpc.SetHeader(ctx, metadata.Pairs("set-cookie", ss.End(ctx, "").String()))
			}

			return auth.Identity{}, err
		}

		return id, nil
	})
}
