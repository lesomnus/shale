package identity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"
	"github.com/lesomnus/roster/rstr"
)

// App is the app an exchange token names (§33.8): roster's
// `DelegationService/Exchange` mints one about the app that asked, issued to
// Shale's holder in the app's tenant, and only that holder may introspect
// it -- so it is asked as Shale in tenant, and a token issued to anybody
// else, or in another tenant, is ErrRefused like one that never was.
func (s *Store) App(ctx context.Context, tenant pdid.Id, token string) (Person, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return Person{}, err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return Person{}, err
	}
	v, err := pdpb.NewTokenServiceClient(s.conn).Introspect(as, pdpb.TokenIntrospectRequest_builder{Token: token}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return Person{}, fmt.Errorf("%w: no such exchange token for Shale in %s", ErrRefused, alias)
		}

		return Person{}, fmt.Errorf("roster: introspect: %w", err)
	}
	id, err := pdid.From(v.GetId())
	if err != nil {
		return Person{}, fmt.Errorf("roster: introspect: %w", err)
	}

	return s.person(as, rstr.HolderRef_builder{Id: id.Bytes()}.Build())
}

// Standing is what roster says of somebody now, from its sync stream: when
// everything issued to them before stopped being good, when they were
// suspended, when they were erased; zero for not.
type Standing struct {
	Holder, Tenant                pdid.Id
	Invalidated, Disabled, Erased time.Time
}

// Since is the latest of the three: what was issued before it, or at all
// when they are suspended or erased, is not good. Zero is good standing.
func (v Standing) Since() time.Time {
	t := v.Invalidated
	for _, u := range []time.Time{v.Disabled, v.Erased} {
		if u.After(t) {
			t = u
		}
	}

	return t
}

// ErrNoSync is a roster that cannot say who stopped being in good standing
// as it happens: the one in this process has no broker to carry it, and a
// key not allowed `/roster.SyncService/Watch` is refused it.
var ErrNoSync = errors.New("roster: no sync stream")

// Watch follows roster's `SyncService.Watch` (§33.8) until ctx ends,
// telling `fn` what it hears, and dialing again after a break, a second
// later and doubling to thirty. Nothing is replayed across a break, and
// what arrives is state rather than a change, so `fn` is told about
// somebody again after one: it has to be safe to repeat.
//
// It answers ErrNoSync at once where there is no stream to follow, and
// when roster refuses it; the caller says so and does without.
func (s *Store) Watch(ctx context.Context, fn func(context.Context, Standing)) error {
	if s.em != nil {
		return fmt.Errorf("%w: roster runs in this process, with no broker", ErrNoSync)
	}
	var creds []string
	switch {
	case s.key != "":
		creds = []string{s.key}
	default:
		for _, k := range s.keys {
			creds = append(creds, k)
		}
	}
	if len(creds) == 0 {
		return fmt.Errorf("%w: no key", ErrNoSync)
	}

	errs := make(chan error, len(creds))
	for _, k := range creds {
		go func() { errs <- s.follow(ctx, k, fn) }()
	}
	var err error
	for range creds {
		err = errors.Join(err, <-errs)
	}

	return err
}

func (s *Store) follow(ctx context.Context, key string, fn func(context.Context, Standing)) error {
	wait := time.Second
	for ctx.Err() == nil {
		began := time.Now()
		err := s.stream(auth.BearerProvider(key).Provide(ctx), fn)
		if ctx.Err() != nil {
			return nil
		}
		switch status.Code(err) {
		case codes.Unimplemented, codes.PermissionDenied:
			return fmt.Errorf("%w: %w", ErrNoSync, err)
		}
		if time.Since(began) > time.Minute {
			wait = time.Second
		}
		s.log.Warn("roster: the sync stream broke; dialing again", "in", wait.String(), "err", fmt.Sprint(err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(2*wait, 30*time.Second)
	}

	return nil
}

// stream is one connection: what it hears about somebody is told once per
// standing, so a repeat of the same state wakes nobody.
func (s *Store) stream(ctx context.Context, fn func(context.Context, Standing)) error {
	st, err := rstr.NewSyncServiceClient(s.conn).Watch(ctx, rstr.SyncWatchRequest_builder{}.Build())
	if err != nil {
		return err
	}
	seen := map[pdid.Id]time.Time{}
	for {
		e, err := st.Recv()
		if err != nil {
			return err
		}
		holder, err := pdid.From(e.GetHolder())
		if err != nil {
			continue
		}
		tenant, _ := pdid.From(e.GetTenant())
		v := Standing{Holder: holder, Tenant: tenant}
		if t := e.GetDateInvalidated(); t != nil {
			v.Invalidated = t.AsTime()
		}
		if t := e.GetDateDisabled(); t != nil {
			v.Disabled = t.AsTime()
		}
		if t := e.GetDateErased(); t != nil {
			v.Erased = t.AsTime()
		}
		since := v.Since()
		if since.IsZero() || !since.After(seen[holder]) {
			continue
		}
		seen[holder] = since
		fn(ctx, v)
	}
}

// Standing is whether somebody of a tenant is in good standing at roster
// now: there, and not suspended. ErrNoPerson is somebody roster does not
// have any more, ErrRefused somebody suspended; anything else is a roster
// that could not be asked.
func (s *Store) Standing(ctx context.Context, tenant, holder pdid.Id) error {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return err
	}
	_, err = s.person(as, rstr.HolderRef_builder{Id: holder.Bytes()}.Build())

	return err
}
