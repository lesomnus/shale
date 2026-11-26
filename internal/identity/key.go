package identity

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"
	"github.com/lesomnus/roster/rstr"
)

// PrefixPersonKey is how a person's key for Shale is told apart from every
// other bearer before roster is asked anything: roster mints a key of a
// tenant's people, for roster itself or for an app (`ApiKey.app`), with
// this prefix (roster's `server/keys`). Which app it is for is roster's to
// say, at `Introspect`.
const PrefixPersonKey = prefixTenantKey

// ErrNoKey is a key roster answers to Shale in no tenant this deployment
// serves: never minted, ended, of somebody suspended or erased, a key of
// roster's own, or a key for another app. roster says the same
// `no such token` of each, and so does this.
var ErrNoKey = errors.New("no such key for Shale")

// Key is who a person's key for Shale acts as, and what it allows, as
// roster answers `TokenService/Introspect` asked as Shale (roster's
// `docs/apps.md`, *Giving an app's users API tokens*): the holder, their
// tenant, the key's methods and its end. Not which key: roster's answer
// does not say.
//
// A key names no tenant, and Shale's deployment key holds nothing without
// `roster-at`, so it is asked in each tenant this deployment serves, the
// way a person is found by identifier ([Store.ById]), until one of them
// answers somebody. Where the configuration says the tenant -- a tenant
// key, or the tenant keys -- that is one asking. A tenant that answers
// `no such token` is the next one's turn; a roster that could not be asked
// is [auth.ErrUnavailable], whatever the other tenants would have said,
// since one of them may have been the key's.
func (s *Store) Key(ctx context.Context, token string) (auth.Identity, error) {
	if !strings.HasPrefix(token, PrefixPersonKey) {
		return auth.Identity{}, ErrNoKey
	}
	tenants, err := s.Tenants(ctx)
	if err != nil {
		return auth.Identity{}, fmt.Errorf("%w: roster: the tenants served here: %w", auth.ErrUnavailable, err)
	}
	slices.Sort(tenants)
	for _, t := range tenants {
		as, err := s.as(ctx, t)
		if err != nil {
			if errors.Is(err, ErrNoTenant) {
				continue
			}

			return auth.Identity{}, fmt.Errorf("%w: roster: %w", auth.ErrUnavailable, err)
		}
		res, err := pdpb.NewTokenServiceClient(s.conn).Introspect(as, pdpb.TokenIntrospectRequest_builder{Token: token}.Build())
		if err != nil {
			if status.Code(err) == codes.NotFound {
				continue
			}

			// A PermissionDenied is Shale's key not allowed Introspect
			// in that tenant (`app install --allow`), and is the
			// operator's to mend, not the caller's: Unavailable, as
			// payday's `auth.Remote` says it.
			return auth.Identity{}, fmt.Errorf("%w: roster: introspect in %s: %w", auth.ErrUnavailable, t, err)
		}
		id, err := auth.IdentityFrom(res)
		if err != nil {
			return auth.Identity{}, fmt.Errorf("%w: roster: introspect in %s: %w", auth.ErrUnavailable, t, err)
		}
		if id.Id == "" {
			return auth.Identity{}, ErrNoKey
		}

		return id, nil
	}

	return auth.Identity{}, ErrNoKey
}

// PersonIn is a person of a tenant this deployment serves, by identifier,
// asked as Shale in that tenant: who a key's holder is, for the rows a
// first arrival is given. Somebody suspended is ErrRefused, nobody there
// ErrNoPerson, as at a sign-in.
func (s *Store) PersonIn(ctx context.Context, tenant, holder pdid.Id) (Person, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return Person{}, err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return Person{}, err
	}

	return s.person(as, rstr.HolderRef_builder{Id: holder.Bytes()}.Build())
}
