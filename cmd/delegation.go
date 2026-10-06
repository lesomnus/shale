package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso"
	"github.com/lesomnus/shale/server/core"
)

// DelegationConfig is `auth.delegation` (§33.8): which apps may view on
// their people's behalf, by the OAuth client their people's tokens are
// issued to, and the audience those tokens are for. Empty serves none.
type DelegationConfig struct {
	// Audience is Shale's API as the issuer names it, e.g.
	// `urn:hday:api:shale`: an access token is believed only when its
	// audience is this, and an app's client is registered at the issuer
	// with it among its audiences.
	Audience string `yaml:"audience"`
	// Clients maps an app's OAuth client ID to the app's name at roster:
	// the alias its holder has in every tenant (`roster app install`).
	Clients map[string]string `yaml:"clients"`
	// MaxTtl is the longest one delegation lives, however it is renewed;
	// 12 h by default.
	MaxTtl time.Duration `yaml:"max_ttl"`
	// TokenTtl is the longest a view or read token issued under one lives,
	// and so how long a live view outlives the delegation; 5 min by
	// default.
	TokenTtl time.Duration `yaml:"token_ttl"`
}

// On says whether any app may view on anybody's behalf here.
func (c DelegationConfig) On() bool { return len(c.Clients) > 0 }

// check refuses a configuration that could not be served.
func (c DelegationConfig) check(a AuthConfig) error {
	if !c.On() {
		return nil
	}
	switch {
	case c.Audience == "":
		return errors.New("auth.delegation.audience: say what the issuer calls Shale's API, so a token for another API is not believed here")
	case !a.Oidc.On():
		return errors.New("auth.delegation: a person is proved by an access token from the issuer, and there is none (auth.oidc.issuer)")
	case !a.Operators.On():
		return errors.New("auth.delegation: what an app may do for people is what roster grants its holder, which needs auth.operators")
	}
	for client, app := range c.Clients {
		if strings.TrimSpace(client) == "" || strings.TrimSpace(app) == "" {
			return fmt.Errorf("auth.delegation.clients: %q: %q is not a client and an app", client, app)
		}
	}

	return nil
}

func (c DelegationConfig) core() core.DelegationConfig {
	return core.DelegationConfig{MaxTtl: c.MaxTtl, TokenTtl: c.TokenTtl}
}

// delegations is core.Delegations over this server: the issuer it signs
// people in through, and roster.
type delegations struct{ s *Server }

func (d delegations) Person(ctx context.Context, token string) (*api.Holder, string, error) {
	s := d.s
	if s.Sso == nil {
		return nil, "", status.Error(codes.FailedPrecondition, "no issuer here (auth.oidc)")
	}
	sub, client, err := s.Sso.AccessToken(ctx, token, s.cfg.Auth.Delegation.Audience)
	if err != nil {
		if errors.Is(err, sso.ErrIssuerDown) {
			return nil, "", status.Errorf(codes.Unavailable, "%v", err)
		}

		return nil, "", err
	}
	p, err := s.Identity.ById(ctx, sub)
	if err != nil {
		if errors.Is(err, identity.ErrNoPerson) || errors.Is(err, identity.ErrRefused) {
			return nil, "", err
		}

		return nil, "", status.Errorf(codes.Unavailable, "roster: %v", err)
	}
	h, err := s.Provision(ctx, p)
	if err != nil {
		return nil, "", err
	}

	return h, client, nil
}

func (d delegations) App(ctx context.Context, tenant pdid.Id, token string) (pdid.Id, string, error) {
	p, err := d.s.Identity.App(ctx, tenant, token)
	if err != nil {
		if errors.Is(err, identity.ErrRefused) || errors.Is(err, identity.ErrNoPerson) {
			return pdid.Nil, "", err
		}

		return pdid.Nil, "", status.Errorf(codes.Unavailable, "%v", err)
	}

	return p.Id, p.Alias, nil
}

func (d delegations) Standing(ctx context.Context, tenant, holder pdid.Id) error {
	err := d.s.Identity.Standing(ctx, tenant, holder)
	if err == nil || errors.Is(err, identity.ErrRefused) || errors.Is(err, identity.ErrNoPerson) {
		return err
	}

	return status.Errorf(codes.Unavailable, "roster: %v", err)
}

func (d delegations) AppOf(client string) (string, bool) {
	v, ok := d.s.cfg.Auth.Delegation.Clients[client]

	return v, ok
}

// followStanding ends the delegations of whoever roster says stopped being
// in good standing (§33.8): signed out everywhere, suspended, erased. Where
// roster cannot say it as it happens -- the one in this process, a key
// not allowed the stream -- a delegation lasts until it is renewed and
// roster is asked about the person, or until it ends.
func (s *Server) followStanding(ctx context.Context) error {
	c := core.Core{}.WithDeps(s.Deps)
	err := s.Identity.Watch(ctx, func(ctx context.Context, v identity.Standing) {
		why := "signed out everywhere"
		switch {
		case !v.Erased.IsZero():
			why = "erased at roster"
		case !v.Disabled.IsZero():
			why = "suspended at roster"
		}
		since := v.Since()
		if v.Invalidated.Equal(since) && v.Disabled.IsZero() && v.Erased.IsZero() {
			// Signed out everywhere: what was issued before it.
		} else {
			// Suspended or erased: all of it.
			since = time.Now()
		}
		n, err := c.Unstanding(ctx, v.Holder, since, why)
		if err != nil {
			slog.Warn("delegation: could not end a person's delegations", "person", v.Holder.String(), "why", why, "err", err.Error())
			return
		}
		if n > 0 {
			slog.Info("delegation: ended a person's delegations", "person", v.Holder.String(), "why", why, "n", n)
		}
	})
	if errors.Is(err, identity.ErrNoSync) {
		slog.Warn("delegation: roster cannot say who stops being in good standing as it happens, so a delegation lasts until its next renewal asks", "err", err.Error())
		return nil
	}

	return err
}
