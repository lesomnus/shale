package sso_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/sso"
	"github.com/lesomnus/shale/internal/sso/ssotest"
)

func rp(t *testing.T, idp *ssotest.Idp) *sso.RP {
	r, err := sso.New(sso.Config{Issuer: idp.URL, ClientId: "shale", ClientSecret: "s3cret", DeviceClientId: "shale-cli"}, []byte("a key"), nil)
	require.NoError(t, err)

	return r
}

// A flow is bound to the browser that began it and the listener it began
// on: a cookie from another key, another surface, a state that is not the
// cookie's, or one past its lifetime is refused before anything is asked
// of the issuer.
func TestFlowRefusals(t *testing.T) {
	ctx := context.Background()
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	r := rp(t, idp)
	sealed, to, err := r.Begin(ctx, "tenant", "https://shale.example", "https://shale.example/#/live")
	require.NoError(t, err)
	u, err := url.Parse(to)
	require.NoError(t, err)
	state := u.Query().Get("state")
	require.Equal(t, "https://shale.example/sso/callback", u.Query().Get("redirect_uri"))
	require.Equal(t, "S256", u.Query().Get("code_challenge_method"))
	require.NotEmpty(t, u.Query().Get("nonce"))
	require.NotContains(t, sealed, state, "the cookie is sealed, not the state in clear")

	q := url.Values{"code": {"c"}, "state": {state}}
	for name, tc := range map[string]struct {
		surface, cookie string
		q               url.Values
	}{
		"no cookie":          {"tenant", "", q},
		"tampered":           {"tenant", func() string { s, _, _ := rp(t, idp).Begin(ctx, "tenant", "https://x", ""); return s[:len(s)-2] + "AA" }(), q},
		"another surface":    {"cluster", sealed, q},
		"another state":      {"tenant", sealed, url.Values{"code": {"c"}, "state": {"nope"}}},
		"no state":           {"tenant", sealed, url.Values{"code": {"c"}}},
		"the issuer said no": {"tenant", sealed, url.Values{"error": {"access_denied"}, "state": {state}}},
	} {
		_, err := r.Finish(ctx, tc.surface, "https://shale.example", tc.cookie, tc.q)
		require.ErrorIs(t, err, sso.ErrRefused, name)
	}

	r.Now = func() time.Time { return time.Now().Add(sso.FlowLifetime + time.Minute) }
	_, err = r.Finish(ctx, "tenant", "https://shale.example", sealed, q)
	require.ErrorIs(t, err, sso.ErrRefused, "expired")
}

// The logout hop asks for the way back only with the hint, which is the
// only way the issuer will honour it.
func TestLogoutURL(t *testing.T) {
	ctx := context.Background()
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	r := rp(t, idp)

	with, err := url.Parse(r.LogoutURL(ctx, "the-token", "https://shale.example/"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(with.String(), idp.URL+"/oauth2/sessions/logout"))
	require.Equal(t, "the-token", with.Query().Get("id_token_hint"))
	require.Equal(t, "https://shale.example/", with.Query().Get("post_logout_redirect_uri"))
	require.Equal(t, "shale", with.Query().Get("client_id"))

	without, err := url.Parse(r.LogoutURL(ctx, "", "https://shale.example/"))
	require.NoError(t, err)
	require.Empty(t, without.Query().Get("post_logout_redirect_uri"))
	require.Empty(t, without.Query().Get("id_token_hint"))
}

// The CLI's token: the CLI's audience, fresh, signed by the issuer.
func TestVerifyDevice(t *testing.T) {
	ctx := context.Background()
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	r := rp(t, idp)
	now := time.Now()
	tok := func(aud string, iat time.Time) string {
		return idp.Sign(t, map[string]any{"iss": idp.URL, "aud": aud, "sub": "someone", "iat": iat.Unix(), "exp": iat.Add(2 * time.Hour).Unix(), "name": "Some One"})
	}

	v, err := r.VerifyDevice(ctx, tok("shale-cli", now))
	require.NoError(t, err)
	require.Equal(t, "someone", v.Token.Subject)
	require.Equal(t, "Some One", v.Name)

	_, err = r.VerifyDevice(ctx, tok("shale", now))
	require.ErrorIs(t, err, sso.ErrRefused)
	_, err = r.VerifyDevice(ctx, tok("shale-cli", now.Add(-sso.DeviceFreshness-time.Minute)))
	require.ErrorIs(t, err, sso.ErrRefused)

	other := ssotest.New(t, "shale", "s3cret", "shale-cli")
	_, err = r.VerifyDevice(ctx, other.Sign(t, map[string]any{"iss": idp.URL, "aud": "shale-cli", "sub": "x", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}))
	require.ErrorIs(t, err, sso.ErrRefused, "another issuer's key")

	none, err := sso.New(sso.Config{Issuer: idp.URL, ClientId: "shale"}, []byte("k"), nil)
	require.NoError(t, err)
	_, err = none.VerifyDevice(ctx, tok("shale-cli", now))
	require.ErrorIs(t, err, sso.ErrRefused, "no device client configured")
}

// What the configuration is refused for, at start.
func TestConfig(t *testing.T) {
	_, err := sso.New(sso.Config{}, []byte("k"), nil)
	require.Error(t, err)
	_, err = sso.New(sso.Config{Issuer: "https://sso.example"}, []byte("k"), nil)
	require.Error(t, err, "no client")
	_, err = sso.New(sso.Config{Issuer: "https://sso.example", ClientId: "c", ClientSecret: "env:SHALE_TEST_NOT_SET_ANYWHERE"}, []byte("k"), nil)
	require.Error(t, err, "a secret that is not there")
	_, err = sso.New(sso.Config{Issuer: "https://sso.example", ClientId: "c", TenantOrigin: "shale.example"}, []byte("k"), nil)
	require.Error(t, err, "not an origin")
}
