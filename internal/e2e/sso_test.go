package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso/ssotest"
)

// browser is a client with a cookie jar that follows redirects, which is a
// browser as far as a sign-in is concerned.
func browser(t *testing.T) *http.Client {
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)

	return &http.Client{Jar: jar, Timeout: 30 * time.Second}
}

func get(t *testing.T, c *http.Client, u string) (*http.Response, []byte) {
	t.Helper()
	resp, err := c.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	return resp, b
}

// cookieOf is the session cookie a jar holds for a listener, as a header.
func cookieOf(c *http.Client, base, name string) string {
	u, _ := url.Parse(base)
	for _, ck := range c.Jar.Cookies(u) {
		if ck.Name == name {
			return ck.Name + "=" + ck.Value
		}
	}

	return ""
}

func ssoCluster(t *testing.T) (*cluster, *ssotest.Idp, string, string) {
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Team: "shale-ops", Ttl: time.Second}
		c.Auth.Oidc.Issuer = idp.URL
		c.Auth.Oidc.ClientId = "shale"
		c.Auth.Oidc.ClientSecret = "s3cret"
		c.Auth.Oidc.DeviceClientId = "shale-cli"
		c.Auth.SsoOnly = true
	})
	var tenant, clusterHttp string
	require.Eventually(t, func() bool {
		tenant, clusterHttp = c.running.CP.HttpAddr(cmd.SurfaceTenant), c.running.CP.HttpAddr(cmd.SurfaceCluster)
		return tenant != "" && clusterHttp != ""
	}, 10*time.Second, 50*time.Millisecond)

	return c, idp, "http://" + tenant, "http://" + clusterHttp
}

// Signing in through the issuer, on both listeners (§33.1): the flow
// carries state, a nonce and PKCE and comes back to the listener it began
// on; the session it ends in is the same cookie a password mints and
// answers `GET /session`; the cluster listener takes operators only;
// somebody in no tenant here is refused; a password is refused when SSO is
// the only way; and signing out ends this session and then sends the
// browser to the issuer with the hint it needs to send it back.
func TestSsoBrowser(t *testing.T) {
	c, idp, tenant, clusterBase := ssoCluster(t)
	ctx := context.Background()
	admin, err := c.running.CP.Identity.Lookup(ctx, "acme", "admin")
	require.NoError(t, err)
	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)

	// How one signs in here: the issuer, and no password.
	resp, b := get(t, http.DefaultClient, tenant+"/session/ways")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ways cmd.Ways
	require.NoError(t, json.Unmarshal(b, &ways))
	require.Equal(t, cmd.Ways{Password: false, Sso: true, Issuer: idp.URL, Login: "/sso/login", DeviceClientId: "shale-cli"}, ways)
	body, _ := json.Marshal(map[string]string{"tenant": "acme", "alias": "admin", "password": "anything"})
	resp, err = http.Post(tenant+"/session", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "SSO only: a password is refused")

	// Bob signs in to the tenant listener and lands where he asked to.
	idp.As(bob.Id.String())
	asBob := browser(t)
	resp, _ = get(t, asBob, tenant+"/sso/login?next="+url.QueryEscape("/#/cameras"))
	// The console's own page, which a test binary was built without.
	require.Equal(t, strings.TrimPrefix(tenant, "http://"), resp.Request.URL.Host)
	require.Equal(t, "/", resp.Request.URL.Path)
	asked := idp.Authorized[len(idp.Authorized)-1]
	require.Equal(t, tenant+"/sso/callback", asked.Get("redirect_uri"))
	require.Equal(t, "S256", asked.Get("code_challenge_method"))
	require.NotEmpty(t, asked.Get("nonce"))

	resp, b = get(t, asBob, tenant+"/session")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var who cmd.Who
	require.NoError(t, json.Unmarshal(b, &who))
	require.Equal(t, "@acme/bob", who.Who)
	require.True(t, who.Sso)
	require.False(t, who.Operator)

	// The cookie is a session like any other: Bob reads, and writes nothing.
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	as := metadata.AppendToOutgoingContext(ctx, "cookie", cookieOf(asBob, tenant, "shale_tenant"))
	_, err = api.NewSetServiceClient(conn).List(as, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	_, err = api.NewSetServiceClient(conn).Add(as, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "mine"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// The cluster listener does not sign him in.
	resp, _ = get(t, asBob, clusterBase+"/sso/login")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	resp, _ = get(t, asBob, clusterBase+"/session")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// The admin, an operator by the team init made, is signed in there, and
	// sent back to the console's origin rather than anywhere else.
	idp.As(admin.Id.String())
	asAdmin := browser(t)
	resp, _ = get(t, asAdmin, clusterBase+"/sso/login?next="+url.QueryEscape(tenant+"/#/hosts"))
	require.Equal(t, strings.TrimPrefix(tenant, "http://"), resp.Request.URL.Host)
	resp, b = get(t, asAdmin, clusterBase+"/session")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.Unmarshal(b, &who))
	require.Equal(t, "@acme/admin", who.Who)
	require.True(t, who.Operator)
	cconn, err := grpc.NewClient(c.running.ClusterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cconn.Close()
	_, err = api.NewNodeServiceClient(cconn).List(metadata.AppendToOutgoingContext(ctx, "cookie", cookieOf(asAdmin, clusterBase, "shale_cluster")), api.NodeListRequest_builder{}.Build())
	require.NoError(t, err)

	// `next` on an origin this deployment does not answer at is not followed.
	resp, _ = get(t, asAdmin, tenant+"/sso/login?next="+url.QueryEscape("https://evil.example/"))
	require.Equal(t, strings.TrimPrefix(tenant, "http://"), resp.Request.URL.Host)

	// Somebody the issuer vouches for who is in no tenant here is nobody.
	idp.As(pdid.New(2).String())
	resp, _ = get(t, browser(t), tenant+"/sso/login")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	// A callback that no flow began is refused.
	resp, _ = get(t, browser(t), tenant+"/sso/callback?code=x&state=y")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Signing out: this session ends, then the issuer is asked to end its
	// own, with the hint, and to send the browser back to the console.
	resp, _ = get(t, asBob, tenant+"/sso/logout?next=/")
	require.Equal(t, strings.TrimPrefix(tenant, "http://"), resp.Request.URL.Host)
	out := idp.LastLogout()
	require.NotNil(t, out)
	require.NotEmpty(t, out.Get("id_token_hint"))
	require.Equal(t, tenant+"/", out.Get("post_logout_redirect_uri"))
	require.Equal(t, "shale", out.Get("client_id"))
	resp, _ = get(t, asBob, tenant+"/session")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// The CLI signs an operator in with the device flow (§33.1): one code,
// a session on each surface; a person who is not an operator is refused;
// and the token endpoint takes only a fresh token minted for the CLI's
// client.
func TestSsoCli(t *testing.T) {
	c, idp, tenant, clusterBase := ssoCluster(t)
	ctx := context.Background()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	admin, err := c.running.CP.Identity.Lookup(ctx, "acme", "admin")
	require.NoError(t, err)
	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)

	cfg := *c.cfg
	cfg.Client.Addr, cfg.Client.Web = "http://"+c.running.TenantAddr, tenant
	cfg.Client.ClusterAddr, cfg.Client.ClusterWeb = "http://"+c.running.ClusterAddr, clusterBase
	login := func(who identity.Person) (string, error) {
		idp.As(who.Id.String())
		var out bytes.Buffer
		done := make(chan error, 1)
		go func() { done <- cli.LoginSso(ctx, &cfg, &out, false) }()
		require.Eventually(t, func() bool { return idp.Pending() > 0 }, 10*time.Second, 20*time.Millisecond)
		idp.Approve()
		select {
		case err := <-done:
			return out.String(), err
		case <-time.After(30 * time.Second):
			t.Fatal("the device flow did not finish")
			return "", nil
		}
	}

	printed, err := login(bob)
	require.Error(t, err, "operators only")
	require.Contains(t, printed, "BCDF-GHJK", "the code to confirm is printed")
	require.Empty(t, cli.SavedSession(cfg.Client.Addr))

	_, err = login(admin)
	require.NoError(t, err)
	for _, at := range []struct{ addr, api string }{{cfg.Client.Addr, c.running.TenantAddr}, {cfg.Client.ClusterAddr, c.running.ClusterAddr}} {
		cookie := cli.SavedSession(at.addr)
		require.NotEmpty(t, cookie, at.addr)
		conn, err := grpc.NewClient(at.api, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()
		as := metadata.AppendToOutgoingContext(ctx, "cookie", cookie)
		if at.api == c.running.ClusterAddr {
			_, err = api.NewNodeServiceClient(conn).List(as, api.NodeListRequest_builder{}.Build())
		} else {
			_, err = api.NewSetServiceClient(conn).Add(as, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "from-cli"}.Build())
		}
		require.NoError(t, err, at.api)
	}

	// The token endpoint: only a fresh token, for the CLI's client.
	post := func(tok string) int {
		b, _ := json.Marshal(map[string]string{"id_token": tok})
		resp, err := http.Post(tenant+"/sso/token", "application/json", bytes.NewReader(b))
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	now := time.Now()
	claims := func(aud string, iat time.Time) map[string]any {
		return map[string]any{"iss": idp.URL, "aud": aud, "sub": admin.Id.String(), "iat": iat.Unix(), "exp": iat.Add(2 * time.Hour).Unix()}
	}
	require.Equal(t, http.StatusNoContent, post(idp.Sign(t, claims("shale-cli", now))))
	require.Equal(t, http.StatusUnauthorized, post(idp.Sign(t, claims("shale", now))), "the browser's client")
	require.Equal(t, http.StatusUnauthorized, post(idp.Sign(t, claims("shale-cli", now.Add(-time.Hour)))), "stale")
	require.Equal(t, http.StatusUnauthorized, post(idp.Sign(t, claims("shale-cli", now))[:40]+"x"), "garbage")
}

// Behind an Ingress the cluster listener is another name on 443, and the
// console's guess -- the tenant listener's host one port up -- is a port
// nothing answers on (§40.4). `GET /session/ways` names it instead.
func TestWaysNamesTheClusterListener(t *testing.T) {
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Oidc.Issuer = idp.URL
		c.Auth.Oidc.ClientId = "shale"
		c.Auth.Oidc.ClientSecret = "s3cret"
		c.Auth.Oidc.ClusterOrigin = "https://ops.shale.example"
	})
	var tenant string
	require.Eventually(t, func() bool {
		tenant = c.running.CP.HttpAddr(cmd.SurfaceTenant)
		return tenant != ""
	}, 10*time.Second, 50*time.Millisecond)

	resp, b := get(t, http.DefaultClient, "http://"+tenant+"/session/ways")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var ways cmd.Ways
	require.NoError(t, json.Unmarshal(b, &ways))
	require.Equal(t, "https://ops.shale.example", ways.Cluster)
}
