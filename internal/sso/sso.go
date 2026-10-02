// Package sso is Shale as a relying party of an OIDC issuer (§33.1): roster
// with Ory Hydra in front, in the deployment it is written for.
//
// It is roster's `itself` shape (roster's docs/relying-party.md and
// examples/product): the control plane does the exchange, verifies the
// id_token, and from then on holds a session of its own -- payday's sealed
// cookie, the same one a password sign-in mints. The token is used once, at
// the callback, to learn who this is; what is kept of it is the token itself,
// in the session, for one purpose: the `id_token_hint` the issuer requires
// before it sends a browser back after a logout.
//
// Identity is `sub` and nothing else (`authoidc.Subject`'s rule). roster's
// `sub` is a `Holder.id`, globally unique, which is what Shale's own Holder
// rows are anchored on; the claims beside it are decoration.
//
// What this package does not do is decide who may sign in to which
// surface, or mint the session: that is the control plane's (`cmd`), which
// asks roster who the `sub` is with the tenant key, and the team who the
// operators are.
package sso

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// Config is `auth.oidc` (§36.1).
type Config struct {
	// Issuer is the OIDC issuer, e.g. https://sso.hday.dev. Empty turns
	// single sign-on off.
	Issuer string `yaml:"issuer"`
	// ClientId is the browser's client at the issuer: a confidential one,
	// with the authorization code grant.
	ClientId string `yaml:"client_id"`
	// ClientSecret is its secret: `env:NAME`, `file:PATH`, or the secret
	// itself, as `auth.roster.keys` is. Sent as HTTP Basic, which is what the
	// client must be registered with (`client_secret_basic`).
	ClientSecret string `yaml:"client_secret"`
	// DeviceClientId is the CLI's client: a public one (no secret), with the
	// device grant of RFC 8628. Empty is no single sign-on from the CLI.
	DeviceClientId string `yaml:"device_client_id"`
	// TenantOrigin and ClusterOrigin are where a browser reaches the two
	// HTTP listeners, e.g. https://shale.hday.dev and
	// https://ops.shale.hday.dev: the callback is `<origin>/sso/callback`
	// and a logout comes back to `<origin>/`. Empty derives it from the
	// request, which is right for development and wrong behind a proxy.
	TenantOrigin  string `yaml:"tenant_origin"`
	ClusterOrigin string `yaml:"cluster_origin"`
	// Scopes asked for; `openid profile email` by default.
	Scopes []string `yaml:"scopes"`
	// CaFile is the CA the issuer's certificate chains to; empty is the
	// system pool.
	CaFile string `yaml:"ca_file"`
}

// On says whether single sign-on is configured.
func (c Config) On() bool { return c.Issuer != "" }

// Paths on each HTTP listener.
const (
	PathLogin    = "/sso/login"
	PathCallback = "/sso/callback"
	PathLogout   = "/sso/logout"
	PathToken    = "/sso/token"
)

// FlowLifetime is how long a browser has between leaving for the issuer and
// coming back.
const FlowLifetime = 10 * time.Minute

// DeviceFreshness is how old an id_token the CLI hands over may be: it was
// minted for this exchange a moment ago, and one older than this is one
// that has been sitting somewhere.
const DeviceFreshness = 10 * time.Minute

// RP is the relying party: one per process, serving both surfaces.
type RP struct {
	cfg    Config
	secret string
	client *http.Client
	aead   cipher.AEAD
	log    *slog.Logger
	// Now is the clock.
	Now func() time.Time

	mu         sync.Mutex
	provider   *oidc.Provider
	endSession string
}

// New is the relying party, sealing its flows under `key`. The issuer is
// asked for its discovery document on first use, and again until it
// answers, so an issuer that is not up yet does not stop the control plane.
func New(cfg Config, key []byte, log *slog.Logger) (*RP, error) {
	if !cfg.On() {
		return nil, errors.New("auth.oidc.issuer: no issuer")
	}
	if cfg.ClientId == "" {
		return nil, errors.New("auth.oidc.client_id: the browser's client at the issuer")
	}
	secret := ""
	if cfg.ClientSecret != "" {
		v, err := resolveRef(cfg.ClientSecret)
		if err != nil {
			return nil, fmt.Errorf("auth.oidc.client_secret: %w", err)
		}
		secret = v
	}
	for name, v := range map[string]string{"tenant_origin": cfg.TenantOrigin, "cluster_origin": cfg.ClusterOrigin} {
		if v == "" {
			continue
		}
		if _, err := origin(v); err != nil {
			return nil, fmt.Errorf("auth.oidc.%s: %w", name, err)
		}
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if cfg.CaFile != "" {
		pem, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, fmt.Errorf("auth.oidc.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("auth.oidc.ca_file: %s holds no certificate", cfg.CaFile)
		}
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	}
	h := hmac.New(sha256.New, key)
	h.Write([]byte("shale sso flow"))
	block, err := aes.NewCipher(h.Sum(nil))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}

	return &RP{cfg: cfg, secret: secret, client: client, aead: aead, log: log, Now: time.Now}, nil
}

// Config is what this was configured with.
func (rp *RP) Config() Config { return rp.cfg }

func (rp *RP) ctx(ctx context.Context) context.Context { return oidc.ClientContext(ctx, rp.client) }

// Provider is the issuer's discovery document, read once it answers.
func (rp *RP) Provider(ctx context.Context) (*oidc.Provider, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.provider != nil {
		return rp.provider, nil
	}
	p, err := oidc.NewProvider(rp.ctx(ctx), rp.cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("the issuer %s: %w", rp.cfg.Issuer, err)
	}
	// `end_session_endpoint` is not in oidc.Provider's struct, so it is read
	// off the raw document; empty is an issuer with no logout of its own.
	var discovered struct {
		EndSession string `json:"end_session_endpoint"`
	}
	_ = p.Claims(&discovered)
	rp.provider, rp.endSession = p, discovered.EndSession
	rp.log.Info("sso: the issuer", "issuer", rp.cfg.Issuer, "end_session", discovered.EndSession != "", "device", p.Endpoint().DeviceAuthURL != "")

	return p, nil
}

func (rp *RP) oauth(p *oidc.Provider, base string) *oauth2.Config {
	// How the secret is sent, said rather than probed: x/oauth2 caches what
	// a probe found for the life of the process, so a registration changed
	// under a running pod would go on being addressed the old way (roster's
	// relying-party.md). `client_secret_basic` is what the client is
	// registered with.
	ep := p.Endpoint()
	ep.AuthStyle = oauth2.AuthStyleInHeader

	return &oauth2.Config{
		ClientID:     rp.cfg.ClientId,
		ClientSecret: rp.secret,
		Endpoint:     ep,
		RedirectURL:  base + PathCallback,
		Scopes:       rp.cfg.Scopes,
	}
}

// Flow is one trip to the issuer and back, sealed into a cookie: what the
// state parameter is compared with, the nonce the id_token must carry, the
// PKCE verifier, and where to go afterwards. In the cookie rather than in
// memory, so a second control plane can finish what the first began.
type Flow struct {
	State    string    `json:"s"`
	Nonce    string    `json:"n"`
	Verifier string    `json:"v"`
	Next     string    `json:"x"`
	Surface  string    `json:"f"`
	Expires  time.Time `json:"e"`
}

func (rp *RP) seal(f Flow) (string, error) {
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, rp.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(rp.aead.Seal(nonce, nonce, b, []byte("flow"))), nil
}

func (rp *RP) open(v string) (Flow, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil || len(b) < rp.aead.NonceSize() {
		return Flow{}, errors.New("the flow cookie does not open")
	}
	plain, err := rp.aead.Open(nil, b[:rp.aead.NonceSize()], b[rp.aead.NonceSize():], []byte("flow"))
	if err != nil {
		return Flow{}, errors.New("the flow cookie does not open")
	}
	var f Flow
	if err := json.Unmarshal(plain, &f); err != nil {
		return Flow{}, err
	}

	return f, nil
}

func random() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Begin starts a flow: the flow, sealed for the cookie that binds it to
// this browser, and the issuer's address to send the browser to. `base` is
// this listener's public origin.
func (rp *RP) Begin(ctx context.Context, surface, base, next string) (sealed, to string, err error) {
	p, err := rp.Provider(ctx)
	if err != nil {
		return "", "", err
	}
	f := Flow{Next: next, Surface: surface, Expires: rp.Now().Add(FlowLifetime), Verifier: oauth2.GenerateVerifier()}
	if f.State, err = random(); err != nil {
		return "", "", err
	}
	if f.Nonce, err = random(); err != nil {
		return "", "", err
	}
	sealed, err = rp.seal(f)
	if err != nil {
		return "", "", err
	}

	return sealed, rp.oauth(p, base).AuthCodeURL(f.State, oidc.Nonce(f.Nonce), oauth2.S256ChallengeOption(f.Verifier)), nil
}

// Signed is who the issuer said came back: the verified token and the raw
// one, kept for the logout hint.
type Signed struct {
	Token *oidc.IDToken
	Raw   string
	Next  string
	// Claims a person is shown, never the identity.
	Name, Username, Email string
}

// ErrRefused is a callback that is not one: no flow, the wrong state, an
// issuer that said no. The reason is in the error, for the log; a browser
// is told less.
var ErrRefused = errors.New("sso: refused")

func refused(why string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(why, args...))
}

// Finish is the callback: the flow out of its cookie, the state compared,
// the code exchanged with the PKCE verifier, the id_token verified -- the
// signature, the issuer, the audience, the expiry -- and its nonce compared.
func (rp *RP) Finish(ctx context.Context, surface, base, cookie string, q url.Values) (Signed, error) {
	if e := q.Get("error"); e != "" {
		return Signed{}, refused("the issuer answered %s: %s", e, q.Get("error_description"))
	}
	if cookie == "" {
		return Signed{}, refused("this browser has no flow cookie")
	}
	f, err := rp.open(cookie)
	if err != nil {
		return Signed{}, refused("%v", err)
	}
	state := q.Get("state")
	switch {
	case state == "":
		return Signed{}, refused("the issuer sent no state")
	case subtle.ConstantTimeCompare([]byte(state), []byte(f.State)) != 1:
		return Signed{}, refused("the state does not match the flow")
	case f.Surface != surface:
		return Signed{}, refused("the flow began on the %s listener", f.Surface)
	case rp.Now().After(f.Expires):
		return Signed{}, refused("the flow is older than %s", FlowLifetime)
	}
	p, err := rp.Provider(ctx)
	if err != nil {
		return Signed{}, err
	}
	tok, err := rp.oauth(p, base).Exchange(rp.ctx(ctx), q.Get("code"), oauth2.VerifierOption(f.Verifier))
	if err != nil {
		return Signed{}, refused("the issuer would not exchange the code: %v", err)
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok || raw == "" {
		return Signed{}, refused("the exchange carried no id_token")
	}
	id, err := p.Verifier(&oidc.Config{ClientID: rp.cfg.ClientId, Now: rp.Now}).Verify(rp.ctx(ctx), raw)
	if err != nil {
		return Signed{}, refused("the id_token does not verify: %v", err)
	}
	if subtle.ConstantTimeCompare([]byte(id.Nonce), []byte(f.Nonce)) != 1 {
		return Signed{}, refused("the id_token's nonce is not the flow's")
	}
	if id.Subject == "" {
		return Signed{}, refused("the id_token names nobody")
	}
	s := Signed{Token: id, Raw: raw, Next: f.Next}
	claimsOf(id, &s)

	return s, nil
}

func claimsOf(id *oidc.IDToken, s *Signed) {
	var c struct {
		Name     string `json:"name"`
		Username string `json:"preferred_username"`
		Email    string `json:"email"`
	}
	_ = id.Claims(&c)
	s.Name, s.Username, s.Email = c.Name, c.Username, c.Email
}

// VerifyDevice reads the id_token the CLI was handed at the end of a device
// flow (RFC 8628): verified for the device client, and fresh. The CLI holds
// no secret, so the token is what it proves itself with, once.
func (rp *RP) VerifyDevice(ctx context.Context, raw string) (Signed, error) {
	if rp.cfg.DeviceClientId == "" {
		return Signed{}, refused("no device client is configured (auth.oidc.device_client_id)")
	}
	p, err := rp.Provider(ctx)
	if err != nil {
		return Signed{}, err
	}
	id, err := p.Verifier(&oidc.Config{ClientID: rp.cfg.DeviceClientId, Now: rp.Now}).Verify(rp.ctx(ctx), raw)
	if err != nil {
		return Signed{}, refused("the id_token does not verify: %v", err)
	}
	if id.IssuedAt.IsZero() || rp.Now().Sub(id.IssuedAt) > DeviceFreshness {
		return Signed{}, refused("the id_token was issued more than %s ago", DeviceFreshness)
	}
	if id.Subject == "" {
		return Signed{}, refused("the id_token names nobody")
	}
	s := Signed{Token: id, Raw: raw}
	claimsOf(id, &s)

	return s, nil
}

// LogoutURL is the second hop of a sign-out: the issuer's end-session
// endpoint, asked to send the browser back to `back` -- which it will only
// do with the id_token as a hint, so without one nothing is asked and the
// browser lands on the issuer's own page. Empty is an issuer with no such
// endpoint: the sign-out was this app's half alone.
func (rp *RP) LogoutURL(ctx context.Context, hint, back string) string {
	if _, err := rp.Provider(ctx); err != nil {
		return ""
	}
	rp.mu.Lock()
	end := rp.endSession
	rp.mu.Unlock()
	if end == "" {
		return ""
	}
	to, err := url.Parse(end)
	if err != nil {
		return ""
	}
	q := to.Query()
	q.Set("client_id", rp.cfg.ClientId)
	if hint != "" {
		q.Set("id_token_hint", hint)
		if back != "" {
			q.Set("post_logout_redirect_uri", back)
		}
	}
	to.RawQuery = q.Encode()

	return to.String()
}

// origin is the scheme and host of a URL, and nothing else.
func origin(v string) (string, error) {
	u, err := url.Parse(v)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("%q is not an origin", v)
	}

	return u.Scheme + "://" + u.Host, nil
}

// Origin is a URL's origin, or "" for one that is not http(s).
func Origin(v string) string {
	o, err := origin(v)
	if err != nil {
		return ""
	}

	return o
}

// resolveRef reads `env:NAME`, `file:PATH`, or the value itself.
func resolveRef(ref string) (string, error) {
	switch {
	case strings.HasPrefix(ref, "env:"):
		v := os.Getenv(ref[4:])
		if v == "" {
			return "", fmt.Errorf("%s is not set", ref[4:])
		}

		return strings.TrimSpace(v), nil
	case strings.HasPrefix(ref, "file:"):
		b, err := os.ReadFile(ref[5:])
		if err != nil {
			return "", err
		}

		return strings.TrimSpace(string(b)), nil
	}

	return strings.TrimSpace(ref), nil
}
