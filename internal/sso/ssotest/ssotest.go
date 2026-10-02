// Package ssotest is an OIDC issuer that agrees with everybody, for tests.
//
// It is roster's `internal/idptest` (a provider that signs whatever
// [Idp.Subject] says), copied rather than imported, since roster keeps it
// internal, and grown by what Shale's relying party uses that the original
// does not check: the client's secret in the header, PKCE, the nonce, the
// device grant of RFC 8628, and the end-session endpoint. Imported by tests
// only.
package ssotest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
)

// Idp is the fake, and its server.
type Idp struct {
	*httptest.Server

	// ClientId and Secret are the browser's client; DeviceClientId the CLI's.
	ClientId       string
	Secret         string
	DeviceClientId string

	mu sync.Mutex
	// Subject is who the next sign-in is; Claims whatever else the token
	// carries.
	Subject string
	Claims  map[string]any
	// IssuedAt, when set, is the `iat` of the tokens it signs.
	IssuedAt time.Time

	codes   map[string]grant
	devices map[string]*device
	// Logouts are the end-session requests it was sent.
	Logouts []url.Values
	// Authorized are the authorization requests it was sent.
	Authorized []url.Values

	key *rsa.PrivateKey
}

type grant struct {
	challenge, nonce, redirect, subject string
}

type device struct {
	approved bool
	subject  string
}

// New stands one up, closed when the test ends.
func New(t *testing.T, clientId, secret, deviceClientId string) *Idp {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	p := &Idp{ClientId: clientId, Secret: secret, DeviceClientId: deviceClientId, key: k, codes: map[string]grant{}, devices: map[string]*device{}}
	m := http.NewServeMux()
	m.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                p.URL,
			"authorization_endpoint":                p.URL + "/oauth2/auth",
			"token_endpoint":                        p.URL + "/oauth2/token",
			"jwks_uri":                              p.URL + "/keys",
			"end_session_endpoint":                  p.URL + "/oauth2/sessions/logout",
			"device_authorization_endpoint":         p.URL + "/oauth2/device/auth",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	m.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &k.PublicKey, Algorithm: "RS256", Use: "sig", KeyID: "k"}}})
	})
	m.HandleFunc("/oauth2/auth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		p.mu.Lock()
		p.Authorized = append(p.Authorized, q)
		p.mu.Unlock()
		if q.Get("client_id") != p.ClientId || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" || q.Get("nonce") == "" {
			http.Error(w, "bad authorization request", http.StatusBadRequest)
			return
		}
		code := random(t)
		p.mu.Lock()
		p.codes[code] = grant{challenge: q.Get("code_challenge"), nonce: q.Get("nonce"), redirect: q.Get("redirect_uri"), subject: p.Subject}
		p.mu.Unlock()
		to, _ := url.Parse(q.Get("redirect_uri"))
		v := to.Query()
		v.Set("code", code)
		v.Set("state", q.Get("state"))
		to.RawQuery = v.Encode()
		http.Redirect(w, r, to.String(), http.StatusFound)
	})
	m.HandleFunc("/oauth2/sessions/logout", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.Logouts = append(p.Logouts, r.URL.Query())
		p.mu.Unlock()
		to := r.URL.Query().Get("post_logout_redirect_uri")
		if to == "" {
			to = p.URL + "/signed-out"
		}
		http.Redirect(w, r, to, http.StatusFound)
	})
	m.HandleFunc("/signed-out", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("signed out")) })
	m.HandleFunc("/oauth2/device/auth", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("client_id") != p.DeviceClientId {
			oauthError(w, http.StatusUnauthorized, "invalid_client")
			return
		}
		code := random(t)
		p.mu.Lock()
		p.devices[code] = &device{}
		p.mu.Unlock()
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code": code, "user_code": "BCDF-GHJK", "verification_uri": p.URL + "/device",
			"verification_uri_complete": p.URL + "/device?user_code=BCDF-GHJK", "expires_in": 600, "interval": 1,
		})
	})
	m.HandleFunc("/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.Form.Get("grant_type") {
		case "authorization_code":
			id, secret, ok := r.BasicAuth()
			if !ok || id != p.ClientId || secret != p.Secret {
				oauthError(w, http.StatusUnauthorized, "invalid_client")
				return
			}
			p.mu.Lock()
			g, ok := p.codes[r.Form.Get("code")]
			delete(p.codes, r.Form.Get("code"))
			p.mu.Unlock()
			sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if !ok || g.redirect != r.Form.Get("redirect_uri") || base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge {
				oauthError(w, http.StatusBadRequest, "invalid_grant")
				return
			}
			p.token(t, w, p.ClientId, g.subject, g.nonce)
		case "urn:ietf:params:oauth:grant-type:device_code":
			if r.Form.Get("client_id") != p.DeviceClientId {
				oauthError(w, http.StatusUnauthorized, "invalid_client")
				return
			}
			p.mu.Lock()
			d, ok := p.devices[r.Form.Get("device_code")]
			p.mu.Unlock()
			switch {
			case !ok:
				oauthError(w, http.StatusBadRequest, "invalid_grant")
			case !d.approved:
				oauthError(w, http.StatusBadRequest, "authorization_pending")
			default:
				p.token(t, w, p.DeviceClientId, d.subject, "")
			}
		default:
			oauthError(w, http.StatusBadRequest, "unsupported_grant_type")
		}
	})

	p.Server = httptest.NewServer(m)
	t.Cleanup(p.Close)

	return p
}

// Approve is somebody typing the device's code and signing in as Subject.
func (p *Idp) Approve() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, d := range p.devices {
		d.approved, d.subject = true, p.Subject
	}
}

// Pending is how many device codes wait for somebody.
func (p *Idp) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, d := range p.devices {
		if !d.approved {
			n++
		}
	}

	return n
}

// LastLogout is the last end-session request, or nil.
func (p *Idp) LastLogout() url.Values {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Logouts) == 0 {
		return nil
	}

	return p.Logouts[len(p.Logouts)-1]
}

// As sets who the next sign-in is.
func (p *Idp) As(subject string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Subject = subject
}

func (p *Idp) token(t *testing.T, w http.ResponseWriter, aud, subject, nonce string) {
	p.mu.Lock()
	iat := p.IssuedAt
	extra := p.Claims
	p.mu.Unlock()
	if iat.IsZero() {
		iat = time.Now()
	}
	claims := map[string]any{"iss": p.URL, "aud": aud, "sub": subject, "exp": iat.Add(time.Hour).Unix(), "iat": iat.Unix()}
	if nonce != "" {
		claims["nonce"] = nonce
	}
	for k, v := range extra {
		claims[k] = v
	}
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "opaque", "token_type": "Bearer", "expires_in": 3600, "id_token": p.Sign(t, claims)})
}

// Sign is one token, for a test that wants to hand a bad one over.
func (p *Idp) Sign(t *testing.T, claims map[string]any) string {
	b, err := json.Marshal(claims)
	require.NoError(t, err)
	s, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key}, (&jose.SignerOptions{}).WithHeader("kid", "k"))
	require.NoError(t, err)
	o, err := s.Sign(b)
	require.NoError(t, err)
	v, err := o.CompactSerialize()
	require.NoError(t, err)

	return v
}

func oauthError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func random(t *testing.T) string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(b)
}
