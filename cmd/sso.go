package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/z"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/auth/authsession"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso"
)

// Single sign-on (§33.1): each HTTP listener is a relying party of the
// issuer `auth.oidc` names, and what a sign-in ends in is the same session
// cookie a password mints -- sealed under the KEK, one per surface, read by
// the same handler. The routes, on both listeners:
//
//	GET  /sso/login?next=     a flow begins: state, nonce, PKCE; to the issuer
//	GET  /sso/callback        the code exchanged, the id_token verified, a session
//	GET  /sso/logout?next=    this session ended, then the issuer's (two hops)
//	POST /sso/token           the CLI's id_token from a device flow, for a session
//	GET  /session             who this browser is signed in as here
//	GET  /session/ways        how one signs in here: password, SSO, the issuer
//
// The cluster listener signs in operators only, and so does the CLI (for
// now); the tenant listener signs in anybody of a tenant this deployment
// serves.

// heldIdToken is where a session keeps the id_token, for the logout hint and
// for nothing else.
const heldIdToken = "id_token"

// flowCookie names one surface's flow cookie.
func flowCookie(surface Surface, dev bool) string {
	name := "shale_sso_tenant"
	if surface == SurfaceCluster {
		name = "shale_sso_cluster"
	}
	if dev {
		return name
	}

	return "__Host-" + name
}

func surfaceName(surface Surface) string {
	if surface == SurfaceCluster {
		return "cluster"
	}

	return "tenant"
}

// passwordOn says whether a password signs anybody in here.
func (s *Server) passwordOn() bool { return !s.cfg.Auth.SsoOnly }

// base is a listener's public origin: configured, or read off the request,
// which is right for development and wrong behind a proxy that does not
// say what it was asked.
func (s *Server) base(surface Surface, r *http.Request) string {
	v := s.cfg.Auth.Oidc.TenantOrigin
	if surface == SurfaceCluster {
		v = s.cfg.Auth.Oidc.ClusterOrigin
	}
	if v != "" {
		return sso.Origin(v)
	}
	scheme := "https"
	if r.TLS == nil && s.cfg.IsDev() {
		scheme = "http"
	}

	return scheme + "://" + r.Host
}

// next is where a browser may be sent after a sign-in or a sign-out: a URL
// on an origin this deployment answers at or names for its pages, or else
// this listener's own root. Anything else would make the sign-in an open
// redirect.
func (s *Server) next(surface Surface, r *http.Request, v string) string {
	own := s.base(surface, r)
	if v == "" {
		return own + "/"
	}
	u, err := url.Parse(v)
	if err != nil {
		return own + "/"
	}
	if u.Scheme == "" && u.Host == "" && strings.HasPrefix(u.Path, "/") && !strings.HasPrefix(u.Path, "//") {
		return own + u.RequestURI() + fragment(u)
	}
	o := sso.Origin(v)
	allowed := []string{own, sso.Origin(s.cfg.Auth.Oidc.TenantOrigin), sso.Origin(s.cfg.Auth.Oidc.ClusterOrigin)}
	for _, x := range append(slices.Clone(s.cfg.Server.Http.Origins), s.cfg.Cluster.Http.Origins...) {
		allowed = append(allowed, sso.Origin(x))
	}
	if s.cfg.IsDev() {
		// Development names no origins: the two listeners, as bound.
		for _, x := range []Surface{SurfaceTenant, SurfaceCluster} {
			if a := s.HttpAddr(x); a != "" {
				allowed = append(allowed, "http://"+a)
			}
		}
	}
	if o != "" && slices.Contains(allowed, o) {
		return u.String()
	}

	return own + "/"
}

func fragment(u *url.URL) string {
	if u.Fragment == "" {
		return ""
	}

	return "#" + u.EscapedFragment()
}

// mountSso puts the routes on one listener's mux.
func (s *Server) mountSso(surface Surface, h interface {
	Handle(string, http.Handler)
}) {
	sessions := s.Sessions[surface]
	h.Handle("GET /session", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.who(surface, w, r) }))
	h.Handle("GET /session/ways", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.ways(w, r) }))
	if s.Sso == nil || sessions == nil {
		return
	}
	h.Handle("GET "+sso.PathLogin, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.ssoBegin(surface, w, r) }))
	h.Handle("GET "+sso.PathCallback, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.ssoCallback(surface, w, r) }))
	logout := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.ssoLogout(surface, w, r) })
	h.Handle("GET "+sso.PathLogout, logout)
	h.Handle("POST "+sso.PathLogout, logout)
	h.Handle("POST "+sso.PathToken, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.ssoToken(surface, w, r) }))
}

// Ways is how one signs in here, which the console and the CLI read before
// drawing a form or starting a device flow. Nothing in it is a secret.
type Ways struct {
	Password bool   `json:"password"`
	Sso      bool   `json:"sso"`
	Issuer   string `json:"issuer,omitempty"`
	Login    string `json:"login,omitempty"`
	// DeviceClientId is the CLI's client at the issuer; empty is no single
	// sign-on from the CLI.
	DeviceClientId string `json:"device_client_id,omitempty"`
	// Cluster is where the cluster API's HTTP listener answers browsers, when
	// that is not this listener's host one port up -- an Ingress in front
	// (§40.4). The console asks for it before it opens the operators' half.
	Cluster string `json:"cluster,omitempty"`
}

func (s *Server) ways(w http.ResponseWriter, _ *http.Request) {
	v := Ways{Password: s.passwordOn() && s.Identity != nil}
	if s.Sso != nil {
		v.Sso, v.Issuer, v.Login, v.DeviceClientId = true, s.cfg.Auth.Oidc.Issuer, sso.PathLogin, s.cfg.Auth.Oidc.DeviceClientId
	}
	v.Cluster = s.cfg.Auth.Oidc.ClusterOrigin
	writeJson(w, http.StatusOK, v)
}

// Who is the answer to `GET /session`.
type Who struct {
	Id     string `json:"id"`
	Tenant string `json:"tenant"`
	Alias  string `json:"alias"`
	Name   string `json:"name,omitempty"`
	// Who is `@tenant/alias`, as the console keys its store.
	Who string `json:"who"`
	// Sso says the session came from the issuer, so signing out of it is two
	// hops.
	Sso bool `json:"sso"`
	// Operator says whether this person may change anything on this surface.
	Operator bool `json:"operator"`
}

func (s *Server) who(surface Surface, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	sessions := s.Sessions[surface]
	if sessions == nil {
		writeJson(w, http.StatusUnauthorized, map[string]string{"error": "no sessions here"})
		return
	}
	v, err := sessions.Read(r.Context(), sessions.KeyOf(r.Header.Values("Cookie")))
	if err == nil {
		// Held to its person's standing, as every call is (§33.7).
		err = s.standing.check(r.Context(), v)
	}
	if errors.Is(err, auth.ErrUnavailable) {
		writeJson(w, http.StatusServiceUnavailable, map[string]string{"error": "cannot tell who is signed in right now"})
		return
	}
	if err != nil {
		writeJson(w, http.StatusUnauthorized, map[string]string{"error": "not signed in"})
		return
	}
	id, err := pdid.Parse(v.Id)
	if err != nil {
		writeJson(w, http.StatusUnauthorized, map[string]string{"error": "not signed in"})
		return
	}
	h, err := s.Ungated.Holder().Get(r.Context(), api.HolderGetRequest_builder{
		Ref:    api.HolderRef_builder{Id: id.Bytes()}.Build(),
		Select: api.HolderSelect_builder{All: z.Ptr(true), Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		writeJson(w, http.StatusUnauthorized, map[string]string{"error": "not signed in"})
		return
	}
	tenant, _ := pdid.From(h.GetTenant().GetId())
	writeJson(w, http.StatusOK, Who{
		Id: id.String(), Tenant: h.GetTenant().GetAlias(), Alias: h.GetAlias(), Name: h.GetName(),
		Who: "@" + h.GetTenant().GetAlias() + "/" + h.GetAlias(), Sso: v.Held[heldIdToken] != "",
		Operator: s.mayOperate(r.Context(), surface, tenant, id) == nil,
	})
}

// mayOperate says whether a person may change things on a surface: where
// roster says what people may change, whether it grants them all of Shale
// -- on the cluster surface as a person of the operators' tenant -- and
// otherwise, on the cluster surface, whether they are of the cluster
// tenant, and on the tenant surface, yes.
//
// This is who signs in to the cluster surface -- by password or through the
// issuer -- and the CLI, and whom the console shows as an operator. The
// cluster policy asks the same of every call there (§33.1), so a session
// that reached the cluster surface some other way is no wider; on the
// tenant surface a narrower grant is still a grant, decided per method.
func (s *Server) mayOperate(ctx context.Context, surface Surface, tenant, holder pdid.Id) error {
	if s.Operators != nil {
		var is bool
		var err error
		if surface == SurfaceCluster {
			is, err = s.Operators.Operates(ctx, tenant, holder)
		} else {
			is, err = s.Operators.Administers(ctx, tenant, holder)
		}
		if err != nil {
			return fmt.Errorf("cannot tell what roster grants you right now: %w", err)
		}
		if !is {
			if surface == SurfaceCluster {
				return fmt.Errorf("not an operator: roster does not grant you %s as a person of @%s", identity.Everything, s.Operators.TenantAlias())
			}

			return fmt.Errorf("not an operator: roster does not grant you %s", identity.Everything)
		}

		return nil
	}
	if surface == SurfaceTenant {
		return nil
	}
	if s.ClusterTenant.IsZero() || tenant != s.ClusterTenant {
		return errors.New("not an operator: the cluster API serves cluster operators")
	}

	return nil
}

func (s *Server) ssoBegin(surface Surface, w http.ResponseWriter, r *http.Request) {
	next := s.next(surface, r, r.URL.Query().Get("next"))
	sealed, to, err := s.Sso.Begin(r.Context(), surfaceName(surface), s.base(surface, r), next)
	if err != nil {
		s.Deps.Log.WarnContext(r.Context(), "sso: cannot begin", "surface", surfaceName(surface), "err", err.Error())
		page(w, http.StatusBadGateway, "The sign-in service is not answering", "Try again in a moment.", next)
		return
	}
	// Lax, so it comes back on the issuer's redirect, which is a top-level
	// GET; Secure from the configuration and not from r.TLS, which is nil
	// behind every proxy that ends TLS.
	http.SetCookie(w, &http.Cookie{
		Name: flowCookie(surface, s.cfg.IsDev()), Value: sealed, Path: "/",
		HttpOnly: true, Secure: !s.cfg.IsDev(), SameSite: http.SameSiteLaxMode, MaxAge: int(sso.FlowLifetime / time.Second),
	})
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, to, http.StatusFound)
}

func (s *Server) ssoCallback(surface Surface, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := flowCookie(surface, s.cfg.IsDev())
	http.SetCookie(w, &http.Cookie{Name: name, Path: "/", MaxAge: -1, HttpOnly: true, Secure: !s.cfg.IsDev(), SameSite: http.SameSiteLaxMode})
	w.Header().Set("Cache-Control", "no-store")
	back := s.base(surface, r) + "/"

	// One answer to the browser and one line in the log: every refusal here
	// is somebody's mistake or an attack, and saying which out loud tells an
	// attacker which (roster's examples/product).
	no := func(status int, title, why string, err error) {
		s.Deps.Log.WarnContext(ctx, "sso: sign-in refused", "surface", surfaceName(surface), "why", why, "err", errString(err))
		page(w, status, title, why, back)
	}

	cookie := ""
	if c, err := r.Cookie(name); err == nil {
		cookie = c.Value
	}
	signed, err := s.Sso.Finish(ctx, surfaceName(surface), s.base(surface, r), cookie, r.URL.Query())
	if err != nil {
		if errors.Is(err, sso.ErrRefused) {
			no(http.StatusBadRequest, "The sign-in did not complete", "Start again from the console.", err)
		} else {
			no(http.StatusBadGateway, "The sign-in service is not answering", "Try again in a moment.", err)
		}
		return
	}
	p, err := s.ssoPerson(ctx, signed.Token.Subject)
	if err != nil {
		if errors.Is(err, identity.ErrNoPerson) || errors.Is(err, identity.ErrRefused) {
			no(http.StatusForbidden, "Not somebody this deployment serves", "Your account is not in a tenant this deployment serves.", err)
		} else {
			no(http.StatusBadGateway, "The directory is not answering", "Try again in a moment.", err)
		}
		return
	}
	if surface == SurfaceCluster {
		if err := s.mayOperate(ctx, surface, p.Tenant, p.Id); err != nil {
			no(http.StatusForbidden, "Operators only", "The cluster API serves the people roster grants all of Shale to.", err)
			return
		}
	}
	if _, err := s.Provision(ctx, p); err != nil {
		no(http.StatusInternalServerError, "The sign-in did not complete", "Try again in a moment.", err)
		return
	}
	_, ck, err := s.Sessions[surface].Mint(ctx, issued(authsession.Session{
		Id: p.Id.String(), TenantId: p.Tenant.String(), Grant: frame.Whole(),
		// Kept for the logout hint alone: nothing reads it to decide anything.
		Held: map[string]string{heldIdToken: signed.Raw},
	}))
	if err != nil {
		no(http.StatusInternalServerError, "The sign-in did not complete", "Try again in a moment.", err)
		return
	}
	s.Deps.Log.InfoContext(ctx, "sso: signed in", "surface", surfaceName(surface), "tenant", p.TenantAlias, "alias", p.Alias)
	http.SetCookie(w, ck)
	http.Redirect(w, r, signed.Next, http.StatusSeeOther)
}

// ssoPerson is who a `sub` is, at roster, refreshed into Shale's rows by
// the caller.
func (s *Server) ssoPerson(ctx context.Context, sub string) (identity.Person, error) {
	if s.Identity == nil {
		return identity.Person{}, identity.ErrNoPerson
	}

	return s.Identity.ById(ctx, sub)
}

// ssoLogout is the two hops: this surface's session ends here, then the
// browser goes to the issuer to end its own, with the id_token as the hint
// it requires before it sends the browser back.
func (s *Server) ssoLogout(surface Surface, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sessions := s.Sessions[surface]
	key := sessions.KeyOf(r.Header.Values("Cookie"))
	hint := ""
	// Read before ending: the hint is in the session about to go.
	if v, err := sessions.Read(ctx, key); err == nil {
		hint = v.Held[heldIdToken]
	}
	http.SetCookie(w, sessions.End(ctx, key))
	w.Header().Set("Cache-Control", "no-store")

	next := s.next(surface, r, r.URL.Query().Get("next"))
	back := sso.Origin(next) + "/"
	to := s.Sso.LogoutURL(ctx, hint, back)
	if to == "" {
		to = next
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

// ssoToken is the CLI's sign-in (§33.1): the id_token a device flow ended
// in, for a session on this surface. Operators only, for now.
func (s *Server) ssoToken(surface Surface, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		IdToken string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body); err != nil || body.IdToken == "" {
		http.Error(w, "id_token is required", http.StatusBadRequest)
		return
	}
	refuse := func(status int, why string, err error) {
		s.Deps.Log.WarnContext(ctx, "sso: CLI sign-in refused", "surface", surfaceName(surface), "why", why, "err", errString(err))
		http.Error(w, why, status)
	}
	signed, err := s.Sso.VerifyDevice(ctx, body.IdToken)
	if err != nil {
		if errors.Is(err, sso.ErrRefused) {
			refuse(http.StatusUnauthorized, "the token is not one this deployment takes", err)
		} else {
			refuse(http.StatusBadGateway, "the sign-in service is not answering", err)
		}
		return
	}
	p, err := s.ssoPerson(ctx, signed.Token.Subject)
	if err != nil {
		if errors.Is(err, identity.ErrNoPerson) || errors.Is(err, identity.ErrRefused) {
			refuse(http.StatusForbidden, "not somebody this deployment serves", err)
		} else {
			refuse(http.StatusBadGateway, "the directory is not answering", err)
		}
		return
	}
	// Operators only from the CLI, on either surface, until there is a
	// reason for anybody else to have a terminal session.
	if err := s.mayOperate(ctx, SurfaceCluster, p.Tenant, p.Id); err != nil {
		refuse(http.StatusForbidden, "the CLI signs in operators only: "+err.Error(), err)
		return
	}
	if _, err := s.Provision(ctx, p); err != nil {
		refuse(http.StatusInternalServerError, "the sign-in did not complete", err)
		return
	}
	_, ck, err := s.Sessions[surface].Mint(ctx, issued(authsession.Session{Id: p.Id.String(), TenantId: p.Tenant.String(), Grant: frame.Whole()}))
	if err != nil {
		refuse(http.StatusInternalServerError, "the sign-in did not complete", err)
		return
	}
	s.Deps.Log.InfoContext(ctx, "sso: CLI signed in", "surface", surfaceName(surface), "tenant", p.TenantAlias, "alias", p.Alias)
	http.SetCookie(w, ck)
	w.WriteHeader(http.StatusNoContent)
}

func writeJson(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// page is what a person sees when a sign-in did not end in the console:
// one sentence and a way back.
func page(w http.ResponseWriter, status int, title, text, back string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>shale</title>
<style>body{font:15px/1.6 system-ui,sans-serif;max-width:32rem;margin:15vh auto;padding:0 1rem}</style>
<h1>%s</h1><p>%s</p><p><a href="%s">Back to the console</a></p>`, html.EscapeString(title), html.EscapeString(text), html.EscapeString(back))
}

var _ = slog.Default
