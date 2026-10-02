package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"
	"golang.org/x/term"

	"github.com/lesomnus/payday/slug"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
)

// `shale login` signs a person in (§33.1): a password to the sign-in
// endpoint on the API's HTTP listener, and the session cookie it answers
// kept in the user's configuration directory, sent on every call after.

// sessionFile is where the CLI keeps a session, per API address.
func sessionFile(addr string) (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	safe := strings.NewReplacer("/", "_", ":", "_", "?", "_").Replace(addr)

	return filepath.Join(dir, "shale", "session-"+safe), nil
}

// savedSession is the cookie a login stored for an address.
func savedSession(addr string) string {
	p, err := sessionFile(addr)
	if err != nil {
		return ""
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(b))
}

// NewCmdLogin is `shale login [@tenant/alias]`, or `shale login --sso`.
func NewCmdLogin(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "login",
		Brief: "sign in as a person and keep the session",
		Flags: flg.Flags{
			&flg.String{Name: "password", Brief: "the password; prompted when absent"},
			&flg.Switch{Name: "cluster", Brief: "sign in to the cluster API (operators)"},
			&flg.Switch{Name: "sso", Brief: "sign in through the deployment's issuer, with a code to confirm in a browser (operators)"},
		},
		Args: arg.Args{
			&arg.String{Name: "WHO", Brief: "@tenant/alias"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			cluster, _ := flg.Find[bool](self, "cluster")
			if v, _ := flg.Find[bool](self, "sso"); v {
				return LoginSso(ctx, c, self, cluster)
			}
			who, _ := arg.Get[string](self, "WHO")
			if who == "" {
				who = c.Client.As
			}
			if who == "" {
				return errors.New("say who: shale login @tenant/alias")
			}
			s, err := slug.Parse(who)
			if err != nil {
				return err
			}
			tenant, alias := s.Tenant(), s.Alias()
			if tenant == "" || alias == "" {
				return errors.New("a person is @tenant/alias")
			}

			password, _ := flg.Find[string](self, "password")
			if password == "" {
				fmt.Fprintf(self, "password for %s: ", who)
				b, err := term.ReadPassword(int(syscall.Stdin))
				fmt.Fprintln(self)
				if err != nil {
					return err
				}
				password = string(b)
			}

			addr := apiAddr(c, cluster)
			cookie, err := signIn(ctx, c, addr, cluster, tenant, alias, password)
			if err != nil {
				return err
			}
			if err := keepSession(addr, cookie); err != nil {
				return err
			}
			fmt.Fprintf(self, "signed in as %s at %s\n", who, addr)

			return nil
		}),
	}
}

// apiAddr is the API a surface is reached at, as the CLI is configured.
func apiAddr(c *cmd.Config, cluster bool) string {
	addr := c.Client.Addr
	if cluster {
		addr = c.Client.ClusterAddr
	}
	if addr == "" {
		if cluster {
			addr = "127.0.0.1:7401"
		} else {
			addr = "127.0.0.1:7400"
		}
	}

	return addr
}

// keepSession writes the cookie a sign-in answered for an API address,
// where every later call reads it.
func keepSession(addr, cookie string) error {
	p, err := sessionFile(addr)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}

	return os.WriteFile(p, []byte(cookie+"\n"), 0o600)
}

// SavedSession is the session cookie kept for an API address, or "".
func SavedSession(addr string) string { return savedSession(addr) }

// webBase is the HTTP listener of a surface: `client.web` or
// `client.cluster_web` when configured, else the API's host two ports up
// (§34.1). plain says it is spoken without TLS.
func webBase(c *cmd.Config, addr string, cluster bool) (base string, plain bool, err error) {
	v := c.Client.Web
	if cluster {
		v = c.Client.ClusterWeb
	}
	if v != "" {
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return "", false, fmt.Errorf("%q is not a URL of an HTTP listener", v)
		}

		return u.Scheme + "://" + u.Host, u.Scheme == "http", nil
	}
	u, plain, err := loginURL(c, addr, cluster)
	if err != nil {
		return "", false, err
	}

	return strings.TrimSuffix(u, "/session"), plain, nil
}

// webClient speaks to an HTTP listener: the CP's CA and the system's roots,
// since an Ingress in front may well present a public certificate.
func webClient(c *cmd.Config, plain bool) (*http.Client, error) {
	if plain {
		return &http.Client{}, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if b, err := caBundle(c); err == nil {
		pool, err := pki.PeerPool(b)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}

	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}, nil
}

// loginURL is the sign-in endpoint beside an API address: the HTTP
// listener two ports up (7402 beside 7400, 7403 beside 7401), or the
// configured one.
func loginURL(c *cmd.Config, addr string, cluster bool) (string, bool, error) {
	plain := false
	host := addr
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", false, err
		}
		plain = u.Scheme == "http"
		host = u.Host
	}
	h, p, err := net.SplitHostPort(host)
	if err != nil {
		h, p = host, "7400"
	}
	httpAddr := c.Server.Http.Addr
	if cluster {
		httpAddr = c.Cluster.Http.Addr
	}
	port := ""
	if httpAddr != "" {
		if _, pp, err := net.SplitHostPort(httpAddr); err == nil {
			port = pp
		}
	}
	if port == "" {
		switch p {
		case "7400":
			port = "7402"
		case "7401":
			port = "7403"
		default:
			// The sign-in listener is the API port plus two, on any port
			// (a NodePort 30400 signs in on 30402).
			if n, err := strconv.Atoi(p); err == nil {
				port = strconv.Itoa(n + 2)
			} else {
				port = p
			}
		}
	}
	scheme := "https"
	if plain {
		scheme = "http"
	}

	return fmt.Sprintf("%s://%s/session", scheme, net.JoinHostPort(h, port)), plain, nil
}

func signIn(ctx context.Context, c *cmd.Config, addr string, cluster bool, tenant, alias, password string) (string, error) {
	body, _ := json.Marshal(map[string]string{"tenant": tenant, "alias": alias, "password": password})

	return postForSession(ctx, c, addr, cluster, "/session", body)
}

// postForSession posts to a surface's HTTP listener and answers the session
// cookie it set.
func postForSession(ctx context.Context, c *cmd.Config, addr string, cluster bool, path string, body []byte) (string, error) {
	base, plain, err := webBase(c, addr, cluster)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client, err := webClient(c, plain)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("sign-in refused: %d %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	ck := sessionCookie(resp.Cookies())
	if ck == nil {
		return "", errors.New("the sign-in answered no session cookie")
	}

	return ck.Name + "=" + ck.Value, nil
}

// sessionCookie is the session among the cookies a sign-in answered with:
// the one named for it, under whichever name this server gives it -- one
// per surface since §40.1 (`__Host-shale_tenant`, `shale_cluster`), or
// payday's own before that -- and failing that the only cookie there is.
// The CLI sends it back as it was named, so the name is the server's to
// choose.
func sessionCookie(cks []*http.Cookie) *http.Cookie {
	for _, ck := range cks {
		if ck.Value != "" && (strings.Contains(ck.Name, "shale_") || strings.Contains(ck.Name, "session")) {
			return ck
		}
	}
	for _, ck := range cks {
		if ck.Value != "" {
			return ck
		}
	}

	return nil
}

var _ = pki.CpName
