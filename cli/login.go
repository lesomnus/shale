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

// NewCmdLogin is `shale login [@tenant/alias]`.
func NewCmdLogin(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "login",
		Brief: "sign in as a person and keep the session",
		Flags: flg.Flags{
			&flg.String{Name: "password", Brief: "the password; prompted when absent"},
			&flg.Switch{Name: "cluster", Brief: "sign in to the cluster API (operators)"},
		},
		Args: arg.Args{
			&arg.String{Name: "WHO", Brief: "@tenant/alias"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
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

			cluster, _ := flg.Find[bool](self, "cluster")
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

			cookie, err := signIn(ctx, c, addr, tenant, alias, password)
			if err != nil {
				return err
			}
			p, err := sessionFile(addr)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(p, []byte(cookie+"\n"), 0o600); err != nil {
				return err
			}
			fmt.Fprintf(self, "signed in as %s at %s\n", who, addr)

			return nil
		}),
	}
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
			port = p
		}
	}
	scheme := "https"
	if plain {
		scheme = "http"
	}

	return fmt.Sprintf("%s://%s/session", scheme, net.JoinHostPort(h, port)), plain, nil
}

func signIn(ctx context.Context, c *cmd.Config, addr, tenant, alias, password string) (string, error) {
	u, plain, err := loginURL(c, addr, strings.Contains(addr, "7401") || addr == c.Client.ClusterAddr && c.Client.ClusterAddr != "")
	if err != nil {
		return "", err
	}
	body, _ := json.Marshal(map[string]string{"tenant": tenant, "alias": alias, "password": password})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	if !plain {
		pool, err := caPool(c)
		if err != nil {
			return "", err
		}
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
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
	for _, ck := range resp.Cookies() {
		if strings.Contains(ck.Name, "pd_session") {
			return ck.Name + "=" + ck.Value, nil
		}
	}

	return "", errors.New("the sign-in answered no session cookie")
}

var _ = pki.CpName
