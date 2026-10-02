package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
	"github.com/lesomnus/z"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/k8s"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// NewCmdInit is `shale init` (§33.4): the CA, the CP certificate, the
// signing key under the KEK, the cluster tenant and its first operator, the
// first tenant and its first admin.
//
// It exists because there is nowhere else it could happen. A tenant is not
// put up from inside one, so the first row of a deployment cannot arrive
// over the API. Running it twice is refused: an init that quietly did
// nothing is one somebody runs against the wrong deployment and believes.
func NewCmdInit(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "init",
		Brief: "create the cluster: CA, signing key, first tenant, first operator and admin",

		Flags: flg.Flags{
			&flg.String{Name: "tenant", Brief: "the alias of the first tenant (the organization)"},
			&flg.String{Name: "admin", Brief: "the alias of the first tenant admin"},
			&flg.String{Name: "operator", Brief: "the alias of the first cluster operator"},
			&flg.String{Name: "dev", Brief: "development mode: everything under this directory"},
			&flg.Switch{Name: "if-needed", Brief: "do nothing when already initialized, instead of refusing"},
			&flg.String{Name: "k8s-secret", Brief: "inside a cluster: put the KEK, CA, and CP certificate into this Secret (§34.5)"},
			&flg.String{Name: "k8s-password-secret", Brief: "inside a cluster: put the first passwords into this Secret instead of printing them (default: the --k8s-secret name with -passwords)"},
		},

		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, next xli.Next) error {
			if v, ok := flg.Find[string](self, "dev"); ok && v != "" {
				ApplyDev(c, v)
			}
			ifNeeded, _ := flg.Find[bool](self, "if-needed")
			tenant, admin, operator := flagOr(self, "tenant", "acme"), flagOr(self, "admin", "admin"), flagOr(self, "operator", "ops")
			secret := flagOr(self, "k8s-secret", "")
			var stash Stash
			if secret != "" {
				// The Secret is the state directory of every CP pod (§34.5):
				// its presence is what "initialized" means here.
				k, ok := inCluster()
				if !ok {
					return errors.New("--k8s-secret: not inside a cluster (no service account)")
				}
				exists, err := k.SecretExists(ctx, secret)
				if err != nil {
					return err
				}
				if exists {
					if ifNeeded {
						self.Printf("already initialized: secret %s/%s\n", k.Namespace(), secret)
						return nil
					}

					return fmt.Errorf("secret %s/%s already exists", k.Namespace(), secret)
				}
				// The state directory is scratch here: what a failed run
				// (the database not up yet) left behind is not state. So
				// roster cannot live in it either (§34.5).
				if c.Auth.Roster.Embedded() && c.Auth.Roster.Db.Driver == "" {
					return errors.New("--k8s-secret: roster in this process needs a database every control plane shares; set auth.roster.db, or auth.roster.addr")
				}
				// A pod's output is its log, and a log is read by whatever
				// collects logs, for as long as it keeps them: the passwords
				// go into a Secret of their own instead (§34.5). Whether it
				// can be made is asked before anything is, so a Secret that
				// is in the way, or a role that does not allow it, stops
				// init while there is nothing yet to lose.
				if c.Auth.Roster.Embedded() {
					ps := &secretStash{k: k, name: flagOr(self, "k8s-password-secret", secret+PasswordSecretSuffix)}
					if err := ps.check(ctx, []Password{{Tenant: clusterTenant(c), Alias: operator}, {Tenant: tenant, Alias: admin}}); err != nil {
						return err
					}
					stash = ps
				}
				if err := os.RemoveAll(c.StateDir("control")); err != nil {
					return err
				}
			} else if ifNeeded {
				if _, err := os.Stat(filepath.Join(c.StateDir("control"), "kek")); err == nil {
					self.Printf("already initialized: %s\n", c.StateDir("control"))
					return nil
				}
			}

			if err := Init(ctx, c, tenant, admin, operator, self, stash); err != nil {
				return err
			}
			if secret == "" {
				return nil
			}
			k, _ := inCluster()
			dir := c.StateDir("control")
			files := map[string][]byte{}
			for _, name := range []string{"kek", cmd.CaCertFile, cmd.CaKeyFile, cmd.CpCertFile, cmd.CpKeyFile} {
				b, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					return err
				}
				files[name] = b
			}
			if err := k.CreateSecret(ctx, secret, files); err != nil {
				return err
			}
			self.Printf("\nsecret %s/%s holds the KEK, the CA, and the CP certificate; mount it at %s in every CP pod\n", k.Namespace(), secret, dir)

			return nil
		}),
	}
}

// inCluster is the API server as this pod; a test answers its own.
var inCluster = k8s.InCluster

// clusterTenant is the alias of the cluster operators' tenant.
func clusterTenant(c *cmd.Config) string {
	if c.Control.ClusterTenant == "" {
		return "cluster"
	}

	return c.Control.ClusterTenant
}

// Password is a first password `init` made: whose, and the password.
type Password struct {
	Tenant, Alias, Secret string
}

// Stash keeps the first passwords somewhere other than init's output
// (§34.5), which is somebody's log.
type Stash interface {
	// Put keeps them, all or none. Where it fails they are printed after
	// all: a password nobody has is a deployment nobody can sign in to.
	Put(ctx context.Context, ps []Password) error
	// Where is what is printed in place of a kept password.
	Where(p Password) string
	// Read is what is printed after them, a sentence with no full stop:
	// how to get p's back.
	Read(p Password) string
}

// Init does the work of `shale init`, writing what it made to `out`. The
// first passwords are printed there too, once, unless stash keeps them.
func Init(ctx context.Context, c *cmd.Config, tenant, admin, operator string, out io.Writer, stash Stash) error {
	clusterAlias := clusterTenant(c)

	dir := c.StateDir("control")
	if _, err := os.Stat(filepath.Join(dir, "kek")); err == nil {
		return fmt.Errorf("%s: already initialized", dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	now := time.Now()

	// The CA and the CP's own certificate (§33.5).
	ca, err := pki.NewCA("shale-ca", now)
	if err != nil {
		return err
	}
	if err := ca.Save(filepath.Join(dir, cmd.CaCertFile), filepath.Join(dir, cmd.CaKeyFile)); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	dns, ips := cmd.Hostnames(c.Control.Names)
	cert, err := ca.IssueServer(&key.PublicKey, pki.CpName, pki.Names{DNS: dns, IPs: ips}, pki.CpLifetime, now)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, cmd.CpCertFile), pki.EncodeCerts(cert, ca.Cert), 0o644); err != nil {
		return err
	}
	kb, err := pki.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, cmd.CpKeyFile), kb, 0o600); err != nil {
		return err
	}
	if _, err := core.NewKek(dir); err != nil {
		return err
	}

	// The rows, through the server the wall was never installed on.
	s, err := cmd.Build(ctx, *c)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := Migrate(ctx, s); err != nil {
		return err
	}

	// The people (§33.1). Where roster runs in this process they are made
	// now, with the passwords shown once; where it runs elsewhere its
	// operator made them, and the rows here come when they first sign in.
	type seeded struct {
		person   identity.Person
		password string
	}
	var ops, adm *seeded
	if s.Identity.Embedded() {
		p, secret, err := s.Identity.Seed(ctx, clusterAlias, operator)
		if err != nil {
			return fmt.Errorf("@%s/%s: %w", clusterAlias, operator, err)
		}
		ops = &seeded{p, secret}
		q, secret, err := s.Identity.Seed(ctx, tenant, admin)
		if err != nil {
			return fmt.Errorf("@%s/%s: %w", tenant, admin, err)
		}
		adm = &seeded{q, secret}
		for _, v := range []*seeded{ops, adm} {
			if _, err := s.Provision(ctx, v.person); err != nil {
				return err
			}
		}
	}
	// Where operators are a team (§33.1), the first admin is put on it at
	// the embedded roster, so the deployment has an operator to begin
	// with; at an external one its operator made the team and its members.
	var team pdid.Id
	if s.Operators != nil && adm != nil && s.Operators.TenantAlias() == tenant {
		team, err = s.Identity.AddTeam(ctx, tenant, c.Auth.Operators.Team)
		if err != nil {
			return err
		}
		if err := s.Identity.JoinTeam(ctx, team, adm.person.Id); err != nil {
			return fmt.Errorf("@%s/%s on %s: %w", tenant, admin, s.Operators.Team(), err)
		}
	}

	var ct, t pdid.Id
	var terr error
	if s.Identity.Embedded() {
		ct, err = s.ProvisionTenant(ctx, clusterAlias)
		if err != nil && !errors.Is(err, identity.ErrNoTenant) {
			return err
		}
		t, terr = s.ProvisionTenant(ctx, tenant)
		if terr != nil && !errors.Is(terr, identity.ErrNoTenant) {
			return terr
		}
	} else {
		// At an external roster nothing is made: its operator made the
		// tenants and the people, and Shale's rows follow the first
		// sign-in. Asking now only says early whether the keys work, so a
		// roster that does not answer yet is said and is not a failure:
		// the CA, the KEK and the CP certificate do not depend on it.
		alias := clusterAlias
		if s.Operators != nil {
			alias = s.Operators.TenantAlias()
		}
		ct, err = s.ProvisionTenant(ctx, alias)
		if err != nil && !errors.Is(err, identity.ErrNoTenant) {
			fmt.Fprintf(out, "warning     roster at %s did not answer for @%s: %v\n            the rows follow the first sign-in; check auth.roster before anybody signs in\n", c.Auth.Roster.Addr, alias, err)
		}
		if s.Operators != nil && err == nil {
			// Whether the team is there, which is a question about the key
			// as much as about the team: it needs TeamService.
			if oerr := s.Operators.Check(ctx); oerr != nil {
				fmt.Fprintf(out, "warning     the operators' team %s: %v\n            nobody is an operator until roster answers for it\n", s.Operators.Team(), oerr)
			}
		}
	}

	// The first signing key (§33.3).
	kek := s.Kek
	if kek == nil {
		kek, err = core.LoadKek(dir)
		if err != nil {
			return err
		}
	}
	keys := core.NewKeys(kek, s.Ungated, nil)
	sk, err := keys.Create(ctx, s.Ungated, true)
	if err != nil {
		return err
	}

	// Kept before anything is printed, so that a failure is said first.
	var opsPw, admPw Password
	kept := false
	if ops != nil {
		opsPw = Password{Tenant: clusterAlias, Alias: operator, Secret: ops.password}
		admPw = Password{Tenant: tenant, Alias: admin, Secret: adm.password}
		if stash != nil {
			if err := stash.Put(ctx, []Password{opsPw, admPw}); err != nil {
				fmt.Fprintf(out, "WARNING: the passwords could not be kept where they belong: %v\n", err)
				fmt.Fprintf(out, "WARNING: they are printed below instead, once, and whatever collects this output keeps them too;\n")
				fmt.Fprintf(out, "WARNING: sign in, then give both people new ones with `shale holder issue-password`\n\n")
			} else {
				kept = true
			}
		}
	}
	shown := func(p Password) string {
		if kept {
			return stash.Where(p)
		}

		return p.Secret
	}

	fmt.Fprintf(out, "state       %s\n", dir)
	fmt.Fprintf(out, "ca          %s\n", pki.Fingerprint(ca.Cert))
	fmt.Fprintf(out, "signing key %s\n", sk.GetAlias())
	if ops != nil {
		fmt.Fprintf(out, "tenant      @%s   %s\n", clusterAlias, ct)
		fmt.Fprintf(out, "operator    @%s/%s   %s   password: %s\n", clusterAlias, operator, ops.person.Id, shown(opsPw))
		fmt.Fprintf(out, "tenant      @%s   %s\n", tenant, t)
		fmt.Fprintf(out, "admin       @%s/%s   %s   password: %s\n", tenant, admin, adm.person.Id, shown(admPw))
		if !team.IsZero() {
			fmt.Fprintf(out, "operators   the members of %s   %s: @%s/%s\n", s.Operators.Team(), team, tenant, admin)
		}
		if kept {
			fmt.Fprintf(out, "\n%s; ", stash.Read(admPw))
			fmt.Fprintf(out, "sign in with `shale login @%s/%s`, and give somebody a new one with `shale holder issue-password`\n", tenant, admin)
		} else {
			fmt.Fprintf(out, "\nthe passwords are printed once; sign in with `shale login @%s/%s`, and give somebody a new one with `shale holder issue-password`\n", tenant, admin)
		}
	} else {
		fmt.Fprintf(out, "people      at roster %s: the tenants it holds keys for are", c.Auth.Roster.Addr)
		for _, v := range slices.Sorted(maps.Keys(c.Auth.Roster.Keys)) {
			fmt.Fprintf(out, " @%s", v)
		}
		switch {
		case s.Operators != nil:
			fmt.Fprintf(out, "\n            operators are the members of %s, by roster's word", s.Operators.Team())
		case err != nil:
			fmt.Fprintf(out, "\n            @%s has no key here yet, so there are no cluster operators until it does", clusterAlias)
		default:
			fmt.Fprintf(out, "\n            @%s is %s", clusterAlias, ct)
		}
		fmt.Fprintf(out, "\n            nobody was made at roster; a person gets their rows here the first time they sign in\n")
	}
	if c.IsDev() {
		fmt.Fprintf(out, "\ndevelopment mode: call as @%s/%s on the tenant API and @%s/%s on the cluster API\n", tenant, admin, clusterAlias, operator)
	}

	return nil
}

func flagOr(self *xli.Command, name, def string) string {
	v, ok := flg.Find[string](self, name)
	if !ok || v == "" {
		return def
	}

	return v
}

// ApplyDev turns a configuration into development mode (§34.7): everything
// under one directory, SQLite, an in-memory broker, plaintext, the local
// node and relay adopted automatically.
func ApplyDev(c *cmd.Config, dir string) {
	abs, err := filepath.Abs(dir)
	if err == nil {
		dir = abs
	}
	c.Dev = dir
	if c.State == "" {
		c.State = dir
	}
	if c.Db.Driver == "" || c.Db.Dsn == "" {
		c.Db.Driver = "sqlite3"
		c.Db.Dsn = "file:" + filepath.Join(dir, "control", "shale.db") + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	}
	c.Db.Migrate = true
	if c.Watch.Broker == "" {
		c.Watch.Broker = "memory"
	}
	c.Control.AutoAdopt = true
	// The addresses of §34.7's single machine, on loopback: payday's own
	// defaults (50051, 8080) would otherwise stand, and the CLI's defaults
	// below would dial the wrong ports.
	if c.Server.Addr == "" || c.Server.Addr == ":50051" {
		c.Server.Addr = "127.0.0.1:7400"
	}
	if c.Server.Http.Addr == "" || c.Server.Http.Addr == ":8080" {
		c.Server.Http.Addr = "127.0.0.1:7402"
	}
	if c.Cluster.Http.Addr == "" || c.Cluster.Http.Addr == ":8080" {
		c.Cluster.Http.Addr = "127.0.0.1:7403"
	}
	// A browser (§40.4). The console is served at the root of the tenant
	// API's HTTP listener, so both listeners have to answer what a page can
	// speak, and each has to name the origins a page calls it from: the
	// console's own origin for the cluster listener, since 7403 is a
	// different origin than 7402, and vite's dev server for both, which is
	// where the console itself is worked on. Production writes these down;
	// development mode is already plaintext on loopback, and a console that
	// loads and then cannot call anything is not a mode worth having.
	c.Server.Http.AllowWeb = true
	c.Cluster.Http.AllowWeb = true
	if len(c.Server.Http.Origins) == 0 {
		c.Server.Http.Origins = devOrigins(c.Server.Http.Addr)
	}
	if len(c.Cluster.Http.Origins) == 0 {
		c.Cluster.Http.Origins = devOrigins(c.Server.Http.Addr)
	}
	if len(c.Storage.Sinks) == 0 {
		c.Storage.Sinks = []cmd.SinkConfig{{Path: filepath.Join(dir, "sink")}}
	}
	if c.Client.Addr == "" {
		c.Client.Addr = "http://127.0.0.1:7400"
	}
	if c.Client.ClusterAddr == "" {
		c.Client.ClusterAddr = "http://127.0.0.1:7401"
	}
	os.MkdirAll(filepath.Join(dir, "control"), 0o700)
}

// devConsolePort is where `npm run dev` serves the console (vite's default).
const devConsolePort = "5173"

// devOrigins are the origins a page reaches a development listener from,
// given the address the console is served on: that address by either
// spelling of loopback, since a browser tells `localhost` and `127.0.0.1`
// apart, and vite's dev server beside it. A host named in the address that
// is neither is kept as well, for a development mode reached over a LAN.
func devOrigins(addr string) []string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil
	}
	hosts := []string{"127.0.0.1", "localhost"}
	if host != "" && host != "0.0.0.0" && host != "::" && !slices.Contains(hosts, host) {
		hosts = append(hosts, host)
	}
	out := make([]string, 0, 2*len(hosts))
	for _, h := range hosts {
		out = append(out, "http://"+net.JoinHostPort(h, port), "http://"+net.JoinHostPort(h, devConsolePort))
	}

	return out
}

var errNotInit = errors.New("not initialized: run `shale init`")

var _ = z.Ptr[int]
