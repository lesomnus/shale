package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
	"github.com/lesomnus/z"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
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
		},

		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, next xli.Next) error {
			if v, ok := flg.Find[string](self, "dev"); ok && v != "" {
				ApplyDev(c, v)
			}
			if v, ok := flg.Find[bool](self, "if-needed"); ok && v {
				if _, err := os.Stat(filepath.Join(c.StateDir("control"), "kek")); err == nil {
					self.Printf("already initialized: %s\n", c.StateDir("control"))
					return nil
				}
			}

			return Init(ctx, c, flagOr(self, "tenant", "acme"), flagOr(self, "admin", "admin"), flagOr(self, "operator", "ops"), self)
		}),
	}
}

// Init does the work of `shale init`, writing what it made to `out`.
func Init(ctx context.Context, c *cmd.Config, tenant, admin, operator string, out io.Writer) error {
	clusterAlias := c.Control.ClusterTenant
	if clusterAlias == "" {
		clusterAlias = "cluster"
	}

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

	ct, err := s.Ungated.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: clusterAlias, Name: "cluster operators"}.Build())
	if err != nil {
		return fmt.Errorf("tenant %q: %w", clusterAlias, err)
	}
	op, err := s.Ungated.Holder().Add(ctx, api.HolderAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: ct.GetId()}.Build(), Alias: operator, AllSites: true,
	}.Build())
	if err != nil {
		return fmt.Errorf("operator %q: %w", operator, err)
	}

	t, err := s.Ungated.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: tenant}.Build())
	if err != nil {
		return fmt.Errorf("tenant %q: %w", tenant, err)
	}
	h, err := s.Ungated.Holder().Add(ctx, api.HolderAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: t.GetId()}.Build(), Alias: admin, AllSites: true,
	}.Build())
	if err != nil {
		return fmt.Errorf("admin %q: %w", admin, err)
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

	pv := func(b []byte) string { id, _ := pdid.From(b); return id.String() }
	fmt.Fprintf(out, "state       %s\n", dir)
	fmt.Fprintf(out, "ca          %s\n", pki.Fingerprint(ca.Cert))
	fmt.Fprintf(out, "signing key %s\n", sk.GetAlias())
	fmt.Fprintf(out, "tenant      @%s   %s\n", clusterAlias, pv(ct.GetId()))
	fmt.Fprintf(out, "operator    @%s/%s   %s\n", clusterAlias, operator, pv(op.GetId()))
	fmt.Fprintf(out, "tenant      @%s   %s\n", tenant, pv(t.GetId()))
	fmt.Fprintf(out, "admin       @%s/%s   %s\n", tenant, admin, pv(h.GetId()))
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

var errNotInit = errors.New("not initialized: run `shale init`")

var _ = z.Ptr[int]
