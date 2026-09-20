// Package cli is this app's command line, and what only a process does.
//
// `cmd` is the wiring both entry points share; this package is everything on
// the other side of that: parsing arguments, the database engines a process
// opens, and the commands of §32.
package cli

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/cmd"
	entmigrate "github.com/lesomnus/shale/internal/ent/migrate"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// Cmd is this app's own command line: what payday supplies, what the app
// has of its own, and the generated verbs of every entity (§32).
func Cmd(c *cmd.Config) *xli.Command {
	root := &xli.Command{
		Name:  cmd.Name,
		Brief: "a loss-tolerant object store for CCTV",

		Flags: flg.Flags{
			pdcmd.ConfigFlag(),
			&flg.String{Name: "as", Brief: "call as @tenant/alias with the plain header (development)"},
			&flg.String{Name: "addr", Brief: "the tenant API address"},
			&flg.String{Name: "cluster-addr", Brief: "the cluster API address"},
			&flg.String{Name: "dev", Brief: "development mode: the directory `serve all --dev` runs in"},
		},

		Commands: []*xli.Command{
			pdcmd.NewCmdVersion(),
			pdcmd.NewCmdConfig(cmd.Loader, c),
			NewCmdInit(c),
			NewCmdServe(c),
			NewCmdLogin(c),
		},

		Handler: xli.Chain(pdcmd.Load(cmd.Loader, c), applyClientFlags(c), xli.RequireSubcommand()),
	}

	// Two trees, one per surface (§32): the generated verbs of the cluster's
	// entities reach the cluster API, the rest the tenant API.
	if t, err := pdcmd.New(&connector{c: c, cluster: false}); err == nil {
		for g := range clusterGroups {
			t.Drop(g)
		}
		addCustom(t, c, false)
		addProducerCommands(t, c)
		root.Commands = append(root.Commands, t.Commands()...)
	}
	if t, err := pdcmd.New(&connector{c: c, cluster: true}); err == nil {
		for g := range tenantGroups {
			t.Drop(g)
		}
		addCustom(t, c, true)
		root.Commands = append(root.Commands, t.Commands()...)
	}

	return root
}

func applyClientFlags(c *cmd.Config) xli.Handler {
	return xli.OnRunPass(func(ctx context.Context, self *xli.Command, next xli.Next) error {
		if v, ok := flg.Find[string](self, "dev"); ok && v != "" {
			ApplyDev(c, v)
		}
		if v, ok := flg.Find[string](self, "as"); ok && v != "" {
			c.Client.As = v
		}
		if v, ok := flg.Find[string](self, "addr"); ok && v != "" {
			c.Client.Addr = v
		}
		if v, ok := flg.Find[string](self, "cluster-addr"); ok && v != "" {
			c.Client.ClusterAddr = v
		}

		return next(ctx)
	})
}

// Migrate brings the database into the shape this app's schema says.
func Migrate(ctx context.Context, s *cmd.Server) error {
	return entmigrate.NewSchema(s.Drv).Create(ctx)
}

// clusterGroups are the command groups that talk to the cluster API, and
// tenantGroups the ones that talk to the tenant API.
var clusterGroups = map[string]bool{
	"node": true, "relay": true, "device": true, "sink": true, "signing-key": true,
	"placement-policy": true, "upload-policy": true, "address-policy": true, "tenant": true, "outbox": true,
}

var tenantGroups = map[string]bool{
	"set": true, "source": true, "object": true, "attempt": true, "site": true, "site-member": true,
	"producer": true, "reader": true, "holder": true, "audit": true,
}

// connector dials one surface (§32). It is asked when a command runs, after
// the configuration was read.
type connector struct {
	c       *cmd.Config
	cluster bool
}

func (r *connector) Connect(ctx context.Context) (pdcmd.Conn, func(), error) {
	addr := r.c.Client.Addr
	if r.cluster {
		addr = r.c.Client.ClusterAddr
	}
	if addr == "" {
		if r.cluster {
			addr = "127.0.0.1:7401"
		} else {
			addr = "127.0.0.1:7400"
		}
	}

	conn, err := dial(r.c, addr)
	if err != nil {
		return nil, nil, err
	}

	return conn, func() { conn.Close() }, nil
}

// dial opens a connection as the CLI: plaintext with the plain header when
// the address says http, else TLS against the CP's CA.
func dial(c *cmd.Config, addr string) (*grpc.ClientConn, error) {
	orig := addr
	plain := false
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		plain = u.Scheme == "http"
		addr = u.Host
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "7400")
	}

	var opts []grpc.DialOption
	if plain {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		pool, err := caPool(c)
		if err != nil {
			return nil, err
		}
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	switch {
	case c.Client.As != "":
		opts = append(opts, auth.Inject(auth.PlainProvider(c.Client.As))...)
	case c.Client.Token != "":
		opts = append(opts, auth.Inject(auth.BearerProvider(c.Client.Token))...)
	default:
		if cookie := savedSession(orig); cookie != "" {
			opts = append(opts, auth.Inject(cookieProvider(cookie))...)
		}
	}

	return grpc.NewClient(addr, opts...)
}

// cookieProvider sends a session cookie as the `cookie` metadata the
// session handler reads.
func cookieProvider(cookie string) auth.Provider {
	return auth.ProviderFunc(func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "cookie", cookie)
	})
}

func caPool(c *cmd.Config) (*x509.CertPool, error) {
	path := c.Client.CaFile
	if path == "" {
		path = filepath.Join(c.StateDir("control"), cmd.CaCertFile)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("no CA to verify the control plane against; set client.ca_file")
	}

	return pki.Pool(b)
}

var _ = core.DomNode
