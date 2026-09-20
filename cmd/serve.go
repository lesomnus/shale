package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/lesomnus/otx/log"
	"github.com/protobuf-orm/ent/dialect"
	entsql "github.com/protobuf-orm/ent/dialect/sql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/grpcx"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"
	"github.com/lesomnus/payday/watch"
	"github.com/lesomnus/payday/web"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/core"
	"github.com/lesomnus/shale/server/pd"
)

// Surface is which API a listener serves (§35.2).
type Surface int

const (
	// SurfaceTenant is producers, readers, and tenant admins: the wall is on.
	SurfaceTenant Surface = iota
	// SurfaceCluster is Storage Nodes, Relays, and cluster operators: spans
	// tenants; internal network only.
	SurfaceCluster
)

// Files in the CP's state directory (§34.3).
const (
	CaCertFile = "ca.crt"
	CaKeyFile  = "ca.key"
	CpCertFile = "cp.crt"
	CpKeyFile  = "cp.key"
)

// Server is a built Control Plane: the database it runs on and the two
// stacks it answers through.
type Server struct {
	Db  *sql.DB
	Ent *ent.Client
	// Drv is what the client was built on, kept because a transaction is
	// begun on a driver.
	Drv     dialect.Driver
	Dialect string
	Watch   *watch.Watch

	// Walled is what a caller reaches, and Ungated is what the deployment
	// does its own work through. Never hand Ungated to anything a caller can
	// reach.
	Walled  api.Server
	Ungated api.Server

	// Deps is what the core layer was built with; the CLI's own commands and
	// tests read the same keys and CA.
	Deps  *core.Deps
	Sites *core.Sites
	Jobs  *core.Jobs

	// CA is the built-in CA, nil before `shale init`.
	CA *pki.CA
	// Kek is nil before `shale init`.
	Kek core.Kek

	// ClusterTenant holds the cluster operators; Nil before init.
	ClusterTenant pdid.Id

	// Auth is how a credential is read. Set by [Build] to mTLS plus, in
	// development, the plain header; sessions are added by the CLI.
	Auth auth.Handler

	// Spin is whatever this deployment has to run besides answering.
	Spin []any

	cfg Config
}

// Build opens the database and stacks the servers.
func Build(ctx context.Context, c Config) (*Server, error) {
	db, dia, err := c.Db.Open(ctx)
	if err != nil {
		return nil, err
	}

	drv := entsql.OpenDB(dia, db)
	client := ent.NewClient(ent.Driver(drv))

	b, err := c.Watch.Build(c.Db)
	if err != nil {
		db.Close()
		return nil, err
	}
	w := watch.New(b)

	rec := bare.Recorders{pd.Recorder(), pd.WatchRecorder(w)}
	if c.Watch.Outbox {
		rec = append(rec, pd.OutboxRecorder())
	}
	opts := []bare.Option{bare.WithMinter(pd.Minter()), bare.WithRecorder(rec)}

	// The server with no wall, which the deployment works through.
	sink, err := pd.NewSink(client, opts...)
	if err != nil {
		db.Close()
		return nil, err
	}
	ungated, err := api.Build(sink.WithWatch(w), pd.AuditBuild())
	if err != nil {
		db.Close()
		return nil, err
	}

	s := &Server{Db: db, Ent: client, Drv: drv, Dialect: dia, Watch: w, Ungated: ungated, cfg: c}

	// The CA and the KEK, when `shale init` has made them.
	dir := c.StateDir("control")
	if ca, err := pki.Load(filepath.Join(dir, CaCertFile), filepath.Join(dir, CaKeyFile)); err == nil {
		s.CA = ca
	} else if !errors.Is(err, os.ErrNotExist) {
		db.Close()
		return nil, err
	}
	if kek, err := core.LoadKek(dir); err == nil {
		s.Kek = kek
	} else if !errors.Is(err, os.ErrNotExist) {
		db.Close()
		return nil, err
	}

	// The cluster tenant, once there is one.
	alias := c.Control.ClusterTenant
	if alias == "" {
		alias = "cluster"
	}
	if t, err := ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Alias: &alias}.Build()}.Build()); err == nil {
		s.ClusterTenant, _ = pdid.From(t.GetId())
	}

	deps := &core.Deps{
		Ent:           client,
		Drv:           drv,
		Own:           ungated,
		Keys:          core.NewKeys(s.Kek, ungated, nil),
		CA:            s.CA,
		ClusterTenant: s.ClusterTenant,
		Readopt:       c.Control.Readopt,
		Dev:           c.IsDev(),
		Log:           slog.Default(),
	}
	if c.Control.AutoAdopt {
		deps.AutoAdopt = func(kind pdid.Domain, _ *api.HostJoin, _ string) bool {
			return kind == core.DomNode || kind == core.DomRelay
		}
	}
	s.Deps = deps
	s.Sites = core.NewSites(deps)

	// The stack a caller reaches: the wall and the site axis on the sink,
	// then Shale's own layer, the trail, secrets cleared on the way out, and
	// the gate outermost so nothing behind it asks again.
	walled, err := pd.NewSink(client, append(opts, bare.WithScope(bare.Scopes{pd.Wall(), pd.Grouped(s.Sites.Of)}))...)
	if err != nil {
		db.Close()
		return nil, err
	}
	stacked, err := api.Build(walled.WithWatch(w), core.Build(deps), pd.AuditBuild(), pd.SecretBuild(), pd.GateBuild())
	if err != nil {
		db.Close()
		return nil, err
	}
	s.Walled = stacked

	s.Jobs = core.NewJobs(deps)
	if c.Control.JobsEvery > 0 {
		s.Jobs.Every = c.Control.JobsEvery
	}
	s.Spin = append(s.Spin, s.Jobs)
	if c.Watch.Outbox && b != nil {
		s.Spin = append(s.Spin, pd.Drain(client, b, c.Watch.Every()))
	}

	// Who is calling: the certificate, and in development the plain header.
	hs := []auth.Handler{auth.MTls()}
	if c.IsDev() {
		hs = append(hs, auth.Plain())
	}
	s.Auth = auth.Seq(hs...)

	return s, nil
}

func (s *Server) Close() error { return s.Db.Close() }

// tlsConfig is the CP's server TLS: external files when configured, else
// the certificate `shale init` issued from the built-in CA. Client
// certificates are verified when given, so people without one still get in
// through a session (§33.5). Nil means plaintext, which only development
// mode allows.
func (s *Server) tlsConfig() (*tls.Config, error) {
	c := s.cfg
	if c.IsDev() && c.Control.CertFile == "" {
		// Development mode is plaintext (§33.5), so the hosts in the same
		// process and the CLI need no certificates.
		return nil, nil
	}
	certFile, keyFile := c.Control.CertFile, c.Control.KeyFile
	if certFile == "" {
		dir := c.StateDir("control")
		certFile, keyFile = filepath.Join(dir, CpCertFile), filepath.Join(dir, CpKeyFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		if c.IsDev() && errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("control plane certificate: %w (run `shale init`, or set control.cert_file)", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		NextProtos:   []string{"h2", "http/1.1"},
	}
	if s.CA != nil {
		pool := x509.NewCertPool()
		pool.AddCert(s.CA.Cert)
		cfg.ClientCAs = pool
	}

	return cfg, nil
}

// Grpc builds the server every call of one surface arrives at.
func (s *Server) Grpc(ctx context.Context, surface Surface, opts ...grpc.ServerOption) (*grpc.Server, error) {
	c := s.cfg
	sc := c.Server
	var policy gate.Policy = core.TenantPolicy{}
	if surface == SurfaceCluster {
		sc = c.Cluster
		policy = core.ClusterPolicy{ClusterTenant: s.ClusterTenant}
	}

	chain := grpcx.Serving(ctx, grpcx.WithDeadline(sc.CallTimeout())).
		WithUnary(auth.InterceptorUnary(s.Auth, Resolver(s), core.Public)).
		WithStream(auth.InterceptorStream(s.Auth, Resolver(s), core.Public)).
		WithUnary(grpcx.LimitUnary(sc.Limiter(), gate.ByTenant())).
		With(gate.Interceptor(policy)).
		With(s.Watch.Interceptor()).
		WithUnary(grpcx.ClosedUnary(sc.Closed()))

	os := append(opts, chain.ServerOptions()...)
	if sc.Tls.Active() {
		vs, err := sc.GrpcOptions()
		if err != nil {
			return nil, err
		}
		os = append(os, vs...)
	} else {
		cfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			os = append(os, grpc.Creds(credentials.NewTLS(cfg)))
		}
	}

	g := grpc.NewServer(os...)
	switch surface {
	case SurfaceTenant:
		registerTenant(g, s.Walled)
	case SurfaceCluster:
		api.RegisterServer(g, s.Walled)
	}
	healthpb.RegisterHealthServer(g, health.NewServer())

	if b, err := pd.Batch(s.Walled, s.Drv, sc.Guard(policy)); err == nil {
		pdpb.RegisterBatchServiceServer(g, b)
	} else {
		log.From(ctx).WarnContext(ctx, "no batch", slog.String("why", err.Error()))
	}

	return g, nil
}

// registerTenant mounts the tenant API and nothing else (§35.2).
func registerTenant(g grpc.ServiceRegistrar, s api.Server) {
	api.RegisterSetServiceServer(g, s.Set())
	api.RegisterSourceServiceServer(g, s.Source())
	api.RegisterObjectServiceServer(g, s.Object())
	api.RegisterAttemptServiceServer(g, s.Attempt())
	api.RegisterSiteServiceServer(g, s.Site())
	api.RegisterSiteMemberServiceServer(g, s.SiteMember())
	api.RegisterProducerServiceServer(g, s.Producer())
	api.RegisterReaderServiceServer(g, s.Reader())
	api.RegisterHolderServiceServer(g, s.Holder())
	api.RegisterAuditServiceServer(g, s.Audit())
}

// Listener is one served surface.
type Listener struct {
	Surface Surface
	Addr    string
	// Http is the second listener for browsers, when configured.
	Http *http.Server
}

// Serve answers one surface on `l` until the context is done.
func (s *Server) Serve(ctx context.Context, surface Surface, l net.Listener) error {
	g, err := s.Grpc(ctx, surface)
	if err != nil {
		return err
	}

	stop, err := s.serveHttp(ctx, surface, g)
	if err != nil {
		return err
	}
	defer stop()

	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()

	return g.Serve(l)
}

// serveHttp is the second listener for a browser, on the same server (a
// Connect call arrives as JSON over POST and goes through the same
// interceptors and behind the same wall). Nothing opens unless configured.
func (s *Server) serveHttp(ctx context.Context, surface Surface, g *grpc.Server) (func(), error) {
	sc := s.cfg.Server
	if surface == SurfaceCluster {
		sc = s.cfg.Cluster
	}
	if !sc.Http.Serves() {
		return func() {}, nil
	}

	h, err := web.New(sc.Http, g)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", sc.Http.Addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{Handler: h}
	if cfg, err := s.tlsConfig(); err == nil && cfg != nil {
		srv.TLSConfig = cfg
		go func() {
			if err := srv.ServeTLS(l, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.From(ctx).ErrorContext(ctx, "http", slog.String("err", err.Error()))
			}
		}()
	} else {
		go func() {
			if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.From(ctx).ErrorContext(ctx, "http", slog.String("err", err.Error()))
			}
		}()
	}

	log.From(ctx).InfoContext(ctx, "http", slog.String("addr", l.Addr().String()))

	return func() { srv.Close() }, nil
}

// ListenAddr is where a surface listens: the configuration, or the
// defaults of §34.1 (7400 for the tenant API, 7401 for the cluster API,
// on localhost when `all` serves both).
func (c Config) ListenAddr(surface Surface, all bool) string {
	switch surface {
	case SurfaceCluster:
		if c.Cluster.Addr != "" {
			return c.Cluster.Addr
		}
		if all {
			return "127.0.0.1:7401"
		}

		return ":7401"
	default:
		if c.Server.Addr != "" {
			return c.Server.Addr
		}

		return ":7400"
	}
}

// Hostnames are what the CP certificate should carry: the machine's names
// and addresses, and what the operator configured.
func Hostnames(extra []string) (dns []string, ips []net.IP) {
	dns = append(dns, "localhost")
	if h, err := os.Hostname(); err == nil && h != "" {
		dns = append(dns, strings.ToLower(h))
	}
	dns = append(dns, extra...)
	ips = append(ips, net.ParseIP("127.0.0.1"), net.ParseIP("::1"))
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && !n.IP.IsLinkLocalUnicast() {
				ips = append(ips, n.IP)
			}
		}
	}

	return dns, ips
}
