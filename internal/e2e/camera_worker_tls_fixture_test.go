package e2e_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// A tenant CP only: real TLS/host certificates without starting cameras or nodes.
func cameraTLSControl(t *testing.T) (*cmd.Server, string, *grpc.ClientConn) {
	t.Helper()
	x := require.New(t)
	ctx := context.Background()
	c := &cmd.Config{}
	cli.ApplyDev(c, t.TempDir())
	c.Control.CertFile = filepath.Join(c.StateDir("control"), cmd.CpCertFile)
	c.Control.KeyFile = filepath.Join(c.StateDir("control"), cmd.CpKeyFile)
	c.Server.Http.Addr = "127.0.0.1:0"
	c.Cluster.Http.Addr = "127.0.0.1:0"
	if dsn := os.Getenv("SHALE_E2E_DB_DSN"); dsn != "" {
		c.Db.Driver, c.Db.Dsn = "pgx", dsn
		db, err := sql.Open("pgx", dsn)
		x.NoError(err)
		_, err = db.Exec("drop schema public cascade; create schema public")
		x.NoError(err)
		x.NoError(db.Close())
	}
	x.NoError(cli.Init(ctx, c, "acme", "admin", "ops", io.Discard, nil))
	s, err := cmd.Build(ctx, *c)
	x.NoError(err)
	t.Cleanup(func() { x.NoError(s.Close()) })
	x.NoError(s.Prepare(ctx))
	g, err := s.Grpc(ctx, cmd.SurfaceTenant)
	x.NoError(err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	x.NoError(err)
	done := make(chan error, 1)
	go func() { done <- g.Serve(l) }()
	t.Cleanup(func() {
		g.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("TLS fixture server did not stop")
		}
	})
	pool := x509.NewCertPool()
	pool.AddCert(s.CA.Cert)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))}
	opts = append(opts, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	admin, err := grpc.NewClient(l.Addr().String(), opts...)
	x.NoError(err)
	t.Cleanup(func() { admin.Close() })
	return s, "https://" + l.Addr().String(), admin
}

func cameraTLSPrincipal(t *testing.T, server *cmd.Server, cp, principal string) *grpc.ClientConn {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(server.CA.Cert)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))}
	opts = append(opts, auth.Inject(auth.PlainProvider(principal))...)
	conn, err := grpc.NewClient(strings.TrimPrefix(cp, "https://"), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// Prototype the fixture with an existing host role before Worker RPCs exist.
func TestCameraWorkerTLSProducerLifecycle(t *testing.T) {
	x := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, cp, admin := cameraTLSControl(t)
	tenant := "acme"
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: &tenant}.Build(), Alias: "tls-fixture"}.Build())
	x.NoError(err)
	state := t.TempDir()
	agent := &hostagent.Agent{Kind: core.DomProducer, HardwareId: "tls-camera-host-fixture", Store: pki.Store{Dir: state}, Cp: cp, CaHash: pki.Fingerprint(s.CA.Cert), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	x.NoError(agent.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewProducerServiceClient(conn).Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := agent.Dial(ctx)
	x.NoError(err)
	host, err := agent.HostJoin()
	x.NoError(err)
	pending, err := join(ctx, bootstrap, host)
	x.NoError(err)
	x.Equal(api.HostState_HOST_STATE_PENDING, pending.GetState())
	bootstrap.Close()
	_, err = api.NewProducerServiceClient(admin).Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	x.NoError(err)
	_, err = agent.Join(ctx, join)
	x.NoError(err)
	old, err := agent.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { old.Close() })
	ref := api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: agent.Id().Bytes()}.Build()}.Build()
	_, err = api.NewProducerServiceClient(old).Get(ctx, ref)
	x.NoError(err)
	x.NoError(agent.Renew(ctx, old, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
		v, err := api.NewProducerServiceClient(conn).RenewCertificate(ctx, api.ProducerRenewCertificateRequest_builder{Csr: csr}.Build())
		if err != nil {
			return nil, nil, err
		}
		return v.GetCertificate(), v.GetCaBundle(), nil
	}))
	_, err = api.NewProducerServiceClient(old).Get(ctx, ref)
	x.Equal(codes.Unauthenticated, status.Code(err), "an already-open TLS connection cannot retain an old serial")
	restarted := &hostagent.Agent{Kind: core.DomProducer, HardwareId: "tls-camera-host-fixture", Store: pki.Store{Dir: state}, Cp: cp, Log: agent.Log}
	x.NoError(restarted.Init())
	x.Equal(agent.Id(), restarted.Id())
	current, err := restarted.Dial(ctx)
	x.NoError(err)
	t.Cleanup(func() { current.Close() })
	_, err = api.NewProducerServiceClient(current).Get(ctx, ref)
	x.NoError(err)
	_, err = api.NewProducerServiceClient(admin).Erase(ctx, api.ProducerRef_builder{Id: agent.Id().Bytes()}.Build())
	x.NoError(err)
	_, err = api.NewProducerServiceClient(current).Get(ctx, ref)
	x.Equal(codes.Unauthenticated, status.Code(err))
}
