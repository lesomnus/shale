//go:build js && wasm

// Command wasm is Shale's control plane served from inside the page it
// serves: the sandbox the console is developed and tested against (§40).
//
// It is the same server the process runs -- the same generated services,
// the same stack, the same wall and gate policies from the same schema.
// What differs is one line each: the database is SQLite in a Web Worker
// instead of a file or a socket, calls arrive over a message port instead
// of HTTP/2, and the CA and the key that wraps signing keys are made in
// memory, since there is no file system to keep them in. What a control
// plane has on the other side of a socket -- nodes with disks, a relay,
// producers beside cameras, a reader -- is played by `internal/sandbox`
// through the same calls the real ones make.
//
//	GOOS=js GOARCH=wasm go build -tags grpcnotrace -o ts/public/app.wasm ./wasm
//
// A browser reload restarts the whole server: new instance, new database,
// nothing left over. Somebody working on the console starts no backend and
// does not have to remember what state they left it in.
//
// The tag is gRPC's own: it drops `golang.org/x/net/trace`, a debug page
// this never serves, which is most of what the tag saves.
package main

import (
	"context"
	"log"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"

	drpc "github.com/lesomnus/grpc-dgram"
	"github.com/lesomnus/grpc-dgram/transport/jsport"

	pdauth "github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"

	// SQLite in a worker of its own. The other driver runs the engine on
	// wazero, which is a wasm runtime written in Go, so here it would be
	// wasm inside wasm.
	_ "github.com/lesomnus/payday/config/dbsqlite3wasm"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	entmigrate "github.com/lesomnus/shale/internal/ent/migrate"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sandbox"
	"github.com/lesomnus/shale/server/core"
)

func main() {
	ctx := context.Background()

	// Both databases held in memory rather than in OPFS, which is the
	// decision that makes a reload a fresh server: a sandbox that
	// remembered would be a sandbox somebody has to clear.
	//
	// The leading slash is load-bearing: `file:sandbox` is a relative name
	// to the memdb VFS, resolved per connection, so a second connection in
	// the pool would open an empty database of its own.
	//
	// More than one connection, unlike payday's own sandbox: the control
	// plane reads through the pool from inside a transaction (an attempt's
	// rows while a lamina is patched, §13), and a pool of one is that read
	// waiting for the transaction that waits for it. The engine is one JS
	// thread, so the connections take turns rather than run at once.
	db := func(name string) config.DbConfig {
		return config.DbConfig{Driver: "sqlite3-wasm", Dsn: "file:/" + name + "?vfs=memdb", MaxOpenConns: 4}
	}
	s, err := cmd.Build(ctx, cmd.Config{
		Db: db("sandbox"),
		// Named, because payday refuses a deployment that leaves it unsaid:
		// `memory` is right for one replica and silently wrong for two.
		// Here it is right by construction; there is one of this server and
		// it is inside the page.
		Watch: config.WatchConfig{Broker: config.BrokerMemory},
		// Who people are: roster in this process too, on a database of its
		// own (§33.1).
		Auth: cmd.AuthConfig{Roster: identity.Config{Db: db("roster")}},
		// Nothing is adopted on its own: adopting is what the console is
		// for.
		Control: cmd.ControlConfig{Names: []string{"sandbox"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	// The schema is created rather than migrated: there is no database here
	// that outlives the page, so there is nothing for a migration to move.
	// (`cli.Migrate` is the same call, behind a package that also carries
	// the storage node and the relay, neither of which builds for a page.)
	if err := entmigrate.NewSchema(s.Drv).Create(ctx); err != nil {
		log.Fatal(err)
	}

	// The first rows and the hosts: what `shale init` writes and what a
	// deployment has on the network, through the server the wall was never
	// installed on.
	box := sandbox.New(s)
	box.Log = slog.Default()
	if err := box.Seed(ctx); err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := box.Run(ctx); err != nil {
			slog.Error("sandbox", "err", err.Error())
		}
	}()
	// The leader's housekeeping (§21, §20.4): DB-only, so it runs here too.
	go func() {
		if err := s.Jobs.Spin(ctx); err != nil {
			slog.Error("jobs", "err", err.Error())
		}
	}()

	// A server that is not gRPC's, taking the same services. The page has
	// one transport for both API surfaces, so the gate decides by the
	// service what a call is: the cluster surface's policy for its services,
	// the tenant surface's for the rest (§35.2), the same two policies
	// `shale serve` installs on its two listeners.
	//
	// `Plain` believes what the caller writes, which is what a sandbox is:
	// there is nobody else in the page to lie to.
	gw := jsport.NewGateway()
	srv := drpc.NewServer(gw,
		drpc.ChainUnaryInterceptor(
			busyAgain,
			pdauth.InterceptorUnary(pdauth.Plain(), cmd.Resolver(s), core.Public),
			gate.Unary(bothSurfaces{cluster: core.ClusterPolicy{ClusterTenant: s.ClusterTenant}}),
		),
		drpc.ChainStreamInterceptor(
			pdauth.InterceptorStream(pdauth.Plain(), cmd.Resolver(s), core.Public),
			gate.Stream(bothSurfaces{cluster: core.ClusterPolicy{ClusterTenant: s.ClusterTenant}}),
		),
	)
	api.RegisterServer(srv, s.Walled)

	// Publishing the entry point is the readiness signal, so nothing may be
	// published before the registration above is done -- and it blocks,
	// because a main that returns takes the instance down.
	log.Fatal(gw.Serve(ctx, srv))
}

// bothSurfaces is the two gate policies on one transport, told apart by
// the service a call is for.
type bothSurfaces struct {
	cluster core.ClusterPolicy
	tenant  core.TenantPolicy
}

func (p bothSurfaces) May(ctx context.Context, c gate.Call) error {
	if core.IsClusterService(c.Action) {
		return p.cluster.May(ctx, c)
	}

	return p.tenant.May(ctx, c)
}

func (p bothSurfaces) Where(ctx context.Context, c gate.Call) (frame.Tenants, error) {
	if core.IsClusterService(c.Action) {
		return p.cluster.Where(ctx, c)
	}

	return p.tenant.Where(ctx, c)
}

// busyAgain answers a page's call again when the database was busy: the
// sandbox's SQLite has no busy handler, so a write that meets a host's
// transaction fails at once rather than waiting a moment, which is what a
// server with a file behind it would have done.
func busyAgain(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var v any
	var err error
	for i := 0; i < 8; i++ {
		sandbox.DB.Lock()
		v, err = handler(ctx, req)
		sandbox.DB.Unlock()
		if err == nil || !strings.Contains(err.Error(), "database is locked") {
			return v, err
		}
		select {
		case <-ctx.Done():
			return v, err
		case <-time.After(time.Duration(20*(i+1)) * time.Millisecond):
		}
	}

	return v, err
}
