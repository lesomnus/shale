// Package core is what the Control Plane writes by hand: the operations that
// mean something (§35.4, §35.5), stacked as one layer over the generated
// servers so that every write it makes is on the trail, behind the wall, and
// published to watchers like any other.
//
// The layer answers the custom RPCs of every entity and adds rules to a few
// generated ones (a Source gets its ordinal, a Set copies its site onto its
// members). Everything else goes straight through.
package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/protoc-gen-orm-ent/runtime/enttx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/pki"
)

// Domains of the entities, as declared in the schema (§35.3). They are read
// off identifiers to tell what kind of actor is calling.
const (
	DomTenant          pdid.Domain = 1
	DomHolder          pdid.Domain = 2
	DomSet             pdid.Domain = 7
	DomSource          pdid.Domain = 8
	DomLamina          pdid.Domain = 9
	DomAttempt         pdid.Domain = 10
	DomNode            pdid.Domain = 12
	DomDevice          pdid.Domain = 13
	DomSink            pdid.Domain = 14
	DomSigningKey      pdid.Domain = 15
	DomPlacementPolicy pdid.Domain = 16
	DomUploadPolicy    pdid.Domain = 18
	DomSite            pdid.Domain = 19
	DomSiteMember      pdid.Domain = 20
	DomAddressPolicy   pdid.Domain = 21
	DomProducer        pdid.Domain = 22
	DomReader          pdid.Domain = 23
	DomRelay           pdid.Domain = 24
)

// Deps is what the layer needs besides the servers below it.
type Deps struct {
	// Ent is the client the layer reads with where the generated List cannot
	// say what it needs (a time range, an open attempt). Every such read
	// narrows by the caller's tenant itself.
	Ent *ent.Client
	// Drv is what a transaction is begun on.
	Drv dialect.Driver
	// Own is the server with no wall, for work outside any tenant: a host
	// joining, an event a node pushes about a row it cannot see.
	Own api.Server
	// Keys signs tokens.
	Keys *Keys
	// CA issues host certificates. Nil on a deployment with external
	// certificates only, where adoption cannot issue any.
	CA *pki.CA
	// Resolve turns a node into the endpoints a caller dials (§34.10).
	Resolve Resolver
	// AutoAdopt says whether a join is adopted at once, which `shale serve
	// all` does for the node and the relay in its own process (§34.7).
	AutoAdopt func(kind pdid.Domain, join *api.HostJoin, from string) bool
	// ClusterTenant holds the cluster operators (§33.1).
	ClusterTenant pdid.Id
	// Readopt is "manual" or "auto" (§33.4).
	Readopt string
	// Dev is development mode: plaintext endpoints.
	Dev bool
	// DialNode opens a node's control API for the leader's directives
	// (§34.9): over mTLS with the CP's certificate, checking that the
	// peer's certificate names `id`. Nil where no directives run.
	DialNode func(ctx context.Context, addr string, id pdid.Id) (*grpc.ClientConn, error)
	// Identity is roster, where people and tenants are (§33.1); nil in a
	// test that has none.
	Identity *identity.Store
	// NodeDownAfter is how long a host may go unheard before it is down
	// (§27), and SinkAutoAdoptAfter how long its sinks wait before the node
	// that reports them takes them over (§28.3). Zero is §36.1's default.
	NodeDownAfter      time.Duration
	SinkAutoAdoptAfter time.Duration
	// Now is the clock.
	Now func() time.Time
	Log *slog.Logger
	// M are the instruments; made from a bare context when nil.
	M *Metrics

	pol policies

	// reports caps what one producer may add to one node's score per day
	// (§27); per process, which is enough for a cap.
	reportsMu sync.Mutex
	reports   map[[2]pdid.Id][]time.Time

	// forecast remembers, per sink and epoch, whether the capacity forecast
	// let the sink in (§11.1), so a sink at the margin does not flap.
	forecastMu sync.Mutex
	forecast   map[forecastKey]bool
	// readmitAt is when the forecast last had every sink out, so that
	// warning comes once a minute while it lasts, not once an allocation.
	readmitAt time.Time
}

type forecastKey struct {
	sink  pdid.Id
	epoch int64
}

// warnReadmit says whether the warning that the forecast excludes every
// sink is due (§11.1): the first time, then once a minute.
func (d *Deps) warnReadmit(now time.Time) bool {
	d.forecastMu.Lock()
	defer d.forecastMu.Unlock()
	if !d.readmitAt.IsZero() && now.Sub(d.readmitAt) < time.Minute {
		return false
	}
	d.readmitAt = now

	return true
}

// allowReport says whether a producer's report against a node still counts
// today (§27).
func (d *Deps) allowReport(node, producer pdid.Id, now time.Time) bool {
	d.reportsMu.Lock()
	defer d.reportsMu.Unlock()
	if d.reports == nil {
		d.reports = map[[2]pdid.Id][]time.Time{}
	}
	k := [2]pdid.Id{node, producer}
	var kept []time.Time
	for _, t := range d.reports[k] {
		if now.Sub(t) < 24*time.Hour {
			kept = append(kept, t)
		}
	}
	if len(kept) >= ProducerReportCap {
		d.reports[k] = kept
		return false
	}
	d.reports[k] = append(kept, now)

	return true
}

func (d *Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}

	return time.Now()
}

// nodeDownAfter is §27's `node_down_after`, which says when a host stops
// counting as alive: nodes, and relays with them (§39.2).
func (d *Deps) nodeDownAfter() time.Duration {
	if d.NodeDownAfter > 0 {
		return d.NodeDownAfter
	}

	return DefaultNodeDownAfter
}

// sinkAutoAdoptAfter is §28.3's `sink_auto_adopt_after`.
func (d *Deps) sinkAutoAdoptAfter() time.Duration {
	if d.SinkAutoAdoptAfter > 0 {
		return d.SinkAutoAdoptAfter
	}

	return DefaultSinkAutoAdoptAfter
}

func (d *Deps) log() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}

	return slog.Default()
}

// Core is the layer.
type Core struct {
	api.Overlay
	d *Deps
}

// WithDeps is a Core with no server below it, for the few things a
// deployment asks the layer outside a request: verifying a password.
func (s Core) WithDeps(d *Deps) Core { return Core{d: d} }

// New stacks the layer over `next`.
func New(next api.Server, d *Deps) Core { return Core{api.NewOverlay(next), d} }

// Build makes a builder of this layer so that it can be stacked.
func Build(d *Deps) api.Builder { return builder{d} }

type builder struct{ d *Deps }

func (b builder) Build(next api.Server) (api.Server, error) { return New(next, b.d), nil }

var (
	_ api.Server               = Core{}
	_ enttx.Binder[api.Server] = Core{}
)

// WithDriver answers with this stack running on `drv`. Every layer writes
// this and none can inherit it; see the server guide.
func (s Core) WithDriver(drv dialect.Driver) (api.Server, error) {
	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		return nil, err
	}

	return New(next, s.d), nil
}

// tx runs `fn` with the stack below this layer rebound onto one transaction,
// so several writes land or fail together.
func (s Core) tx(ctx context.Context, fn func(ctx context.Context, next api.Server) error) error {
	drv, tx, err := dialect.BeginTx(ctx, s.d.Drv)
	if err != nil {
		return err
	}

	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		tx.Rollback()
		return err
	}
	ctx, err = s.inTx(ctx, drv)
	if err != nil {
		tx.Rollback()
		return err
	}

	if err := fn(ctx, next); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// inTx is the context of a transaction: what is read through `ent` and
// `own` inside it reads through the transaction rather than the pool, so
// a read sees the writes before it and no second connection is taken. A
// database that lets one writer exclude every reader (SQLite's memdb, the
// sandbox's) would refuse the pool's read outright; the others would
// answer from before the transaction began.
func (s Core) inTx(ctx context.Context, drv dialect.Driver) (context.Context, error) {
	own, err := enttx.Rebind(s.d.Own, drv)
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, txKey{}, txBound{ent: ent.NewClient(ent.Driver(drv)), own: own}), nil
}

type txKey struct{}

type txBound struct {
	ent *ent.Client
	own api.Server
}

// ent is the rows, through the transaction the context is in, else the
// pool.
func (s Core) ent(ctx context.Context) *ent.Client {
	if v, ok := ctx.Value(txKey{}).(txBound); ok {
		return v.ent
	}

	return s.d.Ent
}

// own is the server with no wall, through the transaction the context is
// in, else the pool.
func (s Core) own(ctx context.Context) api.Server {
	if v, ok := ctx.Value(txKey{}).(txBound); ok {
		return v.own
	}

	return s.d.Own
}

// ownTx is the same over the server with no wall.
func (s Core) ownTx(ctx context.Context, fn func(ctx context.Context, own api.Server) error) error {
	drv, tx, err := dialect.BeginTx(ctx, s.d.Drv)
	if err != nil {
		return err
	}

	own, err := enttx.Rebind(s.d.Own, drv)
	if err != nil {
		tx.Rollback()
		return err
	}
	ctx, err = s.inTx(ctx, drv)
	if err != nil {
		tx.Rollback()
		return err
	}

	if err := fn(ctx, own); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// actor is who is calling, refused when nobody vouched for the call.
func actor(ctx context.Context) (*frame.Frame, error) {
	return gate.Actor(ctx)
}

// kindOf is what kind of thing an identifier names.
func kindOf(id pdid.Id) pdid.Domain { return id.Domain() }

func tenantRef(id pdid.Id) *api.TenantRef {
	return api.TenantRef_builder{Id: id.Bytes()}.Build()
}

func idOf(b []byte) (pdid.Id, error) {
	id, err := pdid.From(b)
	if err != nil {
		return pdid.Nil, status.Errorf(codes.InvalidArgument, "identifier: %v", err)
	}

	return id, nil
}

func mustId(b []byte) pdid.Id {
	id, _ := pdid.From(b)

	return id
}

func invalid(field, msg string) error {
	return status.Errorf(codes.InvalidArgument, "%s: %s", field, msg)
}

// isZero says whether a value is its type's zero, which is how a nullable
// edge's key column reads when it is null.
func isZero[T comparable](v T) bool {
	var z T

	return v == z
}

// uuidsOf converts identifiers to the type ent's predicates take.
func uuidsOf[T any](ids []pdid.Id, conv func(pdid.Id) T) []T {
	out := make([]T, len(ids))
	for i, id := range ids {
		out[i] = conv(id)
	}

	return out
}

// aliasSuffix is the random tail of an identifier, since its head is the
// time it was minted and two rows minted in one minute share it.
func aliasSuffix(id pdid.Id) string {
	v := id.String()

	return v[len(v)-8:]
}

func failed(msg string, args ...any) error {
	return status.Errorf(codes.FailedPrecondition, msg, args...)
}
