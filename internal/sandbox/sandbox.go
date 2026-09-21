// Package sandbox is the hosts a sandbox has none of. The sandbox is the
// real control plane, compiled into the page the console is served from
// (§40); what it lacks is everything on the other side of a socket: storage
// nodes with disks, a relay, producers beside cameras, a reader. This
// package plays them, in the same process, through the same calls the real
// ones make -- Join, adoption, heartbeats, allocations, the node's stored
// events, a dark scene's Skip -- so what the console draws is what it would
// draw in a deployment, and the one thing faked is the bytes.
//
// It is plain Go, so it also runs against a control plane in a test, which
// is how it is checked: the sandbox is only worth having because it is the
// same server.
package sandbox

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// Sandbox is one run of the simulated hosts against a control plane.
type Sandbox struct {
	S   *cmd.Server
	Log *slog.Logger
	// Every is a heartbeat's interval; Segment how long a segment records.
	// Seconds in the page, faster in a test.
	Every   time.Duration
	Segment time.Duration

	tenant  pdid.Id
	cluster pdid.Id
	set     *api.Set

	mu      sync.Mutex
	commits map[pdid.Id]chan commit
}

// commit is a segment a producer finished that its node stores.
type commit struct {
	al      *api.Allocation
	cand    *api.Candidate
	started time.Time
	ended   time.Time
	size    int64
	source  []byte
	set     []byte
	tenant  []byte
}

// New prepares the hosts; Seed and Run do the work.
func New(s *cmd.Server) *Sandbox {
	return &Sandbox{S: s, Log: slog.Default(), Every: 5 * time.Second, Segment: 20 * time.Second, commits: map[pdid.Id]chan commit{}}
}

// As is a context as somebody: a person by name, a host by its id, with
// the scope the surface's gate policy gives them (§35.2), which is what
// the interceptors do for a call that arrives over a socket. The method
// is one the actor may call, to stand for the calls that follow.
func As(ctx context.Context, s *cmd.Server, id auth.Identity, method string) (context.Context, error) {
	f, err := cmd.Resolver(s).Resolve(ctx, id)
	if err != nil {
		return nil, err
	}
	// The credential of a call over a socket is what the auth handler
	// vouched for; here it is this process, which holds everything the
	// actor may see.
	g := frame.New(f.Actor, f.Tenant, frame.Whole())
	if m, ok := f.Row.(proto.Message); ok {
		g = g.WithRow(m)
	}
	var policy gate.Policy = core.TenantPolicy{}
	if f.Tenant == s.ClusterTenant || core.IsClusterService(method) {
		policy = core.ClusterPolicy{ClusterTenant: s.ClusterTenant}
	}

	return gate.Decide(frame.Into(ctx, g), policy, method)
}

func (b *Sandbox) as(ctx context.Context, id auth.Identity, method string) (context.Context, error) {
	return As(ctx, b.S, id, method)
}

func (b *Sandbox) asOps(ctx context.Context) (context.Context, error) {
	return b.as(ctx, auth.Identity{Tenant: "cluster", Alias: "ops"}, api.NodeService_List_FullMethodName)
}

func (b *Sandbox) asAdmin(ctx context.Context) (context.Context, error) {
	return b.as(ctx, auth.Identity{Tenant: "acme", Alias: "admin"}, api.SetService_List_FullMethodName)
}

func (b *Sandbox) asHost(ctx context.Context, id pdid.Id) (context.Context, error) {
	var method string
	switch id.Domain() {
	case core.DomNode:
		method = api.NodeService_Heartbeat_FullMethodName
	case core.DomRelay:
		method = api.RelayService_Heartbeat_FullMethodName
	case core.DomProducer:
		method = api.ProducerService_Heartbeat_FullMethodName
	default:
		method = api.ReaderService_Heartbeat_FullMethodName
	}

	return b.as(ctx, auth.Identity{Id: id.String()}, method)
}

// Seed is what `shale init` does, without a file system: the CA and the key
// that wraps signing keys are made in memory, the two tenants and the two
// people are provisioned, the first signing key is created, and a set is
// there for a producer to record.
func (b *Sandbox) Seed(ctx context.Context) error {
	s := b.S
	now := time.Now()
	ca, err := pki.NewCA("shale-sandbox-ca", now)
	if err != nil {
		return err
	}
	s.CA = ca
	s.Deps.CA = ca
	kek := make(core.Kek, 32)
	if _, err := rand.Read(kek); err != nil {
		return err
	}
	s.Kek = kek
	s.Deps.Keys = core.NewKeys(kek, s.Ungated, nil)

	for _, who := range [][2]string{{"cluster", "ops"}, {"acme", "admin"}} {
		p, _, err := s.Identity.Seed(ctx, who[0], who[1])
		if err != nil {
			return fmt.Errorf("@%s/%s: %w", who[0], who[1], err)
		}
		if _, err := s.Provision(ctx, p); err != nil {
			return err
		}
	}
	if b.cluster, err = s.ProvisionTenant(ctx, "cluster"); err != nil {
		return err
	}
	s.ClusterTenant = b.cluster
	s.Deps.ClusterTenant = b.cluster
	if b.tenant, err = s.ProvisionTenant(ctx, "acme"); err != nil {
		return err
	}
	if _, err := s.Deps.Keys.Create(ctx, s.Ungated, true); err != nil {
		return fmt.Errorf("the signing key: %w", err)
	}

	b.set, err = s.Ungated.Set().Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: b.tenant.Bytes()}.Build(),
		Alias:  "lobby",
		Name:   "The lobby",
	}.Build())
	if err != nil {
		return fmt.Errorf("the set: %w", err)
	}
	b.Log.Info("sandbox: seeded", "set", b.set.GetAlias())

	return nil
}

// Run plays the hosts until the context ends: two nodes and a relay the
// operator has adopted, a producer the admin has adopted for the set, and
// a node, a producer and a reader still waiting -- so the console has
// something to adopt, and adopting it starts it.
func (b *Sandbox) Run(ctx context.Context) error {
	ops, err := b.asOps(ctx)
	if err != nil {
		return err
	}
	admin, err := b.asAdmin(ctx)
	if err != nil {
		return err
	}
	// The nodes and the relay come up first, so the producer's segments
	// have somewhere to go and its live has a relay.
	nodes := []*node{
		newNode("davy", []disk{{name: "sdb", capacity: 4 << 40}, {name: "sdc", capacity: 4 << 40}}),
		newNode("hector", []disk{{name: "sdb", capacity: 8 << 40}}),
		newNode("jack", []disk{{name: "sdb", capacity: 4 << 40}}),
	}
	nodes[0].disks[1].failing = true
	nodes[2].pending = true
	var wg sync.WaitGroup
	started := 0
	// A host that fails says so and starts over, as a real one would; it
	// does not take the others down. They start one after another rather
	// than at once, as machines do.
	host := func(name string, run func(context.Context) error) {
		wg.Add(1)
		stagger := time.Duration(started) * b.Every / 4
		started++
		go func() {
			defer wg.Done()
			if !b.sleep(ctx, stagger) {
				return
			}
			for {
				err := run(ctx)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					b.Log.Warn("sandbox: host", "name", name, "err", err.Error())
				}
				if !b.sleep(ctx, 2*b.Every) {
					return
				}
			}
		}()
	}
	for _, n := range nodes {
		n := n
		host(n.name, func(ctx context.Context) error { return b.runNode(ctx, ops, n) })
	}
	host("relay-1", func(ctx context.Context) error { return b.runRelay(ctx, ops, "relay-1") })
	host("pi", func(ctx context.Context) error {
		return b.runProducer(ctx, admin, "pi", []string{"door", "desk", "window"}, true)
	})
	host("pi-2", func(ctx context.Context) error {
		return b.runProducer(ctx, nil, "pi-2", []string{"gate", "yard"}, false)
	})
	host("wall", func(ctx context.Context) error { return b.runReader(ctx, "wall") })
	wg.Wait()

	return nil
}

// DB is taken around every call the hosts and the page make: the
// sandbox's SQLite is one engine on one thread with no busy handler, so a
// write that meets another connection's transaction fails at once rather
// than waiting. Calls taking turns is what a busy handler would have done,
// without a thread to wait on. A stream is not held under it, since a
// stream lasts.
var DB sync.Mutex

// retry makes a call under DB, and again when the database was busy all
// the same, as a host would try again a moment later.
func retry[T any](ctx context.Context, call func() (T, error)) (T, error) {
	var v T
	var err error
	for i := 0; i < 8; i++ {
		DB.Lock()
		v, err = call()
		DB.Unlock()
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

func (b *Sandbox) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func hostJoin(key *pki.HostKey, name, hw string, kind api.HardwareIdKind) (*api.HostJoin, error) {
	csr, err := key.CSR(name)
	if err != nil {
		return nil, err
	}

	return api.HostJoin_builder{
		HardwareId:     hw,
		HardwareIdKind: kind,
		Hostname:       name,
		Csr:            csr,
		KeyFingerprint: key.Fingerprint(),
		DateJoined:     timestamppb.Now(),
		Version:        "sandbox",
	}.Build(), nil
}

func interfaces(addr string) []*api.HostInterface {
	return []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{addr}}.Build()}
}

// ---- nodes ---------------------------------------------------------------

type disk struct {
	name     string
	capacity int64
	failing  bool
	// The counters the report carries, which only go up.
	ioErrors int64
	timeouts int64
}

type node struct {
	name    string
	addr    string
	pending bool
	key     *pki.HostKey
	disks   []*disk
	sinks   []pdid.Id
	free    []int64
	laminae []int64
	id      pdid.Id
}

func newNode(name string, disks []disk) *node {
	n := &node{name: name, addr: "10.44.0." + fmt.Sprint(len(name)+10)}
	for i := range disks {
		d := disks[i]
		n.disks = append(n.disks, &d)
		n.sinks = append(n.sinks, pdid.New(core.DomSink))
		n.free = append(n.free, d.capacity-d.capacity/7)
		n.laminae = append(n.laminae, 0)
	}

	return n
}

func (n *node) deviceReports(tick int) []*api.DeviceReport {
	var vs []*api.DeviceReport
	for i, d := range n.disks {
		r := api.DeviceReport_builder{
			HardwareId:     n.name + "-" + d.name,
			Slot:           fmt.Sprintf("bay-%d", i+1),
			Model:          "SANDBOX HDD",
			Capacity:       d.capacity,
			WriteLatencyMs: 8 + mrand.Float64()*4,
			ReadLatencyMs:  6 + mrand.Float64()*3,
			QueueWrite:     int32(mrand.IntN(3)),
			Smart:          api.SmartSummary_builder{Passed: !d.failing, Temperature: int32(34 + mrand.IntN(5)), DateRead: timestamppb.Now()}.Build(),
		}
		if d.failing && tick > 4 {
			// A disk going bad: errors the node counts, until the CP
			// quarantines it (§27).
			d.ioErrors += 2
			d.timeouts += 3
			r.WriteLatencyMs = 400 + mrand.Float64()*200
		}
		r.IoErrors, r.Timeouts = d.ioErrors, d.timeouts
		vs = append(vs, r.Build())
	}

	return vs
}

func (n *node) sinkReports() []*api.SinkReport {
	var vs []*api.SinkReport
	for i, d := range n.disks {
		vs = append(vs, api.SinkReport_builder{
			SinkId:           n.sinks[i].Bytes(),
			DeviceHardwareId: n.name + "-" + d.name,
			Path:             "/srv/shale/" + d.name,
			Capacity:         d.capacity,
			Free:             n.free[i],
			Pressure:         api.Pressure_PRESSURE_NORMAL,
			Capabilities:     api.SinkCapabilities_builder{Xattr: true}.Build(),
			AcceptWrites:     !d.failing,
			Laminae:          n.laminae[i],
		}.Build())
	}

	return vs
}

func (b *Sandbox) runNode(ctx context.Context, ops context.Context, n *node) error {
	var err error
	if n.key, err = pki.NewHostKey(); err != nil {
		return err
	}
	hj, err := hostJoin(n.key, n.name, "sandbox-"+n.name, api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID)
	if err != nil {
		return err
	}
	join := func() (*api.JoinAnswer, error) {
		r, err := retry(ctx, func() (*api.NodeJoinResponse, error) {
			return b.S.Walled.Node().Join(ctx, api.NodeJoinRequest_builder{
				Host:           hj,
				Interfaces:     interfaces(n.addr),
				Sinks:          n.sinkReports(),
				Devices:        n.deviceReports(0),
				ControlAddress: n.addr + ":7411",
				DataAddress:    n.addr + ":7410",
			}.Build())
		})
		if err != nil {
			return nil, err
		}

		return r.GetAnswer(), nil
	}
	ans, err := join()
	if err != nil {
		return fmt.Errorf("node %s: join: %w", n.name, err)
	}
	if ans.GetState() != api.HostState_HOST_STATE_ADOPTED && !n.pending {
		// The operator adopts it, as the CLI would.
		if _, err := retry(ctx, func() (*api.Node, error) {
			return b.S.Walled.Node().Adopt(ops, api.NodeAdoptRequest_builder{Ref: api.NodeRef_builder{Id: ans.GetId()}.Build(), Alias: n.name}.Build())
		}); err != nil {
			return fmt.Errorf("node %s: adopt: %w", n.name, err)
		}
	}
	// A host asks again until it is answered with a certificate (§33.4).
	for ans.GetState() != api.HostState_HOST_STATE_ADOPTED {
		if !b.sleep(ctx, b.Every) {
			return nil
		}
		if ans, err = join(); err != nil {
			return fmt.Errorf("node %s: join: %w", n.name, err)
		}
	}
	if n.id, err = pdid.From(ans.GetId()); err != nil {
		return err
	}
	me, err := b.asHost(ctx, n.id)
	if err != nil {
		return fmt.Errorf("node %s: %w", n.name, err)
	}
	b.Log.Info("sandbox: node up", "name", n.name, "id", n.id.String())
	commits := make(chan commit, 64)
	b.mu.Lock()
	b.commits[n.id] = commits
	b.mu.Unlock()

	tick := 0
	for {
		tick++
		var uploads int64
		if _, err := retry(ctx, func() (*api.NodeHeartbeatResponse, error) {
			return b.S.Walled.Node().Heartbeat(me, api.NodeHeartbeatRequest_builder{
				Devices:         n.deviceReports(tick),
				Sinks:           n.sinkReports(),
				Interfaces:      interfaces(n.addr),
				ControlAddress:  n.addr + ":7411",
				DataAddress:     n.addr + ":7410",
				Version:         "sandbox",
				UploadsInFlight: uploads,
				IndexLaminae:    sum(n.laminae),
			}.Build())
		}); err != nil {
			b.Log.Warn("sandbox: node heartbeat", "name", n.name, "err", err.Error())
		}
		// What the producers finished on this node's sinks is stored: the
		// event a node pushes when a lamina is committed (§34.9).
		var evs []*api.Event
	drain:
		for {
			select {
			case c := <-commits:
				evs = append(evs, b.storedEvent(n, c))
			default:
				break drain
			}
		}
		if len(evs) > 0 {
			if _, err := retry(ctx, func() (*api.NodePushEventsResponse, error) {
				return b.S.Walled.Node().PushEvents(me, api.NodePushEventsRequest_builder{Events: evs}.Build())
			}); err != nil {
				b.Log.Warn("sandbox: push events", "name", n.name, "err", err.Error())
			}
		}
		if !b.sleep(ctx, b.Every) {
			return nil
		}
	}
}

func sum(vs []int64) int64 {
	var t int64
	for _, v := range vs {
		t += v
	}

	return t
}

func (b *Sandbox) storedEvent(n *node, c commit) *api.Event {
	sink, _ := pdid.From(c.cand.GetSinkId())
	for i, id := range n.sinks {
		if id == sink {
			n.free[i] -= c.size
			n.laminae[i]++
		}
	}
	lam, _ := pdid.From(c.al.GetLaminaId())
	att, _ := pdid.From(c.cand.GetAttemptId())

	return api.Event_builder{Stored: api.LaminaStored_builder{
		LaminaId:      c.al.GetLaminaId(),
		AttemptId:     c.cand.GetAttemptId(),
		SinkId:        c.cand.GetSinkId(),
		LaminaKey:     c.al.GetLaminaKey(),
		Size:          c.size,
		DateStarted:   timestamppb.New(c.started),
		DateEnded:     timestamppb.New(c.ended),
		DateCommitted: timestamppb.Now(),
		SourceId:      c.source,
		Record: api.LaminaRecord_builder{
			FormatVersion:    1,
			TenantId:         c.tenant,
			SetId:            c.set,
			SourceId:         c.source,
			LaminaId:         lam.Bytes(),
			AttemptId:        att.Bytes(),
			DateStartedMs:    c.started.UnixMilli(),
			DateEndedMs:      c.ended.UnixMilli(),
			Size:             c.size,
			Mode:             api.UploadMode_UPLOAD_MODE_LIVE,
			PlacementVersion: 1,
		}.Build(),
	}.Build()}.Build()
}

// ---- the relay -----------------------------------------------------------

func (b *Sandbox) runRelay(ctx context.Context, ops context.Context, name string) error {
	key, err := pki.NewHostKey()
	if err != nil {
		return err
	}
	hj, err := hostJoin(key, name, "sandbox-"+name, api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID)
	if err != nil {
		return err
	}
	addr := "10.44.0.40"
	join := func() (*api.JoinAnswer, error) {
		r, err := retry(ctx, func() (*api.RelayJoinResponse, error) {
			return b.S.Walled.Relay().Join(ctx, api.RelayJoinRequest_builder{
				Host:          hj,
				Interfaces:    interfaces(addr),
				IngestAddress: addr + ":7440",
				WhepAddress:   addr + ":7441",
			}.Build())
		})
		if err != nil {
			return nil, err
		}

		return r.GetAnswer(), nil
	}
	ans, err := join()
	if err != nil {
		return fmt.Errorf("relay: join: %w", err)
	}
	if ans.GetState() != api.HostState_HOST_STATE_ADOPTED {
		if _, err := retry(ctx, func() (*api.Relay, error) {
			return b.S.Walled.Relay().Adopt(ops, api.RelayAdoptRequest_builder{Ref: api.RelayRef_builder{Id: ans.GetId()}.Build(), Alias: name}.Build())
		}); err != nil {
			return fmt.Errorf("relay: adopt: %w", err)
		}
		if ans, err = join(); err != nil {
			return fmt.Errorf("relay: join: %w", err)
		}
	}
	id, err := pdid.From(ans.GetId())
	if err != nil {
		return err
	}
	me, err := b.asHost(ctx, id)
	if err != nil {
		return fmt.Errorf("relay: %w", err)
	}
	b.Log.Info("sandbox: relay up", "name", name, "id", id.String())
	for {
		viewers := int32(mrand.IntN(3))
		if _, err := retry(ctx, func() (*api.RelayHeartbeatResponse, error) {
			return b.S.Walled.Relay().Heartbeat(me, api.RelayHeartbeatRequest_builder{
				Interfaces:    interfaces(addr),
				IngestAddress: addr + ":7440",
				WhepAddress:   addr + ":7441",
				Version:       "sandbox",
				Status: api.RelayStatus_builder{
					DateReported:      timestamppb.Now(),
					AttachedProducers: 1,
					ActiveSources:     viewers,
					Viewers:           viewers,
					EgressBps:         int64(viewers) * 4_000_000,
					AttachedBitrate:   12_000_000,
					Load:              api.HostLoad_builder{Cpu: 0.05 + mrand.Float64()*0.05}.Build(),
				}.Build(),
			}.Build())
		}); err != nil {
			b.Log.Warn("sandbox: relay heartbeat", "err", err.Error())
		}
		if !b.sleep(ctx, b.Every) {
			return nil
		}
	}
}

// ---- producers -----------------------------------------------------------

type camera struct {
	alias   string
	row     *api.Source
	ceiling int64
	// dark says the scene is dark and the segments are skipped (§38.10).
	dark bool
	// since is when the segment being recorded began; last is the lamina
	// of the segment before, which the next one never reuses (§15).
	since time.Time
	last  []byte
}

func (b *Sandbox) runProducer(ctx context.Context, admin context.Context, name string, cameras []string, adopt bool) error {
	key, err := pki.NewHostKey()
	if err != nil {
		return err
	}
	hj, err := hostJoin(key, name, "100000000000"+fmt.Sprintf("%04x", len(name)*4919), api.HardwareIdKind_HARDWARE_ID_KIND_DEVICE_TREE)
	if err != nil {
		return err
	}
	join := func() (*api.JoinAnswer, error) {
		r, err := retry(ctx, func() (*api.ProducerJoinResponse, error) {
			return b.S.Walled.Producer().Join(ctx, api.ProducerJoinRequest_builder{Host: hj, Tenant: "acme"}.Build())
		})
		if err != nil {
			return nil, err
		}

		return r.GetAnswer(), nil
	}
	ans, err := join()
	if err != nil {
		return fmt.Errorf("producer %s: join: %w", name, err)
	}
	if adopt && ans.GetState() != api.HostState_HOST_STATE_ADOPTED {
		if _, err := retry(ctx, func() (*api.Producer, error) {
			return b.S.Walled.Producer().Adopt(admin, api.ProducerAdoptRequest_builder{
				Ref: api.ProducerRef_builder{Id: ans.GetId()}.Build(),
				Set: api.SetRef_builder{Id: b.set.GetId()}.Build(),
			}.Build())
		}); err != nil {
			return fmt.Errorf("producer %s: adopt: %w", name, err)
		}
	}
	for ans.GetState() != api.HostState_HOST_STATE_ADOPTED {
		if !b.sleep(ctx, b.Every) {
			return nil
		}
		if ans, err = join(); err != nil {
			return fmt.Errorf("producer %s: join: %w", name, err)
		}
	}
	id, err := pdid.From(ans.GetId())
	if err != nil {
		return err
	}
	me, err := b.asHost(ctx, id)
	if err != nil {
		return fmt.Errorf("producer %s: %w", name, err)
	}

	// The set it was adopted for (§33.4), then its sources (§38.4).
	var set *api.Set
	for set == nil {
		row, err := retry(ctx, func() (*api.Producer, error) {
			return b.S.Walled.Producer().Get(me, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: id.Bytes()}.Build()}.Build())
		})
		if err == nil && len(row.GetSet().GetId()) > 0 {
			set, _ = retry(ctx, func() (*api.Set, error) {
				return b.S.Walled.Set().Get(me, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: row.GetSet().GetId()}.Build()}.Build())
			})
		}
		if set == nil && !b.sleep(ctx, b.Every) {
			return nil
		}
	}
	b.Log.Info("sandbox: producer up", "name", name, "set", set.GetAlias())
	// The nodes' first heartbeats attach their sinks; a segment allocated
	// before that has nowhere to go.
	if !b.sleep(ctx, 2*b.Every) {
		return nil
	}
	var cams []*camera
	existing, err := retry(ctx, func() (*api.SourceListResponse, error) {
		return b.S.Walled.Source().List(me, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}, Size: 100,
		}.Build())
	})
	if err != nil {
		return fmt.Errorf("producer %s: sources: %w", name, err)
	}
	for _, alias := range cameras {
		c := &camera{alias: alias, ceiling: 4_000_000}
		for _, v := range existing.GetItems() {
			if v.GetAlias() == alias {
				c.row = v
			}
		}
		if c.row == nil {
			v, err := retry(ctx, func() (*api.Source, error) {
				return b.S.Walled.Source().Add(me, api.SourceAddRequest_builder{
					Tenant:  api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(),
					Alias:   alias,
					Set:     api.SetRef_builder{Id: set.GetId()}.Build(),
					Profile: api.SegmentProfile_builder{MaxBitrate: c.ceiling}.Build(),
				}.Build())
			})
			if err != nil {
				return fmt.Errorf("producer %s: register %s: %w", name, alias, err)
			}
			c.row = v
		}
		cams = append(cams, c)
	}
	var proposals []*api.SourceProposal
	for _, c := range cams {
		proposals = append(proposals, api.SourceProposal_builder{
			Source:      api.SourceRef_builder{Id: c.row.GetId()}.Build(),
			Profile:     api.SegmentProfile_builder{MaxBitrate: c.ceiling, KeyframeIntervalMs: 2000}.Build(),
			ContentType: "video/mp2t",
		}.Build())
	}
	neg, err := retry(ctx, func() (*api.SetNegotiateResponse, error) {
		return b.S.Walled.Set().Negotiate(me, api.SetNegotiateRequest_builder{
			Ref:     api.SetRef_builder{Id: set.GetId()}.Build(),
			Link:    api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_LIVE}.Build(),
			Sources: proposals,
		}.Build())
	})
	if err != nil {
		return fmt.Errorf("producer %s: negotiate: %w", name, err)
	}
	for _, ag := range neg.GetSources() {
		for _, c := range cams {
			if string(c.row.GetId()) == string(ag.GetSourceId()) && ag.GetProfile().GetMaxBitrate() > 0 {
				c.ceiling = ag.GetProfile().GetMaxBitrate()
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.producerHeartbeats(ctx, me, name, cams) })
	for i, c := range cams {
		c := c
		// The last camera of the first producer looks at a room whose
		// lights go off now and then (§38.10); the first one loses a
		// segment once in a long while, as a producer that gave up reports
		// it (§13) -- rarely, since a node the producers report against is
		// scored for it (§27).
		nightly := adopt && i == len(cams)-1
		lossy := adopt && i == 0
		g.Go(func() error { return b.recordCamera(ctx, me, name, set, c, nightly, lossy) })
	}

	return g.Wait()
}

func (b *Sandbox) producerHeartbeats(ctx context.Context, me context.Context, name string, cams []*camera) error {
	for {
		var reports []*api.SourceReport
		for _, c := range cams {
			rate := int64(float64(c.ceiling) * (0.55 + mrand.Float64()*0.4))
			if c.dark {
				rate = int64(float64(c.ceiling) * 0.3)
			}
			reports = append(reports, api.SourceReport_builder{
				SourceId:           c.row.GetId(),
				InputUp:            true,
				FrameRate:          29.6 + mrand.Float64()*0.8,
				MeasuredBitrate:    rate,
				MaxBitrate:         c.ceiling,
				KeyframeIntervalMs: 2000,
				SecondsTotal:       int64(b.Every.Seconds()),
				Dark:               c.dark,
			}.Build())
		}
		if _, err := retry(ctx, func() (*api.ProducerHeartbeatResponse, error) {
			return b.S.Walled.Producer().Heartbeat(me, api.ProducerHeartbeatRequest_builder{
				Sources: reports,
				Load:    api.HostLoad_builder{Cpu: 0.6 + mrand.Float64()*0.2, Temperature: 58 + mrand.Float64()*6, UplinkBps: 9_000_000 + int64(mrand.IntN(2_000_000))}.Build(),
				Version: "sandbox",
			}.Build())
		}); err != nil {
			b.Log.Warn("sandbox: producer heartbeat", "name", name, "err", err.Error())
		}
		if !b.sleep(ctx, b.Every) {
			return nil
		}
	}
}

// recordCamera is one camera's segments: allocated when they open, stored
// by the node of the candidate when they close, skipped while the scene is
// dark, and once in a long while lost, as a producer reports it when it
// gives up (§13).
func (b *Sandbox) recordCamera(ctx context.Context, me context.Context, name string, set *api.Set, c *camera, nightly, lossy bool) error {
	n := 0
	for {
		n++
		if nightly {
			// Six segments lit, three dark, and again.
			c.dark = n%9 >= 6
		}
		c.since = time.Now()
		req := api.LaminaAllocateRequest_builder{
			Source:      api.SourceRef_builder{Id: c.row.GetId()}.Build(),
			DateStarted: timestamppb.New(c.since),
		}
		if len(c.last) > 0 {
			req.After = api.LaminaRef_builder{Id: c.last}.Build()
		}
		al, err := retry(ctx, func() (*api.Allocation, error) {
			return b.S.Walled.Lamina().Allocate(me, req.Build())
		})
		if err != nil {
			b.Log.Warn("sandbox: allocate", "producer", name, "camera", c.alias, "err", err.Error())
			if !b.sleep(ctx, b.Every) {
				return nil
			}
			continue
		}
		c.last = al.GetLaminaId()
		if !b.sleep(ctx, b.Segment) {
			return nil
		}
		ended := time.Now()
		switch {
		case c.dark:
			if _, err := retry(ctx, func() (*api.Lamina, error) {
				return b.S.Walled.Lamina().Skip(me, api.LaminaSkipRequest_builder{
					Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build(), DateEnded: timestamppb.New(ended), Reason: api.LaminaSkipReason_LAMINA_SKIP_REASON_DARK,
				}.Build())
			}); err != nil {
				b.Log.Warn("sandbox: skip", "camera", c.alias, "err", err.Error())
			}
		case lossy && n%41 == 17:
			if _, err := retry(ctx, func() (*api.Lamina, error) {
				return b.S.Walled.Lamina().ReportFailure(me, api.LaminaReportFailureRequest_builder{
					Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build(), Reason: "sandbox: every candidate refused the upload",
				}.Build())
			}); err != nil {
				b.Log.Warn("sandbox: report failure", "camera", c.alias, "err", err.Error())
			}
		case len(al.GetCandidates()) == 0:
			b.Log.Warn("sandbox: no candidate", "camera", c.alias)
		default:
			cand := al.GetCandidates()[0]
			nodeId, _ := pdid.From(cand.GetNodeId())
			b.mu.Lock()
			ch := b.commits[nodeId]
			b.mu.Unlock()
			if ch == nil {
				b.Log.Warn("sandbox: the candidate's node is not here", "camera", c.alias, "node", nodeId.String())
				continue
			}
			size := int64(float64(c.ceiling) * (0.6 + mrand.Float64()*0.35) * ended.Sub(c.since).Seconds() / 8)
			select {
			case ch <- commit{al: al, cand: cand, started: c.since, ended: ended, size: size, source: c.row.GetId(), set: set.GetId(), tenant: set.GetTenant().GetId()}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// ---- readers -------------------------------------------------------------

func (b *Sandbox) runReader(ctx context.Context, name string) error {
	key, err := pki.NewHostKey()
	if err != nil {
		return err
	}
	hj, err := hostJoin(key, name, "sandbox-"+name, api.HardwareIdKind_HARDWARE_ID_KIND_DMI)
	if err != nil {
		return err
	}
	for {
		r, err := retry(ctx, func() (*api.ReaderJoinResponse, error) {
			return b.S.Walled.Reader().Join(ctx, api.ReaderJoinRequest_builder{Host: hj, Tenant: "acme"}.Build())
		})
		if err != nil {
			return fmt.Errorf("reader %s: join: %w", name, err)
		}
		if r.GetAnswer().GetState() == api.HostState_HOST_STATE_ADOPTED {
			id, err := pdid.From(r.GetAnswer().GetId())
			if err != nil {
				return err
			}
			me, err := b.asHost(ctx, id)
			if err != nil {
				return err
			}
			b.Log.Info("sandbox: reader up", "name", name)
			for {
				if _, err := retry(ctx, func() (*api.ReaderHeartbeatResponse, error) {
					return b.S.Walled.Reader().Heartbeat(me, api.ReaderHeartbeatRequest_builder{Version: "sandbox"}.Build())
				}); err != nil {
					b.Log.Warn("sandbox: reader heartbeat", "err", err.Error())
				}
				if !b.sleep(ctx, b.Every) {
					return nil
				}
			}
		}
		if !b.sleep(ctx, b.Every) {
			return nil
		}
	}
}
