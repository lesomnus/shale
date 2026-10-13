package e2e_test

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
)

// cut is a TCP proxy that can be cut: while cut, what it carried is
// closed and what arrives is refused, so a host behind it goes unheard
// while it keeps serving everyone who reaches it directly.
type cut struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	off   bool
	conns map[net.Conn]bool
}

func newCut(t *testing.T, target string) *cut {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	c := &cut{ln: ln, target: target, conns: map[net.Conn]bool{}}
	t.Cleanup(func() { ln.Close(); c.set(true) })
	go c.serve()

	return c
}

func (c *cut) addr() string { return c.ln.Addr().String() }

func (c *cut) serve() {
	for {
		in, err := c.ln.Accept()
		if err != nil {
			return
		}
		c.mu.Lock()
		off := c.off
		c.mu.Unlock()
		if off {
			in.Close()
			continue
		}
		out, err := net.Dial("tcp", c.target)
		if err != nil {
			in.Close()
			continue
		}
		c.mu.Lock()
		c.conns[in], c.conns[out] = true, true
		c.mu.Unlock()
		go func() { io.Copy(out, in); out.Close() }()
		go func() { io.Copy(in, out); in.Close() }()
	}
}

// set cuts the proxy, or mends it.
func (c *cut) set(off bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.off = off
	if off {
		for v := range c.conns {
			v.Close()
		}
		c.conns = map[net.Conn]bool{}
	}
}

// TestRelayFalseDown is §39.6's relay that is held down while it is not:
// it serves on, but the CP stops hearing it. Past `relay_down_after` its
// producer is moved, at the producer's next heartbeat, to the other relay,
// and Live names that one. When the CP hears the relay again it is up
// from that heartbeat on with nothing to repair: the producer stays where
// it was moved, since the assignment is sticky, and the relay takes the
// producer back, and a viewer has a picture from it, when the other relay
// goes in turn.
func TestRelayFalseDown(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)

	// r1 reaches the CP through the proxy, and is the zone's only relay
	// when the producer is assigned.
	proxy := newCut(t, c.running.ClusterAddr)
	r1, _ := c.startRelayVia("r1", filepath.Join(t.TempDir(), "r1"), proxy.addr())
	label := func(id []byte) {
		require.Eventually(t, func() bool {
			r, err := relays.Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: id}.Build()}.Build())
			if err != nil || r.GetState() != api.HostState_HOST_STATE_ADOPTED || r.GetDateSeen() == nil {
				return false
			}
			if r.GetLabels()["zone"] == "b" {
				return true
			}
			_, err = relays.Patch(ctx, api.RelayPatchRequest_builder{
				Ref: api.RelayRef_builder{Id: id}.Build(), Labels: map[string]string{"zone": "b"}, DateUpdated: r.GetDateUpdated(),
			}.Build())

			return err == nil
		}, 30*time.Second, 300*time.Millisecond, "relay adopted and labeled")
	}
	label(r1.Id().Bytes())

	admin := c.dial("@acme/admin")
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zone-b", RelaySelector: map[string]string{"zone": "b"},
	}.Build())
	require.NoError(t, err)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zoned", Site: api.SiteRef_builder{Id: site.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	// A producer heartbeat of 2 s, so that it hears of the relay being
	// down within the drill: its link to the relay never breaks, so the
	// heartbeat is the only way it learns.
	sample := samplePath(t)
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 2 * time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
	})
	require.NoError(t, err)
	go p.Run(pctx)
	producers := api.NewProducerServiceClient(admin)
	var pending *api.Producer
	require.Eventually(t, func() bool {
		vs, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 10}.Build())
		if err != nil {
			return false
		}
		for _, v := range vs.GetItems() {
			if v.GetState() == api.HostState_HOST_STATE_PENDING {
				pending = v
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond)
	_, err = producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	assigned := func() []byte {
		v, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
		if err != nil {
			return nil
		}

		return v.GetRelay().GetId()
	}
	require.Eventually(t, func() bool { return string(assigned()) == string(r1.Id().Bytes()) }, 30*time.Second, 300*time.Millisecond, "assigned to r1")

	sets := api.NewSetServiceClient(admin)
	index := indexFrames(t, sample)
	picture := func(relayId []byte) wallTile {
		t.Helper()
		var tile wallTile
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			live, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
			if err == nil && string(live.GetSources()[0].GetRelayId()) == string(relayId) {
				if tile = watchTile(t, live.GetSources()[0], index, time.Second); tile.err == nil {
					return tile
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		require.NoError(t, tile.err, "a picture from the relay Live names")

		return tile
	}
	picture(r1.Id().Bytes())

	// The other relay of the zone, which nothing is assigned to yet.
	r2, stop2 := c.startRelay("r2")
	label(r2.Id().Bytes())

	// The CP stops hearing r1, which serves on.
	proxy.set(true)
	cutAt := time.Now()
	require.Eventually(t, func() bool { return string(assigned()) == string(r2.Id().Bytes()) },
		30*time.Second, 200*time.Millisecond, "the producer moved off the relay the CP no longer hears")
	moved := time.Since(cutAt)
	// Heard last at most one heartbeat before the cut.
	require.GreaterOrEqual(t, moved, 4*time.Second, "not before relay_down_after")
	picture(r2.Id().Bytes())

	// Heard again: up from its next heartbeat on.
	proxy.set(false)
	mended := time.Now()
	require.Eventually(t, func() bool {
		r, err := relays.Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: r1.Id().Bytes()}.Build()}.Build())
		return err == nil && r.GetDateSeen().AsTime().After(mended)
	}, 20*time.Second, 200*time.Millisecond, "the CP hears r1 again")
	heard := time.Since(mended)

	// The producer stays where it was moved: three of its heartbeats.
	for range 3 {
		time.Sleep(2 * time.Second)
		require.Equal(t, r2.Id().Bytes(), assigned(), "the assignment is sticky; a relay that is back takes nobody back")
	}

	// r1 serves again as any relay does: when r2 goes, the producer is
	// moved back to it and a viewer has a picture from it.
	stop2()
	stopped := time.Now()
	require.Eventually(t, func() bool { return string(assigned()) == string(r1.Id().Bytes()) },
		30*time.Second, 200*time.Millisecond, "the producer moved back to r1 once r2 went")
	tile := picture(r1.Id().Bytes())
	t.Logf("relay held down while up: producer moved %d ms after the cut; r1 heard again %d ms after the mend; a picture from r1 %d ms after r2 went",
		moved.Milliseconds(), heard.Milliseconds(), tile.first.Sub(stopped).Milliseconds())
}
