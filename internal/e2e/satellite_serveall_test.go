package e2e_test

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
)

// TestSatelliteServeAll is §41.10: `serve all` runs a satellite beside its
// node and relay, joined to the cluster listener of its process and adopted
// as they are, so one machine has every host a producer may use. The node
// and the relay listen on every interface here, as `serve all` does outside
// development mode, so where the satellite sends what passes through it is
// the address resolver's to say: loopback, for a caller on the CP's host
// (§34.10). Two producers ask for it. One, under `retain: accepted`, is
// watched live: the satellite feeds the relay from the upload while the
// producer's tee sends nothing, and takes the segments and delivers them.
// The other, under `retain: committed`, has its laminae forwarded.
func TestSatelliteServeAll(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Storage.Addr = ":0"
		c.Relay.IngestAddr = ":0"
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)

	sat := c.running.Satellite
	require.NotNil(t, sat, "serve all runs a satellite")
	select {
	case <-sat.Ready:
	case <-time.After(30 * time.Second):
		t.Fatal("the satellite of serve all did not come up")
	}
	host, _, err := net.SplitHostPort(sat.Addr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host, "development mode: a port of loopback the system picked")
	sats := api.NewSatelliteServiceClient(c.dialCluster(c.operator()))
	satRow := func() *api.Satellite {
		return untilOK(t, func() (*api.Satellite, error) {
			return sats.Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
		})
	}
	require.Eventually(t, func() bool {
		v := satRow()
		return v.GetState() == api.HostState_HOST_STATE_ADOPTED && v.GetStatus().GetInFlightBudget() > 0 && v.GetStatus().GetAccept()
	}, 30*time.Second, 200*time.Millisecond, "adopted, as the node and the relay of the process, and heard from")
	require.Equal(t, sat.Addr, satRow().GetUploadAddress())
	used, peak, _ := sat.InFlight()
	require.Zero(t, used+peak, "nothing held before a request")

	// `retain: accepted`, live, watched.
	p, set := c.satelliteLive(ctx, "door", "av.mp4", producer.ViaSatellite, func(cfg *producer.Config) {
		cfg.Retain = producer.RetainAccepted
		cfg.HardwareId = "test-producer-door"
	})
	fedBy(t, sat, p, "door")
	v := watch(t, c.liveOf(ctx, set))
	defer v.close()
	v.video(t, 100, 20*time.Second)
	// `retain: committed`.
	q, qset, _ := c.recordRetaining(ctx, "yard", producer.ViaSatellite, producer.RetainCommitted)

	require.Eventually(t, func() bool {
		held, committed := c.heldBy(ctx, set, sat, api.LaminaState_LAMINA_STATE_COMMITTED)
		return p.Stats().HandedOff >= 3 && len(held) >= 3 && len(committed) >= 3
	}, 120*time.Second, 500*time.Millisecond, "handed off to the satellite of serve all, which delivered them: %+v", p.Stats())
	require.Eventually(t, func() bool { n, _ := c.committedBytes(ctx, qset); return q.Stats().Stored >= 3 && n >= 3 }, 120*time.Second, 500*time.Millisecond,
		"recording through the satellite of serve all: %+v", q.Stats())
	_, size := c.committedBytes(ctx, qset)
	require.GreaterOrEqual(t, sat.Forwarded(), size, "its laminae came through the satellite")
	_, sent, _ := sat.Live()
	require.Positive(t, sent, "the satellite fed the relay of the process")
	require.Zero(t, p.LiveSent("door"), "the producer's own tee sent nothing")
	v.video(t, 50, 10*time.Second)
	for _, s := range []producer.Stats{p.Stats(), q.Stats()} {
		require.Zero(t, s.Lost, "%+v", s)
		require.Zero(t, s.Cut, "%+v", s)
	}
	require.Eventually(t, func() bool { return satRow().GetStatus().GetProducers() == 2 }, 10*time.Second, 200*time.Millisecond)

	// Both of the satellite's hops are loopback, though the node and the
	// relay say they listen on every interface.
	ops := c.dialCluster(c.operator())
	node := untilOK(t, func() (*api.Node, error) {
		return api.NewNodeServiceClient(ops).Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: c.running.Node.Id().Bytes()}.Build()}.Build())
	})
	relays := untilOK(t, func() (*api.RelayListResponse, error) {
		return api.NewRelayServiceClient(ops).List(ctx, api.RelayListRequest_builder{}.Build())
	})
	require.Len(t, relays.GetItems(), 1)
	unspecified := func(addr string) string {
		t.Helper()
		h, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		require.True(t, net.ParseIP(h).IsUnspecified(), "%s listens on every interface", addr)
		return port
	}
	dataPort := unspecified(node.GetDataAddress())
	ingestPort := unspecified(relays.GetItems()[0].GetIngestAddress())

	routes := sat.Routes()
	require.Len(t, routes, 1)
	base, ok := routes[c.running.Node.Id()]
	require.True(t, ok, "the route is the node's of the process: %v", routes)
	u, err := url.Parse(base)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", u.Hostname(), "the node over loopback")
	require.Equal(t, dataPort, u.Port())
	fed := sat.Relays()
	require.NotEmpty(t, fed)
	for id, addr := range fed {
		h, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", h, "producer %s's relay over loopback", id)
		require.Equal(t, ingestPort, port)
	}
}
