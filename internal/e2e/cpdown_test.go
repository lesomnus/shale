package e2e_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// Ephemeral listeners must be allocated by ServeAll itself, then the actual
// API addresses retained for peers that outlive a control-plane restart.
func TestControlPlaneRestartKeepsAPIAddresses(t *testing.T) {
	c := start(t)
	tenantAddr, clusterAddr := c.running.TenantAddr, c.running.ClusterAddr
	require.NotEqual(t, tenantAddr, clusterAddr)
	admin := api.NewSetServiceClient(c.dial("@acme/admin"))
	ops := api.NewSinkServiceClient(c.dialCluster(c.operator()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := admin.List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	_, err = ops.List(ctx, api.SinkListRequest_builder{}.Build())
	require.NoError(t, err)

	c.stopCP()
	c.startCP()
	require.Equal(t, tenantAddr, c.cfg.Server.Addr)
	require.Equal(t, clusterAddr, c.cfg.Cluster.Addr)
	require.Equal(t, tenantAddr, c.running.TenantAddr)
	require.Equal(t, clusterAddr, c.running.ClusterAddr)

	// Reuse the original connections: replacing the clients with connections to
	// the new addresses would hide the failure that the outage drills exercise.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, tenantErr := admin.List(ctx, api.SetListRequest_builder{}.Build())
		_, clusterErr := ops.List(ctx, api.SinkListRequest_builder{}.Build())
		return tenantErr == nil && clusterErr == nil
	}, 10*time.Second, 100*time.Millisecond, "both original API clients reconnect")
}

// TestControlPlaneOutage is §12.1's second table, what the allocation
// horizon buys: with the control plane gone, a producer writes on the
// allocations it already holds and the node commits them, queueing the
// events in its own RAM; GC has nobody to ask, so a sink that fills drifts
// to critical and refuses new uploads itself; and reads stop at once,
// because a reader has nowhere to ask where the bytes are. When the
// control plane is back the queued commits reach the index with nothing
// uploaded twice.
func TestControlPlaneOutage(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		// The built-in node goes down with the control plane, so the drill
		// writes to a node of its own; this one only has to exist.
		c.Storage.Sinks[0].Capacity = "64MiB"
	})
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)

	// A node outside the control plane's process, with room for a few
	// segments and no more.
	sinkB := filepath.Join(t.TempDir(), "sink-b")
	nodeB, _ := c.startNodeAt("b", filepath.Join(t.TempDir(), "node-b"), sinkB, func(cfg *storage.Config) {
		cfg.Sinks[0].Capacity = 2 << 20
	})
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "node B's sink is attached")

	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "outage", nil)

	// The horizon in hand: allocations made while the control plane is up,
	// each with a candidate on node B.
	type slot struct {
		al   *api.Allocation
		cand *api.Candidate
	}
	var held []slot
	for i := 1; i <= 8; i++ {
		al := allocate(time.Duration(i) * time.Hour)
		for _, cd := range al.GetCandidates() {
			if string(cd.GetNodeId()) == string(nodeB.Id().Bytes()) {
				held = append(held, slot{al, cd})
				break
			}
		}
	}
	require.Len(t, held, 8, "every allocation offers node B")

	// ---- the control plane goes.
	c.stopCP()

	// Reads stop at once: there is nowhere to ask where the bytes are.
	_, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.Error(t, err, "a read cannot be served without the control plane")

	// Writes continue on the allocations in hand, and the node commits
	// them: the answer is 201 with nobody to tell.
	body := make([]byte, 256<<10)
	rand.Read(body)
	code, _ := putAt(t, held[0].al, held[0].cand, body, "?1")
	require.Equal(t, http.StatusCreated, code, "the node takes the write while the control plane is away")
	path := filepath.Join(sinkB, held[0].cand.GetLaminaKey())
	require.FileExists(t, path, "and the bytes are on the sink")

	// GC has nobody to ask for approvals, so the sink only fills: at
	// critical the node refuses new uploads on it, by itself.
	big := make([]byte, 512<<10)
	rand.Read(big)
	refused := false
	for _, s := range held[1:] {
		code, hdr := putAt(t, s.al, s.cand, big, "?1")
		if code == http.StatusServiceUnavailable {
			require.Equal(t, "30", hdr.Get(storage.HdrRetryAfter))
			refused = true
			break
		}
		require.Equal(t, http.StatusCreated, code)
	}
	require.True(t, refused, "the sink filled and the node refused the next upload on its own")

	// ---- the control plane is back, on the same addresses.
	c.startCP()

	// What the node committed in the dark reaches the index, from the
	// events it kept: nothing was uploaded twice and nothing is lost.
	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: held[0].al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 60*time.Second, 500*time.Millisecond, "the commits queued in the node reach the index")
	o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: held[0].al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), o.GetSize())
	require.Equal(t, held[0].cand.GetSinkId(), o.GetSink().GetId())

	// And reads work again, on the lamina written while there was nobody
	// to ask.
	var url string
	require.Eventually(t, func() bool {
		ts := timelineAround(t, ctx, laminae, set, held[0].al.GetDateStarted().AsTime())
		for _, l := range ts.GetLaminae() {
			if string(l.GetLaminaId()) == string(held[0].al.GetLaminaId()) && l.GetState() == api.ReadState_READ_STATE_AVAILABLE {
				url = l.GetUrl()

				return true
			}
		}

		return false
	}, 60*time.Second, 500*time.Millisecond, "the lamina is offered for reading again")
	require.Equal(t, body, fetch(t, url))
	require.Contains(t, url, fmt.Sprintf(":%d/", portOf(t, nodeB.DataAddr)), "served by the node that took it")
}

// TestControlPlaneDownWhileWatching is §39.6's last row: with the control
// plane gone there is no new `Live` and no new publish token, and what is
// already open carries on. The relay and the producer here run outside the
// control plane's process, as they do on a real deployment.
func TestControlPlaneDownWhileWatching(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))

	// A relay of its own, labelled, and a site that selects it: the one in
	// `serve all` would go down with the control plane.
	r1, _ := c.startRelay("r1")
	require.Eventually(t, func() bool {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			if string(r.GetId()) != string(r1.Id().Bytes()) {
				continue
			}
			if r.GetState() != api.HostState_HOST_STATE_ADOPTED || r.GetDateSeen() == nil || r.GetWhepAddress() == "" {
				return false
			}
			if r.GetLabels()["zone"] == "d" {
				return true
			}
			_, err := relays.Patch(ctx, api.RelayPatchRequest_builder{
				Ref: api.RelayRef_builder{Id: r.GetId()}.Build(), Labels: map[string]string{"zone": "d"}, DateUpdated: r.GetDateUpdated(),
			}.Build())

			return err == nil && false
		}

		return false
	}, 30*time.Second, 300*time.Millisecond, "the relay is adopted, labelled and has a WHEP address")

	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zone-d", RelaySelector: map[string]string{"zone": "d"},
	}.Build())
	require.NoError(t, err)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "watched", Site: api.SiteRef_builder{Id: site.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		SegmentDuration:   8 * time.Second,
		HeartbeatInterval: 2 * time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20,
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	sets := api.NewSetServiceClient(admin)
	var live *api.LiveSource
	require.Eventually(t, func() bool {
		resp, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		if err != nil || len(resp.GetSources()) == 0 {
			return false
		}
		live = resp.GetSources()[0]

		return string(live.GetRelayId()) == string(r1.Id().Bytes())
	}, 60*time.Second, 300*time.Millisecond, "Live answers with the site's relay")

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 30*time.Second)

	// ---- the control plane goes, with nobody watching it.
	c.stopCP()

	// The session is between the viewer and the relay, and the stream
	// between the relay and the producer: neither asks the control plane
	// anything, so the picture goes on.
	v.video(t, 200, 30*time.Second)

	// But nothing new opens: a viewer arriving now has nowhere to ask.
	_, err = sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.Error(t, err, "no new Live while the control plane is away")

	c.startCP()
	require.Eventually(t, func() bool {
		resp, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())

		return err == nil && len(resp.GetSources()) == 1
	}, 60*time.Second, 500*time.Millisecond, "and a new viewer is served again once it is back")
}
