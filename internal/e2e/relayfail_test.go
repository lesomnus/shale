package e2e_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/relay"
)

// startRelay runs one more relay in this process, with its own identity.
func (c *cluster) startRelay(name string) (*relay.Relay, context.CancelFunc) {
	return c.startRelayAt(name, filepath.Join(c.t.TempDir(), name))
}

// startRelayAt is startRelay with the state directory given, so a relay
// can be started again as itself.
func (c *cluster) startRelayAt(name, stateDir string) (*relay.Relay, context.CancelFunc) {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r, err := relay.New(relay.Config{
		StateDir:          stateDir,
		Cp:                "http://" + c.running.ClusterAddr,
		Dev:               true,
		HardwareId:        "test-" + name,
		IngestAddr:        "127.0.0.1:0",
		WhepAddr:          "127.0.0.1:0",
		IdleStop:          time.Second,
		HeartbeatInterval: time.Second,
		Log:               slog.Default().With("relay", name),
	})
	require.NoError(c.t, err)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-r.Ready:
	case err := <-done:
		cancel()
		c.t.Fatalf("relay %s: %v", name, err)
	case <-time.After(30 * time.Second):
		cancel()
		c.t.Fatalf("relay %s did not come up", name)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	c.t.Cleanup(stop)

	return r, stop
}

// TestRelayFailover is §39.2's assignment and §39.6's relay-down drill: a
// site's selector picks the relays with a label, the producer is assigned
// the least loaded of them, and when it goes the producer is reassigned
// on its next heartbeat and Live answers the other relay.
func TestRelayFailover(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)

	r1, stop1 := c.startRelay("r1")
	r2, stop2 := c.startRelay("r2")
	defer stop2()
	labeled := 0
	require.Eventually(t, func() bool {
		labeled = 0
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			if string(r.GetId()) != string(r1.Id().Bytes()) && string(r.GetId()) != string(r2.Id().Bytes()) {
				continue
			}
			if r.GetState() != api.HostState_HOST_STATE_ADOPTED || r.GetDateSeen() == nil {
				return false
			}
			if r.GetLabels()["zone"] != "b" {
				if _, err := relays.Patch(ctx, api.RelayPatchRequest_builder{
					Ref: api.RelayRef_builder{Id: r.GetId()}.Build(), Labels: map[string]string{"zone": "b"}, DateUpdated: r.GetDateUpdated(),
				}.Build()); err != nil {
					return false
				}
			}
			labeled++
		}

		return labeled == 2
	}, 30*time.Second, 300*time.Millisecond, "both relays adopted and labeled")

	// A site whose selector names the label, and a set in it.
	admin := c.dial("@acme/admin")
	site, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zone-b", RelaySelector: map[string]string{"zone": "b"},
	}.Build())
	require.NoError(t, err)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "zoned", Site: api.SiteRef_builder{Id: site.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

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

	// Assigned to one of the labeled relays, never the unlabeled one.
	assigned := func() []byte {
		v, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build()}.Build())
		if err != nil {
			return nil
		}

		return v.GetRelay().GetId()
	}
	var first []byte
	require.Eventually(t, func() bool { first = assigned(); return len(first) > 0 }, 30*time.Second, 300*time.Millisecond, "assigned")
	require.Contains(t, [][]byte{r1.Id().Bytes(), r2.Id().Bytes()}, first, "a relay of the site's zone")

	// That relay goes; heartbeats stop; the producer is reassigned to the
	// other one, and Live names it.
	var other []byte
	if string(first) == string(r1.Id().Bytes()) {
		stop1()
		other = r2.Id().Bytes()
	} else {
		stop2()
		other = r1.Id().Bytes()
	}
	require.Eventually(t, func() bool { return string(assigned()) == string(other) }, 90*time.Second, 500*time.Millisecond, "reassigned once the relay is down")
	live, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Len(t, live.GetSources(), 1)
	require.Equal(t, other, live.GetSources()[0].GetRelayId())
}
