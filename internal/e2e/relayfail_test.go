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
	"github.com/lesomnus/shale/cmd"
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
// on its next heartbeat, Live answers the other relay, and a viewer who
// was watching has a picture from it again within E6's 10 s.
//
// A relay is down when it has not been heard for node_down_after, as a
// node is (§39.2), and until then Live keeps naming it: the time to a
// picture again is node_down_after and about a second more. With the
// default of 30 s that is 31.5 s as measured, over E6's bound, so the
// drill runs with 5 s, against relays that heartbeat every second; at
// the relay's default heartbeat of 5 s the bound cannot be met without
// a node_down_after that one late heartbeat would trip.
func TestRelayFailover(t *testing.T) {
	c := start(t, func(cfg *cmd.Config) { cfg.Control.NodeDownAfter = 5 * time.Second })
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)

	// Segments of a few MB, so the recording is seen to go on across the
	// relay outage rather than sit in one long segment.
	policies := api.NewUploadPolicyServiceClient(ops)
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "short-laminae", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 1 << 20, TargetLamina: 4 << 20, MaxLamina: 16 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

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
	// §39.6's recovery for a relay that goes: live stops for a few seconds
	// and the recording is untouched, so the count of stored laminae keeps
	// climbing across the outage.
	require.Eventually(t, func() bool { return p.Stats().Stored >= 1 }, 90*time.Second, 500*time.Millisecond, "recording before the relay goes")
	stored := p.Stats().Stored

	// A viewer is watching when it goes. It does what §39.4 says a viewer
	// does: its session ends, so it asks Live again, every half second
	// until Live names a relay that is up, and opens a session there. E6
	// holds it to 10 s from the relay going to a picture again: the first
	// frame that decodes on its own, received whole.
	sets := api.NewSetServiceClient(admin)
	before, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, first, before.GetSources()[0].GetRelayId())
	v := watch(t, before.GetSources()[0])
	defer v.close()
	v.video(t, 50, 20*time.Second)
	index := indexFrames(t, sample)

	var other []byte
	killed := time.Now()
	if string(first) == string(r1.Id().Bytes()) {
		stop1()
		other = r2.Id().Bytes()
	} else {
		stop2()
		other = r1.Id().Bytes()
	}
	var live *api.SetLiveResponse
	asks := 0
	require.Eventually(t, func() bool {
		asks++
		live, err = sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		return err == nil && string(live.GetSources()[0].GetRelayId()) != string(first)
	}, 90*time.Second, 500*time.Millisecond, "Live names another relay once this one is down")
	named := time.Since(killed)
	require.Len(t, live.GetSources(), 1)
	require.Equal(t, other, live.GetSources()[0].GetRelayId())
	require.Equal(t, other, assigned(), "the producer was reassigned")
	var tile wallTile
	for {
		// The producer may not be on the new relay yet: no picture comes
		// until it is, and the viewer tries again.
		tile = watchTile(t, live.GetSources()[0], index, time.Second)
		if tile.err == nil || time.Since(killed) > 60*time.Second {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, tile.err)
	recovered := tile.first.Sub(killed)
	t.Logf("relay down: Live named the other relay %d ms after, on ask %d; the first frame from it %d ms after (%d ms after that offer)",
		named.Milliseconds(), asks, recovered.Milliseconds(), tile.first.Sub(tile.offered).Milliseconds())
	require.Less(t, recovered, 10*time.Second, "a viewer has its picture again within 10 s of the relay going")

	// The recording never depended on the relay.
	require.Eventually(t, func() bool { return p.Stats().Stored > stored }, 90*time.Second, 500*time.Millisecond, "the recording went on across the relay outage: %+v", p.Stats())
	require.Zero(t, p.Stats().Lost, "and nothing was lost to it")
}
