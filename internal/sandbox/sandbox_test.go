package sandbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/auth"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/sandbox"
)

// The sandbox's hosts against a control plane in this process: what the
// console draws in the page is what the same code draws here, so this is
// where it is checked -- the hosts join and are adopted, the producer's
// segments become laminae the nodes store, a dark scene is skipped, a bad
// disk is quarantined, and Live answers a relay.
func TestSandbox(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &cmd.Config{}
	cli.ApplyDev(c, t.TempDir())
	// Adoption is the console's to do: nothing is adopted on its own.
	c.Control.AutoAdopt = false
	s, err := cmd.Build(ctx, *c)
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, cli.Migrate(ctx, s))

	b := sandbox.New(s)
	b.Every = 300 * time.Millisecond
	b.Segment = 900 * time.Millisecond
	require.NoError(t, b.Seed(ctx))
	done := make(chan error, 1)
	go func() {
		err := b.Run(ctx)
		if err != nil {
			t.Logf("sandbox: %v", err)
		}
		done <- err
	}()

	as := func(id auth.Identity, method string) context.Context {
		c, err := sandbox.As(ctx, s, id, method)
		require.NoError(t, err)

		return c
	}
	ops := as(auth.Identity{Tenant: "cluster", Alias: "ops"}, api.NodeService_List_FullMethodName)
	admin := as(auth.Identity{Tenant: "acme", Alias: "admin"}, api.SetService_List_FullMethodName)

	// Two nodes adopted, one waiting.
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Node().List(ops, api.NodeListRequest_builder{Size: 10}.Build())
		if err != nil {
			return false
		}
		adopted, pending := 0, 0
		for _, v := range vs.GetItems() {
			switch v.GetState() {
			case api.HostState_HOST_STATE_ADOPTED:
				if v.GetDateSeen() != nil {
					adopted++
				}
			case api.HostState_HOST_STATE_PENDING:
				pending++
			}
		}

		return adopted == 2 && pending == 1
	}, 20*time.Second, 100*time.Millisecond, "two nodes up and one pending")

	// The relay, adopted and heard from.
	var relays *api.RelayListResponse
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Relay().List(ops, api.RelayListRequest_builder{Size: 10}.Build())
		if err != nil || len(vs.GetItems()) != 1 {
			return false
		}
		relays = vs

		return vs.GetItems()[0].GetState() == api.HostState_HOST_STATE_ADOPTED && vs.GetItems()[0].GetDateSeen() != nil
	}, 20*time.Second, 100*time.Millisecond, "the relay is up: %v", relays)

	// The producer for the lobby with its three cameras reporting, and a
	// second one waiting.
	var lobby *api.Set
	var producers *api.ProducerListResponse
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Producer().List(admin, api.ProducerListRequest_builder{Size: 10}.Build())
		if err != nil {
			t.Logf("producers: %v", err)

			return false
		}
		producers = vs
		pending := 0
		var pi *api.Producer
		for _, v := range vs.GetItems() {
			if v.GetState() == api.HostState_HOST_STATE_PENDING {
				pending++
			} else if v.GetHostname() == "pi" {
				pi = v
			}
		}
		if pi == nil || pending != 1 || len(pi.GetStatus().GetSources()) != 3 {
			return false
		}
		set, err := s.Walled.Set().Get(admin, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: pi.GetSet().GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		lobby = set

		return true
	}, 20*time.Second, 100*time.Millisecond, "the producer reports three cameras: %v", producers)
	require.Equal(t, "lobby", lobby.GetAlias())

	// Segments: stored by the nodes, and skipped while the scene is dark.
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Lamina().List(admin, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: lobby.GetId()}.Build()}.Build()}, Size: 500,
		}.Build())
		if err != nil {
			return false
		}
		committed, skipped := 0, 0
		for _, v := range vs.GetItems() {
			switch v.GetState() {
			case api.LaminaState_LAMINA_STATE_COMMITTED:
				committed++
			case api.LaminaState_LAMINA_STATE_SKIPPED:
				skipped++
			}
		}

		return committed >= 3 && skipped >= 1
	}, 40*time.Second, 200*time.Millisecond, "laminae are committed and a dark scene skipped")

	// The bad disk is in the quarantine queue.
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Device().List(ops, api.DeviceListRequest_builder{Size: 20}.Build())
		if err != nil {
			return false
		}
		for _, v := range vs.GetItems() {
			if v.GetHealth() == api.DeviceHealth_DEVICE_HEALTH_QUARANTINED {
				return true
			}
		}

		return false
	}, 20*time.Second, 200*time.Millisecond, "the failing disk is quarantined")

	// Live: every camera of the set through the relay.
	require.Eventually(t, func() bool {
		r, err := s.Walled.Set().Live(admin, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: lobby.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}

		return len(r.GetSources()) == 3 && r.GetSources()[0].GetWhepUrl() != ""
	}, 20*time.Second, 200*time.Millisecond, "live answers every camera")

	// Adopting the waiting node starts it: the console's adopt is this call.
	vs, err := s.Walled.Node().List(ops, api.NodeListRequest_builder{Size: 10}.Build())
	require.NoError(t, err)
	for _, v := range vs.GetItems() {
		if v.GetState() == api.HostState_HOST_STATE_PENDING {
			_, err := s.Walled.Node().Adopt(ops, api.NodeAdoptRequest_builder{Ref: api.NodeRef_builder{Id: v.GetId()}.Build(), Alias: v.GetHostname()}.Build())
			require.NoError(t, err)
		}
	}
	require.Eventually(t, func() bool {
		vs, err := s.Walled.Node().List(ops, api.NodeListRequest_builder{Size: 10}.Build())
		if err != nil {
			return false
		}
		up := 0
		for _, v := range vs.GetItems() {
			if v.GetState() == api.HostState_HOST_STATE_ADOPTED && v.GetDateSeen() != nil {
				up++
			}
		}

		return up == 3
	}, 20*time.Second, 100*time.Millisecond, "the adopted node comes up")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the sandbox did not stop")
	}
}
