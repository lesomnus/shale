package producer

import (
	"log/slog"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
)

func TestRelayEpochControlPriorityAndDelayedHelper(t *testing.T) {
	l := newRelayLink(&Producer{log: slog.New(slog.DiscardHandler)})
	id := pdid.New(8)
	l.resetSource(id, "old", false)
	for range sendQueue {
		l.sendDataEpoch(id, "old", []byte("old-payload"))
	}
	require.Len(t, l.send, sendQueue)
	lateHelper := func() { l.sendDataEpoch(id, "old", []byte("late-old-helper")) }
	l.resetSource(id, "new", false)
	control := l.takeControl()
	require.NotNil(t, control.GetReset(), "control is retained even while the lossy queue is full")
	require.Equal(t, "new", control.GetReset().GetCaptureEpoch())
	for len(l.send) > 0 {
		require.False(t, l.currentData(<-l.send), "queued old frames cannot cross a reset")
	}
	lateHelper()
	require.Empty(t, l.send, "an asynchronous helper retains its old epoch")
	l.sendDataEpoch(id, "new", []byte("new"))
	require.True(t, l.currentData(<-l.send))
	l.resetSource(id, "new", true)
	l.sendDataEpoch(id, "new", []byte("stopped"))
	require.Empty(t, l.send)
	require.True(t, l.takeControl().GetReset().GetStopped())
}

func TestRelayEpochMembershipHelloPrecedesRetainedResets(t *testing.T) {
	l := newRelayLink(&Producer{log: slog.New(slog.DiscardHandler)})
	id, relay := pdid.New(8), pdid.New(24)
	ep := []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: "127.0.0.1", Port: 7430}.Build()}
	l.set(api.RelayAssignment_builder{RelayId: relay.Bytes(), PublishToken: "first", Endpoints: ep}.Build())
	<-l.changed
	l.mu.Lock()
	l.relayId = relay
	l.mu.Unlock()
	l.resetSource(id, "epoch-1", false)
	first := l.takeControl()
	require.NotNil(t, first.GetReset())
	l.set(api.RelayAssignment_builder{RelayId: relay.Bytes(), PublishToken: "fresh", Endpoints: ep}.Build())
	select {
	case <-l.changed:
		t.Fatal("membership refresh must not reconnect unrelated live sources")
	default:
	}
	hello := l.takeControl()
	require.Equal(t, "fresh", hello.GetHello().GetPublishToken())
	replayed := l.takeControl()
	require.Equal(t, "epoch-1", replayed.GetReset().GetCaptureEpoch(), "reset is retried after the source's fresh authenticated membership")
	require.Nil(t, l.takeControl())
	l.sourcesChanged()
	l.sourcesChanged()
	require.True(t, l.takeRefresh())
	require.False(t, l.takeRefresh(), "membership refresh work coalesces without blocking admission")
}
