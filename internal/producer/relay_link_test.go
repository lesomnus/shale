package producer

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
)

// fakeIngest is a relay's ingest that welcomes every producer and counts
// the streams it saw, each held open until the producer ends it.
type fakeIngest struct {
	api.UnimplementedRelayIngestServer
	attaches atomic.Int32
	open     atomic.Int32
}

func (f *fakeIngest) Attach(stream api.RelayIngest_AttachServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	f.attaches.Add(1)
	f.open.Add(1)
	defer f.open.Add(-1)
	if err := stream.Send(api.AttachResponse_builder{Welcome: api.AttachResponse_Welcome_builder{Sources: [][]byte{pdid.New(pdid.Domain(23)).Bytes()}}.Build()}.Build()); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

// An assignment handed over before the loop runs, as negotiation does,
// opens one stream and keeps it: the signal that announced it must not
// end the stream it led to (§39.2).
func TestRelayLinkAttachesOnce(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer()
	fake := &fakeIngest{}
	api.RegisterRelayIngestServer(g, fake)
	go g.Serve(lis)
	defer g.Stop()

	p := &Producer{cfg: Config{}, log: slog.Default(), agent: &hostagent.Agent{Dev: true}}
	l := newRelayLink(p)
	port := int32(lis.Addr().(*net.TCPAddr).Port)
	ra := api.RelayAssignment_builder{
		RelayId:      pdid.New(pdid.Domain(31)).Bytes(),
		PublishToken: "tok",
		Endpoints:    []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: "127.0.0.1", Port: port}.Build()},
	}.Build()
	l.set(ra)
	// The same relay again, as a heartbeat's answer: no change.
	l.set(ra)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.run(ctx)
	require.Eventually(t, func() bool { return fake.attaches.Load() == 1 }, 5*time.Second, 20*time.Millisecond, "the producer attaches")
	time.Sleep(1500 * time.Millisecond)
	require.Equal(t, int32(1), fake.attaches.Load(), "one stream, not one per signal")
	require.Equal(t, int32(1), fake.open.Load(), "and it stays up")

	// A different relay is a change: the stream moves.
	other := api.RelayAssignment_builder{
		RelayId:      pdid.New(pdid.Domain(31)).Bytes(),
		PublishToken: "tok",
		Endpoints:    ra.GetEndpoints(),
	}.Build()
	l.set(other)
	require.Eventually(t, func() bool { return fake.attaches.Load() == 2 }, 5*time.Second, 20*time.Millisecond, "reattached to the new assignment")
	require.Eventually(t, func() bool { return fake.open.Load() == 1 }, 5*time.Second, 20*time.Millisecond, "the old stream ended")
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int32(2), fake.attaches.Load())
}
