package cp_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/source"
	catalogcp "github.com/lesomnus/shale/source/cp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type connection struct {
	mu      sync.Mutex
	reply   *api.CameraCatalogSnapshot
	err     error
	block   bool
	calls   int
	called  chan int
	request *api.CameraAssignmentSnapshotRequest
}

func (c *connection) Invoke(ctx context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	if method != api.CameraAssignmentService_Snapshot_FullMethodName {
		return errors.New("unexpected RPC")
	}
	c.mu.Lock()
	c.calls++
	count, failure, block := c.calls, c.err, c.block
	c.request = proto.Clone(request.(proto.Message)).(*api.CameraAssignmentSnapshotRequest)
	reply := proto.Clone(c.reply).(*api.CameraCatalogSnapshot)
	c.mu.Unlock()
	if c.called != nil {
		select {
		case c.called <- count:
		default:
		}
	}
	if block {
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	if failure != nil {
		return failure
	}
	proto.Merge(response.(proto.Message), reply)
	return nil
}
func (*connection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("streaming access is not required")
}
func (c *connection) update(f func(*connection)) { c.mu.Lock(); defer c.mu.Unlock(); f(c) }

func fixture() (*connection, catalogcp.Options) {
	id := func(d pdid.Domain) []byte { return pdid.New(d).Bytes() }
	a := api.CameraAssignment_builder{
		Id: id(28), Tenant: api.Tenant_builder{Id: id(1)}.Build(), Set: api.Set_builder{Id: id(7)}.Build(), Producer: api.Producer_builder{Id: id(22)}.Build(), Network: api.CameraNetwork_builder{Id: id(26)}.Build(), Generation: 1, CatalogRevision: 1,
	}.Build()
	spec := api.CameraCaptureSpec_builder{BindingId: id(29), SourceId: id(8), Revision: 1, Alias: "entrance", Input: "rtsp://192.0.2.10/stream", Profile: api.CameraCaptureProfile_builder{Codec: "h264", Width: 1280, Height: 720, Fps: 29.97, MaxBitrateBps: 2_000_000}.Build()}.Build()
	c := &connection{reply: api.CameraCatalogSnapshot_builder{Assignment: a, Revision: 1, Sources: []*api.CameraCaptureSpec{spec}}.Build(), called: make(chan int, 16)}
	str := func(b []byte) string { v, _ := pdid.From(b); return v.String() }
	return c, catalogcp.Options{TenantID: str(a.GetTenant().GetId()), SetID: str(a.GetSet().GetId()), ProducerID: str(a.GetProducer().GetId()), PollInterval: 5 * time.Millisecond, CallTimeout: 50 * time.Millisecond}
}

func TestSnapshotMapsExactCatalogAndPinsIdentity(t *testing.T) {
	c, options := fixture()
	p, err := catalogcp.New(c, options)
	require.NoError(t, err)
	v, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, 29.97, v.Sources[0].Profile.FPS)
	require.Zero(t, v.Sources[0].Profile.KeyframeInterval)
	require.Equal(t, options.ProducerID, v.Scope.ProducerID)
	c.mu.Lock()
	require.Equal(t, c.reply.GetAssignment().GetSet().GetId(), c.request.GetRef().GetSet().GetSet().GetId())
	c.mu.Unlock()
	v.Sources[0].Input = "consumer-write"
	again, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, "rtsp://192.0.2.10/stream", again.Sources[0].Input)
	c.update(func(c *connection) { c.reply.GetSources()[0].SetInput("rtsp://192.0.2.11/stream") })
	_, err = p.Snapshot(context.Background())
	require.ErrorIs(t, err, source.ErrConflict, "equal revision cannot silently change content")
}

func TestRejectedReadsNeverBecomeEmptyOrReplaceCache(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*connection)
		want   error
	}{
		{"foreign producer", func(c *connection) {
			c.reply.GetAssignment().SetProducer(api.Producer_builder{Id: pdid.New(22).Bytes()}.Build())
		}, source.ErrConflict},
		{"wrong source domain", func(c *connection) { c.reply.GetSources()[0].SetSourceId(pdid.New(22).Bytes()) }, source.ErrInvalid},
		{"missing binding", func(c *connection) { c.reply.GetSources()[0].SetBindingId(nil) }, source.ErrInvalid},
		{"userinfo", func(c *connection) {
			c.reply.GetSources()[0].SetInput("rtsp://operator:fixture-secret@192.0.2.10/stream")
		}, source.ErrInvalid},
		{"transport", func(c *connection) { c.err = errors.New("rtsp://operator:fixture-secret@192.0.2.10/stream") }, source.ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, options := fixture()
			options.Cache = &source.Memory{}
			p, err := catalogcp.New(c, options)
			require.NoError(t, err)
			before, err := p.Snapshot(context.Background())
			require.NoError(t, err)
			c.update(tt.change)
			_, err = p.Snapshot(context.Background())
			require.ErrorIs(t, err, tt.want)
			require.NotContains(t, err.Error(), "fixture-secret")
			after, err := options.Cache.Snapshot(context.Background())
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestPollingRecoversLostLocalNotifications(t *testing.T) {
	c, options := fixture()
	p, err := catalogcp.New(c, options)
	require.NoError(t, err)
	first, err := p.Snapshot(context.Background())
	require.NoError(t, err)
	<-c.called
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		revision source.Revision
		err      error
	}
	done := make(chan result, 1)
	go func() { r, e := p.WaitForChange(ctx, first.Revision); done <- result{r, e} }()
	select {
	case count := <-c.called:
		require.Equal(t, 2, count)
	case <-ctx.Done():
		t.Fatal("wait did not perform its bounded read")
	}
	// Only CP changes. There is no in-memory Publisher notification.
	c.update(func(c *connection) {
		c.reply.SetRevision(2)
		c.reply.GetAssignment().SetCatalogRevision(2)
		c.reply.GetSources()[0].SetRevision(2)
	})
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.EqualValues(t, 2, r.revision.Catalog)
	case <-ctx.Done():
		t.Fatal("lost notification was not recovered by polling")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	require.GreaterOrEqual(t, c.calls, 3)
}

func TestBoundedCallsCancellationAndSafeStatus(t *testing.T) {
	c, options := fixture()
	options.CallTimeout = 5 * time.Millisecond
	p, err := catalogcp.New(c, options)
	require.NoError(t, err)
	c.update(func(c *connection) { c.block = true })
	_, err = p.Snapshot(context.Background())
	require.ErrorIs(t, err, source.ErrUnavailable)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.WaitForChange(ctx, source.Revision{})
	require.ErrorIs(t, err, context.Canceled)
	c.update(func(c *connection) {
		c.block = false
		c.err = status.Error(codes.PermissionDenied, "do not echo fixture-secret")
	})
	_, err = p.Snapshot(context.Background())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.NotContains(t, err.Error(), "fixture-secret")
}
