package e2e_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcfixture "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const edgeRPCPrivateSentinel = "private-rpc-response:/credential+secret"

type edgeRPCBackend struct {
	grpcfixture.UnimplementedTestServiceServer
	observes  atomic.Int64
	reports   atomic.Int64
	publishes atomic.Int64
	resolves  atomic.Int64
	streams   atomic.Int64
	mu        sync.Mutex
	seen      metadata.MD
}

type edgeRPCPolicyBackend struct {
	api.UnimplementedCameraPolicyServiceServer
	b *edgeRPCBackend
}

func (s edgeRPCPolicyBackend) Observe(ctx context.Context, r *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error) {
	return s.b.Observe(ctx, r)
}

type edgeRPCJobBackend struct {
	api.UnimplementedCameraJobServiceServer
	b *edgeRPCBackend
}

func (s edgeRPCJobBackend) Report(ctx context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
	return s.b.Report(ctx, r)
}
func (s edgeRPCJobBackend) Publish(ctx context.Context, r *api.CameraJobPublishRequest) (*api.CameraJobPublishResponse, error) {
	return s.b.Publish(ctx, r)
}

type edgeRPCCredentialBackend struct {
	api.UnimplementedCameraCredentialServiceServer
	b *edgeRPCBackend
}

func (s edgeRPCCredentialBackend) Resolve(ctx context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
	return s.b.Resolve(ctx, r)
}

func (b *edgeRPCBackend) headers(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	b.mu.Lock()
	b.seen = md.Copy()
	b.mu.Unlock()
	_ = grpc.SetHeader(ctx, metadata.Pairs("fixture-header", "header-value"))
	grpc.SetTrailer(ctx, metadata.Pairs("fixture-trailer", "trailer-value", "fixture-bin", string([]byte{0, 255, 1})))
}
func (b *edgeRPCBackend) Observe(ctx context.Context, r *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error) {
	b.headers(ctx)
	if r.GetDiscoveryId() == "fail" {
		e, _ := status.New(codes.PermissionDenied, edgeRPCPrivateSentinel+" "+base64.StdEncoding.EncodeToString([]byte(edgeRPCPrivateSentinel))+" "+url.QueryEscape(edgeRPCPrivateSentinel)).WithDetails(&errdetails.DebugInfo{Detail: edgeRPCPrivateSentinel})
		return nil, e.Err()
	}
	b.observes.Add(1)
	return api.CameraPolicyObserveResponse_builder{Code: r.GetDiscoveryId()}.Build(), nil
}
func (b *edgeRPCBackend) Report(ctx context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
	b.headers(ctx)
	b.reports.Add(1)
	return api.CameraJob_builder{RequestKey: r.GetOperationKey()}.Build(), nil
}
func (b *edgeRPCBackend) Publish(ctx context.Context, r *api.CameraJobPublishRequest) (*api.CameraJobPublishResponse, error) {
	b.headers(ctx)
	b.publishes.Add(1)
	return api.CameraJobPublishResponse_builder{Job: api.CameraJob_builder{RequestKey: r.GetOperationKey()}.Build()}.Build(), nil
}
func (b *edgeRPCBackend) Resolve(ctx context.Context, _ *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
	b.headers(ctx)
	b.resolves.Add(1)
	return api.CameraCredentialLease_builder{Material: []byte(edgeRPCPrivateSentinel)}.Build(), nil
}
func (b *edgeRPCBackend) UnaryCall(ctx context.Context, r *grpcfixture.SimpleRequest) (*grpcfixture.SimpleResponse, error) {
	b.headers(ctx)
	return &grpcfixture.SimpleResponse{Payload: r.Payload}, nil
}
func (b *edgeRPCBackend) FullDuplexCall(s grpc.BidiStreamingServer[grpcfixture.StreamingOutputCallRequest, grpcfixture.StreamingOutputCallResponse]) error {
	b.streams.Add(1)
	defer b.streams.Add(-1)
	b.headers(s.Context())
	for {
		r, err := s.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = s.Send(&grpcfixture.StreamingOutputCallResponse{Payload: r.Payload}); err != nil {
			return err
		}
		if string(r.GetPayload().GetBody()) == "backend-early-eof" {
			return nil
		}
	}
}
func (b *edgeRPCBackend) StreamingInputCall(s grpc.ClientStreamingServer[grpcfixture.StreamingInputCallRequest, grpcfixture.StreamingInputCallResponse]) error {
	b.headers(s.Context())
	var n int32
	for {
		r, err := s.Recv()
		if err == io.EOF {
			return s.SendAndClose(&grpcfixture.StreamingInputCallResponse{AggregatedPayloadSize: n})
		}
		if err != nil {
			return err
		}
		n += int32(len(r.GetPayload().GetBody()))
	}
}
func (b *edgeRPCBackend) StreamingOutputCall(r *grpcfixture.StreamingOutputCallRequest, s grpc.ServerStreamingServer[grpcfixture.StreamingOutputCallResponse]) error {
	b.headers(s.Context())
	for i := 0; i < 3; i++ {
		if err := s.Send(&grpcfixture.StreamingOutputCallResponse{Payload: r.Payload}); err != nil {
			return err
		}
	}
	return nil
}

func edgeRPCFixture(t *testing.T, useTCP bool, parents ...context.Context) (*edgeRPCFaultProxy, *edgeRPCBackend, *grpc.ClientConn) {
	t.Helper()
	parent := t.Context()
	if len(parents) > 0 {
		parent = parents[0]
	}
	b := &edgeRPCBackend{}
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	api.RegisterCameraPolicyServiceServer(server, edgeRPCPolicyBackend{b: b})
	api.RegisterCameraJobServiceServer(server, edgeRPCJobBackend{b: b})
	api.RegisterCameraCredentialServiceServer(server, edgeRPCCredentialBackend{b: b})
	grpcfixture.RegisterTestServiceServer(server, b)
	var listen net.Listener
	var err error
	if useTCP {
		listen, err = net.Listen("tcp", "127.0.0.1:0")
	} else {
		listen = bufconn.Listen(1 << 20)
	}
	require.NoError(t, err)
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listen) }()
	var p *edgeRPCFaultProxy
	if useTCP {
		p, err = newEdgeRPCFaultProxy(parent, "127.0.0.1:0", listen.Addr().String())
		require.NoError(t, err)
	} else {
		backend, err := grpc.NewClient("passthrough:///buf-backend", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listen.(*bufconn.Listener).DialContext(ctx)
		}), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.ForceCodec(edgeRPCCodec{}), grpc.MaxCallRecvMsgSize(edgeRPCMaxMessage), grpc.MaxCallSendMsgSize(edgeRPCMaxMessage)))
		require.NoError(t, err)
		p = newEdgeRPCFaultProxyOn(parent, bufconn.Listen(1<<20), backend)
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry()}
	address := p.Address()
	if !useTCP {
		address = "passthrough:///buf-front"
		options = append(options, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return p.listen.(*bufconn.Listener).DialContext(ctx)
		}))
	}
	client, err := grpc.NewClient(address, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()); client.Close(); server.Stop(); listen.Close(); <-served })
	return p, b, client
}

func edgeRPCBound(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestEdgeRPCFaultPostCommitAndMetadata(t *testing.T) {
	for _, tcp := range []bool{false, true} {
		t.Run(fmt.Sprintf("tcp=%v", tcp), func(t *testing.T) {
			p, b, conn := edgeRPCFixture(t, tcp)
			ctx := metadata.NewOutgoingContext(edgeRPCBound(t), metadata.MD{"authorization": {"Plain @acme/worker", "second-auth-value"}, "fixture-input-bin": {string([]byte{0, 255, 2})}, "fixture-input": {"one", "two"}})
			g, err := p.Arm(edgeRPCFaultObserve)
			require.NoError(t, err)
			_, err = p.Arm(edgeRPCFaultKind(255))
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			_, err = p.Arm(edgeRPCFaultPublish)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			var failedTrailer metadata.MD
			_, err = api.NewCameraPolicyServiceClient(conn).Observe(ctx, api.CameraPolicyObserveRequest_builder{DiscoveryId: "fail"}.Build(), grpc.Trailer(&failedTrailer))
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Empty(t, status.Convert(err).Details())
			require.Empty(t, failedTrailer.Get("grpc-status-details-bin"))
			for _, value := range []string{edgeRPCPrivateSentinel, base64.StdEncoding.EncodeToString([]byte(edgeRPCPrivateSentinel)), url.QueryEscape(edgeRPCPrivateSentinel)} {
				require.NotContains(t, fmt.Sprintf("%#v", err), value)
			}
			select {
			case <-g.Matched():
				t.Fatal("backend failure consumed success-only gate")
			default:
			}
			result := make(chan error, 1)
			go func() {
				_, err := api.NewCameraPolicyServiceClient(conn).Observe(ctx, api.CameraPolicyObserveRequest_builder{DiscoveryId: edgeRPCPrivateSentinel}.Build())
				result <- err
			}()
			select {
			case <-g.Matched():
			case <-ctx.Done():
				t.Fatal("successful backend was not matched")
			}
			require.Equal(t, int64(1), b.observes.Load(), "transaction completed before the reply gate")
			select {
			case <-result:
				t.Fatal("held response returned before release/cancellation")
			default:
			}
			// Other APIs AND the next same-method call remain live while held.
			var header, trailer metadata.MD
			v, err := api.NewCameraPolicyServiceClient(conn).Observe(ctx, api.CameraPolicyObserveRequest_builder{DiscoveryId: "next"}.Build(), grpc.Header(&header), grpc.Trailer(&trailer))
			require.NoError(t, err)
			require.Equal(t, "next", v.GetCode())
			require.Equal(t, []string{"header-value"}, header.Get("fixture-header"))
			require.Equal(t, []string{"trailer-value"}, trailer.Get("fixture-trailer"))
			require.Equal(t, []string{string([]byte{0, 255, 1})}, trailer.Get("fixture-bin"))
			_, err = grpcfixture.NewTestServiceClient(conn).UnaryCall(ctx, &grpcfixture.SimpleRequest{Payload: &grpcfixture.Payload{Body: []byte("catalog-pass-through")}})
			require.NoError(t, err)
			b.mu.Lock()
			md := b.seen.Copy()
			b.mu.Unlock()
			require.Equal(t, []string{"Plain @acme/worker", "second-auth-value"}, md.Get("authorization"))
			require.Equal(t, []string{"one", "two"}, md.Get("fixture-input"))
			require.Equal(t, []string{string([]byte{0, 255, 2})}, md.Get("fixture-input-bin"))
			g.Release()
			g.Release()
			select {
			case err := <-result:
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.NotContains(t, err.Error(), edgeRPCPrivateSentinel)
			case <-ctx.Done():
				t.Fatal("released reply remained held")
			}
			require.Equal(t, int64(2), b.observes.Load(), "response loss cannot roll back or repeat backend mutation")
		})
	}
}

func TestEdgeRPCFaultAllowedKindsAndCancellation(t *testing.T) {
	for _, kind := range []edgeRPCFaultKind{edgeRPCFaultReport, edgeRPCFaultPublish, edgeRPCFaultResolve} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			p, b, conn := edgeRPCFixture(t, true)
			ctx, cancel := context.WithCancel(edgeRPCBound(t))
			defer cancel()
			g, err := p.Arm(kind)
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() {
				var err error
				switch kind {
				case edgeRPCFaultReport:
					_, err = api.NewCameraJobServiceClient(conn).Report(ctx, api.CameraJobReportRequest_builder{OperationKey: edgeRPCPrivateSentinel}.Build())
				case edgeRPCFaultPublish:
					_, err = api.NewCameraJobServiceClient(conn).Publish(ctx, api.CameraJobPublishRequest_builder{OperationKey: edgeRPCPrivateSentinel}.Build())
				case edgeRPCFaultResolve:
					_, err = api.NewCameraCredentialServiceClient(conn).Resolve(ctx, api.CameraCredentialResolveRequest_builder{}.Build())
				}
				result <- err
			}()
			select {
			case <-g.Matched():
			case <-ctx.Done():
				t.Fatal("backend success not matched")
			}
			require.Equal(t, int64(1), b.reports.Load()+b.publishes.Load()+b.resolves.Load())
			cancel()
			select {
			case err := <-result:
				require.Equal(t, codes.Canceled, status.Code(err))
				require.NotContains(t, err.Error(), edgeRPCPrivateSentinel)
			case <-time.After(time.Second):
				t.Fatal("frontend cancellation did not release call")
			}
			require.NoError(t, p.Close())
			require.NoError(t, p.Close())
			require.Equal(t, int64(1), b.reports.Load()+b.publishes.Load()+b.resolves.Load())
			_, err = p.Arm(kind)
			require.Equal(t, codes.Unavailable, status.Code(err))
		})
	}
}

func TestEdgeRPCFaultStreamingAndClose(t *testing.T) {
	for _, tcp := range []bool{false, true} {
		t.Run(fmt.Sprintf("tcp=%v", tcp), func(t *testing.T) {
			p, b, conn := edgeRPCFixture(t, tcp)
			ctx := edgeRPCBound(t)
			client := grpcfixture.NewTestServiceClient(conn)
			duplex, err := client.FullDuplexCall(ctx)
			require.NoError(t, err)
			for _, value := range []string{"one", edgeRPCPrivateSentinel, "three"} {
				require.NoError(t, duplex.Send(&grpcfixture.StreamingOutputCallRequest{Payload: &grpcfixture.Payload{Body: []byte(value)}}))
				v, err := duplex.Recv()
				require.NoError(t, err)
				require.Equal(t, []byte(value), v.GetPayload().GetBody(), "both directions progress before frontend half-close")
			}
			require.NoError(t, duplex.CloseSend())
			_, err = duplex.Recv()
			require.Equal(t, io.EOF, err)
			require.Equal(t, []string{"trailer-value"}, duplex.Trailer().Get("fixture-trailer"))
			input, err := client.StreamingInputCall(ctx)
			require.NoError(t, err)
			for i := 0; i < 3; i++ {
				require.NoError(t, input.Send(&grpcfixture.StreamingInputCallRequest{Payload: &grpcfixture.Payload{Body: []byte("abc")}}))
			}
			sum, err := input.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, int32(9), sum.GetAggregatedPayloadSize())
			output, err := client.StreamingOutputCall(ctx, &grpcfixture.StreamingOutputCallRequest{Payload: &grpcfixture.Payload{Body: []byte("video-control-stream")}})
			require.NoError(t, err)
			for i := 0; i < 3; i++ {
				v, err := output.Recv()
				require.NoError(t, err)
				require.Equal(t, []byte("video-control-stream"), v.GetPayload().GetBody())
			}
			_, err = output.Recv()
			require.Equal(t, io.EOF, err)
			early, err := client.FullDuplexCall(ctx)
			require.NoError(t, err)
			require.NoError(t, early.Send(&grpcfixture.StreamingOutputCallRequest{Payload: &grpcfixture.Payload{Body: []byte("backend-early-eof")}}))
			_, err = early.Recv()
			require.NoError(t, err)
			_, err = early.Recv()
			require.Equal(t, io.EOF, err, "backend EOF must finish even while frontend send stays open")
			drained := make(chan struct{})
			go func() { p.workers.Wait(); close(drained) }()
			select {
			case <-drained:
			case <-time.After(time.Second):
				t.Fatal("completed RPC retained a receive pump until proxy shutdown")
			}
			// Leave the frontend send side OPEN. Closing the proxy must unblock and
			// join both its RecvMsg pump and the real backend stream, not leak them.
			open, err := client.FullDuplexCall(ctx)
			require.NoError(t, err)
			require.NoError(t, open.Send(&grpcfixture.StreamingOutputCallRequest{Payload: &grpcfixture.Payload{Body: []byte("still-open")}}))
			_, err = open.Recv()
			require.NoError(t, err)
			require.Eventually(t, func() bool { return b.streams.Load() == 1 }, time.Second, time.Millisecond)
			require.NoError(t, p.Close())
			select {
			case <-p.stopped:
			default:
				t.Fatal("proxy loop did not join")
			}
			_, err = open.Recv()
			require.Error(t, err)
			require.Eventually(t, func() bool { return b.streams.Load() == 0 }, time.Second, time.Millisecond, "backend received cancellation")
		})
	}
}

func TestEdgeRPCFaultMessageBounds(t *testing.T) {
	p, _, conn := edgeRPCFixture(t, true)
	ctx := edgeRPCBound(t)
	_, err := grpcfixture.NewTestServiceClient(conn).UnaryCall(ctx, &grpcfixture.SimpleRequest{Payload: &grpcfixture.Payload{Body: bytes.Repeat([]byte("x"), edgeRPCMaxMessage+1)}}, grpc.MaxCallSendMsgSize(edgeRPCMaxMessage+1024))
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.NoError(t, p.Close())
	_, err = newEdgeRPCFaultProxy(ctx, "bad-listener-private-sentinel", "127.0.0.1:1")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-sentinel")
	_, err = (edgeRPCCodec{}).Marshal("raw-private-sentinel")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-sentinel")
	err = (edgeRPCCodec{}).Unmarshal([]byte(strings.Repeat("x", edgeRPCMaxMessage+1)), new(edgeRPCFrame))
	require.Error(t, err)
	frame := &edgeRPCFrame{data: []byte(edgeRPCPrivateSentinel)}
	for _, format := range []string{"%v", "%+v", "%#v", "%x", "%s"} {
		require.Equal(t, "[edge RPC frame]", fmt.Sprintf(format, frame))
	}
}

func TestEdgeRPCFaultParentCancellationJoins(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	p, b, conn := edgeRPCFixture(t, true, parent)
	ctx := edgeRPCBound(t)
	stream, err := grpcfixture.NewTestServiceClient(conn).FullDuplexCall(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&grpcfixture.StreamingOutputCallRequest{Payload: &grpcfixture.Payload{Body: []byte("cancel-parent")}}))
	_, err = stream.Recv()
	require.NoError(t, err)
	cancel()
	select {
	case <-p.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("parent cancellation did not join proxy workers")
	}
	_, err = stream.Recv()
	require.Error(t, err)
	require.Eventually(t, func() bool { return b.streams.Load() == 0 }, time.Second, time.Millisecond)
	require.NoError(t, p.Close())
}
