package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const edgeRPCMaxMessage = 4 << 20

type edgeRPCFaultKind uint8

const (
	edgeRPCFaultObserve edgeRPCFaultKind = iota + 1
	edgeRPCFaultReport
	edgeRPCFaultPublish
	edgeRPCFaultResolve
)

func (k edgeRPCFaultKind) method() string {
	switch k {
	case edgeRPCFaultObserve:
		return api.CameraPolicyService_Observe_FullMethodName
	case edgeRPCFaultReport:
		return api.CameraJobService_Report_FullMethodName
	case edgeRPCFaultPublish:
		return api.CameraJobService_Publish_FullMethodName
	case edgeRPCFaultResolve:
		return api.CameraCredentialService_Resolve_FullMethodName
	default:
		return ""
	}
}

type edgeRPCGate struct {
	kind    edgeRPCFaultKind
	matched chan struct{}
	release chan struct{}
	once    sync.Once
	claimed bool // proxy.mu
}

// Matched closes only AFTER one matching backend unary RPC returned SUCCESS.
// Release drops its response with a fixed Unavailable. It does not undo a
// transaction or repeat a request. Cancellation can independently lose the reply.
func (g *edgeRPCGate) Matched() <-chan struct{} { return g.matched }
func (g *edgeRPCGate) Release()                 { g.once.Do(func() { close(g.release) }) }

type edgeRPCFrame struct{ data []byte }

// grpc's optional tracing must not format an opaque credential-bearing frame.
func (*edgeRPCFrame) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "[edge RPC frame]") }

// No proto decoding, global codec registration or payload diagnostics. A copy
// lets the transport own its marshaled buffer without retaining a caller frame.
type edgeRPCCodec struct{}

func (edgeRPCCodec) Name() string { return "proto" }
func (edgeRPCCodec) Marshal(value any) ([]byte, error) {
	f, ok := value.(*edgeRPCFrame)
	if !ok || f == nil || len(f.data) > edgeRPCMaxMessage {
		return nil, errors.New("invalid edge RPC frame")
	}
	return bytes.Clone(f.data), nil
}
func (edgeRPCCodec) Unmarshal(raw []byte, value any) error {
	f, ok := value.(*edgeRPCFrame)
	if !ok || f == nil || len(raw) > edgeRPCMaxMessage {
		return errors.New("invalid edge RPC frame")
	}
	clear(f.data)
	f.data = bytes.Clone(raw)
	return nil
}

// edgeRPCFaultProxy is a plaintext Dev fixture, not a TLS authentication proxy.
// Every incoming application metadata/auth value is forwarded unchanged. It
// parses no messages and grants no CP authority. Only the fixed unary methods
// above may be armed; all other calls preserve streaming/full-duplex behavior.
// Backend error codes survive, but error text/details use fixed diagnostics.
type edgeRPCFaultProxy struct {
	server  *grpc.Server
	backend *grpc.ClientConn
	listen  net.Listener
	ctx     context.Context
	cancel  context.CancelFunc
	parent  func() bool
	served  chan struct{}
	stopped chan struct{}
	close   sync.Once
	workers sync.WaitGroup
	active  chan struct{}
	mu      sync.Mutex
	closed  bool
	gate    *edgeRPCGate
}

func newEdgeRPCFaultProxy(ctx context.Context, listenAddress, backendAddress string) (*edgeRPCFaultProxy, error) {
	if ctx.Err() != nil {
		return nil, edgeRPCError(ctx.Err())
	}
	host, port, err := net.SplitHostPort(backendAddress)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || host == "" || number < 1 || number > 65535 || len(backendAddress) > 512 || strings.ContainsAny(host, "/@?#\\\r\n\x00") {
		return nil, status.Error(codes.InvalidArgument, "invalid edge RPC backend")
	}
	backend, err := grpc.NewClient("passthrough:///"+backendAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.ForceCodec(edgeRPCCodec{}), grpc.MaxCallRecvMsgSize(edgeRPCMaxMessage), grpc.MaxCallSendMsgSize(edgeRPCMaxMessage)))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "edge RPC backend unavailable")
	}
	lis, err := net.Listen("tcp", listenAddress)
	if err != nil {
		backend.Close()
		return nil, status.Error(codes.Unavailable, "edge RPC listener unavailable")
	}
	p := newEdgeRPCFaultProxyOn(ctx, lis, backend)
	return p, nil
}

// The listener/client seam is only for bufconn protocol tests. The coordinator
// uses the TCP constructor and no method can change the backend or arm SQL.
func newEdgeRPCFaultProxyOn(ctx context.Context, lis net.Listener, backend *grpc.ClientConn) *edgeRPCFaultProxy {
	life, cancel := context.WithCancel(ctx)
	p := &edgeRPCFaultProxy{listen: lis, backend: backend, ctx: life, cancel: cancel, served: make(chan struct{}), stopped: make(chan struct{}), active: make(chan struct{}, 128)}
	p.server = grpc.NewServer(grpc.ForceServerCodec(edgeRPCCodec{}), grpc.MaxRecvMsgSize(edgeRPCMaxMessage), grpc.MaxSendMsgSize(edgeRPCMaxMessage), grpc.MaxHeaderListSize(64<<10), grpc.UnknownServiceHandler(p.forward))
	go func() { defer close(p.served); _ = p.server.Serve(lis) }()
	p.mu.Lock()
	p.parent = context.AfterFunc(ctx, func() { _ = p.Close() })
	p.mu.Unlock()
	return p
}

func (p *edgeRPCFaultProxy) Address() string { return p.listen.Addr().String() }

func (p *edgeRPCFaultProxy) Arm(kind edgeRPCFaultKind) (*edgeRPCGate, error) {
	if kind.method() == "" {
		return nil, status.Error(codes.InvalidArgument, "unsupported edge RPC gate")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, status.Error(codes.Unavailable, "edge RPC proxy closed")
	}
	if p.gate != nil {
		return nil, status.Error(codes.FailedPrecondition, "edge RPC gate already armed")
	}
	g := &edgeRPCGate{kind: kind, matched: make(chan struct{}), release: make(chan struct{})}
	p.gate = g
	return g, nil
}

func (p *edgeRPCFaultProxy) matched(method string) *edgeRPCGate {
	p.mu.Lock()
	defer p.mu.Unlock()
	g := p.gate
	if g == nil || g.claimed || g.kind.method() != method {
		return nil
	}
	g.claimed = true
	close(g.matched)
	return g
}

func (p *edgeRPCFaultProxy) disarm(g *edgeRPCGate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gate == g {
		p.gate = nil
	}
}

func (p *edgeRPCFaultProxy) forward(_ any, front grpc.ServerStream) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return status.Error(codes.Unavailable, "edge RPC proxy closed")
	}
	p.workers.Add(1)
	p.mu.Unlock()
	defer p.workers.Done()
	select {
	case p.active <- struct{}{}:
		defer func() { <-p.active }()
	default:
		return status.Error(codes.ResourceExhausted, "edge RPC concurrency limit")
	}
	method, ok := grpc.MethodFromServerStream(front)
	if !ok {
		return status.Error(codes.Internal, "edge RPC method unavailable")
	}
	ctx, cancel := context.WithCancel(front.Context())
	done := context.AfterFunc(p.ctx, cancel)
	defer done()
	defer cancel()
	md, _ := metadata.FromIncomingContext(front.Context())
	ctx = metadata.NewOutgoingContext(ctx, md.Copy())
	for _, kind := range []edgeRPCFaultKind{edgeRPCFaultObserve, edgeRPCFaultReport, edgeRPCFaultPublish, edgeRPCFaultResolve} {
		if method == kind.method() {
			return p.unary(ctx, front, method)
		}
	}
	return p.stream(ctx, front, method)
}

func edgeRPCError(err error) error {
	if err == nil {
		return nil
	}
	code := status.Code(err)
	if errors.Is(err, context.Canceled) {
		code = codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return status.Error(code, "edge RPC forwarding failed")
}

func edgeRPCTrailer(md metadata.MD) metadata.MD {
	copy := md.Copy()
	// A rewritten status must not retain binary details from the old error.
	delete(copy, "grpc-status-details-bin")
	return copy
}

func (p *edgeRPCFaultProxy) unary(ctx context.Context, front grpc.ServerStream, method string) error {
	var request, response, extra edgeRPCFrame
	defer func() { clear(request.data); clear(response.data); clear(extra.data) }()
	if err := front.RecvMsg(&request); err != nil {
		return edgeRPCError(err)
	}
	if err := front.RecvMsg(&extra); err != io.EOF {
		return status.Error(codes.InvalidArgument, "edge RPC unary input invalid")
	}
	var header, trailer metadata.MD
	err := p.backend.Invoke(ctx, method, &request, &response, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		if len(header) > 0 {
			if sendErr := front.SendHeader(edgeRPCTrailer(header)); sendErr != nil {
				return edgeRPCError(sendErr)
			}
		}
		front.SetTrailer(edgeRPCTrailer(trailer))
		return edgeRPCError(err)
	}
	// Invoke includes the final successful status, not just the first payload.
	if gate := p.matched(method); gate != nil {
		defer p.disarm(gate)
		select {
		case <-gate.release:
			return status.Error(codes.Unavailable, "edge RPC response intentionally unavailable")
		case <-ctx.Done():
			return edgeRPCError(ctx.Err())
		}
	}
	if len(header) > 0 {
		if err := front.SendHeader(header); err != nil {
			return edgeRPCError(err)
		}
	}
	front.SetTrailer(edgeRPCTrailer(trailer))
	return edgeRPCError(front.SendMsg(&response))
}

func (p *edgeRPCFaultProxy) stream(ctx context.Context, front grpc.ServerStream, method string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	back, err := p.backend.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
	if err != nil {
		return edgeRPCError(err)
	}
	// A frontend RecvMsg is canceled by gRPC when this handler returns. Track
	// it globally so Close joins it even when the backend ends before half-close.
	p.workers.Add(1)
	upstream := make(chan error, 1)
	go func() {
		defer p.workers.Done()
		var frame edgeRPCFrame
		defer func() { clear(frame.data) }()
		for {
			err := front.RecvMsg(&frame)
			if err == io.EOF {
				upstream <- back.CloseSend()
				return
			}
			if err != nil {
				upstream <- err
				cancel()
				return
			}
			if err := back.SendMsg(&frame); err != nil {
				upstream <- err
				if err != io.EOF {
					cancel()
				}
				return
			}
		}
	}()
	header, err := back.Header()
	if err != nil {
		front.SetTrailer(edgeRPCTrailer(back.Trailer()))
		select {
		case cause := <-upstream:
			if cause != nil && cause != io.EOF {
				return edgeRPCError(cause)
			}
		default:
		}
		return edgeRPCError(err)
	}
	if len(header) > 0 {
		if err := front.SendHeader(header); err != nil {
			return edgeRPCError(err)
		}
	}
	var response edgeRPCFrame
	defer func() { clear(response.data) }()
	for {
		err = back.RecvMsg(&response)
		if err != nil {
			front.SetTrailer(edgeRPCTrailer(back.Trailer()))
			if err == io.EOF {
				return nil
			}
			return edgeRPCError(err)
		}
		if err = front.SendMsg(&response); err != nil {
			return edgeRPCError(err)
		}
	}
}

// Close is idempotent, immediately cancels active/gated calls and their backend
// requests, stops accepting work, closes transports and joins every proxy loop.
func (p *edgeRPCFaultProxy) Close() error {
	p.close.Do(func() {
		go func() {
			p.mu.Lock()
			p.closed = true
			p.mu.Unlock()
			p.cancel()
			p.backend.Close()
			p.server.Stop()
			p.listen.Close()
			<-p.served
			p.workers.Wait()
			if p.parent != nil {
				p.parent()
			}
			close(p.stopped)
		}()
	})
	select {
	case <-p.stopped:
		return nil
	case <-time.After(3 * time.Second):
		return status.Error(codes.DeadlineExceeded, "edge RPC proxy shutdown timed out")
	}
}
