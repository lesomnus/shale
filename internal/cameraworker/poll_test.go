package cameraworker

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/media"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exercise the generated RPC clients, not a replacement local scheduling loop.
// The fixed-order snapshot deliberately keeps a failed camera before its peer.
type pollServer struct {
	api.UnimplementedCameraWorkerServiceServer
	mu        sync.Mutex
	work      []*api.CameraWorkerWork
	deny      map[string]bool
	trace     []string
	reports   chan string
	afterWait chan struct{}
}

func (s *pollServer) Poll(ctx context.Context, _ *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := api.CameraWorkerPollResponse_builder{Revision: "fixed", Jobs: s.work}.Build()
	if s.afterWait != nil && len(s.trace) > 0 {
		allWaiting := true
		for _, work := range s.work {
			allWaiting = allWaiting && work.GetJob().GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_WAITING
		}
		if allWaiting {
			close(s.afterWait)
			s.afterWait = nil
		}
	}
	return proto.Clone(out).(*api.CameraWorkerPollResponse), nil
}

func (s *pollServer) CheckJob(_ context.Context, r *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := string(r.GetRef().GetId())
	s.trace = append(s.trace, "check:"+id)
	if s.deny[id] {
		return nil, status.Error(codes.PermissionDenied, "grant revoked")
	}
	for _, work := range s.work {
		j := work.GetJob()
		if bytes.Equal(j.GetId(), r.GetRef().GetId()) && j.GetLeaseGeneration() == r.GetLeaseGeneration() && j.GetAssignmentGeneration() == r.GetAssignmentGeneration() {
			return proto.Clone(work).(*api.CameraWorkerWork), nil
		}
	}
	return nil, status.Error(codes.FailedPrecondition, "stale grant")
}

type pollJobs struct {
	api.UnimplementedCameraJobServiceServer
	server *pollServer
}

func (s pollJobs) Claim(_ context.Context, r *api.CameraJobClaimRequest) (*api.CameraJob, error) {
	s.server.mu.Lock()
	defer s.server.mu.Unlock()
	id := string(r.GetRef().GetId())
	s.server.trace = append(s.server.trace, "claim:"+id)
	for _, work := range s.server.work {
		if bytes.Equal(work.GetJob().GetId(), r.GetRef().GetId()) {
			return proto.Clone(work.GetJob()).(*api.CameraJob), nil
		}
	}
	return nil, status.Error(codes.NotFound, "job missing")
}

func (s pollJobs) Report(_ context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
	s.server.mu.Lock()
	defer s.server.mu.Unlock()
	for _, work := range s.server.work {
		if bytes.Equal(work.GetJob().GetId(), r.GetRef().GetId()) {
			work.GetJob().SetStage(r.GetStage())
			work.GetJob().SetObserved(r.GetObserved())
			s.server.reports <- string(work.GetJob().GetId())
			return proto.Clone(work.GetJob()).(*api.CameraJob), nil
		}
	}
	return nil, status.Error(codes.NotFound, "job missing")
}

func pollFixture(t *testing.T) (*Worker, *pollServer, *grpc.ClientConn) {
	t.Helper()
	w, assigned, _ := discoveryFixture(t)
	s := &pollServer{deny: map[string]bool{}, reports: make(chan string, 10)}
	for range 2 {
		job := api.CameraJob_builder{Id: pdid.New(30).Bytes(), LeaseOwner: w.ID().Bytes(), LeaseGeneration: 1,
			AssignmentGeneration: assigned.GetAssignment().GetGeneration(), LeaseExpires: timestamppb.New(time.Now().Add(time.Minute)),
			DateUpdated: timestamppb.Now()}.Build()
		s.work = append(s.work, api.CameraWorkerWork_builder{Job: job, Assignment: assigned.GetAssignment(), Network: assigned.GetNetwork()}.Build())
	}
	listener := bufconn.Listen(1 << 20)
	rpc := grpc.NewServer()
	api.RegisterCameraWorkerServiceServer(rpc, s)
	api.RegisterCameraJobServiceServer(rpc, pollJobs{server: s})
	go rpc.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///poll-fairness", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(); rpc.Stop(); listener.Close() })
	return w, s, conn
}

func (s *pollServer) executed(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trace = append(s.trace, "device:"+id)
}

func (s *pollServer) events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.trace...)
}

func TestPollExecutorErrorRotatesAcrossReconnects(t *testing.T) {
	for _, failure := range []error{media.ErrMismatch, camera.ErrUnsupported} {
		t.Run(failure.Error(), func(t *testing.T) {
			w, server, conn := pollFixture(t)
			bad, good := string(server.work[0].GetJob().GetId()), string(server.work[1].GetJob().GetId())
			var executed []string
			w.cfg.Executor = ExecutorFunc(func(ctx context.Context, e *Execution) error {
				return e.Step(ctx, func(_ context.Context, work *api.CameraWorkerWork) error {
					id := string(work.GetJob().GetId())
					server.executed(id)
					executed = append(executed, id)
					if id == bad {
						return failure
					}
					return nil
				})
			})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			require.ErrorIs(t, w.poll(ctx, conn), failure)
			// A new poll invocation has fresh session-local yielded state, just as
			// after Run reconnects its gRPC session following the driver's error.
			require.ErrorIs(t, w.poll(ctx, conn), failure)
			require.Equal(t, []string{bad, good, bad}, executed)
			require.Equal(t, []string{"claim:" + bad, "check:" + bad, "check:" + bad, "device:" + bad,
				"claim:" + good, "check:" + good, "check:" + good, "device:" + good,
				"claim:" + bad, "check:" + bad, "check:" + bad, "device:" + bad}, server.events(),
				"rotation must still Claim and freshly CheckJob before each device operation")
		})
	}
}

func TestPollRotatesAfterDeniedGrantWithoutDeviceOperation(t *testing.T) {
	w, server, conn := pollFixture(t)
	bad, good := string(server.work[0].GetJob().GetId()), string(server.work[1].GetJob().GetId())
	server.deny[bad] = true
	w.cfg.Executor = ExecutorFunc(func(ctx context.Context, e *Execution) error {
		return e.Step(ctx, func(_ context.Context, work *api.CameraWorkerWork) error {
			server.executed(string(work.GetJob().GetId()))
			return media.ErrMismatch
		})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.Equal(t, codes.PermissionDenied, status.Code(w.poll(ctx, conn)))
	require.Equal(t, []string{"claim:" + bad, "check:" + bad}, server.events())
	require.ErrorIs(t, w.poll(ctx, conn), media.ErrMismatch)
	events := server.events()
	require.Equal(t, "device:"+good, events[len(events)-1])
}

func TestPollSkipsOtherLiveOwnerBeforeClaim(t *testing.T) {
	w, server, conn := pollFixture(t)
	server.work[0].GetJob().SetLeaseOwner(pdid.New(Domain).Bytes())
	good := string(server.work[1].GetJob().GetId())
	w.cfg.Executor = ExecutorFunc(func(ctx context.Context, e *Execution) error {
		return e.Step(ctx, func(_ context.Context, work *api.CameraWorkerWork) error {
			server.executed(string(work.GetJob().GetId()))
			return media.ErrMismatch
		})
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, w.poll(ctx, conn), media.ErrMismatch)
	require.Equal(t, []string{"claim:" + good, "check:" + good, "check:" + good, "device:" + good}, server.events())
}

func TestPollPendingExecutorYieldsEachJobUntilReconnect(t *testing.T) {
	w, server, conn := pollFixture(t)
	w.cfg.Executor = ExecutorFunc(pendingExecutor)
	ready := make(chan struct{})
	server.afterWait = ready
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.poll(ctx, conn) }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("pending executor starved the second job")
	}
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled)
	case <-time.After(time.Second):
		t.Fatal("poll did not join its canceled executor")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Len(t, server.reports, 2, "yielded jobs must not busy-loop in the same session")
	for _, work := range server.work {
		require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_WAITING, work.GetJob().GetStage())
		require.Equal(t, "central_credentials_pending", work.GetJob().GetObserved().GetCode())
	}
}

func TestPollCursorFollowsIdentityAndForgetsMissingJobs(t *testing.T) {
	w := &Worker{}
	yielded := map[string]bool{}
	var previous []*api.CameraWorkerWork
	for range 500 {
		j := api.CameraJob_builder{Id: pdid.New(30).Bytes()}.Build()
		previous = append(previous, api.CameraWorkerWork_builder{Job: j}.Build())
		yielded[string(j.GetId())] = true
	}
	retained := previous[170]
	w.lastAttempted = string(retained.GetJob().GetId())
	fresh := api.CameraWorkerWork_builder{Job: api.CameraJob_builder{Id: pdid.New(30).Bytes()}.Build()}.Build()
	next := []*api.CameraWorkerWork{fresh, retained}
	require.Equal(t, 0, w.pollStart(next, yielded))
	require.Equal(t, map[string]bool{w.lastAttempted: true}, yielded,
		"removed jobs must not accumulate yielded state across snapshots")
	// Insertion/reordering must use job identity, not an index from an old list.
	require.Equal(t, 1, w.pollStart([]*api.CameraWorkerWork{retained, fresh}, yielded))
	require.Equal(t, 0, w.pollStart(nil, yielded))
	require.Empty(t, w.lastAttempted)
	require.Empty(t, yielded)
}
