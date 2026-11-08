package cameraworker

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type discoverySessionServer struct {
	api.UnimplementedCameraWorkerServiceServer
	assigned   *api.CameraWorkerAssignment
	work       *api.CameraWorkerWork
	ca         *pki.CA
	heartbeats atomic.Int32
	claims     atomic.Int32
	renew      atomic.Bool
}

func (s *discoverySessionServer) Heartbeat(context.Context, *api.CameraWorkerHeartbeatRequest) (*api.CameraWorkerHeartbeatResponse, error) {
	s.heartbeats.Add(1)
	return api.CameraWorkerHeartbeatResponse_builder{RenewCertificate: s.renew.Load()}.Build(), nil
}

func (s *discoverySessionServer) Poll(ctx context.Context, r *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
	if r.GetWaitSeconds() > 0 {
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	v := api.CameraWorkerPollResponse_builder{Assignments: []*api.CameraWorkerAssignment{s.assigned}, Revision: "revision"}.Build()
	if s.work != nil {
		v.SetJobs([]*api.CameraWorkerWork{s.work})
	}
	return proto.Clone(v).(*api.CameraWorkerPollResponse), nil
}

func (s *discoverySessionServer) CheckJob(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
	return proto.Clone(s.work).(*api.CameraWorkerWork), nil
}

type discoverySessionJobs struct {
	api.UnimplementedCameraJobServiceServer
	server *discoverySessionServer
}

func (s discoverySessionJobs) Claim(context.Context, *api.CameraJobClaimRequest) (*api.CameraJob, error) {
	s.server.claims.Add(1)
	return proto.Clone(s.server.work.GetJob()).(*api.CameraJob), nil
}

func (s *discoverySessionServer) RenewCertificate(_ context.Context, r *api.CameraWorkerRenewCertificateRequest) (*api.CameraWorkerRenewCertificateResponse, error) {
	id, err := pdid.From(s.assigned.GetAssignment().GetWorkerId())
	if err != nil {
		return nil, err
	}
	cert, err := s.ca.IssueClientHost(r.GetCsr(), id, time.Now())
	if err != nil {
		return nil, err
	}
	return api.CameraWorkerRenewCertificateResponse_builder{Certificate: pki.EncodeCerts(cert), CaBundle: s.ca.Bundle()}.Build(), nil
}

func discoverySessionConn(t *testing.T, server *discoverySessionServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	rpc := grpc.NewServer()
	api.RegisterCameraWorkerServiceServer(rpc, server)
	api.RegisterCameraJobServiceServer(rpc, discoverySessionJobs{server: server})
	go rpc.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///discovery-session", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(); rpc.Stop(); listener.Close() })
	return conn
}

func TestDiscoveryErrorsLeaveHeartbeatJobAndLeaseRunning(t *testing.T) {
	w, assigned, ca := discoveryFixture(t)
	j := api.CameraJob_builder{Id: pdid.New(30).Bytes(), LeaseOwner: w.ID().Bytes(), LeaseGeneration: 1, AssignmentGeneration: assigned.GetAssignment().GetGeneration(), LeaseExpires: timestamppb.New(time.Now().Add(time.Minute)), DateUpdated: timestamppb.Now()}.Build()
	server := &discoverySessionServer{assigned: assigned, ca: ca, work: api.CameraWorkerWork_builder{Job: j, Assignment: assigned.GetAssignment(), Network: assigned.GetNetwork()}.Build()}
	conn := discoverySessionConn(t, server)
	var scans atomic.Int32
	w.cfg.Discoverer = discovererFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		scans.Add(1)
		return nil, errors.New("untrusted device response with a secret")
	})
	started, stopped := make(chan struct{}), make(chan struct{})
	w.cfg.Executor = ExecutorFunc(func(ctx context.Context, _ *Execution) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.session(ctx, conn) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
	require.Eventually(t, func() bool { return scans.Load() > 2 && server.heartbeats.Load() > 2 && server.claims.Load() > 1 }, 3*time.Second, 5*time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("discovery failure canceled the current job")
	case err := <-done:
		t.Fatalf("discovery failure stopped the session: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled)
	case <-time.After(time.Second):
		t.Fatal("session did not join its work")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("executor escaped session cancellation")
	}
}

func TestDiscoveryIsJoinedOnSessionCancellationAndRenewal(t *testing.T) {
	for _, renew := range []bool{false, true} {
		name := "cancel"
		if renew {
			name = "renew"
		}
		t.Run(name, func(t *testing.T) {
			w, assigned, ca := discoveryFixture(t)
			server := &discoverySessionServer{assigned: assigned, ca: ca}
			conn := discoverySessionConn(t, server)
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			w.cfg.Discoverer = discovererFunc(func(ctx context.Context, _ camera.DiscoveryScope) ([]camera.Observation, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release // Cleanup still belongs to the old session/connection.
				return nil, ctx.Err()
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- w.session(ctx, conn) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("discovery did not start")
			}
			if renew {
				server.renew.Store(true)
			} else {
				cancel()
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("discovery escaped session cancellation")
			}
			select {
			case <-done:
				t.Fatal("session returned before discovery cleanup completed")
			default:
			}
			close(release)
			select {
			case err := <-done:
				if renew {
					require.ErrorIs(t, err, errReconnect)
				} else {
					require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled)
				}
			case <-time.After(time.Second):
				t.Fatal("session did not finish after discovery cleanup")
			}
		})
	}
}
