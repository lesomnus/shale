package core

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type currentWorkerStore struct {
	api.Server
	service currentWorkerService
}

func (s currentWorkerStore) CameraWorker() api.CameraWorkerServiceServer { return s.service }

type currentWorkerService struct {
	api.CameraWorkerServiceServer
	row *api.CameraWorker
}

func (s currentWorkerService) Get(context.Context, *api.CameraWorkerGetRequest) (*api.CameraWorker, error) {
	return proto.Clone(s.row).(*api.CameraWorker), nil
}

// An RPC frame can outlive the certificate used at the TLS handshake. Current
// state, not that previously-authenticated snapshot, must bound each emission.
func TestCameraWorkerOpenFrameRechecksExpiryAndSerial(t *testing.T) {
	now := time.Now()
	worker, tenant := pdid.New(DomCameraWorker), pdid.New(DomTenant)
	prior := api.CameraWorker_builder{Id: worker.Bytes(), Tenant: api.Tenant_builder{Id: tenant.Bytes()}.Build(), State: api.HostState_HOST_STATE_ADOPTED, CertSerial: "serial-1", DateCertExpires: timestamppb.New(now.Add(time.Hour))}.Build()
	ctx := frame.Into(context.Background(), frame.New(worker, tenant, frame.Whole()).WithRow(prior))
	current := proto.Clone(prior).(*api.CameraWorker)
	own := currentWorkerStore{service: currentWorkerService{row: current}}
	c := New(own, &Deps{Own: own, Now: func() time.Time { return now }})
	_, err := c.cameraWorkerActor(ctx)
	require.NoError(t, err)
	now = now.Add(2 * time.Hour)
	_, err = c.cameraWorkerActor(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an open authenticated frame cannot extend certificate expiry")
	now = now.Add(-2 * time.Hour)
	current.SetCertSerial("serial-2")
	_, err = c.cameraWorkerActor(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an open frame cannot use a replaced serial")
	current.SetCertSerial("serial-1")
	current.SetState(api.HostState_HOST_STATE_PENDING)
	_, err = c.cameraWorkerActor(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "adoption is checked again")
}
