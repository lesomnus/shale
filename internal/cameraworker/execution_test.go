package cameraworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workerClient struct {
	api.CameraWorkerServiceClient
	check func(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error)
}

func (c workerClient) CheckJob(ctx context.Context, r *api.CameraWorkerCheckJobRequest, _ ...grpc.CallOption) (*api.CameraWorkerWork, error) {
	return c.check(ctx, r)
}

type jobClient struct {
	api.CameraJobServiceClient
	report func(context.Context, *api.CameraJobReportRequest) (*api.CameraJob, error)
}

func (c jobClient) Report(ctx context.Context, r *api.CameraJobReportRequest, _ ...grpc.CallOption) (*api.CameraJob, error) {
	return c.report(ctx, r)
}

func executionFixture(t *testing.T) (*Execution, context.Context) {
	t.Helper()
	life, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	work := api.CameraWorkerWork_builder{Job: api.CameraJob_builder{Id: []byte("job"), LeaseGeneration: 7, AssignmentGeneration: 9, LeaseExpires: timestamppb.New(time.Now().Add(time.Minute)), Inputs: api.CameraJobInputs_builder{Preset: api.CameraRevisionReference_builder{Id: "camera-preset", Revision: "revision-1"}.Build()}.Build()}.Build()}.Build()
	e := &Execution{life: life, work: work, cancel: cancel}
	e.workers = workerClient{check: func(_ context.Context, r *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
		require.Equal(t, int64(7), r.GetLeaseGeneration())
		require.Equal(t, int64(9), r.GetAssignmentGeneration())
		return proto.Clone(work).(*api.CameraWorkerWork), nil
	}}
	return e, life
}

func TestRevokedGrantStopsDeviceStepAndEscapedContext(t *testing.T) {
	e, life := executionFixture(t)
	e.workers = workerClient{check: func(context.Context, *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
		return nil, status.Error(codes.PermissionDenied, "worker revoked")
	}}
	calls := 0
	err := e.Step(context.Background(), func(context.Context, *api.CameraWorkerWork) error { calls++; return nil })
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, 0, calls)
	require.ErrorIs(t, life.Err(), context.Canceled)
	// Passing a new context does not revive work after the runtime canceled it.
	_, err = e.Check(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}

func TestStepKeepsFullPinnedInputsAndLeaseDeadline(t *testing.T) {
	e, life := executionFixture(t)
	snapshot := e.Snapshot()
	snapshot.GetJob().GetInputs().GetPreset().SetRevision("changed-locally")
	err := e.Step(life, func(ctx context.Context, w *api.CameraWorkerWork) error {
		require.Equal(t, "revision-1", w.GetJob().GetInputs().GetPreset().GetRevision())
		expiry, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, w.GetJob().GetLeaseExpires().AsTime(), expiry)
		e.cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestUncertainCheckpointCannotProceedToAnotherDeviceWrite(t *testing.T) {
	e, life := executionFixture(t)
	e.jobs = jobClient{report: func(_ context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
		require.Equal(t, "persist-before-write", r.GetOperationKey())
		return nil, errors.New("response lost")
	}}
	_, err := e.Checkpoint(life, "persist-before-write", api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING, nil)
	require.EqualError(t, err, "response lost")
	called := false
	err = e.Step(context.Background(), func(context.Context, *api.CameraWorkerWork) error { called = true; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
	require.Equal(t, []byte("job"), e.Snapshot().GetJob().GetId(), "uncertainty never substitutes a new job")
}

func TestDefaultExecutorWaitsForCentralProviders(t *testing.T) {
	e, life := executionFixture(t)
	e.jobs = jobClient{report: func(_ context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
		require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_WAITING, r.GetStage())
		require.Equal(t, "central_credentials_pending", r.GetObserved().GetCode())
		j := proto.Clone(e.work.GetJob()).(*api.CameraJob)
		j.SetStage(r.GetStage())
		j.SetObserved(r.GetObserved())
		return j, nil
	}}
	require.NoError(t, pendingExecutor(life, e))
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_WAITING, e.Snapshot().GetJob().GetStage())
}
