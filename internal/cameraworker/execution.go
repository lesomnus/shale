// Package cameraworker hosts the outbound CP-authorized camera execution role.
// It has no inbound listener and never uses a person's session as its identity.
package cameraworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"google.golang.org/protobuf/proto"
)

// Executor is the device-operation seam. Implementations consume the complete
// pinned CP job, not a lossy local Run projection. Device I/O must use Step,
// checkpoint recoverable decisions before writes, and honor context cancellation.
// CP credential delivery and the complete preset checkpoint are separate layers.
type Executor interface {
	Execute(context.Context, *Execution) error
}
type ExecutorFunc func(context.Context, *Execution) error

func (f ExecutorFunc) Execute(ctx context.Context, e *Execution) error { return f(ctx, e) }

// Execution serializes lease changes and typed CP checkpoints. It retains the
// same job after an uncertain device operation; it never creates a replacement
// job or substitutes new inputs. The CP remains the durable state authority.
type Execution struct {
	life         context.Context
	mu           sync.Mutex
	work         *api.CameraWorkerWork
	workers      api.CameraWorkerServiceClient
	jobs         api.CameraJobServiceClient
	presets      api.CameraPresetServiceClient
	credentials  api.CameraCredentialServiceClient
	leaseSeconds int64
	cancel       context.CancelFunc
}

// bound prevents an executor from escaping session revocation by passing an
// unrelated caller context to a later operation.
func (e *Execution) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	linked, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(e.life, cancel)
	if e.life.Err() != nil {
		cancel()
	}
	return linked, func() { stop(); cancel() }
}

func (e *Execution) request() *api.CameraWorkerCheckJobRequest {
	j := e.work.GetJob()
	return api.CameraWorkerCheckJobRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration()}.Build()
}

// Snapshot returns a detached typed checkpoint; it is not an execution grant.
func (e *Execution) Snapshot() *api.CameraWorkerWork {
	e.mu.Lock()
	defer e.mu.Unlock()
	return proto.Clone(e.work).(*api.CameraWorkerWork)
}

func (e *Execution) check(ctx context.Context) (*api.CameraWorkerWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, err := e.workers.CheckJob(ctx, e.request())
	if err != nil {
		e.cancel()
		return nil, err
	}
	e.work = v
	return proto.Clone(v).(*api.CameraWorkerWork), nil
}

// Check obtains a fresh adopted-identity, assignment, job-owner and lease grant.
func (e *Execution) Check(ctx context.Context) (*api.CameraWorkerWork, error) {
	ctx, done := e.bound(ctx)
	defer done()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.check(ctx)
}

// Step checks immediately before each device operation and bounds it by the
// current lease. A server cannot fence a camera firmware write already in flight;
// after an uncertain result the executor must reconcile this same pinned job.
func (e *Execution) Step(ctx context.Context, operation func(context.Context, *api.CameraWorkerWork) error) error {
	if operation == nil {
		return errors.New("camera worker operation is missing")
	}
	work, err := e.Check(ctx)
	if err != nil {
		return err
	}
	expiry := work.GetJob().GetLeaseExpires()
	if expiry == nil {
		e.cancel()
		return errors.New("camera worker lease is missing")
	}
	ctx, release := e.bound(ctx)
	defer release()
	if err := ctx.Err(); err != nil {
		return err
	}
	bounded, done := context.WithDeadline(ctx, expiry.AsTime())
	defer done()
	if err := bounded.Err(); err != nil {
		return err
	}
	return operation(bounded, work)
}

// Checkpoint durably reports typed progress before subsequent device writes.
// A stable operation key must be reused when retrying an uncertain response.
func (e *Execution) Checkpoint(ctx context.Context, key string, stage api.CameraJobStage, observed *api.CameraJobObservation) (*api.CameraJob, error) {
	ctx, done := e.bound(ctx)
	defer done()
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.check(ctx); err != nil {
		return nil, err
	}
	j := e.work.GetJob()
	v, err := e.jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: key, LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Stage: stage, Observed: observed}.Build())
	if err != nil {
		e.cancel()
		return nil, err
	}
	e.work.SetJob(v)
	return proto.Clone(v).(*api.CameraJob), nil
}

func (e *Execution) Publish(ctx context.Context, key string, spec *api.CameraCaptureSpec, observed *api.CameraJobObservation) (*api.CameraJobPublishResponse, error) {
	ctx, done := e.bound(ctx)
	defer done()
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.check(ctx); err != nil {
		return nil, err
	}
	j := e.work.GetJob()
	v, err := e.jobs.Publish(ctx, api.CameraJobPublishRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: key, LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Spec: spec, Observed: observed}.Build())
	if err != nil {
		e.cancel()
		return nil, err
	}
	e.work.SetJob(v.GetJob())
	e.work.SetBinding(v.GetBinding())
	return v, nil
}

// Observe reads only this leased job's readiness and Source capability. It does
// not require broad Source/List, Live or Lamina rights for the Worker identity.
func (e *Execution) Observe(ctx context.Context) (*api.CameraWorkerObservation, error) {
	ctx, done := e.bound(ctx)
	defer done()
	e.mu.Lock()
	defer e.mu.Unlock()
	v, err := e.workers.ObserveJob(ctx, e.request())
	if err != nil {
		e.cancel()
		return nil, err
	}
	e.work = v.GetWork()
	return v, nil
}

func (e *Execution) renew(ctx context.Context) error {
	ctx, done := e.bound(ctx)
	defer done()
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.check(ctx); err != nil {
		return err
	}
	j := e.work.GetJob()
	v, err := e.jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: operationKey(), LeaseSeconds: e.leaseSeconds}.Build())
	if err != nil {
		e.cancel()
		return err
	}
	e.work.SetJob(v)
	return nil
}

func pendingExecutor(ctx context.Context, e *Execution) error {
	// A binary without an execution provider may resume an uncertain job from a
	// previous process. Preserve its evidence and warnings while yielding, rather
	// than replacing the recoverable checkpoint with a status-only observation.
	observed := e.Snapshot().GetJob().GetObserved()
	if observed == nil {
		observed = &api.CameraJobObservation{}
	}
	observed.SetCode("central_credentials_pending")
	_, err := e.Checkpoint(ctx, operationKey(), api.CameraJobStage_CAMERA_JOB_STAGE_WAITING, observed)
	return err
}

// Keep cancellation timely even when a long poll has no catalog changes.
func (e *Execution) keep(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(e.leaseSeconds) * time.Second / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.renew(ctx); err != nil {
				return err
			}
		}
	}
}
