package core

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Poll is deliberately bounded and outbound-only. A revision covers the whole
// typed snapshot, including pinned job inputs; timestamps are not change cursors.
func (s coreCameraWorker) Poll(ctx context.Context, r *api.CameraWorkerPollRequest) (*api.CameraWorkerPollResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if r.GetWaitSeconds() < 0 || r.GetWaitSeconds() > 15 {
		return nil, invalid("wait_seconds", "a poll waits zero to 15 seconds")
	}
	if v := r.GetAfterRevision(); v != "" {
		if b, err := hex.DecodeString(v); err != nil || len(b) != 32 {
			return nil, invalid("after_revision", "a snapshot revision is required")
		}
	}
	until := time.Now().Add(time.Duration(r.GetWaitSeconds()) * time.Second)
	for {
		w, err := s.cameraWorkerActor(ctx)
		if err != nil {
			return nil, err
		}
		var out *api.CameraWorkerPollResponse
		err = s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
			c := New(nx, s.d)
			rows, err := c.ent(ctx).CameraAssignment.Query().Where(cameraassignment.WorkerIdEQ(mustId(w.GetId()).Uuid()), cameraassignment.DateErasedIsNil()).Order(ent.Asc(cameraassignment.FieldNetworkId), ent.Asc(cameraassignment.FieldId)).Limit(101).All(ctx)
			if err != nil {
				return err
			}
			if len(rows) > 100 {
				return status.Error(codes.ResourceExhausted, "worker has more than 100 assignments")
			}
			assignments := make([]*api.CameraWorkerAssignment, 0, len(rows))
			jobs := make([]*api.CameraWorkerWork, 0)
			for _, row := range rows {
				a, err := c.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: row.Id[:]}.Build())
				if err != nil {
					return err
				}
				// A concurrent handover can remove a row selected before its lock.
				if !bytes.Equal(a.GetWorkerId(), w.GetId()) {
					continue
				}
				if err := c.cameraExecutionScope(ctx, a); err != nil {
					return err
				}
				n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
				if err != nil {
					return err
				}
				if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
					return err
				}
				assignments = append(assignments, api.CameraWorkerAssignment_builder{Assignment: a, Network: n}.Build())
				pending, err := c.ent(ctx).CameraJob.Query().Where(camerajob.AssignmentIdEQ(row.Id), camerajob.DateErasedIsNil(), camerajob.StageNotIn(int32(api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE), int32(api.CameraJobStage_CAMERA_JOB_STAGE_FAILED), int32(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED))).Order(ent.Asc(camerajob.FieldId)).Limit(501 - len(jobs)).All(ctx)
				if err != nil {
					return err
				}
				if len(jobs)+len(pending) > 500 {
					return status.Error(codes.ResourceExhausted, "worker has more than 500 pending jobs")
				}
				for _, job := range pending {
					j, a, b, d, err := c.cameraJobRows(ctx, api.CameraJobRef_builder{Id: job.Id[:]}.Build())
					if err != nil {
						return err
					}
					jobs = append(jobs, api.CameraWorkerWork_builder{Job: j, Assignment: a, Network: n, Binding: b, Device: d}.Build())
				}
			}
			out = api.CameraWorkerPollResponse_builder{Assignments: assignments, Jobs: jobs}.Build()
			if proto.Size(out) > 2<<20 {
				return status.Error(codes.ResourceExhausted, "worker snapshot exceeds two MiB; split assignments across workers")
			}
			out.SetRevision(hex.EncodeToString(cameraHash(out)))
			return nil
		})
		if err != nil {
			return nil, err
		}
		// Recheck the serial even for an RPC opened before renew/revoke.
		if _, err := s.cameraWorkerActor(ctx); err != nil {
			return nil, err
		}
		if out.GetRevision() != r.GetAfterRevision() || !time.Now().Before(until) {
			return out, nil
		}
		delay := min(250*time.Millisecond, time.Until(until))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s Core) cameraWorkerWork(ctx context.Context, r *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
	w, err := s.cameraWorkerActor(ctx)
	if err != nil {
		return nil, err
	}
	j, a, b, d, err := s.cameraJobRows(ctx, r.GetRef())
	if err != nil {
		return nil, err
	}
	if err := s.cameraLease(j, a, d, mustId(w.GetId()), r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
		return nil, err
	}
	n, err := s.Next().CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
		return nil, err
	}
	return api.CameraWorkerWork_builder{Job: j, Assignment: a, Network: n, Binding: b, Device: d}.Build(), nil
}

func (s coreCameraWorker) CheckJob(ctx context.Context, r *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerWork, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	var out *api.CameraWorkerWork
	err := s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		var err error
		out, err = New(nx, s.d).cameraWorkerWork(ctx, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// ObserveJob mints only the leased binding's Source capability. No generated
// Source, Set or Lamina endpoint is opened to the Worker principal.
func (s coreCameraWorker) ObserveJob(ctx context.Context, r *api.CameraWorkerCheckJobRequest) (*api.CameraWorkerObservation, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	var out *api.CameraWorkerObservation
	err := s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		work, err := c.cameraWorkerWork(ctx, r)
		if err != nil {
			return err
		}
		a, b, j := work.GetAssignment(), work.GetBinding(), work.GetJob()
		p, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if p.GetState() != api.HostState_HOST_STATE_ADOPTED || !bytes.Equal(p.GetSet().GetId(), a.GetSet().GetId()) {
			return failed("assigned producer is no longer adopted for this set")
		}
		out = api.CameraWorkerObservation_builder{Work: work, ProducerStatus: p.GetStatus()}.Build()
		if j.GetDatePublished() == nil || j.GetPublishedRevision() == 0 {
			return nil
		}
		src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		app, managed, err := c.currentSourceApplication(ctx, src)
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				return nil
			}
			return err
		}
		if !managed || app.GetTarget().GetSpecRevision() != j.GetPublishedRevision() || app.GetTarget().GetAssignmentGeneration() != a.GetGeneration() {
			return nil
		}
		after := app.GetDateStarted().AsTime().Add(-5 * time.Second)
		complete, err := c.ent(ctx).Lamina.Query().Where(lamina.SourceIdEQ(mustId(b.GetSource().GetId()).Uuid()), lamina.CaptureEpochEQ(app.GetCaptureEpoch()), lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_COMMITTED)), lamina.IncompleteEQ(false), lamina.DateCommittedGTE(after), lamina.DateStartedGTE(after)).Exist(ctx)
		if err != nil {
			return err
		}
		out.SetRecordingComplete(complete)

		set, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: a.GetSet().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		f, err := actor(ctx)
		if err != nil {
			return err
		}
		live, err := c.liveSourcesUntil(ctx, f, set, []*api.Source{src}, j.GetLeaseExpires().AsTime())
		if err != nil && status.Code(err) != codes.FailedPrecondition {
			return err
		}
		if len(live) > 0 {
			out.SetLive(live[0])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
