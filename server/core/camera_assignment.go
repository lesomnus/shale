package core

import (
	"bytes"
	"context"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type coreCameraAssignment struct {
	Core
	api.CameraAssignmentServiceServer
}

func (s Core) CameraAssignment() api.CameraAssignmentServiceServer {
	return coreCameraAssignment{s, s.Next().CameraAssignment()}
}
func (s Core) cameraAssignmentRow(ctx context.Context, r *api.CameraAssignmentRef) (*api.CameraAssignment, error) {
	a, err := s.Next().CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: r}.Build())
	if err != nil {
		return nil, err
	}
	if err := s.cameraExecutionScope(ctx, a); err != nil {
		return nil, err
	}
	n, err := s.Next().CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
		return nil, err
	}
	if err := s.cameraLockNetwork(ctx, a.GetNetwork().GetId()); err != nil {
		return nil, err
	}
	if err := s.cameraLockAssignment(ctx, a.GetId()); err != nil {
		return nil, err
	}
	a, err = s.Next().CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: r}.Build())
	if err != nil {
		return nil, err
	}
	if err := s.cameraExecutionParents(ctx, a); err != nil {
		return nil, err
	}
	return a, nil
}
func (s Core) cameraCatalogAdvance(ctx context.Context, a *api.CameraAssignment) (*api.CameraAssignment, error) {
	return s.Next().CameraAssignment().Patch(ctx, api.CameraAssignmentPatchRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), CatalogRevision: z.Ptr(a.GetCatalogRevision() + 1), DateUpdated: a.GetDateUpdated()}.Build())
}
func (s coreCameraAssignment) Assign(ctx context.Context, r *api.CameraAssignmentAssignRequest) (*api.CameraAssignment, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if err := cameraToken("worker_slot", r.GetWorkerSlot()); err != nil {
		return nil, err
	}
	var out *api.CameraAssignment
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: r.GetNetwork()}.Build())
		if err != nil {
			return err
		}
		if err := New(nx, s.d).cameraLockNetwork(ctx, n.GetId()); err != nil {
			return err
		}
		n, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		p, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: r.GetProducer()}.Build())
		if err != nil {
			return err
		}
		if p.GetState() != api.HostState_HOST_STATE_ADOPTED || len(p.GetSet().GetId()) == 0 {
			return failed("producer must already be adopted for a set")
		}
		set, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: p.GetSet().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraSameScope(n.GetTenant().GetId(), n.GetSite().GetId(), set.GetTenant().GetId(), set.GetSite().GetId()); err != nil {
			return err
		}
		if err := cameraSameScope(p.GetTenant().GetId(), p.GetSite().GetId(), set.GetTenant().GetId(), set.GetSite().GetId()); err != nil {
			return err
		}
		if len(r.GetWorkerId()) > 0 {
			if id, err := pdid.From(r.GetWorkerId()); err != nil || kindOf(id) != DomCameraWorker {
				return invalid("worker_id", "a worker identity is required")
			}
			w, err := nx.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: r.GetWorkerId()}.Build()}.Build())
			if err != nil {
				return err
			}
			if w.GetState() != api.HostState_HOST_STATE_ADOPTED {
				return failed("worker must already be adopted")
			}
			if err := cameraSameScope(n.GetTenant().GetId(), n.GetSite().GetId(), w.GetTenant().GetId(), w.GetSite().GetId()); err != nil {
				return err
			}
		}
		c := New(nx, s.d)
		if r.GetRef() == nil {
			old, err := c.ent(ctx).CameraAssignment.Query().Where(cameraassignment.SetIdEQ(mustId(set.GetId()).Uuid()), cameraassignment.DateErasedIsNil()).Only(ctx)
			if err == nil {
				v, err := nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: old.Id[:]}.Build()}.Build())
				if err != nil {
					return err
				}
				if !bytes.Equal(v.GetNetwork().GetId(), n.GetId()) || !bytes.Equal(v.GetProducer().GetId(), p.GetId()) || v.GetWorkerSlot() != r.GetWorkerSlot() || !bytes.Equal(v.GetWorkerId(), r.GetWorkerId()) {
					return failed("set already has a different camera assignment; use its versioned update or handover")
				}
				out = v
				return nil
			}
			if !ent.IsNotFound(err) {
				return err
			}
			out, err = nx.CameraAssignment().Add(ctx, api.CameraAssignmentAddRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Site: cameraSite(set.GetSite()), Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: p.GetId()}.Build(), WorkerSlot: r.GetWorkerSlot(), WorkerId: r.GetWorkerId(), Generation: 1, CatalogRevision: 0}.Build())
			return err
		}
		a, err := c.cameraAssignmentRow(ctx, r.GetRef())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), a.GetDateUpdated()); err != nil {
			return err
		}
		if !bytes.Equal(a.GetNetwork().GetId(), n.GetId()) || !bytes.Equal(a.GetProducer().GetId(), p.GetId()) {
			return invalid("assignment", "network and producer changes require explicit replacement; a Set is immutable")
		}
		if a.GetWorkerSlot() == r.GetWorkerSlot() && bytes.Equal(a.GetWorkerId(), r.GetWorkerId()) {
			out = a
			return nil
		}
		patch := api.CameraAssignmentPatchRequest_builder{Ref: r.GetRef(), WorkerSlot: z.Ptr(r.GetWorkerSlot()), Generation: z.Ptr(a.GetGeneration() + 1), CatalogRevision: z.Ptr(a.GetCatalogRevision() + 1), DateUpdated: a.GetDateUpdated()}
		if len(r.GetWorkerId()) > 0 {
			patch.WorkerId = r.GetWorkerId()
		} else {
			patch.WorkerIdNull = z.Ptr(true)
		}
		out, err = nx.CameraAssignment().Patch(ctx, patch.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
func (s coreCameraAssignment) Snapshot(ctx context.Context, r *api.CameraAssignmentSnapshotRequest) (*api.CameraCatalogSnapshot, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	isProducer := kindOf(f.Actor) == DomProducer
	if !isProducer {
		if _, err := cameraOperator(ctx); err != nil {
			return nil, err
		}
	}
	var out *api.CameraCatalogSnapshot
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		if isProducer {
			// Reject a known foreign ref before taking its assignment/network locks.
			a, err := nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: r.GetRef()}.Build())
			if err != nil {
				return err
			}
			if err := c.producerCatalogOwner(ctx, f.Actor, f.Tenant, a); err != nil {
				return err
			}
		}
		a, err := c.cameraAssignmentRow(ctx, r.GetRef())
		if err != nil {
			return err
		}
		if isProducer {
			// Handover, revocation or Set reassignment cannot authorize a stale
			// cached provider merely because the first read used to be its own.
			if err := c.producerCatalogOwner(ctx, f.Actor, f.Tenant, a); err != nil {
				return err
			}
		}
		rows, err := c.ent(ctx).CameraBinding.Query().Where(camerabinding.AssignmentIdEQ(mustId(a.GetId()).Uuid()), camerabinding.DateErasedIsNil(), camerabinding.CollectEQ(true)).Order(ent.Asc(camerabinding.FieldSourceAlias)).Limit(501).All(ctx)
		if err != nil {
			return err
		}
		if len(rows) > 500 {
			return status.Error(codes.ResourceExhausted, "camera catalog has more than 500 sources")
		}
		network, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		specs := make([]*api.CameraCaptureSpec, 0, len(rows))
		for _, v := range rows {
			if v.Desired == nil {
				return failed("collecting binding has no published capture specification")
			}
			if err := cameraURI(v.Desired.GetInput(), network.GetCidrs(), true); err != nil {
				return failed("published capture no longer matches its camera network")
			}
			specs = append(specs, proto.Clone(v.Desired).(*api.CameraCaptureSpec))
		}
		out = api.CameraCatalogSnapshot_builder{Assignment: a, Revision: a.GetCatalogRevision(), Sources: specs}.Build()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s Core) producerCatalogOwner(ctx context.Context, actor, tenant pdid.Id, a *api.CameraAssignment) error {
	p, err := s.own(ctx).Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: actor.Bytes()}.Build()}.Build())
	if err != nil {
		return err
	}
	return producerCatalogAssignment(actor, tenant, p, a)
}

func producerCatalogAssignment(actor, tenant pdid.Id, p *api.Producer, a *api.CameraAssignment) error {
	if actor.Domain() != DomProducer || p == nil || a == nil ||
		p.GetState() != api.HostState_HOST_STATE_ADOPTED || p.GetDateErased() != nil ||
		!bytes.Equal(p.GetId(), actor.Bytes()) || !bytes.Equal(a.GetProducer().GetId(), actor.Bytes()) ||
		!bytes.Equal(p.GetTenant().GetId(), tenant.Bytes()) || !bytes.Equal(a.GetTenant().GetId(), tenant.Bytes()) ||
		len(p.GetSet().GetId()) == 0 || !bytes.Equal(p.GetSet().GetId(), a.GetSet().GetId()) ||
		!bytes.Equal(p.GetSite().GetId(), a.GetSite().GetId()) {
		return denied("camera catalog", "assignment does not belong to this adopted Producer")
	}
	return nil
}
func (s coreCameraAssignment) Handover(ctx context.Context, r *api.CameraAssignmentHandoverRequest) (*api.CameraAssignment, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraAssignment
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		c := New(nx, s.d)
		a, err := c.cameraAssignmentRow(ctx, r.GetRef())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), a.GetDateUpdated()); err != nil {
			return err
		}
		if r.GetExpectedGeneration() != a.GetGeneration() {
			return status.Error(codes.Aborted, "camera assignment generation changed")
		}
		next, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: r.GetReplacement()}.Build())
		if err != nil {
			return err
		}
		if bytes.Equal(next.GetId(), a.GetProducer().GetId()) {
			out = a
			return nil
		}
		if !bytes.Equal(next.GetTenant().GetId(), a.GetTenant().GetId()) {
			return denied("handover", "replacement belongs to a different tenant")
		}
		if next.GetState() != api.HostState_HOST_STATE_PENDING || len(next.GetSet().GetId()) != 0 {
			return failed("replacement must be a separately joined, pending and unassigned Producer")
		}
		old, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(old.GetSet().GetId(), a.GetSet().GetId()) {
			return failed("current producer assignment changed")
		}
		// Revoke future CP access before adopting the replacement into the same
		// immutable Set. Existing data-plane capabilities retain their own expiry;
		// this transaction does not claim to terminate already-open media sessions.
		if _, err := nx.Producer().Erase(ctx, api.ProducerRef_builder{Id: old.GetId()}.Build()); err != nil {
			return err
		}
		if _, err := c.Producer().Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: next.GetId()}.Build(), Set: api.SetRef_builder{Id: a.GetSet().GetId()}.Build()}.Build()); err != nil {
			return err
		}
		out, err = nx.CameraAssignment().Patch(ctx, api.CameraAssignmentPatchRequest_builder{Ref: r.GetRef(), Producer: api.ProducerRef_builder{Id: next.GetId()}.Build(), WorkerIdNull: z.Ptr(true), Generation: z.Ptr(a.GetGeneration() + 1), CatalogRevision: z.Ptr(a.GetCatalogRevision() + 1), DateUpdated: a.GetDateUpdated()}.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
