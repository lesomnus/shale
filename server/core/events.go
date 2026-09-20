package core

import (
	"context"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/object"
	"github.com/lesomnus/shale/internal/ent/sink"
	"github.com/lesomnus/shale/internal/placement"
)

// Events a node pushes (§34.9), applied idempotently: ObjectStored is the
// node's final word about what is on its device (§14), ObjectDeleted
// confirms a deletion (§21.2), ObjectMissing says a key is gone.

func (s coreNode) PushEvents(ctx context.Context, req *api.NodePushEventsRequest) (*api.NodePushEventsResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomNode {
		return nil, status.Error(codes.PermissionDenied, "only a node pushes events")
	}

	applied := 0
	for _, ev := range req.GetEvents() {
		var err error
		switch {
		case ev.GetStored() != nil:
			err = s.ownTx(ctx, func(own api.Server) error { return s.applyStored(ctx, own, f.Actor, ev.GetStored()) })
		case ev.GetDeleted() != nil:
			err = s.ownTx(ctx, func(own api.Server) error { return s.applyDeleted(ctx, own, f.Actor, ev.GetDeleted()) })
		case ev.GetMissing() != nil:
			err = s.ownTx(ctx, func(own api.Server) error { return s.applyMissing(ctx, own, f.Actor, ev.GetMissing()) })
		default:
			continue
		}
		if err != nil {
			// One bad event does not hold the batch: the node resends
			// nothing it was answered for, and logs are where a refused
			// event goes.
			s.d.log().Warn("event refused", "node", f.Actor.String(), "err", err.Error())
			continue
		}
		applied++
	}

	return api.NodePushEventsResponse_builder{Applied: int32(applied)}.Build(), nil
}

// sinkOnNode checks that the event's sink is one the node may speak for: the
// attempt was allocated to that sink on that node, even if the sink has
// since moved (§28.3).
func (s Core) sinkOnNode(ctx context.Context, nodeId pdid.Id, sinkId []byte, at *ent.Attempt) error {
	sid, err := pdid.From(sinkId)
	if err != nil {
		return invalid("sink_id", err.Error())
	}
	if at != nil {
		if pdid.Id(at.SinkId) == sid && pdid.Id(at.NodeId) == nodeId {
			return nil
		}
	}
	row, err := s.d.Ent.Sink.Query().Where(sink.IdEQ(sid.Uuid())).Only(ctx)
	if err != nil {
		return err
	}
	if isZero(row.NodeId) || pdid.Id(row.NodeId) != nodeId {
		return status.Errorf(codes.PermissionDenied, "sink %s is not attached to this node", sid)
	}

	return nil
}

func (s Core) applyStored(ctx context.Context, own api.Server, nodeId pdid.Id, ev *api.ObjectStored) error {
	atId, err := pdid.From(ev.GetAttemptId())
	if err != nil {
		return invalid("attempt_id", err.Error())
	}
	objId, err := pdid.From(ev.GetObjectId())
	if err != nil {
		return invalid("object_id", err.Error())
	}
	now := s.d.now()

	at, err := s.d.Ent.Attempt.Query().Where(attempt.IdEQ(atId.Uuid())).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return err
	}
	if err := s.sinkOnNode(ctx, nodeId, ev.GetSinkId(), at); err != nil {
		return err
	}

	obj, err := s.d.Ent.Object.Query().Where(object.IdEQ(objId.Uuid())).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return err
	}

	// A row that was removed, or never known (a lost DB, reconciliation):
	// recreated from the record (§14, §29).
	if obj == nil {
		rec := ev.GetRecord()
		if rec == nil || len(rec.GetTenantId()) == 0 || len(rec.GetSetId()) == 0 || len(rec.GetSourceId()) == 0 {
			return failed("object %s is unknown and the event carries no record", objId)
		}
		set, err := own.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: rec.GetSetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		add := api.ObjectAddRequest_builder{
			Id:               objId.Bytes(),
			Tenant:           api.TenantRef_builder{Id: rec.GetTenantId()}.Build(),
			Set:              api.SetRef_builder{Id: rec.GetSetId()}.Build(),
			Source:           api.SourceRef_builder{Id: rec.GetSourceId()}.Build(),
			State:            api.ObjectState_OBJECT_STATE_PENDING,
			DateStarted:      timestamppb.New(time.UnixMilli(rec.GetDateStartedMs()).UTC()),
			DateExpired:      timestamppb.New(time.UnixMilli(rec.GetDateExpiredMs()).UTC()),
			DatesSynced:      true,
			PlacementVersion: rec.GetPlacementVersion(),
			Epoch:            placement.Epoch(rec.GetDateStartedMs()/1000, int64(epochOf(set).Seconds())),
		}
		if len(rec.GetSiteId()) > 0 {
			add.Site = api.SiteRef_builder{Id: rec.GetSiteId()}.Build()
		}
		if rec.GetDateDeletedMs() > 0 {
			add.DateDeleted = timestamppb.New(time.UnixMilli(rec.GetDateDeletedMs()).UTC())
		}
		if _, err := own.Object().Add(ctx, add.Build()); err != nil {
			return err
		}
		obj, err = s.d.Ent.Object.Query().Where(object.IdEQ(objId.Uuid())).Only(ctx)
		if err != nil {
			return err
		}
	}

	// And the attempt, when the node knows one the CP does not.
	if at == nil {
		var siteRef *api.SiteRef
		if !isZero(obj.SiteId) {
			siteRef = api.SiteRef_builder{Id: obj.SiteId[:]}.Build()
		}
		if _, err := own.Attempt().Add(ctx, api.AttemptAddRequest_builder{
			Id:          atId.Bytes(),
			Tenant:      api.TenantRef_builder{Id: obj.TenantId[:]}.Build(),
			Site:        siteRef,
			Object:      api.ObjectRef_builder{Id: objId.Bytes()}.Build(),
			Sink:        api.SinkRef_builder{Id: ev.GetSinkId()}.Build(),
			Node:        api.NodeRef_builder{Id: nodeId.Bytes()}.Build(),
			State:       api.AttemptState_ATTEMPT_STATE_ALLOCATED,
			DateExpires: timestamppb.New(now),
		}.Build()); err != nil {
			return err
		}
		at, err = s.d.Ent.Attempt.Query().Where(attempt.IdEQ(atId.Uuid())).Only(ctx)
		if err != nil {
			return err
		}
	}

	// Redelivery: nothing changes.
	if at.State == int32(api.AttemptState_ATTEMPT_STATE_STORED) || at.State == int32(api.AttemptState_ATTEMPT_STATE_DUPLICATE) {
		return nil
	}

	// Who wins (§14): the first stored attempt, except that a complete one
	// beats an incomplete one.
	winner := true
	if obj.State == int32(api.ObjectState_OBJECT_STATE_COMMITTED) && !isZero(obj.SinkId) {
		if obj.Incomplete && !ev.GetIncomplete() {
			// The truncated one becomes the duplicate.
			old, err := s.d.Ent.Attempt.Query().
				Where(attempt.ObjectIdEQ(objId.Uuid()), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_STORED))).
				All(ctx)
			if err != nil {
				return err
			}
			for _, o := range old {
				st := api.AttemptState_ATTEMPT_STATE_DUPLICATE
				if _, err := own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
					Ref: api.AttemptRef_builder{Id: o.Id[:]}.Build(), State: &st, DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
			}
		} else {
			winner = false
		}
	}
	if obj.State == int32(api.ObjectState_OBJECT_STATE_DELETING) || obj.State == int32(api.ObjectState_OBJECT_STATE_DELETED) {
		// The object was deleted meanwhile; this file is a duplicate GC
		// will reclaim from its xattr dates.
		winner = false
	}

	st := api.AttemptState_ATTEMPT_STATE_STORED
	if !winner {
		st = api.AttemptState_ATTEMPT_STATE_DUPLICATE
	}
	if _, err := own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
		Ref:              api.AttemptRef_builder{Id: at.Id[:]}.Build(),
		State:            &st,
		DateFinished:     timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return err
	}
	if !winner {
		return nil
	}

	committed := now
	if ev.GetDateCommitted() != nil {
		committed = ev.GetDateCommitted().AsTime()
	}
	patch := api.ObjectPatchRequest_builder{
		Ref:              api.ObjectRef_builder{Id: objId.Bytes()}.Build(),
		Sink:             api.SinkRef_builder{Id: ev.GetSinkId()}.Build(),
		ObjectKey:        z.Ptr(ev.GetObjectKey()),
		State:            z.Ptr(api.ObjectState_OBJECT_STATE_COMMITTED),
		Size:             z.Ptr(ev.GetSize()),
		Incomplete:       z.Ptr(ev.GetIncomplete()),
		DateCommitted:    timestamppb.New(committed),
		DateFinishedNull: z.Ptr(true),
		DateUpdatedForce: z.Ptr(true),
	}
	if ev.GetDateStarted() != nil {
		patch.DateStarted = ev.GetDateStarted()
	}
	if ev.GetDateEnded() != nil && !ev.GetIncomplete() {
		patch.DateEnded = ev.GetDateEnded()
		patch.EndedEstimated = z.Ptr(false)
	} else {
		patch.EndedEstimated = z.Ptr(true)
	}
	if len(ev.GetChecksum()) > 0 {
		patch.Checksum = ev.GetChecksum()
	}
	if _, err := own.Object().Patch(ctx, patch.Build()); err != nil {
		return err
	}

	// Other open attempts of the object are superseded.
	open, err := s.d.Ent.Attempt.Query().
		Where(attempt.ObjectIdEQ(objId.Uuid()), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.IdNEQ(at.Id)).
		All(ctx)
	if err != nil {
		return err
	}
	for _, o := range open {
		st := api.AttemptState_ATTEMPT_STATE_ABANDONED
		if _, err := own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
			Ref: api.AttemptRef_builder{Id: o.Id[:]}.Build(), State: &st, DateFinished: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}
	}

	// The observed rate (§12.6) and the tenant's stored bytes (§21.4).
	if err := s.observe(ctx, own, obj, ev); err != nil {
		return err
	}

	return s.storedBytes(ctx, own, pdid.Id(obj.TenantId), ev.GetSize())
}

// observe folds a commit into the source's observed rate: an exponentially
// weighted average with a half-life of one epoch (§12.6).
func (s Core) observe(ctx context.Context, own api.Server, obj *ent.Object, ev *api.ObjectStored) error {
	if ev.GetDateStarted() == nil || ev.GetDateEnded() == nil || ev.GetSize() <= 0 {
		return nil
	}
	span := ev.GetDateEnded().AsTime().Sub(ev.GetDateStarted().AsTime())
	if span <= 0 {
		return nil
	}
	rate := float64(ev.GetSize()) * 8 / span.Seconds()

	src, err := own.Source().Get(ctx, api.SourceGetRequest_builder{
		Ref:    api.SourceRef_builder{Id: obj.SourceId[:]}.Build(),
	}.Build())
	if err != nil {
		return err
	}
	set, err := own.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: obj.SetId[:]}.Build()}.Build())
	if err != nil {
		return err
	}

	now := s.d.now()
	epoch := epochOf(set)
	ob := src.GetObserved()
	recent := rate
	if ob != nil && ob.GetRecent() > 0 && ob.GetDateUpdated() != nil {
		dt := now.Sub(ob.GetDateUpdated().AsTime())
		if dt < 0 {
			dt = 0
		}
		// Weight of the old value after dt at a half-life of one epoch.
		w := 0.5
		if epoch > 0 {
			w = pow(0.5, dt.Seconds()/epoch.Seconds())
		}
		recent = w*float64(ob.GetRecent()) + (1-w)*rate
	}
	expected := recent
	if cap := src.GetProfile().GetMaxBitrate(); cap > 0 && expected > float64(cap) {
		expected = float64(cap)
	}

	_, err = own.Source().Patch(ctx, api.SourcePatchRequest_builder{
		Ref: api.SourceRef_builder{Id: src.GetId()}.Build(),
		Observed: api.ObservedRate_builder{
			Recent:      int64(recent),
			Expected:    int64(expected),
			DateUpdated: timestamppb.New(now),
		}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
}

func pow(base, exp float64) float64 {
	// A small helper so the file needs no math import for one call.
	r := 1.0
	if exp <= 0 {
		return 1
	}
	// Use the identity base^exp = e^(exp ln base) through repeated squaring
	// for integer parts and a series for the rest is overkill; math.Pow is
	// fine.
	r = mathPow(base, exp)

	return r
}

// storedBytes moves a tenant's stored bytes by delta (§21.4).
func (s Core) storedBytes(ctx context.Context, own api.Server, tenantId pdid.Id, delta int64) error {
	if delta == 0 {
		return nil
	}
	t, err := own.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: tenantRef(tenantId)}.Build())
	if err != nil {
		return err
	}
	v := t.GetStoredBytes() + delta
	if v < 0 {
		v = 0
	}
	_, err = own.Tenant().Patch(ctx, api.TenantPatchRequest_builder{
		Ref:              tenantRef(tenantId),
		StoredBytes:      &v,
		DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
}

// applyDeleted confirms a deletion: the object at that location is DELETED,
// or a duplicate's file is forgotten (§21.2).
func (s Core) applyDeleted(ctx context.Context, own api.Server, nodeId pdid.Id, ev *api.ObjectDeleted) error {
	if err := s.sinkOnNode(ctx, nodeId, ev.GetSinkId(), nil); err != nil {
		return err
	}
	sid := mustId(ev.GetSinkId())
	now := s.d.now()

	obj, err := s.d.Ent.Object.Query().Where(object.SinkIdEQ(sid.Uuid()), object.ObjectKeyEQ(ev.GetObjectKey())).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return err
	}
	if obj == nil {
		// A duplicate's or an orphan's file: nothing in the index changes.
		if len(ev.GetAttemptId()) > 0 {
			if at, err := s.d.Ent.Attempt.Query().Where(attempt.IdEQ(mustId(ev.GetAttemptId()).Uuid())).First(ctx); err == nil && at.State == int32(api.AttemptState_ATTEMPT_STATE_DUPLICATE) {
				if o, err := s.d.Ent.Object.Query().Where(object.IdEQ(at.ObjectId)).First(ctx); err == nil {
					return s.storedBytes(ctx, own, pdid.Id(o.TenantId), -ev.GetSize())
				}
			}
		}

		return nil
	}
	if obj.State == int32(api.ObjectState_OBJECT_STATE_DELETED) {
		return nil
	}

	when := now
	if ev.GetDateDeleted() != nil {
		when = ev.GetDateDeleted().AsTime()
	}
	if _, err := own.Object().Patch(ctx, api.ObjectPatchRequest_builder{
		Ref:              api.ObjectRef_builder{Id: obj.Id[:]}.Build(),
		State:            z.Ptr(api.ObjectState_OBJECT_STATE_DELETED),
		DateFinished:     timestamppb.New(when),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return err
	}

	return s.storedBytes(ctx, own, pdid.Id(obj.TenantId), -obj.Size)
}

// applyMissing marks an object lost, or confirms a deletion in progress
// (§14).
func (s Core) applyMissing(ctx context.Context, own api.Server, nodeId pdid.Id, ev *api.ObjectMissing) error {
	if err := s.sinkOnNode(ctx, nodeId, ev.GetSinkId(), nil); err != nil {
		return err
	}
	sid := mustId(ev.GetSinkId())

	obj, err := s.d.Ent.Object.Query().Where(object.SinkIdEQ(sid.Uuid()), object.ObjectKeyEQ(ev.GetObjectKey())).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return err
	}
	if obj == nil {
		// An abandoned buffered upload the node removed: the attempt fails.
		if len(ev.GetAttemptId()) > 0 {
			at, err := s.d.Ent.Attempt.Query().Where(attempt.IdEQ(mustId(ev.GetAttemptId()).Uuid())).First(ctx)
			if err == nil && at.State == int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED) {
				st := api.AttemptState_ATTEMPT_STATE_FAILED
				reason := ev.GetReason().String()
				_, err = own.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
					Ref: api.AttemptRef_builder{Id: at.Id[:]}.Build(), State: &st, FailureReason: &reason,
					DateFinished: timestamppb.New(s.d.now()), DateUpdatedForce: z.Ptr(true),
				}.Build())

				return err
			}
		}

		return nil
	}

	now := s.d.now()
	switch api.ObjectState(obj.State) {
	case api.ObjectState_OBJECT_STATE_DELETING:
		if _, err := own.Object().Patch(ctx, api.ObjectPatchRequest_builder{
			Ref: api.ObjectRef_builder{Id: obj.Id[:]}.Build(), State: z.Ptr(api.ObjectState_OBJECT_STATE_DELETED),
			DateFinished: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}

		return s.storedBytes(ctx, own, pdid.Id(obj.TenantId), -obj.Size)
	case api.ObjectState_OBJECT_STATE_COMMITTED:
		if _, err := own.Object().Patch(ctx, api.ObjectPatchRequest_builder{
			Ref: api.ObjectRef_builder{Id: obj.Id[:]}.Build(), State: z.Ptr(api.ObjectState_OBJECT_STATE_LOST),
			DateFinished: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}

		return s.storedBytes(ctx, own, pdid.Id(obj.TenantId), -obj.Size)
	}

	return nil
}
