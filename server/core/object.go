package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/object"
	"github.com/lesomnus/shale/internal/placement"
)

type coreObject struct {
	Core
	api.ObjectServiceServer
}

func (s Core) Object() api.ObjectServiceServer {
	return coreObject{s, s.Next().Object()}
}

// setOf reads a set through the wall by id.
func (s Core) setOf(ctx context.Context, id []byte) (*api.Set, error) {
	return s.Next().Set().Get(ctx, api.SetGetRequest_builder{
		Ref: api.SetRef_builder{Id: id}.Build(),
	}.Build())
}

// Allocate is one allocation for one segment of one source (§12.1).
func (s coreObject) Allocate(ctx context.Context, req *api.ObjectAllocateRequest) (*api.Allocation, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{
		Ref: req.GetSource(),
	}.Build())
	if err != nil {
		return nil, err
	}
	set, err := s.setOf(ctx, src.GetSet().GetId())
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) == DomProducer {
		if err := s.producerOwns(ctx, f.Actor, set); err != nil {
			return nil, err
		}
	}

	a, err := s.allocCtx(ctx, set)
	if err != nil {
		return nil, err
	}
	prof := segmentOf(src, set, a.bounds)
	if prof == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "source %s has no max_bitrate; negotiate first", src.GetAlias())
	}

	t := a.now
	if req.GetDateStarted() != nil {
		t = req.GetDateStarted().AsTime()
	}
	d := time.Duration(prof.GetDurationSeconds()) * time.Second
	start := slotOf(t, set.GetId(), int(src.GetOrdinal()), len(a.members), d)

	var out *api.Allocation
	err = s.tx(ctx, func(next api.Server) error {
		v, err := s.allocateSlot(ctx, next, a, src, start, f.Actor, f.Tenant)
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// objectRow reads an object through the wall.
func (s Core) objectRow(ctx context.Context, ref *api.ObjectRef) (*api.Object, error) {
	return s.Next().Object().Get(ctx, api.ObjectGetRequest_builder{
		Ref: ref,
	}.Build())
}

// Reallocate is the next candidate after the ones an allocation carried
// (§13): the ranking again, skipping every sink that already has an attempt.
func (s coreObject) Reallocate(ctx context.Context, req *api.ObjectReallocateRequest) (*api.Allocation, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	obj, err := s.objectRow(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	if obj.GetState() != api.ObjectState_OBJECT_STATE_PENDING {
		return nil, failed("object is %s, not pending", obj.GetState())
	}

	src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{
		Ref: api.SourceRef_builder{Id: obj.GetSource().GetId()}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	set, err := s.setOf(ctx, obj.GetSet().GetId())
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) == DomProducer {
		if err := s.producerOwns(ctx, f.Actor, set); err != nil {
			return nil, err
		}
	}

	a, err := s.allocCtx(ctx, set)
	if err != nil {
		return nil, err
	}

	objId := mustId(obj.GetId())
	tried, err := s.d.Ent.Attempt.Query().Where(attempt.ObjectIdEQ(objId.Uuid())).All(ctx)
	if err != nil {
		return nil, err
	}
	used := map[pdid.Id]bool{}
	for _, t := range tried {
		used[pdid.Id(t.SinkId)] = true
	}

	epoch := epochOf(set)
	started := obj.GetDateStarted().AsTime()
	ranked := placement.Rank(a.cluster, placement.Key{Set: mustId(set.GetId()), Epoch: placement.Epoch(started.Unix(), int64(epoch.Seconds())), Version: a.placeV},
		placement.Member{Source: mustId(src.GetId()), Ordinal: int(src.GetOrdinal())}, spreadOf(set))
	var next *placement.Sink
	for i := range ranked {
		if !used[ranked[i].Id] {
			next = &ranked[i]
			break
		}
	}
	if next == nil {
		return nil, status.Error(codes.Unavailable, "every eligible sink has been tried")
	}

	// Mark the open attempts abandoned by the producer's own account: it
	// asked to move on.
	link := linkOf(set, a.bounds)
	prof := segmentOf(src, set, a.bounds)
	ttl := time.Duration(link.GetAllocationHorizonSeconds())*time.Second + time.Duration(prof.GetDurationSeconds())*time.Second + time.Duration(link.GetAbandonTimeoutSeconds())*time.Second + 5*time.Minute

	var siteRef *api.SiteRef
	if len(set.GetSite().GetId()) > 0 {
		siteRef = api.SiteRef_builder{Id: set.GetSite().GetId()}.Build()
	}
	at := pdid.New(DomAttempt)
	err = s.tx(ctx, func(nx api.Server) error {
		_, err := nx.Attempt().Add(ctx, api.AttemptAddRequest_builder{
			Id:          at.Bytes(),
			Tenant:      tenantRef(f.Tenant),
			Site:        siteRef,
			Object:      api.ObjectRef_builder{Id: obj.GetId()}.Build(),
			Sink:        api.SinkRef_builder{Id: next.Id.Bytes()}.Build(),
			Node:        api.NodeRef_builder{Id: next.Node.Bytes()}.Build(),
			State:       api.AttemptState_ATTEMPT_STATE_ALLOCATED,
			DateExpires: timestamppb.New(a.now.Add(ttl)),
			Rank:        int32(len(tried)),
		}.Build())

		return err
	})
	if err != nil {
		return nil, err
	}

	// Answer through the ordinary path, which signs tokens for every open
	// attempt including the new one.
	var out *api.Allocation
	err = s.tx(ctx, func(nx api.Server) error {
		v, err := s.allocateSlot(ctx, nx, a, src, started, f.Actor, f.Tenant)
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Renew is a fresh token for an attempt still in progress on the same
// target (§12.1).
func (s coreObject) Renew(ctx context.Context, req *api.ObjectRenewRequest) (*api.Allocation, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	obj, err := s.objectRow(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	at, err := s.Next().Attempt().Get(ctx, api.AttemptGetRequest_builder{
		Ref: req.GetAttempt(),
	}.Build())
	if err != nil {
		return nil, err
	}
	if string(at.GetObject().GetId()) != string(obj.GetId()) {
		return nil, invalid("attempt", "not an attempt of this object")
	}
	if at.GetState() != api.AttemptState_ATTEMPT_STATE_ALLOCATED {
		return nil, failed("attempt is %s", at.GetState())
	}

	src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{
		Ref: api.SourceRef_builder{Id: obj.GetSource().GetId()}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	set, err := s.setOf(ctx, obj.GetSet().GetId())
	if err != nil {
		return nil, err
	}
	a, err := s.allocCtx(ctx, set)
	if err != nil {
		return nil, err
	}
	link := linkOf(set, a.bounds)
	prof := segmentOf(src, set, a.bounds)
	ttl := time.Duration(link.GetAllocationHorizonSeconds())*time.Second + time.Duration(prof.GetDurationSeconds())*time.Second + time.Duration(link.GetAbandonTimeoutSeconds())*time.Second + 5*time.Minute

	exp := timestamppb.New(a.now.Add(ttl))
	if _, err := s.Next().Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
		Ref:         api.AttemptRef_builder{Id: at.GetId()}.Build(),
		DateExpires: exp,
		DateUpdated: at.GetDateUpdated(),
	}.Build()); err != nil {
		return nil, err
	}

	var out *api.Allocation
	err = s.tx(ctx, func(nx api.Server) error {
		v, err := s.allocateSlot(ctx, nx, a, src, obj.GetDateStarted().AsTime(), f.Actor, f.Tenant)
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// ReportAttempt marks one attempt failed (§13). The object stays PENDING.
func (s coreObject) ReportAttempt(ctx context.Context, req *api.ObjectReportAttemptRequest) (*api.Attempt, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	obj, err := s.objectRow(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	at, err := s.Next().Attempt().Get(ctx, api.AttemptGetRequest_builder{
		Ref: req.GetAttempt(),
	}.Build())
	if err != nil {
		return nil, err
	}
	if string(at.GetObject().GetId()) != string(obj.GetId()) {
		return nil, invalid("attempt", "not an attempt of this object")
	}
	if at.GetState() != api.AttemptState_ATTEMPT_STATE_ALLOCATED {
		return at, nil
	}

	st := api.AttemptState_ATTEMPT_STATE_FAILED
	reason := req.GetFailureReason()
	if reason == "" {
		reason = "reported"
	}

	return s.Next().Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
		Ref:           api.AttemptRef_builder{Id: at.GetId()}.Build(),
		State:         &st,
		FailureReason: &reason,
		DateFinished:  timestamppb.New(s.d.now()),
		DateUpdated:   at.GetDateUpdated(),
	}.Build())
}

// ReportFailure is the producer giving up on an object: it becomes LOST
// (§13). A late ObjectStored still brings it back (§14).
func (s coreObject) ReportFailure(ctx context.Context, req *api.ObjectReportFailureRequest) (*api.Object, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	obj, err := s.objectRow(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	if obj.GetState() != api.ObjectState_OBJECT_STATE_PENDING {
		return obj, nil
	}

	now := s.d.now()
	var out *api.Object
	err = s.tx(ctx, func(nx api.Server) error {
		open, err := s.d.Ent.Attempt.Query().
			Where(attempt.ObjectIdEQ(mustId(obj.GetId()).Uuid()), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED))).
			All(ctx)
		if err != nil {
			return err
		}
		for _, a := range open {
			st := api.AttemptState_ATTEMPT_STATE_FAILED
			reason := req.GetReason()
			if _, err := nx.Attempt().Patch(ctx, api.AttemptPatchRequest_builder{
				Ref:              api.AttemptRef_builder{Id: a.Id[:]}.Build(),
				State:            &st,
				FailureReason:    &reason,
				DateFinished:     timestamppb.New(now),
				DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return err
			}
		}

		st := api.ObjectState_OBJECT_STATE_LOST
		v, err := nx.Object().Patch(ctx, api.ObjectPatchRequest_builder{
			Ref:          api.ObjectRef_builder{Id: obj.GetId()}.Build(),
			State:        &st,
			DateFinished: timestamppb.New(now),
			DateUpdated:  obj.GetDateUpdated(),
		}.Build())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Reschedule changes an object's dates, one or in bulk (§20.3). A delete-now
// sets both to now; the node hears about it through its control API.
func (s coreObject) Reschedule(ctx context.Context, req *api.ObjectRescheduleRequest) (*api.ObjectRescheduleResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetReason()) == "" {
		return nil, invalid("reason", "a reason is required and audited")
	}
	now := s.d.now()

	var expired, deleted *time.Time
	if req.GetDeleteNow() {
		expired, deleted = &now, &now
	} else {
		if req.GetDateExpired() != nil {
			t := req.GetDateExpired().AsTime()
			expired = &t
		}
		if req.GetDateDeleted() != nil {
			t := req.GetDateDeleted().AsTime()
			deleted = &t
		}
	}
	if expired == nil && deleted == nil {
		return nil, invalid("dates", "nothing to change")
	}

	var rows []*ent.Object
	switch {
	case req.GetRef() != nil:
		obj, err := s.objectRow(ctx, req.GetRef())
		if err != nil {
			return nil, err
		}
		r, err := s.d.Ent.Object.Get(ctx, mustId(obj.GetId()).Uuid())
		if err != nil {
			return nil, err
		}
		rows = []*ent.Object{r}
	case req.GetSet() != nil || req.GetSource() != nil:
		if req.GetFrom() == nil || req.GetTo() == nil {
			return nil, invalid("from", "a bulk reschedule needs a time range")
		}
		q := s.d.Ent.Object.Query().Where(
			object.TenantIdEQ(f.Tenant.Uuid()),
			object.DateStartedLT(req.GetTo().AsTime()),
			object.Or(object.DateEndedGT(req.GetFrom().AsTime()), object.DateEndedIsNil()),
		)
		if req.GetSource() != nil {
			src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{Ref: req.GetSource()}.Build())
			if err != nil {
				return nil, err
			}
			q = q.Where(object.SourceIdEQ(mustId(src.GetId()).Uuid()))
		} else {
			set, err := s.setOf(ctx, refId(req.GetSet()))
			if err != nil {
				if req.GetSet().GetId() == nil {
					set, err = s.Next().Set().Get(ctx, api.SetGetRequest_builder{Ref: req.GetSet()}.Build())
				}
				if err != nil {
					return nil, err
				}
			}
			q = q.Where(object.SetIdEQ(mustId(set.GetId()).Uuid()))
		}
		rows, err = q.Limit(100000).All(ctx)
		if err != nil {
			return nil, err
		}
	default:
		return nil, invalid("ref", "name an object, a set, or a source")
	}

	s.d.log().Info("reschedule", "actor", f.Actor.String(), "objects", len(rows), "reason", req.GetReason(), "delete_now", req.GetDeleteNow())

	var changed int64
	err = s.tx(ctx, func(nx api.Server) error {
		for _, r := range rows {
			if r.State == int32(api.ObjectState_OBJECT_STATE_DELETING) || r.State == int32(api.ObjectState_OBJECT_STATE_DELETED) {
				if req.GetRef() != nil {
					return failed("object is already %s", api.ObjectState(r.State))
				}
				continue
			}
			p := api.ObjectPatchRequest_builder{
				Ref:              api.ObjectRef_builder{Id: r.Id[:]}.Build(),
				DatesSynced:      z.Ptr(false),
				DateUpdatedForce: z.Ptr(true),
			}
			e := r.DateExpired
			if expired != nil {
				e = *expired
				p.DateExpired = timestamppb.New(e)
			}
			if deleted != nil {
				if deleted.Before(e) {
					e = *deleted
					p.DateExpired = timestamppb.New(e)
				}
				p.DateDeleted = timestamppb.New(*deleted)
			} else if r.DateDeleted != nil && r.DateDeleted.Before(e) {
				// Keeping date_expired ≤ date_deleted.
				p.DateDeleted = timestamppb.New(e)
			}
			if _, err := nx.Object().Patch(ctx, p.Build()); err != nil {
				return err
			}
			changed++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return api.ObjectRescheduleResponse_builder{Changed: changed}.Build(), nil
}

func refId(r *api.SetRef) []byte {
	if r == nil {
		return nil
	}

	return r.GetId()
}

// Timeline answers objects and gaps over a time range with read tokens,
// paged (§17, §19).
func (s coreObject) Timeline(ctx context.Context, req *api.ObjectTimelineRequest) (*api.ObjectTimelineResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, invalid("from", "a time range is required")
	}
	from, to := req.GetFrom().AsTime(), req.GetTo().AsTime()
	if !to.After(from) {
		return nil, invalid("to", "must be after from")
	}

	b, _, _, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}

	// Which sources, through the wall.
	var (
		members []*api.Source
		set     *api.Set
	)
	switch {
	case req.GetSource() != nil:
		src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{Ref: req.GetSource()}.Build())
		if err != nil {
			return nil, err
		}
		members = []*api.Source{src}
		set, err = s.setOf(ctx, src.GetSet().GetId())
		if err != nil {
			return nil, err
		}
	case req.GetSet() != nil:
		set, err = s.Next().Set().Get(ctx, api.SetGetRequest_builder{Ref: req.GetSet()}.Build())
		if err != nil {
			return nil, err
		}
		members, err = s.members(ctx, set.GetId())
		if err != nil {
			return nil, err
		}
	default:
		return nil, invalid("set", "name a set or a source")
	}

	size := int(req.GetSize())
	if size <= 0 || size > 1000 {
		size = 1000
	}

	byId := map[pdid.Id]*api.Source{}
	for _, m := range members {
		byId[mustId(m.GetId())] = m
	}

	q := s.d.Ent.Object.Query().Where(
		object.TenantIdEQ(f.Tenant.Uuid()),
		object.DateStartedLT(to),
		object.Or(object.DateEndedGT(from), object.DateEndedIsNil()),
	)
	if req.GetSource() != nil {
		q = q.Where(object.SourceIdEQ(mustId(members[0].GetId()).Uuid()))
	} else {
		q = q.Where(object.SetIdEQ(mustId(set.GetId()).Uuid()))
	}
	var ends map[pdid.Id]time.Time
	if req.GetAfter() != "" {
		t, id, e, err := decodeCursor(req.GetAfter())
		if err != nil {
			return nil, invalid("after", err.Error())
		}
		ends = e
		q = q.Where(object.Or(object.DateStartedGT(t), object.And(object.DateStartedEQ(t), object.IdGT(id.Uuid()))))
	}
	rows, err := q.Order(ent.Asc(object.FieldDateStarted), ent.Asc(object.FieldId)).Limit(size + 1).WithSink(func(sq *ent.SinkQuery) { sq.WithNode() }).All(ctx)
	if err != nil {
		return nil, err
	}

	more := len(rows) > size
	if more {
		rows = rows[:size]
	}

	// Open attempts of pending objects, for IN_PROGRESS gaps.
	now := s.d.now()
	pending := map[pdid.Id]bool{}
	for _, r := range rows {
		if r.State == int32(api.ObjectState_OBJECT_STATE_PENDING) {
			pending[pdid.Id(r.Id)] = true
		}
	}
	inProgress := map[pdid.Id]bool{}
	if len(pending) > 0 {
		var pids []pdid.Id
		for id := range pending {
			pids = append(pids, id)
		}
		open, err := s.d.Ent.Attempt.Query().
			Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(now), attempt.HasObjectWith(object.IdIn(uuidsOf(pids, pdid.Id.Uuid)...))).
			All(ctx)
		if err != nil {
			return nil, err
		}
		for _, a := range open {
			inProgress[pdid.Id(a.ObjectId)] = true
		}
	}

	expire, del, none := retentionOf(set)
	_ = expire

	// Build per source.
	out := map[pdid.Id]*api.TimelineSource{}
	cursor := map[pdid.Id]time.Time{}
	for _, m := range members {
		id := mustId(m.GetId())
		out[id] = api.TimelineSource_builder{SourceId: m.GetId(), Ordinal: m.GetOrdinal()}.Build()
		// A continuation page carries on from where each source's last
		// listed object ended, so a gap between two pages is told once.
		if e, ok := ends[id]; ok {
			cursor[id] = e
		} else {
			cursor[id] = from
		}
	}

	for _, r := range rows {
		srcId := pdid.Id(r.SourceId)
		ts, ok := out[srcId]
		if !ok {
			continue
		}
		src := byId[srcId]
		prof := segmentOf(src, set, b)

		start := r.DateStarted
		end := start
		estimated := false
		if r.DateEnded != nil {
			end = *r.DateEnded
		} else {
			estimated = true
			rate := src.GetObserved().GetExpected()
			if rate <= 0 && prof != nil {
				rate = prof.GetMaxBitrate()
			}
			if r.Size > 0 && rate > 0 {
				end = start.Add(time.Duration(float64(r.Size) * 8 / float64(rate) * float64(time.Second)))
			} else if prof != nil {
				end = start.Add(time.Duration(prof.GetDurationSeconds()) * time.Second)
			}
		}

		// The gap before this object.
		if c := cursor[srcId]; start.After(c) {
			ts.SetGaps(append(ts.GetGaps(), gap(c, start, api.GapReason_GAP_REASON_NOT_RECEIVED)))
		}

		state := api.ReadState_READ_STATE_UNSPECIFIED
		var reason api.GapReason
		switch api.ObjectState(r.State) {
		case api.ObjectState_OBJECT_STATE_PENDING:
			if inProgress[pdid.Id(r.Id)] {
				reason = api.GapReason_GAP_REASON_IN_PROGRESS
			} else {
				reason = api.GapReason_GAP_REASON_NOT_RECEIVED
			}
		case api.ObjectState_OBJECT_STATE_LOST:
			reason = api.GapReason_GAP_REASON_LOST
		case api.ObjectState_OBJECT_STATE_DELETING, api.ObjectState_OBJECT_STATE_DELETED:
			reason = api.GapReason_GAP_REASON_DELETED
		case api.ObjectState_OBJECT_STATE_COMMITTED:
			if r.DateDeleted != nil && !r.DateDeleted.After(now) {
				reason = api.GapReason_GAP_REASON_DELETED
				break
			}
			state = api.ReadState_READ_STATE_AVAILABLE
			if sk := r.Edges.Sink; sk == nil || sk.Edges.Node == nil || sk.Edges.Node.DateSeen == nil || now.Sub(*sk.Edges.Node.DateSeen) > DefaultNodeDownAfter || sk.Attachment != int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED) {
				state = api.ReadState_READ_STATE_UNAVAILABLE
				reason = api.GapReason_GAP_REASON_UNAVAILABLE
			}
		}

		if state == api.ReadState_READ_STATE_AVAILABLE {
			to := api.TimelineObject_builder{
				ObjectId:       r.Id[:],
				DateStarted:    timestamppb.New(start),
				DateEnded:      timestamppb.New(end),
				EndedEstimated: estimated,
				Size:           r.Size,
				State:          state,
				Incomplete:     r.Incomplete,
				SinkId:         r.SinkId[:],
				ObjectKey:      r.ObjectKey,
			}
			exp := now.Add(b.ReadTokenTTL)
			tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
				Exp:         timestamppb.New(exp),
				Iat:         timestamppb.New(now),
				Aud:         r.Edges.Sink.Edges.Node.Id[:],
				Op:          api.TokenOp_TOKEN_OP_GET,
				SinkId:      r.SinkId[:],
				ObjectKey:   r.ObjectKey,
				Actor:       f.Actor.Bytes(),
				ActorTenant: f.Tenant.Bytes(),
			}.Build())
			if err != nil {
				return nil, err
			}
			to.Token = tok
			to.DateTokenExpires = timestamppb.New(exp)
			eps := s.endpoints(r.Edges.Sink.Edges.Node, callerOf(ctx), address)
			to.Endpoints = eps
			if len(eps) > 0 {
				to.Url = fmt.Sprintf("%s://%s:%d/%s?token=%s", eps[0].GetScheme(), eps[0].GetHost(), eps[0].GetPort(), r.ObjectKey, tok)
			}
			ts.SetObjects(append(ts.GetObjects(), to.Build()))
		} else {
			ts.SetGaps(append(ts.GetGaps(), gap(start, end, reason)))
		}
		if end.After(cursor[srcId]) {
			cursor[srcId] = end
		}
	}

	next := ""
	if more {
		last := rows[len(rows)-1]
		next = encodeCursor(last.DateStarted, pdid.Id(last.Id), cursor)
	}

	// The tail of the window, when this is the last page: spans past what
	// retention keeps answer from policy (§20.4).
	if next == "" {
		for id, ts := range out {
			c := cursor[id]
			if to.After(c) {
				reason := api.GapReason_GAP_REASON_NOT_RECEIVED
				if !none && to.Before(now.Add(-del)) {
					reason = api.GapReason_GAP_REASON_DELETED
				}
				ts.SetGaps(append(ts.GetGaps(), gap(c, to, reason)))
			}
		}
	}

	var sources []*api.TimelineSource
	for _, m := range members {
		sources = append(sources, out[mustId(m.GetId())])
	}

	return api.ObjectTimelineResponse_builder{Sources: sources, Next: next}.Build(), nil
}

func gap(from, to time.Time, reason api.GapReason) *api.TimelineGap {
	return api.TimelineGap_builder{From: timestamppb.New(from), To: timestamppb.New(to), Reason: reason}.Build()
}

// A cursor names the last row read and, per source, where its gaps left
// off, so a page carries on without repeating or skipping a span.
func encodeCursor(t time.Time, id pdid.Id, ends map[pdid.Id]time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%s", t.UnixNano(), id)
	for k, v := range ends {
		fmt.Fprintf(&b, "|%s=%d", k, v.UnixNano())
	}

	return base64.RawURLEncoding.EncodeToString([]byte(b.String()))
}

func decodeCursor(s string) (time.Time, pdid.Id, map[pdid.Id]time.Time, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, pdid.Nil, nil, err
	}
	parts := strings.Split(string(b), "|")
	if len(parts) < 2 {
		return time.Time{}, pdid.Nil, nil, fmt.Errorf("malformed cursor")
	}
	var ns int64
	if _, err := fmt.Sscanf(parts[0], "%d", &ns); err != nil {
		return time.Time{}, pdid.Nil, nil, err
	}
	id, err := pdid.Parse(parts[1])
	if err != nil {
		return time.Time{}, pdid.Nil, nil, err
	}
	ends := map[pdid.Id]time.Time{}
	for _, p := range parts[2:] {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		sid, err := pdid.Parse(k)
		if err != nil {
			continue
		}
		var e int64
		if _, err := fmt.Sscanf(v, "%d", &e); err != nil {
			continue
		}
		ends[sid] = time.Unix(0, e).UTC()
	}

	return time.Unix(0, ns).UTC(), id, ends, nil
}

func callerOf(ctx context.Context) string {
	if f, ok := frame.From(ctx); ok {
		_ = f
	}
	if p, ok := peerAddr(ctx); ok {
		return p
	}

	return ""
}
