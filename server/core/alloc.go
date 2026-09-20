package core

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
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

// The allocation engine (§12.1): one object per segment slot, a ranked list
// of candidates each with its own attempt and token, idempotent per slot.

// slot is the segment a data time falls in, with the staggered phase of §12.2:
//
//	phase(camera) = (hash(set) + ordinal × D / set_size) mod D
func slotOf(t time.Time, setId []byte, ordinal, setSize int, d time.Duration) time.Time {
	if d <= 0 {
		d = time.Minute
	}
	if setSize <= 0 {
		setSize = 1
	}

	h := xxhash.Sum64(setId)
	phase := time.Duration(h%uint64(d)) + time.Duration(ordinal)*d/time.Duration(setSize)
	phase %= d

	base := t.Add(-phase).Truncate(d)

	return base.Add(phase)
}

// Phase is the segment phase of a member, for producers that cut segments
// the same way the CP expects them (§12.2).
func Phase(setId []byte, ordinal, setSize int, d time.Duration) time.Duration {
	if d <= 0 || setSize <= 0 {
		return 0
	}
	h := xxhash.Sum64(setId)

	return (time.Duration(h%uint64(d)) + time.Duration(ordinal)*d/time.Duration(setSize)) % d
}

// context of one allocation: everything read once per call.
type allocCtx struct {
	set     *api.Set
	members []*api.Source
	bounds  Bounds
	placeV  int64
	address *api.AddressParams
	cluster placement.Cluster
	nodes   map[pdid.Id]*ent.Node
	sinks   map[pdid.Id]*ent.Sink
	caller  string
	now     time.Time
}

func (s Core) allocCtx(ctx context.Context, set *api.Set) (*allocCtx, error) {
	b, place, placeV, address, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}

	members, err := s.members(ctx, set.GetId())
	if err != nil {
		return nil, err
	}

	a := &allocCtx{set: set, members: members, bounds: b, placeV: placeV, address: address, now: s.d.now()}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
			a.caller = host
		}
	}

	if err := s.snapshot(ctx, a, place); err != nil {
		return nil, err
	}

	return a, nil
}

// snapshot reads every sink with its device and node and decides
// eligibility (D7).
func (s Core) snapshot(ctx context.Context, a *allocCtx, place *api.PlacementParams) error {
	sinks, err := s.d.Ent.Sink.Query().
		Where(sink.DateErasedIsNil()).
		WithDevice().
		WithNode().
		All(ctx)
	if err != nil {
		return err
	}

	a.nodes = map[pdid.Id]*ent.Node{}
	a.sinks = map[pdid.Id]*ent.Sink{}
	clamp := maxSinkCapacity(place)
	downAfter := DefaultNodeDownAfter

	for _, v := range sinks {
		n, d := v.Edges.Node, v.Edges.Device
		if n == nil || d == nil {
			continue
		}

		id := pdid.Id(v.Id)
		a.sinks[id] = v
		a.nodes[pdid.Id(n.Id)] = n

		weight := float64(v.Capacity)
		if v.Capacity > clamp {
			weight = float64(clamp)
		}
		eligible := true
		switch {
		case n.State != int32(api.HostState_HOST_STATE_ADOPTED), n.DateErased != nil:
			eligible = false
		case n.DateSeen == nil || a.now.Sub(*n.DateSeen) > downAfter:
			eligible = false
		case d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_QUARANTINED),
			d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_RETIRED),
			d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_DEAD),
			d.DateErased != nil:
			eligible = false
		case v.Attachment != int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED):
			eligible = false
		case !v.AcceptWrites:
			eligible = false
		case v.Pressure == int32(api.Pressure_PRESSURE_CRITICAL):
			eligible = false
		case v.Capabilities != nil && !v.Capabilities.GetXattr():
			eligible = false
		}
		if d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_SUSPECT) {
			weight *= 0.5
		}

		a.cluster.Sinks = append(a.cluster.Sinks, placement.Sink{
			Id:       id,
			Device:   pdid.Id(d.Id),
			Node:     pdid.Id(n.Id),
			Weight:   weight,
			Eligible: eligible,
		})
	}

	return nil
}

// ObjectKey is the path of an object within its sink (§23.2).
func ObjectKey(started time.Time, object, attempt pdid.Id) string {
	t := started.UTC()

	return fmt.Sprintf("objects/%04d/%02d/%02d/%02d/%s.%s", t.Year(), int(t.Month()), t.Day(), t.Hour(), object, attempt)
}

// allocateSlot is one allocation: idempotent per (source, slot).
func (s Core) allocateSlot(ctx context.Context, next api.Server, a *allocCtx, src *api.Source, started time.Time, actor pdid.Id, tenant pdid.Id) (*api.Allocation, error) {
	prof := segmentOf(src, a.set, a.bounds)
	if prof == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "source %s has no max_bitrate; negotiate first", src.GetAlias())
	}
	link := linkOf(a.set, a.bounds)
	epoch := epochOf(a.set)
	duration := time.Duration(prof.GetDurationSeconds()) * time.Second

	// The time window (§10, §12.1).
	expire, del, none := retentionOf(a.set)
	horizon := time.Duration(link.GetAllocationHorizonSeconds()) * time.Second
	if started.After(a.now.Add(horizon + a.bounds.ClockTolerance)) {
		return nil, status.Errorf(codes.InvalidArgument, "date_started %s is further in the future than the horizon allows", started.Format(time.RFC3339))
	}
	if started.Before(a.now.Add(-expire)) {
		return nil, status.Errorf(codes.InvalidArgument, "date_started %s is older than the set's retention", started.Format(time.RFC3339))
	}

	srcId := mustId(src.GetId())
	existing, err := s.d.Ent.Object.Query().
		Where(object.SourceIdEQ(srcId.Uuid()), object.DateStartedEQ(started.UTC())).
		WithSink().
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}

	ttl := horizon + duration + time.Duration(link.GetAbandonTimeoutSeconds())*time.Second + 5*time.Minute
	expires := a.now.Add(ttl)

	var (
		objId    pdid.Id
		attempts []cand
	)
	if existing != nil {
		objId = pdid.Id(existing.Id)
		if existing.State != int32(api.ObjectState_OBJECT_STATE_PENDING) {
			// Already committed, lost, or deleted: the same object, with
			// nothing to upload to.
			return api.Allocation_builder{
				ObjectId:       objId.Bytes(),
				SourceId:       src.GetId(),
				Ordinal:        src.GetOrdinal(),
				DateStarted:    timestamppb.New(started),
				ProfileVersion: a.set.GetProfileVersion(),
				DateExpires:    timestamppb.New(a.now),
				Profile:        prof,
				Link:           link,
				ObjectKey:      existing.ObjectKey,
			}.Build(), nil
		}

		// Unexpired attempts are answered again, with fresh tokens.
		open, err := s.d.Ent.Attempt.Query().
			Where(attempt.ObjectIdEQ(existing.Id), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(a.now)).
			Order(ent.Asc(attempt.FieldRank)).
			All(ctx)
		if err != nil {
			return nil, err
		}
		for _, v := range open {
			attempts = append(attempts, cand{id: pdid.Id(v.Id), sink: pdid.Id(v.SinkId), node: pdid.Id(v.NodeId), expires: v.DateExpires})
		}
	}

	if len(attempts) == 0 {
		// Open attempts per source are capped (§12.1).
		n, err := s.d.Ent.Attempt.Query().
			Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(a.now),
				attempt.HasObjectWith(object.SourceIdEQ(srcId.Uuid()))).
			Count(ctx)
		if err != nil {
			return nil, err
		}
		if n >= a.bounds.MaxOpenAttempts {
			return nil, status.Errorf(codes.ResourceExhausted, "source %s holds %d open attempts", src.GetAlias(), n)
		}

		ranked := placement.Rank(a.cluster, placement.Key{Set: mustId(a.set.GetId()), Epoch: placement.Epoch(started.Unix(), int64(epoch.Seconds())), Version: a.placeV},
			placement.Member{Source: srcId, Ordinal: int(src.GetOrdinal())}, spreadOf(a.set))
		if len(ranked) == 0 {
			return nil, status.Error(codes.Unavailable, "no sink can take the write")
		}
		if len(ranked) > Candidates {
			ranked = ranked[:Candidates]
		}

		if existing == nil {
			objId = pdid.New(DomObject)
			var siteRef *api.SiteRef
			if len(a.set.GetSite().GetId()) > 0 {
				siteRef = api.SiteRef_builder{Id: a.set.GetSite().GetId()}.Build()
			}
			add := api.ObjectAddRequest_builder{
				Id:               objId.Bytes(),
				Tenant:           tenantRef(tenant),
				Site:             siteRef,
				Set:              api.SetRef_builder{Id: a.set.GetId()}.Build(),
				Source:           api.SourceRef_builder{Id: src.GetId()}.Build(),
				State:            api.ObjectState_OBJECT_STATE_PENDING,
				DateStarted:      timestamppb.New(started),
				DateExpired:      timestamppb.New(started.Add(expire)),
				DatesSynced:      true,
				PlacementVersion: a.placeV,
				Epoch:            placement.Epoch(started.Unix(), int64(epoch.Seconds())),
			}
			if !none {
				add.DateDeleted = timestamppb.New(started.Add(del))
			}
			if _, err := next.Object().Add(ctx, add.Build()); err != nil {
				return nil, err
			}
		}

		for i, target := range ranked {
			at := pdid.New(DomAttempt)
			var siteRef *api.SiteRef
			if len(a.set.GetSite().GetId()) > 0 {
				siteRef = api.SiteRef_builder{Id: a.set.GetSite().GetId()}.Build()
			}
			_, err := next.Attempt().Add(ctx, api.AttemptAddRequest_builder{
				Id:          at.Bytes(),
				Tenant:      tenantRef(tenant),
				Site:        siteRef,
				Object:      api.ObjectRef_builder{Id: objId.Bytes()}.Build(),
				Sink:        api.SinkRef_builder{Id: target.Id.Bytes()}.Build(),
				Node:        api.NodeRef_builder{Id: target.Node.Bytes()}.Build(),
				State:       api.AttemptState_ATTEMPT_STATE_ALLOCATED,
				DateExpires: timestamppb.New(expires),
				Rank:        int32(i),
			}.Build())
			if err != nil {
				return nil, err
			}
			attempts = append(attempts, cand{id: at, sink: target.Id, node: target.Node, expires: expires})
		}
	}

	// Tokens and endpoints for every candidate (§33.2).
	maxLen := MaxLength(prof)
	var siteId []byte
	if len(a.set.GetSite().GetId()) > 0 {
		siteId = a.set.GetSite().GetId()
	}
	var expiredMs, deletedMs int64
	expiredMs = started.Add(expire).UnixMilli()
	if !none {
		deletedMs = started.Add(del).UnixMilli()
	}

	var cands []*api.Candidate
	var key string
	for _, at := range attempts {
		atId, sinkId, nodeId := at.id, at.sink, at.node
		k := ObjectKey(started, objId, atId)
		if key == "" {
			key = k
		}

		record := api.ObjectRecord_builder{
			FormatVersion:         1,
			TenantId:              tenant.Bytes(),
			SiteId:                siteId,
			SetId:                 a.set.GetId(),
			SourceId:              src.GetId(),
			ObjectId:              objId.Bytes(),
			AttemptId:             atId.Bytes(),
			DateStartedMs:         started.UnixMilli(),
			Mode:                  link.GetMode(),
			AbandonTimeoutSeconds: link.GetAbandonTimeoutSeconds(),
			DateExpiredMs:         expiredMs,
			DateDeletedMs:         deletedMs,
			PlacementVersion:      a.placeV,
		}.Build()

		tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
			Exp:                   timestamppb.New(at.expires),
			Iat:                   timestamppb.New(a.now),
			Aud:                   nodeId.Bytes(),
			Op:                    api.TokenOp_TOKEN_OP_PUT,
			SinkId:                sinkId.Bytes(),
			ObjectKey:             k,
			AttemptId:             atId.Bytes(),
			Record:                record,
			MaxLength:             maxLen,
			Mode:                  link.GetMode(),
			IdleTimeoutSeconds:    link.GetIdleTimeoutSeconds(),
			AbandonTimeoutSeconds: link.GetAbandonTimeoutSeconds(),
			Actor:                 actor.Bytes(),
			ActorTenant:           tenant.Bytes(),
		}.Build())
		if err != nil {
			return nil, err
		}

		n := a.nodes[nodeId]
		cands = append(cands, api.Candidate_builder{
			AttemptId: atId.Bytes(),
			SinkId:    sinkId.Bytes(),
			NodeId:    nodeId.Bytes(),
			Endpoints: s.endpoints(n, a.caller, a.address),
			Token:     tok,
		}.Build())
	}

	// The size hint for a live upload: the expected rate × duration × 1.2,
	// capped at max_length (§12.2).
	rate := src.GetObserved().GetExpected()
	if rate <= 0 {
		rate = prof.GetMaxBitrate()
	}
	hint := int64(math.Ceil(float64(rate) / 8 * float64(prof.GetDurationSeconds()) * SizeHintFactor))
	if hint > maxLen {
		hint = maxLen
	}

	exp := timestamppb.New(a.now)
	if len(attempts) > 0 {
		exp = timestamppb.New(attempts[0].expires)
	}

	return api.Allocation_builder{
		ObjectId:       objId.Bytes(),
		SourceId:       src.GetId(),
		Ordinal:        src.GetOrdinal(),
		DateStarted:    timestamppb.New(started),
		ProfileVersion: a.set.GetProfileVersion(),
		Candidates:     cands,
		DateExpires:    exp,
		MaxLength:      maxLen,
		SizeHint:       hint,
		ObjectKey:      key,
		Profile:        prof,
		Link:           link,
	}.Build(), nil
}

// cand is one candidate as allocation signs it.
type cand struct {
	id, sink, node pdid.Id
	expires        time.Time
}

// Allocate answers allocations for every member of the set up to the
// horizon (§12.1).
func (s coreSet) Allocate(ctx context.Context, req *api.SetAllocateRequest) (*api.SetAllocateResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}

	set, err := s.SetServiceServer.Get(ctx, api.SetGetRequest_builder{
		Ref:    req.GetRef(),
	}.Build())
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
	link := linkOf(set, a.bounds)
	horizon := time.Duration(link.GetAllocationHorizonSeconds()) * time.Second
	if h := time.Duration(req.GetHorizonSeconds()) * time.Second; h > 0 && h < horizon {
		horizon = h
	}
	now := a.now
	if req.GetNow() != nil {
		now = req.GetNow().AsTime()
		if d := now.Sub(a.now); d > a.bounds.ClockTolerance || d < -a.bounds.ClockTolerance {
			return nil, status.Errorf(codes.InvalidArgument, "now: the producer's clock is off by %s", d)
		}
	}

	var out []*api.Allocation
	err = s.tx(ctx, func(next api.Server) error {
		for _, m := range a.members {
			prof := segmentOf(m, set, a.bounds)
			if prof == nil {
				continue
			}
			d := time.Duration(prof.GetDurationSeconds()) * time.Second
			start := slotOf(now, set.GetId(), int(m.GetOrdinal()), len(a.members), d)
			for !start.After(now.Add(horizon)) {
				al, err := s.allocateSlot(ctx, next, a, m, start, f.Actor, f.Tenant)
				if err != nil {
					return err
				}
				out = append(out, al)
				start = start.Add(d)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return api.SetAllocateResponse_builder{ProfileVersion: set.GetProfileVersion(), Allocations: out}.Build(), nil
}

// endpoints runs the address resolver for a node (§34.10).
func (s Core) endpoints(n *ent.Node, caller string, p *api.AddressParams) []*api.Endpoint {
	if n == nil {
		return nil
	}
	r := s.d.Resolve
	if r == nil {
		r = Advertised{}
	}

	return r.Endpoints(NodeAddresses{
		Id:          pdid.Id(n.Id),
		Alias:       n.Alias,
		Interfaces:  n.Interfaces,
		DataAddress: n.DataAddress,
		Dev:         s.d.Dev,
	}, caller, p)
}
