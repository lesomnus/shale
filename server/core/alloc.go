package core

import (
	"context"
	"fmt"
	"math"
	"net"
	"strings"
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
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/sink"
	"github.com/lesomnus/shale/internal/placement"
)

// The allocation engine (§12.1): one lamina per segment, a ranked list of
// candidates each with its own attempt and token, idempotent per segment.
// A slot usually holds one segment; a camera that stops and comes back
// within the slot starts another (§15), which gets a lamina of its own.

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

	phase := Phase(setId, ordinal, setSize, d)
	base := t.Add(-phase).Truncate(d)

	return base.Add(phase)
}

// segmentAfter says whether a segment that begins at `started` comes after
// a stored lamina of its slot: after the lamina's end, or, for a lamina
// with no end (incomplete, lost), more than the slack after its start.
func segmentAfter(o *ent.Lamina, started time.Time, slack time.Duration) bool {
	if o.DateEnded != nil {
		return !started.Before(*o.DateEnded)
	}

	return started.After(o.DateStarted.Add(slack))
}

// Phase is the segment phase of a member, for producers that cut segments
// the same way the CP expects them (§12.2).
func Phase(setId []byte, ordinal, setSize int, d time.Duration) time.Duration {
	if d <= 0 || setSize <= 0 {
		return 0
	}
	h := xxhash.Sum64(setId)
	phase := (time.Duration(h%uint64(d)) + time.Duration(ordinal)*d/time.Duration(setSize)) % d

	// Whole milliseconds: every date Shale stores is one (§23.1), and a
	// database keeps microseconds at best.
	return phase.Truncate(time.Millisecond)
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
	// why says, per sink, what keeps it out of placement.
	why    map[pdid.Id]string
	caller string
	now    time.Time
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
	sinks, err := s.ent(ctx).Sink.Query().
		Where(sink.DateErasedIsNil()).
		WithDevice().
		WithNode().
		All(ctx)
	if err != nil {
		return err
	}

	a.nodes = map[pdid.Id]*ent.Node{}
	a.sinks = map[pdid.Id]*ent.Sink{}
	a.why = map[pdid.Id]string{}
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
			eligible, a.why[id] = false, "its node is not adopted"
		case n.DateSeen == nil || a.now.Sub(*n.DateSeen) > downAfter:
			eligible, a.why[id] = false, "its node is down"
		case d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_QUARANTINED),
			d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_RETIRED),
			d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_DEAD),
			d.DateErased != nil:
			eligible, a.why[id] = false, "its device is "+api.DeviceHealth(d.Health).String()
		case v.Attachment != int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED):
			eligible, a.why[id] = false, "it is "+api.SinkAttachment(v.Attachment).String()
		case !v.AcceptWrites:
			eligible, a.why[id] = false, "it takes no writes"
		case v.Pressure == int32(api.Pressure_PRESSURE_CRITICAL):
			eligible, a.why[id] = false, "it is at CRITICAL pressure"
		case v.Capabilities != nil && !v.Capabilities.GetXattr():
			eligible, a.why[id] = false, "its filesystem has no xattrs"
		}
		if d.Health == int32(api.DeviceHealth_DEVICE_HEALTH_SUSPECT) {
			weight *= 0.5
		}
		// A new device ramps in at a quarter weight for its first month when
		// the policy says so (D6), so a fresh HDD does not take a burst of
		// the cluster's writes at once.
		if place.GetNewDeviceRamp() && a.now.Sub(d.DateCreated) < 30*24*time.Hour {
			weight *= 0.25
		}
		// A device on probation after a release runs at half weight (§27).
		if q := d.Quarantine; q != nil && q.GetDateProbationEnds() != nil && a.now.Before(q.GetDateProbationEnds().AsTime()) {
			weight *= 0.5
		}
		// The node's own score, from what producers reported (§27).
		switch ns := nodeScore(n, a.now); {
		case ns >= ScoreQuarantine:
			eligible, a.why[id] = false, fmt.Sprintf("its node's failure score is %.1f", ns)
		case ns >= ScoreSuspect:
			weight *= 0.5
		}
		// The capacity forecast (§11.1): a sink that would fill before GC
		// could make room sits this epoch out.
		if eligible && !s.forecastOk(ctx, a, v, place) {
			eligible, a.why[id] = false, forecastWhy
		}

		a.cluster.Sinks = append(a.cluster.Sinks, placement.Sink{
			Id:       id,
			Device:   pdid.Id(d.Id),
			Node:     pdid.Id(n.Id),
			Weight:   weight,
			Eligible: eligible,
		})
	}

	// The forecast keeps a sink that would fill this epoch from taking
	// more, so the others have room to spread over (§11.1). When it would
	// keep every sink out, there is nothing to spread over and stopping the
	// writes protects no footage: those sinks take writes anyway, and
	// pressure decides from there.
	if readmitForecast(a.cluster.Sinks, a.why) && s.d.warnReadmit(a.now) {
		s.d.log().Warn("the forecast excludes every sink; writing anyway, pressure decides", "sinks", len(a.cluster.Sinks))
	}

	return nil
}

// forecastWhy is the reason a sink the forecast excluded carries.
const forecastWhy = "the forecast says it would fill this epoch"

// readmitForecast makes the sinks the forecast alone excluded eligible
// again when no sink is eligible at all, and says whether it did.
func readmitForecast(sinks []placement.Sink, why map[pdid.Id]string) bool {
	var byForecast []int
	for i, v := range sinks {
		if v.Eligible {
			return false
		}
		if why[v.Id] == forecastWhy {
			byForecast = append(byForecast, i)
		}
	}
	if len(byForecast) == 0 {
		return false
	}
	for _, i := range byForecast {
		sinks[i].Eligible = true
		delete(why, sinks[i].Id)
	}

	return true
}

// forecastOk is the capacity forecast of §11.1 for one sink and the
// current epoch, decided once per epoch: what the sink took in over the
// last epoch is what it is about to take in, and what it can reclaim is
// its free space plus what expires before the epoch ends.
func (s Core) forecastOk(ctx context.Context, a *allocCtx, v *ent.Sink, place *api.PlacementParams) bool {
	epoch := epochOf(a.set)
	if epoch <= 0 {
		epoch = DefaultEpoch
	}
	start := a.now.Truncate(epoch)
	key := forecastKey{sink: pdid.Id(v.Id), epoch: start.Unix()}
	s.d.forecastMu.Lock()
	if s.d.forecast == nil {
		s.d.forecast = map[forecastKey]bool{}
	}
	ok, seen := s.d.forecast[key]
	s.d.forecastMu.Unlock()
	if seen {
		return ok
	}

	incoming, err := s.ent(ctx).Lamina.Query().
		Where(lamina.SinkIdEQ(v.Id), lamina.DateCommittedGT(a.now.Add(-epoch))).
		Aggregate(ent.Sum(lamina.FieldSize)).
		Int(ctx)
	if err != nil {
		return true
	}
	expiring, err := s.ent(ctx).Lamina.Query().
		Where(lamina.SinkIdEQ(v.Id), lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_COMMITTED)), lamina.DateExpiredLT(start.Add(epoch))).
		Aggregate(ent.Sum(lamina.FieldSize)).
		Int(ctx)
	if err != nil {
		return true
	}
	reclaimable := float64(v.Free + int64(expiring))
	ok = reclaimable >= float64(incoming)*forecastMargin(place)
	if !ok {
		s.d.log().Warn("sink sits this epoch out: it would fill before GC could make room", "sink", v.Alias,
			"incoming", incoming, "reclaimable", int64(reclaimable), "margin", forecastMargin(place))
	}
	s.d.forecastMu.Lock()
	// The map is small: one entry per sink per epoch, pruned as epochs pass.
	for k := range s.d.forecast {
		if k.epoch < start.Unix() {
			delete(s.d.forecast, k)
		}
	}
	s.d.forecast[key] = ok
	s.d.forecastMu.Unlock()

	return ok
}

// LaminaKey is the path of a lamina within its sink (§23.2).
func LaminaKey(started time.Time, lamina, attempt pdid.Id) string {
	t := started.UTC()

	return fmt.Sprintf("laminae/%04d/%02d/%02d/%02d/%s.%s", t.Year(), int(t.Month()), t.Day(), t.Hour(), lamina, attempt)
}

// allocateSlot is one allocation: idempotent per (source, segment). `after`
// is the lamina of the segment the producer says ended before this one,
// or Nil.
func (s Core) allocateSlot(ctx context.Context, next api.Server, a *allocCtx, src *api.Source, started time.Time, after pdid.Id, actor pdid.Id, tenant pdid.Id) (*api.Allocation, error) {
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

	// The slot's laminae: date_started is the slot at allocation and the
	// data time once stored, so the lookup is by the slot's span. A slot
	// holds one lamina per segment (§12.1, §15): `started` belongs to the
	// last lamina that began no later than it, with a keyframe interval and
	// an encoder burst of slack, since the slot's first lamina is allocated
	// at the slot's start and stored at its first keyframe.
	srcId := mustId(src.GetId())
	slotStart := slotOf(started, a.set.GetId(), int(src.GetOrdinal()), len(a.members), duration)
	inSlot, err := s.ent(ctx).Lamina.Query().
		Where(lamina.SourceIdEQ(srcId.Uuid()), lamina.DateStartedGTE(slotStart.UTC()), lamina.DateStartedLT(slotStart.UTC().Add(duration))).
		Order(ent.Asc(lamina.FieldDateStarted)).
		WithSink().
		All(ctx)
	if err != nil {
		return nil, err
	}
	slack := time.Duration(prof.GetKeyframeIntervalMs())*time.Millisecond + EncoderBurst
	var existing *ent.Lamina
	for _, o := range inSlot {
		if !o.DateStarted.After(started.Add(slack)) {
			existing = o
		}
	}
	if existing != nil && existing.State != int32(api.LaminaState_LAMINA_STATE_PENDING) && segmentAfter(existing, started, slack) {
		// The slot's stored lamina ended before this segment began: the
		// camera stopped and came back within the slot (§15), and the
		// segment that follows gets a lamina of its own.
		existing = nil
	}
	if existing != nil && after != pdid.Nil && pdid.Id(existing.Id) == after {
		// The producer says that lamina was the segment before this one
		// and the camera ended it; its commit may still be on its way, so
		// its state does not decide. The next segment gets its own.
		existing = nil
	}

	ttl := horizon + duration + time.Duration(link.GetAbandonTimeoutSeconds())*time.Second + 5*time.Minute
	expires := a.now.Add(ttl)

	var (
		objId    pdid.Id
		attempts []cand
	)
	if existing != nil {
		objId = pdid.Id(existing.Id)
		if existing.State != int32(api.LaminaState_LAMINA_STATE_PENDING) {
			// Already committed, lost, or deleted: the same lamina, with
			// nothing to upload to.
			return api.Allocation_builder{
				LaminaId:       objId.Bytes(),
				SourceId:       src.GetId(),
				Ordinal:        src.GetOrdinal(),
				DateStarted:    timestamppb.New(started),
				ProfileVersion: a.set.GetProfileVersion(),
				DateExpires:    timestamppb.New(a.now),
				Profile:        prof,
				Link:           link,
				LaminaKey:      existing.LaminaKey,
			}.Build(), nil
		}

		// Unexpired attempts are answered again, with fresh tokens.
		open, err := s.ent(ctx).Attempt.Query().
			Where(attempt.LaminaIdEQ(existing.Id), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(a.now)).
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
		if existing == nil {
			// Open attempts per source are capped (§12.1): the cap is on
			// new laminae. A lamina already allocated whose attempts all
			// failed gets fresh ones regardless, so a segment being retried
			// is never held back by the allocations ahead of it (§16).
			n, err := s.ent(ctx).Attempt.Query().
				Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(a.now),
					attempt.HasLaminaWith(lamina.SourceIdEQ(srcId.Uuid()))).
				Count(ctx)
			if err != nil {
				return nil, err
			}
			if n >= a.bounds.MaxOpenAttempts {
				return nil, status.Errorf(codes.ResourceExhausted, "source %s holds %d open attempts", src.GetAlias(), n)
			}
		}

		s.d.metrics().Allocations.Add(ctx, 1)
		ranked := placement.Rank(a.cluster, placement.Key{Set: mustId(a.set.GetId()), Epoch: placement.Epoch(started.Unix(), int64(epoch.Seconds())), Version: a.placeV},
			placement.Member{Source: srcId, Ordinal: int(src.GetOrdinal())}, spreadOf(a.set))
		if len(ranked) == 0 {
			var why []string
			for id, w := range a.why {
				why = append(why, a.sinks[id].Alias+": "+w)
			}
			s.d.log().Warn("no sink can take the write", "sinks", len(a.cluster.Sinks), "why", strings.Join(why, "; "))

			return nil, status.Error(codes.Unavailable, "no sink can take the write")
		}
		if len(ranked) > Candidates {
			ranked = ranked[:Candidates]
		}

		if existing == nil {
			objId = pdid.New(DomLamina)
			var siteRef *api.SiteRef
			if len(a.set.GetSite().GetId()) > 0 {
				siteRef = api.SiteRef_builder{Id: a.set.GetSite().GetId()}.Build()
			}
			add := api.LaminaAddRequest_builder{
				Id:               objId.Bytes(),
				Tenant:           tenantRef(tenant),
				Site:             siteRef,
				Set:              api.SetRef_builder{Id: a.set.GetId()}.Build(),
				Source:           api.SourceRef_builder{Id: src.GetId()}.Build(),
				State:            api.LaminaState_LAMINA_STATE_PENDING,
				DateStarted:      timestamppb.New(started),
				DateExpired:      timestamppb.New(started.Add(expire)),
				DatesSynced:      true,
				PlacementVersion: a.placeV,
				Epoch:            placement.Epoch(started.Unix(), int64(epoch.Seconds())),
			}
			if !none {
				add.DateDeleted = timestamppb.New(started.Add(del))
			}
			if _, err := next.Lamina().Add(ctx, add.Build()); err != nil {
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
				Lamina:      api.LaminaRef_builder{Id: objId.Bytes()}.Build(),
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
		k := LaminaKey(started, objId, atId)
		if key == "" {
			key = k
		}

		record := api.LaminaRecord_builder{
			FormatVersion:         1,
			TenantId:              tenant.Bytes(),
			SiteId:                siteId,
			SetId:                 a.set.GetId(),
			SourceId:              src.GetId(),
			LaminaId:              objId.Bytes(),
			AttemptId:             atId.Bytes(),
			DateStartedMs:         started.UnixMilli(),
			Mode:                  link.GetMode(),
			AbandonTimeoutSeconds: link.GetAbandonTimeoutSeconds(),
			DateExpiredMs:         expiredMs,
			DateDeletedMs:         deletedMs,
			PlacementVersion:      a.placeV,
			Crc32C:                a.set.GetChecksum(),
		}.Build()

		tok, err := s.d.Keys.Sign(ctx, api.TokenClaims_builder{
			Exp:                   timestamppb.New(at.expires),
			Iat:                   timestamppb.New(a.now),
			Aud:                   nodeId.Bytes(),
			Op:                    api.TokenOp_TOKEN_OP_PUT,
			SinkId:                sinkId.Bytes(),
			LaminaKey:             k,
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
			LaminaKey: k,
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
		LaminaId:       objId.Bytes(),
		SourceId:       src.GetId(),
		Ordinal:        src.GetOrdinal(),
		DateStarted:    timestamppb.New(started),
		ProfileVersion: a.set.GetProfileVersion(),
		Candidates:     cands,
		DateExpires:    exp,
		MaxLength:      maxLen,
		SizeHint:       hint,
		LaminaKey:      key,
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
		Ref: req.GetRef(),
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
	err = s.tx(ctx, func(ctx context.Context, next api.Server) error {
		for _, m := range a.members {
			prof := segmentOf(m, set, a.bounds)
			if prof == nil {
				continue
			}
			d := time.Duration(prof.GetDurationSeconds()) * time.Second
			start := slotOf(now, set.GetId(), int(m.GetOrdinal()), len(a.members), d)
			for !start.After(now.Add(horizon)) {
				al, err := s.allocateSlot(ctx, next, a, m, start, pdid.Nil, f.Actor, f.Tenant)
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
