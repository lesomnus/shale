package core

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/sink"
)

// mathPow is math.Pow, named so events.go reads plainly.
func mathPow(a, b float64) float64 { return math.Pow(a, b) }

type coreSink struct {
	Core
	api.SinkServiceServer
}

func (s Core) Sink() api.SinkServiceServer {
	return coreSink{s, s.Next().Sink()}
}

// ProposeGc is the node proposing and the CP approving (§21.2). A candidate
// is matched by location only; approvable ones are ordered by tenant fair
// share (§21.4), then by date, and approved until the target is met.
func (s coreSink) ProposeGc(ctx context.Context, req *api.SinkProposeGcRequest) (*api.SinkProposeGcResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomNode {
		return nil, status.Error(codes.PermissionDenied, "only a node proposes")
	}

	sk, err := s.SinkServiceServer.Get(ctx, api.SinkGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	if sk.GetNode().GetId() == nil || pdid.Id(mustId(sk.GetNode().GetId())) != f.Actor {
		return nil, status.Error(codes.PermissionDenied, "this sink is not attached to the node")
	}
	sid := mustId(sk.GetId())
	now := s.d.now()
	sweep := req.GetReason() == api.GcReason_GC_REASON_SWEEP

	type cand struct {
		c      *api.GcCandidate
		obj    *ent.Lamina
		tenant pdid.Id
		when   time.Time
		ok     bool
		dates  bool
	}
	var cands []*cand
	for _, c := range req.GetCandidates() {
		cc := &cand{c: c}
		obj, err := s.ent(ctx).Lamina.Query().Where(lamina.SinkIdEQ(sid.Uuid()), lamina.LaminaKeyEQ(c.GetLaminaKey())).First(ctx)
		if err != nil && !ent.IsNotFound(err) {
			return nil, err
		}
		if obj == nil {
			// A duplicate's file or an orphan: approvable when the xattr date
			// it was proposed on has passed (§21.3).
			var when *timestamppb.Timestamp
			if sweep {
				when = c.GetDateDeleted()
			} else {
				when = c.GetDateExpired()
			}
			cc.ok = when != nil && !when.AsTime().After(now)
			if len(c.GetTenantId()) > 0 {
				cc.tenant = mustId(c.GetTenantId())
			}
			if when != nil {
				cc.when = when.AsTime()
			}
			cands = append(cands, cc)
			continue
		}
		cc.obj = obj
		cc.tenant = pdid.Id(obj.TenantId)
		switch api.LaminaState(obj.State) {
		case api.LaminaState_LAMINA_STATE_DELETING:
			cc.ok = true
		case api.LaminaState_LAMINA_STATE_COMMITTED, api.LaminaState_LAMINA_STATE_LOST:
			if sweep {
				cc.ok = obj.DateDeleted != nil && !obj.DateDeleted.After(now)
				if obj.DateDeleted != nil {
					cc.when = *obj.DateDeleted
				}
			} else {
				cc.ok = !obj.DateExpired.After(now)
				cc.when = obj.DateExpired
			}
			// Dates the node proposed on that differ from the truth: it
			// rewrites the xattr.
			if c.GetDateExpired() == nil || !c.GetDateExpired().AsTime().Equal(obj.DateExpired) {
				cc.dates = true
			}
			if (obj.DateDeleted == nil) != (c.GetDateDeleted() == nil) || (obj.DateDeleted != nil && c.GetDateDeleted() != nil && !c.GetDateDeleted().AsTime().Equal(*obj.DateDeleted)) {
				cc.dates = true
			}
		default:
			cc.ok = false
		}
		cands = append(cands, cc)
	}

	// Fair share: tenants over their share first (§21.4).
	over := s.overShare(ctx)
	sort.SliceStable(cands, func(i, j int) bool {
		oi, oj := over[cands[i].tenant], over[cands[j].tenant]
		if oi != oj {
			return oi
		}

		return cands[i].when.Before(cands[j].when)
	})

	target := req.GetBytesNeeded()
	var approved int64
	var decisions []*api.GcDecision
	err = s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
		for _, c := range cands {
			d := api.GcDecision_builder{LaminaKey: c.c.GetLaminaKey()}
			if c.obj != nil && c.dates {
				d.DateExpired = timestamppb.New(c.obj.DateExpired)
				if c.obj.DateDeleted != nil {
					d.DateDeleted = timestamppb.New(*c.obj.DateDeleted)
				}
			}
			if c.ok && (sweep || target <= 0 || approved < target) {
				d.Approved = true
				approved += c.c.GetSize()
				if c.obj != nil && c.obj.State != int32(api.LaminaState_LAMINA_STATE_DELETING) {
					if _, err := own.Lamina().Patch(ctx, api.LaminaPatchRequest_builder{
						Ref:              api.LaminaRef_builder{Id: c.obj.Id[:]}.Build(),
						State:            z.Ptr(api.LaminaState_LAMINA_STATE_DELETING),
						DateUpdatedForce: z.Ptr(true),
					}.Build()); err != nil {
						return err
					}
				}
			}
			decisions = append(decisions, d.Build())
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	s.d.metrics().GcApprovedBytes.Add(ctx, approved, pairAttr("sink", mustId(req.GetRef().GetId()).String(), "reason", req.GetReason().String()))

	return api.SinkProposeGcResponse_builder{Decisions: decisions, ApprovedBytes: approved}.Build(), nil
}

// overShare says which tenants hold more than their share of the cluster
// (§21.4). Equal shares unless capacity_share says otherwise.
func (s Core) overShare(ctx context.Context) map[pdid.Id]bool {
	out := map[pdid.Id]bool{}
	ts, err := s.own(ctx).Tenant().List(ctx, api.TenantListRequest_builder{Size: 100}.Build())
	if err != nil {
		return out
	}
	var total int64
	var declared float64
	n := 0
	for _, t := range ts.GetItems() {
		if mustId(t.GetId()) == s.d.ClusterTenant {
			continue
		}
		total += t.GetStoredBytes()
		declared += t.GetCapacityShare()
		n++
	}
	if n == 0 || total == 0 {
		return out
	}
	for _, t := range ts.GetItems() {
		if mustId(t.GetId()) == s.d.ClusterTenant {
			continue
		}
		share := 1 / float64(n)
		if declared > 0 && t.GetCapacityShare() > 0 {
			share = t.GetCapacityShare() / declared
		}
		if float64(t.GetStoredBytes())/float64(total) > share*1.05 {
			out[mustId(t.GetId())] = true
		}
	}

	return out
}

// Adopt attaches a moved sink to a node (§28.3). Every lamina row keeps its
// location; the CP reconciles the sink with the new node.
func (s coreSink) Adopt(ctx context.Context, req *api.SinkAdoptRequest) (*api.Sink, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	sk, err := s.SinkServiceServer.Get(ctx, api.SinkGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	n, err := s.Next().Node().Get(ctx, api.NodeGetRequest_builder{Ref: req.GetNode()}.Build())
	if err != nil {
		return nil, err
	}
	if sk.GetNode().GetId() != nil && string(sk.GetNode().GetId()) != string(n.GetId()) {
		if s.nodeAlive(ctx, pdid.Id(mustId(sk.GetNode().GetId())), s.d.now()) {
			return nil, failed("the sink is served by a live node; erase or stop that node first")
		}
	}

	return s.SinkServiceServer.Patch(ctx, api.SinkPatchRequest_builder{
		Ref:                api.SinkRef_builder{Id: sk.GetId()}.Build(),
		Node:               api.NodeRef_builder{Id: n.GetId()}.Build(),
		Attachment:         z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED),
		DateReconciledNull: z.Ptr(true),
		DateUpdatedForce:   z.Ptr(true),
	}.Build())
}

// Reconcile asks for a reconciliation of one sink, or of every sink, which
// the leader carries out (§34.9); `full` is the index rebuild of §29.
func (s coreSink) Reconcile(ctx context.Context, req *api.SinkReconcileRequest) (*api.SinkReconcileResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	var rows []*api.Sink
	if req.GetRef() != nil {
		sk, err := s.SinkServiceServer.Get(ctx, api.SinkGetRequest_builder{Ref: req.GetRef()}.Build())
		if err != nil {
			return nil, err
		}
		rows = []*api.Sink{sk}
	} else {
		after := ""
		for {
			vs, err := s.SinkServiceServer.List(ctx, api.SinkListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			rows = append(rows, vs.GetItems()...)
			if vs.GetNext() == "" {
				break
			}
			after = vs.GetNext()
		}
	}
	var n int64
	for _, sk := range rows {
		if sk.GetAttachment() != api.SinkAttachment_SINK_ATTACHMENT_ATTACHED {
			continue
		}
		labels := map[string]string{}
		for k, v := range sk.GetLabels() {
			labels[k] = v
		}
		if req.GetFull() {
			labels[LabelReconcile] = "full"
		} else {
			labels[LabelReconcile] = "now"
		}
		if _, err := s.SinkServiceServer.Patch(ctx, api.SinkPatchRequest_builder{
			Ref: api.SinkRef_builder{Id: sk.GetId()}.Build(), Labels: labels, DateReconciledNull: z.Ptr(true), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		n++
	}

	return api.SinkReconcileResponse_builder{Sinks: n}.Build(), nil
}

// Gc asks for a GC round on the sink, which the leader runs through the
// node's control API (§21).
func (s coreSink) Gc(ctx context.Context, req *api.SinkGcRequest) (*api.Sink, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	sk, err := s.SinkServiceServer.Get(ctx, api.SinkGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	labels := map[string]string{}
	for k, v := range sk.GetLabels() {
		labels[k] = v
	}
	labels[LabelGc] = "run"

	return s.SinkServiceServer.Patch(ctx, api.SinkPatchRequest_builder{
		Ref: api.SinkRef_builder{Id: sk.GetId()}.Build(), Labels: labels, DateUpdatedForce: z.Ptr(true),
	}.Build())
}

func (s coreSink) Retire(ctx context.Context, req *api.SinkRetireRequest) (*api.Sink, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	sk, err := s.SinkServiceServer.Get(ctx, api.SinkGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}

	return s.SinkServiceServer.Patch(ctx, api.SinkPatchRequest_builder{
		Ref:              api.SinkRef_builder{Id: sk.GetId()}.Build(),
		Attachment:       z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_RETIRED),
		AcceptWrites:     z.Ptr(false),
		DateUpdatedForce: z.Ptr(true),
	}.Build())
}

// nodeAlive says whether a node's heartbeats are fresh (§27).
func (s Core) nodeAlive(ctx context.Context, id pdid.Id, now time.Time) bool {
	n, err := s.ent(ctx).Node.Get(ctx, id.Uuid())
	if err != nil || n.DateSeen == nil || n.DateErased != nil {
		return false
	}

	return now.Sub(*n.DateSeen) <= s.d.nodeDownAfter()
}

// ---- Device ------------------------------------------------------------

type coreDevice struct {
	Core
	api.DeviceServiceServer
}

func (s Core) Device() api.DeviceServiceServer {
	return coreDevice{s, s.Next().Device()}
}

func (s coreDevice) setHealth(ctx context.Context, ref *api.DeviceRef, h api.DeviceHealth, reason string, operator bool) (*api.Device, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	d, err := s.DeviceServiceServer.Get(ctx, api.DeviceGetRequest_builder{Ref: ref}.Build())
	if err != nil {
		return nil, err
	}
	now := s.d.now()
	id := mustId(d.GetId())
	// A release by an operator starts from a clean score; the rest keep
	// theirs, so a retired device still shows why.
	score := d.GetFailureScore()
	if h == api.DeviceHealth_DEVICE_HEALTH_HEALTHY {
		score = 0
	}
	if err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		return s.setDeviceHealth(ctx, nx, id, h, reason, operator, score, now)
	}); err != nil {
		return nil, err
	}

	return s.DeviceServiceServer.Get(ctx, api.DeviceGetRequest_builder{Ref: api.DeviceRef_builder{Id: d.GetId()}.Build()}.Build())
}

func (s coreDevice) Quarantine(ctx context.Context, req *api.DeviceQuarantineRequest) (*api.Device, error) {
	return s.setHealth(ctx, req.GetRef(), api.DeviceHealth_DEVICE_HEALTH_QUARANTINED, req.GetReason(), true)
}

func (s coreDevice) Release(ctx context.Context, req *api.DeviceReleaseRequest) (*api.Device, error) {
	return s.setHealth(ctx, req.GetRef(), api.DeviceHealth_DEVICE_HEALTH_HEALTHY, req.GetReason(), true)
}

func (s coreDevice) Retire(ctx context.Context, req *api.DeviceRetireRequest) (*api.Device, error) {
	return s.setHealth(ctx, req.GetRef(), api.DeviceHealth_DEVICE_HEALTH_RETIRED, req.GetReason(), true)
}

// DeclareDead marks every lamina on the device's sinks LOST and forgets the
// device (§27).
func (s coreDevice) DeclareDead(ctx context.Context, req *api.DeviceDeclareDeadRequest) (*api.Device, error) {
	d, err := s.setHealth(ctx, req.GetRef(), api.DeviceHealth_DEVICE_HEALTH_DEAD, req.GetReason(), true)
	if err != nil {
		return nil, err
	}
	now := s.d.now()
	err = s.ownTx(ctx, func(ctx context.Context, own api.Server) error {
		sinks, err := s.ent(ctx).Sink.Query().Where(sink.DeviceIdEQ(mustId(d.GetId()).Uuid())).All(ctx)
		if err != nil {
			return err
		}
		for _, sk := range sinks {
			objs, err := s.ent(ctx).Lamina.Query().
				Where(lamina.SinkIdEQ(sk.Id), lamina.StateIn(int32(api.LaminaState_LAMINA_STATE_COMMITTED), int32(api.LaminaState_LAMINA_STATE_DELETING))).
				All(ctx)
			if err != nil {
				return err
			}
			for _, o := range objs {
				if _, err := own.Lamina().Patch(ctx, api.LaminaPatchRequest_builder{
					Ref: api.LaminaRef_builder{Id: o.Id[:]}.Build(), State: z.Ptr(api.LaminaState_LAMINA_STATE_LOST),
					DateFinished: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
				}.Build()); err != nil {
					return err
				}
				if err := s.storedBytes(ctx, own, pdid.Id(o.TenantId), -o.Size); err != nil {
					return err
				}
			}
			if _, err := own.Sink().Patch(ctx, api.SinkPatchRequest_builder{
				Ref: api.SinkRef_builder{Id: sk.Id[:]}.Build(), Attachment: z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_RETIRED),
				AcceptWrites: z.Ptr(false), DateUpdatedForce: z.Ptr(true),
			}.Build()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Locate is carried out through the node's control API (§27); the row
// records the request and the directive runs on the leader.
func (s coreDevice) Locate(ctx context.Context, req *api.DeviceLocateRequest) (*api.Device, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}
	d, err := s.DeviceServiceServer.Get(ctx, api.DeviceGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	labels := d.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if req.GetOff() {
		labels[LabelLocate] = ""
	} else {
		labels[LabelLocate] = "on"
	}

	return s.DeviceServiceServer.Patch(ctx, api.DevicePatchRequest_builder{
		Ref:              api.DeviceRef_builder{Id: d.GetId()}.Build(),
		Labels:           labels,
		DateUpdatedForce: z.Ptr(true),
	}.Build())
}
