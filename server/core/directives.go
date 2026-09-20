package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/device"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/object"
	"github.com/lesomnus/shale/internal/ent/sink"
)

// The leader's traffic toward the nodes (§34.9): every directive is derived
// from state the CP holds, sent when that state says so, retried while a
// node is unreachable, and re-sent in full when its heartbeats resume. A
// directive is therefore never a message that can be lost. Reconciliation
// rides the same loop (§29).

// Labels a row carries while an operator's one-shot request is pending.
const (
	LabelLocate    = "shale.io/locate"
	LabelGc        = "shale.io/gc"
	LabelReconcile = "shale.io/reconcile"
)

const (
	// DefaultDirectivesEvery is how often the state is compared with what
	// the nodes were told.
	DefaultDirectivesEvery = 5 * time.Second
	// DefaultReconcileInterval is how often every sink is reconciled even
	// when nothing happened (§34.9).
	DefaultReconcileInterval = 24 * time.Hour
	// reconcileMargin is subtracted from the newest commit the CP knows
	// when asking a node for what it missed.
	reconcileMargin = time.Hour
	// resendAfter is how long a Delete waits for its event before it is
	// sent again.
	resendAfter = time.Minute
	// directiveTimeout bounds one call to a node.
	directiveTimeout = 30 * time.Second
	// deletePage is how many keys one Delete or SetDates carries.
	deletePage = 500
)

// Directives is what the leader runs toward the nodes.
type Directives struct {
	d      *Deps
	Every  time.Duration
	Leader func(ctx context.Context) bool
	Log    *slog.Logger

	mu      sync.Mutex
	conns   map[pdid.Id]*nodeConn
	alive   map[pdid.Id]bool
	accept  map[pdid.Id]bool
	sentAt  map[string]time.Time
	locate  map[pdid.Id]string
	backoff map[pdid.Id]time.Time
	fails   map[pdid.Id]int
	// Reconciled counts the reconciliations run, for tests.
	Reconciled int
}

type nodeConn struct {
	addr string
	conn *grpc.ClientConn
}

// NewDirectives makes the loop; with a nil Leader every tick runs.
func NewDirectives(d *Deps) *Directives {
	return &Directives{
		d: d, Every: DefaultDirectivesEvery,
		conns: map[pdid.Id]*nodeConn{}, alive: map[pdid.Id]bool{}, accept: map[pdid.Id]bool{},
		sentAt: map[string]time.Time{}, locate: map[pdid.Id]string{}, backoff: map[pdid.Id]time.Time{}, fails: map[pdid.Id]int{},
	}
}

func (s *Directives) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}

	return s.d.log()
}

// Spin runs the loop until the context is done.
func (s *Directives) Spin(ctx context.Context) error {
	t := time.NewTicker(s.Every)
	defer t.Stop()
	defer s.closeAll()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if s.Leader != nil && !s.Leader(ctx) {
			continue
		}
		if err := s.Once(ctx); err != nil {
			s.log().Warn("directives", "err", err.Error())
		}
	}
}

func (s *Directives) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, c := range s.conns {
		c.conn.Close()
		delete(s.conns, id)
	}
}

// Once compares the state with what every live node was told, and tells
// it the difference.
func (s *Directives) Once(ctx context.Context) error {
	if s.d.DialNode == nil {
		return nil
	}
	now := s.d.now()
	nodes, err := s.d.Ent.Node.Query().
		Where(node.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), node.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return err
	}

	for _, n := range nodes {
		id := pdid.Id(n.Id)
		alive := n.DateSeen != nil && now.Sub(*n.DateSeen) <= DefaultNodeDownAfter && n.ControlAddress != ""
		s.mu.Lock()
		was := s.alive[id]
		s.alive[id] = alive
		next := s.backoff[id]
		s.mu.Unlock()
		if !alive {
			if was {
				s.log().Info("node down", "node", id.String(), "alias", n.Alias)
				s.drop(id)
			}
			continue
		}
		if now.Before(next) {
			continue
		}
		resume := !was
		if resume {
			s.log().Info("node up", "node", id.String(), "alias", n.Alias, "control", n.ControlAddress)
		}
		if err := s.node(ctx, n, resume, now); err != nil {
			s.mu.Lock()
			s.fails[id]++
			wait := time.Duration(1<<uint(min(s.fails[id], 6))) * time.Second
			s.backoff[id] = now.Add(wait)
			// Re-send everything on the next success.
			s.alive[id] = false
			s.mu.Unlock()
			s.drop(id)
			s.log().Warn("directives", "node", id.String(), "alias", n.Alias, "retry_in", wait.String(), "err", err.Error())
			continue
		}
		s.mu.Lock()
		s.fails[id] = 0
		delete(s.backoff, id)
		s.mu.Unlock()
	}

	return nil
}

func (s *Directives) drop(id pdid.Id) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.conns[id]; c != nil {
		c.conn.Close()
		delete(s.conns, id)
	}
}

// conn is the connection to a node's control API, re-dialed when its
// address changed.
func (s *Directives) conn(ctx context.Context, n *ent.Node) (*grpc.ClientConn, error) {
	id := pdid.Id(n.Id)
	s.mu.Lock()
	c := s.conns[id]
	s.mu.Unlock()
	if c != nil && c.addr == n.ControlAddress {
		return c.conn, nil
	}
	if c != nil {
		c.conn.Close()
	}
	conn, err := s.d.DialNode(ctx, n.ControlAddress, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.conns[id] = &nodeConn{addr: n.ControlAddress, conn: conn}
	s.mu.Unlock()

	return conn, nil
}

// node tells one live node everything its state implies.
func (s *Directives) node(ctx context.Context, n *ent.Node, resume bool, now time.Time) error {
	conn, err := s.conn(ctx, n)
	if err != nil {
		return err
	}
	client := api.NewNodeControlClient(conn)
	nodeId := pdid.Id(n.Id)

	sinks, err := s.d.Ent.Sink.Query().
		Where(sink.NodeIdEQ(n.Id), sink.DateErasedIsNil()).
		WithDevice().
		All(ctx)
	if err != nil {
		return err
	}

	for _, sk := range sinks {
		sid := pdid.Id(sk.Id)
		attached := sk.Attachment == int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED)

		// Writes on or off: quarantine, retirement, release (§27).
		want := attached && sk.AcceptWrites
		s.mu.Lock()
		last, told := s.accept[sid]
		s.mu.Unlock()
		if resume || !told || last != want {
			cctx, cancel := context.WithTimeout(ctx, directiveTimeout)
			_, err := client.SetSinkState(cctx, api.NodeSetSinkStateRequest_builder{SinkId: sid.Bytes(), AcceptWrites: want}.Build())
			cancel()
			if err != nil {
				return fmt.Errorf("SetSinkState %s: %w", sk.Alias, err)
			}
			s.mu.Lock()
			s.accept[sid] = want
			s.mu.Unlock()
			if told && last != want {
				s.log().Info("sink writes", "sink", sk.Alias, "accept", want)
			}
		}
		if !attached {
			continue
		}

		if err := s.deletes(ctx, client, nodeId, sk, now, resume); err != nil {
			return err
		}
		if err := s.dates(ctx, client, sk); err != nil {
			return err
		}
		if err := s.gc(ctx, client, sk); err != nil {
			return err
		}

		// Reconciliation: on resume, on adoption (never reconciled), on an
		// operator's request, and every reconcile_interval (§34.9).
		full := sk.Labels[LabelReconcile] == "full"
		due := resume || sk.DateReconciled == nil || now.Sub(*sk.DateReconciled) > DefaultReconcileInterval || full
		if due {
			if err := s.reconcile(ctx, client, nodeId, sk, full, now); err != nil {
				return err
			}
		}
	}

	// Bay LEDs (§27): sent once per change, not retried.
	devices, err := s.d.Ent.Device.Query().Where(device.NodeIdEQ(n.Id), device.DateErasedIsNil()).All(ctx)
	if err != nil {
		return err
	}
	for _, d := range devices {
		did := pdid.Id(d.Id)
		want := d.Labels[LabelLocate]
		s.mu.Lock()
		last := s.locate[did]
		s.mu.Unlock()
		if last == want && !resume {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, directiveTimeout)
		resp, err := client.Locate(cctx, api.NodeLocateRequest_builder{DeviceHardwareId: d.HardwareId, Off: want == ""}.Build())
		cancel()
		if err != nil {
			s.log().Warn("locate", "device", d.Alias, "err", err.Error())
		} else if want != "" && !resp.GetSupported() {
			s.log().Warn("locate is not supported on this node", "device", d.Alias, "node", n.Alias)
		}
		s.mu.Lock()
		s.locate[did] = want
		s.mu.Unlock()
	}

	return nil
}

// deletes sends Delete for every object DELETING on the sink and for the
// duplicates of those objects on it (§20.3, §21.2). An answer of absent
// confirms the deletion; deleted waits for the node's event.
func (s *Directives) deletes(ctx context.Context, client api.NodeControlClient, nodeId pdid.Id, sk *ent.Sink, now time.Time, resume bool) error {
	objs, err := s.d.Ent.Object.Query().
		Where(object.SinkIdEQ(sk.Id), object.StateEQ(int32(api.ObjectState_OBJECT_STATE_DELETING))).
		Limit(deletePage).
		All(ctx)
	if err != nil {
		return err
	}
	keys := map[string]*ent.Object{}
	for _, o := range objs {
		keys[o.ObjectKey] = o
	}
	// Duplicates of objects being deleted, wherever the object's own file
	// is: their files sit on this sink.
	dups, err := s.d.Ent.Attempt.Query().
		Where(attempt.SinkIdEQ(sk.Id), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_DUPLICATE)),
			attempt.HasObjectWith(object.StateIn(int32(api.ObjectState_OBJECT_STATE_DELETING), int32(api.ObjectState_OBJECT_STATE_DELETED)))).
		WithObject().
		Limit(deletePage).
		All(ctx)
	if err != nil {
		return err
	}
	for _, a := range dups {
		if a.Edges.Object == nil {
			continue
		}
		key := ObjectKey(a.Edges.Object.DateStarted, pdid.Id(a.ObjectId), pdid.Id(a.Id))
		if _, ok := keys[key]; !ok {
			keys[key] = nil
		}
	}

	var send []string
	s.mu.Lock()
	for key := range keys {
		k := sk.Id.String() + "/" + key
		if at, ok := s.sentAt[k]; ok && !resume && now.Sub(at) < resendAfter {
			continue
		}
		send = append(send, key)
	}
	s.mu.Unlock()
	if len(send) == 0 {
		return nil
	}

	cctx, cancel := context.WithTimeout(ctx, directiveTimeout)
	resp, err := client.Delete(cctx, api.NodeDeleteRequest_builder{SinkId: sk.Id[:], Keys: send}.Build())
	cancel()
	if err != nil {
		return fmt.Errorf("Delete on %s: %w", sk.Alias, err)
	}
	s.mu.Lock()
	for _, key := range send {
		s.sentAt[sk.Id.String()+"/"+key] = now
	}
	s.mu.Unlock()

	for _, r := range resp.GetKeys() {
		obj := keys[r.GetKey()]
		switch r.GetResult() {
		case api.DeleteResult_DELETE_RESULT_ABSENT:
			// Gone already: the event was lost, or never sent. Confirmed.
			s.mu.Lock()
			delete(s.sentAt, sk.Id.String()+"/"+r.GetKey())
			s.mu.Unlock()
			if obj == nil {
				continue
			}
			core := Core{d: s.d}
			if err := core.ownTx(ctx, func(own api.Server) error {
				return core.applyMissing(ctx, own, nodeId, api.ObjectMissing_builder{
					SinkId: sk.Id[:], ObjectKey: r.GetKey(), Reason: api.MissingReason_MISSING_REASON_NOT_FOUND,
				}.Build())
			}); err != nil {
				s.log().Warn("confirm deletion", "key", r.GetKey(), "err", err.Error())
			}
		case api.DeleteResult_DELETE_RESULT_FAILED:
			s.log().Warn("delete failed on the node", "sink", sk.Alias, "key", r.GetKey(), "err", r.GetError())
		}
	}

	return nil
}

// dates rewrites the xattrs of objects whose dates changed (§20.3).
func (s *Directives) dates(ctx context.Context, client api.NodeControlClient, sk *ent.Sink) error {
	objs, err := s.d.Ent.Object.Query().
		Where(object.SinkIdEQ(sk.Id), object.DatesSyncedEQ(false), object.StateEQ(int32(api.ObjectState_OBJECT_STATE_COMMITTED))).
		Limit(deletePage).
		All(ctx)
	if err != nil || len(objs) == 0 {
		return err
	}
	var dates []*api.NodeSetDatesRequest_Dates
	byKey := map[string]*ent.Object{}
	for _, o := range objs {
		d := api.NodeSetDatesRequest_Dates_builder{Key: o.ObjectKey, DateExpired: timestamppb.New(o.DateExpired)}
		if o.DateDeleted != nil {
			d.DateDeleted = timestamppb.New(*o.DateDeleted)
		}
		dates = append(dates, d.Build())
		byKey[o.ObjectKey] = o
	}
	cctx, cancel := context.WithTimeout(ctx, directiveTimeout)
	resp, err := client.SetDates(cctx, api.NodeSetDatesRequest_builder{SinkId: sk.Id[:], Dates: dates}.Build())
	cancel()
	if err != nil {
		return fmt.Errorf("SetDates on %s: %w", sk.Alias, err)
	}
	absent := map[string]bool{}
	for _, k := range resp.GetAbsent() {
		absent[k] = true
		s.log().Warn("dates for a file the node does not have", "sink", sk.Alias, "key", k)
	}
	for key, o := range byKey {
		_ = absent[key]
		// Synced, or nothing to sync to: either way the directive is done.
		// A missing file is the reconciliation's finding, not this one's.
		if _, err := s.d.Own.Object().Patch(ctx, api.ObjectPatchRequest_builder{
			Ref: api.ObjectRef_builder{Id: o.Id[:]}.Build(), DatesSynced: z.Ptr(true), DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}
	}
	s.log().Info("dates synced", "sink", sk.Alias, "objects", len(byKey), "absent", len(absent))

	return nil
}

// gc runs a round an operator asked for (§21).
func (s *Directives) gc(ctx context.Context, client api.NodeControlClient, sk *ent.Sink) error {
	if sk.Labels[LabelGc] == "" {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	resp, err := client.Gc(cctx, api.NodeGcRequest_builder{SinkId: sk.Id[:]}.Build())
	cancel()
	if err != nil {
		return fmt.Errorf("Gc on %s: %w", sk.Alias, err)
	}
	s.log().Info("gc round", "sink", sk.Alias, "proposed", resp.GetProposed(), "deleted", resp.GetDeleted(), "reclaimed", resp.GetReclaimedBytes())
	labels := map[string]string{}
	for k, v := range sk.Labels {
		if k != LabelGc {
			labels[k] = v
		}
	}
	_, err = s.d.Own.Sink().Patch(ctx, api.SinkPatchRequest_builder{
		Ref: api.SinkRef_builder{Id: sk.Id[:]}.Build(), Labels: labels, DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
}

// reconcile asks the node for what the CP may have missed on a sink and
// applies it as if the events had arrived (§34.9, §29).
func (s *Directives) reconcile(ctx context.Context, client api.NodeControlClient, nodeId pdid.Id, sk *ent.Sink, full bool, now time.Time) error {
	var since time.Time
	if !full {
		newest, err := s.d.Ent.Object.Query().
			Where(object.SinkIdEQ(sk.Id), object.DateCommittedNotNil()).
			Order(ent.Desc(object.FieldDateCommitted)).
			First(ctx)
		if err != nil && !ent.IsNotFound(err) {
			return err
		}
		if newest != nil && newest.DateCommitted != nil {
			since = newest.DateCommitted.Add(-reconcileMargin)
		}
	}
	deleting, err := s.d.Ent.Object.Query().
		Where(object.SinkIdEQ(sk.Id), object.StateEQ(int32(api.ObjectState_OBJECT_STATE_DELETING))).
		Limit(10000).
		All(ctx)
	if err != nil {
		return err
	}
	var keys []string
	for _, o := range deleting {
		keys = append(keys, o.ObjectKey)
	}

	cctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	req := api.NodeReconcileRequest_builder{SinkId: sk.Id[:], Deleting: keys}
	if !since.IsZero() {
		req.Since = timestamppb.New(since)
	}
	stream, err := client.Reconcile(cctx, req.Build())
	if err != nil {
		return fmt.Errorf("Reconcile %s: %w", sk.Alias, err)
	}
	core := Core{d: s.d}
	var total, records, learned, absent int64
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("Reconcile %s: %w", sk.Alias, err)
		}
		switch {
		case item.HasTotal():
			total = item.GetTotal()
		case item.HasRecord():
			records++
			rec := item.GetRecord()
			known, err := s.d.Ent.Attempt.Query().Where(attempt.IdEQ(mustId(rec.GetAttemptId()).Uuid())).First(ctx)
			if err == nil && (known.State == int32(api.AttemptState_ATTEMPT_STATE_STORED) || known.State == int32(api.AttemptState_ATTEMPT_STATE_DUPLICATE)) {
				continue
			}
			if err := core.ownTx(ctx, func(own api.Server) error {
				return core.applyStored(ctx, own, nodeId, rec)
			}); err != nil {
				s.log().Warn("reconcile record", "sink", sk.Alias, "key", rec.GetObjectKey(), "err", err.Error())
				continue
			}
			learned++
		case item.HasAbsent():
			absent++
			if err := core.ownTx(ctx, func(own api.Server) error {
				return core.applyMissing(ctx, own, nodeId, api.ObjectMissing_builder{
					SinkId: sk.Id[:], ObjectKey: item.GetAbsent(), Reason: api.MissingReason_MISSING_REASON_NOT_FOUND,
				}.Build())
			}); err != nil {
				s.log().Warn("reconcile absent", "sink", sk.Alias, "key", item.GetAbsent(), "err", err.Error())
			}
		}
	}

	labels := map[string]string{}
	for k, v := range sk.Labels {
		if k != LabelReconcile {
			labels[k] = v
		}
	}
	if _, err := s.d.Own.Sink().Patch(ctx, api.SinkPatchRequest_builder{
		Ref: api.SinkRef_builder{Id: sk.Id[:]}.Build(), DateReconciled: timestamppb.New(now), Labels: labels, DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return err
	}
	s.mu.Lock()
	s.Reconciled++
	s.mu.Unlock()
	s.log().Info("reconciled", "sink", sk.Alias, "full", full, "since", since.Format(time.RFC3339), "files", total, "records", records, "learned", learned, "confirmed_absent", absent)

	return nil
}
