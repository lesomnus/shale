package core

import (
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/ent/dialect/sql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/payday/audit"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/protobuf-patch/patchpb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/predicate"
	"github.com/lesomnus/shale/internal/ent/producer"
	entset "github.com/lesomnus/shale/internal/ent/set"
	"github.com/lesomnus/shale/internal/ent/source"
	"github.com/lesomnus/shale/server/bare"
)

// Place puts a set that exists into a site, or moves it from one site to
// another (§7). A set's site is said when it is added and is immutable in
// the schema, and so are the copies its sources, laminae and attempts
// carry, because payday narrows each row by its own field 3: this is the
// one road that rewrites them, below the generated layer, in one
// transaction, and nothing else writes them.
//
// What moves with the set, each because the wall reads it by its own site:
// the set; every source of the set; every lamina of the set (by its set);
// every attempt of those laminae (by its lamina); and every producer
// adopted for the set (by its set), as Adopt writes it. For a set whose
// cameras Shale manages, its camera network and everything of it too, in
// the same transaction (cameraPlan, place_camera.go), or nothing, refused
// with the reasons, where a row of it is shared with something that does
// not move. A key's lists of cameras name sites and sources and are not
// rewritten: one that named the source still reaches the set, one that
// named the old site no longer does.
//
// Only whoever sees every site may, even between two sites they see: a
// move changes who sees every recording the set ever made, those before
// the move as much as those after, and somebody who sees only some sites
// would be deciding for the people of a site that is not theirs to show.
// Out of a site into none is not offered: a set in no site is seen by
// whoever sees every site alone, and putting one there is hiding it; a
// site of kind MEMBERS with nobody in it says so openly.
//
// While it runs, an allocation for the set waits for it: the move writes
// the set's row first, and every transaction that writes a row carrying
// the set's site reads that site again inside itself, holding the set's
// row (heldSite), so it is written with the site the set has once the move
// is over or before it began, never the one it read before.
func (s coreSet) Place(ctx context.Context, req *api.SetPlaceRequest) (*api.SetPlaceResponse, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomHolder {
		return nil, denied(api.SetService_Place_FullMethodName, "a person places a set")
	}
	if req.GetSet() == nil {
		return nil, invalid("set", "name the set to place")
	}
	if req.GetSite() == nil {
		return nil, invalid("site", "name the site to place the set in; out of a site into none is not offered: "+
			"a set in no site is seen by whoever sees every site alone, and a site of kind MEMBERS with nobody in it says the same openly")
	}
	all, err := everySite(ctx, s.d.Operators, s.own(ctx), f)
	if err != nil {
		return nil, err
	}
	if !all {
		return nil, denied(api.SetService_Place_FullMethodName, "only somebody who sees every site places a set: "+
			"a move changes who sees every recording the set ever made, and that is not for somebody who sees only some sites to decide, "+
			"even between two of theirs")
	}

	set, err := s.SetServiceServer.Get(ctx, api.SetGetRequest_builder{Ref: req.GetSet()}.Build())
	if err != nil {
		return nil, err
	}
	to, err := s.placeTarget(ctx, set, req.GetSite())
	if err != nil {
		return nil, err
	}
	from := set.GetSite().GetId()
	out := api.SetPlaceResponse_builder{Set: set, FromSiteId: from, Moved: &api.SetPlaced{}, DryRun: req.GetDryRun()}
	if bytes.Equal(from, to.Bytes()) {
		return out.Build(), nil
	}
	setId, tenant := mustId(set.GetId()), mustId(set.GetTenant().GetId())
	plan, err := s.cameraPlanOf(ctx, setId, tenant, from)
	if err != nil {
		return nil, err
	}

	if req.GetDryRun() {
		moved, err := s.placeCount(ctx, setId, tenant)
		if err != nil {
			return nil, err
		}
		if err := s.placeCameraCount(ctx, plan, tenant, moved); err != nil {
			return nil, err
		}
		out.Moved, out.Refused = moved, plan.refused

		return out.Build(), nil
	}
	if len(plan.refused) > 0 {
		return nil, placeRefused(plan.refused)
	}

	var moved *api.SetPlaced
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		v, err := c.placeRows(ctx, setId, tenant, from, to)
		if err != nil {
			return err
		}
		moved = v
		after, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := c.placeRecorded(ctx, set, after, from, to); err != nil {
			return err
		}
		if s.d.placeHeld != nil {
			s.d.placeHeld(ctx)
		}
		set = after

		return nil
	})
	if err != nil {
		return nil, err
	}
	s.d.log().Info("set placed", "set", setId.String(), "alias", set.GetAlias(), "from", siteName(from), "to", to.String(),
		"by", f.Actor.String(), "sources", moved.GetSources(), "laminae", moved.GetLaminae(), "attempts", moved.GetAttempts(), "producers", moved.GetProducers())
	out.Set, out.Moved = set, moved

	return out.Build(), nil
}

func siteName(id []byte) string {
	if len(id) == 0 {
		return "none"
	}

	return mustId(id).String()
}

// placeRefused is a placing refused for what its cameras share, every
// reason said.
func placeRefused(why []string) error {
	return failed("the set is not placed: %s", strings.Join(why, "; "))
}

// placeTarget is the site a set is to be placed in: of the set's tenant
// (another tenant's is not found, as the wall answers it) and not erased.
func (s coreSet) placeTarget(ctx context.Context, set *api.Set, ref *api.SiteRef) (pdid.Id, error) {
	site, err := s.Next().Site().Get(ctx, api.SiteGetRequest_builder{Ref: ref}.Build())
	if status.Code(err) == codes.NotFound && len(ref.GetId()) > 0 {
		// The wall answers an erased site as one that is not there; say
		// which it is when it is of this tenant.
		if v, e := s.ent(ctx).Site.Get(ctx, mustId(ref.GetId()).Uuid()); e == nil && v.DateErased != nil && pdid.Id(v.TenantId) == mustId(set.GetTenant().GetId()) {
			return pdid.Nil, failed("site %s is erased", v.Alias)
		}
	}
	if err != nil {
		return pdid.Nil, err
	}
	if !bytes.Equal(site.GetTenant().GetId(), set.GetTenant().GetId()) {
		return pdid.Nil, status.Error(codes.NotFound, "Site not found")
	}
	if site.GetDateErased() != nil {
		return pdid.Nil, failed("site %s is erased", site.GetAlias())
	}

	return mustId(site.GetId()), nil
}

// What a placing rewrites, kind by kind, each one statement over an index
// (docs/09-operations.md, *Placing a set*): a set's sources by
// source_member (set_id, ordinal); its laminae by lamina_set_time (set_id,
// date_started); their attempts by attempt_lamina (lamina_id,
// date_created), the laminae found as before; its producers by the
// tenant's range of producer_page (tenant_id, date_created, id), since
// producer has no index on set_id: the tenant's producers are read, one
// row per host, and no more.
func ofSet(set, tenant pdid.Id) (predicate.Source, predicate.Lamina, predicate.Attempt, predicate.Producer) {
	id := set.Uuid()

	return source.SetIdEQ(id), lamina.SetIdEQ(id), attempt.HasLaminaWith(lamina.SetIdEQ(id)), producer.And(producer.TenantIdEQ(tenant.Uuid()), producer.SetIdEQ(id))
}

// placeCount is what a placing would rewrite, counted with the statements'
// own filters and indexes.
func (s Core) placeCount(ctx context.Context, set, tenant pdid.Id) (*api.SetPlaced, error) {
	src, lam, att, pro := ofSet(set, tenant)
	db := s.ent(ctx)
	sources, err := db.Source.Query().Where(src).Count(ctx)
	if err != nil {
		return nil, err
	}
	laminae, err := db.Lamina.Query().Where(lam).Count(ctx)
	if err != nil {
		return nil, err
	}
	attempts, err := db.Attempt.Query().Where(att).Count(ctx)
	if err != nil {
		return nil, err
	}
	producers, err := db.Producer.Query().Where(pro).Count(ctx)
	if err != nil {
		return nil, err
	}

	return api.SetPlaced_builder{Sources: int64(sources), Laminae: int64(laminae), Attempts: int64(attempts), Producers: int64(producers)}.Build(), nil
}

// placeRows rewrites the site of every row that carries the set's site
// because of the set, inside the transaction ctx is in. The set's row is
// written first, on the condition that its site is still `from`: from
// then on the transaction holds it, and every allocation for the set waits
// for the commit at its own heldSite (or, on SQLite, at its BEGIN). The
// camera rows are checked again under that hold: an assignment made since
// the check before the transaction would have read the site this changes.
func (s Core) placeRows(ctx context.Context, set, tenant pdid.Id, from []byte, to pdid.Id) (*api.SetPlaced, error) {
	db := s.ent(ctx)
	site := to.Uuid()
	write := func(u *sql.UpdateBuilder) { u.Set(entset.FieldSiteId, site) }

	was := entset.SiteIdIsNil()
	if len(from) > 0 {
		was = entset.SiteIdEQ(mustId(from).Uuid())
	}
	n, err := db.Set.Update().Where(entset.IdEQ(set.Uuid()), was).SetDateUpdated(s.d.now()).Modify(write).Save(ctx)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, status.Error(codes.Aborted, "the set was placed elsewhere meanwhile; read it again")
	}
	moved := &api.SetPlaced{}

	// Its cameras, where Shale manages them: the plan again, now that the
	// set's row is held; its networks' rows held next, as every camera
	// operation on an assignment takes the set's and then the network's
	// (cameraAssignmentRowChecked), and adding a device or a policy takes
	// the network's; the plan once more, which nothing can change any
	// more; then every camera row.
	plan, err := s.cameraPlanOf(ctx, set, tenant, from)
	if err != nil {
		return nil, err
	}
	if plan.managed() {
		for _, n := range plan.networks {
			if err := s.cameraLockNetwork(ctx, n[:]); err != nil {
				return nil, err
			}
		}
		held, err := s.cameraPlanOf(ctx, set, tenant, from)
		if err != nil {
			return nil, err
		}
		if len(held.refused) > 0 {
			return nil, placeRefused(held.refused)
		}
		if !slices.Equal(held.networks, plan.networks) || !slices.Equal(held.workers, plan.workers) || !slices.Equal(held.presets, plan.presets) {
			return nil, status.Error(codes.Aborted, "the set's cameras changed meanwhile; place it again")
		}
		nets, err := s.placeCameraNetworks(ctx, held, tenant, to)
		if err != nil {
			return nil, err
		}
		moved.SetCameraNetworks(int64(nets))
		if err := s.placeCameraRows(ctx, held, tenant, to, moved); err != nil {
			return nil, err
		}
	}

	src, lam, att, pro := ofSet(set, tenant)
	sources, err := db.Source.Update().Where(src).Modify(func(u *sql.UpdateBuilder) { u.Set(source.FieldSiteId, site) }).Save(ctx)
	if err != nil {
		return nil, err
	}
	laminae, err := db.Lamina.Update().Where(lam).Modify(func(u *sql.UpdateBuilder) { u.Set(lamina.FieldSiteId, site) }).Save(ctx)
	if err != nil {
		return nil, err
	}
	attempts, err := db.Attempt.Update().Where(att).Modify(func(u *sql.UpdateBuilder) { u.Set(attempt.FieldSiteId, site) }).Save(ctx)
	if err != nil {
		return nil, err
	}
	// A producer's site is mutable and Adopt writes it from the set's; the
	// same value, and its version moves as Adopt's write moves it.
	producers, err := db.Producer.Update().Where(pro).SetSiteId(site).SetDateUpdated(s.d.now()).Save(ctx)
	if err != nil {
		return nil, err
	}

	moved.SetSources(int64(sources))
	moved.SetLaminae(int64(laminae))
	moved.SetAttempts(int64(attempts))
	moved.SetProducers(int64(producers))

	return moved, nil
}

// placeRecorded tells the recorders about the move as any write of the
// set is told (§26.5): a line of the trail with who placed it, the set,
// and a patch of its site from the one it was in to the one it is in, and
// the set's watchers. The rows that moved with it are on no line of their
// own; the counts are in the answer and the log line.
func (s Core) placeRecorded(ctx context.Context, before, after *api.Set, from []byte, to pdid.Id) error {
	if s.d.Recorder == nil {
		return nil
	}
	doc, err := placeDoc(from, to)
	if err != nil {
		return status.Errorf(codes.Internal, "the trail of a placing: %s", err)
	}
	method, ok := grpc.Method(ctx)
	if !ok {
		method = api.SetService_Place_FullMethodName
	}

	return s.d.Recorder.Record(ctx, bare.Server{Store: bare.Store{Db: s.ent(ctx)}}, bare.Change{
		Method:   method,
		By:       api.SetService_Place_FullMethodName,
		Entity:   "shale.Set",
		Key:      mustId(after.GetId()).Uuid(),
		Patch:    doc,
		Previous: before,
		Value:    after,
	})
}

// placeDoc is the patch a placing is: the set's site was `from` (or there
// was none), and is `to`.
func placeDoc(from []byte, to pdid.Id) (*patchpb.Patch, error) {
	doc, err := audit.Created(api.Set_builder{Site: api.Site_builder{Id: to.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	assign := doc.GetDelta().GetEntries()[0]
	var test *patchpb.Entry
	if len(from) == 0 {
		test = patchpb.Entry_builder{
			Targets: patchpb.Targets_builder{Selectors: []*patchpb.Selector{patchpb.Selector_builder{Key: assign.GetPath().GetSegments()[0]}.Build()}}.Build(),
			Test:    patchpb.Test_builder{Exists: proto.Bool(false)}.Build(),
		}.Build()
	} else {
		was, err := audit.Created(api.Set_builder{Site: api.Site_builder{Id: from}.Build()}.Build())
		if err != nil {
			return nil, err
		}
		e := was.GetDelta().GetEntries()[0]
		test = patchpb.Entry_builder{Path: e.GetPath(), Targets: e.GetTargets(), Test: patchpb.Test_builder{Value: e.GetAssign().GetValue()}.Build()}.Build()
	}
	doc.GetDelta().SetEntries([]*patchpb.Entry{test, assign})

	return doc, nil
}

// heldSite is the site of a set as the transaction ctx is in sees it,
// with the set's row held until that transaction ends (§7): a placing
// writes the set's row first, so this waits for one under way and answers
// the site it gave, and one that begins after this waits for the
// transaction that read it. Whatever writes a row that carries the set's
// site -- a lamina, an attempt, a source, a producer adopted, a camera
// assignment -- writes the site this answers, and asks for it before it
// writes anything else, so that the hold is the first a transaction takes
// and two of them cannot wait on each other.
//
// On SQLite every transaction holds the database from its BEGIN, which is
// the same hold for every row at once; elsewhere it is the row's shared
// lock, which allocations for one set take together and only a write of
// the set's row waits for.
func (s Core) heldSite(ctx context.Context, set []byte) ([]byte, error) {
	q := s.ent(ctx).Set.Query().Where(entset.IdEQ(mustId(set).Uuid()))
	if s.driver().Dialect() != dialect.SQLite {
		q.Modify(func(q *sql.Selector) { q.ForShare() })
	}
	v, err := q.Only(ctx)
	if err != nil {
		return nil, err
	}
	if isZero(v.SiteId) {
		return nil, nil
	}

	return v.SiteId[:], nil
}

// siteRefOf is a reference to the site `id`, nil for none.
func siteRefOf(id []byte) *api.SiteRef {
	if len(id) == 0 {
		return nil
	}

	return api.SiteRef_builder{Id: id}.Build()
}

// everySite says whether a person sees every site: where roster says what
// people may change, whoever it grants all of Shale (a failed lookup is
// no, as everywhere), and nobody by their row; where it does not, whoever's
// row has all_sites, from the frame or else read.
func everySite(ctx context.Context, ops Operators, own api.Server, f *frame.Frame) (bool, error) {
	if ops != nil {
		is, err := ops.Administers(ctx, f.Tenant, f.Actor)

		return is && err == nil, nil
	}
	if row, ok := f.Row.(*api.Holder); ok {
		return row.GetAllSites(), nil
	}
	h, err := own.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return false, err
	}

	return h.GetAllSites(), nil
}
