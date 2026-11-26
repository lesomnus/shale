package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/predicate"
	"github.com/lesomnus/shale/internal/ent/reach"
	"github.com/lesomnus/shale/internal/ent/set"
	"github.com/lesomnus/shale/internal/ent/source"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/server/bare"
)

// A key narrowed to some cameras (§33.1). A person's lists of cameras are
// rows of their own, Reach: a name, sites and sources. A key carries one by
// its identifier, as the scope roster keeps on it (`ApiKey.scope`), and
// payday puts that on the frame of every call made with it
// (`frame.Frame.TokenScope`). What such a call may see is what its person
// may see -- the wall and the sites, as for any call of theirs -- and, of
// that, what the list names: a scope beside the two, so that every
// generated read and stream honours it, and every path to a camera's data
// that reads its sources through them (Live, Timeline, Export) signs
// tokens for what the list reaches alone.
//
// The list is read at the call, never kept per person: by its identifier,
// for as long as what roster said of the key is believed
// (`auth.operators.ttl`), and dropped at once when it is written here. A
// list that is gone, or that is not the key's person's, reaches nothing,
// and a call that names a source is told so (reachGone).

// maxReachName is the most characters a list's name is.
const maxReachName = 128

// ReachScope is what a key carries for a list of cameras: the list's
// identifier, in its written form.
func ReachScope(id pdid.Id) string { return id.String() }

// parseReach is the list a key's scope names, if it names one as
// ReachScope writes it.
func parseReach(scope string) (pdid.Id, bool) {
	id, err := pdid.Parse(scope)
	if err != nil || id.Domain() != DomReach || id.String() != scope {
		return pdid.Nil, false
	}

	return id, true
}

// Reaches is the lists of cameras as the calls made with keys see them,
// and the scope that holds such a call to its list.
type Reaches struct {
	d *Deps

	mu    sync.Mutex
	known map[pdid.Id]reachEntry
	// gen counts the drops, so a list read before one is not kept after it.
	gen uint64
}

// reachEntry is a list as it was read: whose, and what it names. The sets
// are those of the sources it names, which a source never leaves.
type reachEntry struct {
	at     time.Time
	found  bool
	tenant pdid.Id
	holder pdid.Id

	sites, sources, sets []uuid.UUID
}

// reachesMax bounds what is remembered: one entry per list a key has been
// used with lately.
const reachesMax = 4096

// NewReaches makes the lists' lookup.
func NewReaches(d *Deps) *Reaches {
	return &Reaches{d: d, known: map[pdid.Id]reachEntry{}}
}

// ttl is how long a list is believed: no longer than what roster said of
// the key that carries it.
func (r *Reaches) ttl() time.Duration {
	if r.d.KeyRemembered > 0 {
		return r.d.KeyRemembered
	}

	return identity.OperatorsTtl
}

// Drop forgets a list, at once: it was written, and the next call made
// with a key that carries it reads it again.
func (r *Reaches) Drop(id pdid.Id) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.known, id)
	r.gen++
}

// client is the rows, through the transaction the context is in, else the
// pool: nothing read here is narrowed, since this is what narrows.
func (r *Reaches) client(ctx context.Context) *ent.Client {
	if v, ok := ctx.Value(txKey{}).(txBound); ok {
		return v.ent
	}

	return r.d.Ent
}

// read is the list by its identifier, as remembered or read again.
func (r *Reaches) read(ctx context.Context, id pdid.Id) (reachEntry, error) {
	now := r.d.now()
	r.mu.Lock()
	e, ok := r.known[id]
	gen := r.gen
	r.mu.Unlock()
	if ok && now.Sub(e.at) < r.ttl() && !now.Before(e.at) {
		return e, nil
	}

	e = reachEntry{at: now}
	db := r.client(ctx)
	row, err := db.Reach.Query().Where(reach.IdEQ(id.Uuid())).Only(ctx)
	switch {
	case ent.IsNotFound(err):
	case err != nil:
		return reachEntry{}, err
	default:
		e.found, e.tenant, e.holder = true, pdid.Id(row.TenantId), pdid.Id(row.HolderId)
		e.sites = reachIds(row.Cameras.GetSites())
		e.sources = reachIds(row.Cameras.GetSources())
		if len(e.sources) > 0 {
			vs, err := db.Source.Query().Where(source.IdIn(e.sources...)).All(ctx)
			if err != nil {
				return reachEntry{}, err
			}
			seen := map[uuid.UUID]bool{}
			for _, v := range vs {
				if !seen[v.SetId] {
					seen[v.SetId] = true
					e.sets = append(e.sets, v.SetId)
				}
			}
		}
	}

	r.mu.Lock()
	if r.gen == gen {
		if len(r.known) >= reachesMax {
			r.known = map[pdid.Id]reachEntry{}
		}
		r.known[id] = e
	}
	r.mu.Unlock()

	return e, nil
}

func reachIds(bs [][]byte) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(bs))
	for _, b := range bs {
		if id, err := pdid.From(b); err == nil {
			out = append(out, id.Uuid())
		}
	}

	return out
}

// on is the list the key a call came with carries: `scoped` false for a
// call whose credential carries no scope, which this narrows nothing of;
// otherwise the list, or none -- not found when it is gone, names no list,
// or is not the key's person's -- which reaches nothing.
func (r *Reaches) on(ctx context.Context) (e reachEntry, scoped bool, err error) {
	f, ok := frame.From(ctx)
	if !ok || f.TokenScope() == "" {
		return reachEntry{}, false, nil
	}
	id, ok := parseReach(f.TokenScope())
	if !ok {
		return reachEntry{}, true, nil
	}
	e, err = r.read(ctx, id)
	if err != nil {
		return reachEntry{}, true, err
	}
	if !e.found || e.tenant != f.Tenant || e.holder != f.Actor {
		return reachEntry{}, true, nil
	}

	return e, true, nil
}

// errReachGone is a call that names a source, made with a key whose list
// of cameras is gone.
var errReachGone = status.Error(codes.FailedPrecondition,
	"this key's list of cameras is gone -- erased, or not its person's -- so it reaches no camera; "+
		"its person makes a token with another list on the Tokens page")

// reachGone refuses a call that names a source when it came with a key
// whose list is gone, rather than answering the source is not there.
func (s Core) reachGone(ctx context.Context) error {
	if s.d == nil || s.d.Reaches == nil {
		return nil
	}
	e, scoped, err := s.d.Reaches.on(ctx)
	if err != nil {
		return err
	}
	if scoped && !e.found {
		return errReachGone
	}

	return nil
}

// Scope is the scope beside the wall (cmd/serve.go): a call made with a key
// that carries a list is held to what it names, in every entity that is a
// camera's data or names one; and a person's lists are their own.
func (r *Reaches) Scope() bare.Scope { return reachScope{r: r} }

// reachScope narrows a call made with a key that carries a list to it.
// Sites and sources are one rule, so that a list of a site and of a camera
// of another site reaches both: a source is reached when it is named or
// its site is; a set when its site is named or a source of it is; and
// what names a source -- a lamina, an attempt by its lamina, a camera's
// binding -- by that source, or by the site it is in. Every other entity
// is not narrowed here: the door refuses such a key every method but those
// of keyScopeMay, whose handlers read the rest without it.
type reachScope struct {
	bare.Unscoped
	r *Reaches
}

// SourceScope: named, or of a site named.
func (x reachScope) SourceScope(ctx context.Context) (predicate.Source, error) {
	e, scoped, err := x.r.on(ctx)
	if !scoped || err != nil {
		return nil, err
	}

	return source.Or(source.IdIn(e.sources...), source.SiteIdIn(e.sites...)), nil
}

// SetScope: of a site named, or with a source named among its members.
func (x reachScope) SetScope(ctx context.Context) (predicate.Set, error) {
	e, scoped, err := x.r.on(ctx)
	if !scoped || err != nil {
		return nil, err
	}

	return set.Or(set.IdIn(e.sets...), set.SiteIdIn(e.sites...)), nil
}

// LaminaScope: of a source named, or of a site named.
func (x reachScope) LaminaScope(ctx context.Context) (predicate.Lamina, error) {
	e, scoped, err := x.r.on(ctx)
	if !scoped || err != nil {
		return nil, err
	}

	return lamina.Or(lamina.SourceIdIn(e.sources...), lamina.SiteIdIn(e.sites...)), nil
}

// AttemptScope: of a lamina the list reaches. An attempt is watched by
// nobody, so it is asked of its lamina rather than of a column of its own.
func (x reachScope) AttemptScope(ctx context.Context) (predicate.Attempt, error) {
	e, scoped, err := x.r.on(ctx)
	if !scoped || err != nil {
		return nil, err
	}

	return attempt.HasLaminaWith(lamina.Or(lamina.SourceIdIn(e.sources...), lamina.SiteIdIn(e.sites...))), nil
}

// CameraBindingScope: of a source named, or of a site named. No method of
// CameraBindingService is offered to such a key (keyScopeMay): this holds
// if one ever is.
func (x reachScope) CameraBindingScope(ctx context.Context) (predicate.CameraBinding, error) {
	e, scoped, err := x.r.on(ctx)
	if !scoped || err != nil {
		return nil, err
	}

	return camerabinding.Or(camerabinding.SourceIdIn(e.sources...), camerabinding.SiteIdIn(e.sites...)), nil
}

// ReachScope: a person's own lists, and nobody else's; nothing to any
// other kind of caller. A call with nothing in its context -- the
// deployment's own -- is not narrowed.
func (x reachScope) ReachScope(ctx context.Context) (predicate.Reach, error) {
	f, ok := frame.From(ctx)
	if !ok {
		return nil, nil
	}
	if kindOf(f.Actor) != DomHolder {
		return reach.IdIn(), nil
	}

	return reach.HolderIdEQ(f.Actor.Uuid()), nil
}

// The generated verbs of Reach, with what is Shale's to say about them: the
// row is the caller's, and what it names is what they see.

type coreReach struct {
	Core
	api.ReachServiceServer
}

func (s Core) Reach() api.ReachServiceServer {
	return coreReach{s, s.Next().Reach()}
}

// reachCaller is the person whose list it is.
func reachCaller(ctx context.Context) (*frame.Frame, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomHolder {
		return nil, status.Error(codes.PermissionDenied, "a list of cameras is a person's own: nothing else has any")
	}

	return f, nil
}

func reachName(v string) (string, error) {
	v = strings.TrimSpace(v)
	switch {
	case v == "":
		return "", invalid("name", "a list of cameras is told apart by its name")
	case utf8.RuneCountInString(v) > maxReachName:
		return "", invalid("name", fmt.Sprintf("at most %d characters", maxReachName))
	}

	return v, nil
}

// reachable is what a list may name: each site and each source as the
// caller sees them, through their own walled view as a delegation's are,
// by the identifier the row has; one they do not see is refused.
func (s coreReach) reachable(ctx context.Context, v *api.ReachCameras) (*api.ReachCameras, error) {
	out := api.ReachCameras_builder{}
	seen := map[pdid.Id]bool{}
	for i, b := range v.GetSites() {
		id, err := pdid.From(b)
		if err != nil {
			return nil, invalid(fmt.Sprintf("cameras.sites[%d]", i), err.Error())
		}
		w, err := s.Next().Site().Get(ctx, api.SiteGetRequest_builder{Ref: api.SiteRef_builder{Id: id.Bytes()}.Build()}.Build())
		if status.Code(err) == codes.NotFound {
			return nil, invalid(fmt.Sprintf("cameras.sites[%d]", i), "no site you see has that identifier")
		} else if err != nil {
			return nil, err
		}
		if id = mustId(w.GetId()); !seen[id] {
			seen[id] = true
			out.Sites = append(out.Sites, id.Bytes())
		}
	}
	for i, b := range v.GetSources() {
		id, err := pdid.From(b)
		if err != nil {
			return nil, invalid(fmt.Sprintf("cameras.sources[%d]", i), err.Error())
		}
		w, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: id.Bytes()}.Build()}.Build())
		if status.Code(err) == codes.NotFound {
			return nil, invalid(fmt.Sprintf("cameras.sources[%d]", i), "no camera you see has that identifier")
		} else if err != nil {
			return nil, err
		}
		if id = mustId(w.GetId()); !seen[id] {
			seen[id] = true
			out.Sources = append(out.Sources, id.Bytes())
		}
	}

	return out.Build(), nil
}

// Add makes the caller a list: theirs, in their tenant, of what they see.
func (s coreReach) Add(ctx context.Context, req *api.ReachAddRequest) (*api.Reach, error) {
	f, err := reachCaller(ctx)
	if err != nil {
		return nil, err
	}
	if h := req.GetHolder(); h != nil && string(h.GetId()) != string(f.Actor.Bytes()) {
		return nil, status.Error(codes.PermissionDenied, "holder: a list of cameras is its person's own; leave holder out, and it is yours")
	}
	name, err := reachName(req.GetName())
	if err != nil {
		return nil, err
	}
	cams, err := s.reachable(ctx, req.GetCameras())
	if err != nil {
		return nil, err
	}
	out, err := s.ReachServiceServer.Add(ctx, api.ReachAddRequest_builder{
		Id:          req.GetId(),
		Tenant:      tenantRef(f.Tenant),
		Name:        name,
		Holder:      api.HolderRef_builder{Id: f.Actor.Bytes()}.Build(),
		Cameras:     cams,
		DateCreated: req.GetDateCreated(),
	}.Build())
	if err != nil {
		return nil, err
	}
	s.dropReach(out.GetId())

	return out, nil
}

// Patch changes one of the caller's lists; what it names afterwards is
// what they see now. Every key that carries it reaches the new from the
// next asking.
func (s coreReach) Patch(ctx context.Context, req *api.ReachPatchRequest) (*api.Reach, error) {
	if _, err := reachCaller(ctx); err != nil {
		return nil, err
	}
	cur, err := s.ReachServiceServer.Get(ctx, api.ReachGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	r := proto.CloneOf(req)
	r.SetRef(api.ReachRef_builder{Id: cur.GetId()}.Build())
	if req.HasName() {
		name, err := reachName(req.GetName())
		if err != nil {
			return nil, err
		}
		r.SetName(name)
	}
	if req.HasCameras() && !req.GetCamerasNull() {
		cams, err := s.reachable(ctx, req.GetCameras())
		if err != nil {
			return nil, err
		}
		r.SetCameras(cams)
	}
	out, err := s.ReachServiceServer.Patch(ctx, r)
	s.dropReach(cur.GetId())

	return out, err
}

// Erase erases one of the caller's lists; the keys that carry it reach
// nothing from the next asking.
func (s coreReach) Erase(ctx context.Context, req *api.ReachRef) (*api.ReachEraseResponse, error) {
	if _, err := reachCaller(ctx); err != nil {
		return nil, err
	}
	out, err := s.ReachServiceServer.Erase(ctx, req)
	s.dropReach(req.GetId())

	return out, err
}

func (s Core) dropReach(id []byte) {
	if s.d == nil || s.d.Reaches == nil {
		return
	}
	if v, err := pdid.From(id); err == nil {
		s.d.Reaches.Drop(v)
	}
}

// keyReach holds a key asked for with a scope to what one may carry: the
// identifier of a list of the caller's, and methods a list confines.
func (s Core) keyReach(ctx context.Context, tenant, holder pdid.Id, k identity.TerminalKey) error {
	if k.Scope == "" {
		return nil
	}
	id, ok := parseReach(k.Scope)
	if !ok {
		return invalid("scope", "the identifier of a list of cameras of yours, as Shale writes it, or nothing for a token that reaches what you reach")
	}
	row, err := s.ent(ctx).Reach.Query().Where(reach.IdEQ(id.Uuid())).Only(ctx)
	if ent.IsNotFound(err) || (err == nil && (pdid.Id(row.TenantId) != tenant || pdid.Id(row.HolderId) != holder)) {
		return invalid("scope", "no list of cameras of yours has that identifier")
	} else if err != nil {
		return err
	}
	var outside []string
	for _, m := range k.Methods {
		if !keyScopeMay[m] {
			outside = append(outside, m)
		}
	}
	if len(outside) > 0 {
		return invalid("methods", fmt.Sprintf("a token narrowed to a list of cameras calls only what the list confines -- reads and views of cameras -- so it cannot carry %s",
			strings.Join(outside, ", ")))
	}

	return nil
}
