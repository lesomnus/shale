package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/delegation"
)

// Viewing on behalf of a person (§33.8). An app that signs its people in at
// the same roster -- a portal -- proves two things together, once: the
// person, with their access token for Shale's API from the issuer, and
// itself, with an exchange token roster issued to Shale. Shale answers a
// handle, a session of its own for "this app, acting for this person",
// and every use of it after brings a fresh exchange token, asks roster
// again about both, and is served as the person would be, narrowed to
// what the app's holder is granted and to what the handle says.

// Delegations is what proving the two takes: the issuer, roster, and the
// configuration that says which OAuth client is which app.
type Delegations interface {
	// Person is who an access token for Shale's API names, with their row
	// here (made on first sight, like a sign-in), and the OAuth client it
	// was issued to. An error is a token that proves nothing.
	Person(ctx context.Context, token string) (*api.Holder, string, error)
	// App is the app's holder at roster that an exchange token issued to
	// Shale in tenant names, and its alias. An error is a token that
	// proves nothing, or a roster that could not be asked.
	App(ctx context.Context, tenant pdid.Id, token string) (pdid.Id, string, error)
	// AppOf is which app an OAuth client is, by the alias of its holder in
	// every tenant (`roster app install`'s name): `auth.delegation.clients`.
	AppOf(client string) (string, bool)
	// Standing is nil for a person roster still has and has not suspended:
	// asked at every use, since what roster grants a person who may read
	// anyway says nothing of whether they still may.
	Standing(ctx context.Context, tenant, holder pdid.Id) error
}

// DelegationConfig bounds what a delegation is.
type DelegationConfig struct {
	// MaxTtl is the longest a handle lives, renewed or not; 12 h when zero.
	MaxTtl time.Duration
	// TokenTtl is the longest a view or read token issued under one lives,
	// which bounds how long an open WHEP session or a signed URL outlives
	// the handle; 5 min when zero.
	TokenTtl time.Duration
}

const (
	DefaultDelegationMaxTtl   = 12 * time.Hour
	DefaultDelegationTokenTtl = 5 * time.Minute
)

func (c DelegationConfig) maxTtl() time.Duration {
	if c.MaxTtl > 0 {
		return c.MaxTtl
	}

	return DefaultDelegationMaxTtl
}

func (c DelegationConfig) tokenTtl() time.Duration {
	if c.TokenTtl > 0 {
		return c.TokenTtl
	}

	return DefaultDelegationTokenTtl
}

// delegatedKey marks a context served for an app on a person's behalf: the
// tokens issued in it name the app and live no longer than TokenTtl.
type delegatedKey struct{}

type delegated struct {
	app pdid.Id
	ttl time.Duration
}

func delegatedFrom(ctx context.Context) (delegated, bool) {
	v, ok := ctx.Value(delegatedKey{}).(delegated)

	return v, ok
}

// claimsFor is what a token issued in ctx says of who asked: the actor and
// its tenant, and the app when it asked for them; and the expiry, shortened
// to a delegation's.
func claimsFor(ctx context.Context, f *frame.Frame, b *api.TokenClaims_builder, now, exp time.Time) {
	b.Actor, b.ActorTenant = f.Actor.Bytes(), f.Tenant.Bytes()
	if d, ok := delegatedFrom(ctx); ok {
		b.Delegator = d.app.Bytes()
		if e := now.Add(d.ttl); e.Before(exp) {
			exp = e
		}
	}
	b.Exp = timestamppb.New(exp)
}

type coreDelegation struct {
	Core
	api.DelegationServiceServer
}

func (s Core) Delegation() api.DelegationServiceServer {
	return coreDelegation{s, s.Next().Delegation()}
}

// The permission methods an app's holder is granted at roster.
var (
	delegatedLive     = api.DelegationService_Live_FullMethodName
	delegatedTimeline = api.DelegationService_Timeline_FullMethodName
)

// IsDelegationEntry says whether a method is one an app calls with its
// exchange token and nothing else (§33.8): public to the interceptor,
// which finds no credential it knows, and proved by the handler.
func IsDelegationEntry(method string) bool {
	switch method {
	case api.DelegationService_Start_FullMethodName, delegatedLive, delegatedTimeline, api.DelegationService_Revoke_FullMethodName:
		return true
	}

	return false
}

// ready is the configuration and the grant lookup a delegation needs.
func (s coreDelegation) ready() error {
	if s.d.Delegations == nil {
		return status.Error(codes.FailedPrecondition, "viewing on behalf of somebody is not configured here (auth.delegation)")
	}
	if s.d.Operators == nil {
		return status.Error(codes.FailedPrecondition, "viewing on behalf of somebody needs what roster grants to decide it (auth.operators)")
	}

	return nil
}

// exchange is the app's exchange token: `authorization: Bearer rd_…`.
func exchange(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if tok, ok := strings.CutPrefix(v, "Bearer "); ok && strings.TrimSpace(tok) != "" {
			return strings.TrimSpace(tok), nil
		}
	}

	return "", status.Error(codes.Unauthenticated, "the app's exchange token from roster, issued to Shale, is the credential: authorization: Bearer rd_…")
}

// app proves the app in tenant with its exchange token.
func (s coreDelegation) app(ctx context.Context, tenant pdid.Id) (pdid.Id, string, error) {
	tok, err := exchange(ctx)
	if err != nil {
		return pdid.Nil, "", err
	}
	app, alias, err := s.d.Delegations.App(ctx, tenant, tok)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return pdid.Nil, "", err
		}

		return pdid.Nil, "", status.Errorf(codes.Unauthenticated, "the exchange token proves no app: %v", err)
	}

	return app, alias, nil
}

// ceiling says whether roster grants the app's holder a method of this
// service: the most it may ever do for anybody.
func (s coreDelegation) ceiling(ctx context.Context, tenant, app pdid.Id, method string) error {
	ok, err := s.d.Operators.May(ctx, tenant, app, method)
	if err != nil {
		return status.Errorf(codes.Unavailable, "%s: cannot tell what roster grants the app right now: %v", method, err)
	}
	if !ok {
		return denied(method, "roster grants the app nothing that covers this; a role at roster naming it, bound to the app's holder, does")
	}

	return nil
}

// as is ctx served as the person, for a delegated method, the way the gate
// would serve them the method it stands for: their tenant, their sites,
// and on a deployment whose reads are granted, what roster grants them --
// the method itself, or the delegated one, which is how a person is let
// view through an app and read nothing here themselves.
func (s coreDelegation) as(ctx context.Context, holder *api.Holder, delegated string) (context.Context, error) {
	method := inner(delegated)
	actor, err := idOf(holder.GetId())
	if err != nil {
		return nil, err
	}
	tenant, err := idOf(holder.GetTenant().GetId())
	if err != nil {
		return nil, err
	}
	f := frame.New(actor, tenant, frame.Whole().In(tenant).To(method)).WithRow(holder)

	return gate.Decide(frame.Into(ctx, f), personPolicy{TenantPolicy{Operators: s.d.Operators, Reads: s.d.Reads}, delegated}, method)
}

// personPolicy is the tenant policy for a person an app is served for: a
// grant of the delegated method is as good as one of the method it
// stands for. A grant of the delegated method is no use to the person
// themselves, whom DelegationService refuses.
type personPolicy struct {
	TenantPolicy
	delegated string
}

func (p personPolicy) May(ctx context.Context, c gate.Call) error {
	if p.Reads && p.Operators != nil {
		if ok, err := p.Operators.May(ctx, c.Tenant, c.Actor, p.delegated); err == nil && ok {
			return nil
		}
	}

	return p.TenantPolicy.May(ctx, c)
}

// person is the row of the person a delegation is for, as the resolver
// reads it.
func (s coreDelegation) person(ctx context.Context, id []byte) (*api.Holder, error) {
	h, err := s.own(ctx).Holder().Get(ctx, api.HolderGetRequest_builder{
		Ref:    api.HolderRef_builder{Id: id}.Build(),
		Select: api.HolderSelect_builder{All: z.Ptr(true), Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.PermissionDenied, "the person this was for is not here any more")
		}

		return nil, err
	}

	return h, nil
}

func (s coreDelegation) Start(ctx context.Context, req *api.DelegationStartRequest) (*api.DelegationStartResponse, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if !req.GetLive() && !req.GetRecordings() {
		return nil, invalid("live", "say what the app will ask for: live, recordings, or both")
	}
	if t := req.GetRecordingsFrom(); t != nil && req.GetRecordingsTo() != nil && !req.GetRecordingsTo().AsTime().After(t.AsTime()) {
		return nil, invalid("recordings_to", "must be after recordings_from")
	}

	// The person, by their token.
	holder, client, err := s.d.Delegations.Person(ctx, req.GetAccessToken())
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return nil, err
		}

		return nil, status.Errorf(codes.Unauthenticated, "the access token proves nobody: %v", err)
	}
	tenant, err := idOf(holder.GetTenant().GetId())
	if err != nil {
		return nil, err
	}
	// The app, by its exchange token, and the two bound: the token was
	// issued to the app's client, and the app is of the person's tenant
	// (its token is introspected there and nowhere else).
	want, ok := s.d.Delegations.AppOf(client)
	if !ok {
		return nil, status.Errorf(codes.PermissionDenied, "the OAuth client %q is no app that views on anybody's behalf here (auth.delegation.clients)", client)
	}
	app, alias, err := s.app(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if alias != want {
		return nil, status.Errorf(codes.PermissionDenied, "the access token was issued to %q's client and the exchange token names %q: the two are not the same app", want, alias)
	}

	// What it may ask for: what roster grants the app, and what the person
	// may read themselves.
	var methods []string
	if req.GetLive() {
		methods = append(methods, delegatedLive)
	}
	if req.GetRecordings() {
		methods = append(methods, delegatedTimeline)
	}
	for _, m := range methods {
		if err := s.ceiling(ctx, tenant, app, m); err != nil {
			return nil, err
		}
		if _, err := s.as(ctx, holder, m); err != nil {
			return nil, err
		}
	}

	// The sets and sources it names, as the person sees them.
	pc, err := s.as(ctx, holder, methods[0])
	if err != nil {
		return nil, err
	}
	var sets, sources [][]byte
	for _, ref := range req.GetSets() {
		v, err := s.Next().Set().Get(pc, api.SetGetRequest_builder{Ref: ref}.Build())
		if err != nil {
			return nil, err
		}
		sets = append(sets, v.GetId())
	}
	for _, ref := range req.GetSources() {
		v, err := s.Next().Source().Get(pc, api.SourceGetRequest_builder{Ref: ref}.Build())
		if err != nil {
			return nil, err
		}
		sources = append(sources, v.GetId())
	}

	now := s.d.now()
	ends := now.Add(s.d.DelegationConfig.maxTtl())
	if t := req.GetDateEnds(); t != nil {
		if !t.AsTime().After(now) {
			return nil, invalid("date_ends", "must be in the future")
		}
		if t.AsTime().Before(ends) {
			ends = t.AsTime()
		}
	}
	secret := make([]byte, 32)
	rand.Read(secret)
	hash := sha256.Sum256(secret)
	id := pdid.New(DomDelegation)
	// Written as the person, so the trail says whose it was.
	row, err := s.own(ctx).Delegation().Add(frame.Into(ctx, frame.New(mustId(holder.GetId()), tenant, frame.Grant{})), api.DelegationAddRequest_builder{
		Id:             id.Bytes(),
		Tenant:         tenantRef(tenant),
		Holder:         api.HolderRef_builder{Id: holder.GetId()}.Build(),
		App:            app.Bytes(),
		ClientId:       client,
		SecretHash:     hash[:],
		Live:           req.GetLive(),
		Recordings:     req.GetRecordings(),
		Sets:           sets,
		Sources:        sources,
		RecordingsFrom: req.GetRecordingsFrom(),
		RecordingsTo:   req.GetRecordingsTo(),
		DateEnds:       timestamppb.New(ends),
	}.Build())
	if err != nil {
		return nil, err
	}
	s.d.log().Info("delegation: started", "delegation", id.String(), "person", mustId(holder.GetId()).String(), "tenant", tenant.String(),
		"app", app.String(), "app_alias", alias, "live", req.GetLive(), "recordings", req.GetRecordings(), "ends", ends.UTC().Format(time.RFC3339))

	return api.DelegationStartResponse_builder{
		Handle:     id.String() + "." + base64.RawURLEncoding.EncodeToString(secret),
		Delegation: shown(row),
	}.Build(), nil
}

// inner is the method a delegated one is served as.
func inner(method string) string {
	if method == delegatedTimeline {
		return api.LaminaService_Timeline_FullMethodName
	}

	return api.SetService_Live_FullMethodName
}

// shown is a delegation as it is answered: never its secret's hash.
func shown(v *api.Delegation) *api.Delegation {
	v = proto.CloneOf(v)
	v.SetSecretHash(nil)

	return v
}

// use is a handle's delegation, good now, with the app's exchange token
// beside it: not revoked, not ended, the same app, and roster still
// granting it method. It answers the delegation and the context to serve
// the person in.
func (s coreDelegation) use(ctx context.Context, handle, method string) (*ent.Delegation, *api.Holder, context.Context, error) {
	if err := s.ready(); err != nil {
		return nil, nil, nil, err
	}
	row, err := s.handle(ctx, handle)
	if err != nil {
		return nil, nil, nil, err
	}
	now := s.d.now()
	switch {
	case row.DateRevoked != nil:
		return nil, nil, nil, status.Error(codes.PermissionDenied, "this delegation was ended; start another")
	case !now.Before(row.DateEnds):
		return nil, nil, nil, status.Error(codes.PermissionDenied, "this delegation has run its time; start another")
	}
	tenant := pdid.Id(row.TenantId)
	app, _, err := s.app(ctx, tenant)
	if err != nil {
		return nil, nil, nil, err
	}
	if app != mustId(row.App) {
		return nil, nil, nil, status.Error(codes.PermissionDenied, "the exchange token names another app than the one this delegation is for")
	}
	if err := s.ceiling(ctx, tenant, app, method); err != nil {
		return nil, nil, nil, err
	}
	if err := s.d.Delegations.Standing(ctx, tenant, pdid.Id(row.HolderId)); err != nil {
		if status.Code(err) == codes.Unavailable {
			return nil, nil, nil, err
		}

		return nil, nil, nil, status.Errorf(codes.PermissionDenied, "the person this is for is not in good standing at roster: %v", err)
	}
	switch method {
	case delegatedLive:
		if !row.Live {
			return nil, nil, nil, denied(method, "this delegation is for recordings, not live view")
		}
	case delegatedTimeline:
		if !row.Recordings {
			return nil, nil, nil, denied(method, "this delegation is for live view, not recordings")
		}
	}
	holder, err := s.person(ctx, row.HolderId[:])
	if err != nil {
		return nil, nil, nil, err
	}
	pc, err := s.as(ctx, holder, method)
	if err != nil {
		return nil, nil, nil, err
	}
	pc = context.WithValue(pc, delegatedKey{}, delegated{app: app, ttl: s.d.DelegationConfig.tokenTtl()})
	s.touch(ctx, row, holder, now)

	return row, holder, pc, nil
}

// handle reads the delegation a handle names and checks its secret.
func (s coreDelegation) handle(ctx context.Context, handle string) (*ent.Delegation, error) {
	bad := status.Error(codes.PermissionDenied, "no such delegation")
	ref, secret, ok := strings.Cut(handle, ".")
	if !ok {
		return nil, bad
	}
	id, err := pdid.Parse(ref)
	if err != nil || id.Domain() != DomDelegation {
		return nil, bad
	}
	raw, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil {
		return nil, bad
	}
	row, err := s.ent(ctx).Delegation.Query().Where(delegation.IdEQ(id.Uuid()), delegation.DateErasedIsNil()).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, bad
		}

		return nil, err
	}
	hash := sha256.Sum256(raw)
	if subtle.ConstantTimeCompare(hash[:], row.SecretHash) != 1 {
		return nil, bad
	}

	return row, nil
}

// touch says when a delegation was last used, at most once a minute: a
// write as the person, so the trail has it beside the start.
func (s coreDelegation) touch(ctx context.Context, row *ent.Delegation, holder *api.Holder, now time.Time) {
	if row.DateUsed != nil && now.Sub(*row.DateUsed) < time.Minute {
		return
	}
	f := frame.New(mustId(holder.GetId()), pdid.Id(row.TenantId), frame.Grant{})
	if _, err := s.own(ctx).Delegation().Patch(frame.Into(ctx, f), api.DelegationPatchRequest_builder{
		Ref:              api.DelegationRef_builder{Id: pdid.Id(row.Id).Bytes()}.Build(),
		DateUsed:         timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		s.d.log().Warn("delegation: could not say when it was used", "delegation", pdid.Id(row.Id).String(), "err", err.Error())
	}
}

// within is whether a source of a set is one a delegation names.
func within(row *ent.Delegation, set, source []byte) bool {
	if len(row.Sets) == 0 && len(row.Sources) == 0 {
		return true
	}
	eq := func(v []byte) func([]byte) bool { return func(w []byte) bool { return string(v) == string(w) } }

	return slices.ContainsFunc(row.Sets, eq(set)) || slices.ContainsFunc(row.Sources, eq(source))
}

func (s coreDelegation) Live(ctx context.Context, req *api.DelegationLiveRequest) (*api.SetLiveResponse, error) {
	row, holder, pc, err := s.use(ctx, req.GetHandle(), delegatedLive)
	if err != nil {
		return nil, err
	}
	var out []*api.LiveSource
	switch {
	case req.GetSource() != nil:
		src, err := s.Next().Source().Get(pc, api.SourceGetRequest_builder{Ref: req.GetSource()}.Build())
		if err != nil {
			return nil, err
		}
		if !within(row, src.GetSet().GetId(), src.GetId()) {
			return nil, denied(delegatedLive, "a camera this delegation does not name")
		}
		v, err := s.Core.Source().Live(pc, api.SourceLiveRequest_builder{Ref: req.GetSource()}.Build())
		if err != nil {
			return nil, err
		}
		out = []*api.LiveSource{v.GetSource()}
	case req.GetSet() != nil:
		set, err := s.Next().Set().Get(pc, api.SetGetRequest_builder{Ref: req.GetSet()}.Build())
		if err != nil {
			return nil, err
		}
		v, err := s.Core.Set().Live(pc, api.SetLiveRequest_builder{Ref: req.GetSet()}.Build())
		if err != nil {
			return nil, err
		}
		for _, src := range v.GetSources() {
			if within(row, set.GetId(), src.GetSourceId()) {
				out = append(out, src)
			}
		}
		if len(out) == 0 {
			return nil, denied(delegatedLive, "a set none of whose cameras this delegation names")
		}
	default:
		return nil, invalid("set", "exactly one of set or source")
	}
	s.issued(row, holder, delegatedLive, len(out))

	return api.SetLiveResponse_builder{Sources: out}.Build(), nil
}

func (s coreDelegation) Timeline(ctx context.Context, req *api.DelegationTimelineRequest) (*api.LaminaTimelineResponse, error) {
	row, holder, pc, err := s.use(ctx, req.GetHandle(), delegatedTimeline)
	if err != nil {
		return nil, err
	}
	tl := proto.CloneOf(req.GetTimeline())
	if tl == nil || tl.GetFrom() == nil || tl.GetTo() == nil {
		return nil, invalid("timeline", "a timeline request with a time range")
	}
	// Narrowed to the delegation's window: a lamina overlapping its edge
	// comes whole, since a lamina is what a token reads.
	if t := row.RecordingsFrom; t != nil && tl.GetFrom().AsTime().Before(*t) {
		tl.SetFrom(timestamppb.New(*t))
	}
	if t := row.RecordingsTo; t != nil && tl.GetTo().AsTime().After(*t) {
		tl.SetTo(timestamppb.New(*t))
	}
	if !tl.GetTo().AsTime().After(tl.GetFrom().AsTime()) {
		return nil, denied(delegatedTimeline, "a time this delegation does not reach")
	}
	var set []byte
	switch {
	case tl.GetSource() != nil:
		src, err := s.Next().Source().Get(pc, api.SourceGetRequest_builder{Ref: tl.GetSource()}.Build())
		if err != nil {
			return nil, err
		}
		if !within(row, src.GetSet().GetId(), src.GetId()) {
			return nil, denied(delegatedTimeline, "a camera this delegation does not name")
		}
		set = src.GetSet().GetId()
	case tl.GetSet() != nil:
		v, err := s.Next().Set().Get(pc, api.SetGetRequest_builder{Ref: tl.GetSet()}.Build())
		if err != nil {
			return nil, err
		}
		set = v.GetId()
	default:
		return nil, invalid("timeline.set", "exactly one of set or source")
	}
	v, err := s.Core.Lamina().Timeline(pc, tl)
	if err != nil {
		return nil, err
	}
	var kept []*api.TimelineSource
	for _, src := range v.GetSources() {
		if within(row, set, src.GetSourceId()) {
			kept = append(kept, src)
		}
	}
	if len(kept) == 0 && len(v.GetSources()) > 0 {
		return nil, denied(delegatedTimeline, "a set none of whose cameras this delegation names")
	}
	v.SetSources(kept)
	n := 0
	for _, src := range kept {
		n += len(src.GetLaminae())
	}
	s.issued(row, holder, delegatedTimeline, n)

	return v, nil
}

// issued logs what was handed out under a delegation, with who for and
// who asked: the tokens carry both too, and the relay and the nodes log
// them where they are spent.
func (s coreDelegation) issued(row *ent.Delegation, holder *api.Holder, method string, n int) {
	s.d.log().Info("delegation: issued", "delegation", pdid.Id(row.Id).String(), "person", mustId(holder.GetId()).String(),
		"tenant", pdid.Id(row.TenantId).String(), "app", mustId(row.App).String(), "method", method, "tokens", n)
}

func (s coreDelegation) Revoke(ctx context.Context, req *api.DelegationRevokeRequest) (*api.DelegationRevokeResponse, error) {
	if s.d.Delegations == nil {
		return nil, s.ready()
	}
	row, err := s.handle(ctx, req.GetHandle())
	if err != nil {
		return nil, err
	}
	app, _, err := s.app(ctx, pdid.Id(row.TenantId))
	if err != nil {
		return nil, err
	}
	if app != mustId(row.App) {
		return nil, status.Error(codes.PermissionDenied, "the exchange token names another app than the one this delegation is for")
	}
	if row.DateRevoked == nil {
		if err := s.revoke(ctx, row.Id, row.HolderId, row.TenantId, "the app ended it"); err != nil {
			return nil, err
		}
	}

	return api.DelegationRevokeResponse_builder{}.Build(), nil
}

// revoke ends a delegation, as the person it was for.
func (s Core) revoke(ctx context.Context, id, holder, tenant uuid.UUID, why string) error {
	f := frame.New(pdid.Id(holder), pdid.Id(tenant), frame.Grant{})
	if _, err := s.own(ctx).Delegation().Patch(frame.Into(ctx, f), api.DelegationPatchRequest_builder{
		Ref:              api.DelegationRef_builder{Id: pdid.Id(id).Bytes()}.Build(),
		DateRevoked:      timestamppb.New(s.d.now()),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return err
	}
	s.d.log().Info("delegation: ended", "delegation", pdid.Id(id).String(), "person", pdid.Id(holder).String(), "why", why)

	return nil
}

// Unstanding ends every delegation for a person issued before `since`
// (§33.8): roster's word that they were signed out everywhere, suspended or
// erased. It answers how many it ended.
func (s Core) Unstanding(ctx context.Context, holder pdid.Id, since time.Time, why string) (int, error) {
	rows, err := s.d.Ent.Delegation.Query().Where(
		delegation.HolderIdEQ(holder.Uuid()), delegation.DateRevokedIsNil(), delegation.DateErasedIsNil(),
		delegation.DateCreatedLTE(since), delegation.DateEndsGT(s.d.now()),
	).All(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range rows {
		if err := s.revoke(ctx, r.Id, r.HolderId, r.TenantId, why); err != nil {
			return n, err
		}
		n++
	}

	return n, nil
}
