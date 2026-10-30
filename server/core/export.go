package core

import (
	"context"
	"crypto/ed25519"
	"net/url"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

const (
	// ExportTicketTTL gates the start of a native download. The HTTP
	// exporter separately bounds how long an admitted job may run.
	ExportTicketTTL = 5 * time.Minute
	// ExportMaxRange bounds planning work, not the output file's size.
	ExportMaxRange = 7 * 24 * time.Hour
	// ExportMaxLaminae bounds the metadata retained by one admitted job.
	ExportMaxLaminae = 20000
	exportAudience   = "shale:mp4-export"
)

// ExportPlan is a verified, source-scoped capability admitted by
// ResolveExport. Its URLs are fresh storage read capabilities, never
// network addresses supplied by the caller. An admitted HTTP job must bind
// its context to a separate bounded execution deadline.
type ExportPlan struct {
	Source                   *api.Source
	From, To, Expires        time.Time
	Actor, Tenant, Delegator pdid.Id
	Laminae                  []*api.TimelineLamina
	refresh                  func(context.Context, []byte) (*api.TimelineLamina, error)
}

// Refresh renews the read URL of a lamina already admitted in this plan.
// It neither admits another source/range nor checks ticket expiry again:
// expiry gates admission, while the HTTP job's deadline bounds execution.
func (p *ExportPlan) Refresh(ctx context.Context, laminaID []byte) (*api.TimelineLamina, error) {
	if p == nil || p.refresh == nil {
		return nil, status.Error(codes.FailedPrecondition, "not an admitted export")
	}

	return p.refresh(ctx, laminaID)
}

func (s Core) exportURL() (string, error) {
	if s.d == nil || s.d.ExportURL == nil || s.d.ExportURL() == "" {
		return "", status.Error(codes.FailedPrecondition, "native MP4 export is not enabled here (export.enabled)")
	}
	if s.d.Keys == nil {
		return "", status.Error(codes.FailedPrecondition, "native MP4 export needs the signing key ring")
	}

	return s.d.ExportURL(), nil
}

func exportRange(from, to *timestamppb.Timestamp) (time.Time, time.Time, error) {
	if from == nil || to == nil || from.CheckValid() != nil || to.CheckValid() != nil {
		return time.Time{}, time.Time{}, invalid("range", "valid from and to timestamps are required")
	}
	f, t := from.AsTime(), to.AsTime()
	if !t.After(f) {
		return time.Time{}, time.Time{}, invalid("to", "must be after from")
	}
	if t.Sub(f) > ExportMaxRange {
		return time.Time{}, time.Time{}, invalid("to", "an export may span at most seven days")
	}

	return f, t, nil
}

// Export proves visibility now, then hands out only an exact source/range
// capability. It does not inspect media or allocate temporary media files.
func (s coreLamina) Export(ctx context.Context, req *api.LaminaExportRequest) (*api.LaminaExportResponse, error) {
	base, err := s.exportURL()
	if err != nil {
		return nil, err
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomHolder && kindOf(f.Actor) != DomReader {
		return nil, denied(api.LaminaService_Export_FullMethodName, "only a person or reader exports recordings")
	}
	if req.GetSource() == nil {
		return nil, invalid("source", "one source is required")
	}
	from, to, err := exportRange(req.GetFrom(), req.GetTo())
	if err != nil {
		return nil, err
	}
	src, err := s.Next().Source().Get(ctx, api.SourceGetRequest_builder{Ref: req.GetSource()}.Build())
	if err != nil {
		return nil, err
	}
	if string(src.GetTenant().GetId()) != string(f.Tenant.Bytes()) {
		return nil, denied(api.LaminaService_Export_FullMethodName, "export on the source's tenant API")
	}
	now := s.d.now()
	claims := api.TokenClaims_builder{
		Iat:        timestamppb.New(now),
		Aud:        []byte(exportAudience),
		Op:         api.TokenOp_TOKEN_OP_EXPORT,
		Source:     src.GetId(),
		ExportFrom: timestamppb.New(from),
		ExportTo:   timestamppb.New(to),
	}
	claimsFor(ctx, f, &claims, now, now.Add(ExportTicketTTL))
	if !claims.Exp.AsTime().After(now) {
		return nil, status.Error(codes.PermissionDenied, "the delegation has ended")
	}
	signed, err := s.d.Keys.Sign(ctx, claims.Build())
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(base)
	if err != nil || u.Path == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.IsAbs() && u.Scheme != "https" && !(s.d.Dev && u.Scheme == "http")) || (!u.IsAbs() && u.Host != "") {
		return nil, status.Error(codes.FailedPrecondition, "export.public_url is not a valid HTTPS URL or relative endpoint path")
	}
	q := u.Query()
	q.Set("token", signed)
	u.RawQuery = q.Encode()

	return api.LaminaExportResponse_builder{Url: u.String(), DateExpires: claims.Exp}.Build(), nil
}

// Export uses the existing delegated recording permission and bounds. A
// person who may read the recording through an app may download its bytes;
// no new roster role is needed for that equivalent operation.
func (s coreDelegation) Export(ctx context.Context, req *api.DelegationExportRequest) (*api.LaminaExportResponse, error) {
	row, holder, pc, err := s.use(ctx, req.GetHandle(), delegatedTimeline)
	if err != nil {
		return nil, err
	}
	ex := proto.CloneOf(req.GetExport())
	if ex == nil || ex.GetSource() == nil {
		return nil, invalid("export.source", "an export request for one source is required")
	}
	if _, _, err := exportRange(ex.GetFrom(), ex.GetTo()); err != nil {
		return nil, err
	}
	if t := row.RecordingsFrom; t != nil && ex.GetFrom().AsTime().Before(*t) {
		ex.SetFrom(timestamppb.New(*t))
	}
	if t := row.RecordingsTo; t != nil && ex.GetTo().AsTime().After(*t) {
		ex.SetTo(timestamppb.New(*t))
	}
	if !ex.GetTo().AsTime().After(ex.GetFrom().AsTime()) {
		return nil, denied(delegatedTimeline, "a time this delegation does not reach")
	}
	src, err := s.Next().Source().Get(pc, api.SourceGetRequest_builder{Ref: ex.GetSource()}.Build())
	if err != nil {
		return nil, err
	}
	if !within(row, src.GetSet().GetId(), src.GetId()) {
		return nil, denied(delegatedTimeline, "a camera this delegation does not name")
	}
	ex.SetSource(api.SourceRef_builder{Id: src.GetId()}.Build())
	out, err := s.Core.Lamina().Export(pc, ex)
	if err != nil {
		return nil, err
	}
	s.issued(row, holder, api.DelegationService_Export_FullMethodName, 1)

	return out, nil
}

// ResolveExport verifies a bearer capability and resolves only its frozen
// source/range through the wall. Construct this Core over the Walled server
// (not Ungated); the native HTTP route requires no cookie, certificate or
// app exchange credential beside this ticket. Current site visibility and
// storage availability still apply. Grant/standing changes and revocation
// cannot admit a new ticket; an issued one can start until its short expiry.
func (s Core) ResolveExport(ctx context.Context, ticket string) (*ExportPlan, error) {
	if _, err := s.exportURL(); err != nil {
		return nil, err
	}
	if len(ticket) == 0 || len(ticket) > 16<<10 {
		return nil, status.Error(codes.Unauthenticated, "invalid export ticket")
	}
	keys, err := s.d.Keys.Public(ctx)
	if err != nil {
		return nil, err
	}
	pub := make(map[string]ed25519.PublicKey, len(keys))
	for _, key := range keys {
		pub[key.GetKid()] = ed25519.PublicKey(key.GetPublicKey())
	}
	v := token.NewVerifier()
	v.Now, v.Skew = s.d.now, 0
	v.Set(pub)
	c, err := v.Verify(ticket)
	if err != nil || c == nil || c.GetExp() == nil || !s.d.now().Before(c.GetExp().AsTime()) {
		return nil, status.Error(codes.Unauthenticated, "invalid or expired export ticket")
	}
	if err := token.Check(c, []byte(exportAudience), api.TokenOp_TOKEN_OP_EXPORT); err != nil {
		return nil, status.Error(codes.PermissionDenied, "not an export ticket")
	}
	from, to, err := exportRange(c.GetExportFrom(), c.GetExportTo())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "the export ticket has no valid range")
	}
	source, e1 := pdid.From(c.GetSource())
	actor, e2 := pdid.From(c.GetActor())
	tenant, e3 := pdid.From(c.GetActorTenant())
	if e1 != nil || e2 != nil || e3 != nil || source.Domain() != DomSource || tenant.Domain() != DomTenant || (actor.Domain() != DomHolder && actor.Domain() != DomReader) {
		return nil, status.Error(codes.PermissionDenied, "the export ticket names no source, actor or tenant")
	}
	delegator := pdid.Nil
	if len(c.GetDelegator()) > 0 {
		delegator, err = pdid.From(c.GetDelegator())
		if err != nil || delegator.Domain() != DomHolder {
			return nil, status.Error(codes.PermissionDenied, "the export ticket names no delegator")
		}
	}
	// The signature vouches for recording permission at issuance. Do not
	// run generic person policy here: EveryPerson delegations intentionally
	// serve somebody who has no direct API read grant. The wall stays in
	// place, narrowed to this tenant and the canonical source below.
	f := frame.New(actor, tenant, frame.Whole().In(tenant)).WithScope(frame.Only(tenant))
	trusted := func(ctx context.Context) context.Context {
		ctx = frame.Into(ctx, f)
		// These endpoints are fetched by the CP. They must not be selected
		// using the browser's network address carried by a gRPC peer.
		ctx = peer.NewContext(ctx, &peer.Peer{})
		if !delegator.IsZero() {
			ctx = context.WithValue(ctx, delegatedKey{}, delegated{app: delegator, ttl: s.d.DelegationConfig.tokenTtl()})
		}

		return ctx
	}
	src, err := s.Next().Source().Get(trusted(ctx), api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: source.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if string(src.GetTenant().GetId()) != string(tenant.Bytes()) {
		return nil, denied(api.LaminaService_Export_FullMethodName, "the source is not in the ticket's tenant")
	}
	load := func(ctx context.Context, a, b time.Time) ([]*api.TimelineLamina, error) {
		return s.exportLaminae(trusted(ctx), source, a, b)
	}
	rows, err := load(ctx, from, to)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, status.Error(codes.NotFound, "no available recording in the export range")
	}
	allowed := make(map[string]*api.TimelineLamina, len(rows))
	for _, row := range rows {
		allowed[string(row.GetLaminaId())] = proto.CloneOf(row)
	}
	plan := &ExportPlan{Source: src, From: from, To: to, Expires: c.GetExp().AsTime(), Actor: actor, Tenant: tenant, Delegator: delegator, Laminae: rows}
	plan.refresh = func(ctx context.Context, id []byte) (*api.TimelineLamina, error) {
		old, ok := allowed[string(id)]
		if !ok {
			return nil, denied(api.LaminaService_Export_FullMethodName, "a lamina not admitted in this export")
		}
		a, b := old.GetDateStarted().AsTime(), old.GetDateEnded().AsTime()
		if a.Before(from) {
			a = from
		}
		if b.After(to) {
			b = to
		}
		fresh, err := load(ctx, a, b)
		if err != nil {
			return nil, err
		}
		for _, row := range fresh {
			if string(row.GetLaminaId()) == string(id) {
				return row, nil
			}
		}

		return nil, status.Error(codes.NotFound, "the admitted recording is no longer available")
	}

	return plan, nil
}

func (s Core) exportLaminae(ctx context.Context, source pdid.Id, from, to time.Time) ([]*api.TimelineLamina, error) {
	var rows []*api.TimelineLamina
	scanned := 0
	after := ""
	for {
		v, err := s.Next().Lamina().Timeline(ctx, api.LaminaTimelineRequest_builder{
			Source: api.SourceRef_builder{Id: source.Bytes()}.Build(), From: timestamppb.New(from), To: timestamppb.New(to), Size: 1000, After: after,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, src := range v.GetSources() {
			if string(src.GetSourceId()) != string(source.Bytes()) {
				return nil, status.Error(codes.Internal, "export timeline answered another source")
			}
			for _, row := range src.GetLaminae() {
				// Timeline also returns incomplete recordings. A NULL stored
				// end can select an old row whose estimated end precedes this
				// window. Such rows are never export inputs: exclude them
				// before validating the complete recordings' range.
				// Count them anyway so filtering cannot bypass the work bound.
				scanned++
				if scanned > ExportMaxLaminae {
					return nil, status.Error(codes.ResourceExhausted, "export spans too many recording segments; use a shorter range")
				}
				if row.GetState() != api.ReadState_READ_STATE_AVAILABLE || row.GetIncomplete() || row.GetUrl() == "" {
					continue
				}
				if row.GetDateStarted() == nil || row.GetDateEnded() == nil || !row.GetDateStarted().AsTime().Before(to) || !row.GetDateEnded().AsTime().After(from) {
					return nil, status.Error(codes.Internal, "export timeline answered a recording outside its range")
				}
				rows = append(rows, row)
			}
		}
		if v.GetNext() == "" {
			return rows, nil
		}
		if v.GetNext() == after {
			return nil, status.Error(codes.Internal, "export timeline cursor did not advance")
		}
		after = v.GetNext()
	}
}
