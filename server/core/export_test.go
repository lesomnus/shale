package core

import (
	"context"
	"crypto/ed25519"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

type exportTestServer struct {
	api.Server
	source   api.SourceServiceServer
	timeline api.LaminaServiceServer
}

func (s exportTestServer) Source() api.SourceServiceServer { return s.source }
func (s exportTestServer) Lamina() api.LaminaServiceServer { return s.timeline }

type exportTestSource struct {
	api.SourceServiceServer
	get func(context.Context, *api.SourceGetRequest) (*api.Source, error)
}

func (s exportTestSource) Get(ctx context.Context, req *api.SourceGetRequest) (*api.Source, error) {
	return s.get(ctx, req)
}

type exportTestTimeline struct {
	api.LaminaServiceServer
	timeline func(context.Context, *api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error)
}

func (s exportTestTimeline) Timeline(ctx context.Context, req *api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
	return s.timeline(ctx, req)
}

type exportFixture struct {
	core              Core
	ctx               context.Context
	now               time.Time
	key               token.Key
	source            *api.Source
	actor, tenant, id pdid.Id
	visible           bool
	loads             int
	row               *api.TimelineLamina
	timeline          func(*api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error)
}

func newExportFixture(t *testing.T) *exportFixture {
	t.Helper()
	f := &exportFixture{now: time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC), actor: pdid.New(DomHolder), tenant: pdid.New(DomTenant), id: pdid.New(DomSource), visible: true}
	var err error
	f.key, err = token.Generate("test-export")
	require.NoError(t, err)
	f.source = api.Source_builder{Id: f.id.Bytes(), Tenant: api.Tenant_builder{Id: f.tenant.Bytes()}.Build()}.Build()
	f.row = api.TimelineLamina_builder{LaminaId: pdid.New(DomLamina).Bytes(), DateStarted: timestamppb.New(f.now.Add(-time.Minute)), DateEnded: timestamppb.New(f.now), State: api.ReadState_READ_STATE_AVAILABLE, Url: "https://storage.example/laminae/test?token=old"}.Build()
	f.ctx = frame.Into(context.Background(), frame.New(f.actor, f.tenant, frame.Whole()).WithScope(frame.Only(f.tenant)))
	next := exportTestServer{
		source: exportTestSource{get: func(ctx context.Context, req *api.SourceGetRequest) (*api.Source, error) {
			fr, ok := frame.From(ctx)
			require.True(t, ok)
			require.Equal(t, f.actor, fr.Actor)
			require.False(t, fr.Scope.All())
			require.Equal(t, []pdid.Id{f.tenant}, fr.Scope.Ids())
			if !f.visible || string(req.GetRef().GetId()) != string(f.id.Bytes()) {
				return nil, status.Error(codes.NotFound, "source not visible")
			}

			return proto.CloneOf(f.source), nil
		}},
		timeline: exportTestTimeline{timeline: func(ctx context.Context, req *api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			fr, ok := frame.From(ctx)
			require.True(t, ok)
			require.Equal(t, f.actor, fr.Actor)
			require.Equal(t, []pdid.Id{f.tenant}, fr.Scope.Ids())
			require.Equal(t, f.id.Bytes(), req.GetSource().GetId())
			require.Equal(t, f.row.GetDateStarted().AsTime(), req.GetFrom().AsTime())
			require.Equal(t, f.row.GetDateEnded().AsTime(), req.GetTo().AsTime())
			require.Empty(t, callerOf(ctx), "the CP must not resolve storage using the browser's peer IP")
			f.loads++
			if f.timeline != nil {
				return f.timeline(req)
			}
			row := proto.CloneOf(f.row)
			row.SetUrl("https://storage.example/laminae/test?token=fresh")

			return api.LaminaTimelineResponse_builder{Sources: []*api.TimelineSource{api.TimelineSource_builder{SourceId: f.id.Bytes(), Laminae: []*api.TimelineLamina{row}}.Build()}}.Build(), nil
		}},
	}
	d := &Deps{Now: func() time.Time { return f.now }, ExportURL: func() string { return "/exports/recording.mp4" }}
	d.Keys = &Keys{now: d.Now, at: f.now, signing: &f.key, public: map[string]ed25519.PublicKey{f.key.Kid: f.key.Public()}}
	f.core = New(next, d)

	return f
}

// A committed row with no final date_ended is returned by Timeline's
// date_ended IS NULL query even when its estimated ending precedes the
// selected range. Such a row cannot be exported and must not poison a
// download that also contains a complete, available recording.
func incompleteExportRow(f *exportFixture) *api.TimelineLamina {
	row := proto.CloneOf(f.row)
	row.SetLaminaId(pdid.New(DomLamina).Bytes())
	row.SetIncomplete(true)
	row.SetEndedEstimated(true)
	row.SetDateStarted(timestamppb.New(f.now.Add(-2 * time.Hour)))
	row.SetDateEnded(timestamppb.New(f.now.Add(-2*time.Hour + time.Minute)))

	return row
}

func exportTimeline(f *exportFixture, rows []*api.TimelineLamina, next string) *api.LaminaTimelineResponse {
	return api.LaminaTimelineResponse_builder{
		Sources: []*api.TimelineSource{api.TimelineSource_builder{SourceId: f.id.Bytes(), Laminae: rows}.Build()},
		Next:    next,
	}.Build()
}

func TestExportSkipsIncompleteTimelineRows(t *testing.T) {
	f := newExportFixture(t)
	old := incompleteExportRow(f)
	f.timeline = func(*api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
		return exportTimeline(f, []*api.TimelineLamina{old, f.row}, ""), nil
	}
	ticket, _ := f.ticket(t, f.ctx)
	plan, err := f.core.ResolveExport(context.Background(), ticket)
	require.NoError(t, err, "an old incomplete estimated row is not a corrupt complete recording")
	require.Len(t, plan.Laminae, 1)
	require.Equal(t, f.row.GetLaminaId(), plan.Laminae[0].GetLaminaId())
	fresh, err := plan.Refresh(context.Background(), f.row.GetLaminaId())
	require.NoError(t, err, "the same incomplete Timeline rows must not prevent renewing an admitted recording")
	require.Equal(t, f.row.GetLaminaId(), fresh.GetLaminaId())
}

func TestExportWithoutUsableRecordingIsNotFound(t *testing.T) {
	for name, alter := range map[string]func(*api.TimelineLamina){
		"incomplete estimated": func(*api.TimelineLamina) {},
		"incomplete without dates": func(row *api.TimelineLamina) {
			row.SetDateStarted(nil)
			row.SetDateEnded(nil)
		},
		"unavailable": func(row *api.TimelineLamina) {
			row.SetIncomplete(false)
			row.SetState(api.ReadState_READ_STATE_UNAVAILABLE)
		},
		"without a read URL": func(row *api.TimelineLamina) {
			row.SetIncomplete(false)
			row.SetUrl("")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newExportFixture(t)
			row := incompleteExportRow(f)
			alter(row)
			f.timeline = func(*api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
				return exportTimeline(f, []*api.TimelineLamina{row}, ""), nil
			}
			ticket, _ := f.ticket(t, f.ctx)
			_, err := f.core.ResolveExport(context.Background(), ticket)
			require.Equal(t, codes.NotFound, status.Code(err), "a range without complete available bytes has no export")
		})
	}
}

func TestExportRejectsCompleteTimelineRowsOutsideRange(t *testing.T) {
	for name, alter := range map[string]func(*api.TimelineLamina, time.Time){
		"before": func(row *api.TimelineLamina, now time.Time) {
			row.SetDateStarted(timestamppb.New(now.Add(-2 * time.Hour)))
			row.SetDateEnded(timestamppb.New(now.Add(-time.Hour)))
		},
		"after": func(row *api.TimelineLamina, now time.Time) {
			row.SetDateStarted(timestamppb.New(now.Add(time.Minute)))
			row.SetDateEnded(timestamppb.New(now.Add(2 * time.Minute)))
		},
		"missing start": func(row *api.TimelineLamina, _ time.Time) { row.SetDateStarted(nil) },
		"missing end":   func(row *api.TimelineLamina, _ time.Time) { row.SetDateEnded(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newExportFixture(t)
			row := proto.CloneOf(f.row)
			alter(row, f.now)
			f.timeline = func(*api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
				return exportTimeline(f, []*api.TimelineLamina{row}, ""), nil
			}
			ticket, _ := f.ticket(t, f.ctx)
			_, err := f.core.ResolveExport(context.Background(), ticket)
			require.Equal(t, codes.Internal, status.Code(err), "complete available rows must still obey the admitted range")
		})
	}
}

func TestExportBoundsScannedRowsIncludingIncomplete(t *testing.T) {
	f := newExportFixture(t)
	old := incompleteExportRow(f)
	rows := make([]*api.TimelineLamina, 1000)
	for i := range rows {
		rows[i] = old
	}
	f.timeline = func(req *api.LaminaTimelineRequest) (*api.LaminaTimelineResponse, error) {
		if f.loads == 1 {
			require.Empty(t, req.GetAfter())
		} else {
			require.Equal(t, strconv.Itoa(f.loads-1), req.GetAfter())
		}
		return exportTimeline(f, rows, strconv.Itoa(f.loads)), nil
	}
	ticket, _ := f.ticket(t, f.ctx)
	_, err := f.core.ResolveExport(context.Background(), ticket)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "discarded rows still consume the metadata scanning budget")
	require.Equal(t, ExportMaxLaminae/len(rows)+1, f.loads)
}

func (f *exportFixture) request() *api.LaminaExportRequest {
	return api.LaminaExportRequest_builder{Source: api.SourceRef_builder{Id: f.id.Bytes()}.Build(), From: f.row.GetDateStarted(), To: f.row.GetDateEnded()}.Build()
}

func (f *exportFixture) ticket(t *testing.T, ctx context.Context) (string, *api.LaminaExportResponse) {
	t.Helper()
	v, err := f.core.Lamina().Export(ctx, f.request())
	require.NoError(t, err)
	u, err := url.Parse(v.GetUrl())
	require.NoError(t, err)
	require.Equal(t, "/exports/recording.mp4", u.Path)

	return u.Query().Get("token"), v
}

func TestExportTicketScopeAndRefresh(t *testing.T) {
	f := newExportFixture(t)
	app := pdid.New(DomHolder)
	// No direct API read grant is needed to redeem an admitted delegated
	// capability: the recording permission was proved at issuance.
	ctx := context.WithValue(f.ctx, delegatedKey{}, delegated{app: app, ttl: 2 * time.Minute, ends: f.now.Add(time.Minute)})
	ticket, response := f.ticket(t, ctx)
	require.Equal(t, f.now.Add(time.Minute), response.GetDateExpires().AsTime(), "bounded by the handle's end, too")
	c, err := token.Parse(ticket)
	require.NoError(t, err)
	require.Equal(t, api.TokenOp_TOKEN_OP_EXPORT, c.GetOp())
	require.Equal(t, f.id.Bytes(), c.GetSource())
	require.Equal(t, f.actor.Bytes(), c.GetActor())
	require.Equal(t, f.tenant.Bytes(), c.GetActorTenant())
	require.Equal(t, app.Bytes(), c.GetDelegator())
	require.Equal(t, f.request().GetFrom().AsTime(), c.GetExportFrom().AsTime())
	require.Equal(t, f.request().GetTo().AsTime(), c.GetExportTo().AsTime())

	// An unrelated browser frame, if one happens to be present, cannot
	// widen the capability; neither can its source address influence the
	// storage endpoint the CP will fetch.
	stranger := frame.Into(context.Background(), frame.New(pdid.New(DomHolder), pdid.New(DomTenant), frame.Whole()).WithScope(frame.Everything))
	stranger = peer.NewContext(stranger, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.123"), Port: 1234}})
	plan, err := f.core.ResolveExport(stranger, ticket)
	require.NoError(t, err)
	require.Equal(t, f.id.Bytes(), plan.Source.GetId())
	require.Equal(t, f.actor, plan.Actor)
	require.Equal(t, f.tenant, plan.Tenant)
	require.Equal(t, app, plan.Delegator)
	require.Len(t, plan.Laminae, 1)
	_, err = plan.Refresh(context.Background(), pdid.New(DomLamina).Bytes())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a job can renew only its frozen laminae")

	// Expiry gates admission. A separately time-bounded admitted job may
	// renew its own storage credentials after that expiry without widening.
	f.now = f.now.Add(2 * time.Minute)
	f.core.d.Keys.at = f.now
	_, err = f.core.ResolveExport(context.Background(), ticket)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	fresh, err := plan.Refresh(context.Background(), f.row.GetLaminaId())
	require.NoError(t, err)
	require.Contains(t, fresh.GetUrl(), "token=fresh")
	require.Equal(t, 2, f.loads)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = plan.Refresh(canceled, f.row.GetLaminaId())
	require.ErrorIs(t, err, context.Canceled)
}

func TestExportRejectsInvalidCapabilities(t *testing.T) {
	f := newExportFixture(t)
	ticket, response := f.ticket(t, f.ctx)
	require.Equal(t, f.now.Add(ExportTicketTTL), response.GetDateExpires().AsTime())
	claims, err := token.Parse(ticket)
	require.NoError(t, err)
	for _, bad := range []string{"", "garbage", strings.Repeat("x", (16<<10)+1), ticket + "x"} {
		_, err := f.core.ResolveExport(context.Background(), bad)
		require.Equal(t, codes.Unauthenticated, status.Code(err), bad[:min(12, len(bad))])
	}
	for name, alter := range map[string]func(*api.TokenClaims){
		"storage get":     func(c *api.TokenClaims) { c.SetOp(api.TokenOp_TOKEN_OP_GET) },
		"other audience":  func(c *api.TokenClaims) { c.SetAud([]byte("other")) },
		"no source":       func(c *api.TokenClaims) { c.SetSource(nil) },
		"no actor":        func(c *api.TokenClaims) { c.SetActor(nil) },
		"no tenant":       func(c *api.TokenClaims) { c.SetActorTenant(nil) },
		"no range":        func(c *api.TokenClaims) { c.SetExportFrom(nil) },
		"invalid range":   func(c *api.TokenClaims) { c.SetExportTo(c.GetExportFrom()) },
		"producer":        func(c *api.TokenClaims) { c.SetActor(pdid.New(DomProducer).Bytes()) },
		"relay delegator": func(c *api.TokenClaims) { c.SetDelegator(pdid.New(DomRelay).Bytes()) },
	} {
		t.Run(name, func(t *testing.T) {
			c := proto.CloneOf(claims)
			alter(c)
			signed, err := f.key.Sign(c)
			require.NoError(t, err)
			_, err = f.core.ResolveExport(context.Background(), signed)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
	f.visible = false
	_, err = f.core.ResolveExport(context.Background(), ticket)
	require.Equal(t, codes.NotFound, status.Code(err), "the source is still resolved through the current wall")
}

func TestExportAdmissionAndPolicy(t *testing.T) {
	f := newExportFixture(t)
	for name, alter := range map[string]func(*api.LaminaExportRequest){
		"no source":         func(r *api.LaminaExportRequest) { r.SetSource(nil) },
		"no from":           func(r *api.LaminaExportRequest) { r.SetFrom(nil) },
		"no to":             func(r *api.LaminaExportRequest) { r.SetTo(nil) },
		"invalid timestamp": func(r *api.LaminaExportRequest) { r.SetFrom(&timestamppb.Timestamp{Nanos: -1}) },
		"empty range":       func(r *api.LaminaExportRequest) { r.SetTo(r.GetFrom()) },
		"too long": func(r *api.LaminaExportRequest) {
			r.SetTo(timestamppb.New(r.GetFrom().AsTime().Add(ExportMaxRange + time.Second)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := f.request()
			alter(r)
			_, err := f.core.Lamina().Export(f.ctx, r)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
	f.source.SetTenant(api.Tenant_builder{Id: pdid.New(DomTenant).Bytes()}.Build())
	_, err := f.core.Lamina().Export(f.ctx, f.request())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the signed capability cannot cross the tenant wall")
	f.core.d.ExportURL = nil
	_, err = f.core.Lamina().Export(f.ctx, f.request())
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the resource-intensive service is opt-in")
	_, err = f.core.ResolveExport(context.Background(), "anything")
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	p := TenantPolicy{}
	for _, dom := range []pdid.Domain{DomHolder, DomReader} {
		require.NoError(t, p.May(context.Background(), gate.Call{Actor: pdid.New(dom), Tenant: f.tenant, Action: api.LaminaService_Export_FullMethodName}))
	}
	for _, dom := range []pdid.Domain{DomProducer, DomNode, DomRelay} {
		require.Equal(t, codes.PermissionDenied, status.Code(p.May(context.Background(), gate.Call{Actor: pdid.New(dom), Tenant: f.tenant, Action: api.LaminaService_Export_FullMethodName})))
	}
}
