package core

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/z"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// acceptFixture is a set recorded by a producer assigned to a satellite,
// another satellite, and laminae of the set to hold.
type acceptFixture struct {
	t        *testing.T
	core     Core
	sink     pd.Sink
	tenant   *api.TenantRef
	set      *api.Set
	src      *api.Source
	node     pdid.Id
	sinkId   pdid.Id
	ours     pdid.Id
	other    pdid.Id
	producer pdid.Id
	now      time.Time
}

func newAcceptFixture(t *testing.T) *acceptFixture {
	ctx := t.Context()
	cfg := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"}
	db, dialect, err := cfg.Open(ctx)
	require.NoError(t, err)
	driver := sql.OpenDB(dialect, db)
	client := enttest.NewClient(t, enttest.WithOptions(ent.Driver(driver)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	sink, err := pd.NewSink(client, bare.WithMinter(pd.Minter()))
	require.NoError(t, err)
	tenant, err := sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
	require.NoError(t, err)
	f := &acceptFixture{t: t, sink: sink, tenant: api.TenantRef_builder{Id: tenant.GetId()}.Build(), now: time.Now()}
	f.core = New(sink, &Deps{Ent: client, Own: sink, Drv: driver, Now: func() time.Time { return f.now }})

	addSat := func(alias string) pdid.Id {
		id := pdid.New(DomSatellite)
		_, err := sink.Satellite().Add(ctx, api.SatelliteAddRequest_builder{
			Id: id.Bytes(), Alias: alias, HardwareId: alias, State: api.HostState_HOST_STATE_ADOPTED,
			DateSeen: timestamppb.New(f.now), UploadAddress: "192.0.2.10:7460",
		}.Build())
		require.NoError(t, err)
		return id
	}
	f.ours, f.other = addSat("ours"), addSat("other")

	f.set, err = sink.Set().Add(ctx, api.SetAddRequest_builder{Tenant: f.tenant, Alias: "cams"}.Build())
	require.NoError(t, err)
	f.src, err = sink.Source().Add(ctx, api.SourceAddRequest_builder{Tenant: f.tenant, Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Alias: "door"}.Build())
	require.NoError(t, err)
	f.producer = pdid.New(DomProducer)
	_, err = sink.Producer().Add(ctx, api.ProducerAddRequest_builder{
		Id: f.producer.Bytes(), Tenant: f.tenant, Alias: "pi", HardwareId: "pi", State: api.HostState_HOST_STATE_ADOPTED,
		Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Satellite: api.SatelliteRef_builder{Id: f.ours.Bytes()}.Build(),
	}.Build())
	require.NoError(t, err)

	f.node = pdid.New(DomNode)
	_, err = sink.Node().Add(ctx, api.NodeAddRequest_builder{Id: f.node.Bytes(), Alias: "n1", HardwareId: "n1", State: api.HostState_HOST_STATE_ADOPTED}.Build())
	require.NoError(t, err)
	dev, err := sink.Device().Add(ctx, api.DeviceAddRequest_builder{Alias: "d1", HardwareId: "d1", Node: api.NodeRef_builder{Id: f.node.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	f.sinkId = pdid.New(DomSink)
	_, err = sink.Sink().Add(ctx, api.SinkAddRequest_builder{
		Id: f.sinkId.Bytes(), Alias: "s1", Node: api.NodeRef_builder{Id: f.node.Bytes()}.Build(), Device: api.DeviceRef_builder{Id: dev.GetId()}.Build(), Path: "/s1",
	}.Build())
	require.NoError(t, err)

	return f
}

// lamina is a PENDING lamina of the set with one open attempt, expiring
// in ttl.
func (f *acceptFixture) lamina(ttl time.Duration) (pdid.Id, pdid.Id) {
	f.t.Helper()
	ctx := f.t.Context()
	id, at := pdid.New(DomLamina), pdid.New(DomAttempt)
	_, err := f.sink.Lamina().Add(ctx, api.LaminaAddRequest_builder{
		Id: id.Bytes(), Tenant: f.tenant, Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Source: api.SourceRef_builder{Id: f.src.GetId()}.Build(),
		State: api.LaminaState_LAMINA_STATE_PENDING, DateStarted: timestamppb.New(f.now), LaminaKey: "laminae/" + id.String(),
	}.Build())
	require.NoError(f.t, err)
	_, err = f.sink.Attempt().Add(ctx, api.AttemptAddRequest_builder{
		Id: at.Bytes(), Tenant: f.tenant, Lamina: api.LaminaRef_builder{Id: id.Bytes()}.Build(),
		Sink: api.SinkRef_builder{Id: f.sinkId.Bytes()}.Build(), Node: api.NodeRef_builder{Id: f.node.Bytes()}.Build(),
		State: api.AttemptState_ATTEMPT_STATE_ALLOCATED, DateExpires: timestamppb.New(f.now.Add(ttl)),
	}.Build())
	require.NoError(f.t, err)

	return id, at
}

func (f *acceptFixture) get(id pdid.Id) *api.Lamina {
	f.t.Helper()
	v, err := f.sink.Lamina().Get(f.t.Context(), api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build()}.Build())
	require.NoError(f.t, err)

	return v
}

func (f *acceptFixture) attempt(id pdid.Id) *api.Attempt {
	f.t.Helper()
	v, err := f.sink.Attempt().Get(f.t.Context(), api.AttemptGetRequest_builder{Ref: api.AttemptRef_builder{Id: id.Bytes()}.Build()}.Build())
	require.NoError(f.t, err)

	return v
}

func as(id pdid.Id) context.Context {
	return frame.Into(context.Background(), frame.New(id, pdid.Nil, frame.Whole()))
}

// §41.9: a satellite records itself as the holder of a lamina of a
// producer it serves, and makes that producer's lamina calls, in its name
// and saying whose they are; for a producer it does not serve it does
// neither.
func TestSatelliteAcceptsForItsProducers(t *testing.T) {
	f := newAcceptFixture(t)
	id, at := f.lamina(time.Hour)

	_, err := f.core.Satellite().Accept(as(f.other), api.SatelliteAcceptRequest_builder{LaminaId: id.Bytes()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "not its producer's")
	_, err = f.core.Lamina().ReportAttempt(as(f.other), api.LaminaReportAttemptRequest_builder{
		Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Attempt: api.AttemptRef_builder{Id: at.Bytes()}.Build(), FailureReason: "x",
	}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = f.core.Lamina().ReportFailure(as(f.other), api.LaminaReportFailureRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Reason: "x"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = f.core.Lamina().Renew(as(f.other), api.LaminaRenewRequest_builder{
		Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Attempt: api.AttemptRef_builder{Id: at.Bytes()}.Build(),
	}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "renew")
	_, err = f.core.Lamina().Reallocate(as(f.other), api.LaminaReallocateRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "reallocate")
	require.Empty(t, f.get(id).GetSatellite().GetId())
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_ALLOCATED, f.attempt(at).GetState())

	// Its own producer's: recorded, and the calls say whose they are.
	v, err := f.core.Satellite().Accept(as(f.ours), api.SatelliteAcceptRequest_builder{LaminaId: id.Bytes()}.Build())
	require.NoError(t, err)
	require.Equal(t, f.producer.Bytes(), v.GetProducerId())
	require.Equal(t, f.ours.Bytes(), f.get(id).GetSatellite().GetId())
	_, err = f.core.Lamina().ReportAttempt(as(f.ours), api.LaminaReportAttemptRequest_builder{
		Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Attempt: api.AttemptRef_builder{Id: at.Bytes()}.Build(), FailureReason: "refused: 403",
	}.Build())
	require.NoError(t, err)
	a := f.attempt(at)
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_FAILED, a.GetState())
	require.Equal(t, "satellite ours, for its producer: refused: 403", a.GetFailureReason())

	id2, at2 := f.lamina(time.Hour)
	_, err = f.core.Lamina().ReportFailure(as(f.ours), api.LaminaReportFailureRequest_builder{Ref: api.LaminaRef_builder{Id: id2.Bytes()}.Build(), Reason: "could not deliver"}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_LOST, f.get(id2).GetState())
	require.Equal(t, "satellite ours, for its producer: could not deliver", f.attempt(at2).GetFailureReason())

	// A lamina past PENDING is no longer one to hold.
	_, err = f.core.Satellite().Accept(as(f.ours), api.SatelliteAcceptRequest_builder{LaminaId: id2.Bytes()}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// Moved to another satellite: the first acts for it no more.
	_, err = f.sink.Producer().Patch(t.Context(), api.ProducerPatchRequest_builder{
		Ref: api.ProducerRef_builder{Id: f.producer.Bytes()}.Build(), Satellite: api.SatelliteRef_builder{Id: f.other.Bytes()}.Build(), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	id3, _ := f.lamina(time.Hour)
	_, err = f.core.Satellite().Accept(as(f.ours), api.SatelliteAcceptRequest_builder{LaminaId: id3.Bytes()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

// What a satellite held when it restarted is LOST at its next heartbeat,
// attributed to it, but what the new process says it holds (§41.9); what
// a satellite that is down held is LOST once no attempt of it is open.
// Laminae nobody holds are not touched.
func TestSatelliteLossIsItsOwn(t *testing.T) {
	f := newAcceptFixture(t)
	ctx := t.Context()
	held, heldAt := f.lamina(time.Hour)
	kept, _ := f.lamina(time.Hour)
	loose, _ := f.lamina(time.Hour)
	for _, id := range []pdid.Id{held, kept} {
		_, err := f.core.Satellite().Accept(as(f.ours), api.SatelliteAcceptRequest_builder{LaminaId: id.Bytes()}.Build())
		require.NoError(t, err)
	}

	last := api.SatelliteStatus_builder{Incarnation: "one", Held: [][]byte{held.Bytes(), kept.Bytes()}}.Build()
	f.core.satelliteRestarted(ctx, f.ours, last, api.SatelliteStatus_builder{Incarnation: "one", Held: [][]byte{held.Bytes()}}.Build())
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(held).GetState(), "the same process: nothing lost")
	f.core.satelliteRestarted(ctx, f.ours, last, api.SatelliteStatus_builder{Held: nil}.Build())
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(held).GetState(), "a satellite that says no incarnation says nothing")

	f.core.satelliteRestarted(ctx, f.ours, last, api.SatelliteStatus_builder{Incarnation: "two", Held: [][]byte{kept.Bytes()}}.Build())
	v := f.get(held)
	require.Equal(t, api.LaminaState_LAMINA_STATE_LOST, v.GetState())
	require.Equal(t, f.ours.Bytes(), v.GetSatellite().GetId(), "lost by the satellite that held it")
	a := f.attempt(heldAt)
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_FAILED, a.GetState())
	require.Contains(t, a.GetFailureReason(), "satellite ours lost what it accepted: it restarted")
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(kept).GetState(), "the new process holds it")
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(loose).GetState())

	// Down: nothing while the attempt it holds is still good; LOST once it
	// is not. An alive satellite's laminae stay.
	gone, _ := f.lamina(10 * time.Minute)
	_, err := f.core.Satellite().Accept(as(f.ours), api.SatelliteAcceptRequest_builder{LaminaId: gone.Bytes()}.Build())
	require.NoError(t, err)
	require.NoError(t, f.core.satellitesDown(ctx, f.now))
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(gone).GetState(), "alive")
	f.now = f.now.Add(DefaultSatelliteDownAfter + time.Second)
	require.NoError(t, f.core.satellitesDown(ctx, f.now))
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(gone).GetState(), "down, but its token is good")
	f.now = f.now.Add(10 * time.Minute)
	require.NoError(t, f.core.satellitesDown(ctx, f.now))
	require.Equal(t, api.LaminaState_LAMINA_STATE_LOST, f.get(gone).GetState())
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(loose).GetState())
}

// The cluster API takes a satellite's delegated lamina calls and Accept,
// and the core layer decides for which laminae (§41.9).
func TestSatelliteMayCallForItsProducers(t *testing.T) {
	p := ClusterPolicy{}
	sat := pdid.New(DomSatellite)
	for _, m := range []string{
		api.SatelliteService_Accept_FullMethodName,
		api.LaminaService_Renew_FullMethodName, api.LaminaService_ReportAttempt_FullMethodName,
		api.LaminaService_Reallocate_FullMethodName, api.LaminaService_ReportFailure_FullMethodName,
	} {
		require.NoError(t, p.May(context.Background(), gate.Call{Actor: sat, Action: m}), m)
	}
	for _, m := range []string{
		api.LaminaService_Allocate_FullMethodName, api.LaminaService_Skip_FullMethodName, api.LaminaService_Get_FullMethodName,
		api.LaminaService_List_FullMethodName, api.LaminaService_Patch_FullMethodName,
	} {
		require.Error(t, p.May(context.Background(), gate.Call{Actor: sat, Action: m}), m)
	}
}
