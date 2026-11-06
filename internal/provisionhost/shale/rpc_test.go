package shale

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type producerRPC struct {
	api.UnimplementedProducerServiceServer
	row *api.Producer
}

func (s *producerRPC) Get(context.Context, *api.ProducerGetRequest) (*api.Producer, error) {
	return s.row, nil
}

type setRPC struct {
	api.UnimplementedSetServiceServer
	row *api.Set
}

func (s *setRPC) Get(context.Context, *api.SetGetRequest) (*api.Set, error) { return s.row, nil }

type sourceRPC struct {
	api.UnimplementedSourceServiceServer
	rows          []*api.Source
	live          *api.LiveSource
	tenant        []byte
	filterChecked bool
}

func (s *sourceRPC) List(_ context.Context, q *api.SourceListRequest) (*api.SourceListResponse, error) {
	s.filterChecked = len(q.GetFilters()) == 1 && string(q.GetFilters()[0].GetTenant().GetId()) == string(s.tenant)
	return api.SourceListResponse_builder{Items: s.rows}.Build(), nil
}
func (s *sourceRPC) Live(context.Context, *api.SourceLiveRequest) (*api.SourceLiveResponse, error) {
	return api.SourceLiveResponse_builder{Source: s.live}.Build(), nil
}

type laminaRPC struct {
	api.UnimplementedLaminaServiceServer
	rows []*api.Lamina
}

func (s *laminaRPC) List(context.Context, *api.LaminaListRequest) (*api.LaminaListResponse, error) {
	return api.LaminaListResponse_builder{Items: s.rows}.Build(), nil
}

type rpcFixture struct {
	authority *RPCAuthority
	dep       provision.Deployment
	binding   provision.BindingState
	producer  *producerRPC
	set       *setRPC
	source    *sourceRPC
	lamina    *laminaRPC
	liveCalls *int
}

func newRPCFixture(t *testing.T) rpcFixture {
	t.Helper()
	now := time.Now().Add(-2 * time.Second).UTC()
	tenant := pdid.New(1)
	set := pdid.New(7)
	producer := pdid.New(22)
	source := pdid.New(8)
	tenantRow := api.Tenant_builder{Id: tenant.Bytes()}.Build()
	setRow := api.Set_builder{Id: set.Bytes(), Tenant: tenantRow}.Build()
	pr := &producerRPC{row: api.Producer_builder{Id: producer.Bytes(), Tenant: tenantRow, Set: setRow, State: api.HostState_HOST_STATE_ADOPTED, Status: api.ProducerStatus_builder{DateReported: timestamppb.New(time.Now()), Sources: []*api.SourceReport{api.SourceReport_builder{SourceId: source.Bytes(), InputUp: true, FrameRate: 30, MeasuredBitrate: 2_000_000}.Build()}}.Build()}.Build()}
	sr := &sourceRPC{tenant: tenant.Bytes(), rows: []*api.Source{api.Source_builder{Id: source.Bytes(), Tenant: tenantRow, Set: setRow, Alias: "cam", Labels: map[string]string{BindingLabel: "front-door", DeviceLabel: "physical"}}.Build()}, live: api.LiveSource_builder{SourceId: source.Bytes(), WhepUrl: "https://relay/whep/source", ViewToken: "do-not-log", DateExpires: timestamppb.New(time.Now().Add(time.Minute))}.Build()}
	lr := &laminaRPC{rows: []*api.Lamina{api.Lamina_builder{Id: pdid.New(9).Bytes(), Tenant: tenantRow, Set: setRow, Source: sr.rows[0], State: api.LaminaState_LAMINA_STATE_COMMITTED, Size: 1234, DateStarted: timestamppb.New(now.Add(time.Second)), DateEnded: timestamppb.New(now.Add(1500 * time.Millisecond)), DateCommitted: timestamppb.New(time.Now())}.Build()}}
	st := &setRPC{row: setRow}
	g := grpc.NewServer()
	api.RegisterProducerServiceServer(g, pr)
	api.RegisterSetServiceServer(g, st)
	api.RegisterSourceServiceServer(g, sr)
	api.RegisterLaminaServiceServer(g, lr)
	lis := bufconn.Listen(1 << 20)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///provision-cp", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	liveCalls := new(int)
	a := &RPCAuthority{Conn: conn, PollInterval: time.Millisecond, ObserveTimeout: 25 * time.Millisecond, LiveCheck: func(context.Context, *api.LiveSource) error { *liveCalls++; return nil }}
	scope := camera.EnrollmentScope{TenantID: tenant.String(), SetID: set.String(), ProducerID: producer.String(), NetworkID: "lan"}
	d := provision.Deployment{Request: provision.Request{Scope: scope, BindingID: "front-door", Device: camera.DeviceHint{ID: "physical"}}}
	return rpcFixture{a, d, provision.BindingState{BindingID: "front-door", Alias: "cam", DeployedAt: now}, pr, st, sr, lr, liveCalls}
}
func TestRPCAuthorityRequiresProducerIngestLiveAndNewCompleteCommit(t *testing.T) {
	f := newRPCFixture(t)
	ctx := context.Background()
	require.NoError(t, f.authority.Check(ctx, f.dep, "cam"))
	require.True(t, f.source.filterChecked)
	b, err := f.authority.Observe(ctx, f.dep, f.binding)
	require.NoError(t, err)
	require.True(t, b.Ingest && b.Live && b.Committed)
	require.NotEmpty(t, b.SourceID)
	require.Equal(t, 1, *f.liveCalls)
}
func TestRPCAuthorityRejectsWrongOwnerAliasAndPhysicalDevice(t *testing.T) {
	t.Run("producer set", func(t *testing.T) {
		f := newRPCFixture(t)
		f.producer.row.SetSet(api.Set_builder{Id: pdid.New(7).Bytes()}.Build())
		require.ErrorIs(t, f.authority.Check(context.Background(), f.dep, "cam"), camera.ErrForbidden)
	})
	t.Run("tenant wide alias collision", func(t *testing.T) {
		f := newRPCFixture(t)
		f.source.rows[0].SetSet(api.Set_builder{Id: pdid.New(7).Bytes()}.Build())
		require.ErrorIs(t, f.authority.Check(context.Background(), f.dep, "cam"), provision.ErrConflict)
		require.True(t, f.source.filterChecked)
	})
	t.Run("unclaimed alias", func(t *testing.T) {
		f := newRPCFixture(t)
		f.source.rows[0].SetLabels(nil)
		require.ErrorIs(t, f.authority.Check(context.Background(), f.dep, "cam"), provision.ErrConflict)
	})
	t.Run("replacement at same address", func(t *testing.T) {
		f := newRPCFixture(t)
		f.dep.Request.Device.ID = "different-physical"
		require.ErrorIs(t, f.authority.Check(context.Background(), f.dep, "cam"), provision.ErrConflict)
	})
}
func TestRPCAuthorityMetadataOldOrIncompleteRecordingNotReady(t *testing.T) {
	tests := map[string]func(rpcFixture){
		"metadata only": func(f rpcFixture) { f.producer.row.SetStatus(nil) },
		"old producer report": func(f rpcFixture) {
			f.producer.row.GetStatus().SetDateReported(timestamppb.New(time.Now().Add(-time.Minute)))
		},
		"old archive":     func(f rpcFixture) { f.lamina.rows[0].SetDateStarted(timestamppb.New(time.Now().Add(-time.Hour))) },
		"incomplete":      func(f rpcFixture) { f.lamina.rows[0].SetIncomplete(true) },
		"pending":         func(f rpcFixture) { f.lamina.rows[0].SetState(api.LaminaState_LAMINA_STATE_PENDING) },
		"empty recording": func(f rpcFixture) { f.lamina.rows[0].SetSize(0) },
		"live delivery failed": func(f rpcFixture) {
			f.authority.LiveCheck = func(context.Context, *api.LiveSource) error { return provision.ErrNotReady }
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			f := newRPCFixture(t)
			change(f)
			b, err := f.authority.Observe(context.Background(), f.dep, f.binding)
			require.ErrorIs(t, err, provision.ErrNotReady)
			require.False(t, b.Ingest && b.Live && b.Committed)
		})
	}
}
