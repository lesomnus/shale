package producer

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	catalog "github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type lifecycleAdmissionSets struct {
	api.UnimplementedSetServiceServer
	calls, commits atomic.Int32
	commit         func()
}

func (s *lifecycleAdmissionSets) Negotiate(_ context.Context, r *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	s.calls.Add(1)
	if !r.GetPreview() {
		s.commits.Add(1)
		s.commit()
	}
	var agreed []*api.SourceAgreed
	for _, p := range r.GetSources() {
		agreed = append(agreed, api.SourceAgreed_builder{SourceId: p.GetSource().GetId(), Profile: api.SegmentProfile_builder{MaxBitrate: p.GetProfile().GetMaxBitrate(), DurationSeconds: 4, KeyframeIntervalMs: 1000}.Build(), MaxLength: 1 << 20}.Build())
	}
	return api.SetNegotiateResponse_builder{Sources: agreed, Link: r.GetLink(), ProfileVersion: 1}.Build(), nil
}

type lifecycleAdmissionSources struct {
	api.UnimplementedSourceServiceServer
	row *api.Source
}

func (s lifecycleAdmissionSources) Get(context.Context, *api.SourceGetRequest) (*api.Source, error) {
	return s.row, nil
}

// The unchanged capture keeps collecting while admission commits. Its queue
// can grow after preview; that growth must prevent a new capture even though
// the old queue still uses less than Buffer and CP admission succeeded.
func TestManagedLifecycleRechecksRAMBeforeStarting(t *testing.T) {
	for _, uplink := range []bool{false, true} {
		t.Run(map[bool]string{false: "queue grows during commit", true: "uplink fails before CP commit"}[uplink], func(t *testing.T) {
			p, err := New(Config{StateDir: t.TempDir(), SourceProvider: &catalog.Memory{}, Buffer: 2 << 20, Log: slog.New(slog.DiscardHandler)})
			require.NoError(t, err)
			p.id = pdid.New(22)
			tenant, setID, sourceID := pdid.New(1), pdid.New(7), pdid.New(8)
			p.set = api.Set_builder{Id: setID.Bytes(), Tenant: api.Tenant_builder{Id: tenant.Bytes()}.Build()}.Build()
			revision := catalog.Revision{AssignmentID: pdid.New(28).String(), Generation: 1, Catalog: 2}
			profile := catalog.Profile{Codec: "h264", Width: 160, Height: 90, FPS: 10, MaxBitrate: 1_000_000}
			keepSpec := catalog.Spec{BindingID: pdid.New(29).String(), SourceID: pdid.New(8).String(), Revision: 1, Alias: "keep", Input: "rtsp://127.0.0.1/keep", Profile: profile}
			newSpec := catalog.Spec{BindingID: pdid.New(29).String(), SourceID: sourceID.String(), Revision: 2, Alias: "new", Input: "rtsp://127.0.0.1/new", Profile: profile}
			keep := &source{cfg: SourceConfig{Alias: "keep", Input: keepSpec.Input, MaxBitrate: 1_128_000}, spec: &keepSpec, catalogRevision: revision, row: api.Source_builder{Id: mustParseID(t, keepSpec.SourceID)}.Build()}
			require.NoError(t, p.installSource(keep))
			sets := &lifecycleAdmissionSets{commit: func() {
				keep.mu.Lock()
				defer keep.mu.Unlock()
				keep.pending = []*Segment{NewSegment(time.Now(), make([]byte, 1536<<10))}
			}}
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			srv := grpc.NewServer()
			api.RegisterSetServiceServer(srv, sets)
			api.RegisterSourceServiceServer(srv, lifecycleAdmissionSources{row: api.Source_builder{Id: sourceID.Bytes(), Alias: "new", Set: api.Set_builder{Id: setID.Bytes()}.Build(), Tenant: api.Tenant_builder{Id: tenant.Bytes()}.Build(), Ordinal: 1}.Build()})
			go srv.Serve(lis)
			t.Cleanup(srv.Stop)
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			p.conn.Store(conn)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() { cancel(); p.stopSources(ctx, p.activeSources(), catalog.StopShutdown) })
			if uplink {
				p.cfg.Uplink = 1_000_000
			}
			err = p.reconcileSources(ctx, catalog.Snapshot{Scope: catalog.Scope{TenantID: tenant.String(), SetID: setID.String(), ProducerID: p.id.String(), NetworkID: pdid.New(26).String()}, Revision: revision, Sources: []catalog.Spec{keepSpec, newSpec}})
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			require.Same(t, keep, p.sourceByAlias("keep"), "unchanged capture stays installed")
			require.Nil(t, p.sourceByAlias("new"), "an unreserved capture cannot start or acknowledge")
			if uplink {
				require.Zero(t, sets.calls.Load())
			} else {
				require.Equal(t, int32(3), sets.calls.Load())
				require.Equal(t, int32(1), sets.commits.Load())
				require.Less(t, p.heldBytes(), p.cfg.Buffer, "having some free memory is insufficient")
			}
		})
	}
}

func mustParseID(t *testing.T, raw string) []byte {
	t.Helper()
	id, err := pdid.Parse(raw)
	require.NoError(t, err)
	return id.Bytes()
}

// The managed catalog loop wakes every second, and an unchanged snapshot
// is not reconciled again: reconciling negotiates with the CP (a preview,
// then a preview and a commit) and re-authorizes with the relay, which a
// managed edge did once a second for as long as it ran. A new generation
// is reconciled once.
func TestManagedCatalogReconcilesOnlyOnChange(t *testing.T) {
	tenant, setID := pdid.New(1), pdid.New(7)
	id := pdid.New(22)
	scope := catalog.Scope{TenantID: tenant.String(), SetID: setID.String(), ProducerID: id.String(), NetworkID: pdid.New(26).String()}
	assignment := pdid.New(28).String()
	snapshot := func(generation int64) catalog.Snapshot {
		return catalog.Snapshot{Scope: scope, Revision: catalog.Revision{AssignmentID: assignment, Generation: generation, Catalog: generation}}
	}
	first := snapshot(1)
	mem, err := catalog.NewMemory(first)
	require.NoError(t, err)
	p, err := New(Config{StateDir: t.TempDir(), SourceProvider: mem, Buffer: 2 << 20, Log: slog.New(slog.DiscardHandler)})
	require.NoError(t, err)
	p.id = id
	p.set = api.Set_builder{Id: setID.Bytes(), Tenant: api.Tenant_builder{Id: tenant.Bytes()}.Build()}.Build()
	p.catalog = mem
	p.acceptCatalog(first)

	sets := &lifecycleAdmissionSets{commit: func() {}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	api.RegisterSetServiceServer(srv, sets)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	p.conn.Store(conn)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		p.sourceLifecycles(ctx)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	require.Eventually(t, func() bool { return sets.calls.Load() == 3 }, 5*time.Second, 10*time.Millisecond, "the first snapshot is reconciled")
	time.Sleep(3500 * time.Millisecond)
	require.Equal(t, int32(3), sets.calls.Load(), "and not again while it stays the same")

	require.NoError(t, mem.Publish(ctx, snapshot(2)))
	require.Eventually(t, func() bool { return sets.calls.Load() == 6 }, 5*time.Second, 10*time.Millisecond, "a new generation is")
	time.Sleep(2500 * time.Millisecond)
	require.Equal(t, int32(6), sets.calls.Load(), "once")
}
