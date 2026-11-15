package e2e_test

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/satellite"
)

// startSatellite runs a satellite in this process (§41), with the state
// directory given so it can be started again as itself, and answers once
// the CP holds it alive: adopted, as `serve all` adopts the hosts that
// join it, and heard from.
func (c *cluster) startSatellite(name, stateDir string) (*satellite.Satellite, context.CancelFunc) {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := satellite.New(satellite.Config{
		StateDir:          stateDir,
		Cp:                "http://" + c.running.ClusterAddr,
		Dev:               true,
		HardwareId:        "test-" + name,
		Addr:              "127.0.0.1:0",
		HeartbeatInterval: time.Second,
		Log:               slog.Default().With("satellite", name),
	})
	require.NoError(c.t, err)
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx)
		close(done)
	}()
	select {
	case <-s.Ready:
	case err := <-done:
		cancel()
		c.t.Fatalf("satellite %s: %v", name, err)
	case <-time.After(30 * time.Second):
		cancel()
		c.t.Fatalf("satellite %s did not come up", name)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	c.t.Cleanup(stop)

	sats := api.NewSatelliteServiceClient(c.dialCluster(c.operator()))
	require.Eventually(c.t, func() bool {
		v, err := sats.Get(context.Background(), api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: s.Id().Bytes()}.Build()}.Build())
		return err == nil && v.GetState() == api.HostState_HOST_STATE_ADOPTED && v.GetStatus().GetInFlightBudget() > 0
	}, 30*time.Second, 200*time.Millisecond, "satellite %s is adopted and heartbeats", name)

	return s, stop
}

// recordInto runs a producer of the bench recording for a new set, with
// its uploads going the way `via` says, and adopts it.
func (c *cluster) recordInto(ctx context.Context, alias string, via producer.Via, edit ...func(*producer.Config)) (*producer.Producer, *api.Set, *api.Producer) {
	c.t.Helper()
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias,
	}.Build())
	require.NoError(c.t, err)
	cfg := producer.Config{
		StateDir:          filepath.Join(c.t.TempDir(), "producer-"+alias),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{{Alias: "door-" + alias, Input: "raw:" + samplePath(c.t), Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Upload:            producer.UploadConfig{Via: via, ResumeTimeout: 20 * time.Second},
	}
	for _, e := range edit {
		e(&cfg)
	}
	p, err := producer.New(cfg)
	require.NoError(c.t, err)
	go p.Run(ctx)
	row := adoptProducer(c.t, ctx, api.NewProducerServiceClient(admin), set)

	return p, set, row
}

// smallLaminae activates an upload policy of 1 MiB laminae, so the bench
// recording makes one every few seconds.
func (c *cluster) smallLaminae(ctx context.Context) {
	c.t.Helper()
	ops := api.NewUploadPolicyServiceClient(c.dialCluster(c.operator()))
	up, err := ops.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 512 << 10, TargetLamina: 1 << 20, MaxLamina: 4 << 20}.Build(),
	}.Build())
	require.NoError(c.t, err)
	_, err = ops.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(c.t, err)
}

// laminaeOf is every lamina of a set.
func (c *cluster) laminaeOf(ctx context.Context, set *api.Set) []*api.Lamina {
	c.t.Helper()
	vs, err := api.NewLaminaServiceClient(c.dial("@acme/admin")).List(ctx, api.LaminaListRequest_builder{
		Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
		Size:    200,
	}.Build())
	require.NoError(c.t, err)

	return vs.GetItems()
}

// committedBytes is how many laminae of a set are committed, and their
// bytes together; none may be lost.
func (c *cluster) committedBytes(ctx context.Context, set *api.Set) (int, int64) {
	c.t.Helper()
	var n int
	var size int64
	for _, o := range c.laminaeOf(ctx, set) {
		require.NotEqual(c.t, api.LaminaState_LAMINA_STATE_LOST, o.GetState(), "a lamina was lost")
		if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
			n++
			size += o.GetSize()
		}
	}

	return n, size
}

// TestSatelliteUpload is §41's upload proxy under `retain: committed`: a
// producer with `upload_via: satellite` is assigned the satellite and
// records through it, every byte of its committed laminae forwarded by the
// satellite to the node its tokens name, and what the node committed plays.
// A producer beside it on an old configuration, which says nothing of
// `upload_via`, is not assigned the satellite and uploads to the node
// directly, as it always has.
func TestSatelliteUpload(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	sat, _ := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))

	through, throughSet, throughRow := c.recordInto(ctx, "through", producer.ViaSatellite)
	direct, directSet, directRow := c.recordInto(ctx, "direct", "")
	require.Eventually(t, func() bool {
		a, _ := c.committedBytes(ctx, throughSet)
		b, _ := c.committedBytes(ctx, directSet)
		return a >= 3 && b >= 3
	}, 120*time.Second, 500*time.Millisecond, "both record")

	// Every byte of the satellite producer's committed laminae came through
	// the satellite.
	_, viaBytes := c.committedBytes(ctx, throughSet)
	require.GreaterOrEqual(t, sat.Forwarded(), viaBytes, "the satellite producer's laminae came through the satellite")
	cancel()

	// What the CP knows: the satellite serves the one producer, which says
	// its uploads go through it; the other has no satellite, so the
	// satellite would refuse it, and says nothing.
	cctx := context.Background()
	producers := api.NewProducerServiceClient(c.dial("@acme/admin"))
	v, err := producers.Get(cctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: throughRow.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, sat.Id().Bytes(), v.GetSatellite().GetId())
	require.Equal(t, "satellite", v.GetStatus().GetUpload().GetVia())
	require.Equal(t, sat.Id().Bytes(), v.GetStatus().GetUpload().GetSatelliteId())
	v, err = producers.Get(cctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: directRow.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Empty(t, v.GetSatellite().GetId())
	require.Nil(t, v.GetStatus().GetUpload())
	row, err := api.NewSatelliteServiceClient(c.dialCluster(c.operator())).Get(cctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, int32(1), row.GetStatus().GetProducers())
	require.Positive(t, row.GetStatus().GetForwardedBytes())
	require.NotEmpty(t, row.GetKeyIds(), "it holds the key set")
	used, peak, budget := sat.InFlight()
	require.Positive(t, peak)
	require.LessOrEqual(t, used, budget)

	// What came through plays, as anything a node committed does.
	laminae := api.NewLaminaServiceClient(c.dial("@acme/admin"))
	var src []byte
	for _, o := range c.laminaeOf(cctx, throughSet) {
		if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
			src = o.GetSource().GetId()
			break
		}
	}
	tl, err := laminae.Timeline(cctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: src}.Build(),
		From:   timestamppb.New(time.Now().Add(-10 * time.Minute)), To: timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	played := 0
	for _, o := range tl.GetSources()[0].GetLaminae() {
		if o.GetState() != api.ReadState_READ_STATE_AVAILABLE {
			continue
		}
		b := fetch(t, o.GetUrl())
		require.Equal(t, int64(len(b)), o.GetSize())
		playsOnItsOwn(t, b)
		played++
	}
	require.GreaterOrEqual(t, played, 3)
	require.Zero(t, through.Stats().Lost)
	require.Zero(t, direct.Stats().Lost)
}

// TestSatelliteDiesMidUpload is §41.6: under `retain: committed` the
// producer holds every segment until the node's 201, so a satellite that
// dies under a live upload loses nothing. The producer waits on it, the CP
// holds it down after satellite_down_after and assigns none, and the
// producer resumes on the node directly from the offset the node has; the
// satellite back, the producer is assigned it again and records through it.
func TestSatelliteDiesMidUpload(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Control.SatelliteDownAfter = 3 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	stateDir := filepath.Join(t.TempDir(), "sat")
	sat, stop := c.startSatellite("sat", stateDir)

	p, set, _ := c.recordInto(ctx, "dies", producer.ViaSatellite)
	require.Eventually(t, func() bool {
		return p.Stats().Stored >= 2 && sat.Forwarded() > 0
	}, 120*time.Second, 200*time.Millisecond, "recording through the satellite: %+v", p.Stats())

	// Live: a segment is always on its way. Kill the satellite under it.
	before := p.Stats().Stored
	stop()
	require.Eventually(t, func() bool { return p.Stats().Stored >= before+3 }, 120*time.Second, 500*time.Millisecond,
		"recording goes on directly once the CP assigns none: %+v", p.Stats())
	st := p.Stats()
	require.Zero(t, st.Lost, "nothing lost: %+v", st)
	require.Zero(t, st.Cut)
	for _, o := range c.laminaeOf(ctx, set) {
		if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
			require.False(t, o.GetIncomplete(), "every committed lamina is whole")
		}
	}

	// The satellite back, as itself: assigned again, and recording goes
	// through it.
	again, _ := c.startSatellite("sat", stateDir)
	require.Equal(t, sat.Id(), again.Id())
	require.Eventually(t, func() bool { return again.Forwarded() > 0 }, 60*time.Second, 500*time.Millisecond, "through the satellite again")
	stored := p.Stats().Stored
	require.Eventually(t, func() bool { return p.Stats().Stored >= stored+2 }, 60*time.Second, 500*time.Millisecond)
	require.Zero(t, p.Stats().Lost)
	n, _ := c.committedBytes(ctx, set)
	require.Positive(t, n)
}

// TestSatelliteAutoFallsBack is `upload_via: auto` (§41.5): when the
// satellite cannot be reached the producer uploads to the node directly at
// once, without waiting for the CP, which here still holds the satellite
// alive and assigned, and loses nothing.
func TestSatelliteAutoFallsBack(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Control.SatelliteDownAfter = 10 * time.Minute })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	sat, stop := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))

	p, set, row := c.recordInto(ctx, "auto", producer.ViaAuto)
	require.Eventually(t, func() bool {
		return p.Stats().Stored >= 2 && sat.Forwarded() > 0
	}, 120*time.Second, 200*time.Millisecond, "recording through the satellite: %+v", p.Stats())

	stop()
	before := p.Stats().Stored
	require.Eventually(t, func() bool { return p.Stats().Stored >= before+3 }, 90*time.Second, 500*time.Millisecond,
		"recording goes on directly: %+v", p.Stats())
	require.Zero(t, p.Stats().Lost)
	n, _ := c.committedBytes(ctx, set)
	require.GreaterOrEqual(t, n, 5)

	// The CP still names the satellite: the producer went directly by
	// itself.
	v, err := api.NewProducerServiceClient(c.dial("@acme/admin")).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: row.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, sat.Id().Bytes(), v.GetSatellite().GetId())
	require.Equal(t, "auto", v.GetStatus().GetUpload().GetVia())
}

// TestSatelliteUploadOverTLS is the satellite as a deployment runs it
// (§41.3): it joins the cluster API over mTLS, serves its upload listener
// with its host certificate, and lets in a producer by the certificate the
// CP issued it; the producer verifies the satellite against the CA it
// pinned, and the satellite the node, as a producer would. A client
// without a producer's certificate does not get past the handshake.
func TestSatelliteUploadOverTLS(t *testing.T) {
	c := startTLS(t)
	c.startNode("node", filepath.Join(t.TempDir(), "state"), filepath.Join(t.TempDir(), "sink"), "", false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fp := pki.Fingerprint(c.cp.CA.Cert)
	policies := api.NewUploadPolicyServiceClient(c.dial(c.cluster, "@cluster/ops"))
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 512 << 10, TargetLamina: 1 << 20, MaxLamina: 4 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

	sat, err := satellite.New(satellite.Config{
		StateDir: filepath.Join(t.TempDir(), "sat"), Cp: "https://" + c.cluster, CaHash: fp, HardwareId: "test-sat",
		Addr: "127.0.0.1:0", HeartbeatInterval: time.Second, Log: slog.Default().With("satellite", "tls"),
	})
	require.NoError(t, err)
	satDone := make(chan error, 1)
	go func() { satDone <- sat.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-satDone
	})
	select {
	case <-sat.Ready:
	case err := <-satDone:
		t.Fatalf("satellite: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the satellite did not come up")
	}

	admin := c.dial(c.tenant, "@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "tls"}.Build())
	require.NoError(t, err)
	p, err := producer.New(producer.Config{
		StateDir: t.TempDir(), Cp: "https://" + c.tenant, CaHash: fp,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + samplePath(t), Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Upload:            producer.UploadConfig{Via: producer.ViaSatellite},
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	require.Eventually(t, func() bool { return p.Stats().Stored >= 3 && sat.Forwarded() > 0 }, 120*time.Second, 300*time.Millisecond,
		"recording through the satellite over TLS")
	require.Zero(t, p.Stats().Lost)
	laminae := api.NewLaminaServiceClient(admin)
	var committed int64
	vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
		Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}, Size: 100,
	}.Build())
	require.NoError(t, err)
	for _, o := range vs.GetItems() {
		if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
			committed += o.GetSize()
		}
	}
	require.Positive(t, committed)
	require.GreaterOrEqual(t, sat.Forwarded(), committed, "every committed byte came through the satellite")

	// No certificate, no handshake.
	row, err := api.NewSatelliteServiceClient(c.dial(c.cluster, "@cluster/ops")).Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, int32(1), row.GetStatus().GetProducers())
	anon := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: c.pool}}}
	_, err = anon.Head("https://" + sat.Addr + "/laminae/x")
	require.Error(t, err)
}
