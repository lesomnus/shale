package e2e_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	credentialcp "github.com/lesomnus/shale/camera/credential/cp"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/source"
	catalogcp "github.com/lesomnus/shale/source/cp"
	"github.com/stretchr/testify/require"
)

// This focused RAM-accounting check complements the default assembled network
// workflow. Operator publication uses the existing authority fixture; real RTSP,
// live decoding, streaming upload retries, loss and MP4 recovery stay actual.
// RSS is deliberately not substituted for the retained-video byte counter.
func TestEdgeRAMBudgetLimitsUnstoredMediaAndRecovers(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	policies := api.NewUploadPolicyServiceClient(c.dialCluster("@cluster/ops"))
	policy, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{Alias: "bounded-ram", Version: 1, Bounds: api.UploadBounds_builder{MinLamina: 64 << 10, TargetLamina: 128 << 10, MaxLamina: 512 << 10}.Build()}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: policy.GetId()}.Build()}.Build())
	require.NoError(t, err)
	set := sourceCatalogSet(t, admin, "bounded-ram")
	fault := &lifecycleUploadFault{limits: map[string]int64{}}
	fault.target.Store("")
	const budget = int64(1 << 20)
	logs := &lifecycleLog{}
	t.Cleanup(func() {
		if t.Failed() {
			text := logs.text()
			t.Logf("RAM qualification admission_wait=%v capture_failed=%v catalog_wait=%v", strings.Contains(text, "retained RAM"), strings.Contains(text, "capture failed"), strings.Contains(text, "catalog"))
		}
	})
	p, err := producer.New(producer.Config{
		StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true,
		Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: time.Second, HeartbeatInterval: 200 * time.Millisecond,
		Buffer: budget, DrainTimeout: 5 * time.Second, Log: slog.New(slog.NewTextHandler(logs, nil)),
		Upload: producer.UploadConfig{Client: &http.Client{Transport: fault}, ResumeTimeout: time.Second, RetryAfterCap: 100 * time.Millisecond, PlacementRetries: 1},
		SourceProviderFactory: func(ctx context.Context, session producer.SourceSession) (source.Provider, error) {
			return catalogcp.New(session.Conn, catalogcp.Options{ProducerID: session.ProducerID, TenantID: session.TenantID, SiteID: session.SiteID, SetID: session.SetID, PollInterval: 200 * time.Millisecond, CallTimeout: time.Second})
		},
		AuthProviderFactory: func(_ context.Context, session producer.SourceSession) (cameramodel.AuthProvider, error) {
			return credentialcp.New(session.Conn, credentialcp.Options{Role: credentialcp.Producer, Scope: cameramodel.EnrollmentScope{TenantID: session.TenantID, SiteID: session.SiteID, SetID: session.SetID, ProducerID: session.ProducerID}})
		},
	})
	require.NoError(t, err)
	pctx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- p.Run(pctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			require.True(t, err == nil || errors.Is(err, context.Canceled), "bounded RAM Producer ended unexpectedly")
		case <-time.After(15 * time.Second):
			t.Error("bounded RAM Producer did not join")
		}
	})
	host := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	lan, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "bounded-ram-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo"}.Build())
	require.NoError(t, err)
	assignment := sourceCatalogAssignment(t, admin, lan, host)
	profile := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 320, Height: 180, FPS: 10, Bitrate: 1_000_000, GOP: 10}
	fixture := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{Profile: profile})
	binding := lifecycleBind(t, admin, assignment, "bounded-ram", fixture.URL())
	wire := api.CameraCaptureProfile_builder{Codec: "h264", Width: 320, Height: 180, Fps: 10, MaxBitrateBps: 1_000_000, KeyframeIntervalMs: 1000}.Build()
	publication := lifecyclePublish(t, admin, assignment, binding, nil, fixture.URL(), wire, nil, "bounded-ram-first")
	binding = lifecycleApplied(t, admin, publication.GetBinding())
	lifecycleCommitted(t, admin, binding)
	epoch := binding.GetAppliedApplication().GetCaptureEpoch()
	evidence, err := qualifyWHEP(ctx, lifecycleLive(t, admin, binding), edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, epoch, evidence.CaptureEpoch)
	fault.target.Store(string(binding.GetSource().GetId()))
	fault.enabled.Store(true)
	before := p.Stats()
	var peak int64
	require.Eventually(t, func() bool {
		stats := p.Stats()
		peak = max(peak, stats.Held)
		return fault.hits.Load() > 0 && stats.Lost > before.Lost && stats.LostBytes > before.LostBytes
	}, 30*time.Second, 25*time.Millisecond, "real unavailable uploads must exceed the finite queue and report media loss")
	// Keep the outage active through further segment/drop cycles. A bound seen
	// only on the first overflow would not qualify a continuing interruption.
	firstLoss := p.Stats().Lost
	continuedUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(continuedUntil) {
		peak = max(peak, p.Stats().Held)
		time.Sleep(25 * time.Millisecond)
	}
	require.Greater(t, p.Stats().Lost, firstLoss)
	// A queue can discard media before it owns an allocation. Native Producer
	// counters cover that loss too; it need not have a CP Lamina row to mark LOST.
	require.Greater(t, peak, budget, "the configured video budget must actually be exercised")
	// Current/closing segments and a bounded retry may temporarily overshoot the
	// queue threshold. They cannot create an unbounded disk-backed backlog.
	require.LessOrEqual(t, peak, budget+4*(512<<10), "retained media grew beyond the bounded retry/segment allowance")
	require.Equal(t, 1, fixture.Sessions(), "dropping old recordings must retain the live RTSP capture")
	restoredAt := time.Now()
	fault.enabled.Store(false)
	require.Eventually(t, func() bool { return p.Stats().Held <= budget && p.Stats().Stored > before.Stored }, 20*time.Second, 50*time.Millisecond)
	current, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: binding.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, epoch, current.GetAppliedApplication().GetCaptureEpoch())
	var recovered *api.Lamina
	require.Eventually(t, func() bool {
		rows, err := api.NewLaminaServiceClient(admin).List(ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build()}.Build()}, Size: 100}.Build())
		if err != nil || len(rows.GetItems()) >= 100 {
			return false
		}
		for _, row := range rows.GetItems() {
			if row.GetDateCommitted() != nil && row.GetDateCommitted().AsTime().After(restoredAt) && row.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && !row.GetIncomplete() && row.GetCaptureEpoch() == epoch {
				recovered = row
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond)
	require.NotNil(t, recovered.GetDateCommitted())
	require.True(t, recovered.GetDateCommitted().AsTime().After(restoredAt))
	qualificationPlayable(t, ctx, admin, exportHTTPBase(t, c, cmd.SurfaceTenant), current, recovered, 320, 180)
	evidence, err = qualifyWHEP(ctx, lifecycleLive(t, admin, current), edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, epoch, evidence.CaptureEpoch)
}
