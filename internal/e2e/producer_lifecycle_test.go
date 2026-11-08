package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/token"
	catalog "github.com/lesomnus/shale/source"
	catalogcp "github.com/lesomnus/shale/source/cp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifecycleObserver struct {
	mu      sync.Mutex
	proofs  map[string]catalog.AppliedProof
	stopped []catalog.StoppedProof
}

func (o *lifecycleObserver) Applied(_ context.Context, p catalog.AppliedProof) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if p.UsedCredentialRef != nil {
		r := *p.UsedCredentialRef
		p.UsedCredentialRef = &r
	}
	o.proofs[p.Epoch] = p
}
func (o *lifecycleObserver) Stopped(_ context.Context, p catalog.StoppedProof) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stopped = append(o.stopped, p)
}
func (o *lifecycleObserver) proof(epoch string) (catalog.AppliedProof, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	p, ok := o.proofs[epoch]
	return p, ok
}

type lifecycleAuth struct {
	mu       sync.Mutex
	provider cameramodel.AuthProvider
	refs     []cameramodel.CredentialRef
}

func (a *lifecycleAuth) Acquire(ctx context.Context, r cameramodel.AuthRequest) (cameramodel.CredentialLease, error) {
	a.mu.Lock()
	p := a.provider
	if r.ExistingRef != nil {
		a.refs = append(a.refs, *r.ExistingRef)
	}
	a.mu.Unlock()
	if p == nil {
		return nil, cameramodel.ErrAuthRequired
	}
	return p.Acquire(ctx, r)
}
func (a *lifecycleAuth) set(p cameramodel.AuthProvider) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.provider = p
}

type lifecycleLog struct {
	mu sync.Mutex
	strings.Builder
}

func (l *lifecycleLog) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Builder.Write(b)
}
func (l *lifecycleLog) text() string { l.mu.Lock(); defer l.mu.Unlock(); return l.Builder.String() }

type lifecycleUploadFault struct {
	enabled atomic.Bool
	target  atomic.Value
	hits    atomic.Int64
	mu      sync.Mutex
	limits  map[string]int64
}

func (f *lifecycleUploadFault) RoundTrip(r *http.Request) (*http.Response, error) {
	claims, _ := token.Parse(token.FromHeader(r.Header.Get("Authorization")))
	if target, _ := f.target.Load().(string); target != "" && string(claims.GetRecord().GetSourceId()) == target && r.Method == http.MethodPut {
		f.mu.Lock()
		f.limits[claims.GetRecord().GetCaptureEpoch()] = claims.GetMaxLength()
		f.mu.Unlock()
		if f.enabled.Load() {
			f.hits.Add(1)
			return &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": []string{"0"}}, Body: io.NopCloser(strings.NewReader("fixture upload unavailable")), Request: r}, nil
		}
	}
	return http.DefaultTransport.RoundTrip(r)
}

// Publication below is synthetic authorized operator evidence. The acceptance
// assertions concern the actual Producer process, parsed new video, live RTP
// and storage commits, not full vendor enrollment or independent live decoding.
func lifecycleBind(t *testing.T, admin *grpc.ClientConn, a *api.CameraAssignment, alias, input string) *api.CameraBinding {
	t.Helper()
	ctx := t.Context()
	d, err := api.NewCameraDeviceServiceClient(admin).Claim(ctx, api.CameraDeviceClaimRequest_builder{Network: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build(), DeviceKey: alias + "-physical"}.Build())
	require.NoError(t, err)
	d, err = api.NewCameraDeviceServiceClient(admin).Observe(ctx, api.CameraDeviceObserveRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), ExpectedVersion: d.GetDateUpdated(), Identity: api.CameraIdentityEvidence_builder{Authenticated: true, Manufacturer: "synthetic", Serial: alias}.Build(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: input, Provenance: "authenticated", ObservedAt: timestamppb.Now()}.Build()}}.Build())
	require.NoError(t, err)
	b, err := api.NewCameraBindingServiceClient(admin).Bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: alias + "-logical", SourceAlias: alias}.Build())
	require.NoError(t, err)
	return b
}

func lifecyclePublish(t *testing.T, admin *grpc.ClientConn, a *api.CameraAssignment, b *api.CameraBinding, prior *api.CameraJob, input string, profile *api.CameraCaptureProfile, ref *api.CameraCredentialReference, key string) *api.CameraJobPublishResponse {
	t.Helper()
	ctx := t.Context()
	jobs := api.NewCameraJobServiceClient(admin)
	if prior != nil {
		// Finish this authority fixture's lease without claiming ACTIVE media;
		// collection remains independent of a superseded provisioning job.
		_, err := jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: api.CameraJobRef_builder{Id: prior.GetId()}.Build(), ExpectedVersion: prior.GetDateUpdated(), OperationKey: key + "-cancel-prior", LeaseGeneration: prior.GetLeaseGeneration(), AssignmentGeneration: prior.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED, Observed: prior.GetObserved()}.Build())
		require.NoError(t, err)
	}
	j, err := jobs.Create(ctx, api.CameraJobCreateRequest_builder{Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), RequestKey: key, Inputs: api.CameraJobInputs_builder{Desired: profile, StreamingCredential: ref}.Build()}.Build())
	require.NoError(t, err)
	jref := api.CameraJobRef_builder{Id: j.GetId()}.Build()
	j, err = jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: jref, ExpectedVersion: j.GetDateUpdated(), OperationKey: key + "-claim", LeaseSeconds: 300}.Build())
	require.NoError(t, err)
	observed := api.CameraJobObservation_builder{Effective: profile, Verified: api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: int64(profile.GetFps() * 2), ObservedAt: timestamppb.Now()}.Build()}.Build()
	j, err = jobs.Report(ctx, api.CameraJobReportRequest_builder{Ref: jref, ExpectedVersion: j.GetDateUpdated(), OperationKey: key + "-verified", LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, Observed: observed}.Build())
	require.NoError(t, err)
	pub, err := jobs.Publish(ctx, api.CameraJobPublishRequest_builder{Ref: jref, ExpectedVersion: j.GetDateUpdated(), OperationKey: key + "-publish", LeaseGeneration: j.GetLeaseGeneration(), AssignmentGeneration: j.GetAssignmentGeneration(), Spec: api.CameraCaptureSpec_builder{BindingId: b.GetId(), SourceId: b.GetSource().GetId(), Alias: b.GetSourceAlias(), Input: input, Profile: profile, Credential: ref}.Build(), Observed: observed}.Build())
	require.NoError(t, err)
	return pub
}

func lifecycleApplied(t *testing.T, admin *grpc.ClientConn, b *api.CameraBinding, afterEpoch ...string) *api.CameraBinding {
	t.Helper()
	var got *api.CameraBinding
	require.Eventually(t, func() bool {
		v, err := api.NewCameraBindingServiceClient(admin).Get(t.Context(), api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		app := v.GetAppliedApplication()
		if len(afterEpoch) > 0 && app.GetCaptureEpoch() == afterEpoch[0] {
			return false
		}
		if app.GetDateApplied() == nil || app.GetTarget().GetSpecRevision() != v.GetDesired().GetRevision() || app.GetCaptureEpoch() != v.GetStartingApplication().GetCaptureEpoch() {
			return false
		}
		got = v
		return true
	}, 35*time.Second, 100*time.Millisecond, "actual new read-loop profile and frames must apply the exact specification")
	return got
}

func lifecycleCommitted(t *testing.T, admin *grpc.ClientConn, b *api.CameraBinding, after ...*api.Lamina) *api.Lamina {
	t.Helper()
	epoch := b.GetAppliedApplication().GetCaptureEpoch()
	var found *api.Lamina
	require.Eventually(t, func() bool {
		rows, err := api.NewLaminaServiceClient(admin).List(t.Context(), api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build()}, Size: 500}.Build())
		if err != nil {
			return false
		}
		for _, v := range rows.GetItems() {
			if len(after) > 0 && !v.GetDateStarted().AsTime().After(after[0].GetDateStarted().AsTime()) {
				continue
			}
			if v.GetCaptureEpoch() == epoch && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && !v.GetIncomplete() {
				found = v
				return true
			}
		}
		return false
	}, 25*time.Second, 100*time.Millisecond, "a complete committed recording must retain this exact new capture epoch")
	return found
}

func lifecycleLive(t *testing.T, admin *grpc.ClientConn, b *api.CameraBinding) *api.LiveSource {
	t.Helper()
	var live *api.LiveSource
	epoch := b.GetAppliedApplication().GetCaptureEpoch()
	require.Eventually(t, func() bool {
		v, err := api.NewSourceServiceClient(admin).Live(t.Context(), api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build())
		if err != nil || v.GetSource().GetCaptureEpoch() != epoch {
			return false
		}
		// An authenticated CP read can precede the restarted relay's first
		// matching init/keyframe. Readiness requires actual current-epoch media,
		// rather than racing a single WHEP POST against that async attachment.
		call, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(call, http.MethodGet, v.GetSource().GetRecentUrl(), nil)
		if err != nil {
			return false
		}
		req.Header.Set("Authorization", token.Scheme+" "+v.GetSource().GetViewToken())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<20))
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Shale-Capture-Epoch") != epoch {
			return false
		}
		live = v.GetSource()
		return true
	}, 15*time.Second, 100*time.Millisecond)
	return live
}

func TestProducerLifecycleReconcilesActualMediaAndHistory(t *testing.T) {
	for _, binary := range []string{"ffmpeg", "ffprobe"} {
		_, err := exec.LookPath(binary)
		require.NoError(t, err)
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set := sourceCatalogSet(t, admin, "dynamic-lifecycle")
	observer := &lifecycleObserver{proofs: map[string]catalog.AppliedProof{}}
	auth := &lifecycleAuth{}
	logs := &lifecycleLog{}
	fault := &lifecycleUploadFault{limits: map[string]int64{}}
	fault.target.Store("")
	state := filepath.Join(t.TempDir(), "producer")
	config := producer.Config{StateDir: state, Cp: "http://" + c.running.TenantAddr, Dev: true, Mode: api.UploadMode_UPLOAD_MODE_LIVE, SegmentDuration: 4 * time.Second, HeartbeatInterval: 200 * time.Millisecond, AllocationHorizon: time.Minute, Buffer: 64 << 20, DrainTimeout: 10 * time.Second, ApplyObserver: observer, AuthProvider: auth, Log: slog.New(slog.NewTextHandler(logs, nil)), Upload: producer.UploadConfig{Client: &http.Client{Transport: fault}, ResumeTimeout: 2 * time.Second, RetryAfterCap: 100 * time.Millisecond, PlacementRetries: 1}}
	var factories atomic.Int32
	config.SourceProviderFactory = func(ctx context.Context, s producer.SourceSession) (catalog.Provider, error) {
		factories.Add(1)
		return catalogcp.New(s.Conn, catalogcp.Options{ProducerID: s.ProducerID, TenantID: s.TenantID, SiteID: s.SiteID, SetID: s.SetID, PollInterval: 200 * time.Millisecond, CallTimeout: 2 * time.Second})
	}
	p, err := producer.New(config)
	require.NoError(t, err)
	pctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); done <- p.Run(pctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			t.Error("Producer outlived cancellation")
		}
	})
	host := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	select {
	case <-p.Ready:
	case err := <-done:
		t.Fatalf("idle Producer ended: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Eventually(t, func() bool {
		v, err := api.NewProducerServiceClient(admin).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: host.GetId()}.Build()}.Build())
		return err == nil && v.GetStatus().GetDateReported() != nil && len(v.GetStatus().GetSources()) == 0
	}, 10*time.Second, 100*time.Millisecond, "approved host heartbeats even before a camera assignment exists")
	require.Zero(t, p.Stats().Stored)
	require.Zero(t, p.Stats().Held)
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "dynamic-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo"}.Build())
	require.NoError(t, err)
	a := sourceCatalogAssignment(t, admin, network, host)
	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 160, Height: 90, Fps: 10, MaxBitrateBps: 1_000_000, KeyframeIntervalMs: 1000}.Build()
	anchorInput := cameratest.ServeRTSP(t)
	anchor := lifecycleBind(t, admin, a, "anchor", anchorInput)
	anchorPub := lifecyclePublish(t, admin, a, anchor, nil, anchorInput, profile, nil, "anchor-first")
	anchor = lifecycleApplied(t, admin, anchorPub.GetBinding())
	anchorEpoch := anchor.GetAppliedApplication().GetCaptureEpoch()
	anchorFirst := lifecycleCommitted(t, admin, anchor)
	anchorViewer := watch(t, lifecycleLive(t, admin, anchor))
	defer anchorViewer.close()
	anchorViewer.video(t, 3, 10*time.Second)
	require.Equal(t, int32(1), factories.Load(), "initial unavailable snapshot must not repeatedly reopen the authenticated provider")
	before := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}
	cam := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{ListenAddress: "127.0.0.2:0", Path: "/main", Profile: before, Username: "fixture-reader", Password: "fixture-pass:/v1@local-only"})
	scope := cameramodel.EnrollmentScope{TenantID: sourceCatalogID(t, set.GetTenant().GetId()), SetID: sourceCatalogID(t, set.GetId()), ProducerID: sourceCatalogID(t, host.GetId()), NetworkID: sourceCatalogID(t, network.GetId())}
	r1 := cameramodel.CredentialRef{Provider: "fixture", ID: "stream", Version: "1", Scope: scope, Purpose: cameramodel.AuthStreaming, Recoverable: true}
	r2 := r1
	r2.Version = "2"
	provider, err := cameramodel.NewStaticProvider([]cameramodel.StaticCredential{{Ref: r1, Access: cameramodel.AccessRead, Material: cameramodel.CredentialMaterial{Username: "fixture-reader", Password: "fixture-pass:/v1@local-only"}}, {Ref: r2, Access: cameramodel.AccessRead, Material: cameramodel.CredentialMaterial{Username: "fixture-reader", Password: "fixture-pass:/v1@local-only"}}})
	require.NoError(t, err)
	auth.set(provider)
	ref := func(v cameramodel.CredentialRef) *api.CameraCredentialReference {
		return api.CameraCredentialReference_builder{Provider: v.Provider, Id: v.ID, Version: v.Version, Purpose: string(v.Purpose)}.Build()
	}
	mutable := lifecycleBind(t, admin, a, "mutable", cam.URL())
	id := string(mutable.GetSource().GetId())
	fault.target.Store(id)
	pub := lifecyclePublish(t, admin, a, mutable, nil, cam.URL(), profile, ref(r1), "mutable-first")
	mutable = lifecycleApplied(t, admin, pub.GetBinding())
	lifecycleCommitted(t, admin, mutable)
	proof, ok := observer.proof(mutable.GetAppliedApplication().GetCaptureEpoch())
	require.True(t, ok)
	require.Equal(t, "1", proof.UsedCredentialRef.Version)
	firstEpoch := mutable.GetAppliedApplication().GetCaptureEpoch()
	pub = lifecyclePublish(t, admin, a, mutable, pub.GetJob(), cam.URL(), profile, ref(r2), "mutable-new-ref")
	mutable = lifecycleApplied(t, admin, pub.GetBinding())
	require.NotEqual(t, firstEpoch, mutable.GetAppliedApplication().GetCaptureEpoch())
	proof, ok = observer.proof(mutable.GetAppliedApplication().GetCaptureEpoch())
	require.True(t, ok)
	require.Equal(t, "2", proof.UsedCredentialRef.Version)

	// Restart the test control/storage/relay process while the external Producer
	// and RTSP fixture keep running. Failed catalog reads must not empty capture.
	outageEpoch := mutable.GetAppliedApplication().GetCaptureEpoch()
	anchorViewer.close()
	c.stopCP()
	require.Equal(t, 1, cam.Sessions(), "control-plane outage does not cancel an established RTSP input")
	time.Sleep(1200 * time.Millisecond)
	c.startCP()
	require.Eventually(t, func() bool {
		v, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: mutable.GetId()}.Build()}.Build())
		return err == nil && v.GetStartingApplication().GetCaptureEpoch() == outageEpoch && v.GetAppliedApplication().GetCaptureEpoch() == outageEpoch
	}, 15*time.Second, 100*time.Millisecond)
	require.Equal(t, 1, cam.Sessions())
	require.Equal(t, int32(1), factories.Load(), "an unavailable snapshot preserves its authenticated provider and capture incarnations")
	anchorViewer = watch(t, lifecycleLive(t, admin, anchor))
	defer anchorViewer.close()
	lifecycleFreshVideo(t, anchorViewer, 5)
	anchorFirst = lifecycleCommitted(t, admin, anchor, anchorFirst)
	newMode := before
	newMode.Width = 320
	newMode.Height = 180
	newMode.FPS = 20
	newMode.Bitrate = 256_000
	newMode.GOP = 20
	require.NoError(t, cam.SetProfile(ctx, newMode))
	larger := proto.Clone(profile).(*api.CameraCaptureProfile)
	larger.SetWidth(320)
	larger.SetHeight(180)
	larger.SetFps(20)
	larger.SetMaxBitrateBps(2_000_000)
	pub = lifecyclePublish(t, admin, a, mutable, pub.GetJob(), cam.URL(), larger, ref(r2), "mutable-new-profile")
	mutable = lifecycleApplied(t, admin, pub.GetBinding())
	lifecycleCommitted(t, admin, mutable)
	require.Equal(t, int32(320), mutable.GetAppliedApplication().GetMedia().GetProfile().GetWidth())
	require.Equal(t, int32(180), mutable.GetAppliedApplication().GetMedia().GetProfile().GetHeight())
	require.InDelta(t, 20, mutable.GetAppliedApplication().GetMedia().GetProfile().GetFps(), 0.5)
	endpoint, err := cam.Rebind("127.0.0.3:0")
	require.NoError(t, err)
	pub = lifecyclePublish(t, admin, a, mutable, pub.GetJob(), endpoint, larger, ref(r2), "mutable-new-endpoint")
	mutable = lifecycleApplied(t, admin, pub.GetBinding())
	mutableBeforeReject := lifecycleCommitted(t, admin, mutable)
	require.Equal(t, id, string(mutable.GetSource().GetId()), "endpoint changes preserve physical/logical Source identity")
	viewer := watch(t, lifecycleLive(t, admin, mutable))
	defer viewer.close()
	viewer.video(t, 3, 10*time.Second)
	// A rejected replacement leaves actual current peers/recording unchanged.
	sets := api.NewSetServiceClient(admin)
	set, err = sets.Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	cap := int64(3_300_000)
	_, err = sets.Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), MaxBitrateTotal: &cap, DateUpdated: set.GetDateUpdated()}.Build())
	require.NoError(t, err)
	priorEpoch := mutable.GetAppliedApplication().GetCaptureEpoch()
	rejected := proto.Clone(larger).(*api.CameraCaptureProfile)
	rejected.SetMaxBitrateBps(3_000_000)
	pub = lifecyclePublish(t, admin, a, mutable, pub.GetJob(), endpoint, rejected, ref(r2), "mutable-over-budget")
	time.Sleep(1500 * time.Millisecond)
	current, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: mutable.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, priorEpoch, current.GetStartingApplication().GetCaptureEpoch())
	require.Equal(t, priorEpoch, current.GetAppliedApplication().GetCaptureEpoch())
	lifecycleFreshVideo(t, viewer, 15)
	lifecycleCommitted(t, admin, mutable, mutableBeforeReject)
	// Force an old FIFO to remain while the new metadata ceiling shrinks.
	fault.enabled.Store(true)
	hits := fault.hits.Load()
	require.Eventually(t, func() bool { return fault.hits.Load() > hits }, 10*time.Second, 50*time.Millisecond)
	shrink := proto.Clone(larger).(*api.CameraCaptureProfile)
	shrink.SetMaxBitrateBps(1_000_000)
	pub = lifecyclePublish(t, admin, a, mutable, pub.GetJob(), endpoint, shrink, ref(r2), "mutable-shrink")
	sources := api.NewSourceServiceClient(admin)
	require.Eventually(t, func() bool {
		s, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: mutable.GetSource().GetId()}.Build()}.Build())
		return err == nil && s.GetProfile().GetMaxBitrate() == 1_128_000
	}, 10*time.Second, 20*time.Millisecond)
	fault.enabled.Store(false)
	mutable = lifecycleApplied(t, admin, pub.GetBinding())
	lifecycleCommitted(t, admin, mutable)
	anchorNow, err := api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: anchor.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, anchorEpoch, anchorNow.GetAppliedApplication().GetCaptureEpoch(), "unrelated capture never restarts for another catalog revision")
	lifecycleFreshVideo(t, anchorViewer, 15)
	lifecycleCommitted(t, admin, anchor, anchorFirst)
	// An intentionally unavailable upload at explicit stop has a bounded,
	// counted loss rather than an unreported queue discard.
	fault.enabled.Store(true)
	hits = fault.hits.Load()
	require.Eventually(t, func() bool { return fault.hits.Load() > hits }, 10*time.Second, 50*time.Millisecond)
	lost := p.Stats().Lost
	lostBytes := p.Stats().LostBytes
	mutable, err = api.NewCameraBindingServiceClient(admin).Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: mutable.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraBindingServiceClient(admin).SetCollect(ctx, api.CameraBindingSetCollectRequest_builder{Ref: api.CameraBindingRef_builder{Id: mutable.GetId()}.Build(), ExpectedVersion: mutable.GetDateUpdated(), Collect: false}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return cam.Sessions() == 0 && p.Stats().Lost > lost && p.Stats().LostBytes > lostBytes }, 20*time.Second, 100*time.Millisecond)
	require.Contains(t, logs.text(), "bounded upload drain expired")
	require.NotContains(t, logs.text(), "fixture-pass:/v1@local-only")
	_, err = sources.Live(ctx, api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: mutable.GetSource().GetId()}.Build()}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: mutable.GetSource().GetId()}.Build()}.Build())
	require.NoError(t, err)
	lifecycleFreshVideo(t, anchorViewer, 15)
	stop()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("initial Producer did not join")
	}
	fault.enabled.Store(false)
	p2, err := producer.New(config)
	require.NoError(t, err)
	restartDone := make(chan error, 1)
	restartExited := make(chan struct{})
	go func() { defer close(restartExited); restartDone <- p2.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-restartExited:
		case <-time.After(15 * time.Second):
			t.Error("restarted Producer did not join")
		}
	})
	select {
	case <-p2.Ready:
	case err := <-restartDone:
		t.Fatalf("restarted Producer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	anchor = lifecycleApplied(t, admin, anchor, anchorEpoch)
	require.NotEqual(t, anchorEpoch, anchor.GetAppliedApplication().GetCaptureEpoch(), "restart requires a fresh incarnation of the same stable Source")
	lifecycleCommitted(t, admin, anchor)
}

func lifecycleFreshVideo(t *testing.T, v *viewer, n int) {
	t.Helper()
	for {
		select {
		case <-v.packets:
		default:
			v.video(t, n, 10*time.Second)
			return
		}
	}
}
