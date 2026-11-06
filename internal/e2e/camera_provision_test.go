package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/producer"
	shaleprovision "github.com/lesomnus/shale/internal/provisionhost/shale"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type interruptAfterCameraDeploy struct {
	provision.SourcePublisher
	interrupted bool
}

func (p *interruptAfterCameraDeploy) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	if !p.interrupted {
		p.interrupted = true
		return b, provision.ErrNotReady
	}
	return p.SourcePublisher.Observe(ctx, d, b)
}

// The complete camera path uses real ONVIF SOAP, RTSP/RTP, a Producer process
// lifecycle, CP registration, node storage, and WebRTC readiness. Controller
// reconstruction after capture deployment must bind the same logical Source.
func TestCameraProvisioningRecordsAndResumes(t *testing.T) {
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " unavailable")
		}
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "provisioned-cameras"}.Build())
	require.NoError(t, err)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "producer.yaml")
	secretDir := filepath.Join(dir, "private-credentials")
	unrelatedSample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	config := cmd.Config{State: dir, Dev: dir, Cp: "http://" + c.running.TenantAddr}
	config.Producer = cmd.ProducerConfig{Sources: []cmd.SourceConfig{{Alias: "unrelated", Input: "raw:" + unrelatedSample, Format: "h264", Fps: 30, MaxBitrate: "2Mbps"}}, Mode: "live", SegmentDuration: 4 * time.Second, HeartbeatInterval: time.Second, AllocationHorizon: time.Minute, Buffer: "64MiB", Credentials: cmd.CredentialStoreConfig{Directory: secretDir, Provider: "local"}}
	document, err := yaml.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, document, 0600))
	var stopProducer context.CancelFunc
	var producerDone chan error
	startProducer := func() (*producer.Producer, error) {
		body, err := os.ReadFile(configPath)
		if err != nil {
			return nil, err
		}
		var configured cmd.Config
		if err = yaml.Unmarshal(body, &configured); err != nil {
			return nil, err
		}
		cfg, err := cli.ProducerConfig(&configured)
		if err != nil {
			return nil, err
		}
		p, err := producer.New(cfg)
		if err != nil {
			return nil, err
		}
		var pctx context.Context
		pctx, stopProducer = context.WithCancel(ctx)
		producerDone = make(chan error, 1)
		go func() { producerDone <- p.Run(pctx) }()
		return p, nil
	}
	t.Cleanup(func() {
		if stopProducer != nil {
			stopProducer()
			select {
			case <-producerDone:
			case <-time.After(15 * time.Second):
				t.Error("Producer outlived cancellation")
			}
		}
	})
	p, err := startProducer()
	require.NoError(t, err)
	joined := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	select {
	case <-p.Ready:
	case err := <-producerDone:
		t.Fatalf("initial Producer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	initialSources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build())
	require.NoError(t, err)
	require.Len(t, initialSources.GetItems(), 1)
	unrelatedID := initialSources.GetItems()[0].GetId()
	asID := func(b []byte) string { id, err := pdid.From(b); require.NoError(t, err); return id.String() }
	scope := cameramodel.EnrollmentScope{TenantID: asID(set.GetTenant().GetId()), SetID: asID(set.GetId()), ProducerID: asID(joined.GetId()), NetworkID: "lan"}
	if set.GetSite() != nil && len(set.GetSite().GetId()) > 0 {
		scope.SiteID = asID(set.GetSite().GetId())
	}
	provider := credential.FileProvider{Dir: secretDir, Name: "local"}
	material := cameramodel.CredentialMaterial{Username: "camera", Password: "synthetic:@/?% fixture"}
	management, err := provider.Prepare(ctx, scope, cameramodel.AuthManagement, "management", material)
	require.NoError(t, err)
	streaming, err := provider.Prepare(ctx, scope, cameramodel.AuthStreaming, "streaming", material)
	require.NoError(t, err)
	profile := cameramodel.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: cameratest.ServeRTSP(t)}
	device := cameratest.ServeONVIF(t, profile)
	device.RequireCredentials(material.Username, material.Password)
	restarts := 0
	publisher := &shaleprovision.FilePublisher{ConfigPath: configPath, StateDir: filepath.Join(dir, "deployment"), Scope: scope, Authority: &shaleprovision.RPCAuthority{Conn: admin, PollInterval: 200 * time.Millisecond, ObserveTimeout: 70 * time.Second}, Restart: func(ctx context.Context) error {
		restarts++
		stopProducer()
		select {
		case err := <-producerDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		var err error
		p, err = startProducer()
		if err != nil {
			return err
		}
		select {
		case <-p.Ready:
			return nil
		case err := <-producerDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	stateDir := filepath.Join(dir, "runs")
	store, err := filestore.Open(stateDir)
	require.NoError(t, err)
	inspector := &onvif.Client{}
	newEngine := func(state provision.Store, pub provision.SourcePublisher) *provision.Engine {
		return &provision.Engine{Store: state, Auth: provider, Inspector: inspector, Configurator: inspector, Verifier: &media.Verifier{AuthProvider: provider, NetworkScope: "lan"}, Publisher: pub}
	}
	request := provision.Request{ID: "enroll-main-v1", BindingID: "main-role", Scope: scope, Device: device.Hint("physical-camera-1"), Desired: cameramodel.DesiredState{ProfileToken: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, Revision: "v1"}, Alias: "provisioned-main", Provider: "local", ManagementRef: &management, StreamingRef: &streaming, AllowProducerRestart: true, ProbeDuration: 2 * time.Second}
	interrupted := &interruptAfterCameraDeploy{SourcePublisher: publisher}
	first, err := newEngine(store, interrupted).Execute(ctx, request, true)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.NotEqual(t, provision.Active, first.Stage)
	require.Equal(t, 1, restarts)
	// Recreate durable state and workflow as a newly started controller would.
	reopened, err := filestore.Open(stateDir)
	require.NoError(t, err)
	active, err := newEngine(reopened, publisher).Execute(ctx, request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, active.Stage)
	require.True(t, active.Binding.Ingest && active.Binding.Live && active.Binding.Committed)
	require.True(t, active.Verified.Decoded)
	require.Equal(t, 1, restarts, "resume must not redeploy an already installed manifest")
	originalSource := active.Binding.SourceID
	repeated, err := newEngine(reopened, publisher).Execute(ctx, request, true)
	require.NoError(t, err)
	require.Equal(t, originalSource, repeated.Binding.SourceID)
	require.Equal(t, 1, restarts)
	// DHCP/endpoint change uses a new desired revision while retaining the same
	// authenticated physical identity and logical recording history.
	profile.StreamURI = cameratest.ServeRTSP(t)
	device.SetVideo(profile)
	request.ID = "enroll-main-v2"
	request.Desired.Revision = "v2"
	changed, err := newEngine(reopened, publisher).Execute(ctx, request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, changed.Stage)
	require.Equal(t, originalSource, changed.Binding.SourceID)
	require.Equal(t, 2, restarts)
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 2)
	foundUnrelated := false
	for _, source := range sources.GetItems() {
		if source.GetAlias() == "unrelated" {
			foundUnrelated = true
			require.Equal(t, unrelatedID, source.GetId())
		} else {
			require.Equal(t, originalSource, asID(source.GetId()))
			require.Equal(t, "main-role", source.GetLabels()[shaleprovision.BindingLabel])
			require.Equal(t, "physical-camera-1", source.GetLabels()[shaleprovision.DeviceLabel])
		}
	}
	require.True(t, foundUnrelated)
	sid, err := pdid.Parse(originalSource)
	require.NoError(t, err)
	timeline, err := api.NewLaminaServiceClient(admin).Timeline(ctx, api.LaminaTimelineRequest_builder{Source: api.SourceRef_builder{Id: sid.Bytes()}.Build(), From: timestamppb.New(first.CreatedAt.Add(-time.Second)), To: timestamppb.New(time.Now().Add(time.Minute)), Size: 100}.Build())
	require.NoError(t, err)
	require.Len(t, timeline.GetSources(), 1)
	require.NotEmpty(t, timeline.GetSources()[0].GetLaminae())
	complete := false
	for _, lamina := range timeline.GetSources()[0].GetLaminae() {
		if lamina.GetState() != api.ReadState_READ_STATE_AVAILABLE || lamina.GetEndedEstimated() {
			continue
		}
		raw := fetch(t, lamina.GetUrl())
		playsOnItsOwn(t, raw)
		decode := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-xerror", "-i", "pipe:0", "-f", "null", "-")
		decode.Stdin = bytes.NewReader(raw)
		decode.Stdout = io.Discard
		require.NoError(t, decode.Run())
		complete = true
		break
	}
	require.True(t, complete, "new complete committed recording must decode")
	body, err := os.ReadFile(configPath)
	require.NoError(t, err)
	serialized, err := json.Marshal(changed)
	require.NoError(t, err)
	for _, artifact := range []string{string(body), string(serialized)} {
		require.NotContains(t, artifact, material.Password)
		require.NotContains(t, artifact, "synthetic%3A")
		require.NotContains(t, artifact, "camera:synthetic")
	}
	for _, call := range device.Calls() {
		require.False(t, strings.Contains(call, "SetSystemDateAndTime"), "inspection changed device clock")
	}
}
