package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/provisionhost"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
)

// Exercise the standalone factory and HTTP worker path, rather than injecting
// an already authenticated gRPC connection into RPCAuthority. The restart is
// deliberately refused: reaching a manifest write proves CP assignment and
// tenant-wide alias checks passed, without a second Producer lifecycle here.
func TestCameraProvisionerUsesCPSession(t *testing.T) {
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " unavailable")
		}
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "session-cameras"}.Build())
	require.NoError(t, err)

	// A push source lets the existing Producer join/adopt without capturing any
	// media. The camera under test is independently qualified over ONVIF/RTSP.
	p, err := producer.New(producer.Config{StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true, Push: "tcp://127.0.0.1:0", Sources: []producer.SourceConfig{{Alias: "unrelated", Input: producer.InputPush, Format: "h264", Fps: 30, MaxBitrate: 2_000_000}}, Buffer: 64 << 20})
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
	joined := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
	select {
	case <-p.Ready:
	case err := <-done:
		t.Fatalf("Producer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	id := func(b []byte) string { v, err := pdid.From(b); require.NoError(t, err); return v.String() }
	scope := cameramodel.EnrollmentScope{TenantID: id(set.GetTenant().GetId()), SetID: id(set.GetId()), ProducerID: id(joined.GetId()), NetworkID: "lan"}
	if set.GetSite() != nil && len(set.GetSite().GetId()) != 0 {
		scope.SiteID = id(set.GetSite().GetId())
	}

	const password = "camera provision session fixture"
	require.NoError(t, c.running.CP.Identity.SetPassword(ctx, "acme", "admin", password))
	require.NoError(t, c.running.CP.Identity.SetPassword(ctx, "cluster", "ops", password))
	tenantBase, clusterBase := httpBases(t, c)
	code, value := signIn(t, tenantBase, "acme", "admin", password)
	require.Equal(t, http.StatusNoContent, code)
	require.NotEmpty(t, value)
	code, clusterValue := signIn(t, clusterBase, "cluster", "ops", password)
	require.Equal(t, http.StatusNoContent, code)
	require.NotEmpty(t, clusterValue)

	sessions := c.running.CP.Sessions[cmd.SurfaceTenant]
	old, err := sessions.Read(ctx, value)
	require.NoError(t, err)
	old.Expires = time.Now().Add(-time.Minute)
	_, expired, err := sessions.Mint(ctx, old)
	require.NoError(t, err)
	invalid := "A" + value[1:]
	if invalid == value {
		invalid = "B" + value[1:]
	}

	profile := cameramodel.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: cameratest.ServeRTSP(t)}
	device := cameratest.ServeONVIF(t, profile)
	for _, test := range []struct {
		name, value string
		accepted    bool
		asBearer    bool
	}{
		{name: "valid", value: value, accepted: true},
		{name: "cookie_as_bearer", value: value, asBearer: true},
		{name: "tampered", value: invalid},
		{name: "expired", value: expired.Value},
		// Even renamed to the expected cookie, a session sealed by the cluster
		// surface cannot authenticate to the tenant surface.
		{name: "cluster_session", value: clusterValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			secrets := filepath.Join(dir, "secrets")
			provider := credential.FileProvider{Dir: secrets, Name: "cameras"}
			material := cameramodel.CredentialMaterial{Username: "camera", Password: "synthetic fixture"}
			management, err := provider.Prepare(ctx, scope, cameramodel.AuthManagement, "management", material)
			require.NoError(t, err)
			streaming, err := provider.Prepare(ctx, scope, cameramodel.AuthStreaming, "streaming", material)
			require.NoError(t, err)
			configPath := filepath.Join(dir, "producer.yaml")
			before := []byte("producer:\n  credentials:\n    provider: cameras\n    directory: " + secrets + "\n  sources:\n    - alias: unrelated\n      input: push\n      format: h264\n      fps: 30\n      max_bitrate: 2Mbps\n")
			require.NoError(t, os.WriteFile(configPath, before, 0600))
			sessionPath := filepath.Join(dir, "cp.session")
			require.NoError(t, os.WriteFile(sessionPath, []byte("shale_tenant="+test.value+"\n"), 0600))
			const workerToken = "camera-provisioner-worker-token-32-bytes"
			workerPath := filepath.Join(dir, "worker.token")
			require.NoError(t, os.WriteFile(workerPath, []byte(workerToken), 0600))
			cp := provisionhost.ConnectionConfig{URL: "http://" + c.running.TenantAddr, SessionFile: sessionPath, AllowLoopbackHTTP: true}
			if test.asBearer {
				cp.SessionFile, cp.TokenFile = "", sessionPath
			}
			built, err := provisionhost.Build(provisionhost.Config{
				StateDir:  filepath.Join(dir, "state"),
				Networks:  []provisionhost.Network{{ID: "lan", CIDRs: []string{"127.0.0.0/8"}}},
				Providers: []provisionhost.ProviderConfig{{Name: "cameras", Directory: secrets}},
				Clients:   []provisionhost.ClientConfig{{TokenFile: workerPath, Scopes: []cameramodel.EnrollmentScope{scope}, Worker: true}},
				Targets:   []provisionhost.TargetConfig{{Scope: scope, Provider: "cameras", ConfigPath: configPath, Restart: []string{"false"}, CP: cp}},
			})
			require.NoError(t, err)
			defer built.Close()
			handler, err := built.Host.Handler()
			require.NoError(t, err)
			// Observe two real keyframes after RTSP startup before testing the
			// CP authentication boundary; a single keyframe cannot prove GOP.
			q := provision.Request{ID: "session-camera", BindingID: "main-role", Scope: scope, Device: device.Hint("physical-camera"), Desired: cameramodel.DesiredState{ProfileToken: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, GOP: 10, Revision: "v1"}, Alias: "session-main", Provider: "cameras", ManagementRef: &management, StreamingRef: &streaming, AllowProducerRestart: true, ProbeDuration: 2500 * time.Millisecond}
			body, err := json.Marshal(map[string]any{"request": q, "apply": true})
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/v1/execute", bytes.NewReader(body)).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer "+workerToken)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			require.Equal(t, http.StatusOK, response.Code)
			var result provisionhost.ExecutionResult
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.NotNil(t, result.Run.Verified, "request must reach the CP authority after media qualification")
			require.True(t, result.Run.Verified.Decoded)
			require.Greater(t, result.Run.Verified.Profile.GOP, 0)
			require.LessOrEqual(t, result.Run.Verified.Profile.GOP, q.Desired.GOP+2)
			require.NotEqual(t, provision.Active, result.Run.Stage)
			require.NotEmpty(t, result.Code)
			after, err := os.ReadFile(configPath)
			require.NoError(t, err)
			if test.accepted {
				require.Contains(t, string(after), "session-main", "valid CP session must pass assignment/alias checks and deploy before the intentional restart refusal")
			} else {
				require.Equal(t, string(before), string(after), "invalid CP authentication must fail before changing the capture manifest")
			}
			require.False(t, strings.Contains(response.Body.String(), test.value), "session material must not enter checkpoints")
		})
	}
}
