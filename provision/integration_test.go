package provision_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/cameratest"
	shalehost "github.com/lesomnus/shale/internal/provisionhost/shale"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// This fixture uses actual ONVIF/TP-Link, file-provider, durable engine,
// independent ffprobe+ffmpeg, and atomic capture deployment. The authority seam
// is controlled here; the Producer integration suite separately proves real CP,
// capture, WHEP and committed storage rather than substituting a Source row.
type integrationAuthority struct {
	ready    bool
	observed int
}

func (a *integrationAuthority) Check(context.Context, provision.Deployment, string) error { return nil }
func (a *integrationAuthority) Observe(_ context.Context, _ provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	a.observed++
	if !a.ready {
		return b, provision.ErrNotReady
	}
	b.SourceID = "stable-logical-source"
	b.Ingest, b.Live, b.Committed = true, true, true
	b.CommittedAt = time.Now().UTC()
	return b, nil
}

type protocolEnrollment struct {
	request         provision.Request
	device          *cameratest.ONVIFCamera
	provider        credential.FileProvider
	dir, configPath string
	authority       *integrationAuthority
	restarts        int
	bootstrap       bool
}

func protocolFixture(t *testing.T) *protocolEnrollment {
	t.Helper()
	for _, binary := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Skip(binary + " required for synthetic media conformance")
		}
	}
	rtsp := cameratest.ServeRTSP(t)
	profile := camera.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: rtsp}
	device := cameratest.ServeONVIF(t, profile)
	device.RequireCredentials("admin", "prepared +:/?#% password")
	dir := t.TempDir()
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "existing-set", ProducerID: "adopted-producer", NetworkID: "lan"}
	q := provision.Request{ID: "protocol-run", BindingID: "front-door", Scope: scope, Device: device.Hint("claimed-device"), Desired: camera.DesiredState{ProfileToken: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, GOP: 10, Revision: "desired-1"}, Provider: "file", AllowProducerRestart: true, ProbeDuration: 1500 * time.Millisecond}
	provider := credential.FileProvider{Dir: filepath.Join(dir, "secrets"), Name: "file"}
	management, err := provider.Prepare(context.Background(), scope, camera.AuthManagement, "management", camera.CredentialMaterial{Username: "admin", Password: "prepared +:/?#% password"})
	require.NoError(t, err)
	initial, err := provider.Prepare(context.Background(), scope, camera.AuthInitialAdmin, provision.Key(scope.TenantID, q.BindingID, "initial_admin"), camera.CredentialMaterial{Username: "admin", Password: "prepared +:/?#% password"})
	require.NoError(t, err)
	stream, err := provider.Prepare(context.Background(), scope, camera.AuthStreaming, "streaming", camera.CredentialMaterial{Username: "stream-reader", Password: "stream +:/?#% password"})
	require.NoError(t, err)
	q.ManagementRef, q.InitialRef, q.StreamingRef = &management, &initial, &stream
	path := filepath.Join(dir, "producer.yaml")
	manifest, err := yaml.Marshal(map[string]any{"producer": map[string]any{"hardware_id": "existing-producer", "credentials": map[string]any{"directory": provider.Dir, "provider": "file"}, "sources": []any{map[string]any{"alias": "unrelated", "input": "rtsp://unrelated-camera/stream", "encoder": "copy"}}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, manifest, 0600))
	return &protocolEnrollment{request: q, device: device, provider: provider, dir: dir, configPath: path, authority: &integrationAuthority{ready: true}}
}

func (f *protocolEnrollment) engine(t *testing.T) *provision.Engine {
	t.Helper()
	store, err := filestore.Open(filepath.Join(f.dir, "runs"))
	require.NoError(t, err)
	baseline := onvif.NewClient(nil)
	e := &provision.Engine{Store: store, Auth: f.provider, Inspector: baseline, Configurator: baseline, Verifier: &media.Verifier{AuthProvider: f.provider, NetworkScope: "lan", StartupTimeout: 3 * time.Second}, Publisher: &shalehost.FilePublisher{ConfigPath: f.configPath, StateDir: filepath.Join(f.dir, "deployments"), Scope: f.request.Scope, Authority: f.authority, Restart: func(context.Context) error { f.restarts++; return nil }}}
	if f.bootstrap {
		vendor := tplink.NewDriver(baseline, f.device.WebURL)
		e.Inspector, e.Configurator, e.Bootstrapper = vendor, vendor, vendor
	}
	return e
}

func (f *protocolEnrollment) assertNoSecrets(t *testing.T) {
	t.Helper()
	for _, path := range []string{filepath.Join(f.dir, "runs"), filepath.Join(f.dir, "deployments"), f.configPath} {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue // failure before deployment must not create rollback state
		}
		err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, secret := range []string{"prepared +:/?#% password", "stream +:/?#% password", "prepared%20", "stream%20"} {
				require.NotContains(t, string(b), secret, path)
			}
			return nil
		})
		require.NoError(t, err)
	}
}

func TestProtocolONVIFConfigureDecodeDeployAndResumeBinding(t *testing.T) {
	f := protocolFixture(t)
	f.authority.ready = false
	// The device initially reports HEVC; standard ONVIF configuration changes it
	// to the requested H.264 stream, which is then independently decoded.
	f.device.SetVideo(camera.MediaProfile{Token: "main", Name: "Main", Codec: "h265", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: cameratestStreamURI(t, f)})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := f.engine(t).Execute(ctx, f.request, true)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.Equal(t, provision.Binding, r.Stage)
	require.NotNil(t, r.Verified)
	require.True(t, r.Verified.Decoded)
	require.True(t, r.Verified.Complete)
	require.Equal(t, "h264", r.Verified.Profile.Codec)
	require.Equal(t, "desired-1", r.Verified.ProfileRevision)
	configuration, activation := f.device.Writes()
	require.Equal(t, 1, configuration)
	require.Zero(t, activation)
	require.Equal(t, 1, f.restarts)
	f.authority.ready = true
	r, err = f.engine(t).Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, "stable-logical-source", r.Binding.SourceID)
	configuration, activation = f.device.Writes()
	require.Equal(t, 1, configuration)
	require.Zero(t, activation)
	require.Equal(t, 1, f.restarts)
	content, err := os.ReadFile(f.configPath)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(content), "shale.provision.binding:"))
	require.Contains(t, string(content), "unrelated-camera")
	f.assertNoSecrets(t)
}

// Read the public profile through real protocol rather than reaching inside the
// simulator or copying an endpoint from a test-only field.
func cameratestStreamURI(t *testing.T, f *protocolEnrollment) string {
	t.Helper()
	l, err := camera.Acquire(context.Background(), f.provider, camera.AuthRequest{Scope: f.request.Scope, Device: f.request.Device, Purpose: camera.AuthManagement, ExistingRef: f.request.ManagementRef})
	require.NoError(t, err)
	defer l.Close()
	i, err := (&onvif.Client{}).Inspect(context.Background(), f.request.Device, l)
	require.NoError(t, err)
	return i.Current.Profiles[0].StreamURI
}

func TestProtocolFreshTPLinkRecoversPreparedPasswordAfterLostWriteResponse(t *testing.T) {
	f := protocolFixture(t)
	f.bootstrap = true
	f.request.AllowActivation = true
	f.device.SetUnactivated(true)
	f.device.LoseActivationResponseOnce()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	r, err := f.engine(t).Execute(ctx, f.request, true)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "password")
	require.True(t, r.ActivationAttempted)
	require.Equal(t, *f.request.InitialRef, *r.InitialRef)
	configuration, activation := f.device.Writes()
	require.Zero(t, configuration)
	require.Equal(t, 1, activation)
	require.Zero(t, f.restarts)
	// Reconstruct provider and engine from disk, and attempt to replace the
	// prepared intent. The durable version is preserved before resume.
	f.provider = credential.FileProvider{Dir: f.provider.Dir, Name: "file"}
	_, err = f.provider.Prepare(ctx, f.request.Scope, camera.AuthInitialAdmin, provision.Key(f.request.Scope.TenantID, f.request.BindingID, "initial_admin"), camera.CredentialMaterial{Username: "admin", Password: "different password"})
	require.ErrorIs(t, err, credential.ErrIntentConflict)
	r, err = f.engine(t).Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.True(t, r.ActivationComplete)
	require.Equal(t, *f.request.InitialRef, *r.InitialRef)
	configuration, activation = f.device.Writes()
	require.Zero(t, configuration)
	require.Equal(t, 1, activation)
	for _, operation := range f.device.Calls() {
		require.NotEqual(t, "boot_set_date", operation)
		require.NotEqual(t, "unexpected_vendor_write", operation)
	}
	f.assertNoSecrets(t)
}

func TestProtocolMedia2ReportedHEVCAndActualH264CannotBecomeActive(t *testing.T) {
	f := protocolFixture(t)
	f.bootstrap = true
	f.request.Desired.Codec = ""
	f.request.Desired.GOP = 0
	uri := cameratestStreamURI(t, f)
	f.device.SetVideo(camera.MediaProfile{Token: "main", Name: "Main", Codec: "h265", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: uri})
	f.device.SetMedia1Codec("h264")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, err := f.engine(t).Execute(ctx, f.request, true)
	require.Error(t, err)
	require.NotEqual(t, provision.Active, r.Stage)
	require.Equal(t, "h265", r.Inspection.Current.Profiles[0].Codec)
	require.Nil(t, r.Verified)
	require.Zero(t, f.restarts)
	require.Zero(t, f.authority.observed)
	f.assertNoSecrets(t)
}

func TestProtocolUnknownVendorActivationRemainsReadOnlyWaiting(t *testing.T) {
	f := protocolFixture(t)
	f.bootstrap = true
	f.request.AllowActivation = true
	f.request.Device.Firmware = "unknown firmware"
	f.device.SetUnactivated(true)
	r, err := f.engine(t).Execute(context.Background(), f.request, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, camera.ErrManualActivation))
	require.Equal(t, provision.WaitingInput, r.Stage)
	configuration, activation := f.device.Writes()
	require.Zero(t, configuration)
	require.Zero(t, activation)
	require.Empty(t, f.device.Calls())
}

func TestProtocolUnsettledCameraWriteBlocksNewDesiredRevision(t *testing.T) {
	f := protocolFixture(t)
	f.request.Desired.Bitrate = 2_000_000
	accepted, release, settled := f.device.HoldConfigurationResponseOnce(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	first, err := f.engine(t).Execute(ctx, f.request, true)
	require.Error(t, err)
	require.NotEqual(t, provision.Active, first.Stage)
	select {
	case <-accepted:
	default:
		t.Fatal("camera did not accept the pending write")
	}
	configuration, _ := f.device.Writes()
	require.Zero(t, configuration, "the firmware has not yet committed its accepted Set")
	newer := f.request
	newer.ID = "new-desired-revision"
	newer.Desired.Revision = "desired-2"
	newer.Desired.Bitrate = 1_000_000
	calls := len(f.device.Calls())
	_, err = f.engine(t).Execute(ctx, newer, true)
	require.ErrorIs(t, err, provision.ErrConflict)
	require.Len(t, f.device.Calls(), calls, "new desired state reached the device before the older write settled")
	// The old operation commits after its HTTP caller already received an
	// error. Reconstruct the original engine and reconcile that exact target;
	// only its verified completion releases the durable mutation reservation.
	release()
	<-settled
	first, err = f.engine(t).Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, first.Stage)
	require.Equal(t, int64(2_000_000), first.Inspection.Current.Profiles[0].Bitrate)
	second, err := f.engine(t).Execute(ctx, newer, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, second.Stage)
	require.Equal(t, int64(1_000_000), second.Inspection.Current.Profiles[0].Bitrate)
	require.Equal(t, first.Binding.SourceID, second.Binding.SourceID)
	configuration, _ = f.device.Writes()
	require.Equal(t, 2, configuration)
	f.assertNoSecrets(t)
}
