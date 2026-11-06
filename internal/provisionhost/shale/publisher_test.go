package shale

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type fakeAuthority struct {
	denied   bool
	notReady bool
	checks   int
}

func (a *fakeAuthority) Check(context.Context, provision.Deployment, string) error {
	a.checks++
	if a.denied {
		return camera.ErrForbidden
	}
	return nil
}
func (a *fakeAuthority) Observe(_ context.Context, _ provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	if a.notReady {
		return b, provision.ErrNotReady
	}
	b.SourceID = "source"
	b.Ingest = true
	b.Live = true
	b.Committed = true
	b.CommittedAt = time.Now()
	return b, nil
}
func publisherFixture(t *testing.T) (*FilePublisher, provision.Deployment, *fakeAuthority, *int) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "shale.yaml")
	require.NoError(t, os.WriteFile(path, []byte("cp: https://cp.example\nproducer:\n  hardware_id: existing-producer\n  credentials:\n    directory: /run/secrets/camera\n    provider: vault\n  sources:\n    - alias: existing\n      input: rtsp://unrelated/stream\n      encoder: copy\n      audio:\n        codec: aac\ncustom:\n  preserve: yes\n"), 0600))
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	d := provision.Deployment{OperationID: "operation1", Request: provision.Request{ID: "job", BindingID: "front-door", Name: "Entrance", Scope: scope, Device: camera.DeviceHint{ID: "physical1"}, AllowProducerRestart: true}, Profile: camera.MediaProfile{Token: "main", Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: 2_000_000, StreamURI: "rtsp://camera/stream"}, Credential: camera.CredentialRef{Provider: "vault", ID: "stream", Version: "1", Scope: scope, Purpose: camera.AuthStreaming, Recoverable: true}, Previous: provision.BindingState{BindingID: "front-door", Alias: "camera-alias"}}
	secretDir := filepath.Join(dir, "secrets")
	require.NoError(t, (credential.FileProvider{Dir: secretDir, Name: "vault"}).Put(context.Background(), d.Credential, camera.CredentialMaterial{Username: "viewer", Password: "do-not-serialize"}))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte(strings.ReplaceAll(string(b), "/run/secrets/camera", secretDir)), 0600))
	a := &fakeAuthority{}
	restarts := new(int)
	p := &FilePublisher{ConfigPath: path, StateDir: filepath.Join(dir, "deployments"), Scope: scope, Authority: a, Restart: func(context.Context) error { *restarts++; return nil }}
	return p, d, a, restarts
}
func TestFileDeployRetryDHCPAndRollbackPreserveOtherSources(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	ctx := context.Background()
	b, err := p.Publish(ctx, d)
	require.NoError(t, err)
	require.Equal(t, 1, *restarts)
	require.Equal(t, "front-door", b.BindingID)
	config, err := os.ReadFile(p.ConfigPath)
	require.NoError(t, err)
	var root map[string]any
	require.NoError(t, yaml.Unmarshal(config, &root))
	require.Equal(t, "https://cp.example", root["cp"])
	require.Equal(t, "yes", root["custom"].(map[string]any)["preserve"])
	sources := root["producer"].(map[string]any)["sources"].([]any)
	require.Len(t, sources, 2)
	require.Equal(t, "aac", sources[0].(map[string]any)["audio"].(map[string]any)["codec"])
	added := sources[1].(map[string]any)
	require.Equal(t, "rtsp://camera/stream", added["input"])
	require.Equal(t, "stream", added["input_credential"].(map[string]any)["id"])
	_, err = p.Publish(ctx, d)
	require.NoError(t, err)
	require.Equal(t, 1, *restarts)
	// A different observed address updates capture, retaining its binding/alias.
	d.OperationID = "operation2"
	d.Profile.StreamURI = "rtsp://new-address/stream"
	d.Previous = b
	b2, err := p.Publish(ctx, d)
	require.NoError(t, err)
	require.Equal(t, b.Alias, b2.Alias)
	require.Equal(t, 2, *restarts)
	require.NoError(t, p.Rollback(ctx, d.OperationID))
	require.Equal(t, 3, *restarts)
	config, err = os.ReadFile(p.ConfigPath)
	require.NoError(t, err)
	require.Contains(t, string(config), "rtsp://camera/stream")
	require.NotContains(t, string(config), "new-address")
	require.Contains(t, string(config), "existing-producer")
	// Repeated rollback never restarts, or deletes an existing Source/archive.
	require.NoError(t, p.Rollback(ctx, d.OperationID))
	require.Equal(t, 3, *restarts)
}
func TestCrashAfterManifestInstallRetriesOnlyConfiguredRestart(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	failed := true
	p.Restart = func(context.Context) error {
		*restarts++
		if failed {
			failed = false
			return errors.New("restart response lost")
		}
		return nil
	}
	_, err := p.Publish(context.Background(), d)
	require.Error(t, err)
	// Recreate the deployer as after process death. The journal recognizes the
	// complete new manifest and never adds a second source or selects new creds.
	p2 := *p
	b, err := p2.Publish(context.Background(), d)
	require.NoError(t, err)
	require.Equal(t, d.OperationID, b.DeploymentID)
	require.Equal(t, 2, *restarts)
	_, sources, _, err := readConfig(p.ConfigPath)
	require.NoError(t, err)
	require.Len(t, sources.Content, 2)
	info, err := os.Stat(p.ConfigPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(p.ConfigPath), ".provision-*"))
	require.NoError(t, err)
	require.Empty(t, temps)
}
func TestUnauthorizedAssignmentAndRestartPolicyHaveNoCaptureWrite(t *testing.T) {
	p, d, a, restarts := publisherFixture(t)
	before, err := os.ReadFile(p.ConfigPath)
	require.NoError(t, err)
	a.denied = true
	_, err = p.Publish(context.Background(), d)
	require.ErrorIs(t, err, camera.ErrForbidden)
	a.denied = false
	d.Request.AllowProducerRestart = false
	_, err = p.Publish(context.Background(), d)
	require.ErrorIs(t, err, camera.ErrNeedsUserInput)
	after, err := os.ReadFile(p.ConfigPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, *restarts)
	d.Request.Scope.SetID = "other"
	_, err = p.Publish(context.Background(), d)
	require.ErrorIs(t, err, camera.ErrForbidden)
}
func TestExistingAliasAndSecretURIRefused(t *testing.T) {
	t.Run("unclaimed alias", func(t *testing.T) {
		p, d, _, _ := publisherFixture(t)
		d.Previous.Alias = "existing"
		_, err := p.Publish(context.Background(), d)
		require.ErrorIs(t, err, provision.ErrConflict)
	})
	t.Run("secret URI", func(t *testing.T) {
		p, d, _, _ := publisherFixture(t)
		d.Profile.StreamURI = "rtsp://user:password@camera/"
		_, err := p.Publish(context.Background(), d)
		require.ErrorIs(t, err, camera.ErrInvalid)
		paths, err := filepath.Glob(filepath.Join(p.StateDir, "deployment-*.json"))
		require.NoError(t, err)
		require.Empty(t, paths)
	})
}
func TestConfigDriftDoesNotOverwriteAnotherWritersChanges(t *testing.T) {
	p, d, _, _ := publisherFixture(t)
	_, err := p.Publish(context.Background(), d)
	require.NoError(t, err)
	b, err := os.ReadFile(p.ConfigPath)
	require.NoError(t, err)
	b = []byte(strings.ReplaceAll(string(b), "rtsp://camera/stream", "rtsp://changed-externally/stream"))
	require.NoError(t, os.WriteFile(p.ConfigPath, b, 0600))
	_, err = p.Publish(context.Background(), d)
	require.ErrorIs(t, err, provision.ErrConflict)
	require.ErrorIs(t, p.Rollback(context.Background(), d.OperationID), provision.ErrConflict)
}

func TestRollbackRestartsAgainAfterLostResponse(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	ctx := context.Background()
	_, err := p.Publish(ctx, d)
	require.NoError(t, err)
	fail := true
	p.Restart = func(context.Context) error {
		*restarts++
		if fail {
			fail = false
			return errors.New("restart interrupted")
		}
		return nil
	}
	require.Error(t, p.Rollback(ctx, d.OperationID))
	require.Equal(t, 2, *restarts)
	// Manifest already contains Before, but a lost restart response is not a
	// durable assertion that the running process consumed it.
	require.NoError(t, p.Rollback(ctx, d.OperationID))
	require.Equal(t, 3, *restarts)
	require.NoError(t, p.Rollback(ctx, d.OperationID))
	require.Equal(t, 3, *restarts)
}

func TestMatchingManifestWithoutJournalStillRequiresRuntimeRestart(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	ctx := context.Background()
	_, err := p.Publish(ctx, d)
	require.NoError(t, err)
	require.Equal(t, 1, *restarts)
	paths, err := filepath.Glob(filepath.Join(p.StateDir, "deployment-*.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	require.NoError(t, os.Remove(paths[0]))
	_, err = p.Publish(ctx, d)
	require.NoError(t, err)
	require.Equal(t, 2, *restarts)
}

func TestFractionalFPSDoesNotBecomeTruncatedCaptureMetadata(t *testing.T) {
	p, d, _, _ := publisherFixture(t)
	d.Profile.FPS = 29.97
	_, err := p.Publish(context.Background(), d)
	require.NoError(t, err)
	_, sources, _, err := readConfig(p.ConfigPath)
	require.NoError(t, err)
	require.Nil(t, mapping(sources.Content[1], "fps"))
	require.Equal(t, "copy", scalar(sources.Content[1], "encoder"))
}

func TestUnsupportedCopyCodecNeverRestartsProducer(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	d.Profile.Codec = "mjpeg"
	_, err := p.Publish(context.Background(), d)
	require.ErrorIs(t, err, camera.ErrUnsupported)
	require.Zero(t, *restarts)
}

func TestDeploymentBudgetsAudioAndRetainsCameraGOP(t *testing.T) {
	_, d, _, _ := publisherFixture(t)
	d.Profile.GOP = 60
	n, err := sourceNode(d, d.Previous.Alias)
	require.NoError(t, err)
	require.Equal(t, "2128000bps", scalar(n, "max_bitrate"))
	require.Equal(t, "2s", scalar(n, "keyframe_interval"))
}

func TestReadOnlyDeploymentImpactDisclosesOtherFeeds(t *testing.T) {
	p, d, _, restarts := publisherFixture(t)
	impacts := p.Impacts(d.Request)
	require.Len(t, impacts, 1)
	require.Equal(t, d.Request.Scope.ProducerID, impacts[0].ProducerID)
	require.True(t, impacts[0].RestartMayBeRequired && impacts[0].AffectsOtherSources)
	require.Zero(t, *restarts)
}
