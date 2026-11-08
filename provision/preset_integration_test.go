package provision_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type countedPresetProvider struct {
	provider camera.DevicePresetProvider
	calls    int
}

func (p *countedPresetProvider) Resolve(ctx context.Context, scope camera.EnrollmentScope, device camera.DeviceHint) (camera.DevicePreset, error) {
	p.calls++
	return p.provider.Resolve(ctx, scope, device)
}

func protocolPresetProvider(t *testing.T, q provision.Request, revision string, fallbackFPS float64) *countedPresetProvider {
	t.Helper()
	p, err := preset.New(preset.Config{ID: "camera-lab", Revision: revision, Rules: []preset.Rule{{
		ID: "synthetic-c420", Scope: q.Scope,
		Match: preset.Match{Manufacturer: "TP-Link", Model: "EasyCam C420", Serial: "synthetic-camera"},
		Settings: camera.PresetSettings{Video: camera.DesiredState{
			ProfileToken: "main", Codec: "h264", Width: 160, Height: 90, FPS: 99, GOP: 10,
		}},
		Fallbacks: []camera.PresetSettings{{Video: camera.DesiredState{FPS: fallbackFPS}}},
		Required:  []string{camera.PresetFieldCodec, camera.PresetFieldResolution},
	}}})
	require.NoError(t, err)
	return &countedPresetProvider{provider: p}
}

// This follows real authenticated ONVIF, synthetic RTSP, independent ffprobe /
// ffmpeg decode, durable file credentials, and atomic capture deployment. The
// existing protocol fixture controls only the final CP registration authority;
// full Producer/CP/live/storage qualification has its separate integration suite.
func TestProtocolPresetFallbackValidatesRealMediaAndResumesSameDeployment(t *testing.T) {
	f := protocolFixture(t)
	f.request.ID = "preset-protocol-run"
	f.request.UsePreset = true
	f.request.Desired = camera.DesiredState{}
	f.authority.ready = false
	// Force a real encoder write for the selected candidate. The synthetic
	// stream is H.264; the initial ONVIF configuration advertises HEVC.
	uri := cameratestStreamURI(t, f)
	f.device.SetVideo(camera.MediaProfile{Token: "main", Name: "Main", Codec: "h265", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: uri})
	provider := protocolPresetProvider(t, f.request, "rules-1", 10)
	engine := f.engine(t)
	engine.Presets = provider
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	run, err := engine.Execute(ctx, f.request, true)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.Equal(t, provision.Binding, run.Stage)
	require.Equal(t, 1, provider.calls)
	require.NotNil(t, run.Preset)
	require.Equal(t, []string{"synthetic-c420"}, run.Preset.MatchedRules)
	require.Equal(t, float64(99), run.Preset.Requested.Video.FPS)
	require.Equal(t, camera.DesiredState{}, run.Request.Desired)
	require.NotNil(t, run.Effective)
	require.Equal(t, float64(10), run.Effective.Video.FPS)
	require.Equal(t, 1, run.CandidateIndex)
	require.Len(t, run.Warnings, 1)
	require.Equal(t, camera.PresetFieldFPS, run.Warnings[0].Field)
	require.Equal(t, "99", run.Warnings[0].Requested)
	require.Equal(t, "10", run.Warnings[0].Applied)
	require.NotEmpty(t, run.Warnings[0].Reason)
	require.NotNil(t, run.Verified)
	require.True(t, run.Verified.Complete)
	require.True(t, run.Verified.Decoded)
	require.Equal(t, "h264", run.Verified.Profile.Codec)
	require.Equal(t, 160, run.Verified.Profile.Width)
	require.Equal(t, 90, run.Verified.Profile.Height)
	require.InDelta(t, 10, run.Verified.Profile.FPS, .2)
	require.Equal(t, camera.PresetRevision(run.Preset.Ref), run.Verified.ProfileRevision)
	configurationWrites, activationWrites := f.device.Writes()
	require.Equal(t, 1, configurationWrites, "unsupported 99fps must be rejected before a write")
	require.Zero(t, activationWrites)
	require.Equal(t, 1, f.restarts)
	pinned := run.Preset.Clone()
	warnings := append([]camera.PresetWarning(nil), run.Warnings...)
	manifest, err := os.ReadFile(f.configPath)
	require.NoError(t, err)
	var capture struct {
		Producer struct {
			Sources []struct {
				Alias  string `yaml:"alias"`
				FPS    int    `yaml:"fps"`
				Format string `yaml:"format"`
				Size   string `yaml:"size"`
			} `yaml:"sources"`
		} `yaml:"producer"`
	}
	require.NoError(t, yaml.Unmarshal(manifest, &capture))
	require.Len(t, capture.Producer.Sources, 2, "the existing unrelated source must remain")
	selected := capture.Producer.Sources[1]
	require.Equal(t, run.Binding.Alias, selected.Alias)
	require.Equal(t, 10, selected.FPS, "capture metadata must use the selected fallback")
	require.Equal(t, "h264", selected.Format)
	require.Equal(t, "160x90", selected.Size)
	store, err := filestore.Open(filepath.Join(f.dir, "runs"))
	require.NoError(t, err)
	checkpoint, err := store.Load(ctx, f.request.ID)
	require.NoError(t, err)
	require.Equal(t, pinned, *checkpoint.Preset)
	require.Equal(t, warnings, checkpoint.Warnings)
	// Reconstruct the engine/provider as another process would. An updated
	// policy now suggests 25 fps; the in-flight run must retain its earlier 10 fps
	// snapshot rather than refetch policy or restart the Producer.
	f.provider = credential.FileProvider{Dir: f.provider.Dir, Name: "file"}
	f.authority.ready = true
	updatedProvider := protocolPresetProvider(t, f.request, "rules-2", 25)
	resumed := f.engine(t)
	resumed.Presets = updatedProvider
	active, err := resumed.Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, active.Stage)
	require.Equal(t, "stable-logical-source", active.Binding.SourceID)
	require.Equal(t, run.Binding.DeploymentID, active.Binding.DeploymentID)
	require.Equal(t, pinned, *active.Preset)
	require.Equal(t, warnings, active.Warnings)
	require.Equal(t, float64(10), active.Effective.Video.FPS)
	require.True(t, active.Verified.Decoded)
	require.InDelta(t, 10, active.Verified.Profile.FPS, .2)
	require.Zero(t, updatedProvider.calls, "resume must use the persisted preset snapshot")
	require.Equal(t, 1, f.restarts)
	configurationWrites, activationWrites = f.device.Writes()
	require.Equal(t, 1, configurationWrites)
	require.Zero(t, activationWrites)
	afterResume, err := os.ReadFile(f.configPath)
	require.NoError(t, err)
	require.Equal(t, manifest, afterResume)
	// Completed delivery is idempotent too: no protocol calls, event changes,
	// new Source, new capture manifest or producer restart.
	calls := len(f.device.Calls())
	again, err := f.engine(t).Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, active, again)
	require.Len(t, f.device.Calls(), calls)
	require.Equal(t, 1, f.restarts)
	afterReplay, err := os.ReadFile(f.configPath)
	require.NoError(t, err)
	require.Equal(t, manifest, afterReplay)
	f.assertNoSecrets(t)
}
