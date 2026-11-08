package provision_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	localpublisher "github.com/lesomnus/shale/internal/provisionhost/shale"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type variableRateFacts struct {
	calls int
	spike bool
}

func (v *variableRateFacts) Verify(_ context.Context, q camera.MediaRequest) (camera.VerifiedState, error) {
	v.calls++
	p := q.Profile
	p.FPS = 30 - 0.01*float64(v.calls)
	p.Bitrate = 800_000 + 10_000*int64(v.calls)
	if v.spike && v.calls == 2 {
		p.Bitrate = 3_000_000
	}
	return camera.VerifiedState{Profile: p, Complete: true, Decoded: true, DecodedFrames: 150, Duration: q.Duration, ObservedAt: time.Now().UTC(), Firmware: "v1", ProfileRevision: q.Desired.Revision}, nil
}

type publicationPendingAuthority struct{}

func (publicationPendingAuthority) Check(context.Context, provision.Deployment, string) error {
	return nil
}
func (publicationPendingAuthority) Observe(_ context.Context, _ provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	return b, provision.ErrNotReady
}

type losePublicationResponse struct {
	publisher *localpublisher.FilePublisher
	lost      bool
	failure   error
	calls     int
}

func (p *losePublicationResponse) Publish(ctx context.Context, d provision.Deployment) (provision.BindingState, error) {
	p.calls++
	b, err := p.publisher.Publish(ctx, d)
	p.failure = err
	if err == nil && !p.lost {
		p.lost = true
		return provision.BindingState{}, errors.New("fixture lost committed publication response")
	}
	return b, err
}
func (p *losePublicationResponse) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	return p.publisher.Observe(ctx, d, b)
}

func TestStandaloneUnknownRatePublicationRecoversCommittedPayload(t *testing.T) {
	standaloneUnknownPublication(t, false, false)
}
func TestStandaloneUnknownRateCannotOutgrowPublishedCeiling(t *testing.T) {
	standaloneUnknownPublication(t, true, false)
}
func TestStandaloneLegacyUncertainPublicationRequiresReconciliation(t *testing.T) {
	standaloneUnknownPublication(t, false, true)
}
func standaloneUnknownPublication(t *testing.T, spike, legacy bool) {
	q, d, _, engine, stateDir := fixture(t)
	d.ready = true
	d.profile.FPS = 0
	d.profile.Bitrate = 0
	probe := &variableRateFacts{spike: spike}
	engine.Verifier = probe
	dir := t.TempDir()
	secretDir := filepath.Join(dir, "credentials")
	ref := camera.CredentialRef{Provider: "vault", ID: "streaming", Version: "1", Scope: q.Scope, Purpose: camera.AuthStreaming, Recoverable: true}
	require.NoError(t, (credential.FileProvider{Dir: secretDir, Name: "vault"}).Put(t.Context(), ref, camera.CredentialMaterial{Username: "administrator", Password: "prepared-password"}))
	config, err := yaml.Marshal(map[string]any{"cp": "https://cp.fixture", "producer": map[string]any{"hardware_id": "fixture-producer", "credentials": map[string]any{"directory": secretDir, "provider": "vault"}, "sources": []any{map[string]any{"alias": "unrelated", "input": "rtsp://unrelated/main", "encoder": "copy"}}}})
	require.NoError(t, err)
	path := filepath.Join(dir, "shale.yaml")
	require.NoError(t, os.WriteFile(path, config, 0600))
	restarts := 0
	p := &losePublicationResponse{publisher: &localpublisher.FilePublisher{ConfigPath: path, StateDir: filepath.Join(dir, "publication"), Scope: q.Scope, Authority: publicationPendingAuthority{}, Restart: func(context.Context) error { restarts++; return nil }}}
	engine.Publisher = p
	first, err := engine.Execute(t.Context(), q, true)
	require.Error(t, err)
	require.True(t, p.lost, "fixture must reach its committed lost-response boundary: %v", p.failure)
	require.NotNil(t, first.DeploymentProfile)
	require.NotEmpty(t, first.Binding.DeploymentID)
	require.Zero(t, first.Binding.DeployedAt, "an intent checkpoint does not claim publication success")
	committed, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 1, restarts)
	if legacy {
		old, err := engine.Store.Load(t.Context(), q.ID)
		require.NoError(t, err)
		old.DeploymentProfile = nil
		old.Binding.DeploymentID = ""
		old.Version++
		old.UpdatedAt = time.Now().UTC()
		require.NoError(t, engine.Store.Save(t.Context(), old))
	}
	engine.Store, err = filestore.Open(stateDir)
	require.NoError(t, err)
	second, err := engine.Execute(t.Context(), q, true)
	if legacy {
		require.ErrorIs(t, err, provision.ErrConflict)
		again, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, committed, again)
		require.Equal(t, 1, restarts)
		require.Equal(t, 1, p.calls)
		require.Equal(t, 1, probe.calls, "old ambiguous intent must stop before another camera operation")
		return
	}
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.Equal(t, first.Binding.DeploymentID, second.Binding.DeploymentID)
	require.Equal(t, first.DeploymentProfile, second.DeploymentProfile)
	require.NotEqual(t, first.Verified.Profile.Bitrate, second.Verified.Profile.Bitrate)
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, committed, again)
	require.Equal(t, 1, restarts, "recovering a lost response must not restart capture again")
	if spike {
		require.Equal(t, 1, p.calls, "media beyond the pinned ceiling cannot re-enter publication")
	} else {
		require.Equal(t, 2, p.calls)
	}
}
