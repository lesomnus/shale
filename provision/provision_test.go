package provision_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
)

type device struct {
	ready                     bool
	profile                   camera.MediaProfile
	activations, writes       int
	failActivation, failApply bool
}

func (d *device) Activation(context.Context, camera.DeviceHint) (camera.ActivationState, error) {
	if d.ready {
		return camera.ActivationReady, nil
	}
	return camera.ActivationRequired, nil
}
func (d *device) Activate(_ context.Context, _ camera.DeviceHint, l camera.CredentialLease) error {
	if err := l.Use(context.Background(), func(m camera.CredentialMaterial) error {
		if m.Password != "prepared-password" {
			return camera.ErrForbidden
		}
		return nil
	}); err != nil {
		return err
	}
	d.activations++
	d.ready = true
	if d.failActivation {
		d.failActivation = false
		return errors.New("lost response rtsp://administrator:prepared-password@camera/")
	}
	return nil
}
func (d *device) Inspect(_ context.Context, h camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
	if !d.ready {
		return camera.Inspection{}, camera.ErrManualActivation
	}
	if err := l.Use(context.Background(), func(camera.CredentialMaterial) error { return nil }); err != nil {
		return camera.Inspection{}, err
	}
	h.Serial = "authenticated-serial"
	h.Hardware = "hardware1"
	return camera.Inspection{Device: h, Activation: camera.ActivationReady, Current: camera.CurrentState{Firmware: "v1", Profiles: []camera.MediaProfile{d.profile}}}, nil
}
func (d *device) Plan(_ context.Context, i camera.Inspection, w camera.DesiredState) (camera.ChangePlan, error) {
	p := i.Current.Profiles[0]
	after := p
	after.Codec = w.Codec
	plan := camera.ChangePlan{ProfileToken: p.Token, Before: p, After: after, Revision: w.Revision}
	if p.Codec != after.Codec {
		plan.Changes = []string{"codec"}
	}
	return plan, nil
}
func (d *device) Apply(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, p camera.ChangePlan) (camera.CurrentState, error) {
	d.writes++
	d.profile = p.After
	if d.failApply {
		d.failApply = false
		return camera.CurrentState{}, errors.New("secret subprocess error prepared-password")
	}
	return camera.CurrentState{Profiles: []camera.MediaProfile{d.profile}}, nil
}

type verifier struct{ incomplete bool }

func (v verifier) Verify(_ context.Context, q camera.MediaRequest) (camera.VerifiedState, error) {
	return camera.VerifiedState{Profile: q.Profile, Complete: !v.incomplete, Decoded: true, Firmware: "v1", ProfileRevision: q.Desired.Revision}, nil
}

type publisher struct {
	binding     provision.BindingState
	deploys     int
	failPublish bool
	notReady    bool
}

func (p *publisher) Publish(_ context.Context, d provision.Deployment) (provision.BindingState, error) {
	if p.binding.DeploymentID == "" {
		p.deploys++
		p.binding = provision.BindingState{BindingID: d.Request.BindingID, Alias: d.Previous.Alias, DeploymentID: d.OperationID, DeployedAt: time.Now().UTC()}
	}
	if p.failPublish {
		p.failPublish = false
		return provision.BindingState{}, errors.New("controller died after fsync")
	}
	return p.binding, nil
}
func (p *publisher) Observe(_ context.Context, _ provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	if p.notReady {
		return b, provision.ErrNotReady
	}
	b.SourceID = "one-source"
	b.Ingest = true
	b.Live = true
	b.Committed = true
	b.CommittedAt = time.Now().UTC()
	return b, nil
}
func fixture(t *testing.T) (provision.Request, *device, *publisher, *provision.Engine, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := filestore.Open(dir)
	require.NoError(t, err)
	q := provision.Request{ID: "request-1", BindingID: "front-door", Scope: camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, Device: camera.DeviceHint{ID: "camera-physical", Endpoints: []camera.Endpoint{{URL: "http://camera/onvif/device_service", NetworkScope: "lan"}}}, Desired: camera.DesiredState{ProfileToken: "main", Codec: "h264", Revision: "r1"}, Provider: "vault", AllowActivation: true, AllowProducerRestart: true}
	var entries []camera.StaticCredential
	for _, purpose := range []camera.AuthPurpose{camera.AuthInitialAdmin, camera.AuthManagement, camera.AuthStreaming} {
		entries = append(entries, camera.StaticCredential{Ref: camera.CredentialRef{Provider: "vault", ID: string(purpose), Version: "1", Scope: q.Scope, Purpose: purpose, Recoverable: true}, Access: camera.AccessAdministrator, Material: camera.CredentialMaterial{Username: "administrator", Password: "prepared-password"}})
	}
	auth, err := camera.NewStaticProvider(entries)
	require.NoError(t, err)
	d := &device{profile: camera.MediaProfile{Token: "main", Codec: "h265", Width: 1280, Height: 720, FPS: 30, StreamURI: "rtsp://camera/main"}}
	p := &publisher{}
	e := &provision.Engine{Store: s, Auth: auth, Inspector: d, Configurator: d, Bootstrapper: d, Verifier: verifier{}, Publisher: p}
	return q, d, p, e, dir
}
func TestExternalCallerPlansWithoutWritesThenActivatesAndResumes(t *testing.T) {
	q, d, p, e, dir := fixture(t)
	ctx := context.Background()
	r, err := e.Execute(ctx, q, false)
	require.NoError(t, err)
	require.Equal(t, provision.Planned, r.Stage)
	require.Zero(t, d.activations)
	require.Zero(t, d.writes)
	d.failActivation = true
	r, err = e.Execute(ctx, q, true)
	require.Error(t, err)
	require.Equal(t, 1, d.activations)
	require.NotNil(t, r.InitialRef)
	b, err := json.Marshal(r)
	require.NoError(t, err)
	require.NotContains(t, string(b), "prepared-password")
	s, err := filestore.Open(dir)
	require.NoError(t, err)
	e.Store = s
	d.failApply = true
	r, err = e.Execute(ctx, q, true)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "prepared-password")
	require.Equal(t, 1, d.writes)
	p.failPublish = true
	r, err = e.Execute(ctx, q, true)
	require.Error(t, err)
	require.Equal(t, 1, d.activations)
	require.Equal(t, 1, d.writes)
	r, err = e.Execute(ctx, q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, "one-source", r.Binding.SourceID)
	r, err = e.Execute(ctx, q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, p.deploys)
	require.Equal(t, 1, d.activations)
	require.Equal(t, 1, d.writes)
}
func TestUnrecoverableActivationWaitsAndLaterCredentialsResume(t *testing.T) {
	q, d, _, e, _ := fixture(t)
	good := e.Auth
	rf := camera.CredentialRef{Provider: q.Provider, ID: "ephemeral", Version: "1", Scope: q.Scope, Purpose: camera.AuthInitialAdmin, Recoverable: false}
	p, err := camera.NewStaticProvider([]camera.StaticCredential{{Ref: rf, Access: camera.AccessAdministrator}})
	require.NoError(t, err)
	e.Auth = p
	r, err := e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, camera.ErrNeedsUserInput)
	require.Equal(t, provision.WaitingInput, r.Stage)
	require.Zero(t, d.activations)
	e.Auth = good
	r, err = e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
}
func TestInspectPlanExistingDeviceDoesNotMutate(t *testing.T) {
	q, d, p, e, _ := fixture(t)
	d.ready = true
	r, err := e.Execute(context.Background(), q, false)
	require.NoError(t, err)
	require.Equal(t, provision.Planned, r.Stage)
	require.NotEmpty(t, r.Plan.Changes)
	require.Zero(t, d.activations)
	require.Zero(t, d.writes)
	require.Zero(t, p.deploys)
}
func TestMetadataAndIncompleteMediaCannotBecomeActive(t *testing.T) {
	t.Run("incomplete decode", func(t *testing.T) {
		q, d, p, e, _ := fixture(t)
		d.ready = true
		e.Verifier = verifier{incomplete: true}
		r, err := e.Execute(context.Background(), q, true)
		require.ErrorIs(t, err, provision.ErrNotReady)
		require.NotEqual(t, provision.Active, r.Stage)
		require.Zero(t, p.deploys)
	})
	t.Run("committed segment pending", func(t *testing.T) {
		q, d, p, e, _ := fixture(t)
		d.ready = true
		p.notReady = true
		r, err := e.Execute(context.Background(), q, true)
		require.ErrorIs(t, err, provision.ErrNotReady)
		require.Equal(t, provision.Binding, r.Stage)
		p.notReady = false
		r, err = e.Execute(context.Background(), q, true)
		require.NoError(t, err)
		require.Equal(t, provision.Active, r.Stage)
		require.Equal(t, 1, p.deploys)
	})
}
func TestAssignmentAndRequestConflicts(t *testing.T) {
	q, d, _, e, _ := fixture(t)
	d.ready = true
	_, err := e.Execute(context.Background(), q, false)
	require.NoError(t, err)
	other := q
	other.ID = "other"
	other.BindingID = "another"
	_, err = e.Execute(context.Background(), other, true)
	require.ErrorIs(t, err, provision.ErrConflict)
	other = q
	other.Desired.Codec = "h265"
	_, err = e.Execute(context.Background(), other, true)
	require.ErrorIs(t, err, provision.ErrConflict)
	other = q
	other.ID = "new-address"
	other.Device.Endpoints[0].URL = "http://new-camera/onvif/device_service"
	_, err = e.Execute(context.Background(), other, false)
	require.NoError(t, err)
	other = q
	other.ID = "new-set"
	other.Scope.SetID = "other-set"
	_, err = e.Execute(context.Background(), other, true)
	require.ErrorIs(t, err, provision.ErrConflict)
}
func TestRequestRejectsSecretURIsBeforePersistence(t *testing.T) {
	q, _, _, e, dir := fixture(t)
	q.Device.Endpoints[0].URL = "http://admin:password@camera/"
	_, err := e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, camera.ErrInvalid)
	es, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, es)
}
func TestFileStoreSerializesAcrossInstancesAndRejectsStaleWrites(t *testing.T) {
	q, _, _, e, dir := fixture(t)
	s2, err := filestore.Open(dir)
	require.NoError(t, err)
	ctx := context.Background()
	unlock, err := e.Store.Lock(ctx, "physical")
	require.NoError(t, err)
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = s2.Lock(blocked, "physical")
	require.Error(t, err)
	require.NoError(t, unlock())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s2.Lock(ctx, "physical")
			require.NoError(t, err)
			require.NoError(t, u())
		}()
	}
	wg.Wait()
	r := provision.Run{Request: q, Version: 1}
	require.NoError(t, e.Store.Save(ctx, r))
	require.ErrorIs(t, s2.Save(ctx, r), provision.ErrConflict)
	b, err := os.ReadFile(filepath.Join(dir, "run-"+provision.Key(q.ID)+".json"))
	require.NoError(t, err)
	require.True(t, json.Valid(b))
	require.False(t, strings.Contains(string(b), "password"))
}
