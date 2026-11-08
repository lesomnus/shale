package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const isolatedRecoveryUsername = "admin"
const isolatedRecoveryPassword = "CameraRecovery42Pass"

// Every fault uses the default Worker and Producer in separate processes.
// Native CP authorization, adapters, capture, storage and independent live
// decoding decide readiness. Only disposable topology process groups are killed.
func TestEdgeIsolatedRecovery(t *testing.T) {
	_ = qualificationTopology(t)
	t.Run("job_insert_rollback", isolatedRecoveryJobRollback)
	t.Run("observe_committed_response_loss", isolatedRecoveryObserveLoss)
	t.Run("activation_response_loss", isolatedRecoveryActivationLoss)
	t.Run("accepted_configuration", isolatedRecoveryConfigurationLoss)
	t.Run("publication_committed_response_loss", isolatedRecoveryPublicationLoss)
	t.Run("first_capture_upload", isolatedRecoveryCaptureCrash)
	if !t.Failed() {
		fmt.Println("SHQA_RESULT_V1 recovery")
	}
}

// Keep policy inactive until the fault is armed. Stopping only Worker after
// discovery/import prevents a policy_missing Observe consuming a success gate.
func isolatedRecoveryPrepare(t *testing.T) (*isolatedFixture, *api.CameraCredentialReference, edgeRuntimeStoredRole, edgeRuntimeStoredRole) {
	t.Helper()
	f := newIsolatedFixture(t, false)
	f.StartCamera("camera", false, isolatedRecoveryUsername, isolatedRecoveryPassword, cameratest.CameraIdentity{
		Serial: "isolated-recovery-camera", MAC: "02:00:00:00:02:02", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-000000000202",
	})
	f.ObserveDevice("camera")
	initial := f.Import("camera", "initial_admin", "administrator", isolatedRecoveryUsername, isolatedRecoveryPassword)
	worker, producer := f.RoleStatus("worker"), f.RoleStatus("producer")
	qualificationCall(t, f.EdgeRoles["worker"], "stop", nil, nil)
	return f, initial, worker, producer
}

func isolatedRecoveryCounts(t *testing.T, f *isolatedFixture) edgeControlEntityCounts {
	t.Helper()
	var state edgeControlStatus
	qualificationCall(t, f.Control, "status", nil, &state)
	require.True(t, state.Running)
	require.NotNil(t, state.Counts, "live physical counts are required for rollback evidence")
	return *state.Counts
}

func isolatedRecoveryCamera(t *testing.T, f *isolatedFixture) edgeCameraStatus {
	t.Helper()
	var state edgeCameraStatus
	qualificationCall(t, f.Camera["camera"], "status", nil, &state)
	return state
}

func isolatedRecoveryJob(t *testing.T, f *isolatedFixture) *api.CameraJob {
	t.Helper()
	var found *api.CameraJob
	require.Eventually(t, func() bool {
		rows, err := api.NewCameraJobServiceClient(f.Admin).List(f.Ctx, api.CameraJobListRequest_builder{}.Build())
		if err != nil || len(rows.GetItems()) != 1 {
			return false
		}
		found = rows.GetItems()[0]
		return true
	}, 25*time.Second, 25*time.Millisecond, "one durable enrollment job must exist")
	return found
}

func isolatedRecoveryBinding(t *testing.T, f *isolatedFixture, job *api.CameraJob) *api.CameraBinding {
	t.Helper()
	binding, err := api.NewCameraBindingServiceClient(f.Admin).Get(f.Ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: job.GetBinding().GetId()}.Build()}.Build())
	require.NoError(t, err, "durable binding must remain readable")
	return binding
}

func isolatedRecoveryMatched(t *testing.T, f *isolatedFixture, gate *edgeRPCGate) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.Ctx, 45*time.Second)
	defer cancel()
	select {
	case <-gate.Matched():
	case <-ctx.Done():
		t.Fatal("post-success response checkpoint was not reached")
	}
}

func isolatedRecoverySameRole(t *testing.T, f *isolatedFixture, role string, before edgeRuntimeStoredRole, after time.Time) {
	t.Helper()
	f.WaitRoleHeartbeat(role, after)
	current := f.RoleStatus(role)
	require.Equal(t, "adopted", current.IdentityState)
	require.NotEmpty(t, before.SPKISHA256)
	require.Equal(t, before.SPKISHA256, current.SPKISHA256, "recovery must reuse the stored machine key")
	require.NotEmpty(t, before.ID)
	require.Equal(t, before.ID, current.ID, "recovery must authenticate the same approved CP identity")
}

func isolatedRecoveryPins(t *testing.T, before, after *api.CameraJob) {
	t.Helper()
	require.True(t, bytes.Equal(before.GetId(), after.GetId()), "replay created a second job")
	require.True(t, bytes.Equal(before.GetBinding().GetId(), after.GetBinding().GetId()), "replay changed the logical binding")
	require.True(t, proto.Equal(before.GetInputs(), after.GetInputs()), "replay changed exact credential/policy/bootstrap pins")
}

func isolatedRecoveryCredentials(t *testing.T, f *isolatedFixture, initial *api.CameraCredentialReference, job *api.CameraJob) {
	t.Helper()
	inputs := job.GetInputs()
	require.Equal(t, initial.GetId(), inputs.GetInitialCredential().GetId(), "initial account inventory must survive rollback/replay")
	material := credentialPayload(t, isolatedRecoveryUsername, isolatedRecoveryPassword)
	defer clear(material)
	for _, ref := range []*api.CameraCredentialReference{initial, inputs.GetInitialCredential(), inputs.GetManagementCredential(), inputs.GetStreamingCredential()} {
		require.NotNil(t, ref)
		require.NotEmpty(t, ref.GetVersion())
		lease, err := api.NewCameraCredentialServiceClient(f.Admin).Resolve(f.Ctx, api.CameraCredentialResolveRequest_builder{Credential: ref}.Build())
		require.NoError(t, err, "exact original and prepared credential versions must remain recoverable")
		require.True(t, bytes.Equal(material, lease.GetMaterial()), "recovery changed prepared account material")
		clear(lease.GetMaterial())
	}
}

func isolatedRecoveryInventory(t *testing.T, f *isolatedFixture) {
	t.Helper()
	require.Equal(t, edgeControlEntityCounts{Device: 1, Binding: 1, Source: 1, Job: 1, Credential: 3, CredentialVersion: 4}, isolatedRecoveryCounts(t, f), "recovery must not duplicate inventory or orphan secret versions")
	state := isolatedRecoveryCamera(t, f)
	require.True(t, state.Active)
	require.Equal(t, 1, state.Activations, "a recovered first-password operation must not activate twice")
	require.Equal(t, 1, state.ConfigurationWrites, "a recovered configuration operation must not Set twice")
}

func isolatedRecoveryFreshRecord(t *testing.T, record *api.Lamina, after time.Time) {
	t.Helper()
	require.NotNil(t, record.GetDateCommitted())
	require.True(t, record.GetDateCommitted().AsTime().After(after), "recovery must produce a newly committed playable recording")
}

func isolatedRecoveryJobRollback(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	baseline := edgeControlEntityCounts{Device: 1, Credential: 1, CredentialVersion: 1}
	require.Equal(t, baseline, isolatedRecoveryCounts(t, f))
	qualificationCall(t, f.Control, "job_insert_failure", map[string]any{"enabled": true}, nil)
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	current := f.RestartRole("worker")
	priorRevision := f.Device["camera"].GetObservationRevision()
	require.Eventually(t, func() bool {
		device, err := api.NewCameraDeviceServiceClient(f.Admin).Get(f.Ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: f.Device["camera"].GetId()}.Build()}.Build())
		logs, overflow := current.logs.snapshot()
		phase := strings.Contains(logs, "phase=observe") || strings.Contains(logs, `"phase":"observe"`)
		return err == nil && device.GetObservationRevision() > priorRevision && !overflow && strings.Contains(logs, "camera worker discovery failed; retrying") && phase
	}, 25*time.Second, 25*time.Millisecond, "default discovery must finish the faulted Observe attempt")
	require.Equal(t, baseline, isolatedRecoveryCounts(t, f))
	state := isolatedRecoveryCamera(t, f)
	require.False(t, state.Active)
	require.Zero(t, state.Activations)
	require.Zero(t, state.ConfigurationWrites)
	f.CrashRole("worker")
	qualificationCall(t, f.Control, "job_insert_failure", map[string]any{"enabled": false}, nil)
	restarted := time.Now()
	f.RestartRole("worker")
	media := f.VerifyMedia("camera")
	isolatedRecoveryFreshRecord(t, media.Record, restarted)
	isolatedRecoveryCredentials(t, f, initial, media.Job)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	isolatedRecoveryInventory(t, f)
}

func isolatedRecoveryObserveLoss(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	gate, err := f.Proxy.Arm(edgeRPCFaultObserve)
	require.NoError(t, err)
	defer gate.Release()
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	f.RestartRole("worker")
	isolatedRecoveryMatched(t, f, gate)
	f.CrashRole("worker")
	committed := isolatedRecoveryJob(t, f)
	require.Equal(t, edgeControlEntityCounts{Device: 1, Binding: 1, Source: 1, Job: 1, Credential: 3, CredentialVersion: 4}, isolatedRecoveryCounts(t, f))
	isolatedRecoveryCredentials(t, f, initial, committed)
	gate.Release()
	restarted := time.Now()
	f.RestartRole("worker")
	media := f.VerifyMedia("camera")
	isolatedRecoveryFreshRecord(t, media.Record, restarted)
	isolatedRecoveryPins(t, committed, media.Job)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	isolatedRecoveryInventory(t, f)
}

func isolatedRecoveryActivationLoss(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	qualificationCall(t, f.Camera["camera"], "lose_activation_response", nil, nil)
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	f.RestartRole("worker")
	require.Eventually(t, func() bool {
		state := isolatedRecoveryCamera(t, f)
		return state.Active && state.Activations == 1
	}, 35*time.Second, 5*time.Millisecond, "native first-password mutation must execute before its lost reply is recovered")
	f.CrashRole("worker")
	committed := isolatedRecoveryJob(t, f)
	require.NotEqual(t, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE, committed.GetStage(), "activation fault checkpoint was missed")
	isolatedRecoveryCredentials(t, f, initial, committed)
	restarted := time.Now()
	f.RestartRole("worker")
	media := f.VerifyMedia("camera")
	isolatedRecoveryFreshRecord(t, media.Record, restarted)
	isolatedRecoveryPins(t, committed, media.Job)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	isolatedRecoveryInventory(t, f)
}

func isolatedRecoveryConfigurationLoss(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	before := isolatedRecoveryCamera(t, f)
	qualificationCall(t, f.Camera["camera"], "hold_configuration", nil, nil)
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	f.RestartRole("worker")
	require.Eventually(t, func() bool {
		state := isolatedRecoveryCamera(t, f)
		return state.Accepted && !state.Settled
	}, 40*time.Second, 10*time.Millisecond, "the device must own an unsettled accepted Set")
	f.CrashRole("worker")
	committed := isolatedRecoveryJob(t, f)
	state := isolatedRecoveryCamera(t, f)
	require.Equal(t, before.Profile.Width, state.Profile.Width, "the held operation has not committed before release")
	require.Zero(t, state.ConfigurationWrites)
	qualificationCall(t, f.Camera["camera"], "release_configuration", nil, &state)
	require.True(t, state.Settled)
	require.Equal(t, 1, state.ConfigurationWrites)
	require.NotEqual(t, before.Profile.Width, state.Profile.Width, "release must apply an actual new encoder profile")
	restarted := time.Now()
	f.RestartRole("worker")
	media := f.VerifyMedia("camera")
	isolatedRecoveryFreshRecord(t, media.Record, restarted)
	isolatedRecoveryPins(t, committed, media.Job)
	isolatedRecoveryCredentials(t, f, initial, media.Job)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	isolatedRecoveryInventory(t, f)
}

func isolatedRecoveryPublicationLoss(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	gate, err := f.Proxy.Arm(edgeRPCFaultPublish)
	require.NoError(t, err)
	defer gate.Release()
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	f.RestartRole("worker")
	isolatedRecoveryMatched(t, f, gate)
	f.CrashRole("worker")
	committed := isolatedRecoveryJob(t, f)
	binding := isolatedRecoveryBinding(t, f, committed)
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, committed.GetStage())
	require.Positive(t, committed.GetPublishedRevision())
	require.Equal(t, committed.GetPublishedRevision(), binding.GetDesired().GetRevision())
	require.NotEmpty(t, binding.GetSource().GetId(), "Source publication must be durable before reply loss")
	require.Empty(t, committed.GetObserved().GetProgress().GetBinding().GetSourceId(), "portable publication hint must still precede the held reply")
	gate.Release()
	restarted := time.Now()
	f.RestartRole("worker")
	media := f.VerifyMedia("camera")
	isolatedRecoveryFreshRecord(t, media.Record, restarted)
	isolatedRecoveryPins(t, committed, media.Job)
	require.True(t, bytes.Equal(binding.GetSource().GetId(), media.Binding.GetSource().GetId()), "publication replay changed the durable Source")
	require.Equal(t, committed.GetPublishedRevision(), media.Job.GetPublishedRevision())
	isolatedRecoveryCredentials(t, f, initial, media.Job)
	isolatedRecoverySameRole(t, f, "worker", worker, restarted)
	isolatedRecoverySameRole(t, f, "producer", producer, restarted)
	isolatedRecoveryInventory(t, f)
}

func isolatedRecoveryCaptureCrash(t *testing.T) {
	f, initial, worker, producer := isolatedRecoveryPrepare(t)
	defer f.FinalizeAndScan([]string{isolatedRecoveryPassword})
	f.ActivatePolicy(isolatedRecoveryUsername, true)
	f.RestartRole("worker")
	job := isolatedRecoveryJob(t, f)
	var first *api.Lamina
	require.Eventually(t, func() bool {
		binding := isolatedRecoveryBinding(t, f, job)
		rows, err := api.NewLaminaServiceClient(f.Admin).List(f.Ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: binding.GetSource().GetId()}.Build()}.Build()}, Size: 500}.Build())
		if err != nil || isolatedRecoveryCamera(t, f).Sessions == 0 {
			return false
		}
		// Set.Allocate can prefetch many future PENDING slots. Select the
		// earliest current slot and reject any already completed first upload.
		var record *api.Lamina
		for _, candidate := range rows.GetItems() {
			if candidate.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED || candidate.GetDateCommitted() != nil {
				return false
			}
			if candidate.GetCaptureEpoch() == "" || candidate.GetState() != api.LaminaState_LAMINA_STATE_PENDING || candidate.GetDateStarted() == nil || candidate.GetDateStarted().AsTime().After(time.Now()) {
				continue
			}
			if record == nil || candidate.GetDateStarted().AsTime().Before(record.GetDateStarted().AsTime()) {
				record = candidate
			}
		}
		if record == nil {
			return false
		}
		current, err := api.NewProducerServiceClient(f.Admin).Get(f.Ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Producer.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		for _, source := range current.GetStatus().GetSources() {
			if bytes.Equal(source.GetSourceId(), binding.GetSource().GetId()) && source.GetInputUp() && source.GetCaptureEpoch() == record.GetCaptureEpoch() {
				first = record
				return true
			}
		}
		return false
	}, 65*time.Second, 10*time.Millisecond, "first live upload must still be uncommitted with an actual current input session")
	f.CrashRole("producer")
	before := isolatedRecoveryJob(t, f)
	binding := isolatedRecoveryBinding(t, f, before)
	require.NotEqual(t, api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE, before.GetStage(), "first-upload crash checkpoint was missed")
	require.Eventually(t, func() bool { return isolatedRecoveryCamera(t, f).Sessions == 0 }, 10*time.Second, 25*time.Millisecond, "SIGKILL must also end the owned capture child/session")
	crashedAt := time.Now()
	isolatedRecoverySameRole(t, f, "worker", worker, crashedAt)
	f.RestartRole("producer")
	media := f.VerifyMedia("camera", first)
	isolatedRecoveryFreshRecord(t, media.Record, crashedAt)
	require.NotEqual(t, first.GetCaptureEpoch(), media.Record.GetCaptureEpoch(), "recovery requires a fresh native capture epoch")
	require.False(t, bytes.Equal(first.GetId(), media.Record.GetId()), "a new complete recording must follow the interrupted upload")
	require.True(t, bytes.Equal(binding.GetSource().GetId(), media.Binding.GetSource().GetId()), "capture recovery must retain the logical Source and history")
	isolatedRecoveryPins(t, before, media.Job)
	isolatedRecoveryCredentials(t, f, initial, media.Job)
	isolatedRecoverySameRole(t, f, "producer", producer, crashedAt)
	history, err := api.NewLaminaServiceClient(f.Admin).Get(f.Ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: first.GetId()}.Build()}.Build())
	require.NoError(t, err, "interrupted history must not disappear when capture restarts")
	require.Equal(t, first.GetCaptureEpoch(), history.GetCaptureEpoch())
	require.True(t, history.GetState() != api.LaminaState_LAMINA_STATE_COMMITTED || history.GetIncomplete(), "interrupted first upload must not become a complete old-epoch success")
	isolatedRecoveryInventory(t, f)
}
