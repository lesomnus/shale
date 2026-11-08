//go:build linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestEdgeControlFaultInputBoundary(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		raw, err := json.Marshal(map[string]any{"version": 1, "id": 1, "op": "job_insert_failure", "enabled": enabled})
		require.NoError(t, err)
		r, ok := edgeControlRequestFrom(raw)
		require.True(t, ok)
		require.NotNil(t, r.Enabled)
		require.Equal(t, enabled, *r.Enabled)
	}
	for _, raw := range []string{
		`{"version":1,"id":1,"op":"job_insert_failure"}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":null}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":"true"}`,
		`{"version":1,"id":1,"op":"status","enabled":true}`,
		`{"version":1,"id":1,"op":"stop_cp","enabled":false}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":true,"sql":"private-control-sentinel"}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":true,"table":"private-control-sentinel"}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":true,"path":"private-control-sentinel"}`,
		`{"version":1,"id":1,"op":"job_insert_failure","enabled":true,"material":"private-control-sentinel"}`,
	} {
		_, ok := edgeControlRequestFrom([]byte(raw))
		require.False(t, ok, "only the fixed boolean fault control is accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	fixture := &edgeControlCluster{cluster: &cluster{}}
	require.Error(t, fixture.jobInsertFailure(ctx, true))
	state, err := fixture.status(t.Context())
	require.NoError(t, err)
	require.False(t, state.Running)
	require.Nil(t, state.Counts)
	require.Nil(t, state.JobInsertFailure, "a stopped CP must not pretend its absent counts/fault are live zeros")
}

type edgeControlFaultClient struct {
	writer       *io.PipeWriter
	reader       *io.PipeReader
	responses    chan edgeControlResponse
	responseDone chan struct{}
	done         chan error
	cancel       context.CancelFunc
	id           int64
	closeOnce    sync.Once
}

func newEdgeControlFaultClient(t *testing.T) *edgeControlFaultClient {
	t.Helper()
	t.Setenv("SHALE_E2E_DB_DSN", "")
	input, writer := io.Pipe()
	reader, output := io.Pipe()
	ctx, cancel := context.WithCancel(t.Context())
	c := &edgeControlFaultClient{writer: writer, reader: reader, responses: make(chan edgeControlResponse, 8), responseDone: make(chan struct{}), done: make(chan error, 1), cancel: cancel}
	go func() { err := runEdgeControlProcess(t, ctx, input, output); output.Close(); c.done <- err }()
	go func() {
		defer close(c.responseDone)
		defer close(c.responses)
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			var r edgeControlResponse
			if json.Unmarshal(scanner.Bytes(), &r) != nil {
				return
			}
			select {
			case c.responses <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() { c.close(t) })
	return c
}

func (c *edgeControlFaultClient) close(t *testing.T) {
	c.closeOnce.Do(func() {
		c.cancel()
		c.writer.Close()
		c.reader.Close()
		select {
		case err := <-c.done:
			if err != nil {
				t.Error("private control fixture did not stop cleanly")
			}
		case <-time.After(25 * time.Second):
			t.Error("private control fixture did not join")
		}
		select {
		case <-c.responseDone:
		case <-time.After(time.Second):
			t.Error("private control response reader did not join")
		}
	})
}

func (c *edgeControlFaultClient) call(t *testing.T, op string, fields map[string]any) *edgeControlStatus {
	t.Helper()
	c.id++
	r := map[string]any{"version": 1, "id": c.id, "op": op}
	for key, value := range fields {
		r[key] = value
	}
	require.NoError(t, json.NewEncoder(c.writer).Encode(r))
	select {
	case response, ok := <-c.responses:
		require.True(t, ok, "private control response missing")
		require.Equal(t, c.id, response.ID)
		require.Empty(t, response.Code, "fixed private control command failed")
		require.NotEqual(t, "error", response.Event)
		if op == "start" {
			// start(t) registered its cluster cleanup during the asynchronous
			// command. Register our join afterward so a failing assertion first
			// stops the protocol owner, avoiding concurrent bare-cluster cleanup.
			t.Cleanup(func() { c.close(t) })
		}
		return response.Status
	case <-time.After(45 * time.Second):
		t.Fatal("bounded private control command did not return")
		return nil
	}
}

func edgeControlFaultWorker(t *testing.T, ctx context.Context, cp string, admin *grpc.ClientConn, state string) (*api.CameraWorker, *hostagent.Agent, *grpc.ClientConn) {
	t.Helper()
	a := &hostagent.Agent{Kind: 31, HardwareId: "fixed-job-fault-worker", Store: pki.Store{Dir: state}, Cp: cp, Dev: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.NoError(t, a.Init())
	join := func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
		v, err := api.NewCameraWorkerServiceClient(conn).Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: "acme", Host: host}.Build())
		if err != nil {
			return hostagent.Answer{}, err
		}
		return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
	}
	bootstrap, err := a.DialBootstrap(ctx)
	require.NoError(t, err)
	defer bootstrap.Close()
	host, err := a.HostJoin()
	require.NoError(t, err)
	pending, err := join(ctx, bootstrap, host)
	require.NoError(t, err)
	w, err := api.NewCameraWorkerServiceClient(admin).Adopt(ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: pending.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = a.JoinBootstrap(ctx, join)
	require.NoError(t, err)
	conn, err := a.Dial(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return w, a, conn
}

func edgeControlFaultAdmin(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	conn, err := grpc.NewClient(address, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// Actual private sidecar JSON commands and generated CP RPCs exercise the
// central transaction. No camera, capture process, encoder or decoder exists.
// The Worker reconstruction below recovers its real approved machine agent/key;
// cross-process physical camera crash qualification remains the assembled suite.
func TestEdgeControlFaultRollsBackEnrollmentAndRecovers(t *testing.T) {
	c := newEdgeControlFaultClient(t)
	state := t.TempDir()
	require.NoError(t, os.Chmod(state, 0700))
	ready := c.call(t, "start", map[string]any{"state_dir": state, "bind_ip": "127.0.0.1"})
	require.NotNil(t, ready.Counts)
	require.Equal(t, edgeControlEntityCounts{}, *ready.Counts)
	cp := "http://" + ready.Endpoints.TenantAPI
	admin := edgeControlFaultAdmin(t, ready.Endpoints.TenantAPI)
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()
	set := sourceCatalogSet(t, admin, "fault-control-set")
	// This fixture has no recording runtime; the adopted Producer is only the
	// approved assignment identity needed before any Source can be published.
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR("fault-control-producer")
	require.NoError(t, err)
	join, err := api.NewProducerServiceClient(plainConn(t, ready.Endpoints.TenantAPI)).Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: api.HostJoin_builder{HardwareId: "fault-control-producer", Hostname: "fixture", Csr: csr, KeyFingerprint: key.Fingerprint()}.Build()}.Build())
	require.NoError(t, err)
	producer, err := api.NewProducerServiceClient(admin).Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: join.GetAnswer().GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	workerState := t.TempDir()
	worker, agent, workerConn := edgeControlFaultWorker(t, ctx, cp, admin, workerState)
	network := cameraPolicyNetwork(t, ctx, admin, "fault-control-lan", nil)
	a, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "fault-control-worker", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	observe := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), AssignmentGeneration: a.GetGeneration(), DiscoveryId: "urn:uuid:00000000-0000-4000-8000-000000000202", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10/onvif/device_service"}.Build()}}.Build()
	first, err := api.NewCameraPolicyServiceClient(workerConn).Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "policy_missing", first.GetCode())
	material := credentialPayload(t, "installed-admin", "private-control-material-sentinel")
	defer clear(material)
	imported, err := api.NewCameraCredentialServiceClient(admin).Import(ctx, api.CameraCredentialImportRequest_builder{Device: api.CameraDeviceRef_builder{Id: first.GetDevice().GetId()}.Build(), Purpose: "initial_admin", IntentKey: "installed-before-enrollment", Access: "administrator", Material: material}.Build())
	require.NoError(t, err)
	baseline := edgeControlEntityCounts{Device: 1, Credential: 1, CredentialVersion: 1}
	for i := 0; i < 2; i++ {
		s := c.call(t, "job_insert_failure", map[string]any{"enabled": true})
		require.True(t, *s.JobInsertFailure)
		require.Equal(t, baseline, *s.Counts)
	}
	preset, err := api.NewCameraPresetServiceClient(admin).Create(ctx, cameraPolicyPresetRequest("fault-control-preset", "1", cameraPolicyRule("default", api.CameraPresetScope_builder{TenantId: cameraPolicyID(t, network.GetTenant().GetId())}.Build())))
	require.NoError(t, err)
	policyReq := cameraPolicyCreateRequest(network, preset, "fault-control-policy", "1")
	policyReq.GetEnrollment().SetAllowActivation(true)
	policy, err := api.NewCameraPolicyServiceClient(admin).Create(ctx, policyReq)
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(admin).Activate(ctx, api.CameraPolicyActivateRequest_builder{Ref: api.CameraPolicyRef_builder{Id: policy.GetId()}.Build(), ExpectedNetworkVersion: network.GetDateUpdated()}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraPolicyServiceClient(workerConn).Observe(ctx, observe)
	require.Error(t, err, "fixed job insert must fail after preparation within the same transaction")
	snapshot := c.call(t, "status", nil)
	require.Equal(t, baseline, *snapshot.Counts, "no orphan Source/binding/job/secret version may escape rollback")
	// The trigger belongs to this DB, so CP outage/reopen neither loses the
	// fault nor accidentally reports stopped services as an empty database.
	stopped := c.call(t, "stop_cp", nil)
	require.False(t, stopped.Running)
	require.Nil(t, stopped.Counts)
	require.Nil(t, stopped.JobInsertFailure)
	restarted := c.call(t, "restart_cp", nil)
	require.True(t, *restarted.JobInsertFailure)
	require.Equal(t, baseline, *restarted.Counts)
	admin.Close()
	admin = edgeControlFaultAdmin(t, ready.Endpoints.TenantAPI)
	workerConn.Close()
	recovered := &hostagent.Agent{Kind: 31, HardwareId: "fixed-job-fault-worker", Store: pki.Store{Dir: workerState}, Cp: cp, Dev: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	require.NoError(t, recovered.Init())
	require.Equal(t, agent.Id(), recovered.Id())
	recoveredConn, err := recovered.Dial(ctx)
	require.NoError(t, err)
	defer recoveredConn.Close()
	_, err = api.NewCameraWorkerServiceClient(recoveredConn).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		s := c.call(t, "job_insert_failure", map[string]any{"enabled": false})
		require.False(t, *s.JobInsertFailure)
		require.Equal(t, baseline, *s.Counts)
	}
	result, err := api.NewCameraPolicyServiceClient(recoveredConn).Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, "enrolled", result.GetCode())
	require.Equal(t, first.GetDevice().GetId(), result.GetDevice().GetId())
	after := c.call(t, "status", nil)
	require.Equal(t, edgeControlEntityCounts{Device: 1, Binding: 1, Source: 1, Job: 1, Credential: 3, CredentialVersion: 4}, *after.Counts)
	inputs := result.GetJob().GetInputs()
	require.Equal(t, imported.GetCredential().GetId(), inputs.GetInitialCredential().GetId())
	for _, ref := range []*api.CameraCredentialReference{imported.GetCredential(), inputs.GetInitialCredential(), inputs.GetManagementCredential(), inputs.GetStreamingCredential()} {
		lease, err := api.NewCameraCredentialServiceClient(admin).Resolve(ctx, api.CameraCredentialResolveRequest_builder{Credential: ref}.Build())
		require.NoError(t, err)
		require.True(t, bytes.Equal(material, lease.GetMaterial()), "recovery keeps both original and newly pinned account material unchanged")
		clear(lease.GetMaterial())
	}
	again, err := api.NewCameraPolicyServiceClient(recoveredConn).Observe(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, result.GetJob().GetId(), again.GetJob().GetId())
	require.True(t, proto.Equal(inputs, again.GetJob().GetInputs()))
	final := c.call(t, "status", nil)
	require.Equal(t, *after.Counts, *final.Counts)
	encoded, err := json.Marshal(final.Counts)
	require.NoError(t, err)
	var counts map[string]int64
	require.NoError(t, json.Unmarshal(encoded, &counts))
	require.Len(t, counts, 6)
	for _, n := range counts {
		require.GreaterOrEqual(t, n, int64(0))
	}
	c.call(t, "stop", nil)
}
