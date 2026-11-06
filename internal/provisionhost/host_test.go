package provisionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
)

const testToken = "this-is-a-private-fixture-token-at-least-32-bytes"

type executorFunc func(context.Context, provision.Request, bool) (provision.Run, error)

func (f executorFunc) Execute(ctx context.Context, q provision.Request, apply bool) (provision.Run, error) {
	return f(ctx, q, apply)
}

func testScope() camera.EnrollmentScope {
	return camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
}
func testRequest() provision.Request {
	return provision.Request{ID: "job-one", BindingID: "role-one", Scope: testScope(), Device: camera.DeviceHint{ID: "camera-one", Endpoints: []camera.Endpoint{{URL: "http://127.0.0.1/onvif", NetworkScope: "lan"}}}, Desired: camera.DesiredState{Revision: "1"}, Provider: "private"}
}
func testOptions(t *testing.T, executor Executor) Options {
	t.Helper()
	return Options{StateDir: filepath.Join(t.TempDir(), "state"), Targets: []Target{{Scope: testScope(), Provider: "private", Executor: executor}}, Networks: []Network{{ID: "lan", CIDRs: []string{"127.0.0.0/8"}}}, Clients: []Client{{Token: testToken, Scopes: []camera.EnrollmentScope{testScope()}, Networks: []string{"lan"}, Worker: true}}}
}
func route(t *testing.T, h *Host, method, path string, input any, token string) *httptest.ResponseRecorder {
	t.Helper()
	handler, err := h.Handler()
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	if input != nil {
		data, err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func waitState(t *testing.T, h *Host, id, state string) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		job, err := h.load(id)
		h.mu.Unlock()
		if err == nil && job.State == state {
			return job
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.mu.Lock()
	job, _ := h.load(id)
	h.mu.Unlock()
	t.Fatalf("job did not reach %s: %+v", state, job)
	return Job{}
}

func TestDurableQueueRecoveryAndReplay(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, q provision.Request, apply bool) (provision.Run, error) {
		run := provision.Run{Request: q, Stage: provision.Planned}
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return run, ctx.Err()
		}
		return run, nil
	})
	options := testOptions(t, executor)
	host, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	q := testRequest()
	job, err := host.submit(q, false)
	if err != nil || job.State != "queued" {
		t.Fatalf("submit %v %v", job, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	<-started
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	waitState(t, host, q.ID, "queued")
	// A new process uses the persisted request and resumes the same core key.
	host, err = New(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done = make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	job = waitState(t, host, q.ID, "done")
	if calls.Load() != 2 || job.Run.Request.ID != q.ID {
		t.Fatalf("not resumed once: %d %+v", calls.Load(), job)
	}
	duplicate, err := host.submit(q, false)
	if err != nil || duplicate.State != "done" || calls.Load() != 2 {
		t.Fatalf("replay duplicated job: %v", err)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAPIRejectsCrossScopeNetworkAndUnauthenticated(t *testing.T) {
	var calls atomic.Int32
	host, err := New(testOptions(t, executorFunc(func(_ context.Context, q provision.Request, _ bool) (provision.Run, error) {
		calls.Add(1)
		return provision.Run{Request: q}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	q := testRequest()
	input := map[string]any{"request": q, "apply": true}
	if w := route(t, host, "POST", "/v1/runs", input, ""); w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	q.Scope.NetworkID = "foreign"
	if w := route(t, host, "POST", "/v1/runs", map[string]any{"request": q}, testToken); w.Code != http.StatusForbidden {
		t.Fatalf("cross scope accepted: %d %s", w.Code, w.Body)
	}
	q = testRequest()
	q.Device.Endpoints[0].URL = "http://192.0.2.1/onvif"
	if w := route(t, host, "POST", "/v1/runs", map[string]any{"request": q}, testToken); w.Code != http.StatusForbidden {
		t.Fatalf("foreign endpoint accepted: %d", w.Code)
	}
	q = testRequest()
	q.Provider = "foreign"
	if w := route(t, host, "POST", "/v1/runs", map[string]any{"request": q}, testToken); w.Code != http.StatusForbidden {
		t.Fatalf("foreign provider accepted: %d", w.Code)
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request reached engine")
	}
}

func TestWaitingResumeCancelAndSanitizedFailure(t *testing.T) {
	var calls atomic.Int32
	host, err := New(testOptions(t, executorFunc(func(_ context.Context, q provision.Request, _ bool) (provision.Run, error) {
		if calls.Add(1) == 1 {
			return provision.Run{Request: q, Stage: provision.WaitingAuth}, camera.ErrAuthRequired
		}
		return provision.Run{Request: q, Stage: provision.Planned}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	q := testRequest()
	if w := route(t, host, "POST", "/v1/runs", map[string]any{"request": q}, testToken); w.Code != http.StatusAccepted {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	job := waitState(t, host, q.ID, "waiting")
	if job.Code != "credentials_required" {
		t.Fatal(job.Code)
	}
	if w := route(t, host, "POST", "/v1/runs/"+q.ID+"/resume", map[string]any{}, testToken); w.Code != http.StatusAccepted {
		t.Fatal(w.Code)
	}
	waitState(t, host, q.ID, "done")
	if w := route(t, host, "POST", "/v1/runs/"+q.ID+"/cancel", map[string]any{}, testToken); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	waitState(t, host, q.ID, "canceled")
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// Arbitrary transport/vendor error strings never cross the API.
	host.options.Targets[0].Executor = executorFunc(func(context.Context, provision.Request, bool) (provision.Run, error) {
		return provision.Run{}, errors.New("password=secret-value")
	})
	w := route(t, host, "POST", "/v1/execute", map[string]any{"request": q}, testToken)
	if strings.Contains(w.Body.String(), "secret-value") || !strings.Contains(w.Body.String(), "operation_failed") {
		t.Fatal(w.Body)
	}
}

func TestAuthenticatedRemoteWorkerAndUnauthorizedRole(t *testing.T) {
	var calls atomic.Int32
	opts := testOptions(t, executorFunc(func(_ context.Context, q provision.Request, apply bool) (provision.Run, error) {
		calls.Add(1)
		return provision.Run{Request: q, Stage: provision.Active}, nil
	}))
	host, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := host.Handler()
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	remote := &RemoteExecutor{URL: server.URL, Token: testToken, HTTPClient: server.Client()}
	run, err := remote.Execute(context.Background(), testRequest(), true)
	if err != nil || run.Stage != provision.Active || calls.Load() != 1 {
		t.Fatalf("worker %v %+v", err, run)
	}
	remote.Token = "wrong-token-that-is-still-long-enough-to-test"
	if _, err = remote.Execute(context.Background(), testRequest(), true); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("wrong token accepted %v", err)
	}
	host.options.Clients[0].Worker = false
	remote.Token = testToken
	if _, err = remote.Execute(context.Background(), testRequest(), true); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("operator accepted as worker %v", err)
	}
}

func TestNetworkRejectsDNSAndUnexpectedStreamBeforeCredentials(t *testing.T) {
	network := Network{ID: "lan", CIDRs: []string{"192.0.2.0/24"}}
	for _, uri := range []string{"rtsp://camera.example/stream", "rtsp://198.51.100.1/stream", "rtsp://user:secret@192.0.2.1/stream", "http://192.0.2.1/?password=secret"} {
		if network.AllowURI(uri) == nil {
			t.Fatalf("accepted %s", camera.SanitizeURI(uri))
		}
	}
	if err := network.AllowURI("rtsp://192.0.2.2/stream"); err != nil {
		t.Fatal(err)
	}
	client, err := network.HTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Get("http://127.0.0.1:1234/"); err == nil {
		t.Fatal("foreign HTTP dial permitted")
	}
}

func TestPrivateTokenAndRemoteTransport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(testToken), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(path); err == nil {
		t.Fatal("public token file accepted")
	}
	os.Chmod(path, 0600)
	if token, err := ReadToken(path); err != nil || token != testToken {
		t.Fatal(err)
	}
	if _, err := validateConnection(ConnectionConfig{URL: "http://192.0.2.1:7415", AllowLoopbackHTTP: true}); err == nil {
		t.Fatal("remote plaintext allowed")
	}
	if _, err := validateConnection(ConnectionConfig{URL: "http://127.0.0.1:7415", AllowLoopbackHTTP: true}); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteRedirectDoesNotForwardBearer(t *testing.T) {
	var received atomic.Int32
	trap := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); io.WriteString(w, "{}") }))
	defer trap.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, trap.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client := server.Client()
	// A deliberately permissive injected transport makes following a redirect
	// possible, so the executor itself must stop it before any second request.
	remote := &RemoteExecutor{URL: server.URL, Token: testToken, HTTPClient: client}
	if _, err := remote.Execute(context.Background(), testRequest(), true); err == nil {
		t.Fatal("redirect accepted")
	}
	if received.Load() != 0 {
		t.Fatal("worker bearer forwarded")
	}
}

func TestControllerReservesPhysicalAssignmentAcrossTargets(t *testing.T) {
	executor := executorFunc(func(context.Context, provision.Request, bool) (provision.Run, error) { return provision.Run{}, nil })
	options := testOptions(t, executor)
	otherScope := testScope()
	otherScope.ProducerID = "other-producer"
	options.Targets = append(options.Targets, Target{Scope: otherScope, Provider: "private", Executor: executor})
	options.Clients[0].Scopes = append(options.Clients[0].Scopes, otherScope)
	host, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	q := testRequest()
	if _, err = host.submit(q, false); err != nil {
		t.Fatal(err)
	}
	q.ID = "other-run"
	q.Scope = otherScope
	if _, err = host.submit(q, false); !errors.Is(err, provision.ErrConflict) {
		t.Fatalf("same device reserved on two producers: %v", err)
	}
}

func TestCancelResumeWaitsForOldWriterToFinish(t *testing.T) {
	started := make(chan struct{})
	settled := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, q provision.Request, _ bool) (provision.Run, error) {
		close(started)
		<-ctx.Done()
		<-settled
		return provision.Run{Request: q}, ctx.Err()
	})
	host, err := New(testOptions(t, executor))
	if err != nil {
		t.Fatal(err)
	}
	q := testRequest()
	if _, err = host.submit(q, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	<-started
	if w := route(t, host, "POST", "/v1/runs/"+q.ID+"/cancel", map[string]any{}, testToken); w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}
	if w := route(t, host, "POST", "/v1/runs/"+q.ID+"/resume", map[string]any{}, testToken); w.Code != http.StatusConflict {
		t.Fatalf("resumed before uncertain writer settled: %d", w.Code)
	}
	close(settled)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		host.mu.Lock()
		running := host.running[q.ID] != nil
		host.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(time.Millisecond)
	}
	waitState(t, host, q.ID, "canceled")
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

type discovererFunc func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error)

func (f discovererFunc) Discover(ctx context.Context, s camera.DiscoveryScope) ([]camera.Observation, error) {
	return f(ctx, s)
}

func TestControllerDispatchesDiscoveryToLANWorker(t *testing.T) {
	var workerCalls atomic.Int32
	workerOptions := testOptions(t, executorFunc(func(context.Context, provision.Request, bool) (provision.Run, error) { return provision.Run{}, nil }))
	workerOptions.Discoverer = discovererFunc(func(_ context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
		workerCalls.Add(1)
		return []camera.Observation{{Scope: scope.ID, Device: camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: "http://127.0.0.2/onvif", NetworkScope: scope.ID}}}}}, nil
	})
	worker, err := New(workerOptions)
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := worker.Handler()
	server := httptest.NewServer(handler)
	defer server.Close()
	controllerOptions := testOptions(t, executorFunc(func(context.Context, provision.Request, bool) (provision.Run, error) { return provision.Run{}, nil }))
	tokenPath := filepath.Join(t.TempDir(), "worker-token")
	os.WriteFile(tokenPath, []byte(testToken), 0600)
	controllerOptions.Networks[0].Worker = ConnectionConfig{URL: server.URL, TokenFile: tokenPath, AllowLoopbackHTTP: true}
	controllerOptions.Discoverer = discovererFunc(func(context.Context, camera.DiscoveryScope) ([]camera.Observation, error) {
		t.Fatal("controller attempted remote multicast")
		return nil, nil
	})
	controller, err := New(controllerOptions)
	if err != nil {
		t.Fatal(err)
	}
	observations, err := controller.discover(context.Background(), camera.DiscoveryScope{ID: "lan"})
	if err != nil || len(observations) != 1 || workerCalls.Load() != 1 {
		t.Fatalf("worker discovery: %v %+v", err, observations)
	}
}
