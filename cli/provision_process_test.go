package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/provisionhost"
	"github.com/lesomnus/shale/provision"
	"go.yaml.in/yaml/v3"
)

// The child runs the real camera CLI/factory/listener; the parent supplies only
// a protocol-backed camera and scoped secret store. No injected host executor is
// used. Full capture/live/committed-media conformance lives in the Producer suite.
func TestCameraServiceSubprocess(t *testing.T) {
	if path := os.Getenv("SHALE_TEST_CAMERA_HOST_CONFIG"); path != "" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		command := NewCmdCamera()
		command.Writer = io.Discard
		if err := command.Run(ctx, []string{"serve", "--host-config", path}); err != nil {
			t.Fatal(err)
		}
		return
	}
	isolate(t)
	dir := t.TempDir()
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	device := cameratest.ServeONVIF(t, camera.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 640, Height: 480, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: "rtsp://127.0.0.1:8554/stream"})
	device.RequireCredentials("test-admin", "subprocess-private-password")
	provider := credential.FileProvider{Dir: filepath.Join(dir, "secrets"), Name: "cameras"}
	management, err := provider.Prepare(context.Background(), scope, camera.AuthManagement, "management", camera.CredentialMaterial{Username: "test-admin", Password: "subprocess-private-password"})
	if err != nil {
		t.Fatal(err)
	}
	request := provision.Request{ID: "process-plan", BindingID: "front-door", Scope: scope, Device: device.Hint("claimed-device"), Desired: camera.DesiredState{ProfileToken: "main", Codec: "h264", Width: 640, Height: 480, FPS: 10, Revision: "1"}, Provider: "cameras", ManagementRef: &management}
	tokenPath := filepath.Join(dir, "token")
	if err = os.WriteFile(tokenPath, []byte("process-test-token-with-at-least-32-private-bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	config := provisionhost.Config{StateDir: filepath.Join(dir, "state"), Listen: address, AllowLoopbackHTTP: true, Networks: []provisionhost.Network{{ID: "lan", CIDRs: []string{"127.0.0.0/8"}}}, Providers: []provisionhost.ProviderConfig{{Name: "cameras", Directory: provider.Dir}}, Targets: []provisionhost.TargetConfig{{Scope: scope, Provider: "cameras", ConfigPath: filepath.Join(dir, "producer.yaml"), Restart: []string{"unused-during-read-only-plan"}, CP: provisionhost.ConnectionConfig{URL: "https://cp.invalid:7400", TokenFile: tokenPath}}}, Clients: []provisionhost.ClientConfig{{TokenFile: tokenPath, Scopes: []camera.EnrollmentScope{scope}, Networks: []string{"lan"}}}}
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	hostPath := filepath.Join(dir, "host.yaml")
	if err = os.WriteFile(hostPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(dir, "request.json")
	data, _ = json.Marshal(request)
	if err = os.WriteFile(requestPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	// The embedded CLI calls the same concrete engine without a service.
	out, err := run(t, &cmd.Config{}, "", "camera", "plan", "--host-config", hostPath, "--request", requestPath)
	if err != nil {
		t.Fatal(err)
	}
	var local provision.Run
	if json.Unmarshal([]byte(out), &local) != nil || local.Stage != provision.Planned {
		t.Fatalf("local plan: %s", out)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	start := func() func() {
		child := exec.Command(executable, "-test.run=^TestCameraServiceSubprocess$", "-test.count=1")
		child.Env = append(os.Environ(), "SHALE_TEST_CAMERA_HOST_CONFIG="+hostPath)
		child.Stdout, child.Stderr = io.Discard, io.Discard
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- child.Wait() }()
		deadline := time.Now().Add(10 * time.Second)
		ready := false
		for time.Now().Before(deadline) {
			client := http.Client{Timeout: 200 * time.Millisecond}
			response, err := client.Get("http://" + address + "/health")
			if err == nil {
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					ready = true
					break
				}
			}
			select {
			case err := <-done:
				t.Fatalf("camera child exited: %v", err)
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !ready {
			child.Process.Kill()
			<-done
			t.Fatal("camera child did not become healthy")
		}
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			child.Process.Signal(os.Interrupt)
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("camera child: %v", err)
				}
			case <-time.After(5 * time.Second):
				child.Process.Kill()
				<-done
				t.Error("camera child did not shut down")
			}
		}
		t.Cleanup(stop)
		return stop
	}
	stop := start()
	connection := provisionhost.ConnectionConfig{URL: "http://" + address, TokenFile: tokenPath, AllowLoopbackHTTP: true}
	var job provisionhost.Job
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = provisionhost.Call(ctx, connection, http.MethodPost, "/v1/runs", map[string]any{"request": request, "apply": false}, &job); err != nil {
		t.Fatal(err)
	}
	for job.State == "queued" || job.State == "running" {
		time.Sleep(20 * time.Millisecond)
		if err = provisionhost.Call(ctx, connection, http.MethodGet, "/v1/runs/"+request.ID, nil, &job); err != nil {
			t.Fatal(err)
		}
	}
	if job.State != "done" || job.Run == nil || job.Run.Stage != provision.Planned || job.Run.Inspection == nil {
		t.Fatalf("service plan: %+v", job)
	}
	if job.Run.Plan.ProfileToken != local.Plan.ProfileToken {
		t.Fatal("CLI/service selected different profile")
	}
	stop()
	stop = start()
	if err = provisionhost.Call(ctx, connection, http.MethodGet, "/v1/runs/"+request.ID, nil, &job); err != nil {
		t.Fatal(err)
	}
	if job.State != "done" || job.Run.Request.ID != request.ID {
		t.Fatal("service restart lost durable job")
	}
	stop()
	configuration, activation := device.Writes()
	if configuration != 0 || activation != 0 {
		t.Fatalf("read-only paths changed camera: %d %d", configuration, activation)
	}
	for _, root := range []string{config.StateDir} {
		if err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(b, []byte("subprocess-private-password")) {
				return fmt.Errorf("credential leaked to service state")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
