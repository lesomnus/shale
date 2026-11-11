package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/provisionhost"
	"github.com/lesomnus/shale/provision"
	"go.yaml.in/yaml/v3"
)

func TestCameraCommandHelp(t *testing.T) {
	isolate(t)
	for _, args := range [][]string{{"camera", "--help"}, {"camera", "plan", "--help"}, {"camera", "worker", "--help"}, {"camera", "credential", "--help"}} {
		out, err := run(t, &cmd.Config{}, "", args...)
		if err != nil || !strings.Contains(out, "camera") {
			t.Fatalf("%v: %s %v", args, out, err)
		}
	}
}

func TestCameraSettingsSummaryIncludesExplicitSmartCodec(t *testing.T) {
	settings := camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: "off"}}
	if got := cameraSettingsSummary(settings); got != `smart_codec="off"` {
		t.Fatalf("smart-only initialization summary: %s", got)
	}
	settings.Initialization.PowerLineFrequency = 60
	got := cameraSettingsSummary(settings)
	if !strings.Contains(got, `smart_codec="off"`) || !strings.Contains(got, "frequency=60Hz") {
		t.Fatalf("combined initialization summary: %s", got)
	}
}

func TestCameraCredentialPreparePrintsReferenceOnly(t *testing.T) {
	isolate(t)
	dir := t.TempDir()
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	request := provision.Request{ID: "prepare-job", BindingID: "front-door", Scope: scope, Device: camera.DeviceHint{ID: "physical-one", Endpoints: []camera.Endpoint{{URL: "http://192.0.2.1/onvif"}}}, Desired: camera.DesiredState{Revision: "1"}, Provider: "private"}
	config := provisionhost.Config{StateDir: filepath.Join(dir, "state"), Providers: []provisionhost.ProviderConfig{{Name: "private", Directory: filepath.Join(dir, "secrets")}}, Targets: []provisionhost.TargetConfig{{Scope: scope, Provider: "private"}}}
	hostPath := filepath.Join(dir, "host.yaml")
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(hostPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(dir, "request.json")
	data, _ = json.Marshal(request)
	if err = os.WriteFile(requestPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	materialPath := filepath.Join(dir, "material.json")
	if err = os.WriteFile(materialPath, []byte(`{"username":"administrator-fixture","password":"secret-fixture-not-in-output"}`), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, &cmd.Config{}, "", "camera", "credential", "--host-config", hostPath, "--request", requestPath, "--purpose", "initial_admin", "--material-file", materialPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "secret-fixture") || strings.Contains(out, "administrator-fixture") {
		t.Fatalf("secret output: %s", out)
	}
	var ref camera.CredentialRef
	if json.Unmarshal([]byte(out), &ref) != nil || !ref.Recoverable || ref.Purpose != camera.AuthInitialAdmin {
		t.Fatalf("not a credential reference: %s", out)
	}
	provider := credential.FileProvider{Name: "private", Dir: filepath.Join(dir, "secrets")}
	lease, err := camera.Acquire(context.Background(), provider, camera.AuthRequest{Scope: scope, Purpose: camera.AuthInitialAdmin, ExistingRef: &ref, Requirement: camera.AuthRequirement{Recoverable: true, Access: camera.AccessAdministrator}})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if err = lease.Use(context.Background(), func(m camera.CredentialMaterial) error {
		if m.Password != "secret-fixture-not-in-output" {
			t.Fatal("wrong prepared credential")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Retry with the same activation intent must preserve the installed value.
	out2, err := run(t, &cmd.Config{}, "", "camera", "credential", "--host-config", hostPath, "--request", requestPath, "--purpose", "initial_admin", "--material-file", materialPath)
	if err != nil || out2 != out {
		t.Fatalf("retry changed ref: %v", err)
	}
	if err = os.Chmod(materialPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = run(t, &cmd.Config{}, "", "camera", "credential", "--host-config", hostPath, "--request", requestPath, "--purpose", "initial_admin", "--material-file", materialPath); err == nil {
		t.Fatal("public material file accepted")
	}
}

func TestCameraStatusSummaryShowsRequestedEffectiveAndWarnings(t *testing.T) {
	isolate(t)
	requested := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}, Video: camera.DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 30}}
	effective := requested
	effective.Video.FPS = 25
	job := provisionhost.Job{ID: "front-door-v3", State: "done", Run: &provision.Run{Stage: provision.Active, Preset: &camera.DevicePreset{Ref: camera.PresetRef{ID: "office", Revision: "v3"}, MatchedRules: []string{"office-default", "serial-override"}, Requested: requested}, Effective: &effective, Warnings: camera.WarningsFor(requested, effective)}}
	const token = "fixture-private-status-token-with-more-than-32-bytes"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/runs" {
			json.NewEncoder(w).Encode([]provisionhost.Job{job, {ID: "queued-camera", State: "queued"}})
			return
		}
		if r.URL.Path != "/v1/runs/front-door-v3" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		data, _ := json.Marshal(job)
		var value map[string]any
		json.Unmarshal(data, &value)
		value["future_status_field"] = "preserved"
		json.NewEncoder(w).Encode(value)
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"camera", "status", "--server", server.URL, "--token-file", tokenPath, "--allow-loopback-http"}
	out, err := run(t, &cmd.Config{}, "", append(args, "--id", job.ID, "--summary")...)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"front-door-v3: active (job: done)", `preset: "office" revision "v3"`, `matched rules: ["office-default" "serial-override"]`, "requested: frequency=60Hz", "fps=30", "effective: frequency=60Hz", "fps=25", `WARNING "preset_adjusted"`, `"video.fps" requested="30" applied="25"`} {
		if !strings.Contains(out, text) {
			t.Fatalf("summary omitted %q: %s", text, out)
		}
	}
	list, err := run(t, &cmd.Config{}, "", append(args, "--summary")...)
	if err != nil || !strings.Contains(list, "queued-camera: queued") || !strings.Contains(list, "WARNING") {
		t.Fatalf("summary list: %s %v", list, err)
	}
	out, err = run(t, &cmd.Config{}, "", append(args, "--id", job.ID)...)
	var value map[string]any
	if err != nil || json.Unmarshal([]byte(out), &value) != nil || value["future_status_field"] != "preserved" {
		t.Fatalf("default JSON compatibility changed: %s %v", out, err)
	}
}

func TestCameraSummaryEscapesTerminalControlText(t *testing.T) {
	job := provisionhost.Job{ID: "job\ninjected", State: "done\x1b[31m", Run: &provision.Run{Stage: provision.Active, Warnings: []camera.PresetWarning{{Code: "preset_adjusted", Field: "video.fps", Requested: "30", Applied: "25", Reason: "unsupported\x1b[31m\nforged-status"}}}}
	job.Run.Effective = &camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: "off\x1b[31m\nforged-status"}}
	var output bytes.Buffer
	if err := printCameraSummary(&output, []provisionhost.Job{job}); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if strings.ContainsRune(text, '\x1b') || strings.Contains(text, "job\ninjected") || strings.Contains(text, "\nforged-status") || !strings.Contains(text, `job\ninjected`) || !strings.Contains(text, `reason="unsupported\x1b[31m\nforged-status"`) {
		t.Fatalf("summary allowed terminal control text: %q", text)
	}
}
