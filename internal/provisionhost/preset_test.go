package provisionhost

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
)

type presetProviderFunc func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error)

func (f presetProviderFunc) Resolve(ctx context.Context, scope camera.EnrollmentScope, hint camera.DeviceHint) (camera.DevicePreset, error) {
	return f(ctx, scope, hint)
}

func presetHostConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	token := filepath.Join(dir, "operator.token")
	if err := os.WriteFile(token, []byte(testToken), 0600); err != nil {
		t.Fatal(err)
	}
	session := filepath.Join(dir, "cp.session")
	if err := os.WriteFile(session, []byte("shale_tenant=fixture-session"), 0600); err != nil {
		t.Fatal(err)
	}
	return Config{
		StateDir:  filepath.Join(dir, "state"),
		Networks:  []Network{{ID: "lan", CIDRs: []string{"127.0.0.0/8"}}},
		Providers: []ProviderConfig{{Name: "private", Directory: filepath.Join(dir, "secrets")}},
		Targets:   []TargetConfig{{Scope: testScope(), Provider: "private", ConfigPath: filepath.Join(dir, "producer.yaml"), Restart: []string{"/bin/true"}, CP: ConnectionConfig{URL: "http://127.0.0.1:7500", SessionFile: session, AllowLoopbackHTTP: true}}},
		Clients:   []ClientConfig{{TokenFile: token, Scopes: []camera.EnrollmentScope{testScope()}}},
	}
}

func TestBuildInjectsPresetProviderAndScopedInitializer(t *testing.T) {
	c := presetHostConfig(t)
	provider := presetProviderFunc(func(_ context.Context, scope camera.EnrollmentScope, hint camera.DeviceHint) (camera.DevicePreset, error) {
		if scope != testScope() || hint.ID != "camera-one" {
			t.Fatal("preset provider lost the authorized scope or device")
		}
		return camera.DevicePreset{Ref: camera.PresetRef{ID: "injected", Revision: "1"}}, nil
	})
	built, err := BuildWithOptions(c, BuildOptions{Presets: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	engine := built.Host.options.Targets[0].Executor.(*provision.Engine)
	got, err := engine.Presets.Resolve(context.Background(), testScope(), testRequest().Device)
	if err != nil || got.Ref.ID != "injected" || engine.Initializer != nil {
		t.Fatalf("injected provider or baseline initializer: %+v %v", got, err)
	}
	c.Targets[0].Vendor = VendorConfig{Name: "tplink", WebEndpoint: "https://127.0.0.1", CompatibilityProfile: "easycam-c420-hw1-1.2.2-260624"}
	vendorBuilt, err := BuildWithOptions(c, BuildOptions{Presets: provider})
	if err != nil {
		t.Fatal(err)
	}
	defer vendorBuilt.Close()
	if vendorBuilt.Host.options.Targets[0].Executor.(*provision.Engine).Initializer == nil {
		t.Fatal("vendor initializer was not installed")
	}
}

func TestBuildLoadsScopedPresetFileAndRejectsAmbiguousSource(t *testing.T) {
	c := presetHostConfig(t)
	c.PresetFile = filepath.Join(t.TempDir(), "presets.yaml")
	if err := os.WriteFile(c.PresetFile, []byte("id: office\nrevision: v3\nrules:\n  - id: tenant-default\n    scope:\n      tenant_id: tenant\n    settings:\n      video:\n        codec: h264\n        width: 1920\n        height: 1080\n        fps: 30\n"), 0600); err != nil {
		t.Fatal(err)
	}
	built, err := Build(c)
	if err != nil {
		t.Fatal(err)
	}
	defer built.Close()
	provider := built.Host.options.Targets[0].Executor.(*provision.Engine).Presets
	got, err := provider.Resolve(context.Background(), testScope(), testRequest().Device)
	if err != nil || got.Ref.ID != "office" || got.Requested.Video.FPS != 30 {
		t.Fatalf("file provider not wired: %+v %v", got, err)
	}
	foreign := testScope()
	foreign.TenantID = "foreign"
	if _, err = provider.Resolve(context.Background(), foreign, testRequest().Device); !errors.Is(err, camera.ErrPresetNotFound) {
		t.Fatalf("preset escaped its tenant: %v", err)
	}
	if _, err = BuildWithOptions(c, BuildOptions{Presets: provider}); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("two preset sources accepted: %v", err)
	}
	if err = os.WriteFile(c.PresetFile, []byte("id: bad\nunknown_field: yes\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Build(c); err == nil {
		t.Fatal("invalid configured preset file accepted")
	}
}

func TestPresetWarningsSurviveHostReloadAndRemainScoped(t *testing.T) {
	requested := camera.PresetSettings{Video: camera.DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 30}}
	effective := requested
	effective.Video.FPS = 25
	warnings := camera.WarningsFor(requested, effective)
	options := testOptions(t, executorFunc(func(_ context.Context, q provision.Request, _ bool) (provision.Run, error) {
		return provision.Run{Request: q, Stage: provision.Active, Preset: &camera.DevicePreset{Ref: camera.PresetRef{ID: "office", Revision: "v3"}, Requested: requested}, Effective: &effective, Warnings: warnings}, nil
	}))
	host, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	q := testRequest()
	if _, err = host.submit(q, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	waitState(t, host, q.ID, "done")
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	foreign := testScope()
	foreign.TenantID = "foreign"
	foreignToken := testToken + "-foreign"
	options.Clients = append(options.Clients, Client{Token: foreignToken, Scopes: []camera.EnrollmentScope{foreign}})
	host, err = New(options)
	if err != nil {
		t.Fatal(err)
	}
	w := route(t, host, http.MethodGet, "/v1/runs/"+q.ID, nil, testToken)
	var job Job
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &job) != nil || job.Run == nil || job.Run.Effective == nil || job.Run.Stage != provision.Active || job.Run.Effective.Video.FPS != 25 || len(job.Run.Warnings) != 1 || job.Run.Warnings[0].Requested != "30" || job.Run.Warnings[0].Applied != "25" {
		t.Fatalf("durable warning status missing: %d %s", w.Code, w.Body)
	}
	if w = route(t, host, http.MethodGet, "/v1/runs/"+q.ID, nil, foreignToken); w.Code != http.StatusForbidden {
		t.Fatalf("foreign client saw warning detail: %d", w.Code)
	}
	w = route(t, host, http.MethodGet, "/v1/runs", nil, foreignToken)
	var jobs []Job
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &jobs) != nil || len(jobs) != 0 {
		t.Fatalf("foreign client saw warning list: %d %s", w.Code, w.Body)
	}
}

func TestInitializationStaysWithinSelectedVendorEndpoint(t *testing.T) {
	driver := &selectedDriver{Endpoint: "https://192.0.2.10"}
	hint := camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: "http://192.0.2.11/onvif"}}}
	if _, err := driver.PlanInitialization(context.Background(), hint, nil, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatalf("foreign endpoint initialization was accepted: %v", err)
	}
	if _, err := driver.ApplyInitialization(context.Background(), hint, nil, camera.InitializationPlan{After: camera.DeviceSettings{PowerLineFrequency: 60}}); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatalf("foreign endpoint apply was accepted: %v", err)
	}
}
