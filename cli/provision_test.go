package cli

import (
	"context"
	"encoding/json"
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
