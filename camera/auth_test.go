package camera_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"go.yaml.in/yaml/v3"
)

func scope() camera.EnrollmentScope {
	return camera.EnrollmentScope{TenantID: "t", SetID: "set", ProducerID: "p", NetworkID: "lan"}
}
func staticEntry(recoverable bool) camera.StaticCredential {
	return camera.StaticCredential{Ref: camera.CredentialRef{Provider: "test", ID: "admin", Version: "1", Scope: scope(), Purpose: camera.AuthInitialAdmin, Recoverable: recoverable}, Access: camera.AccessAdministrator, Material: camera.CredentialMaterial{Username: "admin", Password: "a-secret/?@"}}
}

func TestExactReferenceSurvivesLeaseAndProviderRestart(t *testing.T) {
	ctx := context.Background()
	entry := staticEntry(true)
	p, err := camera.NewStaticProvider([]camera.StaticCredential{entry})
	if err != nil {
		t.Fatal(err)
	}
	req := camera.AuthRequest{Scope: scope(), Purpose: camera.AuthInitialAdmin, Requirement: camera.AuthRequirement{Access: camera.AccessAdministrator, Recoverable: true}, IntentKey: "activation-1"}
	l, err := camera.Acquire(ctx, p, req)
	if err != nil {
		t.Fatal(err)
	}
	ref := l.Ref()
	if err = l.Use(ctx, func(m camera.CredentialMaterial) error {
		if m.Password != entry.Material.Password {
			t.Fatal("wrong credential")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(l.Use(ctx, func(camera.CredentialMaterial) error { return nil }), camera.ErrExpired) {
		t.Fatal("closed lease usable")
	}
	// The provider's durable input is independent from this expired lease.
	p, err = camera.NewStaticProvider([]camera.StaticCredential{entry})
	if err != nil {
		t.Fatal(err)
	}
	req.ExistingRef = &ref
	l, err = camera.Acquire(ctx, p, req)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Ref() != ref {
		t.Fatal("credential version changed")
	}
	wrong := ref
	wrong.Version = "2"
	req.ExistingRef = &wrong
	if _, err = camera.Acquire(ctx, p, req); !errors.Is(err, camera.ErrAuthRequired) {
		t.Fatalf("version selected a default: %v", err)
	}
	wrong = ref
	wrong.Scope.NetworkID = "other"
	req.ExistingRef = &wrong
	if _, err = camera.Acquire(ctx, p, req); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("cross-scope credential: %v", err)
	}
}

func TestInitialIntentIsStableAndEphemeralActivationWaits(t *testing.T) {
	entry := staticEntry(false)
	p, _ := camera.NewStaticProvider([]camera.StaticCredential{entry})
	req := camera.AuthRequest{Scope: scope(), Purpose: camera.AuthInitialAdmin, IntentKey: "same-intent", Requirement: camera.AuthRequirement{Access: camera.AccessRead}}
	first, err := camera.Acquire(context.Background(), p, req)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := camera.Acquire(context.Background(), p, req)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Ref() != second.Ref() {
		t.Fatal("retry selected another initial credential")
	}
	req.Requirement.Recoverable = true
	if _, err = camera.Acquire(context.Background(), p, req); !errors.Is(err, camera.ErrNeedsUserInput) {
		t.Fatalf("ephemeral activation did not wait: %v", err)
	}
}

func TestAcquireRejectsProviderDefaultSubstitution(t *testing.T) {
	entry := staticEntry(true)
	want := entry.Ref
	bad := want
	bad.Version = "2"
	provider := providerFunc(func(context.Context, camera.AuthRequest) (camera.CredentialLease, error) {
		return camera.NewCredentialLease(bad, entry.Material, time.Now().Add(time.Minute)), nil
	})
	if _, err := camera.Acquire(context.Background(), provider, camera.AuthRequest{Scope: scope(), Purpose: camera.AuthInitialAdmin, ExistingRef: &want}); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("accepted wrong version: %v", err)
	}
}

type providerFunc func(context.Context, camera.AuthRequest) (camera.CredentialLease, error)

func (f providerFunc) Acquire(ctx context.Context, r camera.AuthRequest) (camera.CredentialLease, error) {
	return f(ctx, r)
}

func TestPublicSerializationContainsNoCredentialMaterial(t *testing.T) {
	material := camera.CredentialMaterial{Username: "private-user", Password: "private-pass"}
	profile := camera.MediaProfile{StreamURI: "rtsp://private-user:private-pass@camera/stream?token=private-pass&profile=main"}
	data := struct {
		Material camera.CredentialMaterial `json:"material"`
		Profile  camera.MediaProfile       `json:"profile"`
		Endpoint camera.Endpoint           `json:"endpoint"`
	}{material, profile, camera.Endpoint{URL: profile.StreamURI}}
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{string(b), fmt.Sprintf("%+v %#v", material, material)} {
		if strings.Contains(s, "private-") {
			t.Fatalf("secret serialized: %s", s)
		}
	}
	b, err = yaml.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "private-") {
		t.Fatal("YAML secret")
	}
}

// These are external implementations: no generated services, CLI or DB is
// needed to embed the camera contracts.
type embeddedDriver struct{}

func (embeddedDriver) Inspect(context.Context, camera.DeviceHint, camera.CredentialLease) (camera.Inspection, error) {
	return camera.Inspection{Activation: camera.ActivationReady}, nil
}
func (embeddedDriver) Plan(context.Context, camera.Inspection, camera.DesiredState) (camera.ChangePlan, error) {
	return camera.ChangePlan{}, nil
}
func (embeddedDriver) Apply(context.Context, camera.DeviceHint, camera.CredentialLease, camera.ChangePlan) (camera.CurrentState, error) {
	return camera.CurrentState{}, nil
}
func (embeddedDriver) Activation(context.Context, camera.DeviceHint) (camera.ActivationState, error) {
	return camera.ActivationReady, nil
}
func (embeddedDriver) Activate(context.Context, camera.DeviceHint, camera.CredentialLease) error {
	return nil
}

var _ camera.Inspector = embeddedDriver{}
var _ camera.VideoConfigurator = embeddedDriver{}
var _ camera.Bootstrapper = embeddedDriver{}
