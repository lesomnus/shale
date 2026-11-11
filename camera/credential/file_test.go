package credential_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
)

func TestDurablePrepareExactVersionAndIntent(t *testing.T) {
	ctx := context.Background()
	scope := camera.EnrollmentScope{TenantID: "t", SetID: "s", ProducerID: "p", NetworkID: "lan"}
	dir := filepath.Join(t.TempDir(), "private")
	provider := credential.FileProvider{Dir: dir, Name: "local"}
	secret := camera.CredentialMaterial{Username: "camera", Password: "a:@/ ?%&b"}
	ref, err := provider.Prepare(ctx, scope, camera.AuthInitialAdmin, "activation-42", secret)
	if err != nil {
		t.Fatal(err)
	}
	if !ref.Recoverable {
		t.Fatal("activation secret must survive a restart")
	}
	restarted := credential.FileProvider{Dir: dir, Name: "local"}
	repeated, err := restarted.Prepare(ctx, scope, camera.AuthInitialAdmin, "activation-42", secret)
	if err != nil || repeated != ref {
		t.Fatalf("retry changed reference: %v", err)
	}
	_, err = restarted.Prepare(ctx, scope, camera.AuthInitialAdmin, "activation-42", camera.CredentialMaterial{Username: "camera", Password: "different"})
	if !errors.Is(err, credential.ErrIntentConflict) {
		t.Fatal("intent was overwritten")
	}
	request := camera.AuthRequest{Scope: scope, Purpose: camera.AuthInitialAdmin, ExistingRef: &ref, Requirement: camera.AuthRequirement{Access: camera.AccessAdministrator, Recoverable: true}}
	l, err := camera.Acquire(ctx, restarted, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Use(ctx, func(material camera.CredentialMaterial) error {
		if material != secret {
			t.Fatal("wrong immutable version")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if err = l.Use(ctx, func(camera.CredentialMaterial) error { return nil }); err == nil {
		t.Fatal("closed material still usable")
	}
	l, err = camera.Acquire(ctx, restarted, request)
	if err != nil {
		t.Fatal("close deleted installed credential")
	}
	l.Close()
	wrong := scope
	wrong.NetworkID = "other"
	request.Scope = wrong
	if _, err = camera.Acquire(ctx, restarted, request); !errors.Is(err, camera.ErrForbidden) {
		t.Fatal("cross-network credential allowed")
	}
	for _, entry := range mustReadDir(t, dir) {
		st, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0600 {
			t.Fatal("credential file not private")
		}
	}
	b, _ := json.Marshal(ref)
	if strings.Contains(string(b), secret.Password) {
		t.Fatal("reference leaked secret")
	}
}
func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	v, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func TestStreamScopeReservedURIAndRedaction(t *testing.T) {
	ctx := context.Background()
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "p", NetworkID: "lan"}
	p := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "secrets")}
	secret := camera.CredentialMaterial{Username: "u/@", Password: "p:@/?% &x"}
	ref, err := p.Prepare(ctx, scope, camera.AuthStreaming, "stream", secret)
	if err != nil {
		t.Fatal(err)
	}
	sent := false
	err = credential.WithInput(ctx, p, scope, camera.DeviceHint{}, &ref, "rtsp://camera/stream", func(input string, sanitize credential.Sanitizer) error {
		sent = true
		u, e := url.Parse(input)
		if e != nil {
			t.Fatal(e)
		}
		pw, _ := u.User.Password()
		if pw != secret.Password || u.User.Username() != secret.Username {
			t.Fatal("reserved URI material corrupted")
		}
		return fmt.Errorf("tool refused %s password=%s encoded=%s", input, pw, url.QueryEscape(pw))
	})
	if !sent || err == nil {
		t.Fatal("stream input not resolved")
	}
	for _, value := range []string{secret.Password, url.QueryEscape(secret.Password), url.PathEscape(secret.Password)} {
		if strings.Contains(err.Error(), value) {
			t.Fatal("error leaked material")
		}
	}
	wrong := scope
	wrong.SetID = "other"
	if err = credential.WithInput(ctx, p, wrong, camera.DeviceHint{}, &ref, "rtsp://camera/stream", func(string, credential.Sanitizer) error { t.Fatal("unauthorized callback"); return nil }); !errors.Is(err, camera.ErrForbidden) {
		t.Fatal(err)
	}
	ref.Purpose = camera.AuthManagement
	if err = credential.WithInput(ctx, p, scope, camera.DeviceHint{}, &ref, "rtsp://camera/stream", func(string, credential.Sanitizer) error { t.Fatal("management secret used for capture"); return nil }); !errors.Is(err, camera.ErrForbidden) {
		t.Fatal(err)
	}
}
func TestLeaseExpiryAndProviderErrors(t *testing.T) {
	scope := camera.EnrollmentScope{TenantID: "tenant"}
	p := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "private"), LeaseDuration: time.Nanosecond}
	ref, e := p.Prepare(context.Background(), scope, camera.AuthStreaming, "one", camera.CredentialMaterial{Username: "u", Password: "p"})
	if e != nil {
		t.Fatal(e)
	}
	l, e := p.Acquire(context.Background(), camera.AuthRequest{Scope: scope, Purpose: ref.Purpose, ExistingRef: &ref})
	if e != nil {
		t.Fatal(e)
	}
	time.Sleep(time.Millisecond)
	if e = l.Use(context.Background(), func(camera.CredentialMaterial) error { t.Fatal("expired lease used"); return nil }); e == nil {
		t.Fatal("expiry ignored")
	}
	l.Close()
	ref.Version = "missing"
	if _, e = p.Acquire(context.Background(), camera.AuthRequest{Scope: scope, Purpose: ref.Purpose, ExistingRef: &ref}); !errors.Is(e, camera.ErrAuthRequired) {
		t.Fatal("reference chose another version")
	}
}
func TestDiagnosticRedactionLegacyAndMalformed(t *testing.T) {
	values := []string{"tool rtsp://user:password@host/path?token=x", "rtsp://user:bad%escape@host/path", "http://user:pass@host/"}
	for _, input := range values {
		out := credential.Redact(input)
		if strings.Contains(out, "password") || strings.Contains(out, "bad%escape") || strings.Contains(out, "user:pass") {
			t.Fatal("URL material retained")
		}
	}
	sentinel := errors.New("typed outcome")
	e := credential.SanitizeError(fmt.Errorf("failed rtsp://user:pass@host: %w", sentinel), credential.NewSanitizer("pass"))
	if !errors.Is(e, sentinel) || errors.Unwrap(e) != nil || strings.Contains(fmt.Sprintf("%+v", e), "user:pass") {
		t.Fatal("sanitized error lost classification or exposed cause")
	}
}

func TestPrivateStoreDirectoryRequiredOnRead(t *testing.T) {
	ctx := context.Background()
	scope := camera.EnrollmentScope{TenantID: "tenant"}
	dir := filepath.Join(t.TempDir(), "private")
	provider := credential.FileProvider{Dir: dir}
	ref, e := provider.Prepare(ctx, scope, camera.AuthStreaming, "same", camera.CredentialMaterial{Username: "user", Password: "password"})
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(dir, 0777); e != nil {
		t.Fatal(e)
	}
	if _, e = provider.Acquire(ctx, camera.AuthRequest{Scope: scope, Purpose: camera.AuthStreaming, ExistingRef: &ref}); !errors.Is(e, camera.ErrAuthRequired) {
		t.Fatal("credential resolved from replaceable directory")
	}
	if e = os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(t.TempDir(), "link")
	if e = os.Symlink(dir, link); e != nil {
		t.Skip("symlinks unavailable")
	}
	provider.Dir = link
	if _, e = provider.Acquire(ctx, camera.AuthRequest{Scope: scope, Purpose: camera.AuthStreaming, ExistingRef: &ref}); !errors.Is(e, camera.ErrAuthRequired) {
		t.Fatal("credential resolved through directory symlink")
	}
}
