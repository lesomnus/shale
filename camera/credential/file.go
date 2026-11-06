package credential

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/lesomnus/shale/camera"
)

var (
	ErrUnavailable    = camera.ErrAuthRequired
	ErrScope          = camera.ErrForbidden
	ErrIntentConflict = errors.New("credential intent already prepared with different material")
)
var safePart = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// FileProvider is a deliberately small local provider. Dir must be a private
// directory, separate from deployment manifests and backups. Prepare writes an
// immutable version before activation. An application may inject a vault-backed
// AuthProvider instead; this provider is not a network secret distribution API.
type FileProvider struct {
	Dir, Name     string
	LeaseDuration time.Duration
}
type storedMaterial struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func storeMaterial(m camera.CredentialMaterial) storedMaterial {
	return storedMaterial{m.Username, m.Password}
}
func (m storedMaterial) material() camera.CredentialMaterial {
	return camera.CredentialMaterial{Username: m.Username, Password: m.Password}
}

type record struct {
	Ref      camera.CredentialRef `json:"ref"`
	Material storedMaterial       `json:"material"`
	Access   camera.AccessLevel   `json:"access"`
}

func (p FileProvider) name() string {
	if p.Name == "" {
		return "file"
	}
	return p.Name
}
func (p FileProvider) reference(scope camera.EnrollmentScope, purpose camera.AuthPurpose, intent string) camera.CredentialRef {
	b, _ := json.Marshal(struct {
		Provider string
		Scope    camera.EnrollmentScope
		Purpose  camera.AuthPurpose
		Intent   string
	}{p.name(), scope, purpose, intent})
	h := sha256.Sum256(b)
	return camera.CredentialRef{Provider: p.name(), ID: hex.EncodeToString(h[:]), Version: "1", Scope: scope, Purpose: purpose, Recoverable: true}
}
func (p FileProvider) path(ref camera.CredentialRef) (string, error) {
	if p.Dir == "" || ref.Provider != p.name() || !safePart.MatchString(ref.ID) || !safePart.MatchString(ref.Version) {
		return "", ErrUnavailable
	}
	return filepath.Join(p.Dir, ref.ID+"-"+ref.Version+".json"), nil
}
func (p FileProvider) read(ref camera.CredentialRef) (record, error) {
	var out record
	if !privateDirectory(p.Dir) {
		return out, ErrUnavailable
	}
	path, err := p.path(ref)
	if err != nil {
		return out, err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return out, ErrUnavailable
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 64<<10 {
		return out, ErrUnavailable
	}
	f, err := os.Open(path)
	if err != nil {
		return out, ErrUnavailable
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(&out); err != nil {
		return out, ErrUnavailable
	}
	if out.Ref != ref {
		return record{}, ErrScope
	}
	return out, nil
}

// Prepare durably binds an intent to one immutable credential version. Reusing
// an intent with different material is an error, including after process restart.
func (p FileProvider) Prepare(ctx context.Context, scope camera.EnrollmentScope, purpose camera.AuthPurpose, intent string, material camera.CredentialMaterial) (camera.CredentialRef, error) {
	if err := ctx.Err(); err != nil {
		return camera.CredentialRef{}, err
	}
	if intent == "" || material.Username == "" || material.Password == "" {
		return camera.CredentialRef{}, ErrUnavailable
	}
	ref := p.reference(scope, purpose, intent)
	if err := p.Put(ctx, ref, material); err != nil {
		return camera.CredentialRef{}, err
	}
	return ref, nil
}

// Put imports an explicitly versioned credential. It never overwrites material;
// rotation must choose another version and retain existing versions for recovery.
func (p FileProvider) Put(ctx context.Context, ref camera.CredentialRef, material camera.CredentialMaterial) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !ref.Valid() || !ref.Recoverable {
		return ErrUnavailable
	}
	path, err := p.path(ref)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(p.Dir, 0700); err != nil {
		return errors.New("cannot prepare credential store")
	}
	if !privateDirectory(p.Dir) {
		return errors.New("credential store must be a private directory")
	}
	if old, err := p.read(ref); err == nil {
		if old.Material != storeMaterial(material) {
			return ErrIntentConflict
		}
		return nil
	}
	b, err := json.Marshal(record{Ref: ref, Material: storeMaterial(material), Access: purposeAccess(ref.Purpose)})
	if err != nil {
		return ErrUnavailable
	}
	f, err := os.CreateTemp(p.Dir, ".prepare-")
	if err != nil {
		return errors.New("cannot prepare credential")
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	defer f.Close()
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return errors.New("cannot persist credential")
	}
	// Link is a no-replace atomic publication, so competing preparers cannot
	// overwrite a credential already installed by another process.
	if err = os.Link(tmp, path); err != nil {
		old, readErr := p.read(ref)
		if readErr != nil {
			return errors.New("cannot publish credential")
		}
		if old.Material != storeMaterial(material) {
			return ErrIntentConflict
		}
	}
	d, err := os.Open(p.Dir)
	if err != nil {
		return errors.New("cannot sync credential store")
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return errors.New("cannot sync credential store")
	}
	return nil
}
func (p FileProvider) Acquire(ctx context.Context, request camera.AuthRequest) (camera.CredentialLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var ref camera.CredentialRef
	if request.ExistingRef != nil {
		ref = *request.ExistingRef
	} else if request.IntentKey != "" {
		ref = p.reference(request.Scope, request.Purpose, request.IntentKey)
	} else {
		return nil, ErrUnavailable
	}
	if ref.Scope != request.Scope || ref.Purpose != request.Purpose {
		return nil, ErrScope
	}
	if request.Requirement.Recoverable && !ref.Recoverable {
		return nil, ErrUnavailable
	}
	r, err := p.read(ref)
	if err != nil {
		return nil, err
	}
	if !allows(r.Access, request.Requirement.Access) {
		return nil, ErrScope
	}
	ttl := p.LeaseDuration
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &lease{ref: ref, material: r.Material.material(), expires: time.Now().Add(ttl)}, nil
}

type lease struct {
	mu       sync.Mutex
	ref      camera.CredentialRef
	material camera.CredentialMaterial
	expires  time.Time
	closed   bool
}

func (l *lease) Ref() camera.CredentialRef { return l.ref }
func (l *lease) ExpiresAt() time.Time      { return l.expires }
func (l *lease) Use(ctx context.Context, f func(camera.CredentialMaterial) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.closed || !time.Now().Before(l.expires) {
		return camera.ErrExpired
	}
	return f(l.material)
}
func (l *lease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.material = camera.CredentialMaterial{}
	return nil
}

// WithInput resolves exactly the named streaming credential for one runtime
// operation. The authenticated URI only exists inside use; callers must sanitize
// subprocess diagnostics before returning them. Ref scope/purpose are checked
// even when a caller's provider does not implement its own authorization checks.
func WithInput(ctx context.Context, provider camera.AuthProvider, scope camera.EnrollmentScope, device camera.DeviceHint, ref *camera.CredentialRef, input string, use func(string, Sanitizer) error) error {
	if ref == nil {
		return SanitizeError(use(input, InputSanitizer(input)), InputSanitizer(input))
	}
	if provider == nil {
		return ErrUnavailable
	}
	if ref.Scope != scope || ref.Purpose != camera.AuthStreaming {
		return ErrScope
	}
	l, err := camera.Acquire(ctx, provider, camera.AuthRequest{Scope: scope, Device: device, Purpose: ref.Purpose, Requirement: camera.AuthRequirement{Access: camera.AccessRead}, ExistingRef: ref})
	if err != nil {
		return accessError(err)
	}
	defer l.Close()
	if l.Ref() != *ref {
		return ErrUnavailable
	}
	return l.Use(ctx, func(material camera.CredentialMaterial) error {
		u, err := parseInput(input)
		if err != nil {
			return err
		}
		u.User = userPassword(material.Username, material.Password)
		s := NewSanitizer(material.Username, material.Password)
		if err = use(u.String(), s); err != nil {
			return SanitizeError(err, s)
		}
		return nil
	})
}

func purposeAccess(p camera.AuthPurpose) camera.AccessLevel {
	switch p {
	case camera.AuthInitialAdmin:
		return camera.AccessAdministrator
	case camera.AuthManagement:
		return camera.AccessConfigure
	case camera.AuthStreaming:
		return camera.AccessRead
	}
	return ""
}
func allows(have, want camera.AccessLevel) bool {
	n := map[camera.AccessLevel]int{camera.AccessRead: 1, camera.AccessConfigure: 2, camera.AccessAdministrator: 3}
	return n[have] > 0 && (want == "" || (n[want] > 0 && n[have] >= n[want]))
}

// SanitizedError preserves errors.Is classification without exposing a wrapped
// URL-bearing error through Unwrap or diagnostic formatting.
type sanitizedError struct {
	cause   error
	message string
}

func (e sanitizedError) Error() string        { return e.message }
func (e sanitizedError) Is(target error) bool { return errors.Is(e.cause, target) }
func SanitizeError(err error, s Sanitizer) error {
	if err == nil {
		return nil
	}
	return sanitizedError{cause: err, message: s.Redact(err.Error())}
}

func privateDirectory(path string) bool {
	st, err := os.Lstat(path)
	return err == nil && st.IsDir() && st.Mode().Perm()&0077 == 0
}
func accessError(err error) error {
	for _, code := range []error{camera.ErrForbidden, camera.ErrNeedsUserInput, camera.ErrExpired, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, code) {
			return code
		}
	}
	return ErrUnavailable
}
