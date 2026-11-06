package camera

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type AuthPurpose string

const (
	AuthInitialAdmin AuthPurpose = "initial_admin"
	AuthManagement   AuthPurpose = "management"
	AuthStreaming    AuthPurpose = "streaming"
)

type AccessLevel string

const (
	AccessRead          AccessLevel = "read"
	AccessConfigure     AccessLevel = "configure"
	AccessAdministrator AccessLevel = "administrator"
)

// CredentialRef identifies one immutable credential version. Recoverable means
// the provider can resolve this exact version after a process/worker restart.
// Ref expiry is not a lease expiry; credentials installed on a camera survive
// Close and must remain recoverable independently of a short-lived lease.
type CredentialRef struct {
	Provider    string          `json:"provider" yaml:"provider"`
	ID          string          `json:"id" yaml:"id"`
	Version     string          `json:"version" yaml:"version"`
	Scope       EnrollmentScope `json:"scope" yaml:"scope"`
	Purpose     AuthPurpose     `json:"purpose" yaml:"purpose"`
	Recoverable bool            `json:"recoverable" yaml:"recoverable"`
}

func (r CredentialRef) Valid() bool {
	return r.Provider != "" && r.ID != "" && r.Version != "" && r.Scope.TenantID != "" && r.Purpose != ""
}

func (r CredentialRef) Authorizes(scope EnrollmentScope, purpose AuthPurpose) bool {
	return r.Valid() && r.Scope == scope && r.Purpose == purpose
}

type AuthRequirement struct {
	Access      AccessLevel `json:"access"`
	Recoverable bool        `json:"recoverable"`
}

type AuthRequest struct {
	Scope       EnrollmentScope `json:"scope"`
	Device      DeviceHint      `json:"device"`
	Purpose     AuthPurpose     `json:"purpose"`
	Requirement AuthRequirement `json:"requirement"`
	ExistingRef *CredentialRef  `json:"existing_ref,omitempty"`
	IntentKey   string          `json:"intent_key,omitempty"`
}

type AuthProvider interface {
	Acquire(context.Context, AuthRequest) (CredentialLease, error)
}

// CredentialMaterial is only valid inside CredentialLease.Use. It is not a
// persisted/wire model. Applications must not retain it beyond that callback.
type CredentialMaterial struct {
	Username string `json:"-" yaml:"-"`
	Password string `json:"-" yaml:"-"`
}

func (CredentialMaterial) String() string   { return "[camera credential]" }
func (CredentialMaterial) GoString() string { return "[camera credential]" }

type CredentialLease interface {
	Ref() CredentialRef
	ExpiresAt() time.Time
	Use(context.Context, func(CredentialMaterial) error) error
	Close() error
}

// Acquire validates an injected provider's result at the consumer boundary.
// Drivers still implement their protocol's authentication exchange themselves.
func Acquire(ctx context.Context, p AuthProvider, request AuthRequest) (CredentialLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrAuthRequired
	}
	if request.ExistingRef != nil && !request.ExistingRef.Authorizes(request.Scope, request.Purpose) {
		return nil, ErrForbidden
	}
	lease, err := p.Acquire(ctx, request)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, ErrAuthRequired
	}
	ref := lease.Ref()
	if !ref.Authorizes(request.Scope, request.Purpose) || (request.ExistingRef != nil && ref != *request.ExistingRef) {
		lease.Close()
		return nil, ErrForbidden
	}
	if request.Requirement.Recoverable && !ref.Recoverable {
		lease.Close()
		return nil, ErrNeedsUserInput
	}
	if expiry := lease.ExpiresAt(); !expiry.IsZero() && !time.Now().Before(expiry) {
		lease.Close()
		return nil, ErrExpired
	}
	return lease, nil
}

// NewCredentialLease is useful to implement a provider without exporting secret
// fields. The caller owns durable reference storage; this lease stores material
// only in memory and Close cannot erase a credential already installed on a device.
func NewCredentialLease(ref CredentialRef, material CredentialMaterial, expiry time.Time) CredentialLease {
	return &credentialLease{ref: ref, material: material, expiry: expiry}
}

type credentialLease struct {
	mu       sync.Mutex
	ref      CredentialRef
	material CredentialMaterial
	expiry   time.Time
	closed   bool
}

func (l *credentialLease) Ref() CredentialRef   { return l.ref }
func (l *credentialLease) ExpiresAt() time.Time { return l.expiry }
func (l *credentialLease) Use(ctx context.Context, fn func(CredentialMaterial) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.closed || (!l.expiry.IsZero() && !time.Now().Before(l.expiry)) {
		return ErrExpired
	}
	if fn == nil {
		return ErrInvalid
	}
	return fn(l.material)
}
func (l *credentialLease) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.material = CredentialMaterial{}
	l.closed = true
	return nil
}
func (*credentialLease) String() string   { return "[camera credential lease]" }
func (*credentialLease) GoString() string { return "[camera credential lease]" }

// StaticCredential is a provider configuration entry, not job/inventory state.
// Persist it using the caller's secret store. Material is never serialized.
type StaticCredential struct {
	Ref      CredentialRef      `json:"ref"`
	Access   AccessLevel        `json:"access"`
	Material CredentialMaterial `json:"-" yaml:"-"`
}

// StaticProvider serves externally maintained, immutable credential versions.
// An initial credential must be durably configured before it is marked
// Recoverable. Recreate the provider from that store after restart. No secrets
// are generated here; multiple matching defaults require an explicit reference.
type StaticProvider struct {
	mu            sync.Mutex
	entries       []StaticCredential
	intents       map[string]CredentialRef
	LeaseDuration time.Duration
}

func NewStaticProvider(entries []StaticCredential) (*StaticProvider, error) {
	p := &StaticProvider{entries: append([]StaticCredential(nil), entries...), intents: map[string]CredentialRef{}, LeaseDuration: time.Minute}
	seen := map[string]bool{}
	for _, e := range p.entries {
		if !e.Ref.Valid() {
			return nil, ErrInvalid
		}
		key := fmt.Sprintf("%s\x00%s\x00%s", e.Ref.Provider, e.Ref.ID, e.Ref.Version)
		if seen[key] {
			return nil, ErrConflict
		}
		seen[key] = true
	}
	return p, nil
}

func (p *StaticProvider) Acquire(ctx context.Context, req AuthRequest) (CredentialLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.ExistingRef != nil && !req.ExistingRef.Authorizes(req.Scope, req.Purpose) {
		return nil, ErrForbidden
	}
	intent := fmt.Sprintf("%#v\x00%s\x00%s", req.Scope, req.Purpose, req.IntentKey)
	var selected *StaticCredential
	for i := range p.entries {
		e := &p.entries[i]
		if !e.Ref.Authorizes(req.Scope, req.Purpose) {
			continue
		}
		if req.ExistingRef != nil && e.Ref != *req.ExistingRef {
			continue
		}
		if prior, ok := p.intents[intent]; req.IntentKey != "" && ok && e.Ref != prior {
			continue
		}
		if selected != nil {
			return nil, ErrNeedsUserInput
		}
		selected = e
	}
	if selected == nil {
		return nil, ErrAuthRequired
	}
	if !accessAllows(selected.Access, req.Requirement.Access) {
		return nil, ErrForbidden
	}
	if req.Requirement.Recoverable && !selected.Ref.Recoverable {
		return nil, ErrNeedsUserInput
	}
	if req.IntentKey != "" {
		p.intents[intent] = selected.Ref
	}
	d := p.LeaseDuration
	if d <= 0 {
		d = time.Minute
	}
	return NewCredentialLease(selected.Ref, selected.Material, time.Now().Add(d)), nil
}

func accessAllows(have, want AccessLevel) bool {
	level := map[AccessLevel]int{AccessRead: 1, AccessConfigure: 2, AccessAdministrator: 3}
	return level[have] > 0 && (want == "" || (level[want] > 0 && level[have] >= level[want]))
}
