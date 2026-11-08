package cameracredential

import (
	"context"
	"errors"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential/material"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/protobuf-orm/ent/dialect"
)

var (
	ErrInput       = material.ErrInvalid
	ErrConflict    = errors.New("camera credential: intent conflicts with existing preparation")
	ErrNotFound    = errors.New("camera credential: version not found")
	ErrRevoked     = errors.New("camera credential: revoked")
	ErrUnavailable = camerasecret.ErrUnavailable
)

const Provider = "cp"

// Subject is a stable secret scope. Site and current machine/assignment/network
// identifiers are checked by the CP before calling Store, not encoded here.
type Subject struct {
	TenantID  pdid.Id              `json:"tenant_id" yaml:"tenant_id"`
	DeviceID  pdid.Id              `json:"device_id" yaml:"device_id"`
	BindingID pdid.Id              `json:"binding_id" yaml:"binding_id"`
	Purpose   camerasecret.Purpose `json:"purpose" yaml:"purpose"`
}

func (s Subject) Validate() error {
	if s.TenantID.Domain() != 1 || s.DeviceID.Domain() != 27 || !s.BindingID.IsZero() && s.BindingID.Domain() != 29 {
		return ErrInput
	}
	switch s.Purpose {
	case camerasecret.InitialAdmin:
		if !s.BindingID.IsZero() {
			return ErrInput
		}
	case camerasecret.Management, camerasecret.Streaming:
	default:
		return ErrInput
	}
	return nil
}

// Reference is nonsecret and is not a delivery capability. Its exact version
// must be recoverable independently of a Worker/Producer process or key.
type Reference struct {
	Subject      Subject `json:"subject" yaml:"subject"`
	CredentialID pdid.Id `json:"credential_id" yaml:"credential_id"`
	Version      string  `json:"version" yaml:"version"`
}

func (r Reference) Context() (camerasecret.Context, error) {
	c, err := camerasecret.NewContext(camerasecret.Identity{
		TenantID: r.Subject.TenantID, DeviceID: r.Subject.DeviceID, BindingID: r.Subject.BindingID,
		Purpose: r.Subject.Purpose, CredentialID: r.CredentialID, Version: r.Version,
	})
	if err != nil {
		return camerasecret.Context{}, ErrInput
	}
	return c, nil
}

type Origin string

const (
	Generated Origin = "generated"
	Imported  Origin = "imported"
	Reused    Origin = "reused"
)

// Version is the only ordinary result of preparation/import. It contains no
// envelope, keyed import tag, account material or current delivery grant.
type Version struct {
	Ref       Reference          `json:"ref" yaml:"ref"`
	Access    camera.AccessLevel `json:"access" yaml:"access"`
	Origin    Origin             `json:"origin" yaml:"origin"`
	Backend   string             `json:"backend" yaml:"backend"`
	CreatedAt time.Time          `json:"created_at" yaml:"created_at"`
	Revoked   bool               `json:"revoked" yaml:"revoked"`
}

type Intent struct {
	Subject Subject
	Key     string
	Access  camera.AccessLevel
}

type Preparation struct {
	Intent Intent
	// Explicitly reuse this account under another delivery purpose. The
	// subject's tenant/device must match and the installed material is not
	// rotated. Purpose separation does not imply separate camera accounts.
	Reuse    *Reference
	username string
}

type ImportRequest struct {
	Intent   Intent
	Material Material
	// Required to replace an existing current version. A nil expectation is
	// creation, not an instruction to overwrite a concurrently prepared one.
	ExpectedCurrent *Reference
	// Preserve the exact original standalone reference/scope on deliberate
	// migration; never rotate the camera or delete its old recoverable copy.
	Legacy *camera.CredentialRef
}

type Generator func(context.Context) (Material, error)

// StoreFactory must use the supplied CURRENT driver. A Core rebind or outer
// batch must never escape to a captured pool or begin an independent commit.
// The backend/KEK may be shared; database transactions may not be.
type StoreFactory func(dialect.Driver) (Store, error)

// Store is an in-process persistence boundary, not an authenticated RPC.
// The caller validates current tenant/site/device/binding and execution grants
// before calling it. Resolve checks both credential and exact-version
// revocation; it never substitutes a current/default password for a missing ref.
//
// Prepare first looks up the durable subject/intent and existing current
// version. A retry reuses its recorded identity before comparing an import tag
// or invoking a Generator. It must not generate a new initial password merely
// because a second binding/profile is added to the same activated device.
// An unseen intent reusing a current account persists a same-material version
// with Origin Reused, so a later import cannot change what that intent replays.
// A new version reference does not mean the installed password was rotated;
// previous Source/Job version pins remain independently resolvable.
// Import compares exact keyed payload tags and immutable request metadata;
// conflicting retries fail without replacing the installed material.
//
// A successful write belongs to the supplied transaction. The enclosing caller
// MUST commit it before publishing a recoverable reference, returning it to a
// machine or allowing any camera mutation. Policy preparation and Job.Create
// can therefore share one CP transaction without a fake operator identity or
// re-entering an outward-facing credential RPC.
//
// Errors are the fixed sentinels above or context cancellation/deadline errors;
// raw crypto, backend and payload parser diagnostics do not cross this boundary.
type Store interface {
	Prepare(context.Context, Preparation, Generator) (Version, error)
	Import(context.Context, ImportRequest) (Version, error)
	Resolve(context.Context, Reference) (Material, error)
}
