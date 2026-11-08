package cameracredential

import (
	"fmt"
	"io"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
)

const preparationMarker = "[camera credential preparation]"

func (i Intent) Validate() error {
	if err := i.Subject.Validate(); err != nil || !validIntentKey(i.Key) {
		return ErrInput
	}
	switch i.Access {
	case camera.AccessRead, camera.AccessConfigure, camera.AccessAdministrator:
	default:
		return ErrInput
	}
	if i.Subject.Purpose == camerasecret.InitialAdmin && i.Access != camera.AccessAdministrator {
		return ErrInput
	}
	return nil
}

func validIntentKey(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for index, b := range []byte(s) {
		if b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' {
			continue
		}
		if index > 0 && (b == '_' || b == '-' || b == '.' || b == ':' || b == '/') {
			continue
		}
		return false
	}
	return true
}

// NewPreparation copies a requested NEW-account username. It never changes the
// username in imported/current/reused material. The Store records a keyed tag
// of this input so the same intent with changed username bytes is rejected.
func NewPreparation(intent Intent, username []byte, reuse *Reference) (Preparation, error) {
	if err := intent.Validate(); err != nil || len(username) == 0 || len(username) > 1024 {
		return Preparation{}, ErrInput
	}
	var ref *Reference
	if reuse != nil {
		if _, err := reuse.Context(); err != nil || reuse.Subject.TenantID != intent.Subject.TenantID || reuse.Subject.DeviceID != intent.Subject.DeviceID {
			return Preparation{}, ErrInput
		}
		copy := *reuse
		ref = &copy
	}
	return Preparation{Intent: intent, Reuse: ref, username: string(username)}, nil
}

// Username explicitly reveals a fresh copy for the trusted generator/Store.
// It is not ordinary metadata and must not enter an unkeyed request hash.
func (p Preparation) Username() []byte { return []byte(p.username) }

func (Preparation) Format(s fmt.State, _ rune)           { _, _ = io.WriteString(s, preparationMarker) }
func (Preparation) String() string                       { return preparationMarker }
func (Preparation) GoString() string                     { return preparationMarker }
func (Preparation) MarshalJSON() ([]byte, error)         { return []byte(`"` + preparationMarker + `"`), nil }
func (Preparation) MarshalYAML() (any, error)            { return preparationMarker, nil }
func (*Preparation) UnmarshalJSON([]byte) error          { return ErrInput }
func (*Preparation) UnmarshalYAML(func(any) error) error { return ErrInput }
