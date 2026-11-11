package camerasecret

import (
	"encoding/binary"
	"errors"

	"github.com/lesomnus/payday/pdid"
)

var (
	ErrContext     = errors.New("camera secret: invalid context")
	ErrPayload     = errors.New("camera secret: invalid payload")
	ErrEnvelope    = errors.New("camera secret: invalid envelope")
	ErrReplayTag   = errors.New("camera secret: invalid replay tag")
	ErrUnavailable = errors.New("camera secret: backend unavailable")
	ErrOpen        = errors.New("camera secret: cannot open envelope")
)

type Purpose string

const (
	InitialAdmin Purpose = "initial_admin"
	Management   Purpose = "management"
	Streaming    Purpose = "streaming"
)

// Identity is nonsecret metadata. Domain numbers are checked without requiring
// schema registration, so this package does not depend on generated entities.
// CredentialID names the stable credential, not its immutable version row.
type Identity struct {
	TenantID     pdid.Id
	DeviceID     pdid.Id
	BindingID    pdid.Id
	Purpose      Purpose
	CredentialID pdid.Id
	Version      string
}

// Context is immutable after construction. Identity returns a value copy.
// A zero Context cannot be used to protect or open a payload.
type Context struct {
	identity Identity
	aad      string
}

func NewContext(id Identity) (Context, error) {
	if id.TenantID.Domain() != 1 || id.DeviceID.Domain() != 27 || id.CredentialID.Domain() != 32 || !token(id.Version) {
		return Context{}, ErrContext
	}
	if !id.BindingID.IsZero() && id.BindingID.Domain() != 29 {
		return Context{}, ErrContext
	}
	switch id.Purpose {
	case InitialAdmin:
		// Bootstrap is a device-wide operation. Adding another media binding
		// must not manufacture a different initial administrator password.
		if !id.BindingID.IsZero() {
			return Context{}, ErrContext
		}
	case Management, Streaming:
		// Either device-wide or binding-specific credentials are valid. The
		// Store selects a reuse policy and enforces the corresponding ACL.
	default:
		return Context{}, ErrContext
	}

	b := []byte("shale/camera-secret/context/v1\x00")
	b = append(b, id.TenantID[:]...)
	b = append(b, id.DeviceID[:]...)
	if id.BindingID.IsZero() {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		b = append(b, id.BindingID[:]...)
	}
	b = append(b, id.CredentialID[:]...)
	b = appendField(b, []byte(id.Purpose))
	b = appendField(b, []byte(id.Version))
	return Context{identity: id, aad: string(b)}, nil
}

func (c Context) Identity() Identity { return c.identity }
func (c Context) valid() bool        { return c.aad != "" }

func token(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, b := range []byte(s) {
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == ':' || b == '/' {
			continue
		}
		return false
	}
	return true
}

func appendField(dst, value []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}
