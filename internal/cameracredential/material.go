// Package cameracredential defines the CP credential Store contract. Delivery
// authorization belongs to the current CP assignment/job/Source boundary; a
// Store reference or a stored account purpose is not a machine grant.
package cameracredential

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
)

const materialMagic = "SHCAMMAT\x01"
const materialMarker = "[camera credential material]"

// Material is an immutable in-process value. Passwords and usernames may
// contain any byte; JSON encoding would replace invalid UTF-8 and is therefore
// never its persistence/transport format. Use MarshalBinary only at an
// explicitly authorized boundary, then protect the result before persistence.
type Material struct {
	data string
}

func NewMaterial(username, password string) (Material, error) {
	const header = len(materialMagic) + 8
	if username == "" || password == "" || len(username) > camerasecret.MaxPayload-header || len(password) > camerasecret.MaxPayload-header-len(username) {
		return Material{}, ErrInput
	}
	b := []byte(materialMagic)
	b = binary.BigEndian.AppendUint32(b, uint32(len(username)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(password)))
	b = append(b, username...)
	b = append(b, password...)
	return Material{data: string(b)}, nil
}

func ParseMaterial(b []byte) (Material, error) {
	const header = len(materialMagic) + 8
	if len(b) < header || len(b) > camerasecret.MaxPayload || string(b[:len(materialMagic)]) != materialMagic {
		return Material{}, ErrInput
	}
	u := uint64(binary.BigEndian.Uint32(b[len(materialMagic):]))
	p := uint64(binary.BigEndian.Uint32(b[len(materialMagic)+4:]))
	if u == 0 || p == 0 || u+p+uint64(header) != uint64(len(b)) {
		return Material{}, ErrInput
	}
	return Material{data: string(b)}, nil
}

func (m Material) valid() bool { return m.data != "" }

// MarshalBinary explicitly reveals plaintext bytes. It must never be used by
// inventory, Source specifications, status, logs or ordinary request recording.
func (m Material) MarshalBinary() ([]byte, error) {
	if !m.valid() {
		return nil, ErrInput
	}
	return []byte(m.data), nil
}

// Use reveals material only within the callback. The callback must not retain
// it. Scoped delivery clients wrap this value in an expiring credential lease.
func (m Material) Use(ctx context.Context, fn func(camera.CredentialMaterial) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.valid() || fn == nil {
		return ErrInput
	}
	const header = len(materialMagic) + 8
	u := int(binary.BigEndian.Uint32([]byte(m.data[len(materialMagic):])))
	return fn(camera.CredentialMaterial{Username: m.data[header : header+u], Password: m.data[header+u:]})
}

func (Material) Format(s fmt.State, _ rune)           { _, _ = io.WriteString(s, materialMarker) }
func (Material) String() string                       { return materialMarker }
func (Material) GoString() string                     { return materialMarker }
func (Material) MarshalJSON() ([]byte, error)         { return []byte(`"` + materialMarker + `"`), nil }
func (Material) MarshalYAML() (any, error)            { return materialMarker, nil }
func (*Material) UnmarshalJSON([]byte) error          { return ErrInput }
func (*Material) UnmarshalYAML(func(any) error) error { return ErrInput }
