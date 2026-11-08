// Package material encodes opaque camera account bytes without database or
// protocol dependencies. Ordinary serializers never reveal these values.
package material

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/lesomnus/shale/camera"
)

var ErrInvalid = errors.New("camera credential: invalid input")

const MaxPayload = 64 << 10

const materialMagic = "SHCAMMAT\x01"
const materialMarker = "[camera credential material]"

// Material is an immutable in-process value. Passwords and usernames may
// contain any byte; JSON encoding would replace invalid UTF-8 and is therefore
// never its persistence/transport format. Use MarshalBinary only at an
// explicitly authorized boundary, then protect the result before persistence.
type Material struct {
	data string
}

func New(username, password string) (Material, error) {
	const header = len(materialMagic) + 8
	if username == "" || password == "" || len(username) > MaxPayload-header || len(password) > MaxPayload-header-len(username) {
		return Material{}, ErrInvalid
	}
	b := []byte(materialMagic)
	b = binary.BigEndian.AppendUint32(b, uint32(len(username)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(password)))
	b = append(b, username...)
	b = append(b, password...)
	return Material{data: string(b)}, nil
}

func Parse(b []byte) (Material, error) {
	const header = len(materialMagic) + 8
	if len(b) < header || len(b) > MaxPayload || string(b[:len(materialMagic)]) != materialMagic {
		return Material{}, ErrInvalid
	}
	u := uint64(binary.BigEndian.Uint32(b[len(materialMagic):]))
	p := uint64(binary.BigEndian.Uint32(b[len(materialMagic)+4:]))
	if u == 0 || p == 0 || u+p+uint64(header) != uint64(len(b)) {
		return Material{}, ErrInvalid
	}
	return Material{data: string(b)}, nil
}

func (m Material) Valid() bool { return m.data != "" }

// MarshalBinary explicitly reveals plaintext bytes. It must never be used by
// inventory, Source specifications, status, logs or ordinary request recording.
func (m Material) MarshalBinary() ([]byte, error) {
	if !m.Valid() {
		return nil, ErrInvalid
	}
	return []byte(m.data), nil
}

// Use reveals material only within the callback. The callback must not retain
// it. Scoped delivery clients wrap this value in an expiring credential lease.
func (m Material) Use(ctx context.Context, fn func(camera.CredentialMaterial) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !m.Valid() || fn == nil {
		return ErrInvalid
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
func (*Material) UnmarshalJSON([]byte) error          { return ErrInvalid }
func (*Material) UnmarshalYAML(func(any) error) error { return ErrInvalid }
