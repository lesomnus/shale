package camerasecret

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// MaxPayload bounds credential material before encryption. The backend
	// treats it as bytes and does not normalize usernames, passwords or JSON.
	MaxPayload       = 64 << 10
	maxOpaque        = MaxPayload + 4096
	wireVersion      = 1
	envelopeMagic    = "SHCAMSEC"
	redactedEnvelope = "[camera secret envelope]"
	redactedReplay   = "[camera secret replay tag]"
)

// Envelope contains ciphertext or a backend's opaque external-store handle.
// A backend must never put plaintext credentials in an envelope. Ordinary
// serializers deliberately cannot persist it; use MarshalBinary/ParseEnvelope.
type Envelope struct {
	backend string
	opaque  string
}

// NewEnvelope copies opaque backend data. The backend identifier includes the
// backend's algorithm/version; the common wire format has its own version.
func NewEnvelope(backend string, opaque []byte) (Envelope, error) {
	if !backendName(backend) || len(opaque) == 0 || len(opaque) > maxOpaque {
		return Envelope{}, ErrEnvelope
	}
	return Envelope{backend: backend, opaque: string(opaque)}, nil
}

func backendName(s string) bool {
	if len(s) == 0 || len(s) > 63 {
		return false
	}
	for _, b := range []byte(s) {
		if b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.' {
			continue
		}
		return false
	}
	return true
}

func (e Envelope) Backend() string { return e.backend }

// Opaque returns a new copy for the responsible backend. It is not a logging
// or wire DTO. Persist the complete versioned envelope, not this field alone.
func (e Envelope) Opaque() []byte { return []byte(e.opaque) }

func (e Envelope) valid() bool {
	return backendName(e.backend) && len(e.opaque) > 0 && len(e.opaque) <= maxOpaque
}

func envelopeHeader(backend string, size int) []byte {
	b := []byte(envelopeMagic)
	b = append(b, wireVersion, byte(len(backend)))
	b = binary.BigEndian.AppendUint32(b, uint32(size))
	return append(b, backend...)
}

func (e Envelope) MarshalBinary() ([]byte, error) {
	if !e.valid() {
		return nil, ErrEnvelope
	}
	return append(envelopeHeader(e.backend, len(e.opaque)), e.opaque...), nil
}

func ParseEnvelope(b []byte) (Envelope, error) {
	const fixed = len(envelopeMagic) + 1 + 1 + 4
	if len(b) < fixed || string(b[:len(envelopeMagic)]) != envelopeMagic || b[len(envelopeMagic)] != wireVersion {
		return Envelope{}, ErrEnvelope
	}
	nameLen := int(b[len(envelopeMagic)+1])
	size := uint64(binary.BigEndian.Uint32(b[len(envelopeMagic)+2 : fixed]))
	if nameLen == 0 || nameLen > 63 || size == 0 || size > maxOpaque || uint64(len(b)) != uint64(fixed+nameLen)+size {
		return Envelope{}, ErrEnvelope
	}
	return NewEnvelope(string(b[fixed:fixed+nameLen]), b[fixed+nameLen:])
}

func (Envelope) Format(s fmt.State, _ rune)           { _, _ = io.WriteString(s, redactedEnvelope) }
func (Envelope) String() string                       { return redactedEnvelope }
func (Envelope) GoString() string                     { return redactedEnvelope }
func (Envelope) MarshalJSON() ([]byte, error)         { return []byte(`"` + redactedEnvelope + `"`), nil }
func (Envelope) MarshalYAML() (any, error)            { return redactedEnvelope, nil }
func (*Envelope) UnmarshalJSON([]byte) error          { return ErrEnvelope }
func (*Envelope) UnmarshalYAML(func(any) error) error { return ErrEnvelope }
