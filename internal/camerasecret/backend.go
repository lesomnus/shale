package camerasecret

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
)

// Backend protects an opaque credential payload under its stable identity.
// A replacement backend may return an opaque secret-store handle instead of
// ciphertext. Protect must succeed before any camera mutation is attempted.
type Backend interface {
	Protect(context.Context, Context, []byte) (Envelope, error)
	Open(context.Context, Context, Envelope) ([]byte, error)
}

// ImportReplay computes a keyed equality tag for an import operation. A stored
// unkeyed password hash would let a database reader test password guesses.
// Authorizing an import and enforcing durable idempotency are Store duties.
// A retry must look up and reuse the persisted credential/version identity
// before recomputing its tag; a newly generated identity is a different input.
type ImportReplay interface {
	ImportTag(context.Context, Context, string, []byte) (ReplayTag, error)
}

const (
	aesBackend      = "cp-aes256gcm-hkdf-sha256-v1"
	encryptionLabel = "shale/camera-secret/encryption/v1"
	importLabel     = "shale/camera-secret/import-replay/v1"
	redactedBackend = "[camera secret backend]"
)

// AESGCM derives purpose-specific keys from the existing CP KEK without
// retaining the caller's slice or altering the signing-key wrap format.
// It is safe for concurrent use. Ordinary serializers never expose its keys.
type AESGCM struct {
	aead      cipher.AEAD
	importKey [sha256.Size]byte
}

var _ Backend = (*AESGCM)(nil)
var _ ImportReplay = (*AESGCM)(nil)

func NewAESGCM(kek []byte) (*AESGCM, error) {
	if len(kek) != 32 {
		return nil, ErrUnavailable
	}
	enc, err := hkdf.Key(sha256.New, kek, nil, encryptionLabel, 32)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer clear(enc)
	mac, err := hkdf.Key(sha256.New, kek, nil, importLabel, sha256.Size)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer clear(mac)
	blk, err := aes.NewCipher(enc)
	if err != nil {
		return nil, ErrUnavailable
	}
	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, ErrUnavailable
	}
	b := &AESGCM{aead: aead}
	copy(b.importKey[:], mac)
	return b, nil
}

func (b *AESGCM) Protect(ctx context.Context, c Context, payload []byte) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	if b == nil || b.aead == nil {
		return Envelope{}, ErrUnavailable
	}
	if !c.valid() {
		return Envelope{}, ErrContext
	}
	if len(payload) > MaxPayload {
		return Envelope{}, ErrPayload
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Envelope{}, ErrUnavailable
	}
	size := len(nonce) + len(payload) + b.aead.Overhead()
	aad := appendField(envelopeHeader(aesBackend, size), []byte(c.aad))
	opaque := b.aead.Seal(nonce, nonce, payload, aad)
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	return NewEnvelope(aesBackend, opaque)
}

func (b *AESGCM) Open(ctx context.Context, c Context, envelope Envelope) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil || b.aead == nil {
		return nil, ErrUnavailable
	}
	if !c.valid() {
		return nil, ErrContext
	}
	if !envelope.valid() || envelope.backend != aesBackend || len(envelope.opaque) < b.aead.NonceSize()+b.aead.Overhead() || len(envelope.opaque) > b.aead.NonceSize()+MaxPayload+b.aead.Overhead() {
		return nil, ErrEnvelope
	}
	opaque := []byte(envelope.opaque)
	nonce, ciphertext := opaque[:b.aead.NonceSize()], opaque[b.aead.NonceSize():]
	aad := appendField(envelopeHeader(aesBackend, len(opaque)), []byte(c.aad))
	payload, err := b.aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrOpen
	}
	if err := ctx.Err(); err != nil {
		clear(payload)
		return nil, err
	}
	return payload, nil
}

func (b *AESGCM) ImportTag(ctx context.Context, c Context, intent string, payload []byte) (ReplayTag, error) {
	if err := ctx.Err(); err != nil {
		return ReplayTag{}, err
	}
	if b == nil || b.aead == nil {
		return ReplayTag{}, ErrUnavailable
	}
	if !c.valid() || !token(intent) {
		return ReplayTag{}, ErrContext
	}
	if len(payload) > MaxPayload {
		return ReplayTag{}, ErrPayload
	}
	h := hmac.New(sha256.New, b.importKey[:])
	h.Write([]byte("shale/camera-secret/import-tag/v1\x00"))
	writeField(h, []byte(c.aad))
	writeField(h, []byte(intent))
	writeField(h, payload)
	var out ReplayTag
	copy(out.sum[:], h.Sum(nil))
	out.valid = true
	if err := ctx.Err(); err != nil {
		return ReplayTag{}, err
	}
	return out, nil
}

func writeField(h hash.Hash, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	h.Write(size[:])
	h.Write(value)
}

func (AESGCM) Format(s fmt.State, _ rune)           { _, _ = io.WriteString(s, redactedBackend) }
func (AESGCM) String() string                       { return redactedBackend }
func (AESGCM) GoString() string                     { return redactedBackend }
func (AESGCM) MarshalJSON() ([]byte, error)         { return []byte(`"` + redactedBackend + `"`), nil }
func (AESGCM) MarshalYAML() (any, error)            { return redactedBackend, nil }
func (*AESGCM) UnmarshalJSON([]byte) error          { return ErrUnavailable }
func (*AESGCM) UnmarshalYAML(func(any) error) error { return ErrUnavailable }
