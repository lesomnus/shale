// Package token signs and verifies Shale access tokens (§33.2).
//
// A token is a compact set of claims signed by the Control Plane with
// Ed25519. A Storage Node or a Relay verifies one locally, against the key
// set it watches, and needs no call to the CP: signature, known key, expiry
// with a little skew, audience equals its own ID, and the operation matches
// the request.
//
// The wire form is two base64url segments, claims then signature, joined by
// a dot. The claims are the protobuf encoding of [api.TokenClaims], so the
// format is versioned by the message and there is no second schema to keep
// in step.
package token

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
)

// Scheme is what a token is carried under in an Authorization header.
const Scheme = "Shale"

var (
	ErrMalformed  = errors.New("token: malformed")
	ErrSignature  = errors.New("token: bad signature")
	ErrUnknownKey = errors.New("token: unknown key")
	ErrExpired    = errors.New("token: expired")
	ErrAudience   = errors.New("token: not for this host")
	ErrOp         = errors.New("token: operation not allowed")
)

var enc = base64.RawURLEncoding

// Key is one signing key: its ID and the private half.
type Key struct {
	Kid     string
	Private ed25519.PrivateKey
}

// Generate makes a fresh Ed25519 key under the given ID.
func Generate(kid string) (Key, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Key{}, err
	}

	return Key{Kid: kid, Private: priv}, nil
}

// Public is the public half of the key.
func (k Key) Public() ed25519.PublicKey {
	return k.Private.Public().(ed25519.PublicKey)
}

// Sign encodes and signs the claims. The kid in the claims is overwritten
// with the key's, so a caller cannot sign under another key's name.
func (k Key) Sign(c *api.TokenClaims) (string, error) {
	if k.Private == nil {
		return "", errors.New("token: no signing key")
	}

	c.SetKid(k.Kid)
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return "", err
	}

	sig := ed25519.Sign(k.Private, b)

	return enc.EncodeToString(b) + "." + enc.EncodeToString(sig), nil
}

// Verifier holds the public keys a host accepts.
//
// It is safe for concurrent use: a node updates it from its Watch of the key
// set while requests are being verified.
type Verifier struct {
	mu   sync.RWMutex
	keys map[string]ed25519.PublicKey

	// Skew is how far past exp a token is still accepted, for clocks that
	// disagree a little (token_skew, 1 minute).
	Skew time.Duration
	// Now is the clock, for tests.
	Now func() time.Time
}

// NewVerifier makes a verifier with no keys.
func NewVerifier() *Verifier {
	return &Verifier{keys: map[string]ed25519.PublicKey{}, Skew: time.Minute, Now: time.Now}
}

// Set replaces the key set.
func (v *Verifier) Set(keys map[string]ed25519.PublicKey) {
	cp := make(map[string]ed25519.PublicKey, len(keys))
	for k, p := range keys {
		cp[k] = p
	}

	v.mu.Lock()
	v.keys = cp
	v.mu.Unlock()
}

// Add puts one key in the set.
func (v *Verifier) Add(kid string, pub ed25519.PublicKey) {
	v.mu.Lock()
	v.keys[kid] = pub
	v.mu.Unlock()
}

// Remove takes one key out of the set.
func (v *Verifier) Remove(kid string) {
	v.mu.Lock()
	delete(v.keys, kid)
	v.mu.Unlock()
}

// Kids lists the key IDs held, which a heartbeat reports.
func (v *Verifier) Kids() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()

	vs := make([]string, 0, len(v.keys))
	for k := range v.keys {
		vs = append(vs, k)
	}

	return vs
}

// Len is how many keys are held.
func (v *Verifier) Len() int {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return len(v.keys)
}

// Parse decodes a token without verifying it, for logging and for tests.
func Parse(s string) (*api.TokenClaims, error) {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return nil, ErrMalformed
	}

	b, err := enc.DecodeString(s[:i])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	var c api.TokenClaims
	if err := proto.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	return &c, nil
}

// Verify checks the signature, the key, and the expiry, and answers the
// claims. The audience and the operation are checked by [Verifier.Check],
// which knows the host and the request; they are separate so a relay can
// verify a token and then decide what it allows.
func (v *Verifier) Verify(s string) (*api.TokenClaims, error) {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return nil, ErrMalformed
	}

	b, err := enc.DecodeString(s[:i])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	sig, err := enc.DecodeString(s[i+1:])
	if err != nil || len(sig) != ed25519.SignatureSize {
		return nil, ErrMalformed
	}

	var c api.TokenClaims
	if err := proto.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	v.mu.RLock()
	pub, ok := v.keys[c.GetKid()]
	v.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKey, c.GetKid())
	}

	if !ed25519.Verify(pub, b, sig) {
		return nil, ErrSignature
	}

	now := v.Now()
	if exp := c.GetExp(); exp == nil || now.After(exp.AsTime().Add(v.Skew)) {
		return nil, ErrExpired
	}

	return &c, nil
}

// Check is the rest of verification: the token names this host, and allows
// the operation. HEAD is allowed by either a put or a get token (§33.2).
func Check(c *api.TokenClaims, aud []byte, ops ...api.TokenOp) error {
	if string(c.GetAud()) != string(aud) {
		return ErrAudience
	}

	for _, op := range ops {
		if c.GetOp() == op {
			return nil
		}
	}

	return ErrOp
}

// FromHeader reads a token out of an Authorization header value, and
// answers "" when the header carries something else.
func FromHeader(v string) string {
	scheme, rest, ok := strings.Cut(strings.TrimSpace(v), " ")
	if !ok || !strings.EqualFold(scheme, Scheme) {
		return ""
	}

	return strings.TrimSpace(rest)
}
