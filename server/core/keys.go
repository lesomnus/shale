package core

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// Kek is the key-encryption key that wraps signing keys in the DB (§33.3).
// It lives in the CP's state directory or a Kubernetes Secret.
type Kek []byte

const kekFile = "kek"

// LoadKek reads the KEK from `dir`, or from the `SHALE_CONTROL_KEK`
// environment variable (base64) when set.
func LoadKek(dir string) (Kek, error) {
	if v := os.Getenv("SHALE_CONTROL_KEK"); v != "" {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("SHALE_CONTROL_KEK: %w", err)
		}
		if len(b) != 32 {
			return nil, errors.New("SHALE_CONTROL_KEK: not 32 bytes")
		}

		return Kek(b), nil
	}

	b, err := os.ReadFile(filepath.Join(dir, kekFile))
	if err != nil {
		return nil, err
	}
	if len(b) != 32 {
		return nil, fmt.Errorf("%s: not 32 bytes", filepath.Join(dir, kekFile))
	}

	return Kek(b), nil
}

// NewKek makes and stores a KEK, refusing to overwrite one.
func NewKek(dir string) (Kek, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	p := filepath.Join(dir, kekFile)
	if _, err := os.Stat(p); err == nil {
		return nil, fmt.Errorf("%s: already exists", p)
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	if err := os.WriteFile(p, b, 0o600); err != nil {
		return nil, err
	}

	return Kek(b), nil
}

// Wrap seals a private key: nonce || AES-256-GCM ciphertext.
func (k Kek) Wrap(priv []byte) ([]byte, error) {
	blk, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	return append(nonce, g.Seal(nil, nonce, priv, nil)...), nil
}

// Unwrap opens what Wrap sealed.
func (k Kek) Unwrap(b []byte) ([]byte, error) {
	blk, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}
	if len(b) < g.NonceSize() {
		return nil, errors.New("kek: sealed key too short")
	}

	return g.Open(nil, b[:g.NonceSize()], b[g.NonceSize():], nil)
}

// Keys is the CP's view of the SigningKey rows: every key unwrapped, and the
// one it signs with. Every CP process reads the same rows, so a rotation is
// one transaction and never leaves a replica signing with a key the others
// do not know (§33.3).
type Keys struct {
	kek Kek
	own api.Server
	now func() time.Time

	mu      sync.RWMutex
	at      time.Time
	signing *token.Key
	public  map[string]ed25519.PublicKey
}

const keysTTL = 30 * time.Second

// NewKeys makes the ring; it loads lazily.
func NewKeys(kek Kek, own api.Server, now func() time.Time) *Keys {
	if now == nil {
		now = time.Now
	}

	return &Keys{kek: kek, own: own, now: now, public: map[string]ed25519.PublicKey{}}
}

// Refresh reloads the rows.
func (k *Keys) Refresh(ctx context.Context) error {
	vs, err := k.own.SigningKey().List(ctx, api.SigningKeyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return err
	}

	pub := map[string]ed25519.PublicKey{}
	var signing *token.Key
	for _, v := range vs.GetItems() {
		if v.GetState() == api.SigningKeyState_SIGNING_KEY_STATE_RETIRED {
			continue
		}
		pub[v.GetAlias()] = ed25519.PublicKey(v.GetPublicKey())
		if v.GetState() == api.SigningKeyState_SIGNING_KEY_STATE_SIGNING && k.kek != nil {
			priv, err := k.kek.Unwrap(v.GetPrivateKey())
			if err != nil {
				return fmt.Errorf("signing key %s: %w", v.GetAlias(), err)
			}
			signing = &token.Key{Kid: v.GetAlias(), Private: ed25519.PrivateKey(priv)}
		}
	}

	k.mu.Lock()
	k.public, k.signing, k.at = pub, signing, k.now()
	k.mu.Unlock()

	return nil
}

func (k *Keys) fresh(ctx context.Context) error {
	k.mu.RLock()
	stale := k.at.IsZero() || k.now().Sub(k.at) > keysTTL
	k.mu.RUnlock()
	if !stale {
		return nil
	}

	return k.Refresh(ctx)
}

// Sign signs the claims with the current key.
func (k *Keys) Sign(ctx context.Context, c *api.TokenClaims) (string, error) {
	if err := k.fresh(ctx); err != nil {
		return "", err
	}

	k.mu.RLock()
	s := k.signing
	k.mu.RUnlock()
	if s == nil {
		return "", errors.New("keys: no signing key; run `shale init` or `shale signing-key rotate`")
	}

	return s.Sign(c)
}

// Public is the key set as hosts receive it (§33.3).
func (k *Keys) Public(ctx context.Context) ([]*api.KeyEntry, error) {
	if err := k.fresh(ctx); err != nil {
		return nil, err
	}

	k.mu.RLock()
	defer k.mu.RUnlock()

	var vs []*api.KeyEntry
	for kid, p := range k.public {
		vs = append(vs, api.KeyEntry_builder{
			Kid:       kid,
			PublicKey: []byte(p),
			Signing:   k.signing != nil && k.signing.Kid == kid,
		}.Build())
	}

	return vs, nil
}

// Verifier is a verifier over the current set, for the CP's own checks.
func (k *Keys) Verifier(ctx context.Context) (*token.Verifier, error) {
	if err := k.fresh(ctx); err != nil {
		return nil, err
	}

	v := token.NewVerifier()
	k.mu.RLock()
	v.Set(k.public)
	k.mu.RUnlock()

	return v, nil
}

// Create makes a new key row. `signing` makes it the one to sign with, and
// moves the previous signing key to ACTIVE; a rotation that waits for nodes
// creates it ACTIVE and promotes it later (§33.3).
func (k *Keys) Create(ctx context.Context, own api.Server, signing bool) (*api.SigningKey, error) {
	if k.kek == nil {
		return nil, errors.New("keys: no KEK")
	}

	kid := "k-" + aliasSuffix(pdid.New(DomSigningKey))
	key, err := token.Generate(kid)
	if err != nil {
		return nil, err
	}
	wrapped, err := k.kek.Wrap([]byte(key.Private))
	if err != nil {
		return nil, err
	}

	state := api.SigningKeyState_SIGNING_KEY_STATE_ACTIVE
	if signing {
		state = api.SigningKeyState_SIGNING_KEY_STATE_SIGNING
		if err := k.demote(ctx, own); err != nil {
			return nil, err
		}
	}

	v, err := own.SigningKey().Add(ctx, api.SigningKeyAddRequest_builder{
		Alias:      kid,
		PublicKey:  []byte(key.Public()),
		PrivateKey: wrapped,
		State:      state,
	}.Build())
	if err != nil {
		return nil, err
	}

	k.mu.Lock()
	k.at = time.Time{}
	k.mu.Unlock()

	return v, nil
}

// demote moves every SIGNING key to ACTIVE.
func (k *Keys) demote(ctx context.Context, own api.Server) error {
	vs, err := own.SigningKey().List(ctx, api.SigningKeyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return err
	}
	for _, v := range vs.GetItems() {
		if v.GetState() != api.SigningKeyState_SIGNING_KEY_STATE_SIGNING {
			continue
		}
		st := api.SigningKeyState_SIGNING_KEY_STATE_ACTIVE
		if _, err := own.SigningKey().Patch(ctx, api.SigningKeyPatchRequest_builder{
			Ref:         api.SigningKeyRef_builder{Id: v.GetId()}.Build(),
			State:       &st,
			DateUpdated: v.GetDateUpdated(),
		}.Build()); err != nil {
			return err
		}
	}

	return nil
}
