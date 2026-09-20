package hostagent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// KeyRing is the key set a host verifies tokens with (§33.3): what the CP
// handed over at adoption, then whatever `SigningKey` lists every 30 s,
// cached in the state directory so a host that restarts while the CP is
// down still verifies.
type KeyRing struct {
	Dir      string
	Verifier *token.Verifier
}

// NewKeyRing makes a ring over `dir` with a fresh verifier.
func NewKeyRing(dir string) *KeyRing {
	return &KeyRing{Dir: dir, Verifier: token.NewVerifier()}
}

func (k *KeyRing) file() string { return filepath.Join(k.Dir, "keys.json") }

// Load reads the cached key set.
func (k *KeyRing) Load() {
	b, err := os.ReadFile(k.file())
	if err != nil {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return
	}
	keys := map[string]ed25519.PublicKey{}
	for kid, v := range m {
		if pub, err := base64.StdEncoding.DecodeString(v); err == nil && len(pub) == ed25519.PublicKeySize {
			keys[kid] = ed25519.PublicKey(pub)
		}
	}
	k.Verifier.Set(keys)
}

// Apply installs a key set and caches it.
func (k *KeyRing) Apply(vs []*api.KeyEntry) {
	if len(vs) == 0 {
		return
	}
	keys := map[string]ed25519.PublicKey{}
	m := map[string]string{}
	for _, e := range vs {
		if len(e.GetPublicKey()) != ed25519.PublicKeySize {
			continue
		}
		keys[e.GetKid()] = ed25519.PublicKey(e.GetPublicKey())
		m[e.GetKid()] = base64.StdEncoding.EncodeToString(e.GetPublicKey())
	}
	k.Verifier.Set(keys)
	if b, err := json.Marshal(m); err == nil {
		os.WriteFile(k.file(), b, 0o600)
	}
}

// Kids lists the keys held, for the heartbeat.
func (k *KeyRing) Kids() []string { return k.Verifier.Kids() }

// Poll keeps the key set current until the context is done. A Watch names
// rows, and a host cannot name a key it has not heard of, so it lists
// instead; rotation waits for the heartbeat that reports the new key.
func (k *KeyRing) Poll(ctx context.Context, conn func(ctx context.Context) (*grpc.ClientConn, error), onErr func(error)) error {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		if c, err := conn(ctx); err == nil {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			resp, err := api.NewSigningKeyServiceClient(c).List(cctx, api.SigningKeyListRequest_builder{Size: 100}.Build())
			cancel()
			if err == nil {
				var vs []*api.KeyEntry
				for _, v := range resp.GetItems() {
					if v.GetState() == api.SigningKeyState_SIGNING_KEY_STATE_RETIRED {
						continue
					}
					vs = append(vs, api.KeyEntry_builder{Kid: v.GetAlias(), PublicKey: v.GetPublicKey(), Signing: v.GetState() == api.SigningKeyState_SIGNING_KEY_STATE_SIGNING}.Build())
				}
				k.Apply(vs)
			} else if onErr != nil {
				onErr(err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
