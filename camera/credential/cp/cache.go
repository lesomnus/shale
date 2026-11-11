package cp

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential/material"
)

const cacheAAD = "shale/camera-streaming-cache/v1"

// CacheOptions explicitly enables a bounded local streaming cache. KeyMaterial
// must come from this Producer's private machine key; it is never a CP KEK.
// A replacement key cannot open the old cache and must resolve online again.
type CacheOptions struct {
	Directory   string
	KeyMaterial []byte
	MaxAge      time.Duration
	MaxEntries  int
	Now         func() time.Time
}

// StreamingCache never caches initial/management accounts or creates grants.
// Only an online CP grant followed by matching actual-media AppliedProof can
// activate an entry. Current catalog/BeginApply authority remains independent.
type StreamingCache struct {
	*cacheState
}

type cacheState struct {
	mu      sync.Mutex
	dir     string
	aead    cipher.AEAD
	age     time.Duration
	limit   int
	now     func() time.Time
	pending map[string]cacheRecord
}

type cacheRecord struct {
	Source               camera.SourceAuthContext
	Ref                  camera.CredentialRef
	AssignmentID         string
	AssignmentGeneration int64
	Access               camera.AccessLevel
	Until                time.Time
	Material             []byte
	Epoch                string
}

// AppliedProof must originate in the current capture readloop after decoding
// initialization, an IDR and advancing video. A CP status/receipt is not proof.
// The runtime must fence superseded capture epochs before calling this method.
type AppliedProof struct {
	Source                    camera.SourceAuthContext
	Ref                       camera.CredentialRef
	AssignmentID              string
	AssignmentGeneration      int64
	CaptureEpoch              string
	FirstMediaAt, LastMediaAt time.Time
	Frames, Keyframes         int64
}

func NewStreamingCache(o CacheOptions) (*StreamingCache, error) {
	if !filepath.IsAbs(o.Directory) || len(o.KeyMaterial) < 32 || len(o.KeyMaterial) > 64<<10 || o.MaxAge < time.Second || o.MaxAge > 300*time.Second || o.MaxEntries < 0 || o.MaxEntries > 64 {
		return nil, camera.ErrInvalid
	}
	if o.MaxEntries == 0 {
		o.MaxEntries = 32
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if err := os.MkdirAll(o.Directory, 0700); err != nil {
		return nil, ErrUnavailable
	}
	if !privateCacheDir(o.Directory) {
		return nil, ErrUnavailable
	}
	key, err := hkdf.Key(sha256.New, o.KeyMaterial, nil, cacheAAD, 32)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrUnavailable
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &StreamingCache{cacheState: &cacheState{dir: o.Directory, aead: aead, age: o.MaxAge, limit: o.MaxEntries, now: o.Now, pending: map[string]cacheRecord{}}}, nil
}

func privateCacheDir(dir string) bool {
	st, err := os.Lstat(dir)
	return err == nil && st.IsDir() && st.Mode().Perm()&0077 == 0
}

func cachePrefix(sourceID string) string {
	h := sha256.Sum256([]byte(sourceID))
	return hex.EncodeToString(h[:16]) + "-"
}

func cacheName(source camera.SourceAuthContext, ref camera.CredentialRef) string {
	b, _ := json.Marshal(struct {
		Source camera.SourceAuthContext
		Ref    camera.CredentialRef
	}{source, ref})
	h := sha256.Sum256(b)
	return cachePrefix(source.SourceID) + hex.EncodeToString(h[:]) + ".sealed"
}

func (c *StreamingCache) remember(source camera.SourceAuthContext, ref camera.CredentialRef, response *api.CameraCredentialLease) {
	if c == nil || c.cacheState == nil || source.SourceID == "" || source.Revision <= 0 || ref.Purpose != camera.AuthStreaming || !ref.Authorizes(source.Scope, camera.AuthStreaming) || response.GetCacheUntil() == nil || response.GetCacheUntil().CheckValid() != nil {
		return
	}
	until := response.GetCacheUntil().AsTime()
	now := c.now()
	if !until.After(now) || until.After(now.Add(c.age)) || len(response.GetMaterial()) > 64<<10 {
		return
	}
	d := response.GetDelivery()
	assignment, err := id(d.GetAssignmentId(), 28, false)
	if err != nil || source.AssignmentID != assignment || source.AssignmentGeneration <= 0 || source.AssignmentGeneration != d.GetAssignmentGeneration() || d.GetSourceRevision() != source.Revision {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheName(source, ref)
	for name, old := range c.pending {
		if !old.Until.After(now) {
			clear(old.Material)
			delete(c.pending, name)
		}
	}
	if old, ok := c.pending[key]; ok {
		clear(old.Material)
	}
	if _, ok := c.pending[key]; !ok && len(c.pending) >= c.limit {
		return
	}
	c.pending[key] = cacheRecord{Source: source, Ref: ref, AssignmentID: assignment, AssignmentGeneration: d.GetAssignmentGeneration(), Access: camera.AccessLevel(response.GetAccess()), Until: until, Material: append([]byte(nil), response.GetMaterial()...)}
}

func (c *StreamingCache) RecordApplied(ctx context.Context, p AppliedProof) error {
	if c == nil || c.cacheState == nil || ctx.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrUnavailable
	}
	if p.CaptureEpoch == "" || len(p.CaptureEpoch) > 256 || p.Frames < 2 || p.Keyframes < 1 || p.FirstMediaAt.IsZero() || !p.LastMediaAt.After(p.FirstMediaAt) {
		return ErrInvalidLease
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	name := cacheName(p.Source, p.Ref)
	r, ok := c.pending[name]
	if !ok || r.Source != p.Source || r.Ref != p.Ref || r.AssignmentID != p.AssignmentID || r.AssignmentGeneration != p.AssignmentGeneration || !r.Until.After(c.now()) {
		return ErrInvalidLease
	}
	delete(c.pending, name)
	defer clear(r.Material)
	r.Epoch = p.CaptureEpoch
	if !privateCacheDir(c.dir) {
		return ErrUnavailable
	}
	files, err := os.ReadDir(c.dir)
	if err != nil {
		return ErrUnavailable
	}
	count := 0
	exists := false
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sealed") {
			// Authenticate expiry before pruning. A forged/tampered file cannot
			// supply a trusted expiry; bounded refusal then needs cleanup.
			if old, err := c.readRecord(file.Name()); err == nil {
				clear(old.Material)
				if !old.Until.After(c.now()) {
					if os.Remove(filepath.Join(c.dir, file.Name())) == nil {
						continue
					}
				}
			}
			count++
			exists = exists || file.Name() == name
		}
	}
	if !exists && count >= c.limit {
		return ErrUnavailable
	}
	plain, err := json.Marshal(r)
	if err != nil {
		return ErrUnavailable
	}
	defer clear(plain)
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return ErrUnavailable
	}
	sealed := c.aead.Seal(nonce, nonce, plain, []byte(cacheAAD))
	f, err := os.CreateTemp(c.dir, ".stream-")
	if err != nil {
		return ErrUnavailable
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err = f.Write(sealed); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return ErrUnavailable
	}
	if err = os.Rename(path, filepath.Join(c.dir, name)); err != nil {
		return ErrUnavailable
	}
	dir, err := os.Open(c.dir)
	if err != nil {
		return ErrUnavailable
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (c *StreamingCache) recover(ctx context.Context, source camera.SourceAuthContext, ref camera.CredentialRef, access camera.AccessLevel, ttl time.Duration) (camera.CredentialLease, error) {
	if c == nil || c.cacheState == nil || ctx.Err() != nil || ref.Purpose != camera.AuthStreaming || !ref.Authorizes(source.Scope, camera.AuthStreaming) || source.SourceID == "" || source.Revision <= 0 || source.AssignmentID == "" || source.AssignmentGeneration <= 0 {
		return nil, ErrUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !privateCacheDir(c.dir) {
		return nil, ErrUnavailable
	}
	path := filepath.Join(c.dir, cacheName(source, ref))
	r, err := c.readRecord(cacheName(source, ref))
	if err != nil {
		return nil, err
	}
	defer clear(r.Material)
	now := c.now()
	if r.Source != source || r.Ref != ref || r.Epoch == "" || r.AssignmentID == "" || r.AssignmentGeneration <= 0 || !r.Until.After(now) || r.Until.After(now.Add(c.age)) || rank(r.Access) < rank(access) {
		_ = os.Remove(path)
		return nil, ErrUnavailable
	}
	value, err := material.Parse(r.Material)
	if err != nil {
		return nil, ErrUnavailable
	}
	expires := now.Add(ttl)
	if expires.After(r.Until) {
		expires = r.Until
	}
	var lease camera.CredentialLease
	err = value.Use(ctx, func(m camera.CredentialMaterial) error {
		lease = camera.NewCredentialLease(ref, m, expires)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func (c *StreamingCache) readRecord(name string) (cacheRecord, error) {
	var r cacheRecord
	path := filepath.Join(c.dir, name)
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 128<<10 {
		return r, ErrUnavailable
	}
	sealed, err := os.ReadFile(path)
	if err != nil || len(sealed) < c.aead.NonceSize() {
		return r, ErrUnavailable
	}
	plain, err := c.aead.Open(nil, sealed[:c.aead.NonceSize()], sealed[c.aead.NonceSize():], []byte(cacheAAD))
	if err != nil {
		return r, ErrUnavailable
	}
	defer clear(plain)
	if json.Unmarshal(plain, &r) != nil {
		clear(r.Material)
		return cacheRecord{}, ErrUnavailable
	}
	return r, nil
}

// InvalidateSource is called on stop/supersede and on an online denial. It
// removes pending grants and accepted disk entries; it does not close RTSP
// sessions already established with a camera's long-lived account.
func (c *StreamingCache) InvalidateSource(sourceID string) error {
	if c == nil {
		return nil
	}
	if c.cacheState == nil {
		return ErrUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, r := range c.pending {
		if r.Source.SourceID == sourceID {
			clear(r.Material)
			delete(c.pending, name)
		}
	}
	if !privateCacheDir(c.dir) {
		return ErrUnavailable
	}
	files, err := os.ReadDir(c.dir)
	if err != nil {
		return ErrUnavailable
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), cachePrefix(sourceID)) && strings.HasSuffix(file.Name(), ".sealed") {
			if err := os.Remove(filepath.Join(c.dir, file.Name())); err != nil {
				return ErrUnavailable
			}
		}
	}
	return nil
}

const cacheMarker = "[encrypted streaming credential cache]"

func (StreamingCache) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, cacheMarker) }
func (StreamingCache) MarshalJSON() ([]byte, error) { return []byte(`"` + cacheMarker + `"`), nil }
func (StreamingCache) MarshalYAML() (any, error)    { return cacheMarker, nil }

func (CacheOptions) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, cacheMarker) }
func (CacheOptions) MarshalJSON() ([]byte, error) { return []byte(`"` + cacheMarker + `"`), nil }
func (CacheOptions) MarshalYAML() (any, error)    { return cacheMarker, nil }
