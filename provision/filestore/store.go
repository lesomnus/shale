// Package filestore provides a single-host, cross-process durable provisioning
// store. All participating processes must share this private local filesystem.
package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
)

type Store struct{ dir string }

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, os.ErrInvalid
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("provisioning state requires a private directory")
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Lock(ctx context.Context, key string) (func() error, error) {
	f := flock.New(filepath.Join(s.dir, provision.Key("lock", key)+".lock"))
	ok, err := f.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ctx.Err()
	}
	var once sync.Once
	var unlockErr error
	return func() error { once.Do(func() { unlockErr = f.Unlock(); f.Close() }); return unlockErr }, nil
}

type claim struct {
	Tenant     string `json:"tenant"`
	Device     string `json:"device"`
	Binding    string `json:"binding"`
	Set        string `json:"set"`
	Producer   string `json:"producer"`
	Network    string `json:"network"`
	Alias      string `json:"alias"`
	Serial     string `json:"serial,omitempty"`
	Hardware   string `json:"hardware,omitempty"`
	MAC        string `json:"mac,omitempty"`
	PendingRun string `json:"pending_run,omitempty"`
}

func (s *Store) Reserve(ctx context.Context, q provision.Request) error {
	unlock, err := s.Lock(ctx, "inventory")
	if err != nil {
		return err
	}
	defer unlock()
	path := filepath.Join(s.dir, "inventory.json")
	var claims []claim
	if b, err := os.ReadFile(path); err == nil {
		if err = json.Unmarshal(b, &claims); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	c := claim{Tenant: q.Scope.TenantID, Device: string(q.Device.ID), Binding: q.BindingID, Set: q.Scope.SetID, Producer: q.Scope.ProducerID, Network: q.Scope.NetworkID, Alias: q.Alias}
	for _, v := range claims {
		if v.Tenant != c.Tenant {
			continue
		}
		if v.Device == c.Device || v.Binding == c.Binding {
			if v.PendingRun != "" && v.PendingRun != q.ID {
				return provision.ErrConflict
			}
			if v.Device != c.Device || v.Binding != c.Binding || v.Set != c.Set || v.Producer != c.Producer || v.Network != c.Network || v.Alias != c.Alias {
				return provision.ErrConflict
			}
			return nil
		}
	}
	claims = append(claims, c)
	b, err := json.Marshal(claims)
	if err != nil {
		return err
	}
	return WriteAtomic(path, b, 0600)
}

// BeginMutation pins an in-flight physical write to its original run. An HTTP
// timeout does not mean the camera stopped processing it. Another revision may
// not overtake that run merely because its process/file lock was released.
func (s *Store) BeginMutation(ctx context.Context, q provision.Request) error {
	return s.mutation(ctx, q, false)
}

// CompleteMutation is called after the owning run has reconciled its requested
// state, independently qualified media and observed committed capture. It is
// never called just because a worker lease/request expired.
func (s *Store) CompleteMutation(ctx context.Context, q provision.Request) error {
	return s.mutation(ctx, q, true)
}
func (s *Store) mutation(ctx context.Context, q provision.Request, complete bool) error {
	u, err := s.Lock(ctx, "inventory")
	if err != nil {
		return err
	}
	defer u()
	path := filepath.Join(s.dir, "inventory.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var claims []claim
	if err = json.Unmarshal(b, &claims); err != nil {
		return err
	}
	for i := range claims {
		c := &claims[i]
		if c.Tenant != q.Scope.TenantID || c.Device != string(q.Device.ID) {
			continue
		}
		if c.Binding != q.BindingID || c.Set != q.Scope.SetID || c.Producer != q.Scope.ProducerID || c.Network != q.Scope.NetworkID || c.Alias != q.Alias || (c.PendingRun != "" && c.PendingRun != q.ID) {
			return provision.ErrConflict
		}
		if complete {
			c.PendingRun = ""
		} else {
			c.PendingRun = q.ID
		}
		b, err = json.Marshal(claims)
		if err != nil {
			return err
		}
		return WriteAtomic(path, b, 0600)
	}
	return provision.ErrConflict
}

// Identify remembers authenticated physical evidence across desired revisions
// and DHCP changes. Reusing an address cannot silently replace a camera.
func (s *Store) Identify(ctx context.Context, scope camera.EnrollmentScope, device camera.DeviceHint) error {
	u, err := s.Lock(ctx, "inventory")
	if err != nil {
		return err
	}
	defer u()
	path := filepath.Join(s.dir, "inventory.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var claims []claim
	if err = json.Unmarshal(b, &claims); err != nil {
		return err
	}
	for i := range claims {
		c := &claims[i]
		if c.Tenant != scope.TenantID || c.Device != string(device.ID) {
			continue
		}
		if c.Serial != "" && c.Serial != device.Serial || c.Hardware != "" && c.Hardware != device.Hardware {
			return camera.ErrAmbiguousIdentity
		}
		mac := ""
		if device.MAC != "" {
			value, err := net.ParseMAC(device.MAC)
			if err != nil || len(value) != 6 {
				return camera.ErrInvalid
			}
			mac = value.String()
		}
		if c.MAC != "" && c.MAC != mac {
			return camera.ErrAmbiguousIdentity
		}
		if mac != "" {
			c.MAC = mac
		}
		if device.Serial != "" {
			c.Serial = device.Serial
		}
		if device.Hardware != "" {
			c.Hardware = device.Hardware
		}
		b, err = json.Marshal(claims)
		if err != nil {
			return err
		}
		return WriteAtomic(path, b, 0600)
	}
	return provision.ErrConflict
}

func (s *Store) path(id string) string { return filepath.Join(s.dir, "run-"+provision.Key(id)+".json") }
func (s *Store) Load(ctx context.Context, id string) (provision.Run, error) {
	if err := ctx.Err(); err != nil {
		return provision.Run{}, err
	}
	b, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return provision.Run{}, provision.ErrNotFound
	}
	if err != nil {
		return provision.Run{}, err
	}
	var r provision.Run
	err = json.Unmarshal(b, &r)
	return r, err
}
func (s *Store) Save(ctx context.Context, r provision.Run) error {
	unlock, err := s.Lock(ctx, "run/"+r.Request.ID)
	if err != nil {
		return err
	}
	defer unlock()
	old, err := s.Load(ctx, r.Request.ID)
	if errors.Is(err, provision.ErrNotFound) {
		if r.Version != 1 {
			return provision.ErrConflict
		}
	} else if err != nil {
		return err
	} else if old.Version+1 != r.Version {
		return provision.ErrConflict
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return WriteAtomic(s.path(r.Request.ID), b, 0600)
}
func (s *Store) List(ctx context.Context) ([]provision.Run, error) {
	paths, err := filepath.Glob(filepath.Join(s.dir, "run-*.json"))
	if err != nil {
		return nil, err
	}
	var runs []provision.Run
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var r provision.Run
		if err = json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// WriteAtomic syncs both the complete replacement and its directory entry. The
// temporary file is on the same filesystem; a reader sees either full version.
func WriteAtomic(path string, data []byte, mode os.FileMode) (err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".provision-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { f.Close(); os.Remove(name) }()
	if err = f.Chmod(mode); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
