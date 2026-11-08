package cameracredential

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/server/bare"
	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
)

type sqlRecorder struct {
	mu      sync.Mutex
	methods []string
	fail    bool
}

func (r *sqlRecorder) Record(_ context.Context, s bare.Server, c bare.Change) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !dialect.InTx(s.Db.Driver()) {
		return errors.New("recorder escaped the supplied transaction")
	}
	r.methods = append(r.methods, c.By)
	if r.fail && strings.Contains(c.By, "CameraCredentialVersion") {
		return errors.New("recorder failure must not reveal rtsp://camera:fixture-secret@192.0.2.1/")
	}
	return nil
}

type sqlFixture struct {
	t        *testing.T
	db       *ent.Client
	drv      dialect.Driver
	server   api.Server
	factory  StoreFactory
	subject  Subject
	recorder *sqlRecorder
	kek      []byte
	dsn      string
}

func newSQLFixture(t *testing.T) *sqlFixture {
	t.Helper()
	f := &sqlFixture{t: t, recorder: &sqlRecorder{}, kek: []byte("01234567890123456789012345678901"), dsn: "file:" + filepath.Join(t.TempDir(), "credentials.db") + "?_pragma=foreign_keys(1)"}
	f.open()
	t.Cleanup(func() { require.NoError(t, f.db.Close()) })
	ctx := t.Context()
	tenant, device, network := pdid.New(1), pdid.New(27), pdid.New(26)
	_, err := f.server.Tenant().Add(ctx, api.TenantAddRequest_builder{Id: tenant.Bytes(), Alias: "tenant"}.Build())
	require.NoError(t, err)
	_, err = f.server.CameraNetwork().Add(ctx, api.CameraNetworkAddRequest_builder{Id: network.Bytes(), Tenant: api.TenantRef_builder{Id: tenant.Bytes()}.Build(), Alias: "lan"}.Build())
	require.NoError(t, err)
	_, err = f.server.CameraDevice().Add(ctx, api.CameraDeviceAddRequest_builder{Id: device.Bytes(), Tenant: api.TenantRef_builder{Id: tenant.Bytes()}.Build(), Network: api.CameraNetworkRef_builder{Id: network.Bytes()}.Build(), Alias: "camera", DeviceKey: "physical-device", ObservationRevision: 7}.Build())
	require.NoError(t, err)
	f.subject = Subject{TenantID: tenant, DeviceID: device, Purpose: camerasecret.InitialAdmin}
	return f
}

func (f *sqlFixture) open() {
	f.t.Helper()
	db, name, err := (config.DbConfig{Driver: "sqlite3", Dsn: f.dsn}).Open(f.t.Context())
	require.NoError(f.t, err)
	// With one connection, a Store that captures a pool instead of rebinding
	// cannot accidentally pass: its own transaction occupies that connection.
	db.SetMaxOpenConns(1)
	f.drv = sql.OpenDB(name, db)
	f.db = enttest.NewClient(f.t, enttest.WithOptions(ent.Driver(f.drv)))
	server, err := bare.NewServer(f.db, bare.WithRecorder(f.recorder))
	require.NoError(f.t, err)
	f.server = server
	backend, err := camerasecret.NewAESGCM(f.kek)
	require.NoError(f.t, err)
	f.factory = NewSQLFactory(server, backend, backend, SQLOptions{})
}

func (f *sqlFixture) tx(fn func(Store) error) error {
	ctx, cancel := context.WithTimeout(f.t.Context(), 5*time.Second)
	defer cancel()
	drv, tx, err := dialect.BeginTx(ctx, f.drv)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	store, err := f.factory(drv)
	if err == nil {
		err = fn(store)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (f *sqlFixture) prepare(key, username string, reuse *Reference) Preparation {
	f.t.Helper()
	q, err := NewPreparation(Intent{Subject: f.subject, Key: key, Access: camera.AccessAdministrator}, []byte(username), reuse)
	require.NoError(f.t, err)
	return q
}

func sqlMaterial(t *testing.T, username, password string) Material {
	t.Helper()
	m, err := NewMaterial(username, password)
	require.NoError(t, err)
	return m
}

func TestSQLStoreDurablePrepareReplayReuseAndRestart(t *testing.T) {
	f := newSQLFixture(t)
	q := f.prepare("activate-1", "admin", nil)
	material := sqlMaterial(t, "admin", "generated-fixture-password")
	var generated int
	generate := func(context.Context) (Material, error) { generated++; return material, nil }
	var first, same, reused Version
	before, err := f.db.CameraDevice.Get(t.Context(), f.subject.DeviceID.Uuid())
	require.NoError(t, err)
	require.NoError(t, f.tx(func(s Store) (err error) { first, err = s.Prepare(t.Context(), q, generate); return err }))
	require.NoError(t, f.tx(func(s Store) (err error) { same, err = s.Prepare(t.Context(), q, generate); return err }))
	require.Equal(t, first, same)
	require.Equal(t, Generated, first.Origin)
	require.Equal(t, 1, generated)
	q2 := f.prepare("second-profile", "new-account-option-only", nil)
	require.NoError(t, f.tx(func(s Store) (err error) { reused, err = s.Prepare(t.Context(), q2, generate); return err }))
	require.Equal(t, Reused, reused.Origin)
	require.Equal(t, first.Ref.CredentialID, reused.Ref.CredentialID)
	require.NotEqual(t, first.Ref.Version, reused.Ref.Version, "every intent must have its own durable ledger row")
	require.Equal(t, 1, generated, "adding a profile must not allocate or rotate a password")
	after, err := f.db.CameraDevice.Get(t.Context(), f.subject.DeviceID.Uuid())
	require.NoError(t, err)
	require.Equal(t, before.ObservationRevision, after.ObservationRevision)
	require.Equal(t, before.DateUpdated, after.DateUpdated, "SQLite locking must not change camera observations")
	rows, err := f.db.CameraCredentialVersion.Query().All(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, rows[0].RequestHash, rows[1].RequestHash, "unkeyed metadata hash must not include either requested username")
	for _, row := range rows {
		require.NotContains(t, string(row.SealedEnvelope), "generated-fixture-password")
		require.Len(t, row.RequestHash, 32)
		require.NotNil(t, row.PrepareTag)
	}
	require.NoError(t, f.db.Close())
	f.open() // new driver, server, factory and KEK-derived backend; no process state
	for _, ref := range []Reference{first.Ref, reused.Ref} {
		require.NoError(t, f.tx(func(s Store) error {
			got, err := s.Resolve(t.Context(), ref)
			if err == nil {
				require.Equal(t, material, got, "old exact pins must survive restart and a newer current version")
			}
			return err
		}))
	}
	qChanged := f.prepare("activate-1", "changed-username", nil)
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), qChanged, generate); return err }), ErrConflict)
	require.Equal(t, 1, generated)
}

func TestSQLStoreImportExactPayloadStaleReplayAndOldPins(t *testing.T) {
	f := newSQLFixture(t)
	original := sqlMaterial(t, "custom-\xff-username", "original-\xfe-password")
	q := ImportRequest{Intent: Intent{Subject: f.subject, Key: "migration", Access: camera.AccessAdministrator}, Material: original}
	legacy := camera.CredentialRef{Provider: "standalone", ID: "old", Version: "1", Purpose: camera.AuthInitialAdmin, Recoverable: true, Scope: camera.EnrollmentScope{TenantID: "old-tenant", SiteID: "old-site", SetID: "old-set", ProducerID: "old-producer", NetworkID: "old-network"}}
	q.Legacy = &legacy
	var first, changed, replay Version
	require.NoError(t, f.tx(func(s Store) (err error) { first, err = s.Import(t.Context(), q); return err }))
	replacement := sqlMaterial(t, "another-username", "new-password")
	q2 := ImportRequest{Intent: Intent{Subject: f.subject, Key: "explicit-account-change", Access: camera.AccessAdministrator}, Material: replacement, ExpectedCurrent: &first.Ref}
	require.NoError(t, f.tx(func(s Store) (err error) { changed, err = s.Import(t.Context(), q2); return err }))
	require.NoError(t, f.tx(func(s Store) (err error) { replay, err = s.Import(t.Context(), q2); return err }))
	require.Equal(t, changed, replay, "a lost-response retry validates its existing intent before stale ExpectedCurrent")
	for _, pair := range []struct {
		ref      Reference
		material Material
	}{{first.Ref, original}, {changed.Ref, replacement}} {
		require.NoError(t, f.tx(func(s Store) error {
			got, err := s.Resolve(t.Context(), pair.ref)
			require.Equal(t, pair.material, got)
			return err
		}))
	}
	q.Material = replacement
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Import(t.Context(), q); return err }), ErrConflict)
	q2.Intent.Key = "stale-new-intent"
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Import(t.Context(), q2); return err }), ErrConflict)
	q2.ExpectedCurrent = nil
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Import(t.Context(), q2); return err }), ErrConflict)
	q2.ExpectedCurrent, q2.Legacy = &changed.Ref, &legacy
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Import(t.Context(), q2); return err }), ErrConflict, "an original standalone reference cannot be remapped under a different intent")
}

func TestSQLStoreRollbackAndFailureBoundaries(t *testing.T) {
	f := newSQLFixture(t)
	q := f.prepare("first", "admin", nil)
	material := sqlMaterial(t, "admin", "fixture-secret")
	generate := func(context.Context) (Material, error) { return material, nil }
	_, err := f.factory(f.drv)
	require.ErrorIs(t, err, ErrUnavailable, "a pool is not an accepted persistence boundary")
	require.NoError(t, f.tx(func(s Store) error {
		for _, verb := range []string{"%v", "%+v", "%#v", "%q", "%x"} {
			require.Equal(t, sqlStoreMarker, fmt.Sprintf(verb, s), "formatting must not descend into the backend's key state")
		}
		return nil
	}))
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), q, nil); return err }), ErrNotFound)
	require.Zero(t, f.db.CameraCredential.Query().CountX(t.Context()))
	f.recorder.fail = true
	err = f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), q, generate); return err })
	require.ErrorIs(t, err, ErrUnavailable)
	require.NotContains(t, err.Error(), "fixture-secret")
	require.Zero(t, f.db.CameraCredential.Query().CountX(t.Context()), "failed version insert must roll back a new subject")
	require.Zero(t, f.db.CameraCredentialVersion.Query().CountX(t.Context()))
	f.recorder.fail = false
	rollback := errors.New("caller rolled back its composed operation")
	require.ErrorIs(t, f.tx(func(s Store) error {
		_, err := s.Prepare(t.Context(), q, generate)
		if err != nil {
			return err
		}
		return rollback
	}), rollback)
	require.Zero(t, f.db.CameraCredential.Query().CountX(t.Context()), "Store must not commit independently of its caller")
	for _, failure := range []error{errors.New("rtsp://admin:fixture-secret@192.0.2.1/"), errors.Join(ErrInput, errors.New("fixture-secret"))} {
		err := f.tx(func(s Store) error {
			_, err := s.Prepare(t.Context(), q, func(context.Context) (Material, error) { return Material{}, failure })
			return err
		})
		require.Equal(t, ErrUnavailable, err)
	}
	err = f.tx(func(s Store) error {
		_, err := s.Prepare(t.Context(), q, func(context.Context) (Material, error) { return sqlMaterial(t, "wrong", "secret"), nil })
		return err
	})
	require.Equal(t, ErrUnavailable, err)
	require.ErrorIs(t, f.tx(func(s Store) error {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := s.Prepare(ctx, q, generate)
		return err
	}), context.Canceled)
}

type cancelOnOpen struct {
	camerasecret.Backend
	cancel context.CancelFunc
}

func (b cancelOnOpen) Open(_ context.Context, c camerasecret.Context, envelope camerasecret.Envelope) ([]byte, error) {
	payload, err := b.Backend.Open(context.Background(), c, envelope)
	b.cancel() // simulate an injected backend that returns after its grant ended
	return payload, err
}

func TestSQLStoreResolveDoesNotEmitMaterialAfterCancellation(t *testing.T) {
	f := newSQLFixture(t)
	m := sqlMaterial(t, "admin", "cancellation-fixture-password")
	q := ImportRequest{Intent: Intent{Subject: f.subject, Key: "import", Access: camera.AccessAdministrator}, Material: m}
	var v Version
	require.NoError(t, f.tx(func(s Store) (err error) { v, err = s.Import(t.Context(), q); return err }))
	backend, err := camerasecret.NewAESGCM(f.kek)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.factory = NewSQLFactory(f.server, cancelOnOpen{Backend: backend, cancel: cancel}, backend, SQLOptions{})
	require.ErrorIs(t, f.tx(func(s Store) error { got, err := s.Resolve(ctx, v.Ref); require.Equal(t, Material{}, got); return err }), context.Canceled)
}

type afterOpen struct {
	camerasecret.Backend
	after func(context.Context) error
}

func (b afterOpen) Open(ctx context.Context, c camerasecret.Context, envelope camerasecret.Envelope) ([]byte, error) {
	payload, err := b.Backend.Open(ctx, c, envelope)
	if err == nil {
		err = b.after(ctx)
	}
	return payload, err
}

func TestSQLStoreResolveRechecksRevocationAfterBackend(t *testing.T) {
	for _, row := range []string{"credential", "version", "device"} {
		t.Run(row, func(t *testing.T) {
			f := newSQLFixture(t)
			m := sqlMaterial(t, "admin", "recheck-fixture-password")
			q := ImportRequest{Intent: Intent{Subject: f.subject, Key: "import", Access: camera.AccessAdministrator}, Material: m}
			var v Version
			require.NoError(t, f.tx(func(s Store) (err error) { v, err = s.Import(t.Context(), q); return err }))
			backend, err := camerasecret.NewAESGCM(f.kek)
			require.NoError(t, err)
			f.factory = func(drv dialect.Driver) (Store, error) {
				inTx := ent.NewClient(ent.Driver(drv))
				changing := afterOpen{Backend: backend, after: func(ctx context.Context) error {
					if row == "device" {
						return inTx.CameraDevice.UpdateOneId(f.subject.DeviceID.Uuid()).SetDateErased(time.Now()).Exec(ctx)
					}
					if row == "credential" {
						return inTx.CameraCredential.Update().Where(cameracredential.IdEQ(v.Ref.CredentialID.Uuid())).SetDateRevoked(time.Now()).Exec(ctx)
					}
					return inTx.CameraCredentialVersion.Update().Where(cameracredentialversion.VersionEQ(v.Ref.Version)).SetDateRevoked(time.Now()).Exec(ctx)
				}}
				return NewSQLFactory(f.server, changing, backend, SQLOptions{})(drv)
			}
			want := ErrRevoked
			if row == "device" {
				want = ErrNotFound
			}
			require.ErrorIs(t, f.tx(func(s Store) error {
				got, err := s.Resolve(t.Context(), v.Ref)
				require.Equal(t, Material{}, got)
				return err
			}), want)
		})
	}
}

func TestSQLStoreConcurrentSameIntentGeneratesOnce(t *testing.T) {
	f := newSQLFixture(t)
	q := f.prepare("one-intent", "admin", nil)
	material := sqlMaterial(t, "admin", "concurrent-fixture-password")
	var generated atomic.Int64
	const n = 6
	f.drv.(*sql.Driver).DB().SetMaxOpenConns(n)
	values, failures := make([]Version, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			failures[i] = f.tx(func(s Store) (err error) {
				values[i], err = s.Prepare(t.Context(), q, func(context.Context) (Material, error) { generated.Add(1); return material, nil })
				return err
			})
		})
	}
	wg.Wait()
	for i := range n {
		require.NoError(t, failures[i])
		require.Equal(t, values[0], values[i])
	}
	require.EqualValues(t, 1, generated.Load())
	require.Equal(t, 1, f.db.CameraCredential.Query().CountX(t.Context()))
	require.Equal(t, 1, f.db.CameraCredentialVersion.Query().CountX(t.Context()))
}

func TestSQLStoreCrossPurposeReuseCannotReplaceOrEscalate(t *testing.T) {
	f := newSQLFixture(t)
	material := sqlMaterial(t, "imported-\xff-username", "reused-fixture-password")
	var initial, stream, replay Version
	q := ImportRequest{Intent: Intent{Subject: f.subject, Key: "import", Access: camera.AccessAdministrator}, Material: material}
	require.NoError(t, f.tx(func(s Store) (err error) { initial, err = s.Import(t.Context(), q); return err }))
	target := f.subject
	target.Purpose = camerasecret.Streaming
	intent := Intent{Subject: target, Key: "first-profile", Access: camera.AccessRead}
	prepare, err := NewPreparation(intent, []byte("new-account-only-username"), &initial.Ref)
	require.NoError(t, err)
	require.NoError(t, f.tx(func(s Store) (err error) { stream, err = s.Prepare(t.Context(), prepare, nil); return err }))
	require.Equal(t, Reused, stream.Origin)
	require.NotEqual(t, initial.Ref.CredentialID, stream.Ref.CredentialID)
	require.NoError(t, f.tx(func(s Store) (err error) { replay, err = s.Prepare(t.Context(), prepare, nil); return err }))
	require.Equal(t, stream, replay, "same-intent explicit-reuse replay comes before current-version comparison")
	require.NoError(t, f.tx(func(s Store) error {
		got, err := s.Resolve(t.Context(), stream.Ref)
		require.Equal(t, material, got)
		return err
	}))
	intent.Key = "cannot-replace"
	otherReuse, err := NewPreparation(intent, []byte("admin"), &initial.Ref)
	require.NoError(t, err)
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), otherReuse, nil); return err }), ErrConflict)
	intent.Key, intent.Access = "cannot-escalate", camera.AccessAdministrator
	escalate, err := NewPreparation(intent, []byte("admin"), nil)
	require.NoError(t, err)
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), escalate, nil); return err }), ErrConflict)
}

func TestSQLStoreRejectsCrossTenantSite(t *testing.T) {
	f := newSQLFixture(t)
	tenant2, siteID := pdid.New(1), pdid.New(19)
	_, err := f.server.Tenant().Add(t.Context(), api.TenantAddRequest_builder{Id: tenant2.Bytes(), Alias: "other"}.Build())
	require.NoError(t, err)
	_, err = f.server.Site().Add(t.Context(), api.SiteAddRequest_builder{Id: siteID.Bytes(), Tenant: api.TenantRef_builder{Id: tenant2.Bytes()}.Build(), Alias: "other-site"}.Build())
	require.NoError(t, err)
	d, err := f.db.CameraDevice.Get(t.Context(), f.subject.DeviceID.Uuid())
	require.NoError(t, err)
	deviceID := pdid.New(27)
	// Bare intentionally does not provide the Core's graph checks. This
	// malformed private row must still fail the Store's own scope boundary.
	_, err = f.server.CameraDevice().Add(t.Context(), api.CameraDeviceAddRequest_builder{Id: deviceID.Bytes(), Tenant: api.TenantRef_builder{Id: f.subject.TenantID.Bytes()}.Build(), Site: api.SiteRef_builder{Id: siteID.Bytes()}.Build(), Network: api.CameraNetworkRef_builder{Id: d.NetworkId[:]}.Build(), Alias: "malformed", DeviceKey: "malformed"}.Build())
	require.NoError(t, err)
	f.subject.DeviceID = deviceID
	q := f.prepare("attempt", "admin", nil)
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), q, nil); return err }), ErrNotFound)
	require.Zero(t, f.db.CameraCredential.Query().CountX(t.Context()))
}

func TestSQLStoreReplayRequiresRecoverableStoredPayload(t *testing.T) {
	for _, kind := range []string{"prepare", "import"} {
		for _, damage := range []string{"wrong-key", "tampered-envelope"} {
			t.Run(kind+"/"+damage, func(t *testing.T) {
				f := newSQLFixture(t)
				m := sqlMaterial(t, "admin", "recoverable-fixture-password")
				p := f.prepare("same-intent", "admin", nil)
				i := ImportRequest{Intent: p.Intent, Material: m}
				generated := 0
				var result Version
				call := func(s Store) (err error) {
					if kind == "prepare" {
						result, err = s.Prepare(t.Context(), p, func(context.Context) (Material, error) { generated++; return m, nil })
					} else {
						result, err = s.Import(t.Context(), i)
					}
					return err
				}
				require.NoError(t, f.tx(call))
				original := result
				if damage == "wrong-key" {
					wrong, err := camerasecret.NewAESGCM([]byte("11234567890123456789012345678901"))
					require.NoError(t, err)
					f.factory = NewSQLFactory(f.server, wrong, wrong, SQLOptions{})
				} else {
					q, args := sql.Dialect(f.drv.Dialect()).Update(cameracredentialversion.Table).Set(cameracredentialversion.FieldSealedEnvelope, []byte("damaged-envelope")).Where(sql.EQ(cameracredentialversion.FieldVersion, original.Ref.Version)).Query()
					require.NoError(t, f.drv.Exec(t.Context(), q, args, nil))
				}
				require.ErrorIs(t, f.tx(call), ErrUnavailable)
				require.Equal(t, Version{}, result, "damaged state must not emit a replacement or successful ref")
				require.Equal(t, 1, f.db.CameraCredentialVersion.Query().CountX(t.Context()))
				if kind == "prepare" {
					require.Equal(t, 1, generated, "recovery failure must not manufacture another password")
				}
				require.ErrorIs(t, f.tx(func(s Store) error {
					got, err := s.Resolve(t.Context(), original.Ref)
					require.Equal(t, Material{}, got)
					return err
				}), ErrUnavailable)
			})
		}
	}
}

func TestSQLStoreRevocationAndExactScope(t *testing.T) {
	f := newSQLFixture(t)
	q := f.prepare("prepare", "admin", nil)
	material := sqlMaterial(t, "admin", "revocation-fixture-password")
	var first Version
	require.NoError(t, f.tx(func(s Store) (err error) {
		first, err = s.Prepare(t.Context(), q, func(context.Context) (Material, error) { return material, nil })
		return err
	}))
	for _, edit := range []func(*Reference){func(r *Reference) { r.Subject.TenantID = pdid.New(1) }, func(r *Reference) { r.CredentialID = pdid.New(32) }, func(r *Reference) { r.Version = "missing-version" }, func(r *Reference) { r.Subject.Purpose = camerasecret.Management }} {
		ref := first.Ref
		edit(&ref)
		require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Resolve(t.Context(), ref); return err }), ErrNotFound)
	}
	require.NoError(t, f.db.CameraCredentialVersion.Update().Where(cameracredentialversion.VersionEQ(first.Ref.Version)).SetDateRevoked(time.Now()).Exec(t.Context()))
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Prepare(t.Context(), q, nil); return err }), ErrRevoked)
	require.ErrorIs(t, f.tx(func(s Store) error {
		_, err := s.Prepare(t.Context(), f.prepare("new-intent", "admin", nil), nil)
		return err
	}), ErrRevoked)
	qImport := ImportRequest{Intent: Intent{Subject: f.subject, Key: "explicit-recovery", Access: camera.AccessAdministrator}, ExpectedCurrent: &first.Ref, Material: material}
	var recovered Version
	require.NoError(t, f.tx(func(s Store) (err error) { recovered, err = s.Import(t.Context(), qImport); return err }))
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Resolve(t.Context(), first.Ref); return err }), ErrRevoked)
	require.NoError(t, f.tx(func(s Store) error { _, err := s.Resolve(t.Context(), recovered.Ref); return err }))
	require.NoError(t, f.db.CameraCredential.Update().Where(cameracredential.IdEQ(first.Ref.CredentialID.Uuid())).SetDateRevoked(time.Now()).Exec(t.Context()))
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Resolve(t.Context(), recovered.Ref); return err }), ErrRevoked)
	require.ErrorIs(t, f.tx(func(s Store) error { _, err := s.Import(t.Context(), qImport); return err }), ErrRevoked)
	require.NotEmpty(t, f.recorder.methods)
	require.Contains(t, fmt.Sprint(f.recorder.methods), "CameraCredentialVersion")
}
