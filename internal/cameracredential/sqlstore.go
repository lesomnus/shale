package cameracredential

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/site"
	"github.com/lesomnus/shale/internal/ent/tenant"
	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/protobuf-orm/protoc-gen-orm-ent/runtime/enttx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SQLOptions struct{ Now func() time.Time }

// NewSQLFactory retains only the recorded stack and cryptographic services.
// Every instance is rebound onto the supplied transaction, including writes;
// it neither starts a transaction nor commits a reference independently.
func NewSQLFactory(recorded api.Server, backend camerasecret.Backend, replay camerasecret.ImportReplay, opts SQLOptions) StoreFactory {
	return func(drv dialect.Driver) (Store, error) {
		if drv == nil || !dialect.InTx(drv) || recorded == nil || backend == nil || replay == nil {
			return nil, ErrUnavailable
		}
		next, err := enttx.Rebind(recorded, drv)
		if err != nil {
			return nil, ErrUnavailable
		}
		now := opts.Now
		if now == nil {
			now = time.Now
		}
		return &sqlStore{drv: drv, db: ent.NewClient(ent.Driver(drv)), recorded: next, backend: backend, replay: replay, now: now}, nil
	}
}

type sqlStore struct {
	drv      dialect.Driver
	db       *ent.Client
	recorded api.Server
	backend  camerasecret.Backend
	replay   camerasecret.ImportReplay
	now      func() time.Time
}

const sqlStoreMarker = "[camera credential SQL store]"

func (*sqlStore) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, sqlStoreMarker) }
func (*sqlStore) String() string               { return sqlStoreMarker }
func (*sqlStore) GoString() string             { return sqlStoreMarker }
func (*sqlStore) MarshalJSON() ([]byte, error) { return []byte(`"` + sqlStoreMarker + `"`), nil }
func (*sqlStore) MarshalYAML() (any, error)    { return sqlStoreMarker, nil }

func (s *sqlStore) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.drv == nil || !dialect.InTx(s.drv) || s.db == nil || s.recorded == nil || s.backend == nil || s.replay == nil {
		return ErrUnavailable
	}
	return nil
}

func sqlFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	switch {
	case ent.IsNotFound(err), status.Code(err) == codes.NotFound:
		return ErrNotFound
	case ent.IsConstraintError(err), status.Code(err) == codes.Aborted, status.Code(err) == codes.AlreadyExists:
		return ErrConflict
	}
	return ErrUnavailable
}

func sqlOpaqueFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return ErrUnavailable
}

func sqlToken(v string) bool {
	if len(v) == 0 || len(v) > 128 {
		return false
	}
	for _, b := range []byte(v) {
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == ':' || b == '/' {
			continue
		}
		return false
	}
	return true
}

func sqlAccess(v camera.AccessLevel) int {
	switch v {
	case camera.AccessRead:
		return 1
	case camera.AccessConfigure:
		return 2
	case camera.AccessAdministrator:
		return 3
	}
	return 0
}

func sqlIntent(v Intent) error {
	return v.Validate()
}

func sqlBindingKey(v Subject) string {
	if v.BindingID.IsZero() {
		return "device"
	}
	return v.BindingID.String()
}

// scope checks durable parents even on private reads. Taking the device lock
// serializes all purposes, including the race before a subject row exists.
func (s *sqlStore) scope(ctx context.Context, sub Subject, lock bool) (*ent.CameraDevice, error) {
	if sub.Validate() != nil {
		return nil, ErrInput
	}
	if lock && s.drv.Dialect() == dialect.SQLite {
		// A no-op write obtains SQLite's write reservation before any read. It
		// does not advance observation_revision/date_updated or emit a change.
		q, args := sql.Dialect(s.drv.Dialect()).Update(cameradevice.Table).
			Set(cameradevice.FieldId, sql.Expr(cameradevice.FieldId)).
			Where(sql.And(sql.EQ(cameradevice.FieldId, sub.DeviceID.Uuid()), sql.EQ(cameradevice.FieldTenantId, sub.TenantID.Uuid()), sql.IsNull(cameradevice.FieldDateErased))).Query()
		if err := s.drv.Exec(ctx, q, args, nil); err != nil {
			return nil, sqlFailure(ctx, err)
		}
	}
	q := s.db.CameraDevice.Query().Where(cameradevice.IdEQ(sub.DeviceID.Uuid()), cameradevice.TenantIdEQ(sub.TenantID.Uuid()), cameradevice.DateErasedIsNil())
	if lock && s.drv.Dialect() != dialect.SQLite {
		q.Modify(func(q *sql.Selector) { q.ForUpdate() })
	}
	d, err := q.Only(ctx)
	if err != nil {
		return nil, sqlFailure(ctx, err)
	}
	// Tenant is hard-erased; unlike Site it has no soft-erasure timestamp.
	if _, err := s.db.Tenant.Query().Where(tenant.IdEQ(sub.TenantID.Uuid())).Only(ctx); err != nil {
		return nil, sqlFailure(ctx, err)
	}
	if !pdid.Id(d.SiteId).IsZero() {
		if _, err := s.db.Site.Query().Where(site.IdEQ(d.SiteId), site.TenantIdEQ(sub.TenantID.Uuid()), site.DateErasedIsNil()).Only(ctx); err != nil {
			return nil, sqlFailure(ctx, err)
		}
	}
	if !sub.BindingID.IsZero() {
		b, err := s.db.CameraBinding.Query().Where(camerabinding.IdEQ(sub.BindingID.Uuid()), camerabinding.TenantIdEQ(sub.TenantID.Uuid()), camerabinding.DeviceIdEQ(sub.DeviceID.Uuid()), camerabinding.DateErasedIsNil()).Only(ctx)
		if err != nil {
			return nil, sqlFailure(ctx, err)
		}
		if b.SiteId != d.SiteId {
			return nil, ErrInput
		}
	}
	return d, nil
}

func (s *sqlStore) subject(ctx context.Context, sub Subject, device *ent.CameraDevice) (*ent.CameraCredential, error) {
	c, err := s.db.CameraCredential.Query().Where(cameracredential.DeviceIdEQ(sub.DeviceID.Uuid()), cameracredential.BindingKeyEQ(sqlBindingKey(sub)), cameracredential.PurposeEQ(string(sub.Purpose))).Only(ctx)
	if err != nil {
		return nil, sqlFailure(ctx, err)
	}
	if c.TenantId != sub.TenantID.Uuid() || c.SiteId != device.SiteId || c.BindingId != sub.BindingID.Uuid() {
		return nil, ErrInput
	}
	if c.DateRevoked != nil || c.DateErased != nil {
		return nil, ErrRevoked
	}
	return c, nil
}

func sqlRef(c *ent.CameraCredential, sub Subject, version string) Reference {
	return Reference{Subject: sub, CredentialID: pdid.Id(c.Id), Version: version}
}

func sqlVersion(c *ent.CameraCredential, sub Subject, v *ent.CameraCredentialVersion) (Version, error) {
	if v.CredentialId != c.Id || v.TenantId != c.TenantId || v.SiteId != c.SiteId || !sqlToken(v.Version) || sqlAccess(camera.AccessLevel(v.Access)) == 0 {
		return Version{}, ErrInput
	}
	if v.DateErased != nil || v.DateRevoked != nil {
		return Version{}, ErrRevoked
	}
	var origin Origin
	switch api.CameraCredentialOrigin(v.Origin) {
	case api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_GENERATED:
		origin = Generated
	case api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_IMPORTED:
		origin = Imported
	case api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_REUSED:
		origin = Reused
	default:
		return Version{}, ErrUnavailable
	}
	return Version{Ref: sqlRef(c, sub, v.Version), Access: camera.AccessLevel(v.Access), Origin: origin, Backend: v.Backend, CreatedAt: v.DateCreated}, nil
}

func (s *sqlStore) version(ctx context.Context, c *ent.CameraCredential, version string) (*ent.CameraCredentialVersion, error) {
	v, err := s.db.CameraCredentialVersion.Query().Where(cameracredentialversion.CredentialIdEQ(c.Id), cameracredentialversion.VersionEQ(version)).Only(ctx)
	return v, sqlFailure(ctx, err)
}

func (s *sqlStore) intent(ctx context.Context, c *ent.CameraCredential, key string) (*ent.CameraCredentialVersion, error) {
	v, err := s.db.CameraCredentialVersion.Query().Where(cameracredentialversion.CredentialIdEQ(c.Id), cameracredentialversion.IntentKeyEQ(key)).Only(ctx)
	return v, sqlFailure(ctx, err)
}

// The hash contains only immutable NONSECRET request metadata. Username and
// material equality belong solely to the separate keyed tags below.
func sqlRequestHash(kind string, access camera.AccessLevel, reuse, expected *Reference, legacy *api.CameraLegacyCredentialReference) []byte {
	b := []byte("shale/camera-credential/request/v1\x00")
	field := func(v []byte) { b = binary.BigEndian.AppendUint32(b, uint32(len(v))); b = append(b, v...) }
	field([]byte(kind))
	field([]byte(access))
	for _, r := range []*Reference{reuse, expected} {
		if r == nil {
			b = append(b, 0)
			continue
		}
		b = append(b, 1)
		field(r.Subject.TenantID[:])
		field(r.Subject.DeviceID[:])
		field(r.Subject.BindingID[:])
		field([]byte(r.Subject.Purpose))
		field(r.CredentialID[:])
		field([]byte(r.Version))
	}
	if legacy == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		p, _ := proto.MarshalOptions{Deterministic: true}.Marshal(legacy)
		field(p)
	}
	h := sha256.Sum256(b)
	return h[:]
}

func (s *sqlStore) tag(ctx context.Context, r Reference, intent, prefix string, payload []byte) ([]byte, error) {
	c, err := r.Context()
	if err != nil {
		return nil, ErrInput
	}
	p := append([]byte(prefix), payload...)
	defer clear(p)
	t, err := s.replay.ImportTag(ctx, c, intent, p)
	if err != nil {
		return nil, sqlOpaqueFailure(ctx, err)
	}
	b, err := t.MarshalBinary()
	return b, sqlOpaqueFailure(ctx, err)
}

func sqlTagEqual(a, b []byte) bool {
	x, ex := camerasecret.ParseReplayTag(a)
	y, ey := camerasecret.ParseReplayTag(b)
	return ex == nil && ey == nil && x.Equal(y)
}

func (s *sqlStore) Prepare(ctx context.Context, req Preparation, generate Generator) (out Version, err error) {
	defer func() {
		if ctx.Err() != nil {
			out, err = Version{}, ctx.Err()
		}
	}()
	if err := s.ready(ctx); err != nil {
		return Version{}, err
	}
	username := req.Username()
	defer clear(username)
	if sqlIntent(req.Intent) != nil || len(username) == 0 || len(username) > 1024 {
		return Version{}, ErrInput
	}
	if req.Reuse != nil {
		if _, err := req.Reuse.Context(); err != nil || req.Reuse.Subject.TenantID != req.Intent.Subject.TenantID || req.Reuse.Subject.DeviceID != req.Intent.Subject.DeviceID {
			return Version{}, ErrInput
		}
	}
	d, err := s.scope(ctx, req.Intent.Subject, true)
	if err != nil {
		return Version{}, err
	}
	c, err := s.subject(ctx, req.Intent.Subject, d)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Version{}, err
	}
	kind := "prepare"
	if req.FallbackReuse {
		if req.Reuse == nil {
			return Version{}, ErrInput
		}
		kind = "prepare-fallback"
	}
	hash := sqlRequestHash(kind, req.Intent.Access, req.Reuse, nil, nil)
	if c != nil {
		if old, err := s.intent(ctx, c, req.Intent.Key); err == nil {
			out, err := sqlVersion(c, req.Intent.Subject, old)
			if err != nil {
				return Version{}, err
			}
			if _, _, err := s.resolve(ctx, out.Ref); err != nil {
				return Version{}, err
			}
			tag, err := s.tag(ctx, out.Ref, req.Intent.Key, "prepare-v1\x00", username)
			if err != nil {
				return Version{}, err
			}
			if !bytes.Equal(old.RequestHash, hash) || old.PrepareTag == nil || !sqlTagEqual(*old.PrepareTag, tag) {
				return Version{}, ErrConflict
			}
			return out, nil
		} else if !errors.Is(err, ErrNotFound) {
			return Version{}, err
		}
	}
	var material Material
	var reused *Reference
	if c != nil && c.CurrentVersion != "" {
		current := sqlRef(c, req.Intent.Subject, c.CurrentVersion)
		if req.Reuse != nil && !req.FallbackReuse && *req.Reuse != current {
			return Version{}, ErrConflict
		}
		reused = &current
	} else {
		reused = req.Reuse
	}
	origin := Generated
	if reused != nil {
		var source Version
		material, source, err = s.resolve(ctx, *reused)
		if err != nil {
			return Version{}, err
		}
		if sqlAccess(source.Access) < sqlAccess(req.Intent.Access) {
			return Version{}, ErrConflict
		}
		origin = Reused
	} else {
		if generate == nil {
			return Version{}, ErrNotFound
		}
		material, err = generate(ctx)
		if err != nil {
			return Version{}, sqlOpaqueFailure(ctx, err)
		}
		if !material.Valid() {
			return Version{}, ErrUnavailable
		}
		if err := material.Use(ctx, func(v camera.CredentialMaterial) error {
			if !hmac.Equal([]byte(v.Username), username) {
				return ErrUnavailable
			}
			return nil
		}); err != nil {
			return Version{}, sqlOpaqueFailure(ctx, err)
		}
	}
	return s.persist(ctx, req.Intent, d, c, origin, material, hash, username, reused, nil, "")
}

func (s *sqlStore) Import(ctx context.Context, req ImportRequest) (out Version, err error) {
	defer func() {
		if ctx.Err() != nil {
			out, err = Version{}, ctx.Err()
		}
	}()
	if err := s.ready(ctx); err != nil {
		return Version{}, err
	}
	if sqlIntent(req.Intent) != nil || !req.Material.Valid() {
		return Version{}, ErrInput
	}
	if req.ExpectedCurrent != nil {
		if _, err := req.ExpectedCurrent.Context(); err != nil || req.ExpectedCurrent.Subject != req.Intent.Subject {
			return Version{}, ErrInput
		}
	}
	var legacy *api.CameraLegacyCredentialReference
	var legacyKey string
	if req.Legacy != nil {
		var err error
		legacy, err = LegacyRecord(*req.Legacy)
		if err != nil || req.Legacy.Purpose != camera.AuthPurpose(req.Intent.Subject.Purpose) {
			return Version{}, ErrInput
		}
		legacyKey, err = LegacyKey(req.Intent.Subject.TenantID, *req.Legacy)
		if err != nil {
			return Version{}, ErrInput
		}
	}
	d, err := s.scope(ctx, req.Intent.Subject, true)
	if err != nil {
		return Version{}, err
	}
	c, err := s.subject(ctx, req.Intent.Subject, d)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Version{}, err
	}
	hash := sqlRequestHash("import", req.Intent.Access, nil, req.ExpectedCurrent, legacy)
	if c != nil {
		if old, err := s.intent(ctx, c, req.Intent.Key); err == nil {
			out, err := sqlVersion(c, req.Intent.Subject, old)
			if err != nil {
				return Version{}, err
			}
			if _, _, err := s.resolve(ctx, out.Ref); err != nil {
				return Version{}, err
			}
			payload, _ := req.Material.MarshalBinary()
			defer clear(payload)
			tag, err := s.tag(ctx, out.Ref, req.Intent.Key, "import-v1\x00", payload)
			if err != nil {
				return Version{}, err
			}
			if !bytes.Equal(old.RequestHash, hash) || old.ImportTag == nil || !sqlTagEqual(*old.ImportTag, tag) {
				return Version{}, ErrConflict
			}
			return out, nil
		} else if !errors.Is(err, ErrNotFound) {
			return Version{}, err
		}
	}
	if c != nil && c.CurrentVersion != "" {
		if req.ExpectedCurrent == nil || *req.ExpectedCurrent != sqlRef(c, req.Intent.Subject, c.CurrentVersion) {
			return Version{}, ErrConflict
		}
	} else if req.ExpectedCurrent != nil {
		return Version{}, ErrConflict
	}
	if legacyKey != "" {
		exists, err := s.db.CameraCredentialVersion.Query().Where(cameracredentialversion.LegacyKeyEQ(legacyKey)).Exist(ctx)
		if err != nil {
			return Version{}, sqlFailure(ctx, err)
		}
		if exists {
			return Version{}, ErrConflict
		}
	}
	return s.persist(ctx, req.Intent, d, c, Imported, req.Material, hash, nil, nil, legacy, legacyKey)
}

func sqlWireRef(ref Reference) *api.CameraCredentialReference {
	return api.CameraCredentialReference_builder{Provider: Provider, Id: ref.CredentialID.String(), Version: ref.Version, Purpose: string(ref.Subject.Purpose)}.Build()
}

func (s *sqlStore) persist(ctx context.Context, intent Intent, d *ent.CameraDevice, c *ent.CameraCredential, origin Origin, material Material, hash, username []byte, reused *Reference, legacy *api.CameraLegacyCredentialReference, legacyKey string) (Version, error) {
	cid := pdid.New(32)
	if c != nil {
		cid = pdid.Id(c.Id)
	}
	vid := pdid.New(33)
	ref := Reference{Subject: intent.Subject, CredentialID: cid, Version: vid.String()}
	cc, err := ref.Context()
	if err != nil {
		return Version{}, ErrInput
	}
	payload, err := material.MarshalBinary()
	if err != nil {
		return Version{}, ErrInput
	}
	defer clear(payload)
	envelope, err := s.backend.Protect(ctx, cc, payload)
	if err != nil {
		return Version{}, sqlOpaqueFailure(ctx, err)
	}
	sealed, err := envelope.MarshalBinary()
	if err != nil {
		return Version{}, ErrUnavailable
	}
	defer clear(sealed)
	add := api.CameraCredentialVersionAddRequest_builder{Id: vid.Bytes(), Tenant: api.TenantRef_builder{Id: intent.Subject.TenantID.Bytes()}.Build(), Credential: api.CameraCredentialRef_builder{Id: cid.Bytes()}.Build(), Version: ref.Version, IntentKey: intent.Key, Access: string(intent.Access), Backend: envelope.Backend(), SealedEnvelope: sealed, RequestHash: hash, DateCreated: timestamppb.New(s.now())}
	if !pdid.Id(d.SiteId).IsZero() {
		add.Site = api.SiteRef_builder{Id: d.SiteId[:]}.Build()
	}
	if origin == Imported {
		add.Origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_IMPORTED
		add.ImportTag, err = s.tag(ctx, ref, intent.Key, "import-v1\x00", payload)
		add.Legacy = legacy
		if legacyKey != "" {
			add.LegacyKey = &legacyKey
		}
	} else {
		add.Origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_GENERATED
		add.PrepareTag, err = s.tag(ctx, ref, intent.Key, "prepare-v1\x00", username)
		if origin == Reused {
			add.Origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_REUSED
			add.ReusedFrom = sqlWireRef(*reused)
		}
	}
	if err != nil {
		return Version{}, err
	}
	if c == nil {
		create := api.CameraCredentialAddRequest_builder{Id: cid.Bytes(), Tenant: add.Tenant, Site: add.Site, Device: api.CameraDeviceRef_builder{Id: intent.Subject.DeviceID.Bytes()}.Build(), BindingKey: sqlBindingKey(intent.Subject), Purpose: string(intent.Subject.Purpose), CurrentVersion: ref.Version, DateCreated: add.DateCreated}
		if !intent.Subject.BindingID.IsZero() {
			create.Binding = api.CameraBindingRef_builder{Id: intent.Subject.BindingID.Bytes()}.Build()
		}
		if _, err := s.recorded.CameraCredential().Add(ctx, create.Build()); err != nil {
			return Version{}, sqlFailure(ctx, err)
		}
	}
	if _, err := s.recorded.CameraCredentialVersion().Add(ctx, add.Build()); err != nil {
		return Version{}, sqlFailure(ctx, err)
	}
	if c != nil {
		current := ref.Version
		if _, err := s.recorded.CameraCredential().Patch(ctx, api.CameraCredentialPatchRequest_builder{Ref: api.CameraCredentialRef_builder{Id: cid.Bytes()}.Build(), CurrentVersion: &current, DateUpdated: timestamppb.New(c.DateUpdated)}.Build()); err != nil {
			return Version{}, sqlFailure(ctx, err)
		}
	} else {
		c, err = s.subject(ctx, intent.Subject, d)
		if err != nil {
			return Version{}, err
		}
	}
	v, err := s.version(ctx, c, ref.Version)
	if err != nil {
		return Version{}, err
	}
	return sqlVersion(c, intent.Subject, v)
}

func (s *sqlStore) resolve(ctx context.Context, ref Reference) (Material, Version, error) {
	if _, err := ref.Context(); err != nil {
		return Material{}, Version{}, ErrInput
	}
	d, err := s.scope(ctx, ref.Subject, false)
	if err != nil {
		return Material{}, Version{}, err
	}
	c, err := s.subject(ctx, ref.Subject, d)
	if err != nil {
		return Material{}, Version{}, err
	}
	if pdid.Id(c.Id) != ref.CredentialID {
		return Material{}, Version{}, ErrNotFound
	}
	v, err := s.version(ctx, c, ref.Version)
	if err != nil {
		return Material{}, Version{}, err
	}
	metadata, err := sqlVersion(c, ref.Subject, v)
	if err != nil {
		return Material{}, Version{}, err
	}
	envelope, err := camerasecret.ParseEnvelope(v.SealedEnvelope)
	if err != nil || envelope.Backend() != v.Backend {
		return Material{}, Version{}, ErrUnavailable
	}
	cc, _ := ref.Context()
	payload, err := s.backend.Open(ctx, cc, envelope)
	defer clear(payload)
	if err != nil {
		return Material{}, Version{}, sqlOpaqueFailure(ctx, err)
	}
	material, err := ParseMaterial(payload)
	if err != nil {
		return Material{}, Version{}, ErrUnavailable
	}
	// An injected backend may have taken time. Recheck exact stored revocation
	// state before emitting material; a current-version change never replaces
	// this caller's immutable pin. Core separately rechecks the delivery grant.
	d, err = s.scope(ctx, ref.Subject, false)
	if err != nil {
		return Material{}, Version{}, err
	}
	c, err = s.subject(ctx, ref.Subject, d)
	if err != nil {
		return Material{}, Version{}, err
	}
	if pdid.Id(c.Id) != ref.CredentialID {
		return Material{}, Version{}, ErrNotFound
	}
	v, err = s.version(ctx, c, ref.Version)
	if err != nil {
		return Material{}, Version{}, err
	}
	metadata, err = sqlVersion(c, ref.Subject, v)
	if err != nil {
		return Material{}, Version{}, err
	}
	return material, metadata, nil
}

func (s *sqlStore) Resolve(ctx context.Context, ref Reference) (out Material, err error) {
	defer func() {
		if ctx.Err() != nil {
			out, err = Material{}, ctx.Err()
		}
	}()
	if err := s.ready(ctx); err != nil {
		return Material{}, err
	}
	material, _, err := s.resolve(ctx, ref)
	return material, err
}
