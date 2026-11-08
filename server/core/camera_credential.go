package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"math/big"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	cc "github.com/lesomnus/shale/internal/cameracredential"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type coreCameraCredential struct {
	Core
	api.CameraCredentialServiceServer
}

func (s Core) CameraCredential() api.CameraCredentialServiceServer {
	return coreCameraCredential{s, s.Next().CameraCredential()}
}

func credentialError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, cc.ErrInput):
		return status.Error(codes.InvalidArgument, "invalid central credential request")
	case errors.Is(err, cc.ErrConflict):
		return status.Error(codes.Aborted, "credential preparation conflicts with current state or its original intent")
	case errors.Is(err, cc.ErrNotFound):
		return status.Error(codes.NotFound, "credential version not found")
	case errors.Is(err, cc.ErrRevoked):
		return status.Error(codes.PermissionDenied, "credential version is revoked")
	default:
		return status.Error(codes.Unavailable, "central credential store is unavailable")
	}
}

func (s Core) credentialStore(ctx context.Context) (cc.Store, error) {
	if s.d.Credentials == nil {
		return nil, cc.ErrUnavailable
	}
	drv := s.driver()
	if bound, ok := ctx.Value(txKey{}).(txBound); ok {
		drv = bound.drv
	}
	return s.d.Credentials(drv)
}

func credentialAccess(purpose camerasecret.Purpose, value string) camera.AccessLevel {
	if value != "" {
		return camera.AccessLevel(value)
	}
	switch purpose {
	case camerasecret.InitialAdmin:
		return camera.AccessAdministrator
	case camerasecret.Management:
		return camera.AccessConfigure
	default:
		return camera.AccessRead
	}
}

func (s Core) credentialTarget(ctx context.Context, nx api.Server, device *api.CameraDeviceRef, binding *api.CameraBindingRef, purpose string) (cc.Subject, error) {
	d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: device}.Build())
	if err != nil {
		return cc.Subject{}, err
	}
	subject := cc.Subject{TenantID: mustId(d.GetTenant().GetId()), DeviceID: mustId(d.GetId()), Purpose: camerasecret.Purpose(purpose)}
	if binding != nil {
		b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: binding}.Build())
		if err != nil {
			return cc.Subject{}, err
		}
		if !bytes.Equal(b.GetDevice().GetId(), d.GetId()) {
			return cc.Subject{}, denied("credential", "binding belongs to another device")
		}
		if err := cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), b.GetTenant().GetId(), b.GetSite().GetId()); err != nil {
			return cc.Subject{}, err
		}
		subject.BindingID = mustId(b.GetId())
	}
	if err := subject.Validate(); err != nil {
		return cc.Subject{}, credentialError(err)
	}
	return subject, nil
}

func (s Core) credentialReference(ctx context.Context, nx api.Server, wire *api.CameraCredentialReference) (cc.Reference, *api.CameraCredential, error) {
	if wire == nil || wire.GetProvider() != cc.Provider || cameraToken("version", wire.GetVersion()) != nil {
		return cc.Reference{}, nil, invalid("credential", "an exact central credential reference is required")
	}
	id, err := pdid.Parse(wire.GetId())
	if err != nil || id.Domain() != DomCameraCredential || id.String() != wire.GetId() {
		return cc.Reference{}, nil, invalid("credential", "a canonical central credential identifier is required")
	}
	row, err := nx.CameraCredential().Get(ctx, api.CameraCredentialGetRequest_builder{Ref: api.CameraCredentialRef_builder{Id: id.Bytes()}.Build()}.Build())
	if err != nil {
		return cc.Reference{}, nil, err
	}
	r := cc.Reference{CredentialID: id, Version: wire.GetVersion(), Subject: cc.Subject{
		TenantID: mustId(row.GetTenant().GetId()), DeviceID: mustId(row.GetDevice().GetId()), Purpose: camerasecret.Purpose(row.GetPurpose()),
	}}
	if len(row.GetBinding().GetId()) != 0 {
		r.Subject.BindingID = mustId(row.GetBinding().GetId())
	}
	if r.Subject.Purpose != camerasecret.Purpose(wire.GetPurpose()) {
		return cc.Reference{}, nil, denied("credential", "credential purpose differs from its immutable reference")
	}
	if _, err := r.Context(); err != nil {
		return cc.Reference{}, nil, credentialError(err)
	}
	return r, row, nil
}

func credentialWire(r cc.Reference) *api.CameraCredentialReference {
	return api.CameraCredentialReference_builder{Provider: cc.Provider, Id: r.CredentialID.String(), Version: r.Version, Purpose: string(r.Subject.Purpose)}.Build()
}

func credentialPrepared(v cc.Version) *api.CameraCredentialPrepared {
	var binding []byte
	if !v.Ref.Subject.BindingID.IsZero() {
		binding = v.Ref.Subject.BindingID.Bytes()
	}
	origin := api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_UNSPECIFIED
	switch v.Origin {
	case cc.Generated:
		origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_GENERATED
	case cc.Imported:
		origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_IMPORTED
	case cc.Reused:
		origin = api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_REUSED
	}
	return api.CameraCredentialPrepared_builder{
		Credential: credentialWire(v.Ref), DeviceId: v.Ref.Subject.DeviceID.Bytes(), BindingId: binding,
		Access: string(v.Access), Origin: origin, Backend: v.Backend, DateCreated: timestamppb.New(v.CreatedAt), Revoked: v.Revoked,
	}.Build()
}

// GenerateInitialMaterial is useful to compose the private enrollment callback.
// It is passed to Store.Prepare, which invokes it only after its durable intent
// lookup proves a new account is needed. Existing/imported usernames survive.
func GenerateInitialMaterial(username []byte) cc.Generator {
	name := string(username)
	return func(ctx context.Context) (cc.Material, error) {
		if err := ctx.Err(); err != nil {
			return cc.Material{}, err
		}
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		// Common initial-password policies require all three character classes.
		// Rejection avoids adding a predictable class at a fixed position.
		for range 32 {
			password := make([]byte, 32)
			var lower, upper, digit bool
			for i := range password {
				n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
				if err != nil {
					return cc.Material{}, cc.ErrUnavailable
				}
				password[i] = alphabet[n.Int64()]
				lower = lower || password[i] >= 'a' && password[i] <= 'z'
				upper = upper || password[i] >= 'A' && password[i] <= 'Z'
				digit = digit || password[i] >= '0' && password[i] <= '9'
			}
			if lower && upper && digit {
				material, err := cc.NewMaterial(name, string(password))
				clear(password)
				return material, err
			}
			clear(password)
		}
		return cc.Material{}, cc.ErrUnavailable
	}
}

func (s coreCameraCredential) Prepare(ctx context.Context, r *api.CameraCredentialPrepareRequest) (*api.CameraCredentialPrepared, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	username := r.GetUsername()
	if len(username) == 0 {
		username = []byte("admin")
	}
	var out cc.Version
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		subject, err := c.credentialTarget(ctx, nx, r.GetDevice(), r.GetBinding(), r.GetPurpose())
		if err != nil {
			return err
		}
		var reuse *cc.Reference
		if r.GetReuse() != nil {
			v, _, err := c.credentialReference(ctx, nx, r.GetReuse())
			if err != nil {
				return err
			}
			reuse = &v
		}
		p, err := cc.NewPreparation(cc.Intent{Subject: subject, Key: r.GetIntentKey(), Access: credentialAccess(subject.Purpose, r.GetAccess())}, username, reuse)
		if err != nil {
			return credentialError(err)
		}
		store, err := c.credentialStore(ctx)
		if err != nil {
			return credentialError(err)
		}
		var generate cc.Generator
		if subject.Purpose == camerasecret.InitialAdmin {
			generate = GenerateInitialMaterial(username)
		}
		out, err = store.Prepare(ctx, p, generate)
		if err != nil {
			return credentialError(err)
		}
		return c.credentialAudit(ctx, out, api.CameraCredentialService_Prepare_FullMethodName)
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return credentialPrepared(out), nil
}

func (s coreCameraCredential) Import(ctx context.Context, r *api.CameraCredentialImportRequest) (*api.CameraCredentialPrepared, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	material, err := cc.ParseMaterial(r.GetMaterial())
	if err != nil {
		return nil, credentialError(err)
	}
	var out cc.Version
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		subject, err := c.credentialTarget(ctx, nx, r.GetDevice(), r.GetBinding(), r.GetPurpose())
		if err != nil {
			return err
		}
		request := cc.ImportRequest{Intent: cc.Intent{Subject: subject, Key: r.GetIntentKey(), Access: credentialAccess(subject.Purpose, r.GetAccess())}, Material: material}
		if r.GetExpectedCurrent() != nil {
			v, _, err := c.credentialReference(ctx, nx, r.GetExpectedCurrent())
			if err != nil {
				return err
			}
			request.ExpectedCurrent = &v
		}
		if r.GetLegacy() != nil {
			ref, err := cc.LegacyReference(r.GetLegacy())
			if err != nil {
				return credentialError(err)
			}
			request.Legacy = &ref
		}
		store, err := c.credentialStore(ctx)
		if err != nil {
			return credentialError(err)
		}
		out, err = store.Import(ctx, request)
		if err != nil {
			return credentialError(err)
		}
		return c.credentialAudit(ctx, out, api.CameraCredentialService_Import_FullMethodName)
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return credentialPrepared(out), nil
}

// Record machine credential access explicitly: TrailOfPeople deliberately does
// not record machines, and recording raw transfer DTOs would leak secrets.
// The audit value is a metadata-only Version projection, never a raw DB row.
func (s Core) credentialAudit(ctx context.Context, v cc.Version, action string) error {
	row, err := s.ent(ctx).CameraCredentialVersion.Query().Where(
		cameracredentialversion.CredentialIdEQ(v.Ref.CredentialID.Uuid()), cameracredentialversion.VersionEQ(v.Ref.Version),
	).Select(cameracredentialversion.FieldId).Only(ctx)
	if err != nil {
		return credentialError(err)
	}
	metadata := api.CameraCredentialVersion_builder{
		Id: row.Id[:], Version: v.Ref.Version, Access: string(v.Access), Backend: v.Backend,
		Credential: api.CameraCredential_builder{Id: v.Ref.CredentialID.Bytes()}.Build(),
	}.Build()
	value, err := proto.Marshal(metadata)
	if err != nil {
		return credentialError(err)
	}
	f, err := actor(ctx)
	if err != nil {
		return err
	}
	_, err = s.ent(ctx).Audit.Create().SetId(pdid.New(3).Uuid()).SetTenantId(v.Ref.Subject.TenantID.Uuid()).
		SetActorId(f.Actor.Uuid()).SetActorTenantId(f.Tenant.Uuid()).SetTraceId([]byte{}).SetAction(action).
		SetObjectId(row.Id).SetDomain(uint32(DomCameraCredentialVersion)).SetPatch([]byte{}).SetValue(value).SetDateCreated(s.d.now()).Save(ctx)
	if err != nil {
		return credentialError(err)
	}
	return nil
}

func (s coreCameraCredential) Revoke(ctx context.Context, r *api.CameraCredentialRevokeRequest) (*api.CameraCredentialRevokeResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		ref, credential, err := c.credentialReference(ctx, nx, r.GetCredential())
		if err != nil {
			return err
		}
		if err := c.cameraLockDevice(ctx, ref.Subject.DeviceID.Bytes()); err != nil {
			return err
		}
		row, err := c.ent(ctx).CameraCredentialVersion.Query().Where(
			cameracredentialversion.CredentialIdEQ(ref.CredentialID.Uuid()), cameracredentialversion.VersionEQ(ref.Version),
			cameracredentialversion.DateErasedIsNil(),
		).Select(cameracredentialversion.FieldId, cameracredentialversion.FieldDateUpdated, cameracredentialversion.FieldDateRevoked,
			cameracredentialversion.FieldAccess, cameracredentialversion.FieldBackend, cameracredentialversion.FieldDateCreated,
			cameracredentialversion.FieldTenantId, cameracredentialversion.FieldSiteId).Only(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				return credentialError(cc.ErrNotFound)
			}
			return credentialError(err)
		}
		var site []byte
		if !pdid.Id(row.SiteId).IsZero() {
			site = row.SiteId[:]
		}
		if err := cameraSameScope(credential.GetTenant().GetId(), credential.GetSite().GetId(), row.TenantId[:], site); err != nil {
			return err
		}
		if row.DateRevoked != nil {
			return nil
		}
		if _, err := nx.CameraCredentialVersion().Patch(ctx, api.CameraCredentialVersionPatchRequest_builder{
			Ref: api.CameraCredentialVersionRef_builder{Id: row.Id[:]}.Build(), DateUpdated: timestamppb.New(row.DateUpdated), DateRevoked: timestamppb.New(s.d.now()),
		}.Build()); err != nil {
			return err
		}
		return c.credentialAudit(ctx, cc.Version{Ref: ref, Access: camera.AccessLevel(row.Access), Backend: row.Backend, CreatedAt: row.DateCreated, Revoked: true}, api.CameraCredentialService_Revoke_FullMethodName)
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return &api.CameraCredentialRevokeResponse{}, nil
}
