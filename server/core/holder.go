package core

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/lesomnus/z"
	"golang.org/x/crypto/argon2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// People sign in with a password (§33.1). The verifier is argon2id; the
// row stores it as a secret field the Secret layer never answers with.

const (
	argonTime    = 1
	argonMemory  = 64 * 1024
	argonThreads = 4
	argonKeyLen  = 32
)

// HashPassword makes a verifier: "argon2id$<salt>$<hash>", base64.
func HashPassword(password string) ([]byte, error) {
	if len(password) < 8 {
		return nil, errors.New("password: at least 8 characters")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	h := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	return []byte(fmt.Sprintf("argon2id$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(h))), nil
}

// CheckPassword compares a password against a verifier.
func CheckPassword(verifier []byte, password string) bool {
	parts := strings.Split(string(verifier), "$")
	if len(parts) != 3 || parts[0] != "argon2id" {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, uint32(len(want)))

	return subtle.ConstantTimeCompare(got, want) == 1
}

// RandomPassword is what init hands out once.
func RandomPassword() string {
	b := make([]byte, 18)
	rand.Read(b)

	return base64.RawURLEncoding.EncodeToString(b)
}

type coreHolder struct {
	Core
	api.HolderServiceServer
}

func (s Core) Holder() api.HolderServiceServer {
	return coreHolder{s, s.Next().Holder()}
}

// SetPassword hashes and stores a password through the servers below, so
// the row stores a verifier and the trail records the write.
func (s coreHolder) SetPassword(ctx context.Context, req *api.HolderSetPasswordRequest) (*api.Holder, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomHolder {
		return nil, status.Error(codes.PermissionDenied, "only a person sets a password")
	}
	h, err := s.HolderServiceServer.Get(ctx, api.HolderGetRequest_builder{Ref: req.GetRef()}.Build())
	if err != nil {
		return nil, err
	}
	v, err := HashPassword(req.GetPassword())
	if err != nil {
		return nil, invalid("password", err.Error())
	}

	return s.HolderServiceServer.Patch(ctx, api.HolderPatchRequest_builder{
		Ref:              api.HolderRef_builder{Id: h.GetId()}.Build(),
		Password:         v,
		DateUpdatedForce: z.Ptr(true),
	}.Build())
}

// VerifyPassword is what a sign-in checks: the person exists, is not
// erased, and the password matches. It reads through the server with no
// wall, since nobody is signed in yet, and answers the ids the session
// carries.
func (s Core) VerifyPassword(ctx context.Context, tenant, alias, password string) (pdid.Id, pdid.Id, error) {
	if tenant == "" || alias == "" || password == "" {
		return pdid.Nil, pdid.Nil, errors.New("tenant, alias, and password are required")
	}
	h, err := s.d.Own.Holder().Get(ctx, api.HolderGetRequest_builder{
		Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
			Alias:  z.Ptr(alias),
			Tenant: api.TenantRef_builder{Alias: z.Ptr(tenant)}.Build(),
		}.Build()}.Build(),
		Select: api.HolderSelect_builder{All: z.Ptr(true), Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		return pdid.Nil, pdid.Nil, errors.New("no such person, or wrong password")
	}
	if len(h.GetPassword()) == 0 || !CheckPassword(h.GetPassword(), password) {
		return pdid.Nil, pdid.Nil, errors.New("no such person, or wrong password")
	}
	who, err := pdid.From(h.GetId())
	if err != nil {
		return pdid.Nil, pdid.Nil, err
	}
	t, err := pdid.From(h.GetTenant().GetId())
	if err != nil {
		return pdid.Nil, pdid.Nil, err
	}

	return who, t, nil
}
