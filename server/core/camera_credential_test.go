package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	cc "github.com/lesomnus/shale/internal/cameracredential"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCredentialMachineAllowlistIsResolveOnly(t *testing.T) {
	for _, domain := range []pdid.Domain{DomCameraWorker, DomProducer, DomReader} {
		for _, method := range []string{
			api.CameraCredentialService_Resolve_FullMethodName,
			api.CameraCredentialService_Prepare_FullMethodName,
			api.CameraCredentialService_Import_FullMethodName,
			api.CameraCredentialService_Revoke_FullMethodName,
			api.CameraCredentialService_Get_FullMethodName,
			api.CameraCredentialService_List_FullMethodName,
			api.CameraCredentialService_Watch_FullMethodName,
			api.CameraCredentialVersionService_Get_FullMethodName,
			api.CameraCredentialVersionService_List_FullMethodName,
			api.CameraCredentialVersionService_Watch_FullMethodName,
		} {
			call := gate.Call{Actor: pdid.New(domain), Tenant: pdid.New(DomTenant), Action: method}
			err := (TenantPolicy{}).May(t.Context(), call)
			want := method == api.CameraCredentialService_Resolve_FullMethodName && domain != DomReader
			if (err == nil) != want {
				t.Fatalf("unexpected credential permission for domain %d method %s", domain, method)
			}
		}
	}
	for _, method := range []string{api.CameraCredentialService_Add_FullMethodName, api.CameraCredentialService_Patch_FullMethodName,
		api.CameraCredentialService_Apply_FullMethodName, api.CameraCredentialService_Erase_FullMethodName,
		api.CameraCredentialVersionService_Add_FullMethodName, api.CameraCredentialVersionService_Patch_FullMethodName,
		api.CameraCredentialVersionService_Apply_FullMethodName, api.CameraCredentialVersionService_Erase_FullMethodName} {
		if !closedToEveryone(method) {
			t.Fatal("generic mutation became a credential-store interface")
		}
	}
}

func TestCredentialErrorsHideBackendAndPayloadDetails(t *testing.T) {
	const private = "fixture-user:fixture-password"
	for _, raw := range []error{
		errors.New(private), fmt.Errorf("backend request material: %s", private), status.Error(codes.Internal, private),
	} {
		err := credentialError(raw)
		if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), private) || len(status.Convert(err).Details()) != 0 {
			t.Fatal("backend or encoded error escaped the credential boundary")
		}
	}
	for _, item := range []struct {
		input error
		code  codes.Code
	}{
		{cc.ErrInput, codes.InvalidArgument}, {cc.ErrConflict, codes.Aborted}, {cc.ErrNotFound, codes.NotFound}, {cc.ErrRevoked, codes.PermissionDenied},
	} {
		if status.Code(credentialError(item.input)) != item.code {
			t.Fatal("credential error lost its safe classification")
		}
	}
	if !errors.Is(credentialError(fmt.Errorf("%s: %w", private, context.Canceled)), context.Canceled) {
		t.Fatal("credential boundary lost context cancellation")
	}
}

func TestCredentialInitialGeneratorPreservesUsernameAndPolicy(t *testing.T) {
	username := []byte("fixture-user\xff")
	generate := GenerateInitialMaterial(username)
	clear(username)
	seen := map[string]bool{}
	for range 5 {
		material, err := generate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := material.Use(t.Context(), func(value camera.CredentialMaterial) error {
			if value.Username != "fixture-user\xff" || len(value.Password) != 32 || seen[value.Password] {
				t.Fatal("initial generator changed username bytes or reused a password")
			}
			var lower, upper, digit bool
			for _, c := range []byte(value.Password) {
				lower = lower || c >= 'a' && c <= 'z'
				upper = upper || c >= 'A' && c <= 'Z'
				digit = digit || c >= '0' && c <= '9'
			}
			if !lower || !upper || !digit {
				t.Fatal("initial password does not meet its declared character classes")
			}
			seen[value.Password] = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := generate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("initial generator ignored cancellation")
	}
}
