package cp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential/material"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type providerFixture struct {
	now      time.Time
	scope    camera.EnrollmentScope
	ref      camera.CredentialRef
	response *api.CameraCredentialLease
}

func newProviderFixture(t *testing.T) providerFixture {
	t.Helper()
	tenant, set, producer, network := pdid.New(1), pdid.New(7), pdid.New(22), pdid.New(26)
	f := providerFixture{now: time.Now(), scope: camera.EnrollmentScope{TenantID: tenant.String(), SetID: set.String(), ProducerID: producer.String(), NetworkID: network.String()}}
	f.ref = camera.CredentialRef{Provider: "cp", ID: pdid.New(32).String(), Version: "v1", Scope: f.scope, Purpose: camera.AuthStreaming, Recoverable: true}
	decoded, err := material.New("synthetic-user\xff", "synthetic-password\xfe\x00")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	f.response = api.CameraCredentialLease_builder{
		Material: payload, Credential: api.CameraCredentialReference_builder{Provider: "cp", Id: f.ref.ID, Version: f.ref.Version, Purpose: string(f.ref.Purpose)}.Build(),
		Expires: timestamppb.New(f.now.Add(30 * time.Second)), Access: string(camera.AccessAdministrator),
		Delivery: api.CameraCredentialDelivery_builder{TenantId: tenant.Bytes(), SetId: set.Bytes(), ProducerId: producer.Bytes(), NetworkId: network.Bytes(), DeviceId: pdid.New(27).Bytes(), BindingId: pdid.New(29).Bytes(), SourceId: pdid.New(8).Bytes(), AssignmentId: pdid.New(28).Bytes(), AssignmentGeneration: 1, SourceRevision: 3}.Build(),
	}.Build()
	return f
}

func (f providerFixture) request() camera.AuthRequest {
	ref := f.ref
	return camera.AuthRequest{Scope: f.scope, Purpose: camera.AuthStreaming, ExistingRef: &ref, Requirement: camera.AuthRequirement{Access: camera.AccessRead, Recoverable: true}}
}

func TestProviderResolvesExactVersionAndClearsTransfer(t *testing.T) {
	f := newProviderFixture(t)
	response := proto.Clone(f.response).(*api.CameraCredentialLease)
	var sent *api.CameraCredentialResolveRequest
	p, err := NewResolver(func(_ context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		sent = r
		return response, nil
	}, Options{Role: Producer, Scope: f.scope, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := camera.Acquire(t.Context(), p, f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if sent.GetCredential().GetId() != f.ref.ID || sent.GetCredential().GetVersion() != f.ref.Version || sent.GetAccess() != string(camera.AccessRead) || sent.GetLeaseSeconds() != 30 || sent.GetJob() != nil {
		t.Fatal("client changed the requested immutable reference or role")
	}
	if len(response.GetMaterial()) != 0 {
		t.Fatal("client retained material in its raw transfer DTO")
	}
	if err := lease.Use(t.Context(), func(value camera.CredentialMaterial) error {
		if value.Username != "synthetic-user\xff" || value.Password != "synthetic-password\xfe\x00" {
			t.Fatal("client changed binary credential bytes")
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q"} {
			if got := fmt.Sprintf(verb, value); got != "[camera credential]" {
				t.Fatal("material formatting exposed credential fields")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q"} {
		if fmt.Sprintf(verb, lease) != "[camera credential lease]" {
			t.Fatal("lease formatting exposed material")
		}
	}
}

func TestProviderRejectsChangedOrExpiredDelivery(t *testing.T) {
	f := newProviderFixture(t)
	for name, change := range map[string]func(*api.CameraCredentialLease){
		"id":           func(v *api.CameraCredentialLease) { v.GetCredential().SetId(pdid.New(32).String()) },
		"version":      func(v *api.CameraCredentialLease) { v.GetCredential().SetVersion("v2") },
		"purpose":      func(v *api.CameraCredentialLease) { v.GetCredential().SetPurpose("management") },
		"tenant":       func(v *api.CameraCredentialLease) { v.GetDelivery().SetTenantId(pdid.New(1).Bytes()) },
		"network":      func(v *api.CameraCredentialLease) { v.GetDelivery().SetNetworkId(pdid.New(26).Bytes()) },
		"producer":     func(v *api.CameraCredentialLease) { v.GetDelivery().SetProducerId(pdid.New(22).Bytes()) },
		"set":          func(v *api.CameraCredentialLease) { v.GetDelivery().SetSetId(pdid.New(7).Bytes()) },
		"source_shape": func(v *api.CameraCredentialLease) { v.GetDelivery().SetSourceId(pdid.New(27).Bytes()) },
		"expiry":       func(v *api.CameraCredentialLease) { v.SetExpires(timestamppb.New(f.now)) },
		"huge_expiry":  func(v *api.CameraCredentialLease) { v.SetExpires(timestamppb.New(f.now.Add(time.Hour))) },
		"material":     func(v *api.CameraCredentialLease) { v.SetMaterial([]byte("malformed-credential")) },
		"access":       func(v *api.CameraCredentialLease) { v.SetAccess("") },
	} {
		t.Run(name, func(t *testing.T) {
			response := proto.Clone(f.response).(*api.CameraCredentialLease)
			change(response)
			p, err := NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
				return response, nil
			}, Options{Role: Producer, Scope: f.scope, Now: func() time.Time { return f.now }})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := p.Acquire(t.Context(), f.request())
			if lease != nil || !errors.Is(err, ErrInvalidLease) {
				t.Fatal("invalid delivery produced a credential lease")
			}
			if len(response.GetMaterial()) != 0 {
				t.Fatal("rejected raw response retained credential material")
			}
		})
	}
}

func TestProviderDoesNotFallbackOrExposeRemoteErrors(t *testing.T) {
	f := newProviderFixture(t)
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.PermissionDenied, codes.Unauthenticated, codes.NotFound, codes.Aborted} {
		t.Run(code.String(), func(t *testing.T) {
			const private = "rtsp://synthetic-user:synthetic-password@camera/"
			calls := 0
			p, err := NewResolver(func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
				calls++
				return nil, status.Error(code, private)
			}, Options{Role: Producer, Scope: f.scope})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := p.Acquire(t.Context(), f.request())
			if lease != nil || err == nil || strings.Contains(err.Error(), "synthetic-password") || len(status.Convert(err).Details()) != 0 || calls != 1 {
				t.Fatal("client exposed a remote diagnostic or substituted another credential")
			}
			if code == codes.Unavailable || code == codes.DeadlineExceeded {
				if !errors.Is(err, ErrUnavailable) {
					t.Fatal("transport failure lost retry classification")
				}
			} else if !errors.Is(err, camera.ErrForbidden) {
				t.Fatal("authorization failure lost denial classification")
			}
		})
	}
}

func TestWorkerUsesOnlyPinnedPurposeAndCurrentGrant(t *testing.T) {
	f := newProviderFixture(t)
	f.ref.Purpose = camera.AuthManagement
	f.response.GetCredential().SetPurpose(string(camera.AuthManagement))
	refs := map[camera.AuthPurpose]camera.CredentialRef{camera.AuthManagement: f.ref}
	jobID := pdid.New(30)
	f.response.GetDelivery().SetJobId(jobID.Bytes())
	f.response.GetDelivery().SetLeaseGeneration(2)
	f.response.GetDelivery().SetAssignmentGeneration(4)
	var sent *api.CameraCredentialResolveRequest
	p, err := NewResolver(func(_ context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		sent = r
		return proto.Clone(f.response).(*api.CameraCredentialLease), nil
	}, Options{Role: Worker, Scope: f.scope, References: refs, Now: func() time.Time { return f.now }, Job: func(context.Context) (*api.CameraWorkerCheckJobRequest, error) {
		return api.CameraWorkerCheckJobRequest_builder{Ref: api.CameraJobRef_builder{Id: jobID.Bytes()}.Build(), LeaseGeneration: 2, AssignmentGeneration: 4}.Build(), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	delete(refs, camera.AuthManagement)
	request := camera.AuthRequest{Scope: f.scope, Purpose: camera.AuthManagement, Requirement: camera.AuthRequirement{Access: camera.AccessConfigure, Recoverable: true}}
	lease, err := camera.Acquire(t.Context(), p, request)
	if err != nil {
		t.Fatal(err)
	}
	lease.Close()
	if sent.GetJob().GetLeaseGeneration() != 2 || sent.GetJob().GetAssignmentGeneration() != 4 || sent.GetCredential().GetPurpose() != string(camera.AuthManagement) {
		t.Fatal("client did not attach the current job grant and pinned purpose")
	}
	request.Purpose = camera.AuthInitialAdmin
	if lease, err := p.Acquire(t.Context(), request); lease != nil || err == nil {
		t.Fatal("worker invented an unprepared purpose credential")
	}
}
