package cameracredential

import (
	"errors"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestLegacyReferencePreservesFullScopeAndIdentity(t *testing.T) {
	ref := camera.CredentialRef{
		Provider: "file", ID: "existing-camera", Version: "1", Purpose: camera.AuthStreaming, Recoverable: true,
		Scope: camera.EnrollmentScope{TenantID: "original-tenant", SiteID: "original-site", SetID: "original-set", ProducerID: "old-producer", NetworkID: "original-network"},
	}
	v, err := LegacyRecord(ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	stored := new(api.CameraLegacyCredentialReference)
	if err := proto.Unmarshal(b, stored); err != nil {
		t.Fatal(err)
	}
	if recovered, err := LegacyReference(stored); err != nil || recovered != ref {
		t.Fatal("legacy scope/reference did not survive persistence exactly")
	}
	tenant := pdid.New(1)
	key, err := LegacyKey(tenant, ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*camera.CredentialRef){
		func(r *camera.CredentialRef) { r.Scope.SiteID = "different-site" },
		func(r *camera.CredentialRef) { r.Scope.SetID = "different-set" },
		func(r *camera.CredentialRef) { r.Scope.ProducerID = "different-producer" },
		func(r *camera.CredentialRef) { r.Scope.NetworkID = "different-network" },
		func(r *camera.CredentialRef) { r.Scope.TenantID = "different-tenant" },
		func(r *camera.CredentialRef) { r.Version = "2" },
		func(r *camera.CredentialRef) { r.Purpose = camera.AuthManagement },
		func(r *camera.CredentialRef) { r.Recoverable = false },
	} {
		other := ref
		change(&other)
		if otherKey, err := LegacyKey(tenant, other); err != nil || key == otherKey {
			t.Fatal("distinct original references shared a migration key")
		}
	}
	if otherKey, err := LegacyKey(pdid.New(1), ref); err != nil || otherKey == key {
		t.Fatal("legacy alias escaped its actual CP tenant")
	}
	unknown := protowire.AppendTag(nil, 99, protowire.BytesType)
	unknown = protowire.AppendString(unknown, "unexpected-field")
	v.ProtoReflect().SetUnknown(unknown)
	if _, err := LegacyReference(v); !errors.Is(err, ErrInput) {
		t.Fatal("unknown migration metadata accepted")
	}
}
