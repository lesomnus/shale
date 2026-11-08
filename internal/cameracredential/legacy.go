package cameracredential

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"google.golang.org/protobuf/proto"
)

// LegacyRecord preserves every original scope field. It does not authorize the
// old Producer/Worker, rotate a password or remove a standalone copy.
func LegacyRecord(ref camera.CredentialRef) (*api.CameraLegacyCredentialReference, error) {
	if !ref.Valid() || ref.Purpose != camera.AuthInitialAdmin && ref.Purpose != camera.AuthManagement && ref.Purpose != camera.AuthStreaming {
		return nil, ErrInput
	}
	v := api.CameraLegacyCredentialReference_builder{
		Ref:      api.CameraCredentialReference_builder{Provider: ref.Provider, Id: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}.Build(),
		TenantId: ref.Scope.TenantID, SiteId: ref.Scope.SiteID, SetId: ref.Scope.SetID,
		ProducerId: ref.Scope.ProducerID, NetworkId: ref.Scope.NetworkID, Recoverable: ref.Recoverable,
	}.Build()
	if _, err := proto.Marshal(v); err != nil {
		return nil, ErrInput
	}
	return v, nil
}

func LegacyReference(v *api.CameraLegacyCredentialReference) (camera.CredentialRef, error) {
	if v == nil || v.GetRef() == nil || len(v.ProtoReflect().GetUnknown()) != 0 || len(v.GetRef().ProtoReflect().GetUnknown()) != 0 {
		return camera.CredentialRef{}, ErrInput
	}
	r := v.GetRef()
	ref := camera.CredentialRef{
		Provider: r.GetProvider(), ID: r.GetId(), Version: r.GetVersion(), Purpose: camera.AuthPurpose(r.GetPurpose()), Recoverable: v.GetRecoverable(),
		Scope: camera.EnrollmentScope{TenantID: v.GetTenantId(), SiteID: v.GetSiteId(), SetID: v.GetSetId(), ProducerID: v.GetProducerId(), NetworkID: v.GetNetworkId()},
	}
	if _, err := LegacyRecord(ref); err != nil {
		return camera.CredentialRef{}, err
	}
	return ref, nil
}

// LegacyKey fingerprints NONSECRET reference metadata only. Password import
// equality uses the independent keyed ImportReplay contract, never this hash.
// The actual CP tenant namespaces old aliases without changing their spelling.
func LegacyKey(tenant pdid.Id, ref camera.CredentialRef) (string, error) {
	if tenant.Domain() != 1 {
		return "", ErrInput
	}
	v, err := LegacyRecord(ref)
	if err != nil {
		return "", err
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(v)
	if err != nil {
		return "", ErrInput
	}
	h := sha256.New()
	h.Write([]byte("shale/camera-credential/legacy/v1\x00"))
	h.Write(tenant[:])
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}
