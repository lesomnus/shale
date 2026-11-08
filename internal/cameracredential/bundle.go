package cameracredential

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
)

// BundleRequest is supplied only after the caller has authorized the current
// assignment/device/binding graph. Store MUST belong to that caller's CURRENT
// recorded transaction. Commit the entire bundle and Job before camera I/O.
// A purpose describes delivery permission, not separate accounts on a camera.
type BundleRequest struct {
	TenantID, DeviceID, BindingID pdid.Id
	IntentKey                     string
	InitialUsername               []byte `json:"-" yaml:"-"`
	AllowActivation               bool
}

type Bundle struct {
	InitialAdmin, Management, Streaming Reference
}

// PrepareBundle reuses installed/current accounts and only generates material
// for a genuinely new, activation-authorized device. It is independent of any
// policy service or outward operator identity; the edge composition wires it.
func PrepareBundle(ctx context.Context, store Store, r BundleRequest, generate Generator) (Bundle, error) {
	var out Bundle
	if store == nil || r.AllowActivation && generate == nil || r.BindingID.Domain() != 29 {
		return out, ErrInput
	}
	prepare := func(purpose camerasecret.Purpose, binding pdid.Id, access camera.AccessLevel, reuse *Reference, generator Generator) (Version, error) {
		p, err := NewPreparation(Intent{Subject: Subject{TenantID: r.TenantID, DeviceID: r.DeviceID, BindingID: binding, Purpose: purpose}, Key: r.IntentKey, Access: access}, r.InitialUsername, reuse)
		if err != nil {
			return Version{}, err
		}
		p.FallbackReuse = reuse != nil
		return store.Prepare(ctx, p, generator)
	}
	initial, err := prepare(camerasecret.InitialAdmin, pdid.Id{}, camera.AccessAdministrator, nil, nil)
	if errors.Is(err, ErrNotFound) && r.AllowActivation {
		initial, err = prepare(camerasecret.InitialAdmin, pdid.Id{}, camera.AccessAdministrator, nil, generate)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return out, err
	}
	var fallback *Reference
	if err == nil {
		out.InitialAdmin = initial.Ref
		fallback = &initial.Ref
	}
	for _, target := range []struct {
		purpose camerasecret.Purpose
		binding pdid.Id
		access  camera.AccessLevel
		result  *Reference
	}{
		{camerasecret.Management, pdid.Id{}, camera.AccessConfigure, &out.Management},
		{camerasecret.Streaming, r.BindingID, camera.AccessRead, &out.Streaming},
	} {
		// Existing purpose-specific imports take precedence. Only an absent
		// current account is deliberately shared with the installed initial one.
		version, err := prepare(target.purpose, target.binding, target.access, fallback, nil)
		if err != nil {
			return Bundle{}, err
		}
		*target.result = version.Ref
		// A management import can initialize streaming delivery without an
		// initial-admin credential on an already initialized camera.
		fallback = &version.Ref
	}
	return out, nil
}

func (BundleRequest) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, preparationMarker) }
