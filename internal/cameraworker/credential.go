package cameraworker

import (
	"context"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	cpauth "github.com/lesomnus/shale/camera/credential/cp"
)

func credentialScope(work *api.CameraWorkerWork) (camera.EnrollmentScope, error) {
	var scope camera.EnrollmentScope
	if work == nil {
		return scope, camera.ErrInvalid
	}
	a := work.GetAssignment()
	fields := []struct {
		raw      []byte
		domain   pdid.Domain
		target   *string
		optional bool
	}{
		{a.GetTenant().GetId(), 1, &scope.TenantID, false}, {a.GetSite().GetId(), 19, &scope.SiteID, true},
		{a.GetSet().GetId(), 7, &scope.SetID, false}, {a.GetProducer().GetId(), 22, &scope.ProducerID, false},
		{a.GetNetwork().GetId(), 26, &scope.NetworkID, false},
	}
	for _, field := range fields {
		if field.optional && len(field.raw) == 0 {
			continue
		}
		id, err := pdid.From(field.raw)
		if err != nil || id.Domain() != field.domain {
			return camera.EnrollmentScope{}, camera.ErrInvalid
		}
		*field.target = id.String()
	}
	return scope, nil
}

// Credentials resolves only the versions already pinned in this execution's
// immutable JobInputs. It does not prepare local passwords or fall back to a
// Producer/person identity. Each use obtains a fresh current lease grant.
func (e *Execution) Credentials() (camera.AuthProvider, error) {
	if e == nil || e.credentials == nil {
		return nil, cpauth.ErrUnavailable
	}
	work := e.Snapshot()
	scope, err := credentialScope(work)
	if err != nil {
		return nil, err
	}
	refs := map[camera.AuthPurpose]camera.CredentialRef{}
	for purpose, ref := range map[camera.AuthPurpose]*api.CameraCredentialReference{
		camera.AuthInitialAdmin: work.GetJob().GetInputs().GetInitialCredential(),
		camera.AuthManagement:   work.GetJob().GetInputs().GetManagementCredential(),
		camera.AuthStreaming:    work.GetJob().GetInputs().GetStreamingCredential(),
	} {
		if ref != nil {
			refs[purpose] = camera.CredentialRef{Provider: ref.GetProvider(), ID: ref.GetId(), Version: ref.GetVersion(), Purpose: camera.AuthPurpose(ref.GetPurpose()), Scope: scope, Recoverable: ref.GetProvider() == cpauth.Name}
		}
	}
	return cpauth.NewResolver(e.ResolveCredential, cpauth.Options{Role: cpauth.Worker, Scope: scope, References: refs})
}

// ResolveCredential consumes a fresh request and attaches the actual current
// job fence before transport. It mutates only its grant field so the provider
// can correlate the returned delivery with that exact request after renewal.
func (e *Execution) ResolveCredential(ctx context.Context, request *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
	if e == nil || e.credentials == nil || request == nil {
		return nil, cpauth.ErrUnavailable
	}
	ctx, release := e.bound(ctx)
	defer release()
	e.mu.Lock()
	defer e.mu.Unlock()
	work, err := e.check(ctx)
	if err != nil {
		return nil, err
	}
	expires := work.GetJob().GetLeaseExpires()
	if expires == nil {
		return nil, camera.ErrExpired
	}
	request.SetJob(e.request())
	bounded, cancel := context.WithDeadline(ctx, expires.AsTime())
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	return e.credentials.Resolve(bounded, request)
}
