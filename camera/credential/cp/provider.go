// Package cp resolves immutable camera credential versions over an already
// authenticated CP connection. It never enrolls a machine, uses a person's
// session as a fallback, prepares a password on a Worker, or selects a file
// provider when the CP cannot answer.
package cp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential/material"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Role string

const (
	Name          = "cp"
	Producer Role = "producer"
	Worker   Role = "worker"
)

type ResolveFunc func(context.Context, *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error)

type Options struct {
	Role  Role
	Scope camera.EnrollmentScope
	// Workers use already prepared/pinned JobInputs. An explicit ExistingRef
	// remains exact; a missing one selects only this purpose's pinned ref.
	References     map[camera.AuthPurpose]camera.CredentialRef
	Job            func(context.Context) (*api.CameraWorkerCheckJobRequest, error)
	SourceID       string
	SourceRevision int64
	LeaseDuration  time.Duration
	Now            func() time.Time
}

type Provider struct {
	resolve ResolveFunc
	opts    Options
}

var _ camera.AuthProvider = (*Provider)(nil)
var _ camera.SourceAuthProvider = (*Provider)(nil)

func New(conn grpc.ClientConnInterface, opts Options) (*Provider, error) {
	if conn == nil {
		return nil, camera.ErrInvalid
	}
	client := api.NewCameraCredentialServiceClient(conn)
	return NewResolver(func(ctx context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
		return client.Resolve(ctx, r)
	}, opts)
}

// NewResolver supports an Execution-bound Worker resolver that attaches its
// current CheckJob grant and lifetime, without exposing the machine connection.
func NewResolver(resolve ResolveFunc, opts Options) (*Provider, error) {
	if resolve == nil || opts.Scope.TenantID == "" || opts.Role != Producer && opts.Role != Worker || opts.SourceRevision < 0 {
		return nil, camera.ErrInvalid
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	if opts.LeaseDuration < time.Second || opts.LeaseDuration > 300*time.Second {
		return nil, camera.ErrInvalid
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	refs := make(map[camera.AuthPurpose]camera.CredentialRef, len(opts.References))
	for purpose, ref := range opts.References {
		refs[purpose] = ref
	}
	opts.References = refs
	return &Provider{resolve: resolve, opts: opts}, nil
}

func (p *Provider) ForSource(source camera.SourceAuthContext) camera.AuthProvider {
	if p == nil {
		return (*Provider)(nil)
	}
	copy := *p
	copy.opts.SourceID, copy.opts.SourceRevision, copy.opts.Scope = source.SourceID, source.Revision, source.Scope
	return &copy
}

type safeFailure struct{ code codes.Code }

func (e safeFailure) Error() string              { return "central camera credential request failed" }
func (e safeFailure) GRPCStatus() *status.Status { return status.New(e.code, e.Error()) }
func (e safeFailure) Is(target error) bool {
	if e.code == codes.PermissionDenied {
		return target == camera.ErrForbidden
	}
	return target == camera.ErrAuthRequired
}

var ErrUnavailable error = safeFailure{codes.Unavailable}
var ErrRejected error = safeFailure{codes.PermissionDenied}
var ErrInvalidLease error = safeFailure{codes.FailedPrecondition}

func failure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Unavailable {
		return ErrUnavailable
	}
	return ErrRejected
}

func rank(value camera.AccessLevel) int {
	switch value {
	case camera.AccessRead:
		return 1
	case camera.AccessConfigure:
		return 2
	case camera.AccessAdministrator:
		return 3
	}
	return 0
}

func knownScope(expected, got camera.EnrollmentScope) bool {
	return expected.TenantID == got.TenantID && expected.SiteID == got.SiteID && expected.SetID == got.SetID && expected.ProducerID == got.ProducerID && (expected.NetworkID == "" || expected.NetworkID == got.NetworkID)
}

func id(raw []byte, domain pdid.Domain, optional bool) (string, error) {
	if len(raw) == 0 && optional {
		return "", nil
	}
	v, err := pdid.From(raw)
	if err != nil || v.Domain() != domain {
		return "", ErrInvalidLease
	}
	return v.String(), nil
}

func deliveryScope(d *api.CameraCredentialDelivery) (camera.EnrollmentScope, error) {
	if d == nil {
		return camera.EnrollmentScope{}, ErrInvalidLease
	}
	var scope camera.EnrollmentScope
	var err error
	if scope.TenantID, err = id(d.GetTenantId(), 1, false); err != nil {
		return scope, err
	}
	if scope.SiteID, err = id(d.GetSiteId(), 19, true); err != nil {
		return scope, err
	}
	if scope.SetID, err = id(d.GetSetId(), 7, false); err != nil {
		return scope, err
	}
	if scope.ProducerID, err = id(d.GetProducerId(), 22, false); err != nil {
		return scope, err
	}
	if scope.NetworkID, err = id(d.GetNetworkId(), 26, false); err != nil {
		return scope, err
	}
	if _, err = id(d.GetDeviceId(), 27, false); err != nil {
		return scope, err
	}
	if _, err = id(d.GetBindingId(), 29, false); err != nil {
		return scope, err
	}
	if _, err = id(d.GetSourceId(), 8, false); err != nil {
		return scope, err
	}
	if _, err = id(d.GetAssignmentId(), 28, false); err != nil {
		return scope, err
	}
	if d.GetAssignmentGeneration() <= 0 || d.GetSourceRevision() < 0 {
		return scope, ErrInvalidLease
	}
	return scope, nil
}

func (p *Provider) Acquire(ctx context.Context, request camera.AuthRequest) (camera.CredentialLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || p.resolve == nil {
		return nil, ErrUnavailable
	}
	if !knownScope(p.opts.Scope, request.Scope) || p.opts.Role == Producer && request.Purpose != camera.AuthStreaming {
		return nil, ErrRejected
	}
	var ref camera.CredentialRef
	if request.ExistingRef != nil {
		ref = *request.ExistingRef
	} else {
		ref = p.opts.References[request.Purpose]
	}
	if !ref.Authorizes(request.Scope, request.Purpose) || ref.Provider != Name || !ref.Recoverable {
		return nil, ErrRejected
	}
	credentialID, err := pdid.Parse(ref.ID)
	if err != nil || credentialID.Domain() != 32 || credentialID.String() != ref.ID {
		return nil, ErrRejected
	}
	access := request.Requirement.Access
	if access == "" {
		access = camera.AccessRead
	}
	if rank(access) == 0 {
		return nil, ErrRejected
	}
	wire := api.CameraCredentialResolveRequest_builder{Credential: api.CameraCredentialReference_builder{Provider: Name, Id: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}.Build(), LeaseSeconds: int64(p.opts.LeaseDuration / time.Second), Access: string(access), SourceRevision: p.opts.SourceRevision}.Build()
	if p.opts.SourceID != "" {
		source, err := pdid.Parse(p.opts.SourceID)
		if err != nil || source.Domain() != 8 {
			return nil, ErrRejected
		}
		wire.SetSource(api.SourceRef_builder{Id: source.Bytes()}.Build())
	}
	if p.opts.Job != nil {
		job, err := p.opts.Job(ctx)
		if err != nil {
			return nil, failure(ctx, err)
		}
		wire.SetJob(job)
	}
	response, err := p.resolve(ctx, wire)
	if response != nil {
		defer func() { clear(response.GetMaterial()); response.SetMaterial(nil) }()
	}
	if err != nil {
		return nil, failure(ctx, err)
	}
	if response == nil || response.GetCredential() == nil || response.GetExpires() == nil || response.GetExpires().CheckValid() != nil || !response.GetExpires().AsTime().After(p.opts.Now()) || response.GetExpires().AsTime().After(p.opts.Now().Add(300*time.Second)) {
		return nil, ErrInvalidLease
	}
	r := response.GetCredential()
	if r.GetProvider() != ref.Provider || r.GetId() != ref.ID || r.GetVersion() != ref.Version || r.GetPurpose() != string(ref.Purpose) || rank(camera.AccessLevel(response.GetAccess())) < rank(access) {
		return nil, ErrInvalidLease
	}
	scope, err := deliveryScope(response.GetDelivery())
	if err != nil || !knownScope(request.Scope, scope) {
		return nil, ErrInvalidLease
	}
	if p.opts.Role == Worker {
		job := wire.GetJob()
		if job == nil || job.GetLeaseGeneration() <= 0 || response.GetDelivery().GetLeaseGeneration() != job.GetLeaseGeneration() || response.GetDelivery().GetAssignmentGeneration() != job.GetAssignmentGeneration() || string(response.GetDelivery().GetJobId()) != string(job.GetRef().GetId()) {
			return nil, ErrInvalidLease
		}
	}
	if p.opts.SourceID != "" {
		actual, _ := id(response.GetDelivery().GetSourceId(), 8, false)
		if actual != p.opts.SourceID {
			return nil, ErrInvalidLease
		}
	}
	if p.opts.SourceRevision > 0 && response.GetDelivery().GetSourceRevision() != p.opts.SourceRevision {
		return nil, ErrInvalidLease
	}
	decoded, err := material.Parse(response.GetMaterial())
	if err != nil {
		return nil, ErrInvalidLease
	}
	var lease camera.CredentialLease
	err = decoded.Use(ctx, func(value camera.CredentialMaterial) error {
		lease = camera.NewCredentialLease(ref, value, response.GetExpires().AsTime())
		return nil
	})
	if err != nil {
		return nil, failure(ctx, err)
	}
	return lease, nil
}

const marker = "[central camera credential provider]"

func (*Provider) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, marker) }
func (*Provider) String() string               { return marker }
func (*Provider) GoString() string             { return marker }
func (*Provider) MarshalJSON() ([]byte, error) { return []byte(`"` + marker + `"`), nil }
func (*Provider) MarshalYAML() (any, error)    { return marker, nil }
