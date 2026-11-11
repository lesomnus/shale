// Package cp reads a Producer's assigned catalog through its authenticated CP
// connection. It neither provisions cameras nor distributes credential material.
package cp

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Options struct {
	ProducerID, TenantID, SiteID, SetID string
	// AssignmentID optionally pins an explicit assignment. Otherwise the unique
	// assignment of SetID is requested; no tenant-wide list or first-row pick.
	AssignmentID string
	PollInterval time.Duration
	CallTimeout  time.Duration
	// Cache is an optional shared notification optimization. It must receive only
	// centrally committed snapshots; every Snapshot still reads durable CP state.
	Cache *source.Memory
}

type Provider struct {
	client  api.CameraAssignmentServiceClient
	options Options
	ref     *api.CameraAssignmentRef
	cache   *source.Memory
}

var _ source.Provider = (*Provider)(nil)

func identifier(raw string, domain pdid.Domain) (pdid.Id, error) {
	id, err := pdid.Parse(raw)
	if err != nil || id.IsZero() || id.Domain() != domain {
		return pdid.Nil, source.ErrInvalid
	}
	return id, nil
}

func New(conn grpc.ClientConnInterface, options Options) (*Provider, error) {
	if conn == nil || options.PollInterval < 0 || options.CallTimeout < 0 {
		return nil, source.ErrInvalid
	}
	for _, field := range []struct {
		value  *string
		domain pdid.Domain
	}{{&options.ProducerID, 22}, {&options.TenantID, 1}, {&options.SetID, 7}} {
		id, err := identifier(*field.value, field.domain)
		if err != nil {
			return nil, err
		}
		*field.value = id.String()
	}
	if options.SiteID != "" {
		id, err := identifier(options.SiteID, 19)
		if err != nil {
			return nil, err
		}
		options.SiteID = id.String()
	}
	set, _ := pdid.Parse(options.SetID)
	ref := api.CameraAssignmentBySet(api.SetRef_builder{Id: set.Bytes()}.Build())
	if options.AssignmentID != "" {
		id, err := identifier(options.AssignmentID, 28)
		if err != nil {
			return nil, err
		}
		options.AssignmentID = id.String()
		ref = api.CameraAssignmentRef_builder{Id: id.Bytes()}.Build()
	}
	if options.PollInterval == 0 {
		options.PollInterval = time.Second
	}
	if options.CallTimeout == 0 {
		options.CallTimeout = 5 * time.Second
	}
	cache := options.Cache
	if cache == nil {
		cache = &source.Memory{}
	}
	return &Provider{client: api.NewCameraAssignmentServiceClient(conn), options: options, ref: ref, cache: cache}, nil
}

// Snapshot returns only a successfully validated full read. Errors leave the
// cache untouched and never masquerade as a successful empty catalog.
func (p *Provider) Snapshot(ctx context.Context) (source.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return source.Snapshot{}, err
	}
	call, cancel := context.WithTimeout(ctx, p.options.CallTimeout)
	defer cancel()
	v, err := p.client.Snapshot(call, api.CameraAssignmentSnapshotRequest_builder{Ref: p.ref}.Build())
	if err != nil {
		return source.Snapshot{}, rpcError(ctx, err)
	}
	next, err := decode(v)
	if err != nil {
		return source.Snapshot{}, err
	}
	if next.Scope.ProducerID != p.options.ProducerID || next.Scope.TenantID != p.options.TenantID || next.Scope.SetID != p.options.SetID || next.Scope.SiteID != p.options.SiteID || (p.options.AssignmentID != "" && next.Revision.AssignmentID != p.options.AssignmentID) {
		return source.Snapshot{}, source.ErrConflict
	}
	if err := p.cache.Publish(ctx, next); err != nil {
		return source.Snapshot{}, err
	}
	return next.Clone(), nil
}

// WaitForChange uses bounded full reads rather than a broader generated Watch
// grant. A local committed-cache hint can wake it earlier; timeouts resynchronize
// from CP even when every local notification was lost.
func (p *Provider) WaitForChange(ctx context.Context, after source.Revision) (source.Revision, error) {
	for {
		if _, err := p.Snapshot(ctx); err != nil {
			return source.Revision{}, err
		}
		wait, cancel := context.WithTimeout(ctx, p.options.PollInterval)
		revision, err := p.cache.WaitForChange(wait, after)
		cancel()
		if err == nil {
			return revision, nil
		}
		if ctx.Err() != nil {
			return source.Revision{}, ctx.Err()
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return source.Revision{}, err
		}
	}
}

func rpcError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	code := status.Code(err)
	// Do not repeat transport error strings or CP status messages: they can
	// contain endpoints or credentials supplied by an unexpected implementation.
	safe := status.Error(code, "source catalog request failed")
	switch code {
	case codes.Unknown, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return errors.Join(source.ErrUnavailable, safe)
	default:
		return safe
	}
}

func idString(raw []byte) (string, error) {
	id, err := pdid.From(raw)
	if err != nil {
		return "", source.ErrInvalid
	}
	return id.String(), nil
}

func decode(v *api.CameraCatalogSnapshot) (source.Snapshot, error) {
	if v == nil || v.GetAssignment() == nil {
		return source.Snapshot{}, source.ErrInvalid
	}
	a := v.GetAssignment()
	out := source.Snapshot{Revision: source.Revision{Generation: a.GetGeneration(), Catalog: v.GetRevision()}}
	for _, field := range []struct {
		raw []byte
		out *string
	}{{a.GetId(), &out.Revision.AssignmentID}, {a.GetTenant().GetId(), &out.Scope.TenantID}, {a.GetSet().GetId(), &out.Scope.SetID}, {a.GetProducer().GetId(), &out.Scope.ProducerID}, {a.GetNetwork().GetId(), &out.Scope.NetworkID}} {
		id, err := idString(field.raw)
		if err != nil {
			return source.Snapshot{}, err
		}
		*field.out = id
	}
	if len(a.GetSite().GetId()) > 0 {
		id, err := idString(a.GetSite().GetId())
		if err != nil {
			return source.Snapshot{}, err
		}
		out.Scope.SiteID = id
	}
	if a.GetCatalogRevision() != v.GetRevision() {
		return source.Snapshot{}, source.ErrInvalid
	}
	for _, spec := range v.GetSources() {
		if spec == nil || spec.GetProfile() == nil {
			return source.Snapshot{}, source.ErrInvalid
		}
		binding, err := idString(spec.GetBindingId())
		if err != nil {
			return source.Snapshot{}, err
		}
		id, err := idString(spec.GetSourceId())
		if err != nil {
			return source.Snapshot{}, err
		}
		profile := spec.GetProfile()
		ms := profile.GetKeyframeIntervalMs()
		if ms < 0 || ms > math.MaxInt64/int64(time.Millisecond) {
			return source.Snapshot{}, source.ErrInvalid
		}
		next := source.Spec{
			BindingID: binding, SourceID: id, Revision: spec.GetRevision(), Alias: spec.GetAlias(), Name: spec.GetName(), Input: spec.GetInput(),
			Profile: source.Profile{Codec: profile.GetCodec(), Width: int(profile.GetWidth()), Height: int(profile.GetHeight()), FPS: profile.GetFps(), MaxBitrate: profile.GetMaxBitrateBps(), KeyframeInterval: time.Duration(ms) * time.Millisecond},
			Labels:  spec.GetLabels(), Zone: spec.GetZone(),
		}
		if ref := spec.GetCredential(); ref != nil {
			next.Credential = &source.CredentialRef{Provider: ref.GetProvider(), ID: ref.GetId(), Version: ref.GetVersion(), Purpose: ref.GetPurpose()}
		}
		out.Sources = append(out.Sources, next)
	}
	return source.Normalize(out)
}
