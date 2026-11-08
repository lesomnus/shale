package core

import (
	"bytes"
	"context"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	cc "github.com/lesomnus/shale/internal/cameracredential"
	"github.com/lesomnus/shale/internal/camerasecret"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s Core) credentialProducerActor(ctx context.Context) (*api.Producer, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	prior, ok := f.Row.(*api.Producer)
	if f.Actor.Domain() != DomProducer || !ok || !bytes.Equal(prior.GetId(), f.Actor.Bytes()) {
		return nil, denied("credential", "an adopted producer identity is required")
	}
	p, err := s.own(ctx).Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if p.GetState() != api.HostState_HOST_STATE_ADOPTED || p.GetCertSerial() == "" || p.GetCertSerial() != prior.GetCertSerial() || !bytes.Equal(p.GetTenant().GetId(), f.Tenant.Bytes()) || !bytes.Equal(p.GetSet().GetId(), prior.GetSet().GetId()) {
		return nil, denied("credential", "producer adoption, set or certificate was replaced")
	}
	if p.GetDateCertExpires() == nil || !p.GetDateCertExpires().AsTime().After(s.d.now()) {
		return nil, denied("credential", "producer certificate expired")
	}
	return p, nil
}

type credentialGrant struct {
	ref      cc.Reference
	delivery *api.CameraCredentialDelivery
	expires  time.Time
}

func credentialMatches(a, b *api.CameraCredentialReference) bool {
	return a != nil && b != nil && proto.Equal(a, b)
}

func (s Core) credentialWorkerGrant(ctx context.Context, nx api.Server, r *api.CameraCredentialResolveRequest) (credentialGrant, error) {
	if r.GetJob() == nil || r.GetSource() != nil || r.GetCacheSeconds() != 0 || r.GetSourceRevision() != 0 {
		return credentialGrant{}, denied("credential", "workers resolve only a leased job's exact credential")
	}
	work, err := s.cameraWorkerWork(ctx, r.GetJob())
	if err != nil {
		return credentialGrant{}, err
	}
	j, a, b, d := work.GetJob(), work.GetAssignment(), work.GetBinding(), work.GetDevice()
	var pinned *api.CameraCredentialReference
	switch r.GetCredential().GetPurpose() {
	case string(camerasecret.InitialAdmin):
		if !j.GetInputs().GetAllowActivation() {
			return credentialGrant{}, denied("credential", "job does not authorize activation")
		}
		pinned = j.GetInputs().GetInitialCredential()
	case string(camerasecret.Management):
		pinned = j.GetInputs().GetManagementCredential()
	case string(camerasecret.Streaming):
		pinned = j.GetInputs().GetStreamingCredential()
	default:
		return credentialGrant{}, denied("credential", "job does not authorize this credential purpose")
	}
	if !credentialMatches(pinned, r.GetCredential()) {
		return credentialGrant{}, denied("credential", "credential is not pinned in this job")
	}
	ref, row, err := s.credentialReference(ctx, nx, r.GetCredential())
	if err != nil {
		return credentialGrant{}, err
	}
	if ref.Subject.DeviceID != mustId(d.GetId()) || !ref.Subject.BindingID.IsZero() && ref.Subject.BindingID != mustId(b.GetId()) {
		return credentialGrant{}, denied("credential", "credential belongs to another job device or binding")
	}
	if err := cameraSameScope(row.GetTenant().GetId(), row.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
		return credentialGrant{}, err
	}
	w, err := s.cameraWorkerActor(ctx)
	if err != nil {
		return credentialGrant{}, err
	}
	return credentialGrant{ref: ref, expires: minTime(j.GetLeaseExpires().AsTime(), w.GetDateCertExpires().AsTime()), delivery: api.CameraCredentialDelivery_builder{
		TenantId: a.GetTenant().GetId(), SiteId: a.GetSite().GetId(), SetId: a.GetSet().GetId(), ProducerId: a.GetProducer().GetId(),
		NetworkId: a.GetNetwork().GetId(), DeviceId: d.GetId(), BindingId: b.GetId(), SourceId: b.GetSource().GetId(),
		AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), SourceRevision: b.GetDesired().GetRevision(),
		LeaseGeneration: j.GetLeaseGeneration(), JobId: j.GetId(),
	}.Build()}, nil
}

func (s Core) credentialProducerGrant(ctx context.Context, nx api.Server, r *api.CameraCredentialResolveRequest) (credentialGrant, error) {
	if r.GetJob() != nil || r.GetCredential().GetPurpose() != string(camerasecret.Streaming) {
		return credentialGrant{}, denied("credential", "producers resolve only assigned streaming credentials")
	}
	p, err := s.credentialProducerActor(ctx)
	if err != nil {
		return credentialGrant{}, err
	}
	ref, row, err := s.credentialReference(ctx, nx, r.GetCredential())
	if err != nil {
		return credentialGrant{}, err
	}
	if err := cameraSameScope(row.GetTenant().GetId(), row.GetSite().GetId(), p.GetTenant().GetId(), p.GetSite().GetId()); err != nil {
		return credentialGrant{}, err
	}
	var bindingID []byte
	if r.GetSource() != nil {
		src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: r.GetSource()}.Build())
		if err != nil {
			return credentialGrant{}, err
		}
		if !bytes.Equal(src.GetSet().GetId(), p.GetSet().GetId()) {
			return credentialGrant{}, denied("credential", "source belongs to another producer set")
		}
		bound, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid()),
			camerabinding.TenantIdEQ(mustId(p.GetTenant().GetId()).Uuid()), camerabinding.DateErasedIsNil()).Select(camerabinding.FieldId).Only(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				return credentialGrant{}, denied("credential", "source has no current camera binding")
			}
			return credentialGrant{}, credentialError(err)
		}
		bindingID = bound.Id[:]
	} else if !ref.Subject.BindingID.IsZero() {
		bindingID = ref.Subject.BindingID.Bytes()
	} else {
		return credentialGrant{}, invalid("source", "a device-wide streaming credential requires an explicit Source")
	}
	b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: bindingID}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	if !ref.Subject.BindingID.IsZero() && !bytes.Equal(bindingID, ref.Subject.BindingID.Bytes()) || !bytes.Equal(b.GetDevice().GetId(), ref.Subject.DeviceID.Bytes()) {
		return credentialGrant{}, denied("credential", "credential belongs to another source device or binding")
	}
	a, err := nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	if !bytes.Equal(a.GetProducer().GetId(), p.GetId()) || !bytes.Equal(a.GetSet().GetId(), p.GetSet().GetId()) {
		return credentialGrant{}, denied("credential", "source is not assigned to this producer")
	}
	if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), p.GetTenant().GetId(), p.GetSite().GetId()); err != nil {
		return credentialGrant{}, err
	}
	if err := s.cameraLockNetwork(ctx, a.GetNetwork().GetId()); err != nil {
		return credentialGrant{}, err
	}
	if err := s.cameraLockAssignment(ctx, a.GetId()); err != nil {
		return credentialGrant{}, err
	}
	// Reload after locking so stop/handover/publish cannot be missed.
	a, err = nx.CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	b, err = nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: bindingID}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	if !bytes.Equal(a.GetProducer().GetId(), p.GetId()) || !bytes.Equal(a.GetSet().GetId(), p.GetSet().GetId()) || !b.GetCollect() || !credentialMatches(b.GetDesired().GetCredential(), r.GetCredential()) {
		return credentialGrant{}, denied("credential", "source no longer publishes this streaming credential")
	}
	if !bytes.Equal(b.GetAssignment().GetId(), a.GetId()) || !bytes.Equal(b.GetDesired().GetBindingId(), b.GetId()) || !bytes.Equal(b.GetDesired().GetSourceId(), b.GetSource().GetId()) || b.GetDesired().GetRevision() <= 0 {
		return credentialGrant{}, denied("credential", "source publication is inconsistent")
	}
	if r.GetSourceRevision() > 0 && r.GetSourceRevision() != b.GetDesired().GetRevision() {
		return credentialGrant{}, status.Error(codes.Aborted, "source revision changed before credential resolution")
	}
	src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	set, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: a.GetSet().GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: b.GetDevice().GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
	if err != nil {
		return credentialGrant{}, err
	}
	if !bytes.Equal(src.GetSet().GetId(), set.GetId()) || !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) {
		return credentialGrant{}, denied("credential", "source set or camera network changed")
	}
	for _, scope := range [][2][]byte{{b.GetTenant().GetId(), b.GetSite().GetId()}, {src.GetTenant().GetId(), src.GetSite().GetId()},
		{set.GetTenant().GetId(), set.GetSite().GetId()}, {d.GetTenant().GetId(), d.GetSite().GetId()}, {n.GetTenant().GetId(), n.GetSite().GetId()}} {
		if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), scope[0], scope[1]); err != nil {
			return credentialGrant{}, err
		}
	}
	return credentialGrant{ref: ref, expires: p.GetDateCertExpires().AsTime(), delivery: api.CameraCredentialDelivery_builder{
		TenantId: a.GetTenant().GetId(), SiteId: a.GetSite().GetId(), SetId: a.GetSet().GetId(), ProducerId: p.GetId(), NetworkId: n.GetId(),
		DeviceId: d.GetId(), BindingId: b.GetId(), SourceId: src.GetId(), AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), SourceRevision: b.GetDesired().GetRevision(),
	}.Build()}, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func credentialAccessRank(value camera.AccessLevel) int {
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

func (s Core) credentialVersionMetadata(ctx context.Context, ref cc.Reference) (cc.Version, error) {
	row, err := s.ent(ctx).CameraCredentialVersion.Query().Where(cameracredentialversion.CredentialIdEQ(ref.CredentialID.Uuid()),
		cameracredentialversion.VersionEQ(ref.Version), cameracredentialversion.DateErasedIsNil()).
		Select(cameracredentialversion.FieldAccess, cameracredentialversion.FieldBackend, cameracredentialversion.FieldOrigin,
			cameracredentialversion.FieldDateCreated, cameracredentialversion.FieldDateRevoked).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return cc.Version{}, credentialError(cc.ErrNotFound)
		}
		return cc.Version{}, credentialError(err)
	}
	if row.DateRevoked != nil {
		return cc.Version{}, credentialError(cc.ErrRevoked)
	}
	origin := cc.Generated
	switch api.CameraCredentialOrigin(row.Origin) {
	case api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_IMPORTED:
		origin = cc.Imported
	case api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_REUSED:
		origin = cc.Reused
	}
	return cc.Version{Ref: ref, Access: camera.AccessLevel(row.Access), Backend: row.Backend, Origin: origin, CreatedAt: row.DateCreated}, nil
}

func (s coreCameraCredential) Resolve(ctx context.Context, r *api.CameraCredentialResolveRequest) (*api.CameraCredentialLease, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	seconds := r.GetLeaseSeconds()
	access := camera.AccessLevel(r.GetAccess())
	if access == "" {
		access = camera.AccessRead
	}
	if credentialAccessRank(access) == 0 {
		return nil, invalid("access", "a known credential access requirement is required")
	}
	if seconds == 0 {
		seconds = 30
	}
	if seconds < 1 || seconds > 300 || r.GetCacheSeconds() < 0 || r.GetCacheSeconds() > 300 || r.GetSourceRevision() < 0 || r.GetCacheSeconds() > 0 && r.GetSourceRevision() == 0 {
		return nil, invalid("lease", "bounded lease/cache durations and an exact cache Source revision are required")
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	var transact func(context.Context, func(context.Context, api.Server) error) error
	switch f.Actor.Domain() {
	case DomHolder:
		if r.GetJob() != nil || r.GetSource() != nil || r.GetCacheSeconds() != 0 || r.GetSourceRevision() != 0 {
			return nil, invalid("credential", "operator recovery requests contain only an exact credential reference")
		}
		transact = s.tx
	case DomCameraWorker:
		if _, err := s.cameraWorkerActor(ctx); err != nil {
			return nil, err
		}
		transact = s.ownTx
	case DomProducer:
		if _, err := s.credentialProducerActor(ctx); err != nil {
			return nil, err
		}
		transact = s.ownTx
	default:
		return nil, denied("credential", "this actor has no credential delivery role")
	}
	var out *api.CameraCredentialLease
	err = transact(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		var grant credentialGrant
		var err error
		switch f.Actor.Domain() {
		case DomCameraWorker:
			grant, err = c.credentialWorkerGrant(ctx, nx, r)
		case DomProducer:
			grant, err = c.credentialProducerGrant(ctx, nx, r)
		default:
			var row *api.CameraCredential
			grant.ref, row, err = c.credentialReference(ctx, nx, r.GetCredential())
			if err == nil {
				grant.delivery = api.CameraCredentialDelivery_builder{TenantId: row.GetTenant().GetId(), SiteId: row.GetSite().GetId(), DeviceId: row.GetDevice().GetId(), BindingId: row.GetBinding().GetId()}.Build()
				grant.expires = c.d.now().Add(time.Duration(seconds) * time.Second)
			}
		}
		if err != nil {
			return err
		}
		expires := minTime(grant.expires, c.d.now().Add(time.Duration(seconds)*time.Second))
		if !expires.After(c.d.now()) {
			return denied("credential", "credential delivery grant expired")
		}
		store, err := c.credentialStore(ctx)
		if err != nil {
			return credentialError(err)
		}
		metadata, err := c.credentialVersionMetadata(ctx, grant.ref)
		if err != nil {
			return err
		}
		if credentialAccessRank(metadata.Access) < credentialAccessRank(access) {
			return denied("credential", "stored account does not meet this access requirement")
		}
		bounded, release := context.WithTimeout(ctx, expires.Sub(c.d.now()))
		defer release()
		material, err := store.Resolve(bounded, grant.ref)
		if err != nil {
			return credentialError(err)
		}
		metadata, err = c.credentialVersionMetadata(ctx, grant.ref)
		if err != nil {
			return err
		}
		if err := c.credentialAudit(ctx, metadata, api.CameraCredentialService_Resolve_FullMethodName); err != nil {
			return err
		}
		payload, err := material.MarshalBinary()
		if err != nil {
			return credentialError(err)
		}
		out = api.CameraCredentialLease_builder{Material: payload, Credential: credentialWire(grant.ref), Expires: timestamppb.New(expires), Delivery: grant.delivery, Access: string(metadata.Access)}.Build()
		if r.GetCacheSeconds() > 0 {
			out.SetCacheUntil(timestamppb.New(minTime(grant.expires, c.d.now().Add(time.Duration(r.GetCacheSeconds())*time.Second))))
		}
		return nil
	})
	if err == nil {
		switch f.Actor.Domain() {
		case DomCameraWorker:
			_, err = s.cameraWorkerActor(ctx)
		case DomProducer:
			_, err = s.credentialProducerActor(ctx)
		}
	}
	if err == nil && (out == nil || out.GetExpires() == nil || !out.GetExpires().AsTime().After(s.d.now())) {
		err = status.Error(codes.DeadlineExceeded, "credential delivery grant expired")
	}
	if err != nil {
		if out != nil {
			clear(out.GetMaterial())
			out.SetMaterial(nil)
		}
		return nil, cameraConflict(err)
	}
	return out, nil
}
