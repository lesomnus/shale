package core

import (
	"bytes"
	"context"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// currentSourceEpoch is called only after the enclosing operation authorized
// access to src. Binding lookups remain private, scoped and reference-only.
// A managed Source cannot obtain the legacy empty-epoch behavior by waiting
// before its first real Producer acknowledgment or by stopping collection.
func (s Core) currentSourceEpoch(ctx context.Context, src *api.Source) (epoch string, managed bool, err error) {
	app, managed, err := s.currentSourceApplication(ctx, src)
	if err != nil || !managed {
		return "", managed, err
	}
	return app.GetCaptureEpoch(), true, nil
}

func (s Core) cameraHasBinding(ctx context.Context, src *api.Source) (bool, error) {
	return s.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid())).Exist(ctx)
}

// cameraAllocationProfile is called inside the allocation transaction. It
// freezes old negotiated limits for an authorized bounded historical drain;
// neither collect=false nor a new desired revision relabels those old bytes.
func (s Core) cameraAllocationProfile(ctx context.Context, src *api.Source, epoch string, started time.Time, prefetch bool, who, tenant pdid.Id) (*api.SegmentProfile, string, bool, error) {
	row, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid())).Only(ctx)
	if ent.IsNotFound(err) {
		if epoch != "" {
			return nil, "", false, invalid("capture_epoch", "unmanaged Source cannot assert a managed incarnation")
		}
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", true, err
	}
	if who.Domain() != DomProducer {
		return nil, "", true, denied("managed allocation", "only the current Producer may allocate camera media")
	}
	if row.DateErased != nil {
		return nil, "", true, failed("managed binding was removed")
	}
	a, err := s.producerCatalogRow(ctx, api.CameraAssignmentRef_builder{Id: row.AssignmentId[:]}.Build(), who, tenant)
	if err != nil {
		return nil, "", true, err
	}
	b, err := s.own(ctx).CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build()}.Build())
	if err != nil {
		return nil, "", true, err
	}
	if !bytes.Equal(src.GetSet().GetId(), a.GetSet().GetId()) || !bytes.Equal(src.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(src.GetSite().GetId(), a.GetSite().GetId()) {
		return nil, "", true, denied("managed allocation", "Source scope changed")
	}
	validIdentity := func(app *api.CameraApplication) bool {
		return app.GetCaptureEpoch() != "" && bytes.Equal(app.GetProducerId(), who.Bytes()) && bytes.Equal(app.GetTarget().GetAssignmentId(), a.GetId()) && bytes.Equal(app.GetTarget().GetBindingId(), b.GetId()) && bytes.Equal(app.GetTarget().GetSourceId(), src.GetId())
	}
	current := func(app *api.CameraApplication) bool {
		return validIdentity(app) && b.GetCollect() && b.GetDesired() != nil && app.GetDrainCutoff() == nil && app.GetTarget().GetAssignmentGeneration() == a.GetGeneration() && app.GetTarget().GetSpecRevision() >= 1 && app.GetTarget().GetSpecRevision() <= b.GetDesired().GetRevision() && app.GetSequence() == b.GetStartingApplication().GetSequence() && app.GetCaptureEpoch() == b.GetStartingApplication().GetCaptureEpoch()
	}
	var app *api.CameraApplication
	if prefetch && epoch == "" {
		app = b.GetStartingApplication()
		if !current(app) {
			return nil, "", true, nil
		}
		epoch = app.GetCaptureEpoch()
	} else {
		if epoch == "" {
			return nil, "", true, invalid("capture_epoch", "managed Source requires an explicit current or draining capture incarnation")
		}
		// The applied row is audit state and may outlive history eviction.
		// When it is current, Starting names the same ticket; otherwise only
		// the bounded history can authorize another retired upload.
		known := append([]*api.CameraApplication{b.GetStartingApplication()}, b.GetPreviousApplications()...)
		for _, candidate := range known {
			if candidate.GetCaptureEpoch() == epoch && validIdentity(candidate) {
				app = candidate
				break
			}
		}
		if app == nil {
			return nil, "", true, status.Error(codes.Aborted, "capture incarnation is no longer authorized")
		}
		if !current(app) {
			if app.GetDrainCutoff() == nil || app.GetDrainDeadline() == nil || !app.GetDrainDeadline().AsTime().After(s.d.now()) || started.After(app.GetDrainCutoff().AsTime().Add(5*time.Second)) {
				return nil, "", true, failed("historical capture drain expired or segment is after its cutoff")
			}
		}
		if !prefetch && (app.GetDateStarted() == nil || started.Before(app.GetDateStarted().AsTime().Add(-5*time.Second))) {
			return nil, "", true, invalid("date_started", "segment predates its capture incarnation")
		}
	}
	if app.GetSegmentProfile().GetMaxBitrate() <= 0 || app.GetSegmentProfile().GetDurationSeconds() <= 0 {
		return nil, "", true, failed("capture incarnation has no negotiated storage limits")
	}
	return app.GetSegmentProfile(), epoch, true, nil
}

func (s Core) currentSourceApplication(ctx context.Context, src *api.Source) (*api.CameraApplication, bool, error) {
	row, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid())).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	b, err := s.own(ctx).CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	a, err := s.own(ctx).CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	app := b.GetAppliedApplication()
	if !b.GetCollect() || b.GetDesired() == nil || app.GetDateApplied() == nil || app.GetCaptureEpoch() == "" ||
		app.GetDrainCutoff() != nil ||
		app.GetSequence() != b.GetStartingApplication().GetSequence() || app.GetCaptureEpoch() != b.GetStartingApplication().GetCaptureEpoch() ||
		app.GetTarget().GetAssignmentGeneration() != a.GetGeneration() || app.GetTarget().GetSpecRevision() < 1 || app.GetTarget().GetSpecRevision() > b.GetDesired().GetRevision() ||
		!bytes.Equal(app.GetProducerId(), a.GetProducer().GetId()) || !bytes.Equal(app.GetTarget().GetAssignmentId(), a.GetId()) ||
		!bytes.Equal(app.GetTarget().GetBindingId(), b.GetId()) || !bytes.Equal(app.GetTarget().GetSourceId(), src.GetId()) ||
		!bytes.Equal(src.GetSet().GetId(), a.GetSet().GetId()) || !bytes.Equal(src.GetTenant().GetId(), b.GetTenant().GetId()) || !bytes.Equal(src.GetSite().GetId(), b.GetSite().GetId()) {
		return nil, true, failed("managed capture has no current applied Producer incarnation")
	}
	p, err := s.own(ctx).Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	if err := producerCatalogAssignment(mustId(p.GetId()), mustId(a.GetTenant().GetId()), p, a); err != nil {
		return nil, true, err
	}
	return app, true, nil
}

// currentSourcePublishGrant retains managed provenance even before first apply,
// after collection stops, and after a binding was soft-erased. Missing/invalid
// current starting authority produces a signed deny, never a legacy grant.
func (s Core) currentSourcePublishGrant(ctx context.Context, src *api.Source, producerId pdid.Id) (*api.CaptureEpochGrant, bool, error) {
	row, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid())).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	sequence := max(row.StartingApplication.GetSequence(), row.AppliedApplication.GetSequence())
	grant := api.CaptureEpochGrant_builder{SourceId: src.GetId(), Sequence: sequence}.Build()
	if row.DateErased != nil {
		return grant, true, nil
	}
	b, err := s.own(ctx).CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	a, err := s.own(ctx).CameraAssignment().Get(ctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	if producerId.Domain() != DomProducer || !bytes.Equal(a.GetProducer().GetId(), producerId.Bytes()) {
		return nil, true, denied("publish epoch", "managed Source belongs to another assigned Producer")
	}
	app := b.GetStartingApplication()
	if !b.GetCollect() || b.GetDesired() == nil || app.GetCaptureEpoch() == "" || app.GetDrainCutoff() != nil || app.GetTarget().GetAssignmentGeneration() != a.GetGeneration() || app.GetTarget().GetSpecRevision() < 1 || app.GetTarget().GetSpecRevision() > b.GetDesired().GetRevision() ||
		!bytes.Equal(app.GetProducerId(), a.GetProducer().GetId()) || !bytes.Equal(app.GetTarget().GetAssignmentId(), a.GetId()) || !bytes.Equal(app.GetTarget().GetBindingId(), b.GetId()) || !bytes.Equal(app.GetTarget().GetSourceId(), src.GetId()) {
		return grant, true, nil
	}
	if !bytes.Equal(src.GetSet().GetId(), a.GetSet().GetId()) || !bytes.Equal(src.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(src.GetSite().GetId(), a.GetSite().GetId()) {
		return nil, true, denied("publish epoch", "Source does not belong to its camera assignment")
	}
	p, err := s.own(ctx).Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
	if err != nil {
		return nil, true, err
	}
	if err := producerCatalogAssignment(mustId(p.GetId()), mustId(a.GetTenant().GetId()), p, a); err != nil {
		return nil, true, err
	}
	grant.SetCaptureEpoch(app.GetCaptureEpoch())
	return grant, true, nil
}

func (s Core) cameraManagedSet(ctx context.Context, set []byte) (bool, error) {
	return s.ent(ctx).CameraAssignment.Query().Where(cameraassignment.SetIdEQ(mustId(set).Uuid())).Exist(ctx)
}

func (s Core) cameraAllocationPreflight(ctx context.Context, src *api.Source, epoch string, started time.Time, who, tenant pdid.Id) (*api.SegmentProfile, bool, error) {
	managed, err := s.cameraHasBinding(ctx, src)
	if err != nil {
		return nil, managed, err
	}
	if !managed {
		if epoch != "" {
			return nil, false, invalid("capture_epoch", "Source no longer has managed capture authority")
		}
		return nil, false, nil
	}
	var frozen *api.SegmentProfile
	err = s.ownTx(ctx, func(ctx context.Context, nx api.Server) error {
		var err error
		frozen, _, _, err = New(nx, s.d).cameraAllocationProfile(ctx, src, epoch, started, true, who, tenant)
		return err
	})
	return frozen, true, err
}
