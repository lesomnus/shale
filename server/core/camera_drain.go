package core

import (
	"context"
	"sort"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A stopped/superseded ticket can finish already captured RAM data under its
// frozen negotiated limits. Repeated control writes never extend this window.
const cameraDrainWindow = 5 * time.Minute

func cameraDrainApplication(app *api.CameraApplication, now time.Time) *api.CameraApplication {
	if app == nil || app.GetCaptureEpoch() == "" {
		return app
	}
	out := proto.Clone(app).(*api.CameraApplication)
	if out.GetDrainCutoff() == nil {
		out.SetDrainCutoff(timestamppb.New(now))
		out.SetDrainDeadline(timestamppb.New(now.Add(cameraDrainWindow)))
	}
	return out
}

func (s Core) cameraRetireAssignmentApplications(ctx context.Context, nx api.Server, a *api.CameraAssignment) error {
	rows, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.AssignmentIdEQ(mustId(a.GetId()).Uuid()), camerabinding.DateErasedIsNil()).All(ctx)
	if err != nil {
		return err
	}
	now := s.d.now()
	for _, row := range rows {
		if row.StartingApplication == nil && row.AppliedApplication == nil {
			continue
		}
		if _, err := nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build(), StartingApplication: cameraDrainApplication(row.StartingApplication, now), AppliedApplication: cameraDrainApplication(row.AppliedApplication, now), DateUpdated: timestamppb.New(row.DateUpdated)}.Build()); err != nil {
			return err
		}
	}
	return nil
}

// Keep a bounded durable ledger for ordinary process reconnects: their closed
// segments may still be unallocated when a newer capture starts. The ledger
// never grants application or live readiness and retries never extend dates.
func cameraDrainHistory(b *api.CameraBinding, now time.Time) []*api.CameraApplication {
	seen := map[string]bool{}
	var out []*api.CameraApplication
	all := append([]*api.CameraApplication{}, b.GetPreviousApplications()...)
	all = append(all, b.GetAppliedApplication(), b.GetStartingApplication())
	for _, prior := range all {
		if prior.GetCaptureEpoch() == "" || seen[prior.GetCaptureEpoch()] {
			continue
		}
		app := cameraDrainApplication(prior, now)
		if app.GetDrainDeadline() == nil || !app.GetDrainDeadline().AsTime().After(now) {
			continue
		}
		seen[app.GetCaptureEpoch()] = true
		out = append(out, app)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetSequence() > out[j].GetSequence() })
	if len(out) > 16 {
		out = out[:16]
	}
	return out
}
