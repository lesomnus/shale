package core

import (
	"bytes"
	"context"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"google.golang.org/protobuf/proto"
)

// Heartbeats do not create application state. A known managed Source must name
// a current or bounded draining ticket of its actual assigned Producer; old
// reports cannot assert current ingest while another incarnation is starting.
func (s Core) cameraSourceReports(ctx context.Context, p *api.Producer, reports []*api.SourceReport) ([]*api.SourceReport, error) {
	var out []*api.SourceReport
	err := s.ownTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		for _, report := range reports {
			src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: report.GetSourceId()}.Build()}.Build())
			if err != nil {
				return err
			}
			if !bytes.Equal(src.GetTenant().GetId(), p.GetTenant().GetId()) || !bytes.Equal(src.GetSet().GetId(), p.GetSet().GetId()) {
				return denied("source heartbeat", "Source does not belong to this Producer Set")
			}
			row, err := c.ent(ctx).CameraBinding.Query().Where(camerabinding.SourceIdEQ(mustId(src.GetId()).Uuid())).Only(ctx)
			if ent.IsNotFound(err) {
				if report.GetCaptureEpoch() != "" {
					return invalid("capture_epoch", "unmanaged Source cannot assert a managed incarnation")
				}
				out = append(out, report)
				continue
			}
			if err != nil {
				return err
			}
			a, err := c.producerCatalogRow(ctx, api.CameraAssignmentRef_builder{Id: row.AssignmentId[:]}.Build(), mustId(p.GetId()), mustId(p.GetTenant().GetId()))
			if err != nil {
				return err
			}
			if report.GetCaptureEpoch() == "" {
				return invalid("capture_epoch", "managed heartbeat requires its capture incarnation")
			}
			grant, _, err := c.currentSourcePublishGrant(ctx, src, mustId(p.GetId()))
			if err != nil {
				return err
			}
			if grant.GetCaptureEpoch() == report.GetCaptureEpoch() {
				out = append(out, report)
				continue
			}
			b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build()}.Build())
			if err != nil {
				return err
			}
			known := append([]*api.CameraApplication{b.GetStartingApplication()}, b.GetPreviousApplications()...)
			retired := false
			for _, app := range known {
				if app.GetCaptureEpoch() == report.GetCaptureEpoch() && bytes.Equal(app.GetProducerId(), a.GetProducer().GetId()) && app.GetDrainDeadline() != nil && app.GetDrainDeadline().AsTime().After(s.d.now()) {
					retired = true
					break
				}
			}
			if !retired {
				return denied("source heartbeat", "capture incarnation is unknown or retired")
			}
			copy := proto.Clone(report).(*api.SourceReport)
			copy.SetInputUp(false)
			copy.SetFrameRate(0)
			copy.SetError("capture incarnation is draining")
			out = append(out, copy)
		}
		return nil
	})
	return out, err
}
