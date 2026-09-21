package core

import (
	"context"
	"strings"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/device"
	"github.com/lesomnus/shale/internal/ent/sink"
)

// registerSinks applies what a node reports about its devices and sinks
// (§27, §28.3): rows are created on first report, attached when free, and
// refused while another live node holds them. The CP believes a node only
// about what is attached to it.
func (s Core) registerSinks(ctx context.Context, srv api.Server, nodeId pdid.Id, devices []*api.DeviceReport, sinks []*api.SinkReport) ([]*api.SinkAnswer, error) {
	now := s.d.now()
	_, place, _, _, err := s.policies(ctx)
	if err != nil {
		return nil, err
	}
	clamp := maxSinkCapacity(place)

	// The sinks this report names; one of this node's that it does not is
	// pending adoption below.
	reported := map[string]bool{}

	// Devices by hardware id, from both lists.
	reports := map[string]*api.DeviceReport{}
	for _, d := range devices {
		reports[strings.ToLower(d.GetHardwareId())] = d
	}
	for _, sr := range sinks {
		hid := strings.ToLower(sr.GetDeviceHardwareId())
		if _, ok := reports[hid]; !ok && hid != "" {
			reports[hid] = api.DeviceReport_builder{HardwareId: hid, Capacity: sr.GetCapacity()}.Build()
		}
	}

	deviceIds := map[string]pdid.Id{}
	for hid, dr := range reports {
		row, err := s.d.Ent.Device.Query().Where(device.HardwareIdEQ(hid)).First(ctx)
		if err != nil && !ent.IsNotFound(err) {
			return nil, err
		}
		if row == nil {
			id := pdid.New(DomDevice)
			if _, err := srv.Device().Add(ctx, api.DeviceAddRequest_builder{
				Id:         id.Bytes(),
				Alias:      "dev-" + aliasSuffix(id),
				Node:       api.NodeRef_builder{Id: nodeId.Bytes()}.Build(),
				HardwareId: hid,
				Slot:       dr.GetSlot(),
				Model:      dr.GetModel(),
				Health:     api.DeviceHealth_DEVICE_HEALTH_HEALTHY,
				Capacity:   dr.GetCapacity(),
				Report:     dr,
				Smart:      dr.GetSmart(),
				DateSeen:   timestamppb.New(now),
			}.Build()); err != nil {
				return nil, err
			}
			deviceIds[hid] = id
			continue
		}

		id := pdid.Id(row.Id)
		deviceIds[hid] = id
		if row.DateErased != nil {
			continue
		}
		// Attached to another live node: leave it; a re-homed device follows
		// its sinks below.
		patch := api.DevicePatchRequest_builder{
			Ref:              api.DeviceRef_builder{Id: id.Bytes()}.Build(),
			Report:           dr,
			DateSeen:         timestamppb.New(now),
			DateUpdatedForce: z.Ptr(true),
		}
		if dr.GetSmart() != nil {
			patch.Smart = dr.GetSmart()
		}
		if dr.GetCapacity() > 0 {
			patch.Capacity = z.Ptr(dr.GetCapacity())
		}
		if isZero(row.NodeId) || row.NodeId == nodeId.Uuid() || !s.nodeAlive(ctx, pdid.Id(row.NodeId), now) {
			patch.Node = api.NodeRef_builder{Id: nodeId.Bytes()}.Build()
		} else {
			// Another live node holds it: what this node says about the
			// device, its SMART above all, is not taken (§33.7). Its sinks
			// are refused below for the same reason.
			continue
		}
		if _, err := srv.Device().Patch(ctx, patch.Build()); err != nil {
			return nil, err
		}
		// What the report adds to the failure score (§27): counters that
		// rose since the last report, and SMART.
		if delta, reasons := reportDelta(row.Report, dr); delta > 0 {
			if err := s.scoreDevice(ctx, srv, row, delta, reasons, now); err != nil {
				return nil, err
			}
		}
	}

	var answers []*api.SinkAnswer
	for _, sr := range sinks {
		sid, err := pdid.From(sr.GetSinkId())
		if err == nil {
			reported[string(sid.Bytes())] = true
		}
		if err != nil || sid.Domain() != DomSink {
			s.d.log().Warn("sink report with a bad id", "node", nodeId.String(), "path", sr.GetPath())
			continue
		}
		devId, ok := deviceIds[strings.ToLower(sr.GetDeviceHardwareId())]
		if !ok {
			continue
		}

		row, err := s.d.Ent.Sink.Query().Where(sink.IdEQ(sid.Uuid())).First(ctx)
		if err != nil && !ent.IsNotFound(err) {
			return nil, err
		}

		capacity := sr.GetCapacity()
		clamped := capacity > clamp
		warnings := append([]string(nil), sr.GetWarnings()...)
		if clamped {
			// The row holds the clamped value (§27, §33.7): a node's word
			// about its size goes no further than max_sink_capacity.
			warnings = append(warnings, "capacity clamped by max_sink_capacity")
			capacity = clamp
		}

		if row == nil {
			if _, err := srv.Sink().Add(ctx, api.SinkAddRequest_builder{
				Id:              sid.Bytes(),
				Alias:           "sink-" + aliasSuffix(sid),
				Node:            api.NodeRef_builder{Id: nodeId.Bytes()}.Build(),
				Device:          api.DeviceRef_builder{Id: devId.Bytes()}.Build(),
				Path:            sr.GetPath(),
				Capacity:        capacity,
				Free:            sr.GetFree(),
				Pressure:        sr.GetPressure(),
				Capabilities:    sr.GetCapabilities(),
				Attachment:      api.SinkAttachment_SINK_ATTACHMENT_ATTACHED,
				AcceptWrites:    true,
				DateSeen:        timestamppb.New(now),
				UploadsInFlight: sr.GetUploadsInFlight(),
				ReportedBy:      nodeId.Bytes(),
				Warnings:        warnings,
				CapacityClamped: clamped,
				Laminae:         sr.GetLaminae(),
			}.Build()); err != nil {
				return nil, err
			}
			answers = append(answers, api.SinkAnswer_builder{SinkId: sid.Bytes(), Attachment: api.SinkAttachment_SINK_ATTACHMENT_ATTACHED, Serve: true, AcceptWrites: true}.Build())
			continue
		}

		serve := true
		attachment := api.SinkAttachment(row.Attachment)
		patch := api.SinkPatchRequest_builder{
			Ref:              api.SinkRef_builder{Id: sid.Bytes()}.Build(),
			Path:             z.Ptr(sr.GetPath()),
			Capacity:         z.Ptr(capacity),
			Free:             z.Ptr(sr.GetFree()),
			Pressure:         z.Ptr(sr.GetPressure()),
			Capabilities:     sr.GetCapabilities(),
			UploadsInFlight:  z.Ptr(sr.GetUploadsInFlight()),
			ReportedBy:       nodeId.Bytes(),
			Warnings:         warnings,
			CapacityClamped:  z.Ptr(clamped),
			Laminae:          z.Ptr(sr.GetLaminae()),
			DateUpdatedForce: z.Ptr(true),
		}

		switch {
		case row.DateErased != nil, attachment == api.SinkAttachment_SINK_ATTACHMENT_RETIRED:
			serve = false
		case isZero(row.NodeId) || row.NodeId == nodeId.Uuid():
			// Ours, or free: attach.
			patch.Node = api.NodeRef_builder{Id: nodeId.Bytes()}.Build()
			patch.Attachment = z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED)
			patch.DateSeen = timestamppb.New(now)
			attachment = api.SinkAttachment_SINK_ATTACHMENT_ATTACHED
		case s.nodeAlive(ctx, pdid.Id(row.NodeId), now):
			// Another node still serves it: the claim is refused (§28.3),
			// and so is the rest of the report; a node's word about a sink
			// it does not hold changes nothing (§33.7).
			answers = append(answers, api.SinkAnswer_builder{SinkId: sid.Bytes(), Attachment: attachment, Serve: false}.Build())

			continue
		default:
			// Its node is down: pending adoption; auto-adopt after a while
			// is the leader's job.
			if attachment != api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION {
				patch.Attachment = z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION)
				attachment = api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION
			}
			serve = false
		}
		if _, err := srv.Sink().Patch(ctx, patch.Build()); err != nil {
			return nil, err
		}
		answers = append(answers, api.SinkAnswer_builder{
			SinkId:       sid.Bytes(),
			Attachment:   attachment,
			Serve:        serve,
			AcceptWrites: serve && row.AcceptWrites,
		}.Build())
	}

	// A sink of this node that its report no longer names, the disk pulled
	// or the directory gone, is pending adoption from now on (§28.3):
	// placement skips it, and no token names it on a node that would
	// answer "this node does not serve that sink". Its next report, or an
	// adoption by another node, attaches it again.
	stale, err := s.d.Ent.Sink.Query().
		Where(sink.NodeIdEQ(nodeId.Uuid()), sink.AttachmentEQ(int32(api.SinkAttachment_SINK_ATTACHMENT_ATTACHED)), sink.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range stale {
		if reported[string(row.Id[:])] {
			continue
		}
		if _, err := srv.Sink().Patch(ctx, api.SinkPatchRequest_builder{
			Ref:              api.SinkRef_builder{Id: row.Id[:]}.Build(),
			Attachment:       z.Ptr(api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		s.d.log().Warn("sink no longer reported by its node; pending adoption", "sink", row.Alias, "node", nodeId.String())
	}

	return answers, nil
}
