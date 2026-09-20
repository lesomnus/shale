package core

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/device"
	"github.com/lesomnus/shale/internal/ent/sink"
)

// Failure scores and quarantine (§27): each device and node carries an
// exponentially decaying sum of weighted events. Devices move between
// HEALTHY, SUSPECT, and QUARANTINED on separate thresholds; a node's score
// only weighs on placement.

const (
	ScoreIoError        = 10.0
	ScoreFailedWrite    = 5.0
	ScoreTimeout        = 2.0
	ScoreProducerReport = 1.0
	ScoreSmartRealloc   = 5.0
	ScoreSmartPending   = 10.0

	// ProducerReportCap is the most one producer adds to one node per day.
	ProducerReportCap = 5

	ScoreSuspect    = 10.0
	ScoreQuarantine = 30.0
	ScoreRelease    = 5.0

	ScoreHalfLife   = 24 * time.Hour
	CooldownPeriod  = 24 * time.Hour
	ProbationPeriod = 7 * 24 * time.Hour
)

// decayed is a score after `since`.
func decayed(score float64, since *time.Time, now time.Time) float64 {
	if since == nil || score <= 0 {
		return score
	}
	age := now.Sub(*since)
	if age <= 0 {
		return score
	}

	return score * math.Pow(0.5, float64(age)/float64(ScoreHalfLife))
}

// nodeScore is a node's current score.
func nodeScore(n *ent.Node, now time.Time) float64 {
	return decayed(n.FailureScore, n.DateScored, now)
}

// reportDelta is what a new device report adds: increases of its error
// counters and what SMART says. Counters that went down (a node restart)
// add nothing.
func reportDelta(old, cur *api.DeviceReport) (float64, []string) {
	var delta float64
	var reasons []string
	inc := func(a, b int64) int64 {
		if old == nil || b <= a {
			return 0
		}

		return b - a
	}
	if n := inc(old.GetIoErrors(), cur.GetIoErrors()); n > 0 {
		delta += ScoreIoError * float64(n)
		reasons = append(reasons, fmt.Sprintf("%d I/O errors", n))
	}
	if n := inc(old.GetFailedWrites(), cur.GetFailedWrites()); n > 0 {
		delta += ScoreFailedWrite * float64(n)
		reasons = append(reasons, fmt.Sprintf("%d failed writes", n))
	}
	if n := inc(old.GetTimeouts(), cur.GetTimeouts()); n > 0 {
		delta += ScoreTimeout * float64(n)
		reasons = append(reasons, fmt.Sprintf("%d timeouts", n))
	}
	if sm := cur.GetSmart(); sm != nil {
		osm := old.GetSmart()
		if !sm.GetPassed() && (osm == nil || osm.GetPassed()) {
			delta += ScoreQuarantine
			reasons = append(reasons, "SMART overall health failed")
		}
		if osm != nil && sm.GetReallocated() > osm.GetReallocated() {
			delta += ScoreSmartRealloc
			reasons = append(reasons, fmt.Sprintf("reallocated sectors %d → %d", osm.GetReallocated(), sm.GetReallocated()))
		}
		if (sm.GetPending() > 0 && (osm == nil || osm.GetPending() == 0)) || (sm.GetOfflineUncorrectable() > 0 && (osm == nil || osm.GetOfflineUncorrectable() == 0)) {
			delta += ScoreSmartPending
			reasons = append(reasons, "pending or offline-uncorrectable sectors")
		}
	}

	return delta, reasons
}

// nextHealth decides a device's state from its score (§27). Operator
// decisions (a retirement, a death, an operator's quarantine) are not
// undone here.
func nextHealth(cur api.DeviceHealth, score float64, q *api.DeviceQuarantine, failedNow bool, now time.Time) (api.DeviceHealth, string) {
	switch cur {
	case api.DeviceHealth_DEVICE_HEALTH_RETIRED, api.DeviceHealth_DEVICE_HEALTH_DEAD:
		return cur, ""
	case api.DeviceHealth_DEVICE_HEALTH_QUARANTINED:
		if q.GetOperator() {
			return cur, ""
		}
		if score < ScoreRelease && q.GetDate() != nil && now.Sub(q.GetDate().AsTime()) >= CooldownPeriod {
			return api.DeviceHealth_DEVICE_HEALTH_HEALTHY, fmt.Sprintf("score %.1f after cool-down", score)
		}

		return cur, ""
	}

	probation := q.GetDateProbationEnds() != nil && now.Before(q.GetDateProbationEnds().AsTime())
	switch {
	case score >= ScoreQuarantine:
		return api.DeviceHealth_DEVICE_HEALTH_QUARANTINED, fmt.Sprintf("score %.1f", score)
	case probation && failedNow:
		return api.DeviceHealth_DEVICE_HEALTH_QUARANTINED, fmt.Sprintf("a failure during probation (score %.1f)", score)
	case score >= ScoreSuspect:
		if cur != api.DeviceHealth_DEVICE_HEALTH_SUSPECT {
			return api.DeviceHealth_DEVICE_HEALTH_SUSPECT, fmt.Sprintf("score %.1f", score)
		}
	case cur == api.DeviceHealth_DEVICE_HEALTH_SUSPECT && !probation:
		return api.DeviceHealth_DEVICE_HEALTH_HEALTHY, fmt.Sprintf("score %.1f", score)
	}

	return cur, ""
}

// scoreDevice folds new failures into a device's score and moves it
// between states when a threshold is crossed.
func (s Core) scoreDevice(ctx context.Context, srv api.Server, d *ent.Device, delta float64, reasons []string, now time.Time) error {
	score := decayed(d.FailureScore, d.DateScored, now) + delta
	q := d.Quarantine
	if q == nil {
		q = &api.DeviceQuarantine{}
	}
	if delta > 0 {
		errs := append([]string(nil), q.GetRecentErrors()...)
		for _, r := range reasons {
			errs = append(errs, now.UTC().Format(time.RFC3339)+" "+r)
		}
		if len(errs) > 10 {
			errs = errs[len(errs)-10:]
		}
		q = api.DeviceQuarantine_builder{
			Date: q.GetDate(), Reason: q.GetReason(), Score: q.GetScore(), RecentErrors: errs,
			DateProbationEnds: q.GetDateProbationEnds(), DateCooldownEnds: q.GetDateCooldownEnds(), Operator: q.GetOperator(),
		}.Build()
	}

	patch := api.DevicePatchRequest_builder{
		Ref:              api.DeviceRef_builder{Id: d.Id[:]}.Build(),
		FailureScore:     z.Ptr(score),
		DateScored:       timestamppb.New(now),
		Quarantine:       q,
		DateUpdatedForce: z.Ptr(true),
	}
	next, why := nextHealth(api.DeviceHealth(d.Health), score, q, delta > 0, now)
	if next == api.DeviceHealth(d.Health) {
		_, err := srv.Device().Patch(ctx, patch.Build())

		return err
	}
	if _, err := srv.Device().Patch(ctx, patch.Build()); err != nil {
		return err
	}
	s.d.log().Info("device health", "device", d.Alias, "from", api.DeviceHealth(d.Health).String(), "to", next.String(), "why", why)

	return s.setDeviceHealth(ctx, srv, pdid.Id(d.Id), next, why, false, score, now)
}

// setDeviceHealth moves a device and, with it, every sink on it (§27).
// The directive to the node follows from the sinks' state.
func (s Core) setDeviceHealth(ctx context.Context, srv api.Server, id pdid.Id, h api.DeviceHealth, reason string, operator bool, score float64, now time.Time) error {
	d, err := s.d.Ent.Device.Get(ctx, id.Uuid())
	if err != nil {
		return err
	}
	q := api.DeviceQuarantine_builder{Date: timestamppb.New(now), Reason: reason, Score: score, Operator: operator, RecentErrors: d.Quarantine.GetRecentErrors()}
	switch h {
	case api.DeviceHealth_DEVICE_HEALTH_HEALTHY:
		// Released: probation for seven days at half weight (§27).
		q.DateProbationEnds = timestamppb.New(now.Add(ProbationPeriod))
	case api.DeviceHealth_DEVICE_HEALTH_QUARANTINED:
		q.DateCooldownEnds = timestamppb.New(now.Add(CooldownPeriod))
	}
	accept := h == api.DeviceHealth_DEVICE_HEALTH_HEALTHY || h == api.DeviceHealth_DEVICE_HEALTH_SUSPECT
	if _, err := srv.Device().Patch(ctx, api.DevicePatchRequest_builder{
		Ref:              api.DeviceRef_builder{Id: id.Bytes()}.Build(),
		Health:           &h,
		Quarantine:       q.Build(),
		FailureScore:     z.Ptr(score),
		DateScored:       timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return err
	}
	sinks, err := s.d.Ent.Sink.Query().Where(sink.DeviceIdEQ(id.Uuid()), sink.DateErasedIsNil()).All(ctx)
	if err != nil {
		return err
	}
	for _, sk := range sinks {
		if sk.Attachment == int32(api.SinkAttachment_SINK_ATTACHMENT_RETIRED) {
			continue
		}
		if _, err := srv.Sink().Patch(ctx, api.SinkPatchRequest_builder{
			Ref:              api.SinkRef_builder{Id: sk.Id[:]}.Build(),
			AcceptWrites:     z.Ptr(accept),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return err
		}
	}

	return nil
}

// producerReport is a producer's failure report against a node: +1, at
// most ProducerReportCap per producer per node per day (§27).
func (s Core) producerReport(ctx context.Context, nodeId, producer pdid.Id, now time.Time) error {
	if !s.d.allowReport(nodeId, producer, now) {
		return nil
	}
	n, err := s.d.Ent.Node.Get(ctx, nodeId.Uuid())
	if err != nil {
		return err
	}
	score := nodeScore(n, now) + ScoreProducerReport
	_, err = s.d.Own.Node().Patch(ctx, api.NodePatchRequest_builder{
		Ref:              api.NodeRef_builder{Id: nodeId.Bytes()}.Build(),
		FailureScore:     z.Ptr(score),
		DateScored:       timestamppb.New(now),
		DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
}

// healthSweep re-evaluates every device on decay alone, so a quarantined
// device is released after its cool-down and a SUSPECT one returns.
func (s Core) healthSweep(ctx context.Context, srv api.Server, now time.Time) error {
	devices, err := s.d.Ent.Device.Query().
		Where(device.DateErasedIsNil(), device.HealthIn(int32(api.DeviceHealth_DEVICE_HEALTH_SUSPECT), int32(api.DeviceHealth_DEVICE_HEALTH_QUARANTINED))).
		All(ctx)
	if err != nil {
		return err
	}
	for _, d := range devices {
		if err := s.scoreDevice(ctx, srv, d, 0, nil, now); err != nil {
			s.d.log().Warn("health sweep", "device", d.Alias, "err", err.Error())
		}
	}

	return nil
}
