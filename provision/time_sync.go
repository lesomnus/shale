package provision

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

const (
	defaultTimeSyncWait = 90 * time.Second
	defaultTimeSyncPoll = 5 * time.Second
)

// Time-sync warning codes. A time sync never fails an enrollment for what the
// device does with it; each of these is a durable warning instead.
const (
	WarningUnsupported         = "unsupported"
	WarningTimeSyncRefused     = "time_sync_refused"
	WarningTimeSyncUnconverged = "time_sync_unconverged"
)

// TimeSyncState journals a preset's time_sync. Plan is saved before the first
// write and names the server the worker resolved. Settled means its outcome,
// verified or a warning, is recorded; a resumed run does not repeat it.
type TimeSyncState struct {
	Plan    *camera.InitializationPlan `json:"plan,omitempty"`
	Settled bool                       `json:"settled,omitempty"`
}

// syncTime applies the selected candidate's time_sync once its video settings
// are verified (docs/camera-provisioner.md, "Time sync"). It is not a media
// prerequisite, and the device's answer never fails the enrollment: a device
// or firmware that cannot take it, a server outside the camera network, and a
// clock that does not converge are each a warning. Authentication, transport
// and lease failures fail it as they fail any other step.
func (e *Engine) syncTime(ctx context.Context, r *Run, inspection camera.Inspection, lease *camera.CredentialLease, acquire leaseSource) error {
	if r.Effective == nil {
		return nil
	}
	want := r.Effective.Initialization.TimeSync
	if want.IsZero() || r.TimeSync != nil && r.TimeSync.Settled {
		return nil
	}
	desired := camera.DeviceSettings{TimeSync: want}
	settle := func(plan *camera.InitializationPlan, warning *camera.PresetWarning, code string) error {
		r.TimeSync = &TimeSyncState{Plan: plan, Settled: true}
		if warning != nil {
			warning.Field, warning.Requested = camera.PresetFieldTimeSync, want.String()
			r.Warnings = append(r.Warnings, *warning)
		}
		return e.save(ctx, r, Initializing, code)
	}
	unsupported := func(plan *camera.InitializationPlan, applied, reason string) error {
		return settle(plan, &camera.PresetWarning{Code: WarningUnsupported, Applied: applied, Reason: reason}, "time_sync_unsupported")
	}
	refused := func(plan *camera.InitializationPlan) error {
		return settle(plan, &camera.PresetWarning{Code: WarningTimeSyncRefused, Applied: "unchanged", Reason: "the NTP server is not inside the camera network"}, "time_sync_refused")
	}
	initializer := e.initializer()
	if initializer == nil {
		return unsupported(nil, "unchanged", "no device initializer can apply time sync")
	}
	read := func() (camera.InitializationPlan, error) {
		if err := renew(ctx, lease, acquire, leaseMargin); err != nil {
			return camera.InitializationPlan{}, err
		}
		return initializer.PlanInitialization(ctx, inspection.Device, *lease, desired)
	}
	plan, err := read()
	switch {
	case errors.Is(err, camera.ErrTimeServerRefused):
		return refused(nil)
	case err != nil && unsupportedSetting(err):
		return unsupported(nil, "unchanged", "the device or its firmware is not qualified for time sync")
	case err != nil:
		return err
	}
	if camera.ValidateTimeSyncPlan(plan, want) != nil {
		return camera.ErrConflict
	}
	r.TimeSync = &TimeSyncState{Plan: &plan}
	if len(plan.Changes) != 0 {
		if err := e.save(ctx, r, Initializing, "time_sync_planned"); err != nil {
			return err
		}
		if err := e.Store.BeginMutation(ctx, r.Request); err != nil {
			return err
		}
		if err := renew(ctx, lease, acquire, leaseMargin); err != nil {
			return err
		}
		if _, err := initializer.ApplyInitialization(ctx, inspection.Device, *lease, plan); err != nil {
			if errors.Is(err, camera.ErrTimeServerRefused) {
				return refused(&plan)
			}
			if !unsupportedSetting(err) {
				return err
			}
			// The device declined a write, or does not keep it. Say what it
			// has now, so a partial write is not a surprise.
			observed, readErr := read()
			if readErr != nil && !unsupportedSetting(readErr) {
				return readErr
			}
			applied := "unknown"
			if readErr == nil {
				applied = describeTime(observed.TimeSync)
			}
			return unsupported(&plan, applied, "the device declined a time write or does not keep it")
		}
	}
	readback, err := read()
	if err != nil {
		if unsupportedSetting(err) {
			return unsupported(&plan, "unknown", "the device's time settings could not be read back")
		}
		return err
	}
	if len(readback.Changes) != 0 || readback.Before != desired || readback.TimeSync == nil || readback.TimeSync.Server != plan.TimeSync.Server {
		return unsupported(&plan, describeTime(readback.TimeSync), "the device reads back other time settings")
	}
	clock, observed, err := e.awaitClock(ctx, inspection.Device)
	if err != nil {
		return err
	}
	if within(clock) && observed {
		return settle(&plan, nil, "time_sync_verified")
	}
	offset := "unobserved"
	if observed {
		offset = strconv.FormatInt(clock.OffsetMilliseconds(), 10) + "ms"
	}
	wait, _ := e.timeSyncTiming()
	return settle(&plan, &camera.PresetWarning{
		Code:    WarningTimeSyncUnconverged,
		Applied: "ntp=" + plan.TimeSync.Server + " timezone=" + want.TimeZone + " offset=" + offset,
		Reason:  fmt.Sprintf("the settings read back, but the clock was not within %s of the worker's after %s", camera.TimeSyncTolerance, wait),
	}, "time_sync_unconverged")
}

func (e *Engine) timeSyncTiming() (time.Duration, time.Duration) {
	wait, poll := e.TimeSyncWait, e.TimeSyncPoll
	if wait <= 0 {
		wait = defaultTimeSyncWait
	}
	if poll <= 0 {
		poll = defaultTimeSyncPoll
	}
	return wait, poll
}

// awaitClock reads the device's clock until it is within tolerance or the wait
// ends, and returns the last reading. Each reading is credential-free. A
// reading that fails is retried; only cancellation stops the wait early.
func (e *Engine) awaitClock(ctx context.Context, device camera.DeviceHint) (camera.DeviceClock, bool, error) {
	if e.Clock == nil {
		return camera.DeviceClock{}, false, nil
	}
	wait, poll := e.timeSyncTiming()
	deadline := time.Now().Add(wait)
	var last camera.DeviceClock
	observed := false
	for {
		clock, err := e.Clock.ObserveClock(ctx, device)
		if ctx.Err() != nil {
			return last, observed, ctx.Err()
		}
		if errors.Is(err, context.Canceled) {
			return last, observed, err
		}
		if err == nil && clock.RTT <= camera.MaxClockRTT {
			last, observed = clock, true
			if within(clock) {
				return clock, true, nil
			}
		}
		left := time.Until(deadline)
		if left <= 0 {
			return last, observed, nil
		}
		timer := time.NewTimer(min(poll, left))
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, observed, ctx.Err()
		case <-timer.C:
		}
	}
}

func within(c camera.DeviceClock) bool {
	offset := c.OffsetMilliseconds()
	return !c.ObservedAt.IsZero() && offset >= -camera.TimeSyncTolerance.Milliseconds() && offset <= camera.TimeSyncTolerance.Milliseconds()
}

// describeTime is what a device said of its time settings, bounded for a
// warning.
func describeTime(t *camera.InitializationTimeSync) string {
	if t == nil {
		return "unknown"
	}
	ntp := strings.Join(t.BeforeNTP, ",")
	if t.BeforeNTPFromDHCP {
		ntp = strings.TrimSuffix("dhcp,"+ntp, ",")
	}
	or := func(v, fallback string) string {
		if v == "" {
			return fallback
		}
		return v
	}
	out := "ntp=" + or(ntp, "none") + " mode=" + or(t.BeforeMode, "unknown") + " tz=" + or(t.BeforeTZ, "unknown")
	if t.BeforeDaylightSavings {
		out += " daylight_savings"
	}
	if len(out) > 256 {
		out = out[:256]
	}
	return out
}
