package onvif

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

const maxDeviceTimeAuthorities = 32

type deviceTimeSample struct {
	ready   chan struct{}
	utc, at time.Time
	err     error
}

// GetSystemDateAndTime reads public UTC from this exact authorized endpoint.
// It sends no WS-Security, HTTP authentication, or cookies, does not follow
// redirects, and never invokes SetSystemDateAndTime. Transport policy and the
// caller's deadline remain those of the original HTTP client/context.
func (c *Client) GetSystemDateAndTime(ctx context.Context, endpoint string) (time.Time, error) {
	public := &Client{}
	if c.HTTPClient != nil {
		h := *c.HTTPClient
		h.Jar = nil
		public.HTTPClient = &h
	}
	r, err := public.request(ctx, endpoint, deviceNS, "GetSystemDateAndTime", "", camera.CredentialMaterial{})
	if err != nil {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		return time.Time{}, ErrDeviceTime
	}
	return parseDeviceUTC(r)
}

func parseDeviceUTC(response *node) (time.Time, error) {
	dateTimes := response.all("SystemDateAndTime")
	if len(dateTimes) != 1 {
		return time.Time{}, ErrDeviceTime
	}
	utcs := dateTimes[0].all("UTCDateTime")
	if len(utcs) != 1 || len(utcs[0].all("Date")) != 1 || len(utcs[0].all("Time")) != 1 {
		return time.Time{}, ErrDeviceTime
	}
	value := func(parent *node, name string, min, max int) (int, bool) {
		fields := parent.all(name)
		if len(fields) != 1 || len(fields[0].Children) != 0 {
			return 0, false
		}
		raw := strings.TrimSpace(fields[0].Text)
		if len(raw) == 0 || len(raw) > 4 {
			return 0, false
		}
		for _, b := range []byte(raw) {
			if b < '0' || b > '9' {
				return 0, false
			}
		}
		n, err := strconv.Atoi(raw)
		return n, err == nil && n >= min && n <= max
	}
	d, clock := utcs[0].child("Date"), utcs[0].child("Time")
	year, y := value(d, "Year", 1970, 9999)
	month, m := value(d, "Month", 1, 12)
	day, dd := value(d, "Day", 1, 31)
	hour, h := value(clock, "Hour", 0, 23)
	minute, mm := value(clock, "Minute", 0, 59)
	second, s := value(clock, "Second", 0, 59)
	if !y || !m || !dd || !h || !mm || !s {
		return time.Time{}, ErrDeviceTime
	}
	at := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	if at.Year() != year || int(at.Month()) != month || at.Day() != day {
		return time.Time{}, ErrDeviceTime // time.Date normalizes invalid calendar dates.
	}
	return at, nil
}

func (c *Client) authenticationTime(ctx context.Context, endpoint string) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	u, err := validatedEndpoint(endpoint)
	if err != nil {
		return time.Time{}, ErrDeviceTime
	}
	// A media path on the same authority shares the device's sample. Distinct
	// ports/schemes never borrow another endpoint's clock or authentication state.
	// Preserve case-sensitive IPv6 zone/interface names. Harmless DNS-case
	// aliases may miss the cache, but must never mix distinct authorities.
	key := u.Scheme + "://" + u.Host
	c.clockMu.Lock()
	if c.clocks == nil {
		c.clocks = make(map[string]*deviceTimeSample)
	}
	sample, exists := c.clocks[key]
	if !exists {
		if len(c.clocks) >= maxDeviceTimeAuthorities {
			c.clockMu.Unlock()
			return time.Time{}, ErrDeviceTime
		}
		sample = &deviceTimeSample{ready: make(chan struct{})}
		c.clocks[key] = sample
	}
	c.clockMu.Unlock()
	if !exists {
		start := time.Now()
		utc, err := c.GetSystemDateAndTime(ctx, endpoint)
		// The response has second resolution. Its sample was taken during this
		// bounded round trip; use the host's monotonic midpoint for later elapsed
		// time, never a possibly overflowing year-long time.Duration offset.
		midpoint := start.Add(time.Since(start) / 2)
		c.clockMu.Lock()
		if err != nil {
			sample.err = ErrDeviceTime
			delete(c.clocks, key) // A later operation may retry a failed public read.
		} else {
			sample.utc, sample.at = utc, midpoint
		}
		close(sample.ready)
		c.clockMu.Unlock()
	}
	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-sample.ready:
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		if sample.err != nil {
			return time.Time{}, sample.err
		}
		at := sample.utc.Add(time.Since(sample.at))
		if at.Year() < 1970 || at.Year() > 9999 {
			return time.Time{}, ErrDeviceTime
		}
		return at, nil
	}
}
