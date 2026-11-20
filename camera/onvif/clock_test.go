package onvif

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

// skewedClock is a device whose own clock is offset from the host's, as a
// camera on a LAN without a time server drifts. It answers only the public
// GetSystemDateAndTime, and records every operation it was asked.
type skewedClock struct {
	server   *httptest.Server
	offset   time.Duration
	about    string
	mu       sync.Mutex
	asked    []string
	insecure bool
}

func newSkewedClock(t *testing.T, offset time.Duration, about string) *skewedClock {
	t.Helper()
	f := &skewedClock{offset: offset, about: about}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		n, err := parseXML(data)
		if err != nil || n.find("Body") == nil || len(n.find("Body").Children) != 1 {
			w.WriteHeader(400)
			return
		}
		op := n.find("Body").Children[0].Name.Local
		f.mu.Lock()
		f.asked = append(f.asked, op)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || n.find("Security") != nil {
			f.insecure = true
		}
		f.mu.Unlock()
		if op != "GetSystemDateAndTime" {
			w.WriteHeader(400)
			return
		}
		at := time.Now().Add(f.offset).UTC()
		inner := strings.Replace(clockUTC(at), "<tt:SystemDateAndTime>", "<tt:SystemDateAndTime>"+f.about, 1)
		_, _ = io.WriteString(w, wrapResponse(op, deviceNS, inner))
	}))
	t.Cleanup(f.server.Close)
	return f
}

const clockAbout = `<tt:DateTimeType>NTP</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>GMT0GMT</tt:TZ></tt:TimeZone>`

func TestReadClockMeasuresASkewedDeviceClockWithoutCredentials(t *testing.T) {
	for _, offset := range []time.Duration{-147 * 24 * time.Hour, 13 * time.Second, 0, -1500 * time.Millisecond} {
		t.Run(offset.String(), func(t *testing.T) {
			f := newSkewedClock(t, offset, clockAbout)
			jar, _ := cookiejar.New(nil)
			u, _ := url.Parse(f.server.URL)
			jar.SetCookies(u, []*http.Cookie{{Name: "session", Value: "fixture-cookie"}})
			c := &Client{HTTPClient: &http.Client{Jar: jar}, UseDeviceTimeForAuthentication: true}
			before := time.Now()
			clock, err := c.ReadClock(t.Context(), f.server.URL+"/onvif/device_service")
			require.NoError(t, err)
			require.Equal(t, camera.ClockModeNTP, clock.Mode)
			require.Equal(t, "GMT0GMT", clock.TZ)
			require.Positive(t, clock.RTT)
			require.False(t, clock.ObservedAt.Before(before), "the reading is timed on the host's clock")
			require.False(t, clock.ObservedAt.After(before.Add(clock.RTT)))
			// The device answers in whole seconds; the middle of its second is
			// taken, so the offset is within half a second and half the round
			// trip of the truth.
			got := time.Duration(clock.OffsetMilliseconds()) * time.Millisecond
			bound := 500*time.Millisecond + clock.RTT/2 + time.Millisecond
			require.InDelta(t, float64(offset), float64(got), float64(bound), "offset %v, measured %v", offset, got)

			f.mu.Lock()
			defer f.mu.Unlock()
			require.Equal(t, []string{"GetSystemDateAndTime"}, f.asked, "a reading asks the clock and nothing else")
			require.False(t, f.insecure, "a reading sends no credential or cookie")
		})
	}
}

func TestReadClockOffsetDoesNotOverflowACenturiesWrongClock(t *testing.T) {
	now := time.Now()
	for _, utc := range []time.Time{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)} {
		clock := camera.DeviceClock{UTC: utc, ObservedAt: now}
		require.Equal(t, utc.UnixMilli()+500-now.UnixMilli(), clock.OffsetMilliseconds())
	}
}

func TestReadClockKeepsUTCWhenModeOrZoneIsNotOneItMayCarry(t *testing.T) {
	at := time.Date(2026, 6, 25, 4, 5, 6, 0, time.UTC)
	for name, tc := range map[string]struct {
		about    string
		mode, tz string
	}{
		"both":           {clockAbout, "NTP", "GMT0GMT"},
		"manual":         {`<tt:DateTimeType>Manual</tt:DateTimeType>`, "Manual", ""},
		"padded":         {`<tt:DateTimeType> NTP </tt:DateTimeType><tt:TimeZone><tt:TZ> CST-8 </tt:TZ></tt:TimeZone>`, "NTP", "CST-8"},
		"none":           {``, "", ""},
		"unknown_mode":   {`<tt:DateTimeType>Auto</tt:DateTimeType>`, "", ""},
		"lowercase_mode": {`<tt:DateTimeType>ntp</tt:DateTimeType>`, "", ""},
		"repeated_mode":  {`<tt:DateTimeType>NTP</tt:DateTimeType><tt:DateTimeType>Manual</tt:DateTimeType>`, "", ""},
		"nested_mode":    {`<tt:DateTimeType><tt:X>NTP</tt:X></tt:DateTimeType>`, "", ""},
		"spaced_tz":      {`<tt:TimeZone><tt:TZ>GMT 0</tt:TZ></tt:TimeZone>`, "", ""},
		"control_tz":     {"<tt:TimeZone><tt:TZ>GMT\x7f0</tt:TZ></tt:TimeZone>", "", ""},
		"long_tz":        {`<tt:TimeZone><tt:TZ>` + strings.Repeat("A", 65) + `</tt:TZ></tt:TimeZone>`, "", ""},
		"repeated_tz":    {`<tt:TimeZone><tt:TZ>UTC0</tt:TZ><tt:TZ>CST-8</tt:TZ></tt:TimeZone>`, "", ""},
		"repeated_zone":  {`<tt:TimeZone><tt:TZ>UTC0</tt:TZ></tt:TimeZone><tt:TimeZone><tt:TZ>UTC0</tt:TZ></tt:TimeZone>`, "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(clockUTC(at), "<tt:SystemDateAndTime>", "<tt:SystemDateAndTime>"+tc.about, 1)
			n, err := parseXML([]byte(wrapResponse("GetSystemDateAndTime", deviceNS, body)))
			require.NoError(t, err)
			clock, err := parseDeviceClock(n.find("GetSystemDateAndTimeResponse"))
			require.NoError(t, err)
			require.True(t, clock.UTC.Equal(at))
			require.Equal(t, tc.mode, clock.Mode)
			require.Equal(t, tc.tz, clock.TZ)
		})
	}
}

func TestReadClockFailsWithoutAValidUTC(t *testing.T) {
	for name, body := range map[string]string{
		"no_utc":   `<tt:SystemDateAndTime>` + clockAbout + `</tt:SystemDateAndTime>`,
		"bad_date": strings.Replace(clockUTC(time.Date(2025, 2, 28, 0, 0, 0, 0, time.UTC)), "<tt:Day>28", "<tt:Day>30", 1),
	} {
		t.Run(name, func(t *testing.T) {
			n, err := parseXML([]byte(wrapResponse("GetSystemDateAndTime", deviceNS, body)))
			require.NoError(t, err)
			_, err = parseDeviceClock(n.find("GetSystemDateAndTimeResponse"))
			require.ErrorIs(t, err, ErrDeviceTime)
		})
	}
	// Nothing answering is no reading either.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer server.Close()
	_, err := (&Client{}).ReadClock(t.Context(), server.URL+"/onvif/device_service")
	require.True(t, errors.Is(err, ErrDeviceTime), fmt.Sprint(err))
}
