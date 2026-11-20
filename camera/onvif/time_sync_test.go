package onvif

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

// timeDevice is a device whose clock is far off until it is pointed at a time
// server. Like the C420 it checks WS-Security Created against its own clock,
// so a credential sent with a reading from before the clock moved is refused.
type timeDevice struct {
	server  *httptest.Server
	mu      sync.Mutex
	offset  time.Duration
	manual  []string
	dhcp    bool
	mode    string
	tz      string
	fault   map[string]string
	ops     []string
	bodies  map[string]string
	refused int
}

func newTimeDevice(t *testing.T, offset time.Duration) *timeDevice {
	t.Helper()
	d := &timeDevice{offset: offset, manual: []string{"pool.ntp.example"}, mode: "NTP", tz: "GMT0GMT", fault: map[string]string{}, bodies: map[string]string{}}
	d.server = httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(d.server.Close)
	return d
}

func (d *timeDevice) hint() camera.DeviceHint {
	return camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: d.server.URL + "/onvif/device_service", NetworkScope: "lan"}}}
}

func (d *timeDevice) serve(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	n, err := parseXML(data)
	if err != nil || n.find("Body") == nil || len(n.find("Body").Children) != 1 {
		w.WriteHeader(400)
		return
	}
	op := n.find("Body").Children[0]
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops = append(d.ops, op.Name.Local)
	d.bodies[op.Name.Local] = string(data)
	if op.Name.Local != "GetSystemDateAndTime" {
		created, err := time.Parse(time.RFC3339Nano, n.find("UsernameToken").text("Created"))
		if err != nil || created.Sub(time.Now().Add(d.offset)).Abs() > 5*time.Second {
			d.refused++
			fault(w, "NotAuthorized")
			return
		}
	}
	if name := d.fault[op.Name.Local]; name != "" {
		fault(w, name)
		return
	}
	inner := ""
	switch op.Name.Local {
	case "GetSystemDateAndTime":
		inner = strings.Replace(clockUTC(time.Now().Add(d.offset).UTC()), "<tt:SystemDateAndTime>", "<tt:SystemDateAndTime><tt:DateTimeType>"+d.mode+"</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>"+d.tz+"</tt:TZ></tt:TimeZone>", 1)
	case "GetNTP":
		inner = "<t:NTPInformation><tt:FromDHCP>" + map[bool]string{true: "true", false: "false"}[d.dhcp] + "</tt:FromDHCP>"
		for _, s := range d.manual {
			if _, err := netip.ParseAddr(s); err == nil {
				inner += "<tt:NTPManual><tt:Type>IPv4</tt:Type><tt:IPv4Address>" + s + "</tt:IPv4Address></tt:NTPManual>"
			} else {
				inner += "<tt:NTPManual><tt:Type>DNS</tt:Type><tt:DNSname>" + s + "</tt:DNSname></tt:NTPManual>"
			}
		}
		inner += "</t:NTPInformation>"
	case "SetNTP":
		d.dhcp = op.text("FromDHCP") == "true"
		d.manual = nil
		for _, m := range op.all("NTPManual") {
			d.manual = append(d.manual, m.text("IPv4Address"))
		}
		// The server answers at once: the clock comes right.
		d.offset = 0
	case "SetSystemDateAndTime":
		d.mode, d.tz = op.text("DateTimeType"), op.child("TimeZone").text("TZ")
	default:
		w.WriteHeader(400)
		return
	}
	_, _ = io.WriteString(w, wrapResponse(op.Name.Local, deviceNS, inner))
}

func TestTimeConfigurationReadsNTPAndDateTime(t *testing.T) {
	d := newTimeDevice(t, -147*24*time.Hour)
	d.dhcp, d.manual = true, []string{"pool.ntp.example", "192.0.2.9"}
	c := &Client{UseDeviceTimeForAuthentication: true}
	got, err := c.ReadTimeConfiguration(t.Context(), d.hint(), fixtureLease())
	require.NoError(t, err)
	require.True(t, got.NTPFromDHCP)
	require.Equal(t, []string{"pool.ntp.example", "192.0.2.9"}, got.NTPManual)
	require.Equal(t, camera.ClockModeNTP, got.Mode)
	require.Equal(t, "GMT0GMT", got.TZ)
	require.False(t, got.DaylightSavings)
	require.InDelta(t, float64(-147*24*time.Hour), float64(time.Duration(got.Clock.OffsetMilliseconds())*time.Millisecond), float64(2*time.Second))
	require.Equal(t, []string{"GetSystemDateAndTime", "GetNTP", "GetSystemDateAndTime"}, d.ops, "one sample authenticates GetNTP; the reading is its own")
}

func TestTimeWritesHaveTheirStandardShapeAndRefreshTheDeviceTime(t *testing.T) {
	d := newTimeDevice(t, -147*24*time.Hour)
	c := &Client{UseDeviceTimeForAuthentication: true}
	lease := fixtureLease()
	require.NoError(t, c.SetNTP(t.Context(), d.hint(), lease, netip.MustParseAddr("192.0.2.1")))
	// The device's clock moved 147 days with that write. Created for the next
	// call comes from a fresh reading, or the device refuses it.
	require.NoError(t, c.SetNTPDateTime(t.Context(), d.hint(), lease, "KST-9"))
	got, err := c.ReadTimeConfiguration(t.Context(), d.hint(), lease)
	require.NoError(t, err)
	require.Zero(t, d.refused)
	require.False(t, got.NTPFromDHCP)
	require.Equal(t, []string{"192.0.2.1"}, got.NTPManual)
	require.Equal(t, camera.ClockModeNTP, got.Mode)
	require.Equal(t, "KST-9", got.TZ)
	require.Contains(t, d.bodies["SetNTP"], "<t:FromDHCP>false</t:FromDHCP><t:NTPManual><tt:Type>IPv4</tt:Type><tt:IPv4Address>192.0.2.1</tt:IPv4Address></t:NTPManual>")
	require.Contains(t, d.bodies["SetSystemDateAndTime"], "<t:DateTimeType>NTP</t:DateTimeType><t:DaylightSavings>false</t:DaylightSavings><t:TimeZone><tt:TZ>KST-9</tt:TZ></t:TimeZone>")
	require.NotContains(t, d.bodies["SetSystemDateAndTime"], "UTCDateTime", "an NTP device is not given a time")
}

func TestTimeOperationsRefuseBadInputAndClassifyFaults(t *testing.T) {
	d := newTimeDevice(t, 0)
	c := &Client{}
	lease := fixtureLease()
	for _, server := range []netip.Addr{{}, netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("0.0.0.0"), netip.MustParseAddr("224.0.1.1")} {
		require.ErrorIs(t, c.SetNTP(t.Context(), d.hint(), lease, server), camera.ErrInvalid)
	}
	for _, tz := range []string{"", "Asia/Seoul", "KST-9</tt:TZ><tt:TZ>UTC0"} {
		require.ErrorIs(t, c.SetNTPDateTime(t.Context(), d.hint(), lease, tz), camera.ErrInvalid)
	}
	require.ErrorIs(t, c.SetNTP(t.Context(), d.hint(), nil, netip.MustParseAddr("192.0.2.1")), camera.ErrAuthRequired)
	require.Empty(t, d.ops, "refused input reaches no device")

	for _, name := range []string{"ActionNotSupported", "InvalidArgVal", "InvalidTimeZone"} {
		d.fault["SetSystemDateAndTime"] = name
		require.ErrorIs(t, c.SetNTPDateTime(t.Context(), d.hint(), lease, "KST-9"), camera.ErrUnsupported, name)
	}
	d.fault["GetNTP"] = "ActionNotSupported"
	_, err := c.ReadTimeConfiguration(t.Context(), d.hint(), lease)
	require.ErrorIs(t, err, camera.ErrUnsupported)
	d.fault["GetNTP"] = "NotAuthorized"
	_, err = c.ReadTimeConfiguration(t.Context(), d.hint(), lease)
	require.ErrorIs(t, err, camera.ErrAuthRequired, "an authorization fault is not the device declining time sync")
	d.fault["SetNTP"] = "NotAuthorized"
	err = c.SetNTP(t.Context(), d.hint(), lease, netip.MustParseAddr("192.0.2.1"))
	require.False(t, errors.Is(err, camera.ErrUnsupported))
	require.ErrorIs(t, err, camera.ErrForbidden)
}

func TestTimeConfigurationRefusesAnUnboundedAnswer(t *testing.T) {
	d := newTimeDevice(t, 0)
	c := &Client{}
	d.manual = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	_, err := c.ReadTimeConfiguration(t.Context(), d.hint(), fixtureLease())
	require.ErrorIs(t, err, camera.ErrUnsupported)
	d.manual = []string{strings.Repeat("a", 256)}
	_, err = c.ReadTimeConfiguration(t.Context(), d.hint(), fixtureLease())
	require.ErrorIs(t, err, camera.ErrUnsupported)
}
