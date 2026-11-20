package camera_test

import (
	"encoding/json"
	"net/netip"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/stretchr/testify/require"
)

func TestTimeSyncNTPIsEdgeOrAnIPv4LiteralOnly(t *testing.T) {
	for _, ntp := range []string{"edge", "192.0.2.1", "198.51.100.7", "127.0.0.1"} {
		require.NoError(t, camera.TimeSync{NTP: ntp, TimeZone: "KST-9"}.Validate(), ntp)
	}
	for _, ntp := range []string{
		"", "Edge", "EDGE", " edge", "pool.ntp.org", "time.example.com", "localhost",
		"2001:db8::1", "::ffff:192.0.2.1", "192.0.2.1:123", "192.0.2.01", "192.000.2.1", "0.0.0.0",
		"255.255.255.255", "224.0.1.1", "192.0.2.1%eth0", "192.0.2.1/24", "ntp://192.0.2.1",
	} {
		require.Error(t, camera.TimeSync{NTP: ntp, TimeZone: "KST-9"}.Validate(), "%q", ntp)
	}
	require.NoError(t, camera.TimeSync{}.Validate(), "zero leaves the clock alone")
	require.Error(t, camera.TimeSync{NTP: "edge"}.Validate(), "a time zone is required with a server")
	require.Error(t, camera.TimeSync{TimeZone: "KST-9"}.Validate(), "a server is required with a time zone")
}

func TestTimeSyncTimeZoneIsABoundedPOSIXString(t *testing.T) {
	for _, tz := range []string{
		"KST-9", "UTC0", "GMT0", "GMT0GMT", "JST-9", "CST6CDT,M3.2.0,M11.1.0", "EST5EDT4,M3.2.0/2,M11.1.0/2:00:00",
		"<+0330>-3:30", "<-03>3", "CET-1CEST,M3.5.0,M10.5.0/3", "AEST-10AEDT,M10.1.0,M4.1.0/3", "NZST-12NZDT,J60/2,300/3", "XXX+12:30:15", "WGT3WGST,M3.5.0/-2,M10.5.0/-1", "IST-2IDT,M3.4.4/26,M10.5.0",
	} {
		require.True(t, camera.ValidPOSIXTimeZone(tz), tz)
	}
	for _, tz := range []string{
		"", "KST", "KS-9", "K1T-9", "KST-25", "KST-9:60", "KST-9:5", "KST 9", "Asia/Seoul", ":Asia/Seoul", "KST-9,M3.2.0",
		"CST6CDT,M13.2.0,M11.1.0", "CST6CDT,M3.6.0,M11.1.0", "CST6CDT,M3.2.7,M11.1.0", "CST6CDT,J0,J365", "CST6CDT,366,1",
		"CST6CDT,M3.2.0", "CST6CDT,M3.2.0/168,M11.1.0", "<KS>-9", "<KST-9", "KST-9\n", "KST-9\x00", "UTC0;rm",
		strings.Repeat("A", 61) + "-9:00",
	} {
		require.False(t, camera.ValidPOSIXTimeZone(tz), "%q", tz)
		require.Error(t, camera.TimeSync{NTP: "edge", TimeZone: tz}.Validate(), "%q", tz)
	}
}

func TestTimeServerAllowedOnlyInsideTheCameraNetwork(t *testing.T) {
	prefixes := []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.51.100.64/26")}
	require.True(t, camera.TimeServerAllowed(netip.MustParseAddr("192.0.2.1"), prefixes))
	require.True(t, camera.TimeServerAllowed(netip.MustParseAddr("198.51.100.70"), prefixes))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("198.51.100.1"), prefixes))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("203.0.113.5"), prefixes))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("255.255.255.255"), []netip.Prefix{netip.MustParsePrefix("255.255.255.0/24")}))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("192.0.2.1"), []netip.Prefix{netip.MustParsePrefix("2001:db8::/32")}))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("::ffff:192.0.2.1"), prefixes), "only IPv4 is written")
	require.False(t, camera.TimeServerAllowed(netip.Addr{}, prefixes))
	require.False(t, camera.TimeServerAllowed(netip.MustParseAddr("192.0.2.1"), nil))
}

func TestTimeSyncPresetMergePreserveRequiredAndWarnings(t *testing.T) {
	edge := camera.TimeSync{NTP: "edge", TimeZone: "KST-9"}
	explicit := camera.TimeSync{NTP: "192.0.2.1", TimeZone: "UTC0"}
	base := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60, TimeSync: edge}}
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 60, TimeSync: explicit}, camera.MergePresetSettings(base, camera.PresetSettings{Initialization: camera.DeviceSettings{TimeSync: explicit}}).Initialization, "a time sync replaces as one setting")
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 50, TimeSync: edge}, camera.MergePresetSettings(base, camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 50}}).Initialization)
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 60}, camera.MergePresetSettings(base, camera.PresetSettings{Preserve: []string{camera.PresetFieldTimeSync}}).Initialization)
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 60}, base.Initialization.Prerequisites(), "a time sync is no media prerequisite")
	p := camera.DevicePreset{Ref: camera.PresetRef{ID: "model", Revision: "1"}, Requested: base, Required: []string{camera.PresetFieldTimeSync}, Fallbacks: []camera.PresetSettings{{Preserve: []string{camera.PresetFieldTimeSync}}}}
	require.Error(t, p.Validate(), "a required time sync cannot be preserved by a fallback")
	p.Required = nil
	c, err := p.Candidates()
	require.NoError(t, err)
	require.Len(t, c, 2)
	warnings := camera.WarningsFor(c[0], c[1])
	require.Len(t, warnings, 1)
	require.Equal(t, camera.PresetFieldTimeSync, warnings[0].Field)
	require.Equal(t, "ntp=edge timezone=KST-9", warnings[0].Requested)
	require.Equal(t, "preserved", warnings[0].Applied)
}

func TestDeviceSettingsWithoutTimeSyncEncodeAsBefore(t *testing.T) {
	encoded, err := json.Marshal(camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "off"})
	require.NoError(t, err)
	require.JSONEq(t, `{"power_line_frequency":60,"smart_codec":"off"}`, string(encoded), "absent time sync leaves fingerprints and journals unchanged")
	encoded, err = json.Marshal(camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: "edge", TimeZone: "KST-9"}})
	require.NoError(t, err)
	require.JSONEq(t, `{"time_sync":{"ntp":"edge","timezone":"KST-9"}}`, string(encoded))
}

func TestTimeSyncPresetYAMLShape(t *testing.T) {
	const doc = `id: office
revision: "1"
rules:
  - id: c420
    scope: {tenant_id: 00000000-0000-0000-0000-000000000001}
    settings:
      initialization:
        time_sync: {ntp: edge, timezone: "KST-9"}
`
	p, err := preset.Decode(strings.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, camera.TimeSync{NTP: "edge", TimeZone: "KST-9"}, p.Config().Rules[0].Settings.Initialization.TimeSync)
	for _, bad := range []string{
		`{ntp: time.example.com, timezone: "KST-9"}`,
		`{ntp: edge, timezone: "Asia/Seoul"}`,
		`{ntp: edge}`,
		`{ntp: edge, timezone: "KST-9", server: 192.0.2.1}`,
	} {
		_, err := preset.Decode(strings.NewReader(strings.Replace(doc, `{ntp: edge, timezone: "KST-9"}`, bad, 1)))
		require.Error(t, err, bad)
	}
}

func TestTimeSyncPlanIsBoundToItsIntent(t *testing.T) {
	want := camera.TimeSync{NTP: "edge", TimeZone: "KST-9"}
	after := camera.DeviceSettings{TimeSync: want}
	plan := func() camera.InitializationPlan {
		return camera.InitializationPlan{After: after, Changes: []string{camera.TimeSyncChangeNTP, camera.TimeSyncChangeTimeZone}, Revision: "fixture", TimeSync: &camera.InitializationTimeSync{Server: "192.0.2.1", BeforeNTP: []string{"pool.ntp.example"}, BeforeMode: "NTP", BeforeTZ: "GMT0GMT"}}
	}
	require.NoError(t, camera.ValidateTimeSyncPlan(plan(), want))
	settled := plan()
	settled.Before, settled.Changes = after, nil
	require.NoError(t, camera.ValidateTimeSyncPlan(settled, want), "an already-right device plans no write")
	for name, edit := range map[string]func(*camera.InitializationPlan){
		"another intent":         func(p *camera.InitializationPlan) { p.After.TimeSync.TimeZone = "UTC0" },
		"no resolved server":     func(p *camera.InitializationPlan) { p.TimeSync = nil },
		"a hostname server":      func(p *camera.InitializationPlan) { p.TimeSync.Server = "ntp.example.com" },
		"an IPv6 server":         func(p *camera.InitializationPlan) { p.TimeSync.Server = "2001:db8::1" },
		"other settings":         func(p *camera.InitializationPlan) { p.After.PowerLineFrequency = 60 },
		"a native snapshot":      func(p *camera.InitializationPlan) { p.VideoMainBefore = &camera.InitializationVideoMain{} },
		"before without changes": func(p *camera.InitializationPlan) { p.Before = after },
		"no change but not done": func(p *camera.InitializationPlan) { p.Changes = nil },
		"an unknown change":      func(p *camera.InitializationPlan) { p.Changes = []string{"time_sync.clock"} },
		"a repeated change": func(p *camera.InitializationPlan) {
			p.Changes = []string{camera.TimeSyncChangeNTP, camera.TimeSyncChangeNTP}
		},
		"an unbounded before": func(p *camera.InitializationPlan) { p.TimeSync.BeforeNTP = []string{"a b"} },
	} {
		p := plan()
		edit(&p)
		require.Error(t, camera.ValidateTimeSyncPlan(p, want), name)
	}
	explicit := camera.TimeSync{NTP: "192.0.2.7", TimeZone: "KST-9"}
	p := plan()
	p.After.TimeSync = explicit
	require.Error(t, camera.ValidateTimeSyncPlan(p, explicit), "an explicit server resolves to itself only")
	p.TimeSync.Server = "192.0.2.7"
	require.NoError(t, camera.ValidateTimeSyncPlan(p, explicit))
	require.Error(t, camera.ValidateInitializationPlan(p, p.After), "a media prerequisite plan carries no time sync")
	require.Error(t, camera.ValidateInitializationPlan(camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}, TimeSync: &camera.InitializationTimeSync{Server: "192.0.2.1"}}, camera.DeviceSettings{PowerLineFrequency: 60}))
}
