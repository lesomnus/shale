package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

func TestParseWhen(t *testing.T) {
	now := time.Date(2026, 9, 20, 17, 0, 0, 0, time.UTC)
	for s, want := range map[string]time.Time{
		"now":                  now,
		"-24h":                 now.Add(-24 * time.Hour),
		"+90m":                 now.Add(90 * time.Minute),
		"2026-09-20T12:34:56Z": time.Date(2026, 9, 20, 12, 34, 56, 0, time.UTC),
		"2026-09-19":           time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC),
	} {
		got, err := parseWhen(s, now)
		require.NoError(t, err, s)
		require.True(t, got.Equal(want), "%s: %s", s, got)
	}
	for _, s := range []string{"", "yesterday", "-1 day", "2026-13-01"} {
		_, err := parseWhen(s, now)
		require.Error(t, err, s)
	}
}

// The bulk form (§20.3): a set and a range, the new dates, a reason; and
// the combinations the server would refuse are refused before the call.
func TestBuildReschedule(t *testing.T) {
	now := time.Date(2026, 9, 20, 17, 0, 0, 0, time.UTC)
	domain := func(entity string) pdid.Domain {
		d, ok := pdid.Lookup(entity)
		require.True(t, ok, entity)

		return d
	}
	set := pdid.New(domain("shale.Set"))
	req, err := buildReschedule(rescheduleOpts{
		set: set.String(), from: "-2h", to: "now", expired: "now", reason: "the drill",
	}, now)
	require.NoError(t, err)
	require.Equal(t, set.Bytes(), req.GetSet().GetId())
	require.Nil(t, req.GetRef())
	require.True(t, req.GetFrom().AsTime().Equal(now.Add(-2*time.Hour)))
	require.True(t, req.GetTo().AsTime().Equal(now))
	require.True(t, req.GetDateExpired().AsTime().Equal(now))
	require.Nil(t, req.GetDateDeleted())
	require.False(t, req.GetDeleteNow())
	require.Equal(t, "the drill", req.GetReason())

	// A set by alias, deleted now.
	req, err = buildReschedule(rescheduleOpts{set: "@acme/load", from: "2026-09-20", to: "now", deleteNow: true, reason: "privacy"}, now)
	require.NoError(t, err)
	require.Equal(t, "load", req.GetSet().GetSlug().GetAlias())
	require.Equal(t, "acme", req.GetSet().GetSlug().GetTenant().GetAlias())
	require.True(t, req.GetDeleteNow())

	// One object takes no range.
	obj := pdid.New(domain("shale.Object"))
	req, err = buildReschedule(rescheduleOpts{ref: obj.String(), deleted: "+30m", reason: "hold"}, now)
	require.NoError(t, err)
	require.Equal(t, obj.Bytes(), req.GetRef().GetId())
	require.True(t, req.GetDateDeleted().AsTime().Equal(now.Add(30*time.Minute)))

	for name, o := range map[string]rescheduleOpts{
		"nothing named":         {from: "-1h", to: "now", expired: "now", reason: "r"},
		"two named":             {ref: obj.String(), set: set.String(), from: "-1h", to: "now", expired: "now", reason: "r"},
		"a set without a range": {set: set.String(), expired: "now", reason: "r"},
		"an object with one":    {ref: obj.String(), from: "-1h", to: "now", expired: "now", reason: "r"},
		"a range backwards":     {set: set.String(), from: "now", to: "-1h", expired: "now", reason: "r"},
		"nothing to change":     {set: set.String(), from: "-1h", to: "now", reason: "r"},
		"delete-now and a date": {set: set.String(), from: "-1h", to: "now", deleteNow: true, expired: "now", reason: "r"},
		"no reason":             {set: set.String(), from: "-1h", to: "now", expired: "now", reason: " "},
		"a bad date":            {set: set.String(), from: "-1h", to: "now", expired: "soon", reason: "r"},
		"the wrong kind":        {set: obj.String(), from: "-1h", to: "now", expired: "now", reason: "r"},
	} {
		_, err := buildReschedule(o, now)
		require.Error(t, err, name)
	}
}
