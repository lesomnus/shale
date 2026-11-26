package cmd

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"
)

// A tenant roster names by another identifier (#322) is said once, at
// ERROR, with both identifiers; its caller is told what to do and neither.
func TestTenantElsewhereIsSaidOnce(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))
	here, roster := pdid.New(1), pdid.New(1)
	v := TenantElsewhere{Alias: "acme", Here: here, Roster: roster}

	var w elsewhere
	say := func(v TenantElsewhere) {
		w.say(context.Background(), log, v, "elsewhere", "here", v.Here.String(), "roster", v.Roster.String())
	}
	for range 3 {
		say(v)
	}
	say(TenantElsewhere{Alias: "beta", Here: pdid.New(1), Roster: pdid.New(1)})
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2, "once for each")
	require.Contains(t, lines[0], "level=ERROR")
	require.Contains(t, lines[0], here.String())
	require.Contains(t, lines[0], roster.String())

	require.ErrorIs(t, v, ErrTenantElsewhere)
	require.True(t, errors.Is(error(v), ErrTenantElsewhere))
	require.Equal(t, codes.FailedPrecondition, status.Code(v))
	msg := status.Convert(v).Message()
	require.Contains(t, msg, "shale tenant ls")
	require.NotContains(t, msg, here.String())
	require.NotContains(t, msg, roster.String())
}

// And a person, the same way.
func TestPersonElsewhereSaysWhatToDo(t *testing.T) {
	here, roster := pdid.New(2), pdid.New(2)
	v := PersonElsewhere{TenantAlias: "acme", Alias: "bob", Here: here, Roster: roster}
	require.ErrorIs(t, v, ErrPersonElsewhere)
	require.Equal(t, codes.FailedPrecondition, status.Code(v))
	msg := status.Convert(v).Message()
	require.Contains(t, msg, "@acme/bob")
	require.Contains(t, msg, "shale holder ls")
	require.Contains(t, msg, "shale holder erase")
	require.NotContains(t, msg, here.String())
	require.Contains(t, v.Error(), here.String())
	require.Contains(t, v.Error(), roster.String())
	var page elsewhereError
	require.ErrorAs(t, error(v), &page)
}
