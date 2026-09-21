package core

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/internal/identity"
)

// What roster refused stays refused with its code, wrapped or not; what it
// could not answer is Unavailable; a store's own refusals have theirs. An
// OK from nowhere was a success with nothing in it.
func TestRosterErr(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{status.Error(codes.PermissionDenied, "no"), codes.PermissionDenied},
		{fmt.Errorf("roster: %w", status.Error(codes.PermissionDenied, "no")), codes.PermissionDenied},
		{errors.New("dial tcp: refused"), codes.Unavailable},
		{identity.ErrExternal, codes.FailedPrecondition},
		{fmt.Errorf("%w: acme", identity.ErrNoTenant), codes.NotFound},
	} {
		got := rosterErr(tc.err)
		if got == nil || status.Code(got) != tc.code {
			t.Fatalf("%v: %v, want %s", tc.err, got, tc.code)
		}
	}
}
