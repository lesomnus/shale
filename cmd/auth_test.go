package cmd

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
)

// A caller's row that could not be read is told apart by why: nobody by that
// id is a credential naming nobody, the server's own answer stands, and a
// database that did not answer is Unavailable -- not Internal, which is what
// every host was told while the CP could not reach PostgreSQL (2026-10-13).
func TestRefused(t *testing.T) {
	x := require.New(t)

	err := refused(status.Error(codes.NotFound, "no such producer"))
	x.ErrorIs(err, auth.ErrNoCredential)
	x.NotErrorIs(err, auth.ErrUnavailable)

	err = refused(status.Error(codes.PermissionDenied, "not yours"))
	x.Equal(codes.PermissionDenied, status.Code(err))
	x.NotErrorIs(err, auth.ErrUnavailable)

	err = refused(errors.New("failed to connect to `user=shale database=shale`: dial tcp 10.0.0.1:5432: connect: connection refused"))
	x.ErrorIs(err, auth.ErrUnavailable)
	x.NotErrorIs(err, auth.ErrNoCredential)
	_, isStatus := status.FromError(err)
	x.False(isStatus, "payday asks for a status first; a wrapped plain error must not look like one")
}
