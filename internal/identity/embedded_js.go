//go:build js

package identity

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/lesomnus/payday/pdid"
)

// A page runs no roster: the console's sandbox seeds its people itself and
// signs them in with the plain header (wasm/main.go), so none of roster, its
// database engines or its command line is in the page (#154). These are the
// embedded store's operations, each refused as an external roster refuses
// them.

var errInPage = errors.New("identity: a page runs no roster")

type embedded struct{}

func openEmbedded(context.Context, Config, string, *slog.Logger) (*embedded, error) {
	return nil, errInPage
}

func (*embedded) dial() (*grpc.ClientConn, error)           { return nil, errInPage }
func (*embedded) run(context.Context) error                 { return errInPage }
func (*embedded) Close() error                              { return nil }
func (*embedded) agent(context.Context, string) error       { return errInPage }
func (*embedded) tenants(context.Context) ([]string, error) { return nil, errInPage }
func (s *Store) Seed(context.Context, string, string) (Person, string, error) {
	return Person{}, "", ErrExternal
}
func (s *Store) IssuePassword(context.Context, string, string) (string, error) {
	return "", ErrExternal
}
func (s *Store) SetPassword(context.Context, string, string, string) error { return ErrExternal }
func (s *Store) Adopt(context.Context, pdid.Id, string, string, []Adoptee) (map[string]string, error) {
	return nil, ErrExternal
}
func (s *Store) Grant(context.Context, string, string, []string, pdid.Id) (pdid.Id, error) {
	return pdid.Nil, ErrExternal
}
func (s *Store) Ungrant(context.Context, pdid.Id) error { return ErrExternal }
