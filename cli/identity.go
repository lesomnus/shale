package cli

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// NewCmdIdentity is `shale identity migrate` (§33.1): a deployment made
// before roster held its people has Tenant and Holder rows of its own
// minting, and nobody at roster. This writes every tenant and person into
// the embedded roster with the identifiers they have, binds each admin to
// the role that administers the tenant there, and prints each person's new
// password once; the old verifiers are gone with the upgrade. Run again it
// makes nothing and prints nothing. Where roster runs elsewhere, its
// operator makes the rows, with these identifiers.
func NewCmdIdentity(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "identity",
		Brief: "who people are: roster, in this process or elsewhere (§33.1)",
		Commands: []*xli.Command{{
			Name:  "migrate",
			Brief: "write the tenants and people of a deployment that predates roster into the embedded roster, with their identifiers",
			Flags: flg.Flags{
				&flg.String{Name: "dev", Brief: "development mode: everything under this directory"},
			},
			Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
				if v, ok := flg.Find[string](self, "dev"); ok && v != "" {
					ApplyDev(c, v)
				}

				return migrateIdentity(ctx, c, self)
			}),
		}},
		Handler: xli.RequireSubcommand(),
	}
}

func migrateIdentity(ctx context.Context, c *cmd.Config, self *xli.Command) error {
	s, err := cmd.Build(ctx, *c)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := Migrate(ctx, s); err != nil {
		return err
	}
	if !s.Identity.Embedded() {
		return fmt.Errorf("%w: give roster's operator the identifiers `shale tenant ls` and `shale holder ls` print", identity.ErrExternal)
	}

	tenants, err := every(ctx, func(after string) ([]*api.Tenant, string, error) {
		res, err := s.Ungated.Tenant().List(ctx, api.TenantListRequest_builder{Size: 100, After: after}.Build())

		return res.GetItems(), res.GetNext(), err
	})
	if err != nil {
		return err
	}
	made := 0
	for _, t := range tenants {
		tid, err := pdid.From(t.GetId())
		if err != nil {
			return err
		}
		people, err := every(ctx, func(after string) ([]*api.Holder, string, error) {
			res, err := s.Ungated.Holder().List(ctx, api.HolderListRequest_builder{
				Filters: []*api.HolderFilter{api.HolderFilter_builder{Tenant: api.TenantRef_builder{Id: t.GetId()}.Build()}.Build()},
				Size:    100, After: after,
			}.Build())

			return res.GetItems(), res.GetNext(), err
		})
		if err != nil {
			return err
		}
		var adoptees []identity.Adoptee
		for _, h := range people {
			id, err := pdid.From(h.GetId())
			if err != nil {
				return err
			}
			adoptees = append(adoptees, identity.Adoptee{Id: id, Alias: h.GetAlias(), Name: h.GetName(), Admin: h.GetAllSites()})
		}
		passwords, err := s.Identity.Adopt(ctx, tid, t.GetAlias(), t.GetName(), adoptees)
		if err != nil {
			return err
		}
		for _, alias := range slices.Sorted(maps.Keys(passwords)) {
			self.Printf("@%s/%s   password: %s\n", t.GetAlias(), alias, passwords[alias])
			made++
		}
	}
	if made == 0 {
		self.Printf("nothing to migrate: roster holds everyone already\n")
	} else {
		self.Printf("\n%d password(s), printed once; the ones from before the upgrade are gone\n", made)
	}

	return nil
}

// every pages through a list.
func every[T any](ctx context.Context, page func(after string) ([]T, string, error)) ([]T, error) {
	var vs []T
	after := ""
	for {
		items, next, err := page(after)
		if err != nil {
			return nil, err
		}
		vs = append(vs, items...)
		if next == "" || len(items) == 0 {
			return vs, nil
		}
		after = next
	}
}
