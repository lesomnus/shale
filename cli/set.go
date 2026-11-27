package cli

import (
	"bytes"
	"context"
	"fmt"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"

	"github.com/lesomnus/payday/pdcmd"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
)

// addSetCommands mounts `set place <set> <site>` (§7, §32) by hand: it
// takes two references, which the generated mounting of a custom verb
// does not, and says what moved.
func addSetCommands(t *pdcmd.Tree, c *cmd.Config) {
	t.Add("set/place", &xli.Command{
		Name:  "place",
		Brief: "put a set into a site, or move it to another, with everything it ever recorded",
		Flags: flg.Flags{
			&flg.Switch{Name: "dry-run", Brief: "count what would move and move nothing"},
		},
		Args: arg.Args{
			&arg.String{Name: "SET", Brief: "the set, by id or @tenant/alias"},
			&arg.String{Name: "SITE", Brief: "the site, by id or @tenant/alias"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			setArg, _ := arg.Get[string](self, "SET")
			siteArg, _ := arg.Get[string](self, "SITE")
			dry, _ := flg.Find[bool](self, "dry-run")
			set, site := &api.SetRef{}, &api.SiteRef{}
			if err := fillRef(set, setArg, "shale.Set"); err != nil {
				return fmt.Errorf("SET: %w", err)
			}
			if err := fillRef(site, siteArg, "shale.Site"); err != nil {
				return fmt.Errorf("SITE: %w", err)
			}

			conn, done, err := (&connector{c: c}).Connect(ctx)
			if err != nil {
				return err
			}
			defer done()
			sites := api.NewSiteServiceClient(conn)
			to, err := sites.Get(ctx, api.SiteGetRequest_builder{Ref: site}.Build())
			if err != nil {
				return fmt.Errorf("SITE: %w", err)
			}
			resp, err := api.NewSetServiceClient(conn).Place(ctx, api.SetPlaceRequest_builder{
				Set: set, Site: api.SiteRef_builder{Id: to.GetId()}.Build(), DryRun: dry,
			}.Build())
			if err != nil {
				return err
			}
			from := "no site"
			if id := resp.GetFromSiteId(); len(id) > 0 {
				from = pdid.Id(id).String()
				if v, err := sites.Get(ctx, api.SiteGetRequest_builder{Ref: api.SiteRef_builder{Id: id}.Build()}.Build()); err == nil {
					from = v.GetAlias()
				}
			}
			self.Print(placedLines(resp, bytes.Equal(resp.GetFromSiteId(), to.GetId()), resp.GetSet().GetAlias(), from, to.GetAlias()))

			return nil
		}),
	})
}

// placedLines is what `set place` says: where the set was and is, and how
// many rows of each kind moved with it, or would have.
func placedLines(resp *api.SetPlaceResponse, already bool, set, from, to string) string {
	m := resp.GetMoved()
	counts := fmt.Sprintf("%d source(s), %d lamina(s), %d attempt(s), %d producer(s)", m.GetSources(), m.GetLaminae(), m.GetAttempts(), m.GetProducers())
	if m.GetCameraAssignments() > 0 {
		counts += fmt.Sprintf("; cameras: %d assignment(s), %d binding(s), %d job(s), %d network(s), %d device(s), %d credential(s), %d credential version(s), %d policy(ies), %d preset(s), %d worker(s)",
			m.GetCameraAssignments(), m.GetCameraBindings(), m.GetCameraJobs(), m.GetCameraNetworks(), m.GetCameraDevices(), m.GetCameraCredentials(),
			m.GetCameraCredentialVersions(), m.GetCameraPolicies(), m.GetCameraPresets(), m.GetCameraWorkers())
	}
	if already {
		return fmt.Sprintf("%s: in %s already; nothing moved\n", set, to)
	}
	if len(resp.GetRefused()) > 0 {
		out := fmt.Sprintf("%s: in %s, would not be placed in %s:\n", set, from, to)
		for _, why := range resp.GetRefused() {
			out += "  " + why + "\n"
		}
		return out
	}
	in := func(s string) string {
		if s == "no site" {
			return "in no site"
		}
		return "in " + s
	}
	if resp.GetDryRun() {
		return fmt.Sprintf("%s: %s, would be %s\nwould move %s\n", set, in(from), in(to), counts)
	}

	return fmt.Sprintf("%s: %s, now %s\nmoved %s\n", set, in(from), in(to), counts)
}
