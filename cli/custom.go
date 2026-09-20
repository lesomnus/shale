package cli

import (
	"context"
	"strings"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"

	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/api"

	"github.com/lesomnus/shale/cmd"
)

// custom are the operations that mean something (§32), mounted beside the
// generated verbs: each takes a REF where the request names a row, and the
// rest of the request as protojson.
var custom = map[string]string{
	"node/adopt":                "shale.NodeService.Adopt",
	"node/resolve":              "shale.NodeService.Resolve",
	"relay/adopt":               "shale.RelayService.Adopt",
	"relay/assign":              "shale.RelayService.Assign",
	"producer/adopt":            "shale.ProducerService.Adopt",
	"reader/adopt":              "shale.ReaderService.Adopt",
	"set/negotiate":             "shale.SetService.Negotiate",
	"set/allocate":              "shale.SetService.Allocate",
	"set/live":                  "shale.SetService.Live",
	"source/live":               "shale.SourceService.Live",
	"object/allocate":           "shale.ObjectService.Allocate",
	"object/reallocate":         "shale.ObjectService.Reallocate",
	"object/renew":              "shale.ObjectService.Renew",
	"object/report-attempt":     "shale.ObjectService.ReportAttempt",
	"object/report-failure":     "shale.ObjectService.ReportFailure",
	"object/reschedule":         "shale.ObjectService.Reschedule",
	"object/timeline":           "shale.ObjectService.Timeline",
	"sink/adopt":                "shale.SinkService.Adopt",
	"sink/retire":               "shale.SinkService.Retire",
	"sink/reconcile":            "shale.SinkService.Reconcile",
	"sink/gc":                   "shale.SinkService.Gc",
	"gc/run":                    "shale.SinkService.Gc",
	"device/quarantine":         "shale.DeviceService.Quarantine",
	"device/release":            "shale.DeviceService.Release",
	"device/retire":             "shale.DeviceService.Retire",
	"device/declare-dead":       "shale.DeviceService.DeclareDead",
	"device/locate":             "shale.DeviceService.Locate",
	"signing-key/rotate":        "shale.SigningKeyService.Rotate",
	"placement-policy/activate": "shale.PlacementPolicyService.Activate",
	"upload-policy/activate":    "shale.UploadPolicyService.Activate",
	"address-policy/activate":   "shale.AddressPolicyService.Activate",
}

// groups are the command groups of §32 that no entity generates, with
// their one-line help.
var groups = map[string]string{
	"gc": "GC on demand: `gc run <sink>` runs a round through the node",
}

// addIndexCommands mounts `index rebuild [<sink>]` (§29): every attached
// sink, or one, reconciled from the beginning by the leader.
func addIndexCommands(t *pdcmd.Tree, c *cmd.Config) {
	t.Add("index", &xli.Command{Brief: "the metadata index, a cache of what the sinks hold"})
	t.Add("index/rebuild", &xli.Command{
		Name:  "rebuild",
		Brief: "reconcile every sink, or one, from the beginning",
		Args: arg.Args{
			&arg.String{Name: "SINK", Brief: "one sink, by id or @alias; every sink when absent", Optional: true},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			conn, done, err := (&connector{c: c, cluster: true}).Connect(ctx)
			if err != nil {
				return err
			}
			defer done()
			req := api.SinkReconcileRequest_builder{Full: true}
			if v, ok := arg.Get[string](self, "SINK"); ok && v != "" {
				ref, err := pdcmd.RefParser{}.Parse(v)
				if err != nil {
					return err
				}
				r := &api.SinkRef{}
				if err := ref.Fill(r.ProtoReflect()); err != nil {
					return err
				}
				req.Ref = r
			}
			resp, err := api.NewSinkServiceClient(conn).Reconcile(ctx, req.Build())
			if err != nil {
				return err
			}
			self.Printf("reconciling %d sink(s) from the beginning; watch the leader's log for `reconciled`\n", resp.GetSinks())

			return nil
		}),
	})
}

func addCustom(t *pdcmd.Tree, c *cmd.Config, cluster bool) {
	for path, method := range custom {
		group := path[:strings.IndexByte(path, '/')]
		if clusterGroups[group] != cluster {
			continue
		}
		u, err := t.Unary(method)
		if err != nil {
			continue
		}
		if t.Command(group) == nil {
			t.Add(group, &xli.Command{Brief: groups[group]})
		}
		t.Add(path, u)
	}
}
