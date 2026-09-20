package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/lesomnus/xli"

	"github.com/lesomnus/payday/pdcmd"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
)

// The state views of §32 that the generated `ls` cannot filter, since a
// state is an enum: hosts waiting for adoption, sinks pending adoption,
// devices in quarantine. Each lists every row and keeps the ones that
// matter, which is the right size for these: they are meant to be empty.

type stateRow struct {
	alias, id, detail string
	since             time.Time
}

type stateView struct {
	group, verb, brief string
	cluster            bool
	list               func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error)
}

func ts(t interface{ AsTime() time.Time }) time.Time {
	if t == nil {
		return time.Time{}
	}

	return t.AsTime()
}

var stateViews = []stateView{
	{"node", "pending", "nodes waiting for adoption (§33.4)", true, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewNodeServiceClient(conn).List(ctx, api.NodeListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetState() == api.HostState_HOST_STATE_PENDING {
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), v.GetHostname() + " " + v.GetJoin().GetSourceAddress(), ts(v.GetDateCreated())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
	{"relay", "pending", "relays waiting for adoption (§33.4)", true, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewRelayServiceClient(conn).List(ctx, api.RelayListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetState() == api.HostState_HOST_STATE_PENDING {
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), v.GetHostname() + " " + v.GetJoin().GetSourceAddress(), ts(v.GetDateCreated())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
	{"producer", "pending", "producers waiting for adoption (§33.4)", false, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewProducerServiceClient(conn).List(ctx, api.ProducerListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetState() == api.HostState_HOST_STATE_PENDING {
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), v.GetHostname() + " " + v.GetJoin().GetSourceAddress(), ts(v.GetDateCreated())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
	{"reader", "pending", "readers waiting for adoption (§33.4)", false, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewReaderServiceClient(conn).List(ctx, api.ReaderListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetState() == api.HostState_HOST_STATE_PENDING {
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), v.GetHostname() + " " + v.GetJoin().GetSourceAddress(), ts(v.GetDateCreated())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
	{"sink", "pending", "sinks reported by a node while their own is down (§28.3)", true, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewSinkServiceClient(conn).List(ctx, api.SinkListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_PENDING_ADOPTION {
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), "reported by " + idStr(v.GetReportedBy()) + ", attached to " + idStr(v.GetNode().GetId()), ts(v.GetDateSeen())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
	{"device", "quarantined", "devices awaiting a decision (§27)", true, func(ctx context.Context, conn pdcmd.Conn) ([]stateRow, error) {
		var out []stateRow
		after := ""
		for {
			vs, err := api.NewDeviceServiceClient(conn).List(ctx, api.DeviceListRequest_builder{Size: 500, After: after}.Build())
			if err != nil {
				return nil, err
			}
			for _, v := range vs.GetItems() {
				if v.GetHealth() == api.DeviceHealth_DEVICE_HEALTH_QUARANTINED || v.GetHealth() == api.DeviceHealth_DEVICE_HEALTH_SUSPECT {
					q := v.GetQuarantine()
					out = append(out, stateRow{v.GetAlias(), idStr(v.GetId()), fmt.Sprintf("%s score %.1f: %s", v.GetHealth().String()[len("DEVICE_HEALTH_"):], v.GetFailureScore(), q.GetReason()), ts(q.GetDate())})
				}
			}
			if after = vs.GetNext(); after == "" {
				return out, nil
			}
		}
	}},
}

func idStr(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	id, err := pdid.From(b)
	if err != nil {
		return "?"
	}

	return id.String()
}

// addStateCommands mounts the views under their groups.
func addStateCommands(t *pdcmd.Tree, c *cmd.Config, cluster bool) {
	for _, v := range stateViews {
		if v.cluster != cluster {
			continue
		}
		v := v
		t.Add(v.group+"/"+v.verb, &xli.Command{
			Name:  v.verb,
			Brief: v.brief,
			Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
				conn, done, err := (&connector{c: c, cluster: v.cluster}).Connect(ctx)
				if err != nil {
					return err
				}
				defer done()
				rows, err := v.list(ctx, conn)
				if err != nil {
					return err
				}
				if len(rows) == 0 {
					fmt.Fprintln(self, "none")
					return nil
				}
				fmt.Fprintf(self, "%-16s %-38s %-20s %s\n", "ALIAS", "ID", "SINCE", "DETAIL")
				for _, r := range rows {
					since := "-"
					if !r.since.IsZero() {
						since = r.since.Local().Format("2006-01-02 15:04:05")
					}
					fmt.Fprintf(self, "%-16s %-38s %-20s %s\n", r.alias, r.id, since, r.detail)
				}

				return nil
			}),
		})
	}
}
