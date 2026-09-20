package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdcmd"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
)

// addObjectCommands mounts `object reschedule` (§20.3, §32) by hand. The
// generated verb makes REF mandatory because the request has a `ref`, but
// the bulk form names a set or a source and a time range instead, so the
// reference is optional here and the rest are flags.
func addObjectCommands(t *pdcmd.Tree, c *cmd.Config) {
	t.Add("object/reschedule", &xli.Command{
		Name:  "reschedule",
		Brief: "change when objects expire and are deleted: one, or a set's or a source's over a range",
		Flags: flg.Flags{
			&flg.String{Name: "set", Brief: "every object of the set, by id or @tenant/alias, within --from and --to"},
			&flg.String{Name: "source", Brief: "every object of the source, by id or @tenant/alias, within --from and --to"},
			&flg.String{Name: "from", Brief: "start of the range: RFC 3339, a date, `now`, or a duration from now (--from=-24h)"},
			&flg.String{Name: "to", Brief: "end of the range, in the same forms"},
			&flg.String{Name: "expired", Brief: "the new date_expired, in the same forms; absent leaves it"},
			&flg.String{Name: "deleted", Brief: "the new date_deleted, in the same forms; absent leaves it"},
			&flg.Switch{Name: "delete-now", Brief: "set both dates to now: the objects read as deleted at once"},
			&flg.String{Name: "reason", Brief: "why, for the audit trail (required)"},
		},
		Args: arg.Args{
			&arg.String{Name: "REF", Brief: "one object, by id; absent with --set or --source", Optional: true},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			var o rescheduleOpts
			o.ref, _ = arg.Get[string](self, "REF")
			o.set, _ = flg.Find[string](self, "set")
			o.source, _ = flg.Find[string](self, "source")
			o.from, _ = flg.Find[string](self, "from")
			o.to, _ = flg.Find[string](self, "to")
			o.expired, _ = flg.Find[string](self, "expired")
			o.deleted, _ = flg.Find[string](self, "deleted")
			o.deleteNow, _ = flg.Find[bool](self, "delete-now")
			o.reason, _ = flg.Find[string](self, "reason")
			req, err := buildReschedule(o, time.Now())
			if err != nil {
				return err
			}

			conn, done, err := (&connector{c: c}).Connect(ctx)
			if err != nil {
				return err
			}
			defer done()
			// A bulk reschedule works in pages and stops short of its
			// deadline; the answer says how many remain, and the same
			// request (its dates were fixed above) goes again for them.
			client := api.NewObjectServiceClient(conn)
			var total int64
			for {
				resp, err := client.Reschedule(ctx, req)
				if err != nil {
					if total > 0 {
						return fmt.Errorf("after %d object(s): %w", total, err)
					}

					return err
				}
				total += resp.GetChanged()
				if resp.GetRemaining() == 0 {
					break
				}
				self.Printf("rescheduled %d object(s), %d to go\n", total, resp.GetRemaining())
			}
			self.Printf("rescheduled %d object(s)\n", total)

			return nil
		}),
	})
}

// rescheduleOpts is `object reschedule` as typed.
type rescheduleOpts struct {
	ref, set, source           string
	from, to, expired, deleted string
	deleteNow                  bool
	reason                     string
}

// buildReschedule turns the command line into the request, refusing the
// combinations the server would: exactly one of an object, a set, or a
// source; a range with the bulk forms and none with an object; something to
// change; a reason.
func buildReschedule(o rescheduleOpts, now time.Time) (*api.ObjectRescheduleRequest, error) {
	req := api.ObjectRescheduleRequest_builder{DeleteNow: o.deleteNow, Reason: strings.TrimSpace(o.reason)}
	named := 0
	if o.ref != "" {
		r := &api.ObjectRef{}
		if err := fillRef(r, o.ref, "shale.Object"); err != nil {
			return nil, err
		}
		req.Ref, named = r, named+1
	}
	if o.set != "" {
		r := &api.SetRef{}
		if err := fillRef(r, o.set, "shale.Set"); err != nil {
			return nil, err
		}
		req.Set, named = r, named+1
	}
	if o.source != "" {
		r := &api.SourceRef{}
		if err := fillRef(r, o.source, "shale.Source"); err != nil {
			return nil, err
		}
		req.Source, named = r, named+1
	}
	if named != 1 {
		return nil, fmt.Errorf("name exactly one of an object (REF), --set, or --source")
	}
	switch {
	case req.Ref != nil && (o.from != "" || o.to != ""):
		return nil, fmt.Errorf("--from and --to go with --set or --source, not with one object")
	case req.Ref == nil && (o.from == "" || o.to == ""):
		return nil, fmt.Errorf("--set and --source take a range: --from and --to")
	}

	when := func(name, s string) (*timestamppb.Timestamp, error) {
		if s == "" {
			return nil, nil
		}
		t, err := parseWhen(s, now)
		if err != nil {
			return nil, fmt.Errorf("--%s: %w", name, err)
		}

		return timestamppb.New(t), nil
	}
	var err error
	if req.From, err = when("from", o.from); err != nil {
		return nil, err
	}
	if req.To, err = when("to", o.to); err != nil {
		return nil, err
	}
	if req.From != nil && !req.To.AsTime().After(req.From.AsTime()) {
		return nil, fmt.Errorf("--to must be after --from")
	}
	if req.DateExpired, err = when("expired", o.expired); err != nil {
		return nil, err
	}
	if req.DateDeleted, err = when("deleted", o.deleted); err != nil {
		return nil, err
	}
	switch {
	case o.deleteNow && (req.DateExpired != nil || req.DateDeleted != nil):
		return nil, fmt.Errorf("--delete-now sets both dates itself")
	case !o.deleteNow && req.DateExpired == nil && req.DateDeleted == nil:
		return nil, fmt.Errorf("nothing to change: --expired, --deleted, or --delete-now")
	case req.Reason == "":
		return nil, fmt.Errorf("--reason is required and audited")
	}

	return req.Build(), nil
}

// fillRef reads a reference off the command line into a generated Ref,
// checking the kind it claims against the entity's full name.
func fillRef(dst interface{ ProtoReflect() protoreflect.Message }, s, entity string) error {
	ref, err := pdcmd.RefParser{}.Parse(s)
	if err != nil {
		return err
	}
	if d, ok := pdid.Lookup(entity); ok {
		if err := ref.Expect(d); err != nil {
			return err
		}
	}

	return ref.Fill(dst.ProtoReflect())
}

// parseWhen reads a moment off the command line: RFC 3339, a date (that
// day's midnight, UTC), `now`, or a duration from now (`-24h`, `+1h`). A
// signed one is typed joined to its flag (`--from=-24h`): on its own, an
// argument that starts with `-` is read as a flag.
func parseWhen(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "":
		return time.Time{}, fmt.Errorf("empty")
	case s == "now":
		return now, nil
	case s[0] == '+' || s[0] == '-':
		d, err := time.ParseDuration(s[1:])
		if err != nil {
			return time.Time{}, fmt.Errorf("%q: %w", s, err)
		}
		if s[0] == '-' {
			d = -d
		}

		return now.Add(d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("%q: not RFC 3339, a date, `now`, or a duration from now", s)
}
