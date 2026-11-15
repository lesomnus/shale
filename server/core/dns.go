package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/dnsupdate"
	"github.com/lesomnus/shale/internal/ent/node"
	"github.com/lesomnus/shale/internal/ent/relay"
	"github.com/lesomnus/shale/internal/ent/satellite"
)

// The `dns` address resolver's records (§34.10). The resolver hands out
// the template's names, as `template` does; the leader keeps them in the
// zone the policy names, through RFC 2136 updates signed with TSIG:
//
//   - a name for every adopted node and relay, with A and AAAA records of
//     the addresses its data plane is reached on (dnsAddrs);
//   - looked at every tick (the directives' 5 s), so a host adopted,
//     erased, renamed, or reporting other addresses is in the zone within
//     seconds, whichever CP replica heard of it;
//   - and the whole zone read (AXFR) every dns_sync_every (10 min), on
//     becoming the leader, and when the policy changes, which repairs
//     what changed behind Shale's back and removes the names of hosts the
//     ticks did not see go.
//
// A name is Shale's when it carries the marker, a TXT record naming this
// cluster. Shale writes a name only when it holds nothing at all or
// carries the marker, removes only names carrying it, and within them
// touches A, AAAA and the marker alone; the server checks each of those
// conditions in the same transaction as the update (internal/dnsupdate).

const (
	// DefaultDnsSyncEvery is how often the leader reads the whole zone.
	DefaultDnsSyncEvery = 10 * time.Minute
	// DefaultDnsTTL is the records' TTL when the policy says none.
	DefaultDnsTTL = 60
	// tsigLabel names the KEK's key for TSIG secrets.
	tsigLabel = "shale address-policy dns_tsig_secret"
	// dnsProbeTimeout bounds Activate's question to the server.
	dnsProbeTimeout = 5 * time.Second
	// dnsBackoffMax is the longest the upkeep waits after a failure.
	dnsBackoffMax = 5 * time.Minute
)

// DnsMarker is the TXT record every name the cluster holds carries.
func DnsMarker(owner string) string { return "heritage=shale,shale/owner=" + owner }

// dnsOwner names this cluster in the marker: its operators' tenant, which
// is made once by `shale init`, and which two clusters sharing a zone do
// not share.
func (d *Deps) dnsOwner() string {
	if d.ClusterTenant == (pdid.Id{}) {
		return "shale"
	}

	return d.ClusterTenant.String()
}

// sealTsig seals a TSIG secret with the KEK, under a key of its own.
func (d *Deps) sealTsig(secret []byte) ([]byte, error) {
	k := d.kek()
	if k == nil {
		return nil, status.Error(codes.FailedPrecondition, "dns_tsig_secret: this control plane has no KEK to seal it with; run `shale init`")
	}

	return k.Derive(tsigLabel).Wrap(secret)
}

// openTsig opens what sealTsig sealed.
func (d *Deps) openTsig(sealed []byte) ([]byte, error) {
	if len(sealed) == 0 {
		return nil, errors.New("dns_tsig_secret: none was written")
	}
	k := d.kek()
	if k == nil {
		return nil, errors.New("dns_tsig_secret: this control plane has no KEK to open it with")
	}
	v, err := k.Derive(tsigLabel).Unwrap(sealed)
	if err != nil {
		return nil, errors.New("dns_tsig_secret: does not open with this control plane's KEK; write it again")
	}

	return v, nil
}

func (d *Deps) kek() Kek {
	if d.Kek == nil {
		return nil
	}

	return d.Kek()
}

// dnsConfig is how the leader reaches the zone of a dns policy.
func (d *Deps) dnsConfig(pol *api.AddressPolicy) (dnsupdate.Config, error) {
	p := pol.GetParams()
	secret, err := d.openTsig(pol.GetDnsTsigSecret())
	if err != nil {
		return dnsupdate.Config{}, err
	}

	return dnsupdate.Config{
		Server: p.GetDnsServer(), Zone: p.GetDnsZone(),
		KeyName: p.GetDnsTsigName(), Algorithm: p.GetDnsTsigAlgorithm(), Secret: secret,
	}, nil
}

// checkDnsPolicy is what a dns policy must be to be active (§34.10): a
// template that tells hosts apart and names them inside the zone, a server
// address that parses, a key, and networks that parse. It asks the server
// nothing.
func checkDnsPolicy(p *api.AddressParams, sealed []byte) error {
	tmpl := p.GetTemplate()
	if !strings.Contains(tmpl, "{alias}") && !strings.Contains(tmpl, "{id}") {
		return invalid("params.template", "a dns policy names each host apart: the template carries {alias} (or {id})")
	}
	zone := p.GetDnsZone()
	if !dnsupdate.ValidName(zone) {
		return invalid("params.dns_zone", "the zone the names are in, e.g. nodes.example.com")
	}
	sample := templateName(tmpl, NodeAddresses{Id: pdid.New(DomNode), Alias: "node-1"})
	if !dnsupdate.ValidName(sample) {
		return invalid("params.template", fmt.Sprintf("%q is not a host name", sample))
	}
	if !dnsupdate.InZone(sample, zone) {
		return invalid("params.template", fmt.Sprintf("its names end in the zone, %s; %q does not", dnsupdate.Fqdn(zone), sample))
	}
	if _, err := dnsupdate.ServerAddr(p.GetDnsServer()); err != nil {
		return invalid("params.dns_server", "host or host:port of the server that takes the updates: "+err.Error())
	}
	if !dnsupdate.ValidName(p.GetDnsTsigName()) {
		return invalid("params.dns_tsig_name", "the name of the TSIG key the server takes the updates with")
	}
	if _, ok := dnsupdate.Algorithm(p.GetDnsTsigAlgorithm()); !ok {
		return invalid("params.dns_tsig_algorithm", "hmac-sha256 (default), hmac-sha224, hmac-sha384, hmac-sha512, or hmac-sha1")
	}
	if len(sealed) == 0 {
		return invalid("dns_tsig_secret", "the TSIG key's secret, base64 as tsig-keygen prints it")
	}
	if p.GetDnsTtl() < 0 {
		return invalid("params.dns_ttl", "seconds, zero for the default")
	}
	if _, err := dnsNetworks(p); err != nil {
		return invalid("params.dns_networks", err.Error())
	}

	return nil
}

func dnsNetworks(p *api.AddressParams) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, v := range p.GetDnsNetworks() {
		n, err := netip.ParsePrefix(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("%q is not a CIDR", v)
		}
		out = append(out, n.Masked())
	}

	return out, nil
}

func dnsTTL(p *api.AddressParams) uint32 {
	if v := p.GetDnsTtl(); v > 0 {
		return uint32(v)
	}

	return DefaultDnsTTL
}

// probeDns asks the server for the zone's SOA with the key, which answers
// whether it is there, serves the zone, and takes the key.
func (d *Deps) probeDns(ctx context.Context, pol *api.AddressPolicy) error {
	cfg, err := d.dnsConfig(pol)
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	cfg.Timeout = dnsProbeTimeout
	c, err := dnsupdate.New(cfg)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, dnsProbeTimeout)
	defer cancel()
	if err := c.Probe(ctx); err != nil {
		return status.Errorf(codes.FailedPrecondition, "params.dns_server: asking %s for the SOA of %s with the key %s: %v", cfg.Server, c.Zone(), dnsupdate.Fqdn(cfg.KeyName), err)
	}

	return nil
}

// dnsAddrs are the addresses a host's name carries (§34.10): the IP its
// data plane listens on, when it reports one; otherwise the addresses it
// reports inside the policy's dns_networks, or with none listed, the first
// IPv4 and the first IPv6 address that are neither loopback nor link-local,
// which is what `advertised` hands a caller no rule matches.
func dnsAddrs(ifs []*api.HostInterface, data string, nets []netip.Prefix) []netip.Addr {
	if host, _ := splitAddr(data); host != "" {
		if a, err := netip.ParseAddr(host); err == nil && !a.IsUnspecified() {
			return []netip.Addr{a.Unmap().WithZone("")}
		}
	}
	var all []netip.Addr
	for _, i := range ifs {
		for _, v := range i.GetAddresses() {
			ip, _, _ := strings.Cut(v, "/")
			a, err := netip.ParseAddr(ip)
			if err != nil {
				continue
			}
			a = a.Unmap().WithZone("")
			if a.IsUnspecified() || a.IsMulticast() || a.IsLinkLocalUnicast() {
				continue
			}
			all = append(all, a)
		}
	}
	var out []netip.Addr
	if len(nets) > 0 {
		for _, a := range all {
			if slices.ContainsFunc(nets, func(n netip.Prefix) bool { return n.Contains(a) }) && !slices.Contains(out, a) {
				out = append(out, a)
			}
		}

		return out
	}
	for _, v4 := range []bool{true, false} {
		for _, a := range all {
			if a.Is4() == v4 && !a.IsLoopback() {
				out = append(out, a)
				break
			}
		}
	}

	return out
}

// DnsRecords is the leader's upkeep of a dns policy's records.
type DnsRecords struct {
	d *Deps
	// Every is how often the hosts are compared with what the zone was
	// told; SyncEvery how often the whole zone is read.
	Every     time.Duration
	SyncEvery time.Duration
	// Leader says whether this process may update the zone now.
	Leader func(ctx context.Context) bool
	Log    *slog.Logger

	mu sync.Mutex
	// key is the policy and parameters the state below is of.
	key string
	// have is what the zone holds at the names this cluster owns, as last
	// read or written; nil until a pass learned it.
	have map[string]*dnsupdate.Records
	// conflict are names a host wants that somebody else holds.
	conflict map[string]bool
	// next is when the zone is read whole again, after xfrFails reads in
	// a row that failed; retry is when names that failed are tried again,
	// after fails passes in a row that failed.
	next     time.Time
	xfrFails int
	retry    time.Time
	fails    int
	// synced is when a whole read last went through.
	synced time.Time
	// status is the policy's dns_sync as last written.
	status *api.DnsSync
	// Syncs counts the reads of the whole zone, for tests.
	Syncs int
}

// NewDnsRecords makes the upkeep; with a nil Leader every tick runs.
func NewDnsRecords(d *Deps) *DnsRecords {
	return &DnsRecords{d: d, Every: DefaultDirectivesEvery, SyncEvery: DefaultDnsSyncEvery}
}

func (r *DnsRecords) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}

	return r.d.log()
}

// Spin runs the upkeep until the context is done.
func (r *DnsRecords) Spin(ctx context.Context) error {
	t := time.NewTicker(r.Every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if r.Leader != nil && !r.Leader(ctx) {
			// Another replica updates the zone meanwhile: what this one
			// knew of it is stale by the time it leads again.
			r.forget()
			continue
		}
		if err := r.Once(ctx); err != nil {
			r.log().Warn("dns", "err", err.Error())
		}
	}
}

func (r *DnsRecords) forget() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.key, r.have, r.conflict, r.status = "", nil, nil, nil
	r.next, r.retry, r.fails, r.xfrFails, r.synced = time.Time{}, time.Time{}, 0, 0, time.Time{}
}

// Sync reads the whole zone on the next pass, and runs it.
func (r *DnsRecords) Sync(ctx context.Context) error {
	r.mu.Lock()
	r.next, r.retry = time.Time{}, time.Time{}
	r.mu.Unlock()

	return r.Once(ctx)
}

// Once compares the hosts with the zone once: the whole zone when it is
// due, else the names whose hosts changed.
func (r *DnsRecords) Once(ctx context.Context) error {
	pol, err := r.active(ctx)
	if err != nil {
		return err
	}
	if pol == nil || pol.GetParams().GetResolver() != "dns" {
		r.forget()
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.d.now()
	if key := dnsPolicyKey(pol); key != r.key {
		r.key, r.have, r.conflict, r.status = key, nil, nil, pol.GetDnsSync()
		r.next, r.retry, r.fails, r.xfrFails = time.Time{}, time.Time{}, 0, 0
	}
	if !r.synced.IsZero() {
		r.d.metrics().DnsSyncLag.Record(ctx, int64(now.Sub(r.synced)/time.Second))
	}

	cfg, err := r.d.dnsConfig(pol)
	if err == nil {
		var c *dnsupdate.Client
		if c, err = dnsupdate.New(cfg); err == nil {
			return r.pass(ctx, pol, c, now)
		}
	}
	// A policy that cannot be used as it is: the next change to it is
	// what helps, so it is said again only after a while.
	if now.Before(r.retry) {
		return nil
	}
	r.failed(ctx, pol, now, err)

	return nil
}

// pass is one comparison, the whole zone's when it is due.
func (r *DnsRecords) pass(ctx context.Context, pol *api.AddressPolicy, c *dnsupdate.Client, now time.Time) error {
	want, err := r.desired(ctx, pol.GetParams(), c.Zone())
	if err != nil {
		return err
	}
	if r.have == nil || !now.Before(r.next) {
		return r.sync(ctx, pol, c, want, now)
	}
	if now.Before(r.retry) {
		return nil
	}
	if err := r.follow(ctx, c, want); err != nil {
		r.failed(ctx, pol, now, err)
		return nil
	}
	r.fails, r.retry = 0, time.Time{}

	return nil
}

// active is the active address policy, as Own reads it: with its sealed
// secret.
func (r *DnsRecords) active(ctx context.Context) (*api.AddressPolicy, error) {
	vs, err := r.d.Own.AddressPolicy().List(ctx, api.AddressPolicyListRequest_builder{Size: 100}.Build())
	if err != nil {
		return nil, err
	}
	var out *api.AddressPolicy
	for _, v := range vs.GetItems() {
		if v.GetActive() {
			out = v
		}
	}

	return out, nil
}

// dnsPolicyKey tells one policy and its parameters from another, and not
// from itself with another dns_sync.
func dnsPolicyKey(pol *api.AddressPolicy) string {
	h := sha256.New()
	h.Write(pol.GetId())
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(pol.GetParams())
	h.Write(b)
	h.Write(pol.GetDnsTsigSecret())

	return hex.EncodeToString(h.Sum(nil))
}

// desired is what the zone should hold: a name per adopted node, relay and
// satellite,
// with the addresses each reports; hosts the template gives one name
// share it.
func (r *DnsRecords) desired(ctx context.Context, p *api.AddressParams, zone string) (map[string]*dnsupdate.Want, error) {
	nets, _ := dnsNetworks(p)
	ttl := dnsTTL(p)
	out := map[string]*dnsupdate.Want{}
	add := func(n NodeAddresses) {
		name := dnsupdate.Fqdn(templateName(p.GetTemplate(), n))
		if !dnsupdate.ValidName(name) || !dnsupdate.InZone(name, zone) {
			return
		}
		addrs := dnsAddrs(n.Interfaces, n.DataAddress, nets)
		if len(addrs) == 0 {
			return
		}
		w := out[name]
		if w == nil {
			w = &dnsupdate.Want{TTL: ttl}
			out[name] = w
		}
		for _, a := range addrs {
			if !slices.Contains(w.Addrs, a) {
				w.Addrs = append(w.Addrs, a)
			}
		}
		slices.SortFunc(w.Addrs, netip.Addr.Compare)
	}

	nodes, err := r.d.Ent.Node.Query().
		Where(node.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), node.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		add(NodeAddresses{Id: pdid.Id(n.Id), Alias: n.Alias, Interfaces: n.Interfaces, DataAddress: n.DataAddress})
	}
	relays, err := r.d.Ent.Relay.Query().
		Where(relay.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), relay.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range relays {
		add(NodeAddresses{Id: pdid.Id(v.Id), Alias: v.Alias, Interfaces: v.Interfaces, DataAddress: v.IngestAddress})
	}
	sats, err := r.d.Ent.Satellite.Query().
		Where(satellite.StateEQ(int32(api.HostState_HOST_STATE_ADOPTED)), satellite.DateErasedIsNil()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range sats {
		add(NodeAddresses{Id: pdid.Id(v.Id), Alias: v.Alias, Interfaces: v.Interfaces, DataAddress: v.UploadAddress})
	}

	return out, nil
}

// sync reads the whole zone and makes the names this cluster owns what the
// hosts want.
func (r *DnsRecords) sync(ctx context.Context, pol *api.AddressPolicy, c *dnsupdate.Client, want map[string]*dnsupdate.Want, now time.Time) error {
	r.Syncs++
	zone, err := c.Transfer(ctx)
	if err != nil {
		// Without the zone the names the hosts want are still kept, one
		// by one, every tick; a name nobody wants any more waits for a
		// read that goes through, since only that finds it.
		r.xfrFails++
		r.next = now.Add(min(r.Every*time.Duration(1<<uint(min(r.xfrFails, 10))), r.syncEvery()))
		if r.have == nil {
			r.have = map[string]*dnsupdate.Records{}
		}
		if ferr := r.follow(ctx, c, want); ferr != nil {
			err = errors.Join(err, ferr)
		}
		r.failed(ctx, pol, now, err)

		return nil
	}
	r.xfrFails = 0

	marker := DnsMarker(r.d.dnsOwner())
	have := map[string]*dnsupdate.Records{}
	for name, rec := range zone {
		if rec.OwnedBy(marker) {
			have[name] = rec
		}
	}
	conflict := map[string]bool{}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(union(want, have))) {
		got, foreign, err := r.converge(ctx, c, name, zone[name], want[name], marker)
		if err != nil {
			errs = append(errs, err)
		}
		switch {
		case foreign:
			conflict[name] = true
			if !r.conflict[name] {
				r.log().Warn("dns: a name a host wants holds records this cluster does not own; left alone", "name", name)
			}
		case got != nil:
			have[name] = got
		default:
			delete(have, name)
		}
	}
	r.have, r.conflict = have, conflict
	r.next = now.Add(r.syncEvery())

	st := api.DnsSync_builder{
		DateSynced: timestamppb.New(now),
		Names:      int32(len(have)),
		Conflicts:  slices.Sorted(maps.Keys(conflict)),
	}.Build()
	r.synced = now
	r.d.metrics().DnsSyncLag.Record(ctx, 0)
	if err := errors.Join(errs...); err != nil {
		st.SetError(err.Error())
		st.SetDateFailed(timestamppb.New(now))
		r.backoff(now)
	} else {
		r.fails, r.retry = 0, time.Time{}
	}
	r.write(ctx, pol, st)

	return nil
}

// follow brings the names whose hosts changed since the zone was last
// read or written: it asks the server about each before it writes.
func (r *DnsRecords) follow(ctx context.Context, c *dnsupdate.Client, want map[string]*dnsupdate.Want) error {
	marker := DnsMarker(r.d.dnsOwner())
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(union(want, r.have))) {
		w, h := want[name], r.have[name]
		if r.conflict[name] {
			// Until the next whole read: asking every tick would not
			// change whose it is.
			continue
		}
		if w == nil && h == nil || w != nil && h != nil && h.Holds(*w) {
			continue
		}
		cur, err := c.Lookup(ctx, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		got, foreign, err := r.converge(ctx, c, name, cur, w, marker)
		if err != nil {
			errs = append(errs, err)
		}
		switch {
		case foreign:
			if r.conflict == nil {
				r.conflict = map[string]bool{}
			}
			r.conflict[name] = true
			r.log().Warn("dns: a name a host wants holds records this cluster does not own; left alone", "name", name)
		case got != nil:
			r.have[name] = got
		default:
			delete(r.have, name)
		}
	}

	return errors.Join(errs...)
}

// converge makes one name what a host wants, given what it holds now:
// created when it holds nothing, replaced or removed when it carries the
// marker, and left alone otherwise. It answers what the name holds after,
// nil for nothing of this cluster's, and whether somebody else holds it.
func (r *DnsRecords) converge(ctx context.Context, c *dnsupdate.Client, name string, cur *dnsupdate.Records, w *dnsupdate.Want, marker string) (*dnsupdate.Records, bool, error) {
	owned := cur.OwnedBy(marker)
	var op string
	var err error
	switch {
	case w == nil && !owned:
		return nil, false, nil
	case w == nil:
		op, err = "delete", c.Delete(ctx, name, cur, marker)
	case cur.Empty():
		op, err = "create", c.Create(ctx, name, *w, marker)
	case !owned:
		r.d.metrics().DnsUpdates.Add(ctx, 1, dnsAttrs("create", "conflict"))
		return nil, true, nil
	case cur.Holds(*w):
		return cur, false, nil
	default:
		op, err = "replace", c.Replace(ctx, name, cur, *w, marker)
	}

	if err != nil {
		result := "error"
		if errors.Is(err, dnsupdate.ErrPrerequisite) {
			// Somebody wrote the name since it was read: the next pass
			// reads it again.
			result = "conflict"
		}
		r.d.metrics().DnsUpdates.Add(ctx, 1, dnsAttrs(op, result))
		r.log().Warn("dns: update failed", "op", op, "name", name, "err", err.Error())
		if !owned {
			cur = nil
		}

		return cur, false, fmt.Errorf("%s %s: %w", op, name, err)
	}
	r.d.metrics().DnsUpdates.Add(ctx, 1, dnsAttrs(op, "ok"))
	if w == nil {
		r.log().Info("dns: name removed", "name", name)
		return nil, false, nil
	}
	addrs := make([]string, len(w.Addrs))
	for i, a := range w.Addrs {
		addrs[i] = a.String()
	}
	r.log().Info("dns: name "+op+"d", "name", name, "addrs", strings.Join(addrs, ","))
	txt := [][]string{{marker}}
	if owned {
		txt = cur.TXT
	}

	return &dnsupdate.Records{Addrs: slices.Clone(w.Addrs), TTL: w.TTL, TXT: txt}, false, nil
}

// failed says a pass failed: in the log, in the metric the update itself
// counted, and on the policy, and it waits before the next.
func (r *DnsRecords) failed(ctx context.Context, pol *api.AddressPolicy, now time.Time, err error) {
	r.log().Warn("dns: the zone is not up to date", "zone", pol.GetParams().GetDnsZone(), "err", err.Error())
	r.backoff(now)
	st := proto.CloneOf(r.status)
	if st == nil {
		st = &api.DnsSync{}
	}
	if st.GetError() == err.Error() {
		return
	}
	st.SetError(err.Error())
	st.SetDateFailed(timestamppb.New(now))
	r.write(ctx, pol, st)
}

func (r *DnsRecords) backoff(now time.Time) {
	r.fails++
	wait := min(r.Every*time.Duration(1<<uint(min(r.fails, 10))), dnsBackoffMax)
	r.retry = now.Add(wait)
}

// write puts the upkeep's state on the policy, where an operator reads it.
func (r *DnsRecords) write(ctx context.Context, pol *api.AddressPolicy, st *api.DnsSync) {
	if proto.Equal(st, r.status) {
		return
	}
	if _, err := r.d.Own.AddressPolicy().Patch(ctx, api.AddressPolicyPatchRequest_builder{
		Ref:              api.AddressPolicyRef_builder{Id: pol.GetId()}.Build(),
		DnsSync:          st,
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		r.log().Warn("dns: writing the policy's dns_sync", "err", err.Error())
		return
	}
	r.status = st
}

func (r *DnsRecords) syncEvery() time.Duration {
	if r.SyncEvery > 0 {
		return r.SyncEvery
	}

	return DefaultDnsSyncEvery
}

func union[A, B any](a map[string]A, b map[string]B) map[string]bool {
	out := make(map[string]bool, len(a)+len(b))
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}

	return out
}

func dnsAttrs(op, result string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op), attribute.String("result", result))
}
