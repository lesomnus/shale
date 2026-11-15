package core

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// §41.2: a producer is assigned a satellite only when it asks for one; the
// one it gets carries its site's selector's labels, is adopted, not erased
// and heard within satellite_down_after, and has room before one that has
// none; the assignment is sticky while that satellite is alive, moves when
// it goes silent, and is let go when the producer stops asking. The
// satellite is told the producers assigned to it, with their certificates'
// serials.
func TestSatelliteAssignment(t *testing.T) {
	core, sink, tenant := producerAliasFixture(t)
	ctx := t.Context()
	now := time.Now()
	core.d.Now = func() time.Time { return now }
	tref := api.TenantRef_builder{Id: tenant.GetId()}.Build()

	type sat struct {
		alias  string
		labels map[string]string
		state  api.HostState
		seen   time.Duration
		full   bool
	}
	add := func(v sat) pdid.Id {
		t.Helper()
		id := pdid.New(DomSatellite)
		st := api.SatelliteStatus_builder{InFlightBudget: 100}
		if v.full {
			st.InFlightBytes = 100
		}
		_, err := sink.Satellite().Add(ctx, api.SatelliteAddRequest_builder{
			Id: id.Bytes(), Alias: v.alias, Labels: v.labels, HardwareId: v.alias, State: v.state,
			DateSeen: timestamppb.New(now.Add(-v.seen)), UploadAddress: "192.0.2.10:7460", Status: st.Build(),
		}.Build())
		require.NoError(t, err)

		return id
	}
	adopted := api.HostState_HOST_STATE_ADOPTED
	add(sat{alias: "other-zone", labels: map[string]string{"zone": "b"}, state: adopted})
	add(sat{alias: "silent", labels: map[string]string{"zone": "a"}, state: adopted, seen: time.Minute})
	add(sat{alias: "pending", labels: map[string]string{"zone": "a"}, state: api.HostState_HOST_STATE_PENDING})
	first := add(sat{alias: "first", labels: map[string]string{"zone": "a", "rack": "1"}, state: adopted})

	site, err := sink.Site().Add(ctx, api.SiteAddRequest_builder{Tenant: tref, Alias: "lobby", SatelliteSelector: map[string]string{"zone": "a"}}.Build())
	require.NoError(t, err)
	set, err := sink.Set().Add(ctx, api.SetAddRequest_builder{Tenant: tref, Alias: "cams", Site: api.SiteRef_builder{Id: site.GetId()}.Build()}.Build())
	require.NoError(t, err)
	pid := pdid.New(DomProducer)
	_, err = sink.Producer().Add(ctx, api.ProducerAddRequest_builder{
		Id: pid.Bytes(), Tenant: tref, Alias: "pi", HardwareId: "pi", State: adopted, CertSerial: "abc",
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), Site: api.SiteRef_builder{Id: site.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	edge := func() []byte {
		p, err := sink.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pid.Bytes()}.Build()}.Build())
		require.NoError(t, err)
		return p.GetSatellite().GetId()
	}

	// Not asked for: none, and no edge.
	a, err := core.satelliteAssignment(ctx, pid, set, false)
	require.NoError(t, err)
	require.Nil(t, a)
	require.Empty(t, edge())

	// Asked for: the one adopted, alive satellite the selector matches,
	// resolved, with an expiry.
	a, err = core.satelliteAssignment(ctx, pid, set, true)
	require.NoError(t, err)
	require.Equal(t, first.Bytes(), a.GetSatelliteId())
	require.Equal(t, first.Bytes(), edge())
	require.Len(t, a.GetEndpoints(), 1)
	require.Equal(t, "192.0.2.10", a.GetEndpoints()[0].GetHost())
	require.Equal(t, int32(7460), a.GetEndpoints()[0].GetPort())
	require.Equal(t, now.Add(SatelliteAssignmentTTL).Unix(), a.GetDateExpires().AsTime().Unix())
	ps, err := core.satelliteProducers(ctx, first)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	require.Equal(t, pid.Bytes(), ps[0].GetProducerId())
	require.Equal(t, "abc", ps[0].GetCertSerial())

	// Sticky while alive, even full and with another that has room.
	roomy := add(sat{alias: "roomy", labels: map[string]string{"zone": "a"}, state: adopted})
	_, err = sink.Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
		Ref: api.SatelliteRef_builder{Id: first.Bytes()}.Build(), Status: api.SatelliteStatus_builder{InFlightBytes: 100, InFlightBudget: 100}.Build(),
		DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	a, err = core.satelliteAssignment(ctx, pid, set, true)
	require.NoError(t, err)
	require.Equal(t, first.Bytes(), a.GetSatelliteId())

	// Silent past satellite_down_after: another, one with room first.
	now = now.Add(DefaultSatelliteDownAfter + time.Second)
	for _, id := range []pdid.Id{roomy} {
		_, err := sink.Satellite().Patch(ctx, api.SatellitePatchRequest_builder{
			Ref: api.SatelliteRef_builder{Id: id.Bytes()}.Build(), DateSeen: timestamppb.New(now), DateUpdatedForce: z.Ptr(true),
		}.Build())
		require.NoError(t, err)
	}
	full := add(sat{alias: "full", labels: map[string]string{"zone": "a"}, state: adopted, full: true})
	a, err = core.satelliteAssignment(ctx, pid, set, true)
	require.NoError(t, err)
	require.Equal(t, roomy.Bytes(), a.GetSatelliteId())
	ps, err = core.satelliteProducers(ctx, first)
	require.NoError(t, err)
	require.Empty(t, ps, "the silent one serves it no more")

	// The only one left that matches is full: taken all the same.
	_, err = sink.Satellite().Erase(ctx, api.SatelliteRef_builder{Id: roomy.Bytes()}.Build())
	require.NoError(t, err)
	a, err = core.satelliteAssignment(ctx, pid, set, true)
	require.NoError(t, err)
	require.Equal(t, full.Bytes(), a.GetSatelliteId())

	// Stops asking: let go.
	a, err = core.satelliteAssignment(ctx, pid, set, false)
	require.NoError(t, err)
	require.Nil(t, a)
	require.Empty(t, edge())

	// None alive: none, and the producer uploads directly.
	now = now.Add(time.Hour)
	a, err = core.satelliteAssignment(ctx, pid, set, true)
	require.NoError(t, err)
	require.Nil(t, a)
	require.Empty(t, edge())
}

// §41.8: the satellite's heartbeat answer carries, for a producer that hands
// it the live of some of its sources, the producer's relay and a feed
// token: for exactly those of them the producer's own publish token would
// name, for that relay, for that satellite, as that producer. A relay that
// predates satellites, a report that names another satellite, and a
// producer that hands nothing over get none.
func TestSatelliteFeedToken(t *testing.T) {
	core, sink, tenant := producerAliasFixture(t)
	ctx := t.Context()
	now := time.Now()
	core.d.Now = func() time.Time { return now }
	key, err := token.Generate("feed")
	require.NoError(t, err)
	core.d.Keys = &Keys{now: core.d.Now, at: now, signing: &key, public: map[string]ed25519.PublicKey{key.Kid: key.Public()}}
	tref := api.TenantRef_builder{Id: tenant.GetId()}.Build()
	adopted := api.HostState_HOST_STATE_ADOPTED

	relay := pdid.New(DomRelay)
	_, err = sink.Relay().Add(ctx, api.RelayAddRequest_builder{
		Id: relay.Bytes(), Alias: "relay", HardwareId: "relay", State: adopted, DateSeen: timestamppb.New(now),
		IngestAddress: "192.0.2.20:7430", Status: api.RelayStatus_builder{SatelliteFeeders: true}.Build(),
	}.Build())
	require.NoError(t, err)
	sat := pdid.New(DomSatellite)
	_, err = sink.Satellite().Add(ctx, api.SatelliteAddRequest_builder{
		Id: sat.Bytes(), Alias: "sat", HardwareId: "sat", State: adopted, DateSeen: timestamppb.New(now), UploadAddress: "192.0.2.10:7460",
	}.Build())
	require.NoError(t, err)
	set, err := sink.Set().Add(ctx, api.SetAddRequest_builder{Tenant: tref, Alias: "cams"}.Build())
	require.NoError(t, err)
	source := func(alias string, ordinal int32, live bool) *api.Source {
		v, err := sink.Source().Add(ctx, api.SourceAddRequest_builder{
			Tenant: tref, Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: alias, Ordinal: ordinal, LiveCapable: z.Ptr(live),
			Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000, DurationSeconds: 60}.Build(),
		}.Build())
		require.NoError(t, err)
		return v
	}
	door, gate, raw := source("door", 1, true), source("gate", 2, true), source("raw", 3, false)
	pid := pdid.New(DomProducer)
	_, err = sink.Producer().Add(ctx, api.ProducerAddRequest_builder{
		Id: pid.Bytes(), Tenant: tref, Alias: "pi", HardwareId: "pi", State: adopted, CertSerial: "abc",
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), Relay: api.RelayRef_builder{Id: relay.Bytes()}.Build(),
		Satellite: api.SatelliteRef_builder{Id: sat.Bytes()}.Build(), Live: z.Ptr(api.LivePolicy_LIVE_POLICY_ON_DEMAND),
	}.Build())
	require.NoError(t, err)
	report := func(satellite []byte, sources ...*api.Source) {
		t.Helper()
		var ids [][]byte
		for _, v := range sources {
			ids = append(ids, v.GetId())
		}
		_, err := sink.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref:              api.ProducerRef_builder{Id: pid.Bytes()}.Build(),
			Status:           api.ProducerStatus_builder{Upload: api.UploadReport_builder{Via: "auto", SatelliteId: satellite, LiveSources: ids, LiveFeed: "auto"}.Build()}.Build(),
			DateUpdatedForce: z.Ptr(true),
		}.Build())
		require.NoError(t, err)
	}
	feed := func() *api.RelayAssignment {
		t.Helper()
		ps, err := core.satelliteProducers(ctx, sat)
		require.NoError(t, err)
		require.Len(t, ps, 1)
		return ps[0].GetRelay()
	}

	require.Nil(t, feed(), "a producer that hands nothing over")
	report(sat.Bytes(), door, raw)
	ra := feed()
	require.NotNil(t, ra)
	require.Equal(t, relay.Bytes(), ra.GetRelayId())
	require.Len(t, ra.GetEndpoints(), 1)
	require.Equal(t, "192.0.2.20", ra.GetEndpoints()[0].GetHost())
	c, err := token.Parse(ra.GetPublishToken())
	require.NoError(t, err)
	require.Equal(t, api.TokenOp_TOKEN_OP_FEED, c.GetOp())
	require.Equal(t, relay.Bytes(), c.GetAud())
	require.Equal(t, sat.Bytes(), c.GetFeeder())
	require.Equal(t, pid.Bytes(), c.GetActor())
	require.Equal(t, tenant.GetId(), c.GetActorTenant())
	require.Equal(t, [][]byte{door.GetId()}, c.GetSources(), "what it hands over that a relay shows, and nothing else")
	require.Equal(t, api.LivePolicy_LIVE_POLICY_ON_DEMAND, c.GetLive())
	require.Positive(t, c.GetRewindSeconds())
	require.Equal(t, now.Add(SatelliteFeedTokenTTL).Unix(), c.GetExp().AsTime().Unix())
	_ = gate

	report(pdid.New(DomSatellite).Bytes(), door)
	require.Nil(t, feed(), "handed to another satellite")

	report(sat.Bytes(), door)
	_, err = sink.Relay().Patch(ctx, api.RelayPatchRequest_builder{
		Ref: api.RelayRef_builder{Id: relay.Bytes()}.Build(), Status: api.RelayStatus_builder{}.Build(), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Nil(t, feed(), "a relay that predates satellites never sees one")
}

// What the CP keeps of a producer's upload report, and whether it asks for
// a satellite (§41.5).
func TestUploadReport(t *testing.T) {
	require.Nil(t, uploadReportOf(nil))
	require.Nil(t, uploadReportOf(api.UploadReport_builder{Via: "relay"}.Build()))
	require.Nil(t, uploadReportOf(api.UploadReport_builder{Via: "auto", Direct: -1}.Build()))
	require.Nil(t, uploadReportOf(api.UploadReport_builder{Via: "auto", SatelliteId: pdid.New(DomNode).Bytes()}.Build()))
	id := pdid.New(DomSatellite)
	v := uploadReportOf(api.UploadReport_builder{Via: "satellite", SatelliteId: id.Bytes(), ThroughSatellite: 3, Fallbacks: 1}.Build())
	require.Equal(t, "satellite", v.GetVia())
	require.Equal(t, int64(3), v.GetThroughSatellite())
	require.True(t, wantsSatellite(v))
	require.True(t, wantsSatellite(api.UploadReport_builder{Via: "auto"}.Build()))
	require.False(t, wantsSatellite(api.UploadReport_builder{Via: "direct"}.Build()))
	require.False(t, wantsSatellite(nil))
	require.True(t, satelliteFull(api.SatelliteStatus_builder{InFlightBytes: 2, InFlightBudget: 2}.Build()))
	require.False(t, satelliteFull(api.SatelliteStatus_builder{InFlightBytes: 1, InFlightBudget: 2}.Build()))
	require.False(t, satelliteFull(nil))

	// The live it hands its satellite (§41.8): Source ids, and a feed that
	// is one of the two.
	src := pdid.New(DomSource)
	v = uploadReportOf(api.UploadReport_builder{Via: "auto", SatelliteId: id.Bytes(), LiveSources: [][]byte{src.Bytes()}, LiveFeed: "auto"}.Build())
	require.Equal(t, [][]byte{src.Bytes()}, v.GetLiveSources())
	require.Equal(t, "auto", v.GetLiveFeed())
	require.Nil(t, uploadReportOf(api.UploadReport_builder{Via: "auto", LiveSources: [][]byte{pdid.New(DomNode).Bytes()}}.Build()))
	require.Nil(t, uploadReportOf(api.UploadReport_builder{Via: "auto", LiveFeed: "satellite"}.Build()))
}
