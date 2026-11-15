package core

import (
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
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
}
