package core

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// starvationFixture is a set with auto_raise, its producer, and a clock:
// what the CP's half of §38.5 needs, without a producer.
type starvationFixture struct {
	t        *testing.T
	core     Core
	sink     pd.Sink
	tenant   *api.Tenant
	set      *api.Set
	producer *api.Producer
	now      time.Time
	logs     *lockedBuffer
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.b.String(), s)
}

func newStarvationFixture(t *testing.T, set *api.SetAddRequest_builder, live api.LivePolicy) *starvationFixture {
	t.Helper()
	ctx := t.Context()
	cfg := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"}
	db, dialect, err := cfg.Open(ctx)
	require.NoError(t, err)
	driver := sql.OpenDB(dialect, db)
	client := enttest.NewClient(t, enttest.WithOptions(ent.Driver(driver)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	sink, err := pd.NewSink(client, bare.WithMinter(pd.Minter()))
	require.NoError(t, err)
	f := &starvationFixture{t: t, sink: sink, now: time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC), logs: &lockedBuffer{}}
	f.core = New(sink, &Deps{Ent: client, Own: sink, Drv: driver, Now: func() time.Time { return f.now }, Log: slog.New(slog.NewTextHandler(f.logs, nil))})
	f.tenant, err = sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
	require.NoError(t, err)
	set.Tenant = api.TenantRef_builder{Id: f.tenant.GetId()}.Build()
	set.Alias = "yard"
	f.set, err = sink.Set().Add(ctx, set.Build())
	require.NoError(t, err)
	f.producer, err = sink.Producer().Add(ctx, api.ProducerAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: f.tenant.GetId()}.Build(), Alias: "yard-pi", HardwareId: "yard-pi",
		Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), State: api.HostState_HOST_STATE_ADOPTED, Live: &live,
	}.Build())
	require.NoError(t, err)

	return f
}

func (f *starvationFixture) source(alias string, ordinal int32, ceiling int64) *api.Source {
	f.t.Helper()
	v, err := f.sink.Source().Add(f.t.Context(), api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: f.tenant.GetId()}.Build(), Set: api.SetRef_builder{Id: f.set.GetId()}.Build(),
		Alias: alias, Ordinal: ordinal, Profile: api.SegmentProfile_builder{MaxBitrate: ceiling, DurationSeconds: 128, KeyframeIntervalMs: 2000}.Build(),
	}.Build())
	require.NoError(f.t, err)

	return v
}

func (f *starvationFixture) get(src *api.Source) *api.Source {
	f.t.Helper()
	v, err := f.sink.Source().Get(f.t.Context(), api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.NoError(f.t, err)

	return v
}

func (f *starvationFixture) version() int64 {
	f.t.Helper()
	v, err := f.sink.Set().Get(f.t.Context(), api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build()}.Build())
	require.NoError(f.t, err)

	return v.GetProfileVersion()
}

// patchSet changes the set and keeps the fixture's copy current.
func (f *starvationFixture) patchSet(b api.SetPatchRequest_builder) {
	f.t.Helper()
	b.Ref = api.SetRef_builder{Id: f.set.GetId()}.Build()
	b.DateUpdatedForce = z.Ptr(true)
	v, err := f.sink.Set().Patch(f.t.Context(), b.Build())
	require.NoError(f.t, err)
	f.set = v
}

// report is one heartbeat's account of a source: `seconds` of it, `atCap`
// of them at the cap, and `episodes`.
func report(src *api.Source, rc api.RateControl, seconds, atCap, episodes int64) *api.SourceReport {
	return api.SourceReport_builder{SourceId: src.GetId(), RateControl: rc, SecondsTotal: seconds, SecondsAtCap: atCap, Episodes: episodes}.Build()
}

// beat is one heartbeat at the fixture's clock, with the uplink the
// producer declares.
func (f *starvationFixture) beat(uplink int64, rs ...*api.SourceReport) []*api.Suggestion {
	f.t.Helper()
	out, err := f.core.starvation(f.t.Context(), f.producer, uplink, rs)
	require.NoError(f.t, err)

	return out
}

// starve is a heartbeat whose one report starves a source on its own:
// 50 episodes in an hour, 5% of it at the cap.
func (f *starvationFixture) starve(src *api.Source, rc api.RateControl, uplink int64, others ...*api.SourceReport) *api.Suggestion {
	f.t.Helper()
	out := f.beat(uplink, append([]*api.SourceReport{report(src, rc, 3600, 180, 50)}, others...)...)
	for _, sg := range out {
		if bytes.Equal(sg.GetSourceId(), src.GetId()) {
			return sg
		}
	}
	f.t.Fatalf("no suggestion for %s", src.GetAlias())

	return nil
}

func (f *starvationFixture) negotiate(proposals ...*api.SourceProposal) *api.SetNegotiateResponse {
	f.t.Helper()
	ctx := frame.Into(context.Background(), frame.New(pdid.New(DomHolder), mustId(f.tenant.GetId()), frame.Whole()).WithScope(frame.Only(mustId(f.tenant.GetId()))))
	resp, err := f.core.Set().Negotiate(ctx, api.SetNegotiateRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build(), Sources: proposals}.Build())
	require.NoError(f.t, err)

	return resp
}

func proposal(src *api.Source, ceiling int64) *api.SourceProposal {
	return api.SourceProposal_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: ceiling}.Build()}.Build()
}

func agreedOf(resp *api.SetNegotiateResponse, src *api.Source) *api.SegmentProfile {
	for _, ag := range resp.GetSources() {
		if bytes.Equal(ag.GetSourceId(), src.GetId()) {
			return ag.GetProfile()
		}
	}

	return nil
}

const vbr = api.RateControl_RATE_CONTROL_VBR

// A day of heartbeats adds up (§38.5): a source is starved once 50 episodes
// fell in its day while fewer than 10% of the day's seconds were at the
// cap, and with auto_raise its ceiling is raised by a quarter. The raise
// lands at the producer's next negotiation, which the new profile version
// asks for, and stays at every one after it.
func TestStarvationAccumulatesToAutoRaise(t *testing.T) {
	f := newStarvationFixture(t, &api.SetAddRequest_builder{AutoRaise: true}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	door := f.source("door", 1, 4_000_000)
	start := f.now
	version := f.version()

	// A heartbeat every 30 s; an episode in every other one, 3 s at the
	// cap each: 5% of the seconds.
	for i := 1; i <= 99; i++ {
		f.now = f.now.Add(30 * time.Second)
		episodes, atCap := int64(0), int64(0)
		if i%2 == 0 {
			episodes, atCap = 1, 3
		}
		out := f.beat(0, report(door, vbr, 30, atCap, episodes))
		require.Empty(t, out, "heartbeat %d: not starved yet", i)
	}
	st := f.get(door).GetStarvation()
	require.Equal(t, int64(49), st.GetEpisodes())
	require.Equal(t, int64(99*30), st.GetSecondsTotal())
	require.False(t, st.GetStarved())
	require.True(t, st.GetDateReset().AsTime().Equal(start.Add(30*time.Second)), "the day began with the first report")

	f.now = f.now.Add(30 * time.Second)
	out := f.beat(0, report(door, vbr, 30, 3, 1))
	require.Len(t, out, 1)
	require.True(t, out[0].GetApplied())
	require.Equal(t, int64(4_000_000), out[0].GetMaxBitrate())
	require.Equal(t, int64(5_000_000), out[0].GetSuggested())

	got := f.get(door)
	st = got.GetStarvation()
	require.True(t, st.GetDateRaised().AsTime().Equal(f.now))
	require.Equal(t, int64(4_000_000), st.GetRaisedFrom())
	require.Equal(t, int64(5_000_000), st.GetRaisedMaxBitrate())
	require.True(t, st.GetDateReset().AsTime().Equal(f.now), "a raise starts a new day")
	require.Zero(t, st.GetEpisodes())
	require.False(t, st.GetStarved())
	require.Nil(t, st.GetBlocked())
	require.Equal(t, int64(4_000_000), got.GetProfile().GetMaxBitrate(), "agreed at the next negotiation, not written behind the producer's back")
	require.Equal(t, version+1, f.version(), "a new profile version makes the producer negotiate again")
	require.Equal(t, 1, f.logs.count("msg=auto_raise"))

	// The producer proposes what it had, or its starting ceiling after a
	// restart: the raised ceiling is agreed, its segment re-derived.
	resp := f.negotiate(proposal(door, 4_000_000))
	agreed := agreedOf(resp, door)
	require.Equal(t, int64(5_000_000), agreed.GetMaxBitrate())
	require.Equal(t, int64(107), agreed.GetDurationSeconds(), "64 MB at 5 Mbps")
	said := false
	for _, a := range resp.GetAdjustments() {
		said = said || (a.GetField() == "max_bitrate" && a.GetAgreed() == "5000000" && strings.Contains(a.GetReason(), "auto_raise"))
	}
	require.True(t, said, "the producer is told why it was agreed more than it proposed")
	require.Equal(t, int64(5_000_000), f.get(door).GetProfile().GetMaxBitrate())
	// A proposal above it is the producer's to make.
	require.Equal(t, int64(6_000_000), agreedOf(f.negotiate(proposal(door, 6_000_000)), door).GetMaxBitrate())

	// Turning auto_raise off hands the ceiling back to what the producer
	// proposes.
	f.patchSet(api.SetPatchRequest_builder{AutoRaise: z.Ptr(false)})
	require.Equal(t, int64(4_000_000), agreedOf(f.negotiate(proposal(door, 4_000_000)), door).GetMaxBitrate())
}

// At most once a day (§38.5): a source starved again within 24 hours of
// its last raise is suggested a raise but not raised, and is raised once
// the 24 hours have passed.
func TestAutoRaiseAtMostOnceADay(t *testing.T) {
	f := newStarvationFixture(t, &api.SetAddRequest_builder{AutoRaise: true}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	door := f.source("door", 1, 4_000_000)
	raised := f.now
	require.True(t, f.starve(door, vbr, 0).GetApplied())
	version := f.version()

	f.now = raised.Add(time.Hour)
	sg := f.starve(door, vbr, 0)
	require.False(t, sg.GetApplied(), "raised an hour ago")
	require.Equal(t, int64(5_000_000), sg.GetMaxBitrate(), "the raised ceiling is the one starved now")
	require.Equal(t, int64(6_250_000), sg.GetSuggested())
	st := f.get(door).GetStarvation()
	require.True(t, st.GetStarved())
	require.Equal(t, int64(6_250_000), st.GetSuggestedMaxBitrate())
	require.True(t, st.GetDateRaised().AsTime().Equal(raised))

	f.now = raised.Add(24*time.Hour - time.Second)
	require.False(t, f.beat(0, report(door, vbr, 30, 0, 0))[0].GetApplied(), "a second short of a day")
	require.Equal(t, version, f.version())

	f.now = raised.Add(24 * time.Hour)
	require.True(t, f.beat(0, report(door, vbr, 30, 0, 0))[0].GetApplied(), "a day after the last raise")
	st = f.get(door).GetStarvation()
	require.Equal(t, int64(5_000_000), st.GetRaisedFrom())
	require.Equal(t, int64(6_250_000), st.GetRaisedMaxBitrate())
	require.Equal(t, version+1, f.version())
}

// Every raise stays inside the upload policy's cap, the set's
// max_bitrate_total, and the producer's uplink as producer admission counts
// it (§38.5). A raise that breaks one is not applied: the limit and its
// amounts are kept on the source, the CP warns once, and the raise is
// applied when the limit moves.
func TestAutoRaiseStaysInsideLimits(t *testing.T) {
	cases := []struct {
		name     string
		set      api.SetAddRequest_builder
		live     api.LivePolicy
		ceilings []int64
		uplink   int64
		want     *api.RaiseBlocked
		// lift moves the limit out of the way.
		lift func(f *starvationFixture) int64
	}{
		{
			name: "upload policy cap", ceilings: []int64{30_000_000}, live: api.LivePolicy_LIVE_POLICY_ON_DEMAND,
			want: api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_UPLOAD_POLICY, Required: 37_500_000, Available: DefaultMaxBitrateCap}.Build(),
		},
		{
			name: "the set's max_bitrate_total", set: api.SetAddRequest_builder{MaxBitrateTotal: 8_500_000}, ceilings: []int64{4_000_000, 4_000_000}, live: api.LivePolicy_LIVE_POLICY_ON_DEMAND,
			want: api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_SET_BITRATE, Required: 9_000_000, Available: 8_500_000}.Build(),
			lift: func(f *starvationFixture) int64 {
				f.patchSet(api.SetPatchRequest_builder{MaxBitrateTotal: z.Ptr(int64(9_000_000))})
				return 0
			},
		},
		{
			// 9 Mbps × 2.2: recording and live at all times.
			name: "the producer's uplink, live always", ceilings: []int64{4_000_000, 4_000_000}, live: api.LivePolicy_LIVE_POLICY_ALWAYS, uplink: 12_000_000,
			want: api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_UPLINK, Required: 19_800_000, Available: 12_000_000}.Build(),
			lift: func(*starvationFixture) int64 { return 20_000_000 },
		},
		{
			// 9 Mbps × 1.2.
			name: "the producer's uplink, live on demand", ceilings: []int64{4_000_000, 4_000_000}, live: api.LivePolicy_LIVE_POLICY_ON_DEMAND, uplink: 10_000_000,
			want: api.RaiseBlocked_builder{Limit: api.RaiseLimit_RAISE_LIMIT_UPLINK, Required: 10_800_000, Available: 10_000_000}.Build(),
			lift: func(*starvationFixture) int64 { return 10_800_000 },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.set.AutoRaise = true
			f := newStarvationFixture(t, &c.set, c.live)
			var srcs []*api.Source
			for i, v := range c.ceilings {
				srcs = append(srcs, f.source("cam"+string(rune('a'+i)), int32(i+1), v))
			}
			src := srcs[0]
			version := f.version()

			since := f.now
			sg := f.starve(src, vbr, c.uplink)
			require.False(t, sg.GetApplied())
			require.Contains(t, sg.GetReason(), "held")
			held := f.get(src).GetStarvation().GetBlocked()
			require.Equal(t, c.want.GetLimit(), held.GetLimit())
			require.Equal(t, c.want.GetRequired(), held.GetRequired())
			require.Equal(t, c.want.GetAvailable(), held.GetAvailable())
			require.True(t, held.GetDateSince().AsTime().Equal(since))
			require.Nil(t, f.get(src).GetStarvation().GetDateRaised())
			require.Equal(t, version, f.version())
			require.Equal(t, 1, f.logs.count("auto_raise held"))

			// Held still: the CP warned when it was first held, and the
			// console says since when.
			f.now = f.now.Add(30 * time.Second)
			require.False(t, f.beat(c.uplink, report(src, vbr, 30, 0, 0))[0].GetApplied())
			require.True(t, f.get(src).GetStarvation().GetBlocked().GetDateSince().AsTime().Equal(since))
			require.Equal(t, 1, f.logs.count("auto_raise held"))

			if c.lift == nil {
				return
			}
			uplink := c.lift(f)
			f.now = f.now.Add(30 * time.Second)
			require.True(t, f.beat(uplink, report(src, vbr, 30, 0, 0))[0].GetApplied(), "inside the limit now")
			st := f.get(src).GetStarvation()
			require.Nil(t, st.GetBlocked())
			require.Equal(t, int64(5_000_000), st.GetRaisedMaxBitrate())
			require.Equal(t, version+1, f.version())
		})
	}
}

// Two raises in one heartbeat count each other against the set's total.
func TestAutoRaiseCountsRaisesOfTheSameHeartbeat(t *testing.T) {
	f := newStarvationFixture(t, &api.SetAddRequest_builder{AutoRaise: true, MaxBitrateTotal: 9_000_000}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	a := f.source("a", 1, 4_000_000)
	b := f.source("b", 2, 4_000_000)
	out := f.beat(0, report(a, vbr, 3600, 180, 50), report(b, vbr, 3600, 180, 50))
	require.Len(t, out, 2)
	require.True(t, out[0].GetApplied())
	require.False(t, out[1].GetApplied(), "5 + 5 Mbps is over the set's 9")
	require.Equal(t, api.RaiseLimit_RAISE_LIMIT_SET_BITRATE, f.get(b).GetStarvation().GetBlocked().GetLimit())
	require.Equal(t, int64(10_000_000), f.get(b).GetStarvation().GetBlocked().GetRequired())
}

// A CBR source sits at its ceiling by definition, a copied stream's rate
// is the camera's, and a producer that does not say is not guessed at:
// none is raised this way (§38.5), though the suggestion stands. Neither is
// a source of a set without auto_raise, whose suggestion still says which
// limit a person raising it would meet.
func TestAutoRaiseOnlyRaisesVBR(t *testing.T) {
	for _, rc := range []api.RateControl{api.RateControl_RATE_CONTROL_CBR, api.RateControl_RATE_CONTROL_COPY, api.RateControl_RATE_CONTROL_UNSPECIFIED} {
		t.Run(rc.String(), func(t *testing.T) {
			f := newStarvationFixture(t, &api.SetAddRequest_builder{AutoRaise: true}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
			door := f.source("door", 1, 4_000_000)
			version := f.version()
			sg := f.starve(door, rc, 0)
			require.False(t, sg.GetApplied())
			require.Equal(t, int64(5_000_000), sg.GetSuggested())
			st := f.get(door).GetStarvation()
			require.True(t, st.GetStarved())
			require.Equal(t, int64(5_000_000), st.GetSuggestedMaxBitrate())
			require.Nil(t, st.GetDateRaised())
			require.Equal(t, version, f.version())
		})
	}
	t.Run("suggest only", func(t *testing.T) {
		f := newStarvationFixture(t, &api.SetAddRequest_builder{MaxBitrateTotal: 4_500_000}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
		door := f.source("door", 1, 4_000_000)
		require.False(t, f.starve(door, vbr, 0).GetApplied())
		st := f.get(door).GetStarvation()
		require.Nil(t, st.GetDateRaised())
		require.Equal(t, api.RaiseLimit_RAISE_LIMIT_SET_BITRATE, st.GetBlocked().GetLimit())
		require.Zero(t, f.logs.count("auto_raise held"), "only a set that raises by itself warns")
	})
}

// A day's counts start again a day after they began, and the last raise
// is kept through it.
func TestStarvationDayEnds(t *testing.T) {
	f := newStarvationFixture(t, &api.SetAddRequest_builder{AutoRaise: true}, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	door := f.source("door", 1, 4_000_000)
	require.True(t, f.starve(door, vbr, 0).GetApplied())
	f.now = f.now.Add(time.Hour)
	f.beat(0, report(door, vbr, 3600, 180, 40))
	require.Equal(t, int64(40), f.get(door).GetStarvation().GetEpisodes())

	f.now = f.now.Add(24 * time.Hour)
	require.Empty(t, f.beat(0, report(door, vbr, 30, 3, 1)), "40 episodes yesterday and one today is not 50 in a day")
	st := f.get(door).GetStarvation()
	require.Equal(t, int64(1), st.GetEpisodes())
	require.True(t, st.GetDateReset().AsTime().Equal(f.now))
	require.Equal(t, int64(5_000_000), st.GetRaisedMaxBitrate())
}

// The producer's admission rule, shared (§38.5).
func TestUplinkNeed(t *testing.T) {
	require.Equal(t, int64(12_000_000), UplinkNeed(10_000_000, false))
	require.Equal(t, int64(22_000_000), UplinkNeed(10_000_000, true))
	require.Equal(t, int64(2), UplinkNeed(1, false), "rounded up: a need is never under-counted")
}
