package e2e_test

import (
	"context"
	"log/slog"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/relay"
	"github.com/lesomnus/shale/internal/satellite"
)

// The live fan-out of §41.8: a producer whose live uploads go through a
// satellite hands it the live of its sources, and the satellite feeds the
// relay from the upload, so the producer's uplink carries each byte once.

// satelliteLive runs a producer of a recording from internal/producer/
// testdata for a new set, uploading `via`, live on demand from its first
// attachment, and adopts it; it answers once Live names the source. edit
// changes the producer's configuration.
func (c *cluster) satelliteLive(ctx context.Context, alias, recording string, via producer.Via, edit ...func(*producer.Config)) (*producer.Producer, *api.Set) {
	c.t.Helper()
	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias,
	}.Build())
	require.NoError(c.t, err)
	relays := api.NewRelayServiceClient(c.dialCluster(c.operator()))
	require.Eventually(c.t, func() bool {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			if r.GetState() == api.HostState_HOST_STATE_ADOPTED && r.GetStatus().GetSatelliteFeeders() {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "the relay is adopted and says it takes satellites")

	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", recording))
	require.NoError(c.t, err)
	cfg := producer.Config{
		StateDir:          filepath.Join(c.t.TempDir(), "producer-"+alias),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{{Alias: alias, Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		SegmentDuration:   8 * time.Second,
		HeartbeatInterval: time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Upload:            producer.UploadConfig{Via: via, ResumeTimeout: 20 * time.Second},
	}
	for _, e := range edit {
		e(&cfg)
	}
	p, err := producer.New(cfg)
	require.NoError(c.t, err)
	go p.Run(ctx)

	// On demand from the first attachment: nothing goes to the relay
	// before somebody watches.
	producers := api.NewProducerServiceClient(admin)
	var pending *api.Producer
	require.Eventually(c.t, func() bool {
		vs, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 50}.Build())
		if err != nil {
			return false
		}
		for _, v := range vs.GetItems() {
			if v.GetState() == api.HostState_HOST_STATE_PENDING {
				pending = v
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "the producer joins")
	untilOK(c.t, func() (*api.Producer, error) {
		return producers.Patch(ctx, api.ProducerPatchRequest_builder{
			Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Live: z.Ptr(api.LivePolicy_LIVE_POLICY_ON_DEMAND), DateUpdatedForce: z.Ptr(true),
		}.Build())
	})
	untilOK(c.t, func() (*api.Producer, error) {
		return producers.Adopt(ctx, api.ProducerAdoptRequest_builder{
			Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		}.Build())
	})

	return p, set
}

// liveOf is what Live answers for the set's one source.
func (c *cluster) liveOf(ctx context.Context, set *api.Set) *api.LiveSource {
	c.t.Helper()
	sets := api.NewSetServiceClient(c.dial("@acme/admin"))
	var live *api.LiveSource
	require.Eventually(c.t, func() bool {
		resp, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		if err != nil || len(resp.GetSources()) == 0 {
			return false
		}
		live = resp.GetSources()[0]
		return true
	}, 30*time.Second, 300*time.Millisecond, "Live answers once the producer is assigned a relay")

	return live
}

// fedBy waits until the satellite feeds the relay the producer's source.
func fedBy(t *testing.T, sat *satellite.Satellite, p *producer.Producer, alias string) {
	t.Helper()
	require.Eventually(t, func() bool {
		n, _, _ := sat.Live()
		return n == 1 && len(p.LiveHanded()) == 1 && p.LiveHanded()[0] == alias
	}, 60*time.Second, 100*time.Millisecond, "the producer hands the satellite its live, which feeds the relay")
}

// arrivals is when a viewer's video packets came.
type arrivals struct {
	mu sync.Mutex
	at []time.Time
}

// timeVideo reads a viewer's video from now on, noting when each packet came.
func timeVideo(v *viewer) *arrivals {
	a := &arrivals{}
	go func() {
		for range v.packets {
			a.mu.Lock()
			a.at = append(a.at, time.Now())
			a.mu.Unlock()
		}
	}()

	return a
}

func (a *arrivals) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.at)
}

// gap is the longest wait between two packets that came in [from, to].
func (a *arrivals) gap(from, to time.Time) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	var longest time.Duration
	var last time.Time
	for _, at := range a.at {
		if at.Before(from) || at.After(to) {
			continue
		}
		if !last.IsZero() {
			longest = max(longest, at.Sub(last))
		}
		last = at
	}

	return longest
}

// keyInterval is the recording's: a keyframe every 2 s (av.mp4, aac.mp4).
const keyInterval = 2 * time.Second

// TestSatelliteLive is §41.8: a producer that uploads through a satellite,
// live, on demand, hands the satellite the live of its source, and a viewer
// who attaches through the relay gets the camera's video from the
// satellite, which reads it out of the upload. The producer's own tee sends
// nothing for it.
func TestSatelliteLive(t *testing.T) {
	c := start(t, noBuiltInSatellite)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sat, _ := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))
	p, set := c.satelliteLive(ctx, "door", "av.mp4", producer.ViaSatellite)
	fedBy(t, sat, p, "door")
	live := c.liveOf(ctx, set)

	v := watch(t, live)
	defer v.close()
	v.video(t, 200, 20*time.Second)
	v.audio(t, 50, 10*time.Second)
	_, sent, _ := sat.Live()
	require.Positive(t, sent, "the satellite fed the relay")
	require.Zero(t, p.LiveSent("door"), "the producer's own tee sent nothing")
	require.Zero(t, p.Stats().LiveSent)

	// What the CP and the satellite say.
	producers := api.NewProducerServiceClient(c.dial("@acme/admin"))
	rows, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 50}.Build())
	require.NoError(t, err)
	var report *api.UploadReport
	for _, row := range rows.GetItems() {
		if string(row.GetSet().GetId()) == string(set.GetId()) {
			report = row.GetStatus().GetUpload()
		}
	}
	require.Len(t, report.GetLiveSources(), 1, "the heartbeat says which source the satellite feeds")
	require.Equal(t, "auto", report.GetLiveFeed())
	sats := api.NewSatelliteServiceClient(c.dialCluster(c.operator()))
	require.Eventually(t, func() bool {
		row, err := sats.Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: sat.Id().Bytes()}.Build()}.Build())
		return err == nil && row.GetStatus().GetLiveSources() == 1 && row.GetStatus().GetLiveBytes() > 0
	}, 10*time.Second, 200*time.Millisecond, "the satellite's heartbeat reports its live")

	// Leaving stops the satellite as it stopped the producer.
	v.leave(t)
	relays := api.NewRelayServiceClient(c.dialCluster(c.operator()))
	require.Eventually(t, func() bool {
		st := relayStatus(t, ctx, relays)
		return st.GetViewers() == 0 && st.GetActiveSources() == 0
	}, 25*time.Second, 300*time.Millisecond, "no viewer, nothing active")
	_, before, _ := sat.Live()
	time.Sleep(2 * time.Second)
	_, after, _ := sat.Live()
	require.Equal(t, before, after, "nothing sent once stopped")
	require.Zero(t, p.LiveSent("door"))
}

// TestSatelliteLiveAcrossLaminae is §41.8 at lamina boundaries: each
// lamina is its own upload, and the satellite's feed goes on from one to
// the next with nothing missing, so a viewer watches across them as across
// any keyframe.
func TestSatelliteLiveAcrossLaminae(t *testing.T) {
	c := start(t, noBuiltInSatellite)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	sat, _ := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))
	p, set := c.satelliteLive(ctx, "yard", "av.mp4", producer.ViaSatellite)
	fedBy(t, sat, p, "yard")
	live := c.liveOf(ctx, set)

	v := watch(t, live)
	defer v.close()
	v.video(t, 50, 20*time.Second)
	a := timeVideo(v)
	began := time.Now()
	stored := p.Stats().Stored
	require.Eventually(t, func() bool { return p.Stats().Stored >= stored+3 }, 90*time.Second, 200*time.Millisecond,
		"three laminae end while the viewer watches: %+v", p.Stats())
	time.Sleep(time.Second)
	ended := time.Now()

	gap := a.gap(began, ended)
	t.Logf("%d video packets over %s, the longest wait %s", a.count(), ended.Sub(began).Round(time.Millisecond), gap)
	require.Less(t, gap, keyInterval, "no lamina boundary costs the viewer a keyframe")
	require.Zero(t, p.LiveSent("yard"), "the satellite fed it all along")
	n, _, _ := sat.Live()
	require.Equal(t, 1, n)
	require.Zero(t, p.Stats().Lost)
}

// TestSatelliteLiveFallsBack is §41.8's handoff: the satellite feeds while
// the producer's tee waits; when the satellite goes, `auto` uploads go to
// the node directly and the relay takes the source back to the producer's
// own stream at once, which feeds it from its next keyframe: a viewer waits
// no longer than a keyframe interval, and never gets two feeders' video.
func TestSatelliteLiveFallsBack(t *testing.T) {
	c := start(t, noBuiltInSatellite, func(c *cmd.Config) { c.Control.SatelliteDownAfter = 10 * time.Minute })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.smallLaminae(ctx)
	sat, stop := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))
	p, set := c.satelliteLive(ctx, "porch", "av.mp4", producer.ViaAuto)
	fedBy(t, sat, p, "porch")
	live := c.liveOf(ctx, set)

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 20*time.Second)
	a := timeVideo(v)
	time.Sleep(3 * time.Second)
	require.Zero(t, p.LiveSent("porch"), "one feeder: the satellite")
	_, fed, _ := sat.Live()
	require.Positive(t, fed)

	stopped := time.Now()
	stop()
	require.Eventually(t, func() bool { return p.LiveSent("porch") > 0 }, 10*time.Second, 50*time.Millisecond, "the producer's tee feeds again")
	_, after, _ := sat.Live()
	time.Sleep(4 * time.Second)
	gap := a.gap(stopped.Add(-time.Second), time.Now())
	t.Logf("the viewer's longest wait across the handoff: %s", gap)
	require.LessOrEqual(t, gap, keyInterval+500*time.Millisecond, "no longer than a keyframe interval, and the handoff's round trip")
	_, still, _ := sat.Live()
	require.Equal(t, after, still, "the satellite that went sends nothing more")

	// The uploads went directly, and the producer says it hands nothing
	// over now.
	before := p.Stats().Stored
	require.Eventually(t, func() bool { return p.Stats().Stored > before }, 60*time.Second, 300*time.Millisecond)
	require.Eventually(t, func() bool { return len(p.LiveHanded()) == 0 }, 30*time.Second, 200*time.Millisecond)
	require.Zero(t, p.Stats().Lost)
	seen := a.count()
	require.Eventually(t, func() bool { return a.count() > seen+50 }, 10*time.Second, 100*time.Millisecond, "the viewer watches on")
}

// TestSatelliteLiveHelperStaysOnProducer is §41.8's limit: a camera whose
// audio is not Opus goes through the live helper (§38.7), which a satellite
// does not run, so its live stays on the producer's tee while its uploads
// go through the satellite.
func TestSatelliteLiveHelperStaysOnProducer(t *testing.T) {
	c := start(t, noBuiltInSatellite)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sat, _ := c.startSatellite("sat", filepath.Join(t.TempDir(), "sat"))
	p, set := c.satelliteLive(ctx, "aac", "aac.mp4", producer.ViaSatellite)
	require.Eventually(t, func() bool { return sat.Forwarded() > 0 }, 60*time.Second, 200*time.Millisecond, "uploading through the satellite")
	live := c.liveOf(ctx, set)

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 20*time.Second)
	require.Positive(t, p.LiveSent("aac"), "the producer's tee feeds it")
	require.Empty(t, p.LiveHanded(), "nothing handed over")
	n, sent, _ := sat.Live()
	require.Zero(t, n)
	require.Zero(t, sent)
	if _, err := exec.LookPath("ffmpeg"); err == nil {
		require.Equal(t, int64(1), p.Stats().LiveTranscodes, "through the helper")
		v.audio(t, 50, 15*time.Second)
	}
}

// TestSatelliteLiveOverTLS is §41.8 as a deployment runs it: the relay
// takes the satellite's feed token only from the satellite its client
// certificate names (§39.7), and the producer's own stream, started under
// `always`, is told to stop once the satellite feeds its source.
func TestSatelliteLiveOverTLS(t *testing.T) {
	c := startTLS(t)
	c.startNode("node", filepath.Join(t.TempDir(), "state"), filepath.Join(t.TempDir(), "sink"), "", false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fp := pki.Fingerprint(c.cp.CA.Cert)
	run := func(name string, ready <-chan struct{}, fn func(context.Context) error) {
		done := make(chan error, 1)
		go func() { done <- fn(ctx) }()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("%s: %v", name, err)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not come up", name)
		}
	}
	r, err := relay.New(relay.Config{
		StateDir: filepath.Join(t.TempDir(), "relay"), Cp: "https://" + c.cluster, CaHash: fp, HardwareId: "test-relay",
		IngestAddr: "127.0.0.1:0", WhepAddr: "127.0.0.1:0", HeartbeatInterval: time.Second, Log: slog.Default().With("relay", "tls"),
	})
	require.NoError(t, err)
	run("relay", r.Ready, r.Run)
	sat, err := satellite.New(satellite.Config{
		StateDir: filepath.Join(t.TempDir(), "sat"), Cp: "https://" + c.cluster, CaHash: fp, HardwareId: "test-sat",
		Addr: "127.0.0.1:0", HeartbeatInterval: time.Second, Log: slog.Default().With("satellite", "tls"),
	})
	require.NoError(t, err)
	run("satellite", sat.Ready, sat.Run)

	admin := c.dial(c.tenant, "@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "tls"}.Build())
	require.NoError(t, err)
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "av.mp4"))
	require.NoError(t, err)
	p, err := producer.New(producer.Config{
		StateDir: t.TempDir(), Cp: "https://" + c.tenant, CaHash: fp,
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
		Upload:            producer.UploadConfig{Via: producer.ViaSatellite},
	})
	require.NoError(t, err)
	go p.Run(ctx)
	adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)

	fedBy(t, sat, p, "door")
	require.Eventually(t, func() bool { _, sent, _ := sat.Live(); return sent > 0 }, 30*time.Second, 100*time.Millisecond,
		"under `always` the relay started the satellite: it took the feed token over mTLS")
	// The producer's own stream was told to stop: what it sent stays.
	time.Sleep(time.Second)
	before := p.LiveSent("door")
	time.Sleep(3 * time.Second)
	require.Equal(t, before, p.LiveSent("door"), "one feeder at a time")
	_, a, _ := sat.Live()
	time.Sleep(time.Second)
	_, b, _ := sat.Live()
	require.Greater(t, b, a, "the satellite feeds on")
}
