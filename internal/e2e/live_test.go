package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/token"
)

// TestLive is E6's slice: the relay in `serve all`, a producer assigned to
// it, `Live` handing a viewer a token and a WHEP URL, and a WebRTC viewer
// receiving the camera's H.264 from a keyframe on and the Opus it
// recorded; when it leaves, the producer stops sending after
// relay_idle_stop.
func TestLive(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	set, live := liveSource(t, ctx, c, admin, "av.mp4")
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))

	// On demand (§39.3): the producer attached under the default, `always`,
	// so its one source is active with nobody watching; the patch reaches
	// it with the next heartbeat's assignment and it attaches again.
	setLive(t, ctx, admin, set, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	awaitActive(t, ctx, relays, 0, "on demand: no viewer, no active source")

	claims, err := token.Parse(live.GetViewToken())
	require.NoError(t, err)
	require.Equal(t, api.TokenOp_TOKEN_OP_VIEW, claims.GetOp())
	require.Equal(t, live.GetRelayId(), claims.GetAud())

	v := watch(t, live)
	defer v.close()

	// Video arrives, and it starts decodable: the first packets carry a
	// parameter set or an IDR slice (a keyframe), never a P-frame.
	first := v.video(t, 200, 20*time.Second)
	nal := first[0] & 0x1f
	switch nal {
	case 28: // FU-A: the type is in the FU header
		nal = first[1] & 0x1f
	case 24: // STAP-A: a size, then the first NAL unit
		require.GreaterOrEqual(t, len(first), 4)
		nal = first[3] & 0x1f
	}
	require.Contains(t, []byte{5, 7, 8, 6}, nal, "the stream starts at a keyframe (NAL %d)", nal)

	// And the Opus the producer recorded arrives as audio.
	v.audio(t, 50, 10*time.Second)

	// A wrong token is refused.
	req2, _ := http.NewRequest(http.MethodPost, live.GetWhepUrl(), bytes.NewReader([]byte(v.pc.LocalDescription().SDP)))
	req2.Header.Set("Authorization", token.Scheme+" nope")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)

	// A fresh token from `Live` renews the session in place (§39.4): it
	// names the same source and actor, and the video goes on.
	fresh, err := api.NewSetServiceClient(admin).Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	renew, _ := http.NewRequest(http.MethodPatch, v.base+v.session, nil)
	renew.Header.Set("Authorization", token.Scheme+" "+fresh.GetSources()[0].GetViewToken())
	resp3, err := http.DefaultClient.Do(renew)
	require.NoError(t, err)
	resp3.Body.Close()
	require.Equal(t, http.StatusNoContent, resp3.StatusCode)
	v.video(t, 50, 10*time.Second)

	// Leaving ends the session; the relay stops the producer after
	// relay_idle_stop, which the relay's heartbeat reflects.
	v.leave(t)
	awaitActive(t, ctx, relays, 0, "no viewer, no active source, the producer still attached")
}

// TestLiveAlways is the default policy (§39.3): the relay starts every
// source of a producer at Hello and stops none, so bytes flow with nobody
// watching; a patch to on demand stops them within a heartbeat, and back.
// A patch of what is the system's is refused.
func TestLiveAlways(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	set, _ := liveSource(t, ctx, c, admin, "av.mp4")
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))

	awaitActive(t, ctx, relays, 1, "always: the source is active with no viewer")
	before := relayStatus(t, ctx, relays).GetIngressBytes()
	require.Eventually(t, func() bool {
		return relayStatus(t, ctx, relays).GetIngressBytes() > before+100_000
	}, 20*time.Second, 300*time.Millisecond, "bytes keep coming with no viewer")

	setLive(t, ctx, admin, set, api.LivePolicy_LIVE_POLICY_ON_DEMAND)
	awaitActive(t, ctx, relays, 0, "on demand: nothing flows until a viewer")
	setLive(t, ctx, admin, set, api.LivePolicy_LIVE_POLICY_ALWAYS)
	awaitActive(t, ctx, relays, 1, "always again")

	p := producerOf(t, ctx, admin, set)
	_, err := api.NewProducerServiceClient(admin).Patch(ctx, api.ProducerPatchRequest_builder{
		Ref: api.ProducerRef_builder{Id: p.GetId()}.Build(), State: z.Ptr(api.HostState_HOST_STATE_PENDING), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.Error(t, err, "the state is the system's")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

// producerOf is the producer adopted for a set.
func producerOf(t *testing.T, ctx context.Context, admin *grpc.ClientConn, set *api.Set) *api.Producer {
	t.Helper()
	vs, err := api.NewProducerServiceClient(admin).List(ctx, api.ProducerListRequest_builder{Size: 50}.Build())
	require.NoError(t, err)
	for _, p := range vs.GetItems() {
		if string(p.GetSet().GetId()) == string(set.GetId()) {
			return p
		}
	}
	t.Fatalf("no producer for set %s", set.GetAlias())

	return nil
}

// setLive patches the live policy of a set's producer (§39.3).
func setLive(t *testing.T, ctx context.Context, admin *grpc.ClientConn, set *api.Set, v api.LivePolicy) {
	t.Helper()
	p := producerOf(t, ctx, admin, set)
	_, err := api.NewProducerServiceClient(admin).Patch(ctx, api.ProducerPatchRequest_builder{
		Ref: api.ProducerRef_builder{Id: p.GetId()}.Build(), Live: z.Ptr(v), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
}

// relayStatus is the one relay's last heartbeat.
func relayStatus(t *testing.T, ctx context.Context, relays api.RelayServiceClient) *api.RelayStatus {
	t.Helper()
	vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, vs.GetItems(), 1)

	return vs.GetItems()[0].GetStatus()
}

// awaitActive waits until the relay reports n active sources, no viewer,
// and the producer attached.
func awaitActive(t *testing.T, ctx context.Context, relays api.RelayServiceClient, n int32, why string) {
	t.Helper()
	require.Eventually(t, func() bool {
		st := relayStatus(t, ctx, relays)

		return st != nil && st.GetViewers() == 0 && st.GetActiveSources() == n && st.GetAttachedProducers() == 1
	}, 25*time.Second, 300*time.Millisecond, why)
}

// TestLiveTranscode is the live helper (§38.7): a camera recording AAC is
// watched with sound, since the producer runs ffmpeg on the tee and the
// relay gets Opus, while the recording keeps the camera's AAC.
func TestLiveTranscode(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", "aac.mp4"))
	require.NoError(t, err)
	set, live, p := liveProducer(t, ctx, c, admin, producer.SourceConfig{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"})

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 20*time.Second)
	v.audio(t, 50, 15*time.Second)
	require.Equal(t, int64(1), p.Stats().LiveTranscodes, "the helper runs while the camera is watched")
	v.leave(t)

	// The stored segment carries the camera's AAC, not Opus.
	init := committedInit(t, ctx, admin, set)
	require.Equal(t, []string{"mp4a"}, audioEntries(init), "the recording keeps the camera's AAC")
}

// TestLiveTwoTracks is the second audio track (§38.7): a source the
// producer encodes records AAC for the archive and Opus beside it, so it
// is watched with sound while no live helper runs, and its lamina carries
// both; the relay takes the Opus and never looks at the AAC.
func TestLiveTwoTracks(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set, live, p := liveProducer(t, ctx, c, admin, producer.DemoSources(1)[0])

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 20*time.Second)
	v.audio(t, 50, 15*time.Second)
	require.Zero(t, p.Stats().LiveTranscodes, "the Opus is the capture's own: no helper")
	v.leave(t)

	init := committedInit(t, ctx, admin, set)
	require.Equal(t, []string{"mp4a", "Opus"}, audioEntries(init), "AAC for the archive, Opus for live")
}

// smallLaminae activates an UploadPolicy of laminae of a few MB, so a
// segment commits while a test watches; before the set negotiates.
func smallLaminae(t *testing.T, ctx context.Context, c *cluster) {
	t.Helper()
	ops := c.dialCluster("@cluster/ops")
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 512 << 10, TargetLamina: 1 << 20, MaxLamina: 4 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewUploadPolicyServiceClient(ops).Activate(ctx, api.UploadPolicyActivateRequest_builder{
		Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
}

// committedInit waits for the set's one source to commit a lamina,
// fetches it, checks it plays on its own, and answers its init segment.
func committedInit(t *testing.T, ctx context.Context, admin *grpc.ClientConn, set *api.Set) *fmp4.Init {
	t.Helper()
	_, url := committedLamina(t, ctx, admin, set)
	init, _ := playsOnItsOwn(t, fetch(t, url))

	return init
}

// committedLamina waits for the set's one source to commit a lamina and
// answers the row and the URL Timeline hands out for it.
func committedLamina(t *testing.T, ctx context.Context, admin *grpc.ClientConn, set *api.Set) (*api.Lamina, string) {
	t.Helper()
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{
		Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 1)
	src := sources.GetItems()[0]
	laminae := api.NewLaminaServiceClient(admin)
	var committed *api.Lamina
	require.Eventually(t, func() bool {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build()},
			Size:    100,
		}.Build())
		if err != nil {
			return false
		}
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				committed = o
				return true
			}
		}

		return false
	}, 90*time.Second, 500*time.Millisecond, "a segment commits")
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
		From:   timestamppb.New(committed.GetDateStarted().AsTime().Add(-time.Second)),
		To:     timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	var url string
	for _, o := range tl.GetSources()[0].GetLaminae() {
		if string(o.GetLaminaId()) == string(committed.GetId()) {
			url = o.GetUrl()
		}
	}
	require.NotEmpty(t, url)

	return committed, url
}

// TestLiveRecent is the recent window (§39.4): with live always on, the
// relay keeps the last while of a source as it came and GET /recent hands
// it out as one TS, the tables first, from a keyframe, reaching back past
// the end of the last committed lamina, so nothing is unreadable between
// the store and now; a token for another source is refused.
func TestLiveRecent(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	smallLaminae(t, ctx, c)
	set, live, _ := liveProducer(t, ctx, c, admin, producer.DemoSources(1)[0])
	require.NotEmpty(t, live.GetRecentUrl())
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))
	awaitActive(t, ctx, relays, 1, "always: the source is fed with no viewer")

	// Once a lamina committed, the window reaches back past its end.
	committed, _ := committedLamina(t, ctx, admin, set)
	b, hdr := getRecent(t, live.GetRecentUrl(), live.GetViewToken(), "", http.StatusOK)
	start, err := time.Parse(time.RFC3339Nano, hdr.Get("Shale-Recent-Start"))
	require.NoError(t, err)
	ended := committed.GetDateEnded().AsTime()
	require.False(t, start.After(ended), "the window starts at %s, after the last lamina ended at %s", start, ended)
	require.Equal(t, "video/mp4", hdr.Get("Content-Type"))
	secs, err := strconv.ParseFloat(hdr.Get("Shale-Recent-Seconds"), 64)
	require.NoError(t, err)
	require.Greater(t, secs, 2.0)

	// The init segment first, a key fragment first, the fragments of a
	// window in a row, and the Opus track in it, as in the lamina.
	require.Equal(t, "ftyp", string(b[4:8]), "the init segment first")
	init, frags := laminaOf(t, b)
	require.NotEmpty(t, frags)
	require.True(t, frags[0].Key(init), "the window starts at a key fragment")
	frames := 0
	for i, f := range frags {
		if v := f.Video(init); v != nil {
			frames += len(v.Samples)
		}
		if i > 0 {
			require.Equal(t, frags[i-1].Seq+1, f.Seq, "nothing torn")
		}
	}
	require.GreaterOrEqual(t, frames, 60, "at least two seconds of frames")
	require.NotNil(t, init.Opus(), "the Opus track is in it, as in the lamina")

	// The last second only is less than the whole, and starts decodable too.
	tail, _ := getRecent(t, live.GetRecentUrl(), live.GetViewToken(), "1.5", http.StatusOK)
	require.Less(t, len(tail), len(b))
	init2, frags2 := laminaOf(t, tail)
	require.NotEmpty(t, frags2)
	require.True(t, frags2[0].Key(init2))

	// The heartbeat says what the windows hold and what bounds them.
	st := relayStatus(t, ctx, relays)
	require.Greater(t, st.GetRewindBytes(), int64(0))
	require.Equal(t, int64(1<<30), st.GetRewindBudget())

	// Refused: a bad token, and a token for another source.
	getRecent(t, live.GetRecentUrl(), "nope", "", http.StatusUnauthorized)
	other := whepBase(live.GetWhepUrl()) + "/recent/" + pdid.New(pdid.Domain(8)).String()
	getRecent(t, other, live.GetViewToken(), "", http.StatusForbidden)
}

// getRecent fetches a source's recent window with a view token and the
// `since` given, expecting a status.
func getRecent(t *testing.T, url, tok, since string, want int) ([]byte, http.Header) {
	t.Helper()
	if since != "" {
		url += "?since=" + since
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, want, resp.StatusCode, string(b[:min(len(b), 200)]))

	return b, resp.Header
}

// liveSource sets up one set with one producer playing a recording from
// internal/producer/testdata, adopted and assigned a relay, and answers
// what Live says about its source.
func liveSource(t *testing.T, ctx context.Context, c *cluster, admin *grpc.ClientConn, recording string) (*api.Set, *api.LiveSource) {
	t.Helper()
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", recording))
	require.NoError(t, err)
	set, live, _ := liveProducer(t, ctx, c, admin, producer.SourceConfig{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"})

	return set, live
}

// liveProducer sets up one set with one producer of the given source,
// adopted and assigned a relay, and answers what Live says about its
// source, and the producer.
func liveProducer(t *testing.T, ctx context.Context, c *cluster, admin *grpc.ClientConn, src producer.SourceConfig) (*api.Set, *api.LiveSource, *producer.Producer) {
	t.Helper()
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "live",
	}.Build())
	require.NoError(t, err)

	// The relay in the same process is adopted by the time it heartbeats.
	relays := api.NewRelayServiceClient(c.dialCluster(c.operator()))
	require.Eventually(t, func() bool {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			if r.GetState() == api.HostState_HOST_STATE_ADOPTED && r.GetDateSeen() != nil && r.GetWhepAddress() != "" {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the relay is adopted and heartbeats")

	p, err := producer.New(producer.Config{
		StateDir:          filepath.Join(t.TempDir(), "producer"),
		Cp:                "http://" + c.running.TenantAddr,
		Dev:               true,
		Sources:           []producer.SourceConfig{src},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		SegmentDuration:   8 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
	})
	require.NoError(t, err)
	go p.Run(ctx)

	producers := api.NewProducerServiceClient(admin)
	var pending *api.Producer
	require.Eventually(t, func() bool {
		vs, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 10}.Build())
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
	_, err = producers.Adopt(ctx, api.ProducerAdoptRequest_builder{
		Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)

	// Live: once the producer negotiated (and got its relay), every member
	// has a WHEP URL and a view token.
	sets := api.NewSetServiceClient(admin)
	var live *api.LiveSource
	require.Eventually(t, func() bool {
		resp, err := sets.Live(ctx, api.SetLiveRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
		if err != nil || len(resp.GetSources()) == 0 {
			return false
		}
		live = resp.GetSources()[0]

		return true
	}, 30*time.Second, 300*time.Millisecond, "Live answers once the producer is assigned a relay")
	require.NotEmpty(t, live.GetWhepUrl())
	require.NotEmpty(t, live.GetViewToken())

	return set, live, p
}

// viewer is one WHEP session: a WebRTC peer receiving video and audio.
type viewer struct {
	pc      *webrtc.PeerConnection
	base    string
	session string
	packets chan []byte
	sounds  chan int
}

// watch opens a WHEP session for a source with an offer to receive video
// and audio.
func watch(t *testing.T, live *api.LiveSource) *viewer {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	v := &viewer{pc: pc, base: whepBase(live.GetWhepUrl()), packets: make(chan []byte, 4096), sounds: make(chan int, 4096)}
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		isAudio := track.Kind() == webrtc.RTPCodecTypeAudio
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			if isAudio {
				select {
				case v.sounds <- len(pkt.Payload):
				default:
				}
				continue
			}
			select {
			case v.packets <- pkt.Payload:
			default:
			}
		}
	})
	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(offer))
	<-gathered

	req, err := http.NewRequest(http.MethodPost, live.GetWhepUrl(), bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+live.GetViewToken())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))
	v.session = resp.Header.Get("Location")
	require.NotEmpty(t, v.session)
	require.NoError(t, pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}))

	return v
}

// video waits for n video RTP packets and answers the first payload.
func (v *viewer) video(t *testing.T, n int, timeout time.Duration) []byte {
	t.Helper()
	var first []byte
	got := 0
	deadline := time.After(timeout)
	for got < n {
		select {
		case pl := <-v.packets:
			if len(pl) == 0 {
				continue
			}
			if first == nil {
				first = pl
			}
			got++
		case <-deadline:
			t.Fatalf("only %d video RTP packets arrived", got)
		}
	}

	return first
}

// audio waits for n audio RTP packets with a payload.
func (v *viewer) audio(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	heard := 0
	deadline := time.After(timeout)
	for heard < n {
		select {
		case size := <-v.sounds:
			if size > 0 {
				heard++
			}
		case <-deadline:
			t.Fatalf("only %d audio packets arrived", heard)
		}
	}
}

// leave ends the session as a viewer does.
func (v *viewer) leave(t *testing.T) {
	t.Helper()
	del, _ := http.NewRequest(http.MethodDelete, v.base+v.session, nil)
	resp, err := http.DefaultClient.Do(del)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func (v *viewer) close() { v.pc.Close() }

// whepBase is the scheme and host of a WHEP URL, up to the path.
func whepBase(u string) string {
	i := bytes.Index([]byte(u), []byte("/whep/"))

	return u[:i]
}
