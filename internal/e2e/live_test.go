package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/z"

	"github.com/lesomnus/shale/api"
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
	_, live := liveSource(t, ctx, c, admin, "av.ts")
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))

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

	// Leaving ends the session; the relay stops the producer after
	// relay_idle_stop, which the relay's heartbeat reflects.
	v.leave(t)
	require.Eventually(t, func() bool {
		vs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, r := range vs.GetItems() {
			st := r.GetStatus()
			if st != nil && st.GetViewers() == 0 && st.GetActiveSources() == 0 && st.GetAttachedProducers() == 1 {
				return true
			}
		}

		return false
	}, 20*time.Second, 300*time.Millisecond, "no viewer, no active source, the producer still attached")
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

	// Small objects, so a segment commits while the test watches: an
	// UploadPolicy of the test's own, activated before the set negotiates.
	ops := c.dialCluster("@cluster/ops")
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinObject: 512 << 10, TargetObject: 1 << 20, MaxObject: 4 << 20}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewUploadPolicyServiceClient(ops).Activate(ctx, api.UploadPolicyActivateRequest_builder{
		Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	set, live := liveSource(t, ctx, c, admin, "aac.ts")

	v := watch(t, live)
	defer v.close()
	v.video(t, 100, 20*time.Second)
	v.audio(t, 50, 15*time.Second)
	v.leave(t)

	// The stored segment carries the camera's AAC, not Opus.
	sources, err := api.NewSourceServiceClient(admin).List(ctx, api.SourceListRequest_builder{
		Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, sources.GetItems(), 1)
	src := sources.GetItems()[0]
	objects := api.NewObjectServiceClient(admin)
	var committed *api.Object
	require.Eventually(t, func() bool {
		vs, err := objects.List(ctx, api.ObjectListRequest_builder{
			Filters: []*api.ObjectFilter{api.ObjectFilter_builder{Source: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build()},
			Size:    100,
		}.Build())
		if err != nil {
			return false
		}
		for _, o := range vs.GetItems() {
			if o.GetState() == api.ObjectState_OBJECT_STATE_COMMITTED {
				committed = o
				return true
			}
		}

		return false
	}, 90*time.Second, 500*time.Millisecond, "a segment commits")
	tl, err := objects.Timeline(ctx, api.ObjectTimelineRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
		From:   timestamppb.New(committed.GetDateStarted().AsTime().Add(-time.Second)),
		To:     timestamppb.New(time.Now().Add(time.Minute)),
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	var url string
	for _, o := range tl.GetSources()[0].GetObjects() {
		if string(o.GetObjectId()) == string(committed.GetId()) {
			url = o.GetUrl()
		}
	}
	require.NotEmpty(t, url)
	b := fetch(t, url)
	r := producer.NewReader(bytesReader(b))
	var pk producer.Packet
	for r.Next(&pk) == nil {
	}
	st := r.Streams()
	require.Equal(t, []byte{0x0f}, st.AudioTypes, "the recording keeps the camera's AAC")
	require.False(t, st.AudioOpus)
}

// liveSource sets up one set with one producer playing a recording from
// internal/producer/testdata, adopted and assigned a relay, and answers
// what Live says about its source.
func liveSource(t *testing.T, ctx context.Context, c *cluster, admin *grpc.ClientConn, recording string) (*api.Set, *api.LiveSource) {
	t.Helper()
	sample, err := filepath.Abs(filepath.Join("..", "producer", "testdata", recording))
	require.NoError(t, err)

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "live",
	}.Build())
	require.NoError(t, err)

	// The relay in the same process is adopted by the time it heartbeats.
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))
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
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
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

	return set, live
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
