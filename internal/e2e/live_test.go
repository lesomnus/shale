package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/z"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/token"
)

// TestLive is E6's slice: the relay in `serve all`, a producer assigned to
// it, `Live` handing a viewer a token and a WHEP URL, and a WebRTC viewer
// receiving the camera's H.264 from a keyframe on; when it leaves, the
// producer stops sending after relay_idle_stop.
func TestLive(t *testing.T) {
	sample := samplePath(t)
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	admin := c.dial("@acme/admin")
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "live",
	}.Build())
	require.NoError(t, err)

	// The relay in the same process is adopted by the time it heartbeats.
	ops := c.dialCluster("@cluster/ops")
	relays := api.NewRelayServiceClient(ops)
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
	claims, err := token.Parse(live.GetViewToken())
	require.NoError(t, err)
	require.Equal(t, api.TokenOp_TOKEN_OP_VIEW, claims.GetOp())
	require.Equal(t, live.GetRelayId(), claims.GetAud())

	// A WebRTC viewer: an offer to receive video, posted as WHEP.
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	packets := make(chan []byte, 4096)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			select {
			case packets <- pkt.Payload:
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
	session := resp.Header.Get("Location")
	require.NotEmpty(t, session)
	require.NoError(t, pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}))

	// Video arrives, and it starts decodable: the first packets carry a
	// parameter set or an IDR slice (a keyframe), never a P-frame.
	var first []byte
	n := 0
	deadline := time.After(20 * time.Second)
	for n < 200 {
		select {
		case pl := <-packets:
			if len(pl) == 0 {
				continue
			}
			if first == nil {
				first = pl
			}
			n++
		case <-deadline:
			t.Fatalf("only %d RTP packets arrived", n)
		}
	}
	nal := first[0] & 0x1f
	switch nal {
	case 28: // FU-A: the type is in the FU header
		nal = first[1] & 0x1f
	case 24: // STAP-A: a size, then the first NAL unit
		require.GreaterOrEqual(t, len(first), 4)
		nal = first[3] & 0x1f
	}
	require.Contains(t, []byte{5, 7, 8, 6}, nal, "the stream starts at a keyframe (NAL %d)", nal)

	// A wrong token is refused.
	req2, _ := http.NewRequest(http.MethodPost, live.GetWhepUrl(), bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	req2.Header.Set("Authorization", token.Scheme+" nope")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)

	// Leaving ends the session; the relay stops the producer after
	// relay_idle_stop, which the relay's heartbeat reflects.
	del, _ := http.NewRequest(http.MethodDelete, whepBase(live.GetWhepUrl())+session, nil)
	resp3, err := http.DefaultClient.Do(del)
	require.NoError(t, err)
	resp3.Body.Close()
	require.Equal(t, http.StatusNoContent, resp3.StatusCode)
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

// whepBase is the scheme and host of a WHEP URL, up to the path.
func whepBase(u string) string {
	i := bytes.Index([]byte(u), []byte("/whep/"))

	return u[:i]
}
