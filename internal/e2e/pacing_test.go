package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// TestLivePacing is what a browser's jitter buffer has to absorb (§39.4):
// a viewer watching a camera whose producer sends a fragment every 500 ms
// should get each frame about when its timestamp says, not a fragment's
// frames in one burst. The spread of arrival minus timestamp, over the
// frames after the first seconds, is the playout delay needed to never
// stall; a burst per fragment makes it about a fragment long, and a
// browser that starts with less stutters at the fragments' pace.
func TestLivePacing(t *testing.T) {
	c := start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")
	_, live := liveSource(t, ctx, c, admin, "av.mp4")

	frames := watchArrivals(t, live, 12*time.Second)
	require.Greater(t, len(frames), 200, "video arrived")

	// From 3 s on: the start, the group of pictures handed over at once so
	// the picture comes up, is not what playback stalls on.
	t0 := frames[0].at
	var offsets []float64
	for _, f := range frames {
		if f.at.Sub(t0) < 3*time.Second {
			continue
		}
		offsets = append(offsets, f.at.Sub(t0).Seconds()-float64(f.ts-frames[0].ts)/90000)
	}
	sort.Float64s(offsets)
	spread := offsets[len(offsets)-1] - offsets[0]
	p99 := offsets[len(offsets)*99/100] - offsets[0]
	t.Logf("frames %d; arrival-minus-timestamp spread %.0f ms (p99 %.0f ms)", len(offsets), spread*1000, p99*1000)
	require.Less(t, p99, 0.150, "frames arrive about when their timestamps say, not a fragment at a time")
}

type arrival struct {
	at time.Time
	ts uint32
}

// watchArrivals is a WHEP viewer that notes when each video frame's first
// packet arrived, for a while.
func watchArrivals(t *testing.T, live *api.LiveSource, d time.Duration) []arrival {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	got := make(chan arrival, 4096)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			for {
				if _, _, err := track.ReadRTP(); err != nil {
					return
				}
			}
		}
		var last uint32
		seen := false
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			if seen && pkt.Timestamp == last {
				continue
			}
			seen, last = true, pkt.Timestamp
			select {
			case got <- arrival{time.Now(), pkt.Timestamp}:
			default:
			}
		}
	})
	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(offer))
	<-gathered
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, live.GetWhepUrl(), bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+live.GetViewToken())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))
	require.NoError(t, pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}))

	var out []arrival
	end := time.After(d)
	for {
		select {
		case a := <-got:
			out = append(out, a)
		case <-end:
			return out
		}
	}
}
