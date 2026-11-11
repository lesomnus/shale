package shale

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/provision"
	"github.com/pion/webrtc/v4"
)

func checkLive(ctx context.Context, client *http.Client, s *api.LiveSource) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if client == nil {
		client = http.DefaultClient
	}
	// Do not forward a view token to a redirect chosen by an HTTP peer.
	clone := *client
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client = &clone
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer pc.Close()
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		return err
	}
	video := make(chan struct{})
	var once sync.Once
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			return
		}
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			if len(pkt.Payload) > 0 {
				once.Do(func() { close(video) })
				return
			}
		}
	})
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gathered:
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.GetWhepUrl(), bytes.NewBufferString(pc.LocalDescription().SDP))
	if err != nil {
		return provision.ErrNotReady
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+s.GetViewToken())
	resp, err := client.Do(req)
	if err != nil {
		return provision.ErrNotReady
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusCreated {
		return provision.ErrNotReady
	}
	base, err := url.Parse(s.GetWhepUrl())
	if err != nil {
		return provision.ErrNotReady
	}
	if resp.Header.Get("Location") == "" {
		return provision.ErrNotReady
	}
	location, err := base.Parse(resp.Header.Get("Location"))
	if err != nil || location.Host != base.Host || location.Scheme != base.Scheme {
		return provision.ErrNotReady
	}
	defer func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		r, err := http.NewRequestWithContext(c, http.MethodDelete, location.String(), nil)
		if err != nil {
			return
		}
		r.Header.Set("Authorization", token.Scheme+" "+s.GetViewToken())
		if resp, err := client.Do(r); err == nil {
			resp.Body.Close()
		}
	}()
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}); err != nil {
		return provision.ErrNotReady
	}
	select {
	case <-ctx.Done():
		return provision.ErrNotReady
	case <-video:
		return nil
	}
}
