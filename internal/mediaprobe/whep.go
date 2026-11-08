package mediaprobe

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lesomnus/shale/internal/token"
	"github.com/pion/webrtc/v4"
)

func capture(ctx context.Context, o Options, endpoint Endpoint) (result *captured, resultErr error) {
	reading, stop := context.WithCancel(ctx)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		stop()
		return nil, ErrNegotiation
	}
	var session string
	tracks := make(chan *webrtc.TrackRemote, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			select {
			case tracks <- track:
			default:
			}
		}
	})
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		stop()
		pc.Close()
		return nil, ErrNegotiation
	}
	type outcome struct {
		media *captured
		err   error
	}
	delivered := make(chan outcome, 1)
	done := make(chan struct{})
	go func() { defer close(done); media, err := receive(reading, o, tracks); delivered <- outcome{media, err} }()
	defer func() {
		stop()
		pc.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			result = nil
			if resultErr == nil {
				resultErr = ErrMedia
			}
		}
		if session != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(cleanup, http.MethodDelete, session, nil)
			if err == nil {
				req.Header.Set("Authorization", token.Scheme+" "+endpoint.Token)
				if resp, err := o.HTTPClient.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}
	}()
	session, err = negotiate(ctx, o.HTTPClient, pc, endpoint)
	if err != nil {
		return nil, err
	}
	select {
	case answer := <-delivered:
		return answer.media, answer.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func receive(ctx context.Context, o Options, tracks <-chan *webrtc.TrackRemote) (*captured, error) {
	var track *webrtc.TrackRemote
	select {
	case track = <-tracks:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeH264) || track.Codec().ClockRate != 90000 {
		return nil, ErrMedia
	}
	receiver := newReceiver(o)
	for ctx.Err() == nil {
		track.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		packet, _, err := track.ReadRTP()
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrMedia
		}
		result, err := receiver.push(packet, time.Now())
		if result != nil || err != nil {
			return result, err
		}
	}
	return nil, ctx.Err()
}

func negotiate(ctx context.Context, client *http.Client, pc *webrtc.PeerConnection, endpoint Endpoint) (string, error) {
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return "", ErrNegotiation
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if pc.SetLocalDescription(offer) != nil {
		return "", ErrNegotiation
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, strings.NewReader(pc.LocalDescription().SDP))
	if err != nil {
		return "", ErrEndpoint
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+endpoint.Token)
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrNegotiation
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", ErrNegotiation
	}
	base, _ := url.Parse(endpoint.URL)
	location, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		return "", ErrNegotiation
	}
	resolved := base.ResolveReference(location)
	if resolved.Scheme != base.Scheme || resolved.Host != base.Host || resolved.User != nil || resolved.Fragment != "" {
		return "", ErrNegotiation
	}
	session := resolved.String()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(answer) > 1<<20 {
		if ctx.Err() != nil {
			return session, ctx.Err()
		}
		return session, ErrNegotiation
	}
	if pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}) != nil {
		return session, ErrNegotiation
	}
	return session, nil
}
