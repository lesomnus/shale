package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"
	"github.com/pion/webrtc/v4"

	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/token"
)

// `shale live <set>` is the test client of §39.4 without a browser: it asks
// Live, opens a WHEP session per source as a WebRTC viewer, and reports
// what arrived: packets, bytes, keyframes, the first NAL unit.
func NewCmdLive(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "live",
		Brief: "watch a set through its relay for a while and report what arrived",
		Flags: flg.Flags{
			&flg.Duration{Name: "for", Brief: "how long to watch (5s)"},
		},
		Args: arg.Args{
			&arg.String{Name: "SET", Brief: "the set, @tenant/alias"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			d, ok := flg.Find[time.Duration](self, "for")
			if !ok || d <= 0 {
				d = 5 * time.Second
			}
			setArg, _ := arg.Get[string](self, "SET")
			ref, err := pdcmd.RefParser{}.Parse(setArg)
			if err != nil {
				return err
			}
			setRef := &api.SetRef{}
			if err := ref.Fill(setRef.ProtoReflect()); err != nil {
				return err
			}
			conn, done, err := (&connector{c: c}).Connect(ctx)
			if err != nil {
				return err
			}
			defer done()
			resp, err := api.NewSetServiceClient(conn).Live(ctx, api.SetLiveRequest_builder{Ref: setRef}.Build())
			if err != nil {
				return err
			}
			if len(resp.GetSources()) == 0 {
				return errors.New("the set has no sources")
			}

			client, err := liveHTTPClient(c)
			if err != nil {
				return err
			}
			var wg sync.WaitGroup
			results := make([]liveResult, len(resp.GetSources()))
			for i, ls := range resp.GetSources() {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i] = watch(ctx, client, ls, d)
				}()
			}
			wg.Wait()
			fmt.Fprintf(self, "%-10s %-8s %10s %12s %6s %6s %-6s %s\n", "SOURCE", "PACKETS", "BYTES", "RATE", "KEYS", "AUDIO", "FIRST", "STATUS")
			for i, r := range results {
				id := fmt.Sprintf("%x", resp.GetSources()[i].GetSourceId()[:4])
				rate := fmt.Sprintf("%.2f Mbps", float64(r.bytes)*8/d.Seconds()/1e6)
				fmt.Fprintf(self, "%-10s %-8d %10d %12s %6d %6d %-6s %s\n", id, r.packets, r.bytes, rate, r.keys, r.audio, r.first, r.status)
			}

			return nil
		}),
	}
}

type liveResult struct {
	packets, bytes, keys int
	audio                int
	first                string
	status               string
}

// liveHTTPClient trusts the CP's CA, which signs the relay's certificate.
func liveHTTPClient(c *cmd.Config) (*http.Client, error) {
	pool, err := caPool(c)
	if err != nil {
		// Plaintext relays (development) need no pool.
		return http.DefaultClient, nil
	}

	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}, nil
}

// watch is one WHEP session for `d`.
func watch(ctx context.Context, client *http.Client, ls *api.LiveSource, d time.Duration) liveResult {
	res := liveResult{status: "ok"}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		res.status = err.Error()
		return res
	}
	defer pc.Close()
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		if _, err := pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			res.status = err.Error()
			return res
		}
	}
	var mu sync.Mutex
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		audio := track.Kind() == webrtc.RTPCodecTypeAudio
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			mu.Lock()
			if audio {
				res.audio++
				mu.Unlock()
				continue
			}
			res.packets++
			res.bytes += len(pkt.Payload)
			if t := nalType(pkt.Payload); t == 5 {
				res.keys++
			}
			if res.first == "" && len(pkt.Payload) > 0 {
				res.first = nalName(nalType(pkt.Payload))
			}
			mu.Unlock()
		}
	})
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		res.status = err.Error()
		return res
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		res.status = err.Error()
		return res
	}
	select {
	case <-gathered:
	case <-time.After(3 * time.Second):
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ls.GetWhepUrl(), bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+ls.GetViewToken())
	resp, err := client.Do(req)
	if err != nil {
		res.status = err.Error()
		return res
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		res.status = fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		return res
	}
	session := resp.Header.Get("Location")
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}); err != nil {
		res.status = err.Error()
		return res
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	base := ls.GetWhepUrl()[:strings.Index(ls.GetWhepUrl(), "/whep/")]
	if del, err := http.NewRequest(http.MethodDelete, base+session, nil); err == nil {
		if r, err := client.Do(del); err == nil {
			r.Body.Close()
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if res.packets == 0 {
		res.status = "no video arrived"
	}

	return res
}

// nalType is the H.264 NAL unit type an RTP payload starts with, looking
// through FU-A and STAP-A.
func nalType(pl []byte) byte {
	if len(pl) == 0 {
		return 0
	}
	t := pl[0] & 0x1f
	switch t {
	case 28:
		if len(pl) > 1 && pl[1]&0x80 != 0 { // the first fragment only
			return pl[1] & 0x1f
		}

		return 0
	case 24:
		if len(pl) > 3 {
			return pl[3] & 0x1f
		}

		return 0
	}

	return t
}

func nalName(t byte) string {
	switch t {
	case 5:
		return "IDR"
	case 7:
		return "SPS"
	case 8:
		return "PPS"
	case 6:
		return "SEI"
	case 1:
		return "P"
	}

	return fmt.Sprintf("nal%d", t)
}
