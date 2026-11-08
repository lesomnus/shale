package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// edgeMediaOptions bounds network capture, memory and the independent decoder.
// It is shared by the legacy proof here and the assembled edge qualification.
type edgeMediaOptions struct {
	Timeout     time.Duration
	AccessUnits int
	MaxBytes    int
	FFmpeg      string
	HTTP        *http.Client
}

type edgeMediaEvidence struct {
	AccessUnits       int
	DecodedFrames     int
	RTPAdvances       int
	FirstRTP, LastRTP uint32
	// Receiver-local complete-AU receipt times plus RTP progress prove incoming
	// media advanced; they do not identify a camera capture or publication epoch.
	FirstReceivedAt, LastReceivedAt time.Time
	// Independent decoder output observation times, after network capture. Later
	// cutover qualification must separately correlate the Producer/Source epoch.
	FirstDecodedAt, LastDecodedAt time.Time
	Bytes                         int
}

type edgeMediaCapture struct {
	annexB   []byte
	evidence edgeMediaEvidence
	err      error
}

// edgeAccessUnits accepts only a contiguous complete sequence starting with an
// SPS/PPS/IDR access unit. It is owned by one RTP reader, never an OnTrack callback.
type edgeAccessUnits struct {
	target, maxBytes int
	annexB           []byte
	evidence         edgeMediaEvidence
}

func (c *edgeAccessUnits) push(u accessUnit) (bool, error) {
	hasVCL, key, sps, pps := false, false, false, false
	for _, n := range u.nals {
		if len(n) == 0 {
			return false, errors.New("empty H264 NAL unit")
		}
		switch n[0] & 31 {
		case 7:
			if !hasVCL {
				sps = true
			}
		case 8:
			if !hasVCL {
				pps = true
			}
		case 1, 2, 3, 4:
			hasVCL = true
		case 5:
			hasVCL = true
			key = sps && pps
		}
	}
	if !u.whole || !u.marked || (c.evidence.AccessUnits > 0 && int32(u.ts-c.evidence.LastRTP) <= 0) {
		c.annexB = nil
		c.evidence = edgeMediaEvidence{}
		return false, nil
	}
	if c.evidence.AccessUnits == 0 && !key {
		return false, nil
	}
	if !hasVCL {
		return false, nil
	}
	size := 0
	for _, n := range u.nals {
		if len(n) > c.maxBytes-size-4 {
			return false, errors.New("H264 access unit exceeds capture budget")
		}
		size += 4 + len(n)
	}
	if size > c.maxBytes-len(c.annexB) {
		return false, errors.New("H264 capture exceeds byte budget")
	}
	if c.evidence.AccessUnits == 0 {
		c.evidence.FirstRTP = u.ts
		c.evidence.FirstReceivedAt = u.at
	} else {
		c.evidence.RTPAdvances++
	}
	for _, n := range u.nals {
		c.annexB = append(c.annexB, 0, 0, 0, 1)
		c.annexB = append(c.annexB, n...)
	}
	c.evidence.AccessUnits++
	c.evidence.LastRTP = u.ts
	c.evidence.LastReceivedAt = u.at
	c.evidence.Bytes = len(c.annexB)
	return c.evidence.AccessUnits >= c.target, nil
}

// qualifyWHEP proves actual WebRTC video decodes independently. A Live URL,
// packets, parameter-set bytes or an IDR marker alone cannot satisfy it.
func qualifyWHEP(parent context.Context, live *api.LiveSource, options edgeMediaOptions) (evidence edgeMediaEvidence, result error) {
	if options.Timeout == 0 {
		options.Timeout = 15 * time.Second
	}
	if options.AccessUnits == 0 {
		options.AccessUnits = 8
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 16 << 20
	}
	if options.FFmpeg == "" {
		options.FFmpeg = "ffmpeg"
	}
	if options.HTTP == nil {
		options.HTTP = http.DefaultClient
	}
	// WHEP capabilities authorize one endpoint. Copy the caller's client, then
	// use the same redirect-disabled instance for offer and session cleanup.
	client := *options.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	options.HTTP = &client
	if options.Timeout <= 0 || options.Timeout > time.Minute || options.AccessUnits < 2 || options.AccessUnits > 120 || options.MaxBytes < 1024 || options.MaxBytes > 64<<20 {
		return evidence, errors.New("invalid media qualification bounds")
	}
	ctx, stop := context.WithTimeout(parent, options.Timeout)
	defer stop()
	captureCtx, cancelCapture := context.WithCancel(ctx)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		cancelCapture()
		return evidence, errors.New("create WebRTC viewer")
	}
	tracks := make(chan *webrtc.TrackRemote, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			return
		}
		select {
		case tracks <- track:
		default:
		}
	})
	if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		cancelCapture()
		pc.Close()
		return evidence, errors.New("configure WebRTC video receiver")
	}
	captured := make(chan edgeMediaCapture, 1)
	done := make(chan struct{})
	go func() { defer close(done); captured <- readEdgeVideo(captureCtx, tracks, options) }()
	var session string
	// Cancel/close/drain before returning or decoding. Dynamic Pion callbacks
	// only send a track pointer; they never mutate capture or decoder evidence.
	stopped := false
	stopViewer := func() {
		if stopped {
			return
		}
		stopped = true
		cancelCapture()
		pc.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			if result == nil {
				result = errors.New("WebRTC reader outlived cancellation")
			}
		}
		if session != "" {
			cleanup, finish := context.WithTimeout(context.Background(), 2*time.Second)
			defer finish()
			req, err := http.NewRequestWithContext(cleanup, http.MethodDelete, session, nil)
			if err == nil {
				req.Header.Set("Authorization", token.Scheme+" "+live.GetViewToken())
				if resp, err := options.HTTP.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}
	}
	defer stopViewer()
	session, err = offerEdgeWHEP(ctx, options.HTTP, pc, live)
	if err != nil {
		return evidence, err
	}
	var media edgeMediaCapture
	select {
	case media = <-captured:
	case <-ctx.Done():
		return evidence, ctx.Err()
	}
	if media.err != nil {
		return evidence, media.err
	}
	stopViewer()
	if result != nil {
		return evidence, result
	}
	decoded, first, last, err := decodeEdgeH264(ctx, options.FFmpeg, media.annexB, options.AccessUnits)
	if err != nil {
		return evidence, err
	}
	evidence = media.evidence
	evidence.DecodedFrames = decoded
	evidence.FirstDecodedAt = first
	evidence.LastDecodedAt = last
	if decoded < 2 || decoded > evidence.AccessUnits || evidence.RTPAdvances < 1 || !last.After(first) {
		return evidence, errors.New("decoded video did not advance")
	}
	return evidence, nil
}

func readEdgeVideo(ctx context.Context, tracks <-chan *webrtc.TrackRemote, options edgeMediaOptions) edgeMediaCapture {
	var track *webrtc.TrackRemote
	select {
	case track = <-tracks:
	case <-ctx.Done():
		return edgeMediaCapture{err: ctx.Err()}
	}
	if !strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeH264) {
		return edgeMediaCapture{err: errors.New("WebRTC did not negotiate H264")}
	}
	asm := &assembler{}
	jb := &jitter{wait: jitterWait}
	collector := edgeAccessUnits{target: options.AccessUnits, maxBytes: options.MaxBytes}
	var complete bool
	var captureErr error
	var received int
	var payloadBytes int
	for ctx.Err() == nil {
		track.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		p, _, err := track.ReadRTP()
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				continue
			}
			return edgeMediaCapture{err: errors.New("WebRTC video ended before qualification")}
		}
		received++
		payloadBytes += len(p.Payload)
		if payloadBytes > options.MaxBytes {
			return edgeMediaCapture{err: errors.New("RTP payload budget exceeded")}
		}
		if received > 200000 {
			return edgeMediaCapture{err: errors.New("RTP packet budget exceeded")}
		}
		jb.push(p, time.Now(), func(p *rtp.Packet, at time.Time) {
			if complete || captureErr != nil {
				return
			}
			asm.push(p, at, func(u accessUnit) {
				if len(asm.frag) > 0 {
					u.whole = false
				}
				complete, captureErr = collector.push(u)
			})
		})
		if captureErr != nil {
			return edgeMediaCapture{err: captureErr}
		}
		if complete {
			return edgeMediaCapture{annexB: collector.annexB, evidence: collector.evidence}
		}
	}
	return edgeMediaCapture{err: ctx.Err()}
}

// offerEdgeWHEP is context bounded, including ICE gathering and SDP I/O. Errors
// contain no response body, URL or bearer capability.
func offerEdgeWHEP(ctx context.Context, client *http.Client, pc *webrtc.PeerConnection, live *api.LiveSource) (string, error) {
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return "", errors.New("create WHEP offer")
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return "", errors.New("set WHEP local description")
	}
	select {
	case <-gathered:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, live.GetWhepUrl(), strings.NewReader(pc.LocalDescription().SDP))
	if err != nil {
		return "", errors.New("invalid WHEP request")
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Authorization", token.Scheme+" "+live.GetViewToken())
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errors.New("WHEP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("WHEP returned HTTP %d", resp.StatusCode)
	}
	base, err := url.Parse(live.GetWhepUrl())
	if err != nil {
		return "", errors.New("invalid WHEP endpoint")
	}
	location, err := url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		return "", errors.New("WHEP has no session location")
	}
	resolved := base.ResolveReference(location)
	if resolved.Scheme != base.Scheme || resolved.Host != base.Host || resolved.User != nil || resolved.Fragment != "" {
		return "", errors.New("WHEP session changed origin")
	}
	session := resolved.String()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return session, errors.New("invalid WHEP answer size")
	}
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}); err != nil {
		return session, errors.New("set WHEP remote description")
	}
	return session, nil
}

type edgeDecodeFrames struct {
	pending     []byte
	frames      int
	first, last time.Time
}

func (d *edgeDecodeFrames) Write(p []byte) (int, error) {
	if len(d.pending)+len(p) > 64<<10 {
		return 0, errors.New("decoder output exceeds budget")
	}
	d.pending = append(d.pending, p...)
	for {
		line, rest, found := bytes.Cut(d.pending, []byte{'\n'})
		if !found {
			break
		}
		d.pending = rest
		text := strings.TrimSpace(string(line))
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, ",")
		if len(fields) != 6 {
			return 0, errors.New("invalid decoder frame evidence")
		}
		size, err := strconv.Atoi(strings.TrimSpace(fields[4]))
		if err != nil || size <= 0 || len(strings.TrimSpace(fields[5])) != 32 {
			return 0, errors.New("invalid decoded frame checksum")
		}
		now := time.Now()
		if d.frames == 0 {
			d.first = now
		}
		d.last = now
		d.frames++
	}
	return len(p), nil
}

func decodeEdgeH264(ctx context.Context, ffmpeg string, annexB []byte, maxFrames int) (int, time.Time, time.Time, error) {
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-xerror", "-err_detect", "explode", "-threads", "1", "-f", "h264", "-i", "pipe:0", "-an", "-fps_mode", "passthrough", "-frames:v", strconv.Itoa(maxFrames), "-threads", "1", "-f", "framemd5", "pipe:1")
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = bytes.NewReader(annexB)
	frames := &edgeDecodeFrames{}
	cmd.Stdout = frames
	// Discard diagnostic text: decoder rejection is evidence, while stderr may
	// include arbitrary camera-supplied NAL/SEI data and is not needed in logs.
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return 0, time.Time{}, time.Time{}, ctx.Err()
		}
		return 0, time.Time{}, time.Time{}, errors.New("independent H264 decoder rejected captured media")
	}
	return frames.frames, frames.first, frames.last, nil
}

// This proof intentionally uses the existing legacy Producer/Relay path. The
// assembled CP-managed edge scenario will reuse qualifyWHEP after its dependent
// policy, credential and runtime APIs have landed.
func TestEdgeMediaDecodesActualWHEPVideo(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := c.dial("@acme/admin")
	_, live := liveSource(t, ctx, c, admin, "av.mp4")
	evidence, err := qualifyWHEP(ctx, live, edgeMediaOptions{})
	require.NoError(t, err)
	require.Equal(t, 8, evidence.AccessUnits)
	require.Greater(t, evidence.DecodedFrames, 1, "independent decoder must produce advancing frames")
	require.Equal(t, 7, evidence.RTPAdvances)
	require.Greater(t, int32(evidence.LastRTP-evidence.FirstRTP), int32(0))
	require.True(t, evidence.LastReceivedAt.After(evidence.FirstReceivedAt))
	require.True(t, evidence.LastDecodedAt.After(evidence.FirstDecodedAt))
	require.Greater(t, evidence.Bytes, 0)
	t.Logf("WHEP decoded %d frames from %d complete AUs, RTP advances=%d, captured=%d bytes", evidence.DecodedFrames, evidence.AccessUnits, evidence.RTPAdvances, evidence.Bytes)
}

func TestEdgeMediaMarkersDoNotProveDecoding(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	collector := edgeAccessUnits{target: 2, maxBytes: 1024}
	at := time.Now()
	complete, err := collector.push(accessUnit{ts: 10, at: at, whole: true, marked: true, nals: [][]byte{{0x67, 0}, {0x68, 0}, {0x65, 0}}})
	require.NoError(t, err)
	require.False(t, complete)
	complete, err = collector.push(accessUnit{ts: 3010, at: at.Add(time.Millisecond), whole: true, marked: true, nals: [][]byte{{0x41, 0}}})
	require.NoError(t, err)
	require.True(t, complete, "structure alone would have looked ready")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	count, _, _, err := decodeEdgeH264(ctx, ffmpeg, collector.annexB, 2)
	require.Error(t, err)
	require.Zero(t, count, "NAL markers with invalid coded data cannot qualify")
}

func TestEdgeMediaDropsBrokenOrNonadvancingAccessUnits(t *testing.T) {
	collector := edgeAccessUnits{target: 2, maxBytes: 1024}
	key := accessUnit{ts: 10, at: time.Now(), whole: true, marked: true, nals: [][]byte{{0x67}, {0x68}, {0x65}}}
	complete, err := collector.push(key)
	require.NoError(t, err)
	require.False(t, complete)
	complete, err = collector.push(accessUnit{ts: 3010, whole: false, marked: true, nals: [][]byte{{0x41}}})
	require.NoError(t, err)
	require.False(t, complete)
	require.Zero(t, collector.evidence.AccessUnits, "a packet gap invalidates the decoding chain")
	complete, err = collector.push(accessUnit{ts: 6010, whole: true, marked: true, nals: [][]byte{{0x41}}})
	require.NoError(t, err)
	require.False(t, complete)
	require.Empty(t, collector.annexB, "P frames alone cannot restart qualification")
	_, err = collector.push(key)
	require.NoError(t, err)
	complete, err = collector.push(key)
	require.NoError(t, err)
	require.False(t, complete)
	require.Zero(t, collector.evidence.AccessUnits, "repeated RTP timestamps cannot prove progress")
	tiny := edgeAccessUnits{target: 2, maxBytes: 8}
	_, err = tiny.push(key)
	require.Error(t, err, "buffer budget applies before appending an access unit")
}

func TestEdgeMediaRTPProgressionSurvivesClockWrap(t *testing.T) {
	collector := edgeAccessUnits{target: 2, maxBytes: 1024}
	_, err := collector.push(accessUnit{ts: ^uint32(0) - 1000, whole: true, marked: true, nals: [][]byte{{0x67}, {0x68}, {0x65}}})
	require.NoError(t, err)
	complete, err := collector.push(accessUnit{ts: 2000, whole: true, marked: true, nals: [][]byte{{0x41}}})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, 1, collector.evidence.RTPAdvances)
}

func TestEdgeMediaWHEPNegotiationIsContextBounded(t *testing.T) {
	entered, requestDone, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(requestDone)
		io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() { close(release); endpoint.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := qualifyWHEP(ctx, api.LiveSource_builder{WhepUrl: endpoint.URL, ViewToken: "private-test-capability"}.Build(), edgeMediaOptions{Timeout: 2 * time.Second})
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("WHEP did not reach server: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("WHEP request did not start")
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("WHEP helper outlived cancellation")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("WHEP request outlived cancellation")
	}
}

func TestEdgeMediaNeverRedirectsViewCapabilities(t *testing.T) {
	for _, phase := range []string{"offer", "cleanup"} {
		for _, sameOrigin := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/same_origin_%t", phase, sameOrigin), func(t *testing.T) {
				const capability = "private-view-capability-must-not-follow-redirects"
				var remoteRequests, localRequests, cleanupRequests, originalRedirects atomic.Int32
				var cleanupAuthorized atomic.Bool
				sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					remoteRequests.Add(1)
					w.WriteHeader(http.StatusNoContent)
				}))
				defer sink.Close()
				redirect := func(w http.ResponseWriter) {
					target := sink.URL + "/redirected"
					if sameOrigin {
						target = "/redirected"
					}
					w.Header().Set("Location", target)
					w.WriteHeader(http.StatusTemporaryRedirect)
				}
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/whep":
						if phase == "offer" {
							redirect(w)
							return
						}
						w.Header().Set("Location", "/session")
						w.WriteHeader(http.StatusCreated)
						io.WriteString(w, "invalid-sdp")
					case "/session":
						if r.Method == http.MethodDelete {
							cleanupRequests.Add(1)
							cleanupAuthorized.Store(r.Header.Get("Authorization") == token.Scheme+" "+capability)
						}
						redirect(w)
					default:
						localRequests.Add(1)
						w.WriteHeader(http.StatusNoContent)
					}
				}))
				defer endpoint.Close()
				client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { originalRedirects.Add(1); return nil }}
				_, err := qualifyWHEP(context.Background(), api.LiveSource_builder{WhepUrl: endpoint.URL + "/whep", ViewToken: capability}.Build(), edgeMediaOptions{HTTP: client, Timeout: 2 * time.Second})
				require.Error(t, err)
				require.NotContains(t, err.Error(), capability)
				require.Zero(t, remoteRequests.Load(), "the other HTTP origin must never receive the capability")
				require.Zero(t, localRequests.Load(), "even a same-origin redirect is outside the WHEP request contract")
				require.Zero(t, originalRedirects.Load(), "the qualification client must override redirect policy")
				if phase == "cleanup" {
					require.Equal(t, int32(1), cleanupRequests.Load())
					require.True(t, cleanupAuthorized.Load(), "exercise an authenticated DELETE before its redirect is refused")
				} else {
					require.Zero(t, cleanupRequests.Load())
				}
				require.NoError(t, client.CheckRedirect(nil, nil))
				require.Equal(t, int32(1), originalRedirects.Load(), "caller client's redirect policy must remain unchanged")
			})
		}
	}
}
