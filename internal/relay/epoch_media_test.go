package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/token"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type epochVideo struct{ units [][]byte }

func makeEpochVideo(t *testing.T, width, height int) epochVideo {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is required for actual Relay decode proof")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", fmt.Sprintf("testsrc2=size=%dx%d:rate=10", width, height), "-t", "2", "-an", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-tune", "zerolatency", "-pix_fmt", "yuv420p", "-g", "5", "-keyint_min", "5", "-sc_threshold", "0", "-movflags", "+empty_moov+default_base_moof+frag_keyframe", "-f", "mp4", "pipe:1")
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	b, err := cmd.Output()
	require.NoError(t, err)
	require.Less(t, len(b), 1<<20)
	reader := fmp4.NewReader(bytes.NewReader(b))
	out := epochVideo{}
	for {
		u, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		out.units = append(out.units, append([]byte(nil), u.Bytes()...))
	}
	require.Greater(t, len(out.units), 2)
	return out
}

type epochPublisher struct {
	stream  api.RelayIngest_AttachClient
	mu      sync.Mutex
	welcome chan *api.AttachResponse_Welcome
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	err     chan error
}

func newEpochPublisher(t *testing.T, f epochFixture) *epochPublisher {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer(grpc.MaxRecvMsgSize(MaxFragment))
	api.RegisterRelayIngestServer(g, &ingest{r: f.r})
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	stream, err := api.NewRelayIngestClient(conn).Attach(ctx)
	require.NoError(t, err)
	p := &epochPublisher{stream: stream, welcome: make(chan *api.AttachResponse_Welcome, 8), ctx: ctx, cancel: cancel, done: make(chan struct{}), err: make(chan error, 1)}
	t.Cleanup(func() {
		cancel()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Error("publication receiver did not stop after cancellation")
		}
	})
	go func() {
		defer close(p.done)
		for {
			msg, err := stream.Recv()
			if err != nil {
				p.err <- err
				return
			}
			if msg.GetWelcome() != nil {
				select {
				case p.welcome <- msg.GetWelcome():
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return p
}
func (p *epochPublisher) send(msg *api.AttachRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stream.Send(msg)
}
func (p *epochPublisher) hello(t *testing.T, f epochFixture, c *api.TokenClaims) {
	t.Helper()
	signed, err := f.key.Sign(c)
	require.NoError(t, err)
	require.NoError(t, p.send(api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: signed}.Build()}.Build()))
	select {
	case <-p.welcome:
	case <-p.ctx.Done():
		t.Fatal("Relay did not acknowledge publication membership")
	}
}

type epochAU struct {
	data []byte
	rtp  uint32
	at   time.Time
	key  bool
}
type epochViewer struct {
	t                   *testing.T
	pc                  *webrtc.PeerConnection
	url, session, token string
	client              *http.Client
	cancel              context.CancelFunc
	done                chan struct{}
	mu                  sync.Mutex
	units               []epochAU
	size                int
	err                 error
}

func nalKinds(b []byte) (sps, pps, key, vcl bool) {
	for i := 0; i+3 < len(b); i++ {
		n := 0
		if i+4 < len(b) && bytes.Equal(b[i:i+4], []byte{0, 0, 0, 1}) {
			n = i + 4
		} else if bytes.Equal(b[i:i+3], []byte{0, 0, 1}) {
			n = i + 3
		}
		if n == 0 || n >= len(b) {
			continue
		}
		switch b[n] & 31 {
		case 7:
			sps = true
		case 8:
			pps = true
		case 5:
			key = true
			vcl = true
		case 1, 2, 3, 4:
			vcl = true
		}
	}
	return
}

func openEpochViewer(t *testing.T, f epochFixture, base string, id pdid.Id, epoch string) *epochViewer {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	viewer := &epochViewer{t: t, pc: pc, url: base, cancel: cancel, done: make(chan struct{}), client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	stop := context.AfterFunc(ctx, func() { pc.Close() })
	t.Cleanup(func() { viewer.close(); stop() })
	tracks := make(chan *webrtc.TrackRemote, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			select {
			case tracks <- track:
			default:
			}
		}
	})
	go func() {
		defer close(viewer.done)
		var track *webrtc.TrackRemote
		select {
		case track = <-tracks:
		case <-ctx.Done():
			return
		}
		builder := samplebuilder.New(50, &codecs.H264Packet{}, 90000)
		started := false
		for packets := 0; packets < 4000; packets++ {
			packet, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			builder.Push(packet)
			for {
				sample := builder.Pop()
				if sample == nil {
					break
				}
				sps, pps, key, vcl := nalKinds(sample.Data)
				if !vcl {
					continue
				}
				if !started {
					if !sps || !pps || !key {
						continue
					}
					started = true
				}
				viewer.mu.Lock()
				if sample.PrevDroppedPackets != 0 || len(sample.Data) > 256<<10 || viewer.size+len(sample.Data) > 2<<20 || len(viewer.units) >= 200 {
					viewer.err = errors.New("bounded complete RTP capture failed")
					viewer.mu.Unlock()
					return
				}
				if len(viewer.units) > 0 && int32(sample.PacketTimestamp-viewer.units[len(viewer.units)-1].rtp) <= 0 {
					viewer.err = errors.New("RTP media timestamp did not advance")
					viewer.mu.Unlock()
					return
				}
				viewer.units = append(viewer.units, epochAU{data: append([]byte(nil), sample.Data...), rtp: sample.PacketTimestamp, at: time.Now(), key: key && sps && pps})
				viewer.size += len(sample.Data)
				viewer.mu.Unlock()
			}
		}
	}()
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	gather := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(offer))
	select {
	case <-gather:
	case <-ctx.Done():
		t.Fatal("viewer ICE gathering timed out")
	}
	claims := api.TokenClaims_builder{Op: api.TokenOp_TOKEN_OP_VIEW, Aud: f.r.id.Bytes(), Actor: f.viewerActor.Bytes(), Source: id.Bytes(), CaptureEpoch: epoch, Iat: timestamppb.Now(), Exp: timestamppb.New(time.Now().Add(time.Minute))}.Build()
	viewer.token, err = f.key.Sign(claims)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/whep/"+id.String(), strings.NewReader(pc.LocalDescription().SDP))
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+viewer.token)
	response, err := viewer.client.Do(req)
	require.NoError(t, err)
	answer, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode, string(answer))
	require.Equal(t, epoch, response.Header.Get("Shale-Capture-Epoch"))
	require.Contains(t, response.Header.Get("Access-Control-Expose-Headers"), "Shale-Capture-Epoch")
	viewer.session = response.Header.Get("Location")
	require.True(t, strings.HasPrefix(viewer.session, "/whep/"))
	require.NoError(t, pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}))
	return viewer
}

func (v *epochViewer) snapshot() ([]epochAU, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]epochAU(nil), v.units...), v.err
}
func (v *epochViewer) close() {
	v.cancel()
	v.pc.Close()
	select {
	case <-v.done:
	case <-time.After(2 * time.Second):
		v.t.Error("viewer RTP reader did not stop after cancellation")
	}
	if v.session != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, v.url+v.session, nil)
		req.Header.Set("Authorization", token.Scheme+" "+v.token)
		if response, err := v.client.Do(req); err == nil {
			response.Body.Close()
		}
	}
}

// Decode independently of Relay's fMP4/parser/packetizer. A packet or NAL count
// cannot satisfy this gate; ffmpeg must emit multiple complete decoded frames.
func decodedEpochFrames(t *testing.T, units []epochAU, width, height int) int {
	t.Helper()
	var annex bytes.Buffer
	for _, u := range units {
		annex.Write(u.data)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-threads", "1", "-err_detect", "explode", "-xerror", "-f", "h264", "-i", "pipe:0", "-an", "-fps_mode", "passthrough", "-f", "framemd5", "pipe:1")
	cmd.Stdin = &annex
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	require.NoError(t, err)
	require.Contains(t, string(out), fmt.Sprintf("#dimensions 0: %dx%d", width, height))
	frames := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "0,") {
			frames++
		}
	}
	require.Greater(t, frames, 1, "actual H.264 decoding, not packet structure, is required")
	return frames
}

func TestRelayCaptureEpochActualDecodedCutover(t *testing.T) {
	f := newEpochFixture(t)
	oldVideo, newVideo := makeEpochVideo(t, 160, 90), makeEpochVideo(t, 320, 180)
	aID, bID := pdid.New(8), pdid.New(8)
	publisher := newEpochPublisher(t, f)
	publisher.hello(t, f, f.claims(nil))
	f.at = time.Now().UTC()
	publisher.hello(t, f, f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-1", 1), epochGrant(bID, "b-1", 1)))
	type incarnation struct {
		epoch string
		video epochVideo
	}
	var mediaMu sync.Mutex
	aMedia := incarnation{"a-1", oldVideo}
	ctx, cancel := context.WithCancel(t.Context())
	feedDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		publisher.cancel()
		select {
		case <-feedDone:
		case <-time.After(2 * time.Second):
			t.Error("publication media sender did not stop after cancellation")
		}
	})
	go func() {
		defer close(feedDone)
		for ctx.Err() == nil {
			mediaMu.Lock()
			captured := aMedia
			mediaMu.Unlock() // immutable for this old capture cycle
			for i := range oldVideo.units {
				for _, entry := range []struct {
					id      pdid.Id
					capture incarnation
				}{{aID, captured}, {bID, incarnation{"b-1", oldVideo}}} {
					if i >= len(entry.capture.video.units) {
						continue
					}
					if publisher.send(api.AttachRequest_builder{Data: api.AttachRequest_Data_builder{SourceId: entry.id.Bytes(), CaptureEpoch: entry.capture.epoch, Payload: entry.capture.video.units[i]}.Build()}.Build()) != nil {
						return
					}
				}
				if i > 0 {
					select {
					case <-time.After(500 * time.Millisecond):
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	require.Eventually(t, func() bool { return f.r.sources.get(aID).viewEpoch("a-1") && f.r.sources.get(bID).viewEpoch("b-1") }, 5*time.Second, 10*time.Millisecond)
	httpServer := httptest.NewServer(f.r.whep)
	t.Cleanup(httpServer.Close)
	aViewer, bViewer := openEpochViewer(t, f, httpServer.URL, aID, "a-1"), openEpochViewer(t, f, httpServer.URL, bID, "b-1")
	require.Eventually(t, func() bool {
		a, e := aViewer.snapshot()
		b, be := bViewer.snapshot()
		return e == nil && be == nil && len(a) >= 5 && len(b) >= 5
	}, 8*time.Second, 10*time.Millisecond)
	bBefore, _ := bViewer.snapshot()
	cutover := time.Now()
	f.at = time.Now().UTC()
	publisher.hello(t, f, f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-2", 2), epochGrant(bID, "b-1", 1)))
	f.r.whep.mu.Lock()
	_, oldAAlive := f.r.whep.sessions[strings.TrimPrefix(aViewer.session, "/whep/")]
	_, bAlive := f.r.whep.sessions[strings.TrimPrefix(bViewer.session, "/whep/")]
	f.r.whep.mu.Unlock()
	require.False(t, oldAAlive)
	require.True(t, bAlive, "unrelated source keeps the same live peer")
	aViewer.close()
	// A delayed old init/fragment cannot prepare the new incarnation.
	for _, u := range oldVideo.units {
		require.NoError(t, publisher.send(api.AttachRequest_builder{Data: api.AttachRequest_Data_builder{SourceId: aID.Bytes(), CaptureEpoch: "a-1", Payload: u}.Build()}.Build()))
	}
	require.False(t, f.r.sources.get(aID).viewEpoch("a-2"))
	mediaMu.Lock()
	aMedia = incarnation{"a-2", newVideo}
	mediaMu.Unlock()
	require.Eventually(t, func() bool { return f.r.sources.get(aID).viewEpoch("a-2") }, 6*time.Second, 10*time.Millisecond)
	newViewer := openEpochViewer(t, f, httpServer.URL, aID, "a-2")
	require.Eventually(t, func() bool {
		a, e := newViewer.snapshot()
		b, be := bViewer.snapshot()
		return e == nil && be == nil && len(a) >= 8 && len(b) >= len(bBefore)+8
	}, 8*time.Second, 10*time.Millisecond)
	aUnits, aErr := newViewer.snapshot()
	bUnits, bErr := bViewer.snapshot()
	require.NoError(t, aErr)
	require.NoError(t, bErr)
	for _, endpoint := range []struct{ method, path string }{{http.MethodPost, "/whep/" + aID.String()}, {http.MethodGet, "/recent/" + aID.String()}, {http.MethodPatch, newViewer.session}} {
		req, err := http.NewRequestWithContext(t.Context(), endpoint.method, httpServer.URL+endpoint.path, strings.NewReader(newViewer.pc.LocalDescription().SDP))
		require.NoError(t, err)
		req.Header.Set("Authorization", token.Scheme+" "+aViewer.token)
		response, err := newViewer.client.Do(req)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, http.StatusConflict, response.StatusCode, "old capture capability must not open/read/renew the new epoch")
	}
	f.r.whep.mu.Lock()
	_, newAlive := f.r.whep.sessions[strings.TrimPrefix(newViewer.session, "/whep/")]
	f.r.whep.mu.Unlock()
	require.True(t, newAlive, "stale renewal must not close the valid new peer")
	decodedEpochFrames(t, aUnits, 320, 180)
	var bAfter []epochAU
	for _, u := range bUnits {
		if len(bAfter) == 0 {
			if !u.at.After(cutover) || !u.key {
				continue
			}
		}
		bAfter = append(bAfter, u)
	}
	decodedEpochFrames(t, bAfter, 160, 90)
	require.Greater(t, int32(bUnits[len(bUnits)-1].rtp-bBefore[len(bBefore)-1].rtp), int32(0))
	t.Log("new capture decoded as 320x180; unchanged peer decoded advancing 160x90 media after cutover; epoch", strconv.Itoa(2))
}

func TestRelayCaptureEpochIdleZeroPublisherExpires(t *testing.T) {
	f := newEpochFixture(t)
	p := newEpochPublisher(t, f)
	claims := f.claims(nil)
	claims.SetExp(timestamppb.New(time.Now().Add(500 * time.Millisecond)))
	p.hello(t, f, claims)
	select {
	case err := <-p.err:
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Equal(t, "publication capability expired", status.Convert(err).Message())
	case <-time.After(2 * time.Second):
		t.Fatal("idle zero-source publisher remained authenticated after expiry")
	}
}
