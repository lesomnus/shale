package relay

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/mpegts"
	"github.com/lesomnus/shale/internal/token"
)

// The WHEP side (§35.8, §39.4): POST /whep/{source} with a view token and
// an SDP offer answers 201 with the SDP answer and a session Location;
// DELETE /whep/{session} ends it. Video only, never transcoded: an H.264
// (or H.265) track packetized from the access units the producer sends.

type whepServer struct {
	r   *Relay
	api *webrtc.API
	ice []webrtc.ICEServer

	mu       sync.Mutex
	sessions map[string]*viewer
}

// viewer is one WHEP session: a peer connection with one video track.
type viewer struct {
	key    string
	actor  string
	source *source
	pc     *webrtc.PeerConnection
	track  *webrtc.TrackLocalStaticSample
	audio  *webrtc.TrackLocalStaticSample
	once   sync.Once
	done   chan struct{}
	// caught says the group of pictures was handed over after connecting.
	caught bool
	mu     sync.Mutex
}

func newWhepServer(r *Relay) (*whepServer, error) {
	se := webrtc.SettingEngine{}
	if r.cfg.UdpPortMin > 0 && r.cfg.UdpPortMax > 0 {
		if err := se.SetEphemeralUDPPortRange(uint16(r.cfg.UdpPortMin), uint16(r.cfg.UdpPortMax)); err != nil {
			return nil, err
		}
	}
	if len(r.cfg.Nat1To1) > 0 {
		se.SetNAT1To1IPs(r.cfg.Nat1To1, webrtc.ICECandidateTypeHost)
	}
	me := &webrtc.MediaEngine{}
	if err := me.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	w := &whepServer{r: r, api: webrtc.NewAPI(webrtc.WithSettingEngine(se), webrtc.WithMediaEngine(me)), sessions: map[string]*viewer{}}
	for _, u := range r.cfg.Ice {
		w.ice = append(w.ice, webrtc.ICEServer{URLs: []string{u}})
	}

	return w, nil
}

func (w *whepServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/whep/")
	switch {
	case req.URL.Path == "/healthz":
		rw.WriteHeader(http.StatusNoContent)
	case req.Method == http.MethodPost && path != "" && path == strings.TrimSuffix(path, "/"):
		w.post(rw, req, path)
	case req.Method == http.MethodDelete && path != "":
		w.del(rw, path)
	case req.Method == http.MethodOptions:
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		rw.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS")
		rw.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		rw.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(rw, req)
	}
}

// post opens a session: the token must name this relay, the source, and
// the operation; the offer must carry a video codec the source uses.
func (w *whepServer) post(rw http.ResponseWriter, req *http.Request, sourceRef string) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	tok := token.FromHeader(req.Header.Get("Authorization"))
	if tok == "" {
		tok = req.URL.Query().Get("token")
	}
	claims, err := w.r.verify(tok, api.TokenOp_TOKEN_OP_VIEW)
	if err != nil {
		w.r.m.sessions.Add(req.Context(), 1, outcomeAttr("bad_token"))
		http.Error(rw, err.Error(), http.StatusUnauthorized)
		return
	}
	sourceId, err := pdid.Parse(sourceRef)
	if err != nil || string(claims.GetSource()) != string(sourceId.Bytes()) {
		http.Error(rw, "the token names another source", http.StatusForbidden)
		return
	}
	actor := ""
	if a, err := pdid.From(claims.GetActor()); err == nil {
		actor = a.String()
	}
	total, mine := w.r.sources.viewerCount(actor)
	if total >= w.r.cfg.MaxViewers || mine >= w.r.cfg.ViewersPerActor {
		w.r.m.sessions.Add(req.Context(), 1, outcomeAttr("limit"))
		rw.Header().Set("Retry-After", "10")
		http.Error(rw, "at the viewer limit", http.StatusServiceUnavailable)
		return
	}
	offer, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil || len(offer) == 0 {
		http.Error(rw, "an SDP offer is the body", http.StatusBadRequest)
		return
	}

	src := w.r.sources.get(sourceId)
	mime := webrtc.MimeTypeH264
	if src.codec() == mpegts.StreamH265 {
		mime = webrtc.MimeTypeH265
	}
	pc, err := w.api.NewPeerConnection(webrtc.Configuration{ICEServers: w.ice})
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: mime, ClockRate: 90000}, "video", "shale")
	if err != nil {
		pc.Close()
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		pc.Close()
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	// RTCP from the viewer is read and dropped, so the sender's interceptors
	// keep running.
	drain := func(s *webrtc.RTPSender) {
		buf := make([]byte, 1500)
		for {
			if _, _, err := s.Read(buf); err != nil {
				return
			}
		}
	}
	go drain(sender)
	// Audio rides along as Opus when the producer records it (§39.4); the
	// track is offered whenever the viewer asked for audio, and stays
	// silent for a source without it.
	var audio *webrtc.TrackLocalStaticSample
	if strings.Contains(string(offer), "m=audio") {
		audio, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "shale")
		if err != nil {
			pc.Close()
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		as, err := pc.AddTrack(audio)
		if err != nil {
			pc.Close()
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		go drain(as)
	}

	key := newSessionKey(actor)
	v := &viewer{key: key, actor: actor, source: src, pc: pc, track: track, audio: audio, done: make(chan struct{})}
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		switch st {
		case webrtc.PeerConnectionStateConnected:
			v.mu.Lock()
			first := !v.caught
			v.caught = true
			v.mu.Unlock()
			if first {
				src.catchUp(v)
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateDisconnected:
			w.end(key)
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)}); err != nil {
		pc.Close()
		http.Error(rw, "offer: "+err.Error(), http.StatusBadRequest)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		http.Error(rw, "answer: "+err.Error(), http.StatusBadRequest)
		return
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	select {
	case <-gathered:
	case <-time.After(3 * time.Second):
	}

	w.mu.Lock()
	w.sessions[key] = v
	w.mu.Unlock()
	w.r.m.sessions.Add(req.Context(), 1, outcomeAttr("started"))
	src.addViewer(key, v)
	w.r.log.Info("viewer", "source", sourceId.String(), "actor", actor, "session", key[:8])

	rw.Header().Set("Content-Type", "application/sdp")
	rw.Header().Set("Location", "/whep/"+key)
	for _, u := range w.r.cfg.Ice {
		rw.Header().Add("Link", "<"+u+">; rel=\"ice-server\"")
	}
	rw.WriteHeader(http.StatusCreated)
	io.WriteString(rw, pc.LocalDescription().SDP)
}

func (w *whepServer) del(rw http.ResponseWriter, session string) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	if !w.end(session) {
		http.NotFound(rw, nil)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

// end closes a session; false when there is none.
func (w *whepServer) end(key string) bool {
	w.mu.Lock()
	v, ok := w.sessions[key]
	delete(w.sessions, key)
	w.mu.Unlock()
	if !ok {
		return false
	}
	v.once.Do(func() {
		v.source.removeViewer(key)
		v.pc.Close()
		close(v.done)
		w.r.log.Info("viewer left", "source", v.source.id.String(), "session", key[:8])
	})

	return true
}

func (w *whepServer) closeAll() {
	w.mu.Lock()
	keys := make([]string, 0, len(w.sessions))
	for k := range w.sessions {
		keys = append(keys, k)
	}
	w.mu.Unlock()
	for _, k := range keys {
		w.end(k)
	}
}

// write is the sink of a viewer: one access unit as one sample, which the
// track packetizes into RTP.
func (v *viewer) write(au mpegts.AccessUnit, d time.Duration) {
	if err := v.track.WriteSample(sampleOf(au, d)); err != nil {
		return
	}
}

// writeAudio is one Opus packet as one sample.
func (v *viewer) writeAudio(u mpegts.AudioUnit, d time.Duration) {
	if v.audio == nil {
		return
	}
	v.audio.WriteSample(media.Sample{Data: u.Data, Duration: d})
}

// A session key is random, prefixed by the actor for the per-actor limit.
func newSessionKey(actor string) string {
	b := make([]byte, 16)
	rand.Read(b)

	return actor + "." + hex.EncodeToString(b)
}

func actorOf(key string) string {
	if i := strings.LastIndexByte(key, '.'); i >= 0 {
		return key[:i]
	}

	return ""
}

// codec is the source's video stream type, once its tables were seen.
func (s *source) codec() byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.demux.Codec()
}

var _ = net.JoinHostPort
