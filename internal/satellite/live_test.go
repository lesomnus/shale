package satellite

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/token"
)

// fakeRelay is a relay's ingest as far as a feed can tell: what it was sent,
// and when, and a Start for what the satellite says it feeds, as the relay
// gives a source's new feeder while somebody watches (§39.3).
type fakeRelay struct {
	api.UnimplementedRelayIngestServer
	t    *testing.T
	addr string
	// stall stops reading the stream once the satellite feeds something: a
	// relay that is behind.
	stall bool

	mu     sync.Mutex
	hellos []string
	feeds  [][]pdid.Id
	data   []got
	wake   chan struct{}
}

type got struct {
	source  pdid.Id
	epoch   string
	payload []byte
	at      time.Time
	readAt  time.Time
}

func newFakeRelay(t *testing.T, stall bool) *fakeRelay {
	f := &fakeRelay{t: t, stall: stall, wake: make(chan struct{}, 1)}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// Windows that do not grow, so a relay that stops reading holds the
	// satellite back at once.
	srv := grpc.NewServer(grpc.InitialWindowSize(64<<10), grpc.InitialConnWindowSize(64<<10))
	api.RegisterRelayIngestServer(srv, f)
	go srv.Serve(l)
	t.Cleanup(srv.Stop)
	f.addr = l.Addr().String()

	return f
}

func (f *fakeRelay) Attach(stream api.RelayIngest_AttachServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		at := time.Now()
		f.mu.Lock()
		switch {
		case msg.GetHello() != nil:
			f.hellos = append(f.hellos, msg.GetHello().GetPublishToken())
			f.mu.Unlock()
			stream.Send(api.AttachResponse_builder{Welcome: api.AttachResponse_Welcome_builder{}.Build()}.Build())
			continue
		case msg.GetFeed() != nil:
			var ids []pdid.Id
			for _, raw := range msg.GetFeed().GetSources() {
				id, _ := pdid.From(raw)
				ids = append(ids, id)
			}
			f.feeds = append(f.feeds, ids)
			f.mu.Unlock()
			for _, id := range ids {
				stream.Send(api.AttachResponse_builder{Start: api.AttachResponse_Start_builder{SourceId: id.Bytes()}.Build()}.Build())
			}
			if f.stall && len(ids) > 0 {
				<-stream.Context().Done()
				return nil
			}
		case msg.GetData() != nil:
			d := msg.GetData()
			id, _ := pdid.From(d.GetSourceId())
			f.data = append(f.data, got{source: id, epoch: d.GetCaptureEpoch(), payload: d.GetPayload(), at: at, readAt: d.GetReadAt().AsTime()})
			f.mu.Unlock()
		default:
			f.mu.Unlock()
		}
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
}

func (f *fakeRelay) received() []got {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]got(nil), f.data...)
}

func (f *fakeRelay) lastFeed() []pdid.Id {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.feeds) == 0 {
		return nil
	}

	return f.feeds[len(f.feeds)-1]
}

// liveRig is a satellite's proxy with its live feeds, a producer it serves,
// one node, and the producer's relay.
type liveRig struct {
	*rig
	node  *node
	relay *fakeRelay
	live  *liveFeeds
	p     *producer
	src   pdid.Id
}

func newLiveRig(t *testing.T, stall bool) *liveRig {
	n := newNode(t)
	r := newRig(t, 256<<20, 64<<10, n)
	lr := &liveRig{rig: r, node: n, relay: newFakeRelay(t, stall), p: r.producer(true), src: pdid.New(8)}
	lr.live = newLiveFeeds(slog.New(slog.DiscardHandler))
	lr.live.dial = func(ctx context.Context, addr string, _ bool) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	lr.live.start(t.Context())
	t.Cleanup(func() { lr.live.set(nil) })
	r.proxy.live = lr.live
	lr.assignFeed(lr.src)

	return lr
}

// assignFeed is the CP's heartbeat answer: the producer's relay, with a feed
// token for these sources.
func (lr *liveRig) assignFeed(sources ...pdid.Id) {
	var ids [][]byte
	for _, id := range sources {
		ids = append(ids, id.Bytes())
	}
	relay := pdid.New(24)
	tok, err := lr.key.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(time.Now().Add(time.Hour)), Iat: timestamppb.Now(),
		Aud: relay.Bytes(), Op: api.TokenOp_TOKEN_OP_FEED, Sources: ids, Actor: lr.p.id.Bytes(), Feeder: pdid.New(DomSatellite).Bytes(),
	}.Build())
	require.NoError(lr.t, err)
	host, port, _ := net.SplitHostPort(lr.relay.addr)
	p, _ := strconv.Atoi(port)
	lr.live.set([]*api.SatelliteProducer{api.SatelliteProducer_builder{
		ProducerId: lr.p.id.Bytes(),
		Relay: api.RelayAssignment_builder{
			RelayId: relay.Bytes(), PublishToken: tok,
			Endpoints: []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(p)}.Build()},
		}.Build(),
	}.Build()})
	require.Eventually(lr.t, func() bool {
		lr.relay.mu.Lock()
		defer lr.relay.mu.Unlock()
		return len(lr.relay.hellos) > 0
	}, 5*time.Second, 10*time.Millisecond, "the satellite attaches to the relay")
}

// liveToken is the producer's put token for a live upload of the source.
func (lr *liveRig) liveToken(key string) string {
	return lr.token(lr.node.id, key, lr.p.id, func(c *api.TokenClaims) {
		c.SetMode(api.UploadMode_UPLOAD_MODE_LIVE)
		c.SetRecord(api.LaminaRecord_builder{SourceId: lr.src.Bytes()}.Build())
	})
}

// upload is a live upload in progress: its body is written as the test
// says, and its answer comes when it ends.
type upload struct {
	w    *io.PipeWriter
	done chan *http.Response
}

func (lr *liveRig) upload(key string, offset, from int64) *upload {
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, lr.srv.URL+"/"+key, pr)
	require.NoError(lr.t, err)
	req.ContentLength = -1
	req.Header.Set("Authorization", token.Scheme+" "+lr.liveToken(key))
	req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set(HdrLive, strconv.FormatInt(from, 10))
	u := &upload{w: pw, done: make(chan *http.Response, 1)}
	go func() {
		resp, err := lr.p.client.Do(req)
		if err != nil {
			u.done <- nil
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		u.done <- resp
	}()

	return u
}

func (u *upload) end(t *testing.T) *http.Response {
	u.w.Close()
	select {
	case resp := <-u.done:
		return resp
	case <-time.After(10 * time.Second):
		t.Fatal("the upload did not end")
		return nil
	}
}

// recording is av.mp4, a capture's stream: its init segment and fragments,
// a key fragment every 2 s.
func recording(t *testing.T) (*fmp4.Init, []*fmp4.Fragment) {
	b, err := os.ReadFile("../fmp4/testdata/av.mp4")
	require.NoError(t, err)
	var init *fmp4.Init
	var frags []*fmp4.Fragment
	s := &fmp4.Splitter{Unit: func(u *fmp4.Unit) {
		if u.Init != nil {
			init = u.Init
		} else {
			frags = append(frags, u.Frag)
		}
	}}
	s.Write(b)
	require.NoError(t, s.Err())

	return init, frags
}

// longRecording is av.mp4 played `times` times as one stream: the fragments
// numbered on, a key fragment every fourth.
func longRecording(t *testing.T, times int) (*fmp4.Init, [][]byte) {
	init, frags := recording(t)
	var out [][]byte
	for range times {
		// The thirteenth is the audio's tail, no video in it.
		for _, f := range frags[:12] {
			b := append([]byte(nil), f.Bytes...)
			i := bytes.Index(b, []byte("mfhd"))
			require.Positive(t, i)
			binary.BigEndian.PutUint32(b[i+8:], uint32(len(out)+1))
			out = append(out, b)
		}
	}

	return init, out
}

func seqOf(t *testing.T, init *fmp4.Init, b []byte) uint32 {
	f, err := fmp4.ParseFragment(b, init)
	require.NoError(t, err)

	return f.Seq
}

// waitData waits until the relay has n Data messages.
func (lr *liveRig) waitData(n int) []got {
	lr.t.Helper()
	require.Eventually(lr.t, func() bool { return len(lr.relay.received()) >= n }, 5*time.Second, 5*time.Millisecond, "the relay gets %d messages", n)

	return lr.relay.received()
}

// §41.8: the fragments of a live upload go to the relay as they pass, the
// init segment first and from a key fragment on, once the satellite said it
// feeds the source and the relay started it; a lamina that follows with
// the same init segment continues the feed, with nothing sent twice and
// nothing left out; the node gets every byte, and not the producer's word
// to the satellite.
func TestLiveFeedsTheRelayFromTheUpload(t *testing.T) {
	lr := newLiveRig(t, false)
	init, frags := recording(t)
	require.True(t, frags[0].Key(init) && frags[4].Key(init) && !frags[1].Key(init))

	// The first lamina: the init segment, then fragments as they come.
	u := lr.upload("laminae/a", 0, 0)
	u.w.Write(init.Bytes)
	u.w.Write(frags[0].Bytes)
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 1 }, 5*time.Second, 5*time.Millisecond, "the satellite says it feeds the source")
	require.Equal(t, lr.src, lr.relay.lastFeed()[0])
	// Started after the first fragment went by: from the next key fragment.
	time.Sleep(50 * time.Millisecond)
	for _, f := range frags[1:4] {
		u.w.Write(f.Bytes)
	}
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, lr.relay.received(), "nothing before a key fragment")
	for _, f := range frags[4:8] {
		u.w.Write(f.Bytes)
	}
	mfra := fmp4.Mfra(init.Video().ID, nil)
	u.w.Write(mfra)
	resp := u.end(t)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	vs := lr.waitData(5)
	require.Equal(t, init.Bytes, vs[0].payload, "the init segment first")
	for i, f := range frags[4:8] {
		require.Equal(t, f.Bytes, vs[i+1].payload, "fragment %d", 4+i)
		require.Equal(t, lr.src, vs[i+1].source)
	}

	// The next lamina, the same init segment: the feed goes on.
	u = lr.upload("laminae/b", 0, 0)
	u.w.Write(init.Bytes)
	for _, f := range frags[8:] {
		u.w.Write(f.Bytes)
	}
	u.w.Write(mfra)
	resp = u.end(t)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	vs = lr.waitData(5 + len(frags) - 8)
	require.Len(t, vs, 5+len(frags)-8, "no init segment again, nothing twice")
	for i, f := range frags[8:] {
		require.Equal(t, f.Bytes, vs[5+i].payload, "fragment %d", 8+i)
	}
	require.Equal(t, []pdid.Id{lr.src}, lr.relay.lastFeed(), "fed across the boundary")

	// The node got every byte, and nothing of what the producer told the
	// satellite.
	reqs := lr.node.requests()
	require.Len(t, reqs, 2)
	require.Empty(t, reqs[0].header.Get(HdrLive))
	var whole []byte
	whole = append(whole, init.Bytes...)
	for _, f := range frags[:8] {
		whole = append(whole, f.Bytes...)
	}
	require.Equal(t, append(whole, mfra...), reqs[0].body)

	// No live upload for liveIdle: the source is handed back.
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 0 }, liveIdle+2*time.Second, 50*time.Millisecond)
	require.Zero(t, lr.live.feeding())
	require.Positive(t, lr.live.bytes.Load())
}

// The bytes before the producer's live offset are a backlog, which goes to
// the node and not to the relay; the satellite feeds the source from there.
// A request that resumes where an earlier one broke continues what was read
// without sending it again, and one that broke hands the source back at
// once.
func TestLiveSkipsBacklogAndResumes(t *testing.T) {
	lr := newLiveRig(t, false)
	init, frags := longRecording(t, 2)
	require.Len(t, frags, 24)
	off := int64(len(init.Bytes))
	offsets := make([]int64, len(frags)+1)
	for i, f := range frags {
		offsets[i] = off
		off += int64(len(f))
	}
	offsets[len(frags)] = off
	settle := func() { time.Sleep(50 * time.Millisecond) }

	// Live from fragment 4: the four before it are old.
	u := lr.upload("laminae/c", 0, offsets[4])
	u.w.Write(init.Bytes)
	for _, f := range frags[:4] {
		u.w.Write(f)
	}
	settle()
	require.Empty(t, lr.relay.lastFeed(), "the backlog claims nothing")
	u.w.Write(frags[4])
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 1 }, 5*time.Second, 5*time.Millisecond, "fed from the live offset on")
	settle()
	for _, f := range frags[5:10] {
		u.w.Write(f)
	}
	vs := lr.waitData(3)
	require.Equal(t, init.Bytes, vs[0].payload)
	require.Equal(t, frags[8], vs[1].payload, "from the first key fragment the relay started it at")
	require.Equal(t, frags[9], vs[2].payload)

	// The request breaks mid-fragment: handed back at once.
	u.w.Write(frags[10][:100])
	u.w.CloseWithError(io.ErrUnexpectedEOF)
	<-u.done
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 0 }, 5*time.Second, 5*time.Millisecond, "a broken request hands the source back")

	// It resumes from the node's offset, behind what the satellite read:
	// what was read is not read again, and the feed goes on from the next
	// key fragment once the relay started the satellite again.
	u = lr.upload("laminae/c", offsets[7], offsets[7])
	for _, f := range frags[7:12] {
		u.w.Write(f)
	}
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 1 }, 5*time.Second, 5*time.Millisecond, "fed again")
	settle()
	for _, f := range frags[12:] {
		u.w.Write(f)
	}
	resp := u.end(t)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	vs = lr.waitData(3 + 1 + len(frags) - 12)
	require.Equal(t, init.Bytes, vs[3].payload, "the init segment first again")
	var seqs []uint32
	for _, v := range vs {
		if _, ok := fmp4.Find(v.payload, "moov"); !ok {
			seqs = append(seqs, seqOf(t, init, v.payload))
		}
	}
	require.Equal(t, []uint32{9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}, seqs, "nothing twice, from key fragments")
	require.Equal(t, frags[12], vs[4].payload)

	// The node has the lamina whole, as the two requests made it.
	reqs := lr.node.requests()
	require.Len(t, reqs, 2)
	require.Equal(t, offsets[12]-offsets[7]+offsets[len(frags)]-offsets[12], int64(len(reqs[1].body)))
}

// A request whose source the producer does not hand over, or that is not a
// live upload, or whose token the CP's feed does not name, feeds nothing.
func TestLiveOnlyWhatIsHandedOver(t *testing.T) {
	lr := newLiveRig(t, false)
	init, frags := recording(t)
	body := append([]byte(nil), init.Bytes...)
	for _, f := range frags {
		body = append(body, f.Bytes...)
	}
	put := func(key string, edit func(*http.Request), claims ...func(*api.TokenClaims)) {
		tok := lr.token(lr.node.id, key, lr.p.id, append([]func(*api.TokenClaims){func(c *api.TokenClaims) {
			c.SetMode(api.UploadMode_UPLOAD_MODE_LIVE)
			c.SetRecord(api.LaminaRecord_builder{SourceId: lr.src.Bytes()}.Build())
		}}, claims...)...)
		req, err := http.NewRequest(http.MethodPut, lr.srv.URL+"/"+key, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", token.Scheme+" "+tok)
		req.Header.Set("Upload-Offset", "0")
		req.Header.Set(HdrLive, "0")
		edit(req)
		resp, err := lr.p.client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}
	put("laminae/d", func(r *http.Request) { r.Header.Del(HdrLive) })
	put("laminae/e", func(*http.Request) {}, func(c *api.TokenClaims) { c.SetMode(api.UploadMode_UPLOAD_MODE_BUFFERED) })
	put("laminae/f", func(*http.Request) {}, func(c *api.TokenClaims) {
		c.SetRecord(api.LaminaRecord_builder{SourceId: pdid.New(8).Bytes()}.Build())
	})
	put("laminae/g", func(*http.Request) {}, func(c *api.TokenClaims) {
		c.SetRecord(api.LaminaRecord_builder{SourceId: lr.src.Bytes(), CaptureEpoch: "another"}.Build())
	})
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, lr.relay.received())
	lr.relay.mu.Lock()
	defer lr.relay.mu.Unlock()
	for _, f := range lr.relay.feeds {
		require.Empty(t, f)
	}
}

// §41.8: a relay that is behind drops live, counted, and never holds the
// upload: every byte reaches the node as fast as without it.
func TestLiveSlowRelayNeverHoldsTheUpload(t *testing.T) {
	lr := newLiveRig(t, true)
	init, frags := recording(t)
	u := lr.upload("laminae/h", 0, 0)
	u.w.Write(init.Bytes)
	sent := len(init.Bytes)
	began := time.Now()
	// About 30 MB: the recording again and again, as fast as it goes.
	for range 50 {
		for _, f := range frags {
			_, err := u.w.Write(f.Bytes)
			require.NoError(t, err)
			sent += len(f.Bytes)
		}
	}
	resp := u.end(t)
	took := time.Since(began)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Positive(t, lr.live.dropped.Load(), "a relay that is behind drops live")
	reqs := lr.node.requests()
	require.Len(t, reqs, 1)
	require.Len(t, reqs[0].body, sent, "the node got every byte")
	require.Less(t, took, 5*time.Second, "the upload was not held: %s", took)
	t.Logf("%d MB through in %s with the relay stalled, %d fragments dropped", sent>>20, took, lr.live.dropped.Load())
}

// §41.8: a fragment goes to the relay as soon as its last byte passes, not
// with the next one: what the satellite adds to live is the time it takes
// to read it.
func TestLiveAddsNoBuffering(t *testing.T) {
	lr := newLiveRig(t, false)
	init, frags := recording(t)
	u := lr.upload("laminae/i", 0, 0)
	defer u.end(t)
	u.w.Write(init.Bytes)
	u.w.Write(frags[0].Bytes[:len(frags[0].Bytes)-1])
	require.Eventually(t, func() bool { return len(lr.relay.lastFeed()) == 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	u.w.Write(frags[0].Bytes[len(frags[0].Bytes)-1:])
	for _, f := range frags[1:4] {
		u.w.Write(f.Bytes)
	}
	// Key fragment 4 is the first the feed sends; each after it goes alone.
	var worst time.Duration
	for i, f := range frags[4:] {
		wrote := time.Now()
		u.w.Write(f.Bytes)
		vs := lr.waitData(i + 2)
		last := vs[len(vs)-1]
		require.Equal(t, f.Bytes, last.payload, "the fragment just written, not later")
		worst = max(worst, last.at.Sub(wrote))
	}
	t.Logf("from a fragment's last byte into the satellite to the relay: at most %s", worst)
	require.Less(t, worst, 250*time.Millisecond)
}
