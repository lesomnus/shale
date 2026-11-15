package producer

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/upload"
)

// keeper is a satellite under `retain: accepted` as far as a producer can
// tell (§41.9): it takes what it is sent and says how much it holds, or,
// told to, passes requests through as the node, or stops answering.
type keeper struct {
	srv *httptest.Server

	mu       sync.Mutex
	got      []byte
	handoffs int
	retains  int
	puts     int
	pass     string
	// dieAfter stops it answering after that many PUTs; 0 never.
	dieAfter int
}

func newKeeper(t *testing.T) *keeper {
	k := &keeper{}
	k.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.mu.Lock()
		defer k.mu.Unlock()
		if r.Method == http.MethodHead {
			if k.dieAfter > 0 && k.puts >= k.dieAfter {
				hijackClose(w)
				return
			}
			w.Header().Set(upload.HdrAccepted, strconv.Itoa(len(k.got)))
			w.WriteHeader(http.StatusOK)
			return
		}
		k.puts++
		if k.dieAfter > 0 && k.puts > k.dieAfter {
			hijackClose(w)
			return
		}
		if r.Header.Get(upload.HdrRetain) == upload.RetainAccepted {
			k.retains++
		}
		if r.Header.Get(upload.HdrHandoff) != "" {
			hd, err := upload.DecodeHandoff(r.Header.Get(upload.HdrHandoff))
			if err == nil && len(hd.GetAllocation().GetCandidates()) > 0 {
				k.handoffs++
			}
		}
		off, _ := strconv.Atoi(r.Header.Get(storage.HdrUploadOffset))
		if off != len(k.got) {
			w.Header().Set(upload.HdrAccepted, strconv.Itoa(len(k.got)))
			w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(k.got)))
			w.WriteHeader(http.StatusConflict)
			return
		}
		b, _ := io.ReadAll(r.Body)
		k.got = append(k.got, b...)
		complete := r.Header.Get(storage.HdrUploadComplete) == "?1"
		switch {
		case k.pass != "":
			// The node's answers, marked passed through.
			w.Header().Set(upload.HdrPass, k.pass)
			w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(k.got)))
			if complete {
				w.Header().Set(storage.HdrUploadComplete, "?1")
				w.WriteHeader(http.StatusCreated)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case complete:
			w.Header().Set(upload.HdrAccepted, strconv.Itoa(len(k.got)))
			w.WriteHeader(http.StatusAccepted)
		default:
			w.Header().Set(upload.HdrAccepted, strconv.Itoa(len(k.got)))
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(k.srv.Close)

	return k
}

func hijackClose(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			c.Close()
		}
	}
}

// accepting is an uploader under `retain: accepted` through the keeper,
// for an allocation whose node is node.
func accepting(t *testing.T, k *keeper, part int64) *Uploader {
	// Development mode: the keeper is plain http.
	link := NewSatelliteLink(ViaSatellite, true, slog.New(slog.DiscardHandler))
	host := &host{srv: k.srv}
	a := assignment(host.endpoint(), time.Now().Add(time.Hour))
	a.SetAccept(true)
	link.Set(a)
	u := uploaderVia(link, noLaminae{})
	u.Accepted = true
	u.Cfg.AcceptPart = part
	u.Cfg.ResumeTimeout = time.Second

	return u
}

// liveSegment is a segment the test writes into while it is uploaded:
// n writes of size bytes, then it closes.
func liveSegment(n, size int) (*Segment, []byte) {
	seg := NewSegment(time.Now(), nil)
	var all []byte
	for i := 0; i < n; i++ {
		for j := 0; j < size; j++ {
			all = append(all, byte(i))
		}
	}
	go func() {
		for i := 0; i < n; i++ {
			seg.Write(all[i*size : (i+1)*size])
			time.Sleep(5 * time.Millisecond)
		}
		seg.Close(time.Now())
	}()

	return seg, all
}

// §41.9: a live segment goes to the satellite in parts; the producer lets
// go of what the satellite says it holds as it says it, and once it holds
// the whole segment the segment is handed off. The allocation goes with
// the first request only.
func TestAcceptedHandsOff(t *testing.T) {
	node := newHost(t)
	k := newKeeper(t)
	u := accepting(t, k, 4000)
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/handed")
	seg, want := liveSegment(20, 1000)

	res := u.Upload(context.Background(), al, seg)
	require.NoError(t, res.Err)
	require.True(t, res.HandedOff)
	require.False(t, res.Stored, "stored is the satellite's to say, not the producer's")
	require.Equal(t, want, k.got)
	require.Zero(t, seg.Held(), "the producer holds nothing of it")
	require.Equal(t, seg.Len(), seg.Released())
	require.Equal(t, 1, k.handoffs, "the allocation with the first request")
	require.Greater(t, k.puts, 2, "in parts")
	require.Equal(t, k.puts, k.retains)
	require.Zero(t, node.puts.Load(), "the node is the satellite's to reach")
}

// A satellite that passes the segment through (full, or refusing
// `accepted`) answers as the node: the producer keeps every byte, as under
// `committed`, and the segment goes on that way, counted (§41.9).
func TestAcceptedPassedGoesAsCommitted(t *testing.T) {
	node := newHost(t)
	k := newKeeper(t)
	k.pass = upload.PassFull
	u := accepting(t, k, 1<<20)
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/passed")

	seg := closedSegment("all of it")
	res := u.Upload(context.Background(), al, seg)
	require.NoError(t, res.Err)
	require.True(t, res.Stored)
	require.True(t, res.Passed)
	require.False(t, res.HandedOff)
	require.Zero(t, seg.Released(), "nothing let go")
	require.Equal(t, int64(1), u.passed.Load())
	require.Equal(t, int64(1), u.Satellite.report().GetPassed())
	require.Equal(t, "all of it", string(k.got))

	// A live one: the rest goes as under committed, from where the node
	// stands, the producer holding all of it until the node commits.
	k.got, k.puts, k.retains = nil, 0, 0
	seg, want := liveSegment(10, 1000)
	res = u.Upload(context.Background(), al, seg)
	require.NoError(t, res.Err)
	require.True(t, res.Stored)
	require.True(t, res.Passed)
	require.Zero(t, seg.Released())
	require.Equal(t, want, k.got)
	require.Equal(t, 1, k.retains, "only the first request asked")
}

// Without a satellite `accepted` is `committed`: the segment goes to the
// node, and the producer holds it until the node commits.
func TestAcceptedWithoutSatelliteIsCommitted(t *testing.T) {
	node := newHost(t)
	u := uploaderVia(NewSatelliteLink(ViaDirect, false, slog.New(slog.DiscardHandler)), noLaminae{})
	u.Accepted = true
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/plain")
	seg := closedSegment("direct")
	res := u.Upload(context.Background(), al, seg)
	require.True(t, res.Stored, "%v", res.Err)
	require.False(t, res.HandedOff)
	require.Zero(t, seg.Released())
	require.Empty(t, node.header.Get(upload.HdrRetain), "a node is never asked")
}

// A satellite the CP does not say takes `accepted`, one of an earlier
// release say, is sent no handoff: it would pass the other candidates'
// tokens on to the node. The segment goes through it as under `committed`.
func TestAcceptedOnlyToASatelliteThatTakesIt(t *testing.T) {
	node := newHost(t)
	k := newKeeper(t)
	u := accepting(t, k, 4000)
	a := assignment((&host{srv: k.srv}).endpoint(), time.Now().Add(time.Hour))
	u.Satellite.Set(a)
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/old")
	k.pass = upload.PassRefused // as a node would answer, through it

	seg := closedSegment("old satellite")
	res := u.Upload(context.Background(), al, seg)
	require.True(t, res.Stored, "%v", res.Err)
	require.False(t, res.HandedOff)
	require.Zero(t, k.retains, "not asked")
	require.Zero(t, k.handoffs, "no handoff sent")
}

// A satellite that dies holding the start of a segment: the producer let
// those bytes go and cannot send them anywhere else, so after
// resume_timeout the segment is cut short at what the satellite held
// (§41.9).
func TestAcceptedCutWhenTheSatelliteDies(t *testing.T) {
	node := newHost(t)
	k := newKeeper(t)
	k.dieAfter = 2
	u := accepting(t, k, 2000)
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/cut")
	seg, _ := liveSegment(20, 1000)

	res := u.Upload(context.Background(), al, seg)
	require.True(t, res.Cut, "%+v", res)
	require.False(t, res.HandedOff)
	require.Positive(t, seg.Released(), "the start was let go")
	require.Zero(t, node.puts.Load(), "the rest is not sent elsewhere")
}
