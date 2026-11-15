package producer

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/satellite"
	"github.com/lesomnus/shale/internal/storage"
)

func TestParseVia(t *testing.T) {
	for in, want := range map[string]Via{"": ViaDirect, "direct": ViaDirect, " Satellite ": ViaSatellite, "auto": ViaAuto} {
		v, err := ParseVia(in)
		require.NoError(t, err, in)
		require.Equal(t, want, v, in)
	}
	_, err := ParseVia("relay")
	require.Error(t, err)
}

// host is an upload endpoint that stores what it is sent: a node, or a
// satellite that answers as the node behind it would. answer may say
// otherwise for a request.
type host struct {
	srv    *httptest.Server
	puts   atomic.Int32
	heads  atomic.Int32
	mu     sync.Mutex
	got    []byte
	header http.Header
	answer func(n int32, w http.ResponseWriter) bool
}

func newHost(t *testing.T) *host {
	h := &host{}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			h.heads.Add(1)
			h.mu.Lock()
			w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(h.got)))
			h.mu.Unlock()
			w.Header().Set(storage.HdrUploadComplete, "?0")
			w.WriteHeader(http.StatusOK)
			return
		}
		n := h.puts.Add(1)
		if h.answer != nil && h.answer(n, w) {
			io.Copy(io.Discard, r.Body)
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		h.mu.Lock()
		h.got = append(h.got, b...)
		h.header = r.Header.Clone()
		h.mu.Unlock()
		w.Header().Set(storage.HdrUploadComplete, "?1")
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(h.srv.Close)

	return h
}

func (h *host) endpoint() *api.Endpoint {
	host, port, _ := net.SplitHostPort(h.srv.Listener.Addr().String())
	p, _ := strconv.Atoi(port)

	return api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(p)}.Build()
}

func assignment(ep *api.Endpoint, expires time.Time) *api.SatelliteAssignment {
	return api.SatelliteAssignment_builder{
		SatelliteId: pdid.New(43).Bytes(),
		Endpoints:   []*api.Endpoint{ep},
		DateExpires: timestamppb.New(expires),
	}.Build()
}

// reports counts the attempts reported failed, which a satellite's own
// failure must not be.
type reports struct {
	noLaminae
	n atomic.Int32
}

func (r *reports) ReportAttempt(ctx context.Context, req *api.LaminaReportAttemptRequest, opts ...grpc.CallOption) (*api.Attempt, error) {
	r.n.Add(1)
	return r.noLaminae.ReportAttempt(ctx, req, opts...)
}

func closedSegment(body string) *Segment {
	seg := NewSegment(time.Now(), []byte(body))
	seg.Close(time.Now())

	return seg
}

func uploaderVia(link *SatelliteLink, laminae api.LaminaServiceClient) *Uploader {
	return &Uploader{Cfg: UploadConfig{ResumeTimeout: 30 * time.Second}, Laminae: laminae, Log: slog.New(slog.DiscardHandler), Satellite: link}
}

// Through a satellite, a request is the same request -- the path, the
// token -- sent to the satellite instead of the node; in development mode
// it names the producer. Directly is the default, and what `satellite`
// does while no satellite is assigned, or the assignment has run out.
func TestUploadGoesThroughTheSatellite(t *testing.T) {
	node, sat := newHost(t), newHost(t)
	al := allocationAt(t, node.srv.Listener.Addr().String(), "laminae/through")
	me := pdid.New(DomProducer)

	link := NewSatelliteLink(ViaSatellite, true, slog.New(slog.DiscardHandler))
	link.producer = me
	link.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
	res := uploaderVia(link, noLaminae{}).Upload(context.Background(), al, closedSegment("through"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, int32(1), sat.puts.Load())
	require.Zero(t, node.puts.Load())
	require.Equal(t, "through", string(sat.got))
	require.Equal(t, "Shale tok", sat.header.Get("Authorization"))
	require.Equal(t, me.String(), sat.header.Get(satellite.HdrProducer))
	rep := link.report()
	require.Equal(t, "satellite", rep.GetVia())
	require.Equal(t, int64(1), rep.GetThroughSatellite())
	require.Zero(t, rep.GetDirect())

	// None assigned: directly.
	link.Set(nil)
	res = uploaderVia(link, noLaminae{}).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/none"), closedSegment("none"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, int32(1), node.puts.Load())
	require.Empty(t, node.header.Get(satellite.HdrProducer))

	// Run out: directly.
	link.Set(assignment(sat.endpoint(), time.Now().Add(-time.Second)))
	res = uploaderVia(link, noLaminae{}).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/expired"), closedSegment("expired"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, int32(2), node.puts.Load())
	require.Equal(t, int32(1), sat.puts.Load())
	require.Equal(t, int64(2), link.report().GetDirect())

	// `direct` takes no assignment, and reports nothing.
	direct := NewSatelliteLink(ViaDirect, true, slog.New(slog.DiscardHandler))
	direct.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
	res = uploaderVia(direct, noLaminae{}).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/direct"), closedSegment("direct"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, int32(3), node.puts.Load())
	require.Nil(t, direct.report())
	require.False(t, direct.wanted())
}

// On `auto`, a satellite that cannot be reached is left for the node at
// once, and for a while after: the candidate is not failed for it, nor
// reported. A direct request that fails as well brings the satellite back
// at once, since the producer's own link may be what failed.
func TestUploadAutoFallsBackToTheNode(t *testing.T) {
	node := newHost(t)
	gone, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host, port, _ := net.SplitHostPort(gone.Addr().String())
	gone.Close()
	p, _ := strconv.Atoi(port)
	dead := api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(p)}.Build()

	link := NewSatelliteLink(ViaAuto, true, slog.New(slog.DiscardHandler))
	link.Set(assignment(dead, time.Now().Add(time.Hour)))
	woke := 0
	link.wake = func() { woke++ }
	rep := &reports{}
	u := uploaderVia(link, rep)
	res := u.Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/auto"), closedSegment("auto"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, 1, res.Attempts, "the same candidate")
	require.Zero(t, rep.n.Load(), "nothing reported against the node")
	require.Equal(t, "auto", string(node.got))
	require.Equal(t, 1, woke)
	r := link.report()
	require.Equal(t, int64(1), r.GetFallbacks())
	require.Equal(t, int64(1), r.GetThroughSatellite())
	require.Equal(t, int64(1), r.GetDirect())

	// Paused: directly from the first request.
	_, through := link.route(node.endpoint())
	require.False(t, through)
	link.directFailed()
	_, through = link.route(node.endpoint())
	require.True(t, through, "the satellite again once the node failed too")

	// The pause doubles with every failure in a row.
	now := time.Now()
	link.now = func() time.Time { return now }
	link.failed(io.EOF)
	require.Equal(t, now.Add(2*FirstSatellitePause), link.paused)
	now = now.Add(3 * FirstSatellitePause)
	link.succeeded()
	link.failed(io.EOF)
	require.Equal(t, now.Add(FirstSatellitePause), link.paused)
}

// A satellite that refuses this producer is the satellite's failure, not
// the node's: on `satellite` the request is asked again, which the
// satellite takes once it has heard of the producer; on `auto` it goes to
// the node. Neither fails the candidate.
func TestUploadSatelliteRefusalIsNotTheNodes(t *testing.T) {
	for _, via := range []Via{ViaSatellite, ViaAuto} {
		t.Run(string(via), func(t *testing.T) {
			node, sat := newHost(t), newHost(t)
			sat.answer = func(n int32, w http.ResponseWriter) bool {
				if n > 1 {
					return false
				}
				w.Header().Set(satellite.HdrSatellite, satellite.ReasonUnassigned)
				http.Error(w, "not a producer this satellite serves", http.StatusForbidden)
				return true
			}
			link := NewSatelliteLink(via, true, slog.New(slog.DiscardHandler))
			link.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
			rep := &reports{}
			res := uploaderVia(link, rep).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/refused"), closedSegment("refused"))
			require.True(t, res.Stored, "%v", res.Err)
			require.Equal(t, 1, res.Attempts)
			require.Zero(t, rep.n.Load())
			if via == ViaAuto {
				require.Equal(t, "refused", string(node.got))
				require.Equal(t, int32(1), sat.puts.Load())
			} else {
				require.Equal(t, "refused", string(sat.got))
				require.Zero(t, node.puts.Load())
			}
		})
	}
}

// A node's answer that comes through the satellite is the node's, handled
// as it always was: a 503 is waited out on the same target, through the
// same satellite, and a 403 of the node's fails the candidate.
func TestUploadThroughTheSatelliteKeepsTheNodesAnswers(t *testing.T) {
	node, sat := newHost(t), newHost(t)
	sat.answer = func(n int32, w http.ResponseWriter) bool {
		if n > 1 {
			return false
		}
		w.Header().Set(storage.HdrRetryAfter, "1")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return true
	}
	link := NewSatelliteLink(ViaAuto, true, slog.New(slog.DiscardHandler))
	link.Set(assignment(sat.endpoint(), time.Now().Add(time.Hour)))
	res := uploaderVia(link, noLaminae{}).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/busy"), closedSegment("busy"))
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, int32(2), sat.puts.Load())
	require.Zero(t, node.puts.Load())
	require.Zero(t, link.report().GetFallbacks())

	sat.answer = func(_ int32, w http.ResponseWriter) bool {
		http.Error(w, "the token names another key", http.StatusForbidden)
		return true
	}
	rep := &reports{}
	res = uploaderVia(link, rep).Upload(context.Background(), allocationAt(t, node.srv.Listener.Addr().String(), "laminae/forbidden"), closedSegment("forbidden"))
	require.False(t, res.Stored)
	require.Equal(t, int32(1), rep.n.Load(), "the node's refusal fails the candidate")
	require.Zero(t, node.puts.Load())
}
