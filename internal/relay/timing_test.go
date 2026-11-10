package relay

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// The producer's queue and the uplink are measured from the stamps a
// payload brings (§39.3): only on one the source took, an uplink below zero
// is a clock skew and not a time, and a producer that sends no stamps
// measures nothing.
func TestIngestTiming(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	r := &Relay{m: newMetrics(ctx), log: slog.New(slog.DiscardHandler)}
	s := newSource(r, pdid.New(pdid.Domain(8)))

	t1 := time.Now()
	s.feedTimed(nil, "", []byte("x"), stamps{read: t1, sent: t1.Add(40 * time.Millisecond), arrived: t1.Add(140 * time.Millisecond), producer: "p"})
	// The producer's clock 50 ms ahead of the relay's, over a 10 ms uplink.
	s.feedTimed(nil, "", []byte("x"), stamps{read: t1, sent: t1.Add(60 * time.Millisecond), arrived: t1.Add(20 * time.Millisecond), producer: "p"})
	// A producer that predates the stamps.
	s.feedEpoch(nil, "", []byte("x"))
	// A payload of another epoch the source does not take.
	s.feedTimed(nil, "other", []byte("x"), stamps{read: t1, sent: t1, arrived: t1})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	hist := map[string]metricdata.HistogramDataPoint[float64]{}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				hist[m.Name] = d.DataPoints[0]
			case metricdata.Sum[int64]:
				sums[m.Name] = d.DataPoints[0].Value
			}
		}
	}
	queue := hist["shale.relay.producer_queue_ms"]
	require.Equal(t, uint64(2), queue.Count)
	require.InDelta(t, 100, queue.Sum, 1e-9, "40 and 60 ms")
	by, _ := queue.Attributes.Value("producer")
	require.Equal(t, "p", by.AsString(), "per producer host")
	uplink := hist["shale.relay.uplink_ms"]
	require.Equal(t, uint64(1), uplink.Count, "the skewed one is not a time")
	require.InDelta(t, 100, uplink.Sum, 1e-9)
	require.Equal(t, int64(1), sums["shale.relay.uplink_skewed"])
}

// A timing SEI goes before an access unit's first slice, after the
// parameter sets, and reads back as written, zeros and all, which need
// emulation prevention bytes (§39.4).
func TestTimingSEI(t *testing.T) {
	start := []byte{0, 0, 0, 1}
	unit := func(header ...byte) []byte {
		return append(append(append([]byte(nil), start...), header...), 0xaa, 0xbb)
	}
	t0 := time.Unix(1_800_000_000, 123_000_000)
	ft := frameTiming{media: 0, read: t0, sent: time.Time{}, arrived: t0.Add(30 * time.Millisecond), relayed: t0.Add(250 * time.Millisecond)}

	for _, c := range []struct {
		name  string
		h265  bool
		head  [][]byte
		slice []byte
	}{
		{"H.264", false, [][]byte{{0x09, 0x10}, {0x67}, {0x68}}, []byte{0x65}},
		{"H.265", true, [][]byte{{0x40, 0x01}, {0x42, 0x01}, {0x44, 0x01}}, []byte{0x26, 0x01}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var au, head []byte
			for _, h := range c.head {
				head = append(head, unit(h...)...)
			}
			au = append(append(au, head...), unit(c.slice...)...)

			got := withSEI(au, timingSEI(c.h265, ft), c.h265)
			require.Equal(t, head, got[:len(head)], "the parameter sets first")
			sei := timingSEI(c.h265, ft)
			require.Equal(t, sei, got[len(head):len(head)+len(sei)], "then the SEI")
			require.Equal(t, unit(c.slice...), got[len(head)+len(sei):], "then the slice")

			back, ok := readTiming(c.h265, got)
			require.True(t, ok)
			require.Equal(t, ft.media, back.media)
			require.True(t, back.read.Equal(ft.read))
			require.True(t, back.sent.IsZero(), "unknown stays unknown")
			require.True(t, back.arrived.Equal(ft.arrived))
			require.True(t, back.relayed.Equal(ft.relayed))

			_, ok = readTiming(c.h265, au)
			require.False(t, ok, "none in an access unit without one")
		})
	}
}

// No start code survives inside an escaped payload, and unescaping gives
// the payload back.
func TestEscape(t *testing.T) {
	raw := []byte{0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4, 9}
	esc := escape(raw)
	for i := 0; i+2 < len(esc); i++ {
		require.False(t, esc[i] == 0 && esc[i+1] == 0 && esc[i+2] <= 2, "a start code or a reserved one at %d", i)
	}
	require.Equal(t, raw, unescape(esc))
}

// A timed session says so in its answer; its viewer reports on it with the
// session's view token, and is told the relay's clock (§39.4). A report on
// another actor's session, or one that is not JSON, records nothing.
func TestWhepTimingReport(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	r, err := New(Config{StateDir: t.TempDir(), MaxViewers: 10, ViewersPerActor: 10})
	require.NoError(t, err)
	r.m = newMetrics(ctx)
	w, err := newWhepServer(r)
	require.NoError(t, err)
	k, err := token.Generate("k-timing")
	require.NoError(t, err)
	r.keys.Verifier.Add("k-timing", k.Public())

	source := pdid.New(pdid.Domain(8))
	actor := pdid.New(pdid.Domain(2))
	sign := func(actor pdid.Id) string {
		now := time.Now()
		s, err := k.Sign(api.TokenClaims_builder{
			Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
			Aud: r.id.Bytes(), Op: api.TokenOp_TOKEN_OP_VIEW, Source: source.Bytes(), Actor: actor.Bytes(),
		}.Build())
		require.NoError(t, err)
		return s
	}
	do := func(method, path, tok, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", token.Scheme+" "+tok)
		rec := httptest.NewRecorder()
		w.ServeHTTP(rec, req)
		return rec
	}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)
	offer, err := pc.CreateOffer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(offer))
	<-gathered
	rec := do(http.MethodPost, "/whep/"+source.String()+"?timing=1", sign(actor), pc.LocalDescription().SDP)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, "1", rec.Header().Get("Shale-Timing"))
	session := rec.Header().Get("Location")
	defer do(http.MethodDelete, session, "", "")

	before := time.Now()
	rec = do(http.MethodPost, session+"/timing", sign(actor), `{"frames":[{"network":12.5,"playout":40,"total":900},{"network":-1.5,"total":1e9}]}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var clock map[string]float64
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &clock))
	require.GreaterOrEqual(t, clock["received"], float64(before.UnixMicro())/1000)
	require.GreaterOrEqual(t, clock["replied"], clock["received"])
	require.Equal(t, http.StatusOK, do(http.MethodPost, session+"/timing", sign(actor), "").Code, "an empty report measures the clock")

	require.Equal(t, http.StatusForbidden, do(http.MethodPost, session+"/timing", sign(pdid.New(pdid.Domain(2))), `{"frames":[{"total":1}]}`).Code)
	require.Equal(t, http.StatusBadRequest, do(http.MethodPost, session+"/timing", sign(actor), `frames`).Code)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	got := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Histogram[float64]); ok {
				got[m.Name] = d.DataPoints[0]
			}
		}
	}
	require.Equal(t, uint64(2), got["shale.relay.viewer_network_ms"].Count)
	require.InDelta(t, 12.5, got["shale.relay.viewer_network_ms"].Sum, 1e-9, "a little below zero is none")
	require.Equal(t, uint64(1), got["shale.relay.viewer_playout_ms"].Count)
	require.Equal(t, uint64(1), got["shale.relay.viewer_total_ms"].Count, "a span past a minute is not live")
}

// The timing SEI the console reads (ts/src/sei.ts) is the one written here:
// testdata/timing-au.hex is an H.264 access unit with one, which this keeps
// current and ts/test/sei.mjs reads.
func TestTimingSEIGolden(t *testing.T) {
	t0 := time.Unix(1_800_000_000, 123_456_789)
	ft := frameTiming{media: 3*time.Second + 33_366_667, read: t0, sent: t0.Add(2 * time.Millisecond), arrived: t0.Add(140 * time.Millisecond), relayed: t0.Add(390 * time.Millisecond)}
	au := []byte{0, 0, 0, 1, 0x09, 0x10, 0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x68, 0xce, 0, 0, 0, 1, 0x65, 0x88, 0x84}
	got := hex.EncodeToString(withSEI(au, timingSEI(false, ft), false)) + "\n"
	want, err := os.ReadFile("testdata/timing-au.hex")
	if os.IsNotExist(err) || os.Getenv("SHALE_UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile("testdata/timing-au.hex", []byte(got), 0o644))
		return
	}
	require.NoError(t, err)
	require.Equal(t, string(want), got)
}
