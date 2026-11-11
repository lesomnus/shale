package storage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/lesomnus/payday/pdid"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

type timedNode struct {
	n      *Node
	reader *sdkmetric.ManualReader
	ctx    context.Context
	sign   func(key string, op api.TokenOp) string
}

func newTimedNode(t *testing.T) timedNode {
	t.Helper()
	dir := t.TempDir()
	n, err := New(Config{StateDir: dir, Log: slog.New(slog.DiscardHandler), Limits: DefaultLimits})
	require.NoError(t, err)
	s, err := OpenSink(SinkConfig{Path: filepath.Join(dir, "sink"), Capacity: 1 << 30, Device: "disk-timing"}, DefaultWatermarks)
	require.NoError(t, err)
	n.sinks = append(n.sinks, s)
	n.byId[s.Id] = s
	n.id = pdid.New(DomNode)
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	n.m = newMetrics(ctx)
	k, err := token.Generate("k-timing")
	require.NoError(t, err)
	n.verifier.Add("k-timing", k.Public())
	sign := func(key string, op api.TokenOp) string {
		lamina, attempt := pdid.New(pdid.Domain(9)), pdid.New(pdid.Domain(10))
		now := time.Now()
		tok, err := k.Sign(api.TokenClaims_builder{
			Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
			Aud: n.id.Bytes(), Op: op, SinkId: s.Id.Bytes(), LaminaKey: key, AttemptId: attempt.Bytes(),
			Record: api.LaminaRecord_builder{
				FormatVersion: FormatVersion, LaminaId: lamina.Bytes(), AttemptId: attempt.Bytes(),
				DateStartedMs: now.UnixMilli(), SizeHint: 4 << 20,
			}.Build(),
			MaxLength: 8 << 20, Mode: api.UploadMode_UPLOAD_MODE_LIVE,
			IdleTimeoutSeconds: 5, AbandonTimeoutSeconds: 10,
		}.Build())
		require.NoError(t, err)
		return tok
	}
	return timedNode{n: n, reader: reader, ctx: ctx, sign: sign}
}

func (tn timedNode) do(method, path, tok string, hdr map[string]string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://node"+path, bytes.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", token.Scheme+" "+tok)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	tn.n.dp.ServeHTTP(rec, req)
	return rec
}

func (tn timedNode) collect(t *testing.T) (map[string]metricdata.HistogramDataPoint[float64], map[string]int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, tn.reader.Collect(tn.ctx, &rm))
	hist := map[string]metricdata.HistogramDataPoint[float64]{}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				hist[m.Name] = d.DataPoints[0]
			case metricdata.Sum[int64]:
				var v int64
				for _, p := range d.DataPoints {
					v += p.Value
				}
				sums[m.Name] = v
			}
		}
	}
	return hist, sums
}

// One read's time splits into the device's wait and read and the rest,
// and the node counts what it read beside what it sent: a short range
// costs a whole aligned chunk of the device.
func TestReadTimingSplitsDeviceFromSending(t *testing.T) {
	tn := newTimedNode(t)
	key := fmt.Sprintf("laminae/2026/11/11/12/%s", pdid.New(pdid.Domain(9)).String())
	body := bytes.Repeat([]byte{0x42}, 3<<20)
	put := tn.do(http.MethodPut, "/"+key, tn.sign(key, api.TokenOp_TOKEN_OP_PUT), map[string]string{HdrUploadOffset: "0", HdrUploadComplete: "?1"}, body)
	require.Less(t, put.Code, 300, put.Body.String())

	got := tn.do(http.MethodGet, "/"+key, tn.sign(key, api.TokenOp_TOKEN_OP_GET), map[string]string{"Range": "bytes=0-1048575"}, nil)
	require.Equal(t, http.StatusPartialContent, got.Code)
	require.Equal(t, 1<<20, got.Body.Len())

	hist, sums := tn.collect(t)
	for _, name := range []string{"shale.node.read_wait_ms", "shale.node.read_device_ms", "shale.node.read_send_ms"} {
		require.Equal(t, uint64(1), hist[name].Count, name)
		require.GreaterOrEqual(t, hist[name].Sum, 0.0, name)
	}
	require.InDelta(t, hist["shale.node.read_latency_ms"].Sum, hist["shale.node.read_wait_ms"].Sum+hist["shale.node.read_device_ms"].Sum+hist["shale.node.read_send_ms"].Sum, 0.01)
	require.Equal(t, int64(1<<20), sums["shale.node.read_sent_bytes"])
	// The lamina is shorter than one read chunk: its whole length was read.
	require.GreaterOrEqual(t, sums["shale.node.read_device_bytes"], int64(len(body)))
}

func TestExportReport(t *testing.T) {
	tn := newTimedNode(t)
	key := fmt.Sprintf("laminae/2026/11/11/12/%s", pdid.New(pdid.Domain(9)).String())
	get := tn.sign(key, api.TokenOp_TOKEN_OP_GET)
	report := `{"laminae":2,"requests":40,"bytes":40000000,"total_ms":12000,"index_ms":300,"fetch_ms":10000,"first_byte_ms":6000,"body_ms":3500,"assemble_ms":900}`

	pre := tn.do(http.MethodOptions, "/reports/export", "", map[string]string{"Origin": "https://cp.example", "Access-Control-Request-Method": "POST"}, nil)
	require.Equal(t, http.StatusNoContent, pre.Code)
	require.Equal(t, "*", pre.Header().Get("Access-Control-Allow-Origin"))
	require.Contains(t, pre.Header().Get("Access-Control-Allow-Methods"), "POST")

	ok := tn.do(http.MethodPost, "/reports/export?token="+get, "", nil, []byte(report))
	require.Equal(t, http.StatusNoContent, ok.Code, ok.Body.String())
	require.Equal(t, "*", ok.Header().Get("Access-Control-Allow-Origin"))

	hist, sums := tn.collect(t)
	require.InDelta(t, 12000, hist["shale.node.export_total_ms"].Sum, 1e-9)
	require.InDelta(t, 6000, hist["shale.node.export_first_byte_ms"].Sum, 1e-9)
	require.InDelta(t, 500, hist["shale.node.export_gap_ms"].Sum, 1e-9, "what fetching spent outside requests")
	require.InDelta(t, 32, hist["shale.node.export_rate_mbps"].Sum, 1e-9, "40 MB over 10 s")
	require.Equal(t, int64(40000000), sums["shale.node.export_bytes"])

	for name, c := range map[string]struct {
		tok  string
		body string
		code int
	}{
		"no token":       {"", report, http.StatusUnauthorized},
		"upload token":   {tn.sign(key, api.TokenOp_TOKEN_OP_PUT), report, http.StatusForbidden},
		"not json":       {get, "nope", http.StatusBadRequest},
		"negative":       {get, strings.Replace(report, `"total_ms":12000`, `"total_ms":-1`, 1), http.StatusBadRequest},
		"too long":       {get, strings.Replace(report, `"total_ms":12000`, `"total_ms":1e12`, 1), http.StatusBadRequest},
		"negative count": {get, strings.Replace(report, `"bytes":40000000`, `"bytes":-4`, 1), http.StatusBadRequest},
	} {
		got := tn.do(http.MethodPost, "/reports/export", c.tok, nil, []byte(c.body))
		require.Equal(t, c.code, got.Code, name)
	}
	require.Equal(t, http.StatusMethodNotAllowed, tn.do(http.MethodGet, "/reports/export", get, nil, nil).Code)
	_, sums = tn.collect(t)
	require.Equal(t, int64(40000000), sums["shale.node.export_bytes"], "refused reports record nothing")
}
