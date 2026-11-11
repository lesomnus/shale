package storage

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// readTiming is where one read's time went. The device's share is measured
// around each chunk; what is left of the request is writing to the client,
// which is the network and the client's reading.
type readTiming struct {
	wait, device time.Duration
	deviceBytes  int64
}

// countingWriter counts the bytes a read put on the wire.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (w *countingWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.n += int64(n)

	return n, err
}

func (t *readTiming) record(ctx context.Context, m *metrics, s *Sink, elapsed time.Duration, sent int64) {
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	send := max(0, elapsed-t.wait-t.device)
	m.readWait.Record(ctx, ms(t.wait), sinkAttr(s))
	m.readDevice.Record(ctx, ms(t.device), sinkAttr(s))
	m.readSend.Record(ctx, ms(send), sinkAttr(s))
	m.readDeviceBytes.Add(ctx, t.deviceBytes, sinkAttr(s))
	m.readSentBytes.Add(ctx, sent, sinkAttr(s))
}

// exportReport is what a page measured of one export (ts/src/playback.tsx):
// durations in milliseconds, the request ones summed over the export.
type exportReport struct {
	Laminae   int     `json:"laminae"`
	Requests  int     `json:"requests"`
	Bytes     int64   `json:"bytes"`
	Total     float64 `json:"total_ms"`
	Index     float64 `json:"index_ms"`
	Fetch     float64 `json:"fetch_ms"`
	FirstByte float64 `json:"first_byte_ms"`
	Body      float64 `json:"body_ms"`
	Assemble  float64 `json:"assemble_ms"`
}

// An export the page reports is at most a day of one camera.
const (
	reportSpan  = float64(24 * time.Hour / time.Millisecond)
	reportBytes = 1 << 40
)

// exportTiming takes a page's report of an export. Any token this node
// would serve a read for may report: the numbers grant nothing, and they
// are bounded before they are recorded.
func (d *DataPlane) exportTiming(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tok := token.FromHeader(r.Header.Get("Authorization"))
	if tok == "" {
		tok = r.URL.Query().Get("token")
	}
	c, err := d.node.verifier.Verify(tok)
	if err != nil {
		http.Error(w, "a read token is required", http.StatusUnauthorized)
		return
	}
	if err := token.Check(c, d.node.id.Bytes(), api.TokenOp_TOKEN_OP_GET); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	var report exportReport
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&report); err != nil {
		http.Error(w, "an export report is JSON", http.StatusBadRequest)
		return
	}
	spans := []float64{report.Total, report.Index, report.Fetch, report.FirstByte, report.Body, report.Assemble}
	for _, v := range spans {
		if math.IsNaN(v) || v < 0 || v > reportSpan {
			http.Error(w, "a duration is out of range", http.StatusBadRequest)
			return
		}
	}
	if report.Bytes < 0 || report.Bytes > reportBytes || report.Requests < 0 || report.Laminae < 0 {
		http.Error(w, "a count is out of range", http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	m := d.node.m
	m.exportTotal.Record(ctx, report.Total)
	m.exportIndex.Record(ctx, report.Index)
	m.exportFirstByte.Record(ctx, report.FirstByte)
	m.exportBody.Record(ctx, report.Body)
	// The page awaits each request before the next: whatever of fetching
	// is not inside one is the page's own time between them.
	m.exportGap.Record(ctx, max(0, report.Fetch-report.FirstByte-report.Body))
	m.exportAssemble.Record(ctx, report.Assemble)
	if report.Fetch > 0 {
		m.exportRate.Record(ctx, float64(report.Bytes)*8/1e3/report.Fetch)
	}
	m.exportBytes.Add(ctx, report.Bytes)
	w.WriteHeader(http.StatusNoContent)
}
