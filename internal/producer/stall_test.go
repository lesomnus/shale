package producer

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A capture that stops for three minutes and comes back with its
// timestamps rebased, as a remux stage writes them: the media runs on as if
// nothing happened, and the wall clock says what did (#112).
func TestStallTrackerRebased(t *testing.T) {
	var tr stallTracker
	t0 := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	media := time.Duration(0)
	at := t0
	for range 10 {
		quiet, hidden, warned := tr.seen(at, media)
		require.LessOrEqual(t, quiet, 500*time.Millisecond)
		require.Zero(t, hidden)
		require.False(t, warned)
		at = at.Add(500 * time.Millisecond)
		media += 500 * time.Millisecond
	}

	// Quiet: reported once past the threshold, while it lasts.
	_, ok := tr.quiet(at.Add(4*time.Second), 5*time.Second)
	require.False(t, ok, "4.5 s is under 5")
	d, ok := tr.quiet(at.Add(6*time.Second), 5*time.Second)
	require.True(t, ok)
	require.Equal(t, 6500*time.Millisecond, d)
	_, ok = tr.quiet(at.Add(time.Minute), 5*time.Second)
	require.False(t, ok, "once a stall")

	// Back, with the media where it left off.
	quiet, hidden, warned := tr.seen(at.Add(3*time.Minute), media)
	require.Equal(t, 3*time.Minute+500*time.Millisecond, quiet)
	require.Equal(t, 3*time.Minute, hidden)
	require.True(t, warned)
	quiet, hidden, warned = tr.seen(at.Add(3*time.Minute+500*time.Millisecond), media+500*time.Millisecond)
	require.Equal(t, 500*time.Millisecond, quiet)
	require.Zero(t, hidden)
	require.False(t, warned)
}

// A stall the timestamps show (the media jumps with the wall clock) hides
// nothing: accountGap or the recording's own gap says it.
func TestStallTrackerShown(t *testing.T) {
	var tr stallTracker
	t0 := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	tr.seen(t0, 0)
	quiet, hidden, _ := tr.seen(t0.Add(time.Minute), time.Minute)
	require.Equal(t, time.Minute, quiet)
	require.Zero(t, hidden)

	// A new init segment starts its media clock again: no hidden time from that.
	tr.reset()
	_, hidden, _ = tr.seen(t0.Add(time.Minute+time.Second), 0)
	require.Zero(t, hidden)
}

// A stall counts once, with its length; the frames the timestamps hid count
// where accountGap's go; a pause under the threshold is nothing (§38.6).
func TestAccountStall(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	var logs bytes.Buffer
	p := &Producer{m: newMetrics(ctx), log: slog.New(slog.NewTextHandler(&logs, nil))}
	s := &source{cfg: SourceConfig{Alias: "cam", Fps: 30}}

	p.accountStall(s, time.Second, 0, false)
	require.Empty(t, logs.String(), "a second is no stall")
	p.accountStall(s, 3*time.Minute, 3*time.Minute, true)
	require.Contains(t, logs.String(), "capture resumed after delivering nothing")
	require.Contains(t, logs.String(), "for=3m0s")
	require.Contains(t, logs.String(), "timestamps_hid=true")
	p.accountStall(s, 6*time.Second, 0, false)

	got := collect(t, ctx, reader)
	require.InDelta(t, 3*60+6, got["shale.producer.capture_stalled_seconds"], 1e-9)
	require.Equal(t, 2.0, got["shale.producer.capture_stalls"])
	require.Equal(t, 1.0, got["shale.producer.frame_gaps"], "the 6 s stall showed in the timestamps")
	require.Equal(t, float64(3*60*30), got["shale.producer.frames_missed"])
}

// ffmpeg's word for a stall it rebased is a warning and a count, and is
// not left for the info log.
func TestDiscontinuity(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	var logs bytes.Buffer
	p := &Producer{m: newMetrics(ctx), log: slog.New(slog.NewTextHandler(&logs, nil))}
	s := &source{cfg: SourceConfig{Alias: "cam"}}

	require.False(t, p.discontinuity(s, "[mpegts @ 0x5590] Packet corrupt"))
	require.True(t, p.discontinuity(s, "[mpegts @ 0x5590] timestamp discontinuity (stream id=65): 16200000, new offset= -16196400"))
	require.Contains(t, logs.String(), "level=WARN")
	require.Equal(t, 1.0, collect(t, ctx, reader)["shale.producer.timestamp_discontinuities"])
}

// collect reads every counter's first point, as a float.
func collect(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader) map[string]float64 {
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				got[m.Name] = float64(d.DataPoints[0].Value)
			case metricdata.Sum[float64]:
				got[m.Name] = d.DataPoints[0].Value
			}
		}
	}

	return got
}

// The soak's stall, end to end (#112): a camera's MPEG-TS stops for six
// seconds and comes back with its clock gone on, through the remux stage a
// v4l2m2m capture runs. ffmpeg rebases the jump, so the fMP4 is continuous,
// and the tracker still sees the six seconds and that the timestamps hid
// them.
func TestStallThroughRemux(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg on this host")
	}
	if testing.Short() {
		t.Skip("waits six seconds")
	}
	dir := t.TempDir()
	ts := func(name, offset string) []byte {
		out := filepath.Join(dir, name)
		cmd := exec.Command(ffmpeg, "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=10", "-t", "3",
			"-c:v", "libx264", "-b:v", "2M", "-g", "10", "-bf", "0", "-output_ts_offset", offset, "-f", "mpegts", out)
		require.NoError(t, cmd.Run())
		b, err := os.ReadFile(out)
		require.NoError(t, err)
		return b
	}
	before, after := ts("a.ts", "0"), ts("b.ts", "300")

	// A camera's stream never ends, so ffmpeg's probe of its first 5 MB
	// finishes; three seconds of this one would wait out the pause there.
	args := append([]string{"-loglevel", "info", "-probesize", "32768", "-analyzeduration", "0", "-f", "mpegts", "-i", "pipe:0", "-map", "0", "-c", "copy"}, RemuxMuxArgs(500*time.Millisecond)...)
	cmd := exec.Command(ffmpeg, append(args, "pipe:1")...)
	in, err := cmd.StdinPipe()
	require.NoError(t, err)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var said bytes.Buffer
	cmd.Stderr = &said
	require.NoError(t, cmd.Start())
	go func() {
		_, _ = in.Write(before)
		time.Sleep(6 * time.Second)
		_, _ = in.Write(after)
		_ = in.Close()
	}()

	var tr stallTracker
	r := NewMP4Reader(out)
	var f Frame
	var quiet, hidden time.Duration
	for r.Next(&f) == nil {
		v := r.Init().Video()
		if f.Kind != FrameData || f.Frames == 0 || v == nil {
			continue
		}
		q, h, _ := tr.seen(time.Now(), time.Duration(f.Ticks)*time.Second/time.Duration(v.Timescale))
		quiet, hidden = max(quiet, q), max(hidden, h)
	}
	require.NoError(t, cmd.Wait())
	t.Logf("quiet %s, hidden %s", quiet, hidden)
	require.Contains(t, said.String(), "timestamp discontinuity", "ffmpeg rebased the jump")
	require.Greater(t, quiet, 5*time.Second)
	require.Greater(t, hidden, 4*time.Second, "the media ran on through the pause")
}
