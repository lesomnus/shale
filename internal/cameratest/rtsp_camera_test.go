package cameratest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
)

type mediaProbe struct {
	Width, Height int
	FPS, Bitrate  float64
}

func requireMediaTools(t *testing.T) {
	t.Helper()
	for _, binary := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Skip(binary + " unavailable")
		}
	}
}

func probeRTSP(ctx context.Context, endpoint string, seconds int) (mediaProbe, error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-rtsp_transport", "tcp", "-analyzeduration", "500000", "-probesize", "1000000", "-read_intervals", fmt.Sprintf("%%+%d", seconds), "-select_streams", "v:0", "-show_packets", "-show_streams", "-show_entries", "stream=width,height:packet=pts_time,size", "-of", "json", endpoint)
	cmd.WaitDelay = time.Second
	data, err := cmd.Output()
	if err != nil {
		return mediaProbe{}, fmt.Errorf("probe synthetic RTSP: %w", err)
	}
	var result struct {
		Streams []struct{ Width, Height int }
		Packets []struct {
			PTS  string `json:"pts_time"`
			Size string
		}
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return mediaProbe{}, err
	}
	if len(result.Streams) != 1 || len(result.Packets) < 2 {
		return mediaProbe{}, fmt.Errorf("missing actual media packets")
	}
	valid := result.Packets[:0]
	for _, p := range result.Packets {
		if p.PTS != "" {
			valid = append(valid, p)
		}
	}
	result.Packets = valid
	if len(valid) < 2 {
		return mediaProbe{}, fmt.Errorf("missing actual RTP timestamps")
	}
	first, err := strconv.ParseFloat(result.Packets[0].PTS, 64)
	if err != nil {
		return mediaProbe{}, err
	}
	last, err := strconv.ParseFloat(result.Packets[len(result.Packets)-1].PTS, 64)
	if err != nil || last <= first {
		return mediaProbe{}, fmt.Errorf("media timestamps did not advance")
	}
	bytes := 0
	for _, packet := range result.Packets {
		n, err := strconv.Atoi(packet.Size)
		if err != nil {
			return mediaProbe{}, err
		}
		bytes += n
	}
	span := last - first
	return mediaProbe{Width: result.Streams[0].Width, Height: result.Streams[0].Height, FPS: float64(len(result.Packets)-1) / span, Bitrate: float64(bytes) * 8 / span}, nil
}

func profiles() (camera.MediaProfile, camera.MediaProfile) {
	return camera.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 64_000, GOP: 10}, camera.MediaProfile{Token: "main", Codec: "h264", Width: 320, Height: 180, FPS: 20, Bitrate: 256_000, GOP: 20}
}

func TestRTSPCameraNewSessionsReceiveActualChangedMedia(t *testing.T) {
	requireMediaTools(t)
	before, after := profiles()
	fixture := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{Profile: before, ListenAddress: "127.0.0.2:0", Path: "/main", SetupTimeout: 15 * time.Second})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	type measured struct {
		media mediaProbe
		err   error
	}
	old := make(chan measured, 1)
	go func() { media, err := probeRTSP(ctx, fixture.URL(), 3); old <- measured{media, err} }()
	require.Eventually(t, func() bool { return fixture.Sessions() > 0 }, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, fixture.SetProfile(ctx, after))
	current, err := probeRTSP(ctx, fixture.URL(), 1)
	require.NoError(t, err)
	var previous measured
	select {
	case previous = <-old:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, previous.err)
	require.Equal(t, before.Width, previous.media.Width)
	require.Equal(t, before.Height, previous.media.Height)
	require.InDelta(t, before.FPS, previous.media.FPS, 0.1)
	require.Equal(t, after.Width, current.Width)
	require.Equal(t, after.Height, current.Height)
	require.InDelta(t, after.FPS, current.FPS, 0.1)
	require.Greater(t, current.Bitrate, 2*previous.media.Bitrate, "the new rate must be present in actual RTP payloads, not just ONVIF metadata")
	require.Greater(t, current.Bitrate, float64(after.Bitrate)*0.6)
	t.Logf("actual existing session %dx%d %.2ffps %.0fbps; new session %dx%d %.2ffps %.0fbps", previous.media.Width, previous.media.Height, previous.media.FPS, previous.media.Bitrate, current.Width, current.Height, current.FPS, current.Bitrate)
}

func TestRTSPCameraRejectsUnboundedProfileWithoutChangingMedia(t *testing.T) {
	requireMediaTools(t)
	before, after := profiles()
	fixture := cameratest.ServeRTSPCamera(t, cameratest.RTSPOptions{Profile: before})
	after.Width = 16384
	require.Error(t, fixture.SetProfile(t.Context(), after))
	require.Equal(t, before.Width, fixture.Profile().Width)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, after = profiles()
	require.ErrorIs(t, fixture.SetProfile(cancelled, after), context.Canceled)
	require.Equal(t, before.Width, fixture.Profile().Width)
}
