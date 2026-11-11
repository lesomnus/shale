package e2e_test

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/lesomnus/shale/internal/mediaprobe"
	"github.com/stretchr/testify/require"
)

// Production qualification consumes decoded pixel frames. This proof uses the
// existing real legacy Producer/Relay path, with no CP-managed Worker APIs or
// dependence on the separate #202 assembler/framemd5 test oracle.
func TestMediaProbeDecodesRelay(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	c := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, live := liveSource(t, ctx, c, c.dial("@acme/admin"), "av.mp4")
	e, err := mediaprobe.Probe(ctx, mediaprobe.Options{}, mediaprobe.Endpoint{URL: live.GetWhepUrl(), Token: live.GetViewToken()})
	require.NoError(t, err)
	require.Equal(t, 8, e.AccessUnits)
	require.Greater(t, e.DecodedFrames, 1)
	require.LessOrEqual(t, e.DecodedFrames, e.AccessUnits)
	require.Equal(t, 640, e.Width)
	require.Equal(t, 360, e.Height)
	require.Equal(t, 7, e.RTPAdvances)
	require.Equal(t, uint32(90000), e.RTPClockRate)
	require.Greater(t, int32(e.LastRTP-e.FirstRTP), int32(0))
	require.True(t, e.LastReceivedAt.After(e.FirstReceivedAt))
	require.True(t, e.LastDecodedAt.After(e.FirstDecodedAt))
	t.Logf("production probe decoded %d complete %dx%d pixel frames from %d AUs", e.DecodedFrames, e.Width, e.Height, e.AccessUnits)
}
