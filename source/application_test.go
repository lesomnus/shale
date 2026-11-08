package source

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppliedProofChecksObservedCapture(t *testing.T) {
	want := Profile{Codec: "h264", Width: 1280, Height: 720, FPS: 29.97, MaxBitrate: 2_000_000, KeyframeInterval: time.Second}
	now := time.Now()
	got := MediaProgress{Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: 1_500_000, Frames: 60, Keyframes: 2, KeyframeInterval: time.Second, FirstMediaAt: now, LastMediaAt: now.Add(2 * time.Second)}
	require.NoError(t, CheckMedia(want, got))
	for _, tc := range []struct {
		name   string
		change func(*MediaProgress)
	}{
		{"old resolution", func(v *MediaProgress) { v.Width = 640 }},
		{"old codec", func(v *MediaProgress) { v.Codec = "h265" }},
		{"wrong cadence", func(v *MediaProgress) { v.FPS = 15 }},
		{"unknown cadence", func(v *MediaProgress) { v.FPS = 0 }},
		{"nonfinite cadence", func(v *MediaProgress) { v.FPS = math.NaN() }},
		{"above ceiling", func(v *MediaProgress) { v.Bitrate = 3_000_000 }},
		{"unknown bitrate", func(v *MediaProgress) { v.Bitrate = 0 }},
		{"no random access", func(v *MediaProgress) { v.Keyframes = 0 }},
		{"unknown key cadence", func(v *MediaProgress) { v.KeyframeInterval = 0 }},
		{"slow key cadence", func(v *MediaProgress) { v.KeyframeInterval = 12 * time.Second }},
		{"no advance", func(v *MediaProgress) { v.LastMediaAt = v.FirstMediaAt }},
		{"old progress window", func(v *MediaProgress) { v.LastMediaAt = v.FirstMediaAt.Add(2 * time.Minute) }},
	} {
		t.Run(tc.name, func(t *testing.T) { v := got; tc.change(&v); require.Error(t, CheckMedia(want, v)) })
	}
	unknown := want
	unknown.KeyframeInterval = 0
	got.KeyframeInterval = 0
	require.NoError(t, CheckMedia(unknown, got), "unknown requested cadence is not manufactured evidence")
}
