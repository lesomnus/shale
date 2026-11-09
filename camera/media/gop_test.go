package media_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/media"
)

// Produce a full five-second packet observation with monotonic DTS. Reordered
// PTS and repeated keyframe PTS must not change the presentation-time GOP bound.
func gopPackets(keys []int, reordered, duplicate, variableRate bool) string {
	key := map[int]bool{}
	for _, frame := range keys {
		key[frame] = true
	}
	order := make([]int, 151)
	for i := range order {
		order[i] = i
	}
	if reordered {
		order[10], order[50] = order[50], order[10]
	}
	var output strings.Builder
	output.WriteString("printf '%s\\n' ")
	for dts, pts := range order {
		flags := "__"
		if key[pts] {
			flags = "K_"
		}
		presentation := float64(pts) / 30
		decode := float64(dts) / 30
		if variableRate {
			// 151 frames in five seconds still average 30fps. The first
			// 40-frame GOP lasts 5/6s; later GOPs are 22 frames in 5/6s.
			// Time gap * average FPS would misreport every interval as 25.
			if pts <= 40 {
				presentation = float64(pts) / 48
			} else {
				presentation = 5.0/6 + float64(pts-40)*(25.0/6)/110
			}
			decode = presentation // Valid monotonic PTS/DTS, without B-frames.
		}
		fmt.Fprintf(&output, "'packet|stream_index=0|pts_time=%.9f|dts_time=%.9f|size=100|flags=%s' ", presentation, decode, flags)
		if duplicate && key[pts] {
			fmt.Fprintf(&output, "'packet|stream_index=0|pts_time=%.9f|dts_time=%.9f|size=100|flags=K_' ", presentation, decode+0.0001)
		}
	}
	output.WriteString("'stream|index=0|codec_type=video|codec_name=h264|width=1920|height=1080'")
	return output.String()
}

func TestExplicitGOPRequiresObservedMaximumCadence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keys         []int
		reordered    bool
		duplicate    bool
		variableRate bool
		desiredGOP   int
		wantGOP      int
		wantErr      error
	}{
		{name: "missing-keyframes", desiredGOP: 25, wantErr: media.ErrIncomplete},
		{name: "single-keyframe", keys: []int{0}, desiredGOP: 25, wantErr: media.ErrIncomplete},
		{name: "duplicate-single-keyframe", keys: []int{0}, duplicate: true, desiredGOP: 25, wantErr: media.ErrIncomplete},
		{name: "unknown-unconstrained-gop", wantGOP: 0},
		{name: "regular-cadence", keys: []int{0, 25, 50, 75, 100, 125, 150}, desiredGOP: 25, wantGOP: 25},
		{name: "long-gap-hidden-by-average", keys: []int{0, 10, 50, 75, 100, 125, 150}, desiredGOP: 25, wantGOP: 40, wantErr: media.ErrMismatch},
		{name: "long-gap-reordered-and-duplicated", keys: []int{0, 10, 50, 75, 100, 125, 150}, reordered: true, duplicate: true, desiredGOP: 25, wantGOP: 40, wantErr: media.ErrMismatch},
		{name: "reordered-and-duplicated-within-bound", keys: []int{0, 10, 50, 90, 130}, reordered: true, duplicate: true, desiredGOP: 40, wantGOP: 40},
		{name: "bounded-timestamp-tolerance", keys: []int{0, 27, 54, 81, 108, 135}, desiredGOP: 25, wantGOP: 27},
		{name: "outside-timestamp-tolerance", keys: []int{0, 28, 56, 84, 112, 140}, desiredGOP: 25, wantGOP: 28, wantErr: media.ErrMismatch},
		{name: "stalled-keyframe-tail", keys: []int{0, 25, 50}, desiredGOP: 25, wantGOP: 100, wantErr: media.ErrMismatch},
		{name: "stalled-keyframe-head", keys: []int{100, 125, 150}, desiredGOP: 25, wantGOP: 100, wantErr: media.ErrMismatch},
		{name: "stalled-keyframe-both-edges", keys: []int{50, 75, 100}, desiredGOP: 25, wantGOP: 50, wantErr: media.ErrMismatch},
		{name: "tail-within-tolerance", keys: []int{0, 25, 50, 75, 100, 123}, desiredGOP: 25, wantGOP: 27},
		{name: "tail-outside-tolerance", keys: []int{0, 25, 50, 75, 100, 122}, desiredGOP: 25, wantGOP: 28, wantErr: media.ErrMismatch},
		{name: "head-within-tolerance", keys: []int{27, 52, 77, 102, 127}, desiredGOP: 25, wantGOP: 27},
		{name: "head-outside-tolerance", keys: []int{28, 53, 78, 103, 128}, desiredGOP: 25, wantGOP: 28, wantErr: media.ErrMismatch},
		{name: "variable-rate-gap-exceeds-frame-bound", keys: []int{0, 40, 62, 84, 106, 128, 150}, variableRate: true, desiredGOP: 25, wantGOP: 40, wantErr: media.ErrMismatch},
		{name: "variable-rate-gap-within-frame-bound", keys: []int{0, 40, 62, 84, 106, 128, 150}, variableRate: true, desiredGOP: 40, wantGOP: 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "decoder-called")
			decoder := script(t, fmt.Sprintf("touch '%s'\nprintf 'frame=151\\n'", marker))
			verifier := media.Verifier{NetworkScope: "lan", AllowFile: true,
				Ffprobe: script(t, gopPackets(tc.keys, tc.reordered, tc.duplicate, tc.variableRate)), Ffmpeg: decoder}
			request := fixtureRequest("/controlled")
			request.Duration = 5 * time.Second
			request.Desired = camera.DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 30, GOP: tc.desiredGOP, Revision: "gop-proof"}
			// Configured/read-back GOP is never substituted for missing evidence.
			request.Profile.GOP = 25
			observed, err := verifier.Verify(context.Background(), request)
			if !errors.Is(err, tc.wantErr) || observed.Profile.GOP != tc.wantGOP {
				t.Fatalf("GOP=%d error=%v; want GOP=%d error=%v", observed.Profile.GOP, err, tc.wantGOP, tc.wantErr)
			}
			_, markerErr := os.Stat(marker)
			if tc.wantErr != nil {
				if !os.IsNotExist(markerErr) || observed.Decoded || observed.DecodedFrames != 0 {
					t.Fatal("unverified GOP reached the independent decoder")
				}
				if errors.Is(err, media.ErrIncomplete) && observed.Complete {
					t.Fatal("missing cadence reported as complete verification")
				}
			} else if markerErr != nil || !observed.Complete || !observed.Decoded || observed.DecodedFrames != 151 {
				t.Fatal("valid observed GOP was not independently decoded")
			}
			if observed.Profile.FPS < 29.99 || observed.Profile.FPS > 30.01 || observed.Duration < 4999*time.Millisecond {
				t.Fatal("keyframe handling changed packet timing evidence")
			}
		})
	}
}
