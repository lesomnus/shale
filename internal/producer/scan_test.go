package producer

import (
	"bytes"
	"strings"
	"testing"
)

func TestSkeletonSize(t *testing.T) {
	cases := []struct {
		name  string
		cam   Camera
		want  string
		lines []string
	}{
		{"1080p camera", Camera{Modes: []string{"yuyv422: 640x480 1920x1080", "mjpeg: 640x480 1280x720 1920x1080"}}, "1920x1080", nil},
		{"720p camera", Camera{Modes: []string{"yuyv422: 640x480 1280x960", "mjpeg: 160x120 640x480 1280x720 1280x960"}}, "1280x720", nil},
		{"odd sizes only", Camera{Modes: []string{"mjpeg: 800x600 1024x768 2560x1440"}}, "1024x768", nil},
		{"no mjpeg", Camera{Modes: []string{"yuyv422: 640x480"}}, "640x480", nil},
		{"h264 camera", Camera{H264: true, Modes: []string{"mjpeg: 1920x1080", "h264: 640x480 1280x720"}}, "1280x720", []string{"format: h264"}},
		{"unparsed", Camera{Modes: []string{"mjpeg: "}}, "1920x1080", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cam.suggestedSize(); got != c.want {
				t.Fatalf("suggestedSize = %q, want %q", got, c.want)
			}
			var b bytes.Buffer
			Scan{Cameras: []Camera{c.cam}}.WriteSkeleton(&b)
			out := b.String()
			for _, l := range append(c.lines, "size: "+c.want) {
				if !strings.Contains(out, l) {
					t.Fatalf("skeleton lacks %q:\n%s", l, out)
				}
			}
		})
	}
}
