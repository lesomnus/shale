package producer

import (
	"testing"
)

func TestV4L2Name(t *testing.T) {
	cases := map[string]string{
		"Exposure, Dynamic Framerate":  "exposure_dynamic_framerate",
		"Power Line Frequency":         "power_line_frequency",
		"Focus, Automatic Continuous":  "focus_automatic_continuous",
		"Backlight Compensation":       "backlight_compensation",
		"Auto Exposure":                "auto_exposure",
		"  White Balance, Auto  ":      "white_balance_auto",
		"Brightness":                   "brightness",
		"Exposure Time, Absolute":      "exposure_time_absolute",
		"Zoom, Absolute":               "zoom_absolute",
		"exposure_dynamic_framerate":   "exposure_dynamic_framerate",
		"H264 I-Frame Period (Legacy)": "h264_i_frame_period_legacy",
	}
	for in, want := range cases {
		if got := V4L2Name(in); got != want {
			t.Errorf("V4L2Name(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestV4L2Value(t *testing.T) {
	boolean := V4L2Control{Type: v4l2CtrlTypeBoolean, Min: 0, Max: 1}
	menu := V4L2Control{Type: v4l2CtrlTypeMenu, Min: 0, Max: 3}
	integer := V4L2Control{Type: v4l2CtrlTypeInteger, Min: -64, Max: 64}
	cases := []struct {
		c    V4L2Control
		in   string
		want int32
		bad  bool
	}{
		{boolean, "0", 0, false},
		{boolean, "1", 1, false},
		{boolean, "off", 0, false},
		{boolean, "true", 1, false},
		{boolean, "2", 0, true},
		{menu, "3", 3, false},
		{menu, "4", 0, true},
		{integer, "-10", -10, false},
		{integer, "0x10", 16, false},
		{integer, "65", 0, true},
		{integer, "bright", 0, true},
		{V4L2Control{Type: v4l2CtrlTypeInteger64}, "1", 0, true},
		{V4L2Control{Type: v4l2CtrlTypeInteger, Flags: v4l2CtrlFlagReadOnly, Max: 9}, "1", 0, true},
		{V4L2Control{Type: v4l2CtrlTypeButton}, "", 0, false},
	}
	for _, c := range cases {
		got, err := v4l2Value(c.c, c.in)
		if (err != nil) != c.bad {
			t.Errorf("v4l2Value(type %d, %q): err = %v, want bad=%v", c.c.Type, c.in, err, c.bad)
			continue
		}
		if !c.bad && got != c.want {
			t.Errorf("v4l2Value(type %d, %q) = %d, want %d", c.c.Type, c.in, got, c.want)
		}
	}
}

func TestSetControlsUnknownDevice(t *testing.T) {
	set, errs := setControls("/dev/video-none", map[string]string{"brightness": "1"})
	if len(set) != 0 || len(errs) != 1 {
		t.Fatalf("set = %v, errs = %v", set, errs)
	}
}
