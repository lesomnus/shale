package producer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// V4L2 controls (§38.3): a `controls:` map on a `v4l2:` source is set on
// the device before every capture start, by name, through the ioctls
// `v4l2-ctl -c name=value` uses, so a host without v4l-utils can set them
// too. A camera forgets them when re-plugged, which is why every start
// sets them again.

// V4L2Control is one control a device offers, with its current value.
type V4L2Control struct {
	Id      uint32
	Type    uint32
	Name    string
	Min     int32
	Max     int32
	Default int32
	Flags   uint32
	Value   int32
}

// The control types of videodev2.h that matter here.
const (
	v4l2CtrlTypeInteger     = 1
	v4l2CtrlTypeBoolean     = 2
	v4l2CtrlTypeMenu        = 3
	v4l2CtrlTypeButton      = 4
	v4l2CtrlTypeInteger64   = 5
	v4l2CtrlTypeCtrlClass   = 6
	v4l2CtrlTypeString      = 7
	v4l2CtrlTypeBitmask     = 8
	v4l2CtrlTypeIntegerMenu = 9

	v4l2CtrlFlagDisabled = 0x0001
	v4l2CtrlFlagReadOnly = 0x0004
	v4l2CtrlFlagInactive = 0x0010
)

// V4L2Name is v4l2-ctl's spelling of a control's name: lowercase, with a
// run of anything but letters and digits as one underscore, so the
// kernel's "Exposure, Dynamic Framerate" is exposure_dynamic_framerate.
func V4L2Name(name string) string {
	var b strings.Builder
	underscore := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			if underscore {
				b.WriteByte('_')
			}
			underscore = false
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			if underscore {
				b.WriteByte('_')
			}
			underscore = false
			b.WriteRune(r + 'a' - 'A')
		default:
			if b.Len() > 0 {
				underscore = true
			}
		}
	}

	return b.String()
}

// v4l2Value is the value a control is set to from the configuration's
// text: an integer, a menu index, or a boolean's on/off.
func v4l2Value(c V4L2Control, s string) (int32, error) {
	switch c.Type {
	case v4l2CtrlTypeInteger64, v4l2CtrlTypeString:
		return 0, errors.New("a control of this type cannot be set here")
	case v4l2CtrlTypeButton:
		return 0, nil
	}
	if c.Flags&v4l2CtrlFlagReadOnly != 0 {
		return 0, errors.New("read-only")
	}
	t := strings.TrimSpace(strings.ToLower(s))
	switch t {
	case "true", "on", "yes":
		t = "1"
	case "false", "off", "no":
		t = "0"
	}
	v, err := strconv.ParseInt(t, 0, 32)
	if err != nil {
		return 0, fmt.Errorf("value %q: an integer is expected", s)
	}
	if c.Type != v4l2CtrlTypeBitmask && (v < int64(c.Min) || v > int64(c.Max)) {
		return 0, fmt.Errorf("value %d is outside %d..%d", v, c.Min, c.Max)
	}

	return int32(v), nil
}

// setControls sets a source's controls on its device, sorted by name so
// the log reads the same every time; every failure is answered and the
// rest are set regardless.
func setControls(dev string, want map[string]string) (set []string, errs []error) {
	ctrls, err := V4L2Controls(dev)
	if err != nil {
		return nil, []error{err}
	}
	byName := map[string]V4L2Control{}
	for _, c := range ctrls {
		byName[c.Name] = c
	}
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sortStrings(names)
	for _, name := range names {
		c, ok := byName[V4L2Name(name)]
		if !ok {
			errs = append(errs, fmt.Errorf("%s: no such control on %s", name, dev))
			continue
		}
		v, err := v4l2Value(c, want[name])
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		if err := v4l2Set(dev, c.Id, v); err != nil {
			errs = append(errs, fmt.Errorf("%s=%s: %w", name, want[name], err))
			continue
		}
		set = append(set, name+"="+want[name])
	}

	return set, errs
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}
