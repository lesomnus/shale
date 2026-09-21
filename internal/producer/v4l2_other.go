//go:build !linux

package producer

import "errors"

var errNoV4L2 = errors.New("V4L2 is Linux only")

// V4L2Controls lists a device's controls; only Linux has V4L2.
func V4L2Controls(string) ([]V4L2Control, error) { return nil, errNoV4L2 }

func v4l2Set(string, uint32, int32) error { return errNoV4L2 }
