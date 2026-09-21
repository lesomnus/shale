//go:build linux

package producer

import (
	"bytes"
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The ioctls of videodev2.h: _IOWR('V', nr, size) is dir 3 in the top two
// bits, the size, 'V', and the number, the same on every Linux
// architecture the producer runs on.
const (
	vidiocQueryctrl = (3 << 30) | (68 << 16) | ('V' << 8) | 36
	vidiocGCtrl     = (3 << 30) | (8 << 16) | ('V' << 8) | 27
	vidiocSCtrl     = (3 << 30) | (8 << 16) | ('V' << 8) | 28

	v4l2CtrlFlagNextCtrl = 0x80000000
)

// struct v4l2_queryctrl, 68 bytes.
type v4l2Queryctrl struct {
	Id       uint32
	Type     uint32
	Name     [32]byte
	Minimum  int32
	Maximum  int32
	Step     int32
	Default  int32
	Flags    uint32
	Reserved [2]uint32
}

// struct v4l2_control, 8 bytes.
type v4l2ControlArg struct {
	Id    uint32
	Value int32
}

func v4l2Open(dev string) (int, error) {
	return unix.Open(dev, unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
}

func v4l2Ioctl(fd int, req uintptr, arg unsafe.Pointer) error {
	_, _, e := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(arg))
	if e != 0 {
		return e
	}

	return nil
}

// V4L2Controls lists a device's controls with their current values, the
// way `v4l2-ctl --list-ctrls` walks them.
func V4L2Controls(dev string) ([]V4L2Control, error) {
	fd, err := v4l2Open(dev)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	var out []V4L2Control
	q := v4l2Queryctrl{Id: v4l2CtrlFlagNextCtrl}
	for {
		if err := v4l2Ioctl(fd, vidiocQueryctrl, unsafe.Pointer(&q)); err != nil {
			if errors.Is(err, unix.EINVAL) {
				break
			}

			return out, err
		}
		id := q.Id
		if q.Type != v4l2CtrlTypeCtrlClass && q.Flags&v4l2CtrlFlagDisabled == 0 {
			c := V4L2Control{Id: id, Type: q.Type, Name: V4L2Name(cstr(q.Name[:])), Min: q.Minimum, Max: q.Maximum, Default: q.Default, Flags: q.Flags}
			g := v4l2ControlArg{Id: id}
			if err := v4l2Ioctl(fd, vidiocGCtrl, unsafe.Pointer(&g)); err == nil {
				c.Value = g.Value
			}
			out = append(out, c)
		}
		q = v4l2Queryctrl{Id: id | v4l2CtrlFlagNextCtrl}
	}

	return out, nil
}

// v4l2Set sets one control.
func v4l2Set(dev string, id uint32, value int32) error {
	fd, err := v4l2Open(dev)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	arg := v4l2ControlArg{Id: id, Value: value}

	return v4l2Ioctl(fd, vidiocSCtrl, unsafe.Pointer(&arg))
}

func cstr(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}

	return string(b)
}
