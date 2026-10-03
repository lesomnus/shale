package producer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// A microphone is named the way ALSA does (`alsa:hw:1`, `alsa:hw:CARD=WEBCAM`)
// or by a path under /dev/snd (§38.3): `alsa:/dev/snd/by-path/…`. A card's
// number follows the order the USB devices came up in, and three identical
// webcams share one name, so the path of the USB port is what stays put; it
// is resolved to the card's number at every start of the capture, as a
// camera's /dev/v4l path is.

var sndNode = regexp.MustCompile(`^(?:controlC(\d+)|pcmC(\d+)D(\d+)c)$`)

// alsaDevice is the device ALSA opens for a source's `audio.device`: the
// `alsa:` taken off, and a /dev/snd path resolved to `hw:<card>,<device>`.
func alsaDevice(device string) (string, error) {
	dev := strings.TrimPrefix(device, "alsa:")
	if !strings.HasPrefix(dev, "/") {
		return dev, nil
	}
	target, err := filepath.EvalSymlinks(dev)
	if err != nil {
		return "", fmt.Errorf("audio device %s: %w", dev, err)
	}
	m := sndNode.FindStringSubmatch(filepath.Base(target))
	switch {
	case m == nil:
		return "", fmt.Errorf("audio device %s is %s, not a sound card's control or capture node", dev, target)
	case m[1] != "":
		return "hw:" + m[1] + ",0", nil
	default:
		return "hw:" + m[2] + "," + m[3], nil
	}
}
