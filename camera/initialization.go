package camera

import (
	"slices"
	"strconv"
	"strings"
)

// InitializationVideoMain is the closed native main-profile precondition of an
// explicit smart-codec edit. Values retain their wire spelling; no arbitrary
// vendor map, URI, account or independently editable after snapshot is accepted.
type InitializationVideoMain struct {
	EncodeType   string `json:"encode_type"`
	Resolution   string `json:"resolution"`
	FrameRate    string `json:"frame_rate"`
	BitrateType  string `json:"bitrate_type"`
	Quality      string `json:"quality"`
	Bitrate      string `json:"bitrate"`
	SmartCodec   string `json:"smart_codec"`
	AdaptiveMode string `json:"adaptive_mode"`
}

func initializationNumber(raw string, maxDigits int, min, max uint64) bool {
	if len(raw) == 0 || len(raw) > maxDigits {
		return false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return false
		}
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	return err == nil && v >= min && v <= max
}

func (v InitializationVideoMain) Validate() error {
	if !slices.Contains([]string{"h264", "H264", "H.264", "h265", "H265", "H.265", "mjpeg", "MJPEG"}, v.EncodeType) || v.BitrateType != "vbr" && v.BitrateType != "cbr" || v.SmartCodec != "on" && v.SmartCodec != "off" || v.AdaptiveMode != "on" && v.AdaptiveMode != "off" {
		return ErrInvalid
	}
	w, h, ok := strings.Cut(v.Resolution, "*")
	if !ok || !initializationNumber(w, 4, 1, 8192) || !initializationNumber(h, 4, 1, 8192) || !initializationNumber(v.Quality, 1, 1, 5) || !initializationNumber(v.Bitrate, 7, 1, 1000000) || !initializationNumber(v.FrameRate, 5, 1, 240) && !initializationNumber(v.FrameRate, 5, 65537, 65776) {
		return ErrInvalid
	}
	return nil
}

// ValidateInitializationPlan binds a native precondition to explicit intent.
// Frequency-only plans from earlier versions do not require a video snapshot.
func ValidateInitializationPlan(p InitializationPlan, desired DeviceSettings) error {
	if desired.Validate() != nil || p.Before.Validate() != nil || p.After.Validate() != nil || p.After != desired {
		return ErrInvalid
	}
	if desired.PowerLineFrequency == 0 && p.Before.PowerLineFrequency != 0 {
		return ErrInvalid
	}
	if desired.SmartCodec == "" {
		if p.VideoMainBefore != nil || p.Before.SmartCodec != "" {
			return ErrInvalid
		}
		return nil
	}
	if p.VideoMainBefore == nil || p.VideoMainBefore.Validate() != nil || p.Before.SmartCodec == "" || p.VideoMainBefore.SmartCodec != p.Before.SmartCodec {
		return ErrInvalid
	}
	return nil
}
