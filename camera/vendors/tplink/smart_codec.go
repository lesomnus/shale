package tplink

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/lesomnus/shale/camera"
)

func validSmartCodec(mode string) bool { return mode == "off" || mode == "on" }

func validNativeVideoMain(v camera.InitializationVideoMain) bool {
	if v.Validate() != nil || v.EncodeType != "H264" && v.EncodeType != "H265" {
		return false
	}
	// The qualified UI sends the native encoded frame-rate value unchanged.
	// Its visible FPS label is that value minus 65536, not a wire conversion.
	fps, err := strconv.Atoi(v.FrameRate)
	return err == nil && fps > 65536 && fps <= 65776
}

func smartCodecTarget(h camera.DeviceHint) bool {
	if deviceProfile(h) != ProfileC420V122 {
		return false
	}
	fw := strings.ReplaceAll(strings.Join(strings.Fields(h.Firmware), " "), "Build ", "Build")
	return fw == "1.2.2 Build260624" || fw == "1.2.2 Build260624 Rel.39352n"
}

// videoMain projects the known 1.2.2 web UI's closed eight writable fields out
// of a bounded response. Other response metadata is never stored or replayed.
func (s *webSession) videoMain(ctx context.Context) (camera.InitializationVideoMain, error) {
	var out camera.InitializationVideoMain
	reply, err := s.call(ctx, map[string]any{"method": "get", "video": map[string]any{"name": []string{"main"}}})
	if err != nil {
		return out, err
	}
	if reply.Status != http.StatusOK {
		return out, errProtocol
	}
	if err = reply.success(); err != nil {
		return out, err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(reply.Video["main"], &fields) != nil || len(fields) > 64 {
		return out, errProtocol
	}
	for _, field := range []struct {
		name   string
		target *string
	}{
		{"encode_type", &out.EncodeType}, {"resolution", &out.Resolution}, {"frame_rate", &out.FrameRate}, {"bitrate_type", &out.BitrateType},
		{"quality", &out.Quality}, {"bitrate", &out.Bitrate}, {"smart_codec", &out.SmartCodec}, {"adaptive_mode", &out.AdaptiveMode},
	} {
		if json.Unmarshal(fields[field.name], field.target) != nil {
			return camera.InitializationVideoMain{}, errProtocol
		}
	}
	if !validNativeVideoMain(out) {
		return camera.InitializationVideoMain{}, errProtocol
	}
	return out, nil
}

func (s *webSession) applySmartCodec(ctx context.Context, before camera.InitializationVideoMain, target string) error {
	current, err := s.videoMain(ctx)
	if err != nil {
		return err
	}
	if current.SmartCodec == target {
		return nil
	}
	if current != before {
		return camera.ErrConflict
	}
	after := before
	after.SmartCodec = target
	bitrate, err := strconv.Atoi(after.Bitrate)
	if err != nil {
		return errProtocol
	}
	// Match the known UI's SET shape. In particular bitrate is numeric, VBR
	// quality remains a string, and an existing adaptive "off" is omitted.
	main := map[string]any{
		"encode_type": after.EncodeType, "resolution": after.Resolution, "frame_rate": after.FrameRate,
		"bitrate_type": after.BitrateType, "bitrate": bitrate, "smart_codec": after.SmartCodec,
	}
	if after.BitrateType == "vbr" {
		main["quality"] = after.Quality
	}
	if after.AdaptiveMode == "on" {
		main["adaptive_mode"] = "on"
	}
	reply, err := s.call(ctx, map[string]any{"method": "set", "video": map[string]any{"main": main}})
	if err != nil {
		return err
	}
	if reply.Status != http.StatusOK {
		return errProtocol
	}
	if err = reply.success(); err != nil {
		return err
	}
	observed, err := s.videoMain(ctx)
	if err != nil {
		return err
	}
	if observed != after {
		return camera.ErrConflict
	}
	return nil
}
