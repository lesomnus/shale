package tplink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

type smartCodecFixture struct {
	mu                                                                          sync.Mutex
	base                                                                        *initializationFixture
	video                                                                       camera.InitializationVideoMain
	reads, writes                                                               int
	setBodies                                                                   []map[string]json.RawMessage
	ignoreWrite, loseReply, driftReadback, malformed, extraField, tooManyFields bool
	frequencyFrameRate                                                          string
}

func newSmartCodecFixture(t *testing.T) *smartCodecFixture {
	t.Helper()
	base := newInitializationFixture(t)
	base.server.Close()
	f := &smartCodecFixture{base: base, video: camera.InitializationVideoMain{
		EncodeType: "H264", Resolution: "1920*1080", FrameRate: "65561", BitrateType: "vbr", Quality: "3", Bitrate: "4096", SmartCodec: "on", AdaptiveMode: "off",
	}}
	base.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(base.server.Close)
	return f
}

func (f *smartCodecFixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/stok=private-session-token/ds" {
		f.base.serve(w, r)
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "fixture read", 500)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var req map[string]json.RawMessage
	if json.Unmarshal(raw, &req) != nil {
		http.Error(w, "fixture JSON", 500)
		return
	}
	var method string
	_ = json.Unmarshal(req["method"], &method)
	if req["video"] == nil {
		if req["image"] != nil && method == "set" && f.frequencyFrameRate != "" {
			f.mu.Lock()
			f.video.FrameRate = f.frequencyFrameRate
			f.mu.Unlock()
		}
		f.base.serve(w, r)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch method {
	case "get":
		f.reads++
		if f.malformed {
			_, _ = io.WriteString(w, `{"error_code":0,"video":{"main":{"smart_codec":"on"}}}`)
			return
		}
		data, _ := json.Marshal(f.video)
		var m map[string]json.RawMessage
		_ = json.Unmarshal(data, &m)
		if f.extraField {
			m["readonly_vendor_metadata"] = json.RawMessage(`"fixture-private-unused"`)
			m["readonly_vendor_revision"] = json.RawMessage(`"fixture-private-revision"`)
		}
		if f.tooManyFields {
			for i := range 57 {
				m["readonly_"+strconv.Itoa(i)] = json.RawMessage(`"fixture-private-unused"`)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0, "video": map[string]any{"main": m}})
	case "set":
		var outer struct {
			Main map[string]json.RawMessage `json:"main"`
		}
		if json.Unmarshal(req["video"], &outer) != nil {
			http.Error(w, "fixture write", 500)
			return
		}
		f.writes++
		f.setBodies = append(f.setBodies, outer.Main)
		if !f.ignoreWrite {
			for name, value := range outer.Main {
				switch name {
				case "encode_type":
					_ = json.Unmarshal(value, &f.video.EncodeType)
				case "resolution":
					_ = json.Unmarshal(value, &f.video.Resolution)
				case "frame_rate":
					_ = json.Unmarshal(value, &f.video.FrameRate)
				case "bitrate_type":
					_ = json.Unmarshal(value, &f.video.BitrateType)
				case "quality":
					_ = json.Unmarshal(value, &f.video.Quality)
				case "smart_codec":
					_ = json.Unmarshal(value, &f.video.SmartCodec)
				case "adaptive_mode":
					_ = json.Unmarshal(value, &f.video.AdaptiveMode)
				case "bitrate":
					var n int
					_ = json.Unmarshal(value, &n)
					f.video.Bitrate = strconv.Itoa(n)
				default:
					http.Error(w, "unexpected fixture setting", 500)
					return
				}
			}
		}
		if f.driftReadback {
			f.video.Quality = "5"
		}
		if f.loseReply {
			f.loseReply = false
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"error_code": 0})
	default:
		http.Error(w, "fixture method", 500)
	}
}

func TestSmartCodecInitializationPreservesFullMainAndKnownUIWire(t *testing.T) {
	for _, tc := range []struct{ name, mode, rate, adaptive string }{{"off-vbr-adaptive-off", "off", "vbr", "off"}, {"on-cbr-adaptive-on", "on", "cbr", "on"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSmartCodecFixture(t)
			f.video.BitrateType, f.video.AdaptiveMode = tc.rate, tc.adaptive
			if tc.mode == "on" {
				f.video.SmartCodec = "off"
			}
			before := f.video
			d, lease := f.base.driver(), f.base.lease()
			defer lease.Close()
			plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: tc.mode})
			if err != nil || plan.VideoMainBefore == nil || *plan.VideoMainBefore != before || plan.Before.SmartCodec != before.SmartCodec || plan.After.SmartCodec != tc.mode || len(plan.Changes) != 1 || plan.Changes[0] != "smart_codec" || f.writes != 0 || f.base.writes != 0 {
				t.Fatal("smart plan missing exact read-only snapshot")
			}
			got, err := d.ApplyInitialization(t.Context(), testHint(), lease, plan)
			after := before
			after.SmartCodec = tc.mode
			if err != nil || got.SmartCodec != tc.mode || f.video != after || f.writes != 1 || f.base.writes != 0 {
				t.Fatal("smart-only apply did not preserve main stream")
			}
			body := f.setBodies[0]
			var bitrate int
			if json.Unmarshal(body["bitrate"], &bitrate) != nil || bitrate != 4096 {
				t.Fatal("native bitrate must be numeric")
			}
			_, quality := body["quality"]
			_, adaptive := body["adaptive_mode"]
			if quality != (tc.rate == "vbr") || adaptive != (tc.adaptive == "on") {
				t.Fatal("native optional UI fields differ")
			}
			if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); err != nil || f.writes != 1 {
				t.Fatal("replay wrote main again")
			}
			if f.base.logins != f.base.logouts || f.base.unexpected != 0 {
				t.Fatal("session cleanup or unexpected device call")
			}
		})
	}
}

func TestSmartCodecDriftNeverOverwritesUnrelatedEncoderFields(t *testing.T) {
	for _, field := range []string{"encoding", "resolution", "fps", "bitrate_type", "quality", "bitrate", "adaptive"} {
		t.Run(field, func(t *testing.T) {
			f := newSmartCodecFixture(t)
			d, lease := f.base.driver(), f.base.lease()
			defer lease.Close()
			plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"})
			if err != nil {
				t.Fatal("fixture plan")
			}
			switch field {
			case "encoding":
				f.video.EncodeType = "H265"
			case "resolution":
				f.video.Resolution = "1280*720"
			case "fps":
				f.video.FrameRate = "65566"
			case "bitrate_type":
				f.video.BitrateType = "cbr"
			case "quality":
				f.video.Quality = "5"
			case "bitrate":
				f.video.Bitrate = "2048"
			case "adaptive":
				f.video.AdaptiveMode = "on"
			}
			if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); !errors.Is(err, camera.ErrConflict) || f.writes != 0 {
				t.Fatal("unrelated drift was overwritten")
			}
		})
	}
}

func TestSmartCodecReadbackAndLostResponse(t *testing.T) {
	for _, tc := range []struct {
		name                string
		ignore, drift, lost bool
	}{{"ignored", true, false, false}, {"unrelated-readback-drift", false, true, false}, {"lost-response", false, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSmartCodecFixture(t)
			d, lease := f.base.driver(), f.base.lease()
			defer lease.Close()
			plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"})
			if err != nil {
				t.Fatal("fixture plan")
			}
			f.ignoreWrite, f.driftReadback, f.loseReply = tc.ignore, tc.drift, tc.lost
			_, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan)
			want := camera.ErrConflict
			if tc.lost {
				want = onvif.ErrTransport
			}
			if !errors.Is(err, want) || f.writes != 1 {
				t.Fatal("ambiguous or mismatched write accepted")
			}
			if tc.lost {
				// No video write is needed once the requested mode is present.
				f.video.FrameRate = "65566"
				if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); err != nil || f.writes != 1 || f.video.FrameRate != "65566" {
					t.Fatal("lost reply recovery overwrote unrelated new state")
				}
			}
		})
	}
}

func TestSmartCodecFrequencyLostReplyPreservesPostFrequencyEncoder(t *testing.T) {
	f := newSmartCodecFixture(t)
	d, lease := f.base.driver(), f.base.lease()
	defer lease.Close()
	plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "off"})
	if err != nil {
		t.Fatal("fixture plan")
	}
	f.frequencyFrameRate = "65566"
	f.base.loseWriteReply = true
	if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); !errors.Is(err, onvif.ErrTransport) || f.writes != 1 || f.base.writes != 1 || f.video.SmartCodec != "off" {
		t.Fatal("combined fixture did not stop at lost frequency response")
	}
	got, err := d.ApplyInitialization(t.Context(), testHint(), lease, plan)
	if err != nil || got.PowerLineFrequency != 60 || got.SmartCodec != "off" || f.writes != 1 || f.base.writes != 1 || f.video.FrameRate != "65566" {
		t.Fatal("combined recovery replayed main or frequency write")
	}
}

func TestSmartCodecAlreadyTargetKeepsUnrelatedDriftWithoutWriting(t *testing.T) {
	f := newSmartCodecFixture(t)
	f.video.SmartCodec = "off"
	d, lease := f.base.driver(), f.base.lease()
	defer lease.Close()
	plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"})
	if err != nil || len(plan.Changes) != 0 || plan.VideoMainBefore == nil {
		t.Fatal("already-target read-only plan")
	}
	f.video.Bitrate = "2048"
	f.video.FrameRate = "65566"
	got, err := d.ApplyInitialization(t.Context(), testHint(), lease, plan)
	if err != nil || got.SmartCodec != "off" || f.writes != 0 || f.base.writes != 0 || f.video.Bitrate != "2048" || f.video.FrameRate != "65566" {
		t.Fatal("already-target no-op overwrote unrelated state")
	}
}

func TestSmartCodecRejectsFrequencyDriftBeforeAnySettingWrite(t *testing.T) {
	f := newSmartCodecFixture(t)
	f.base.frequency = "60hz"
	d, lease := f.base.driver(), f.base.lease()
	defer lease.Close()
	plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "off"})
	if err != nil || plan.Before.PowerLineFrequency != 60 || plan.After.PowerLineFrequency != 60 {
		t.Fatal("frequency-stable fixture plan")
	}
	f.base.frequency = "50hz"
	_, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan)
	if !errors.Is(err, camera.ErrConflict) || f.writes != 0 || f.base.writes != 0 || f.video.SmartCodec != "on" {
		t.Fatal("frequency drift caused a partial setting write")
	}
}

func TestSmartCodecOnlyKnownFirmwareAndBoundedPlan(t *testing.T) {
	for _, fw := range []string{"1.2.0 Build251020 Rel.55235n", "1.2.2 Build260625", "1.3.0 Build260624", "1.2.2 Build260624 Rel.unknown"} {
		f := newSmartCodecFixture(t)
		d, lease := f.base.driver(), f.base.lease()
		hint := testHint()
		hint.Firmware = fw
		if _, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{SmartCodec: "off"}); !errors.Is(err, camera.ErrUnsupported) || f.base.logins != 0 || f.writes != 0 {
			t.Fatal("unqualified smart-mode operation")
		}
		_ = lease.Close()
	}
	for _, bad := range []string{"malformed", "too-many"} {
		f := newSmartCodecFixture(t)
		d, lease := f.base.driver(), f.base.lease()
		f.malformed = bad == "malformed"
		f.tooManyFields = bad == "too-many"
		if _, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"}); err == nil || f.writes != 0 {
			t.Fatal("unbounded native video state accepted")
		}
		_ = lease.Close()
	}
	for _, codec := range []string{"h264", "H.264", "MJPEG", "H264plus"} {
		f := newSmartCodecFixture(t)
		f.video.EncodeType = codec
		d, lease := f.base.driver(), f.base.lease()
		if _, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"}); err == nil || f.writes != 0 {
			t.Fatal("unqualified native codec spelling accepted")
		}
		_ = lease.Close()
	}
	f := newSmartCodecFixture(t)
	d, lease := f.base.driver(), f.base.lease()
	defer lease.Close()
	plan, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"})
	if err != nil {
		t.Fatal("fixture plan")
	}
	plan.VideoMainBefore.SmartCodec = "off"
	if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); !errors.Is(err, camera.ErrInvalid) || f.writes != 0 {
		t.Fatal("plan snapshot escaped mode binding")
	}
}

func TestSmartCodecTenFieldResponseProjectsOnlyKnownWritableFields(t *testing.T) {
	f := newSmartCodecFixture(t)
	f.extraField = true
	d, lease := f.base.driver(), f.base.lease()
	defer lease.Close()
	plan, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{SmartCodec: "off"})
	if err != nil || plan.VideoMainBefore == nil {
		t.Fatal("known fields in ten-field response were rejected")
	}
	raw, err := json.Marshal(plan.VideoMainBefore)
	if err != nil {
		t.Fatal("snapshot serialization")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if len(fields) != 8 || bytes.Contains(raw, []byte("readonly_")) || bytes.Contains(raw, []byte("fixture-private")) {
		t.Fatal("response metadata entered durable snapshot")
	}
	if _, err = d.ApplyInitialization(t.Context(), testHint(), lease, plan); err != nil || f.writes != 1 {
		t.Fatal("ten-field response failed known-setting apply")
	}
	raw, _ = json.Marshal(f.setBodies[0])
	if bytes.Contains(raw, []byte("readonly_")) || bytes.Contains(raw, []byte("fixture-private")) {
		t.Fatal("response metadata entered setting payload")
	}
}
