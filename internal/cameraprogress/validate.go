package cameraprogress

import (
	"math"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func stage(v provision.Stage) bool {
	switch v {
	case provision.Pending, provision.Inspecting, provision.Planned, provision.Activating, provision.Initializing, provision.Applying, provision.Verifying, provision.Deploying, provision.Binding, provision.WaitingAuth, provision.WaitingInput, provision.Unsupported, provision.Failed, provision.Canceled, provision.Active:
		return true
	}
	return false
}
func uri(s string) bool {
	if s == "" {
		return true
	}
	if len(s) > 2048 || s != camera.SanitizeURI(s) {
		return false
	}
	u, err := url.Parse(s)
	if err != nil || u.User != nil || u.Fragment != "" || u.Hostname() == "" {
		return false
	}
	switch u.Scheme {
	case "http", "https", "rtsp", "rtsps":
	default:
		return false
	}
	// Match the central camera URI contract. A generic sanitizer recognizes
	// common secret names, but cannot classify a vendor's opaque auth selector.
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return false
	}
	for key, values := range query {
		switch strings.ToLower(key) {
		case "channel", "subtype", "stream", "streamid", "profile", "transport", "unicast", "proto":
		default:
			return false
		}
		for _, value := range values {
			if len(value) > 128 || strings.ContainsAny(value, "@:/\\\x00\r\n") {
				return false
			}
		}
	}
	return true
}
func validateRun(r provision.Run) error {
	if !stage(r.Stage) || r.CandidateIndex < 0 || r.CandidateIndex > 16 || len(r.Events) > 256 || len(r.Impacts) > 64 || len(r.Warnings) > 128 {
		return ErrInvalid
	}
	for _, event := range r.Events {
		if !stage(event.Stage) {
			return ErrInvalid
		}
	}
	for _, pinned := range []struct {
		ref     *camera.CredentialRef
		purpose camera.AuthPurpose
	}{{r.InitialRef, camera.AuthInitialAdmin}, {r.ManagementRef, camera.AuthManagement}, {r.StreamingRef, camera.AuthStreaming}} {
		if pinned.ref != nil && !pinned.ref.Authorizes(r.Request.Scope, pinned.purpose) {
			return ErrInvalid
		}
	}
	nodes, text := 0, 0
	var visit func(reflect.Value, int) bool
	visit = func(v reflect.Value, depth int) bool {
		nodes++
		if depth > 32 || nodes > 4096 {
			return false
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return true
			}
			return visit(v.Elem(), depth+1)
		}
		if v.Type() == reflect.TypeFor[time.Time]() {
			t := v.Interface().(time.Time)
			return t.IsZero() || timestamppb.New(t).CheckValid() == nil
		}
		if v.Type() == reflect.TypeFor[time.Duration]() {
			return v.Int() >= 0 && v.Int() <= int64(24*time.Hour)
		}
		if v.Type() == reflect.TypeFor[camera.Endpoint]() {
			e := v.Interface().(camera.Endpoint)
			if !uri(e.URL) || e.URL == "" {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.MediaProfile]() {
			p := v.Interface().(camera.MediaProfile)
			if !uri(p.StreamURI) || len(p.Evidence) > 32 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.DeviceHint]() {
			h := v.Interface().(camera.DeviceHint)
			if len(h.Endpoints) > 32 || len(h.Evidence) > 32 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.ProfileCapability]() {
			p := v.Interface().(camera.ProfileCapability)
			if len(p.Codecs) > 16 || len(p.Resolutions) > 64 || len(p.FrameRates) > 128 || len(p.Bitrates) > 128 || len(p.Evidence) > 32 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.Capabilities]() {
			p := v.Interface().(camera.Capabilities)
			if len(p.Profiles) > 64 || len(p.Evidence) > 32 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.CurrentState]() {
			if len(v.Interface().(camera.CurrentState).Profiles) > 64 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.VerifiedState]() {
			if len(v.Interface().(camera.VerifiedState).Evidence) > 32 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.DevicePreset]() {
			p := v.Interface().(camera.DevicePreset)
			if len(p.Fallbacks) > 16 || len(p.MatchedRules) > 64 || len(p.Required) > 16 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.PresetSettings]() {
			if len(v.Interface().(camera.PresetSettings).Preserve) > 16 {
				return false
			}
		}
		if v.Type() == reflect.TypeFor[camera.DeviceSettings]() {
			f := v.Interface().(camera.DeviceSettings).PowerLineFrequency
			if f != 0 && f != 50 && f != 60 {
				return false
			}
		}
		switch v.Kind() {
		case reflect.String:
			text += len(v.String())
			return len(v.String()) <= 2048 && text <= MaxStateBytes && utf8.ValidString(v.String())
		case reflect.Float64:
			return !math.IsNaN(v.Float()) && !math.IsInf(v.Float(), 0) && v.Float() >= 0 && v.Float() <= 1000
		case reflect.Int:
			return v.Int() >= 0 && v.Int() <= math.MaxInt32
		case reflect.Int64:
			return v.Int() >= 0 && v.Int() <= 10_000_000_000
		case reflect.Uint64, reflect.Bool:
			return true
		case reflect.Slice:
			if v.Len() > 256 {
				return false
			}
			for i := 0; i < v.Len(); i++ {
				if !visit(v.Index(i), depth+1) {
					return false
				}
			}
			return true
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if !visit(v.Field(i), depth+1) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !visit(reflect.ValueOf(r), 0) {
		return ErrInvalid
	}
	// Bound raw URLs and list counts before the portable validator parses them.
	if r.Request.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

// Validate wire shape before portable allocation; its limits also bound nested
// messages and forbid unknown future fields from being silently projected away.
func validateWire(v proto.Message) error {
	if v == nil {
		return ErrInvalid
	}
	nodes := 0
	var visit func(protoreflect.Message, int) bool
	visit = func(m protoreflect.Message, depth int) bool {
		nodes++
		if !m.IsValid() || depth > 32 || nodes > 4096 || len(m.GetUnknown()) > 0 {
			return false
		}
		if t, ok := m.Interface().(*timestamppb.Timestamp); ok {
			return t.CheckValid() == nil
		}
		valid := true
		m.Range(func(f protoreflect.FieldDescriptor, x protoreflect.Value) bool {
			if f.IsMap() {
				valid = false
				return false
			}
			check := func(x protoreflect.Value) bool {
				switch f.Kind() {
				case protoreflect.MessageKind:
					return visit(x.Message(), depth+1)
				case protoreflect.StringKind:
					return len(x.String()) <= 2048 && utf8.ValidString(x.String())
				case protoreflect.DoubleKind:
					return !math.IsNaN(x.Float()) && !math.IsInf(x.Float(), 0)
				}
				return true
			}
			if f.IsList() {
				l := x.List()
				if l.Len() > 256 {
					valid = false
					return false
				}
				for i := 0; i < l.Len(); i++ {
					if !check(l.Get(i)) {
						valid = false
						return false
					}
				}
			} else {
				valid = check(x)
			}
			return valid
		})
		return valid
	}
	if !visit(v.ProtoReflect(), 0) || proto.Size(v) > MaxStateBytes {
		return ErrInvalid
	}
	return nil
}
