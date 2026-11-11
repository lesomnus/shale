package camera

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

var ErrPresetNotFound = errors.New("camera preset not found")

// PresetRef names an immutable version of a preset or composed ruleset. A
// provider must return the same settings for the same reference. Enrollment
// persists the complete resolved snapshot before making configuration writes.
type PresetRef struct {
	ID       string `json:"id" yaml:"id"`
	Revision string `json:"revision" yaml:"revision"`
}

// DeviceSettings are prerequisites to media planning. Zero preserves the
// current setting. Changing power-line frequency can change the frame rates
// reported by a device, so media capabilities must be inspected again afterward.
type DeviceSettings struct {
	PowerLineFrequency int    `json:"power_line_frequency,omitempty" yaml:"power_line_frequency,omitempty"`
	SmartCodec         string `json:"smart_codec,omitempty" yaml:"smart_codec,omitempty"`
}

func (s DeviceSettings) Validate() error {
	if s.PowerLineFrequency != 0 && s.PowerLineFrequency != 50 && s.PowerLineFrequency != 60 || s.SmartCodec != "" && s.SmartCodec != "off" && s.SmartCodec != "on" {
		return ErrInvalid
	}
	return nil
}

type PresetSettings struct {
	Initialization DeviceSettings `json:"initialization" yaml:"initialization"`
	Video          DesiredState   `json:"video" yaml:"video"`
	// Preserve is patch policy: leave these fields at their current device
	// values instead of inheriting a requested value. Materialized settings
	// clear this list and use zero desired fields for preservation.
	Preserve []string `json:"preserve,omitempty" yaml:"preserve,omitempty"`
}

// DevicePreset preserves what was requested separately from any selected
// alternative. Fallbacks are ordered patches over Requested: zero fields keep
// the requested value. There is no implicit downgrade or unbounded search.
// Required lists fields that alternatives must preserve exactly.
type DevicePreset struct {
	Ref          PresetRef        `json:"ref" yaml:"ref"`
	MatchedRules []string         `json:"matched_rules,omitempty" yaml:"matched_rules,omitempty"`
	Requested    PresetSettings   `json:"requested" yaml:"requested"`
	Fallbacks    []PresetSettings `json:"fallbacks,omitempty" yaml:"fallbacks,omitempty"`
	Required     []string         `json:"required,omitempty" yaml:"required,omitempty"`
	// Firmware is applied before any setting, and only to an exact source build.
	Firmware *FirmwareUpgrade `json:"firmware,omitempty" yaml:"firmware,omitempty"`
}

// DevicePresetProvider selects policy for an already authorized scope. Device
// hints are matching inputs, never authorization or an inventory identity claim.
// Implementations must be read-only and return snapshots owned by the caller.
type DevicePresetProvider interface {
	Resolve(context.Context, EnrollmentScope, DeviceHint) (DevicePreset, error)
}

type InitializationPlan struct {
	Before          DeviceSettings           `json:"before"`
	After           DeviceSettings           `json:"after"`
	Changes         []string                 `json:"changes,omitempty"`
	Revision        string                   `json:"revision"`
	VideoMainBefore *InitializationVideoMain `json:"video_main_before,omitempty"`
}

// DeviceInitializer applies prerequisites after authentication and before media
// planning. PlanInitialization is read-only. ApplyInitialization must recheck
// its precondition and read back the resulting state. It does not factory reset
// a device or replace the first-password Bootstrapper contract.
type DeviceInitializer interface {
	PlanInitialization(context.Context, DeviceHint, CredentialLease, DeviceSettings) (InitializationPlan, error)
	ApplyInitialization(context.Context, DeviceHint, CredentialLease, InitializationPlan) (DeviceSettings, error)
}

// PresetWarning is durable enrollment state, suitable for an API response and
// an operator-facing warning. Requested and Applied contain typed setting
// values formatted as text, not raw vendor responses or credential material.
type PresetWarning struct {
	Code      string `json:"code"`
	Field     string `json:"field"`
	Requested string `json:"requested"`
	Applied   string `json:"applied"`
	Reason    string `json:"reason"`
}

const (
	PresetFieldFrequency  = "initialization.power_line_frequency"
	PresetFieldSmartCodec = "initialization.smart_codec"
	PresetFieldCodec      = "video.codec"
	PresetFieldResolution = "video.resolution"
	PresetFieldFPS        = "video.fps"
	PresetFieldBitrate    = "video.bitrate"
	PresetFieldGOP        = "video.gop"
	PresetFieldProfile    = "video.profile_token"
)

// ValidateSettings accepts partial patches. A resolution, when specified, must
// include both dimensions. Revision and AllowFallback are workflow metadata;
// neither may authorize fallback outside the explicit candidate list.
func ValidatePresetSettings(s PresetSettings) error {
	if err := s.Initialization.Validate(); err != nil {
		return fmt.Errorf("%w: invalid device initialization setting", ErrInvalid)
	}
	v := s.Video
	if v.Width < 0 || v.Height < 0 || (v.Width == 0) != (v.Height == 0) || v.FPS < 0 || math.IsNaN(v.FPS) || math.IsInf(v.FPS, 0) || v.Bitrate < 0 || v.GOP < 0 {
		return fmt.Errorf("%w: invalid preset media settings", ErrInvalid)
	}
	if v.Codec != "" && v.Codec != "h264" && v.Codec != "h265" && v.Codec != "mjpeg" {
		return fmt.Errorf("%w: invalid preset codec", ErrInvalid)
	}
	seen := make(map[string]bool, len(s.Preserve))
	for _, f := range s.Preserve {
		if seen[f] || !knownPresetField(f) || presetField(s, f) != "" {
			return fmt.Errorf("%w: invalid or conflicting preserved preset field", ErrInvalid)
		}
		seen[f] = true
	}
	return nil
}

// MergePresetSettings applies nonzero fields from override. It is also the
// documented interpretation of model/device overrides and fallback patches.
func MergePresetSettings(base, override PresetSettings) PresetSettings {
	if override.Initialization.PowerLineFrequency != 0 {
		base.Initialization.PowerLineFrequency = override.Initialization.PowerLineFrequency
	}
	if override.Initialization.SmartCodec != "" {
		base.Initialization.SmartCodec = override.Initialization.SmartCodec
	}
	v, o := &base.Video, override.Video
	if o.ProfileToken != "" {
		v.ProfileToken = o.ProfileToken
	}
	if o.Codec != "" {
		v.Codec = o.Codec
	}
	if o.Width != 0 {
		v.Width = o.Width
	}
	if o.Height != 0 {
		v.Height = o.Height
	}
	if o.FPS != 0 {
		v.FPS = o.FPS
	}
	if o.Bitrate != 0 {
		v.Bitrate = o.Bitrate
	}
	if o.GOP != 0 {
		v.GOP = o.GOP
	}
	for _, f := range override.Preserve {
		switch f {
		case PresetFieldFrequency:
			base.Initialization.PowerLineFrequency = 0
		case PresetFieldSmartCodec:
			base.Initialization.SmartCodec = ""
		case PresetFieldProfile:
			v.ProfileToken = ""
		case PresetFieldCodec:
			v.Codec = ""
		case PresetFieldResolution:
			v.Width, v.Height = 0, 0
		case PresetFieldFPS:
			v.FPS = 0
		case PresetFieldBitrate:
			v.Bitrate = 0
		case PresetFieldGOP:
			v.GOP = 0
		}
	}
	base.Preserve = nil
	// Preset revisions are set by Candidates from the immutable reference.
	v.AllowFallback = false
	v.Revision = ""
	return base
}

func (p DevicePreset) Validate() error {
	if strings.TrimSpace(p.Ref.ID) == "" || strings.TrimSpace(p.Ref.Revision) == "" || len(p.Fallbacks) > 16 {
		return fmt.Errorf("%w: invalid preset reference or candidate count", ErrInvalid)
	}
	if err := ValidatePresetSettings(p.Requested); err != nil {
		return err
	}
	if p.Firmware != nil {
		if err := p.Firmware.Validate(); err != nil {
			return err
		}
	}
	if len(p.Requested.Preserve) != 0 {
		return fmt.Errorf("%w: requested preset must be materialized", ErrInvalid)
	}
	seen := make(map[string]bool, len(p.Required))
	for _, f := range p.Required {
		if seen[f] || !knownPresetField(f) {
			return fmt.Errorf("%w: invalid required preset field", ErrInvalid)
		}
		seen[f] = true
		if presetField(p.Requested, f) == "" {
			return fmt.Errorf("%w: required preset field has no requested value", ErrInvalid)
		}
	}
	for _, patch := range p.Fallbacks {
		if err := ValidatePresetSettings(patch); err != nil {
			return err
		}
		effective := MergePresetSettings(p.Requested, patch)
		for _, f := range p.Required {
			if presetField(p.Requested, f) != presetField(effective, f) {
				return fmt.Errorf("%w: fallback changes required preset field", ErrInvalid)
			}
		}
	}
	return nil
}

// Candidates returns fully materialized, independent candidates in preference
// order. Revision belongs to the snapshot, even when a patch contains metadata
// copied from an older configuration.
func (p DevicePreset) Candidates() ([]PresetSettings, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	out := make([]PresetSettings, 0, len(p.Fallbacks)+1)
	primary := MergePresetSettings(PresetSettings{}, p.Requested)
	primary.Video.Revision = PresetRevision(p.Ref)
	out = append(out, primary)
	for _, patch := range p.Fallbacks {
		candidate := MergePresetSettings(primary, patch)
		candidate.Video.Revision = primary.Video.Revision
		if !slices.ContainsFunc(out, func(previous PresetSettings) bool { return reflect.DeepEqual(previous, candidate) }) {
			out = append(out, candidate)
		}
	}
	return out, nil
}

// PresetRevision identifies the entire immutable reference without delimiter
// ambiguities between an ID and its revision. It is used by media verification
// as well as the selected configuration's deployment fingerprint.
func PresetRevision(ref PresetRef) string {
	encoded, _ := json.Marshal(ref)
	digest := sha256.Sum256(encoded)
	return "preset:" + hex.EncodeToString(digest[:])
}

// Clone prevents a provider's internal rule slices from becoming mutable
// enrollment state shared between callers.
func (p DevicePreset) Clone() DevicePreset {
	p.MatchedRules = slices.Clone(p.MatchedRules)
	p.Fallbacks = slices.Clone(p.Fallbacks)
	p.Requested.Preserve = slices.Clone(p.Requested.Preserve)
	for i := range p.Fallbacks {
		p.Fallbacks[i].Preserve = slices.Clone(p.Fallbacks[i].Preserve)
	}
	p.Required = slices.Clone(p.Required)
	p.Firmware = p.Firmware.Clone()
	return p
}

// WarningsFor describes a deliberate candidate choice. A caller should set or
// supplement Reason with the rejected candidate's structured failure category.
func WarningsFor(requested, effective PresetSettings) []PresetWarning {
	var out []PresetWarning
	for _, f := range []string{PresetFieldFrequency, PresetFieldSmartCodec, PresetFieldProfile, PresetFieldCodec, PresetFieldResolution, PresetFieldFPS, PresetFieldBitrate, PresetFieldGOP} {
		want, got := presetField(requested, f), presetField(effective, f)
		if want != got {
			if got == "" {
				got = "preserved"
			}
			out = append(out, PresetWarning{Code: "preset_adjusted", Field: f, Requested: want, Applied: got, Reason: "selected an explicitly allowed fallback"})
		}
	}
	return out
}

func knownPresetField(f string) bool {
	return slices.Contains([]string{PresetFieldFrequency, PresetFieldSmartCodec, PresetFieldProfile, PresetFieldCodec, PresetFieldResolution, PresetFieldFPS, PresetFieldBitrate, PresetFieldGOP}, f)
}

func presetField(s PresetSettings, f string) string {
	integer := func(n int64) string {
		if n == 0 {
			return ""
		}
		return strconv.FormatInt(n, 10)
	}
	switch f {
	case PresetFieldFrequency:
		return integer(int64(s.Initialization.PowerLineFrequency))
	case PresetFieldSmartCodec:
		return s.Initialization.SmartCodec
	case PresetFieldProfile:
		return s.Video.ProfileToken
	case PresetFieldCodec:
		return s.Video.Codec
	case PresetFieldResolution:
		if s.Video.Width == 0 && s.Video.Height == 0 {
			return ""
		}
		return strconv.Itoa(s.Video.Width) + "x" + strconv.Itoa(s.Video.Height)
	case PresetFieldFPS:
		if s.Video.FPS == 0 {
			return ""
		}
		return strconv.FormatFloat(s.Video.FPS, 'f', -1, 64)
	case PresetFieldBitrate:
		return integer(s.Video.Bitrate)
	case PresetFieldGOP:
		return integer(int64(s.Video.GOP))
	default:
		return ""
	}
}
