// Package camerapreset translates typed central preset snapshots without
// changing the portable provider's matching, fallback or fingerprint semantics.
package camerapreset

import (
	"slices"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
)

func settings(v *api.CameraPresetSettings) camera.PresetSettings {
	x := v.GetVideo()
	return camera.PresetSettings{
		Initialization: camera.DeviceSettings{PowerLineFrequency: int(v.GetPowerLineFrequency())},
		Video:          camera.DesiredState{ProfileToken: x.GetProfileToken(), Codec: x.GetCodec(), Width: int(x.GetWidth()), Height: int(x.GetHeight()), FPS: x.GetFps(), Bitrate: x.GetBitrateBps(), GOP: int(x.GetGopFrames()), Revision: x.GetRevision(), AllowFallback: x.GetAllowFallback()},
		Preserve:       slices.Clone(v.GetPreserve()),
	}
}

func encodeSettings(v camera.PresetSettings) *api.CameraPresetSettings {
	x := v.Video
	return api.CameraPresetSettings_builder{
		PowerLineFrequency: int32(v.Initialization.PowerLineFrequency),
		Video:              api.CameraPresetVideo_builder{ProfileToken: x.ProfileToken, Codec: x.Codec, Width: int32(x.Width), Height: int32(x.Height), Fps: x.FPS, BitrateBps: x.Bitrate, GopFrames: int32(x.GOP), Revision: x.Revision, AllowFallback: x.AllowFallback}.Build(),
		Preserve:           slices.Clone(v.Preserve),
	}.Build()
}

func decodeRule(v *api.CameraPresetRule) preset.Rule {
	s, m := v.GetScope(), v.GetMatch()
	out := preset.Rule{
		ID:       v.GetId(),
		Scope:    camera.EnrollmentScope{TenantID: s.GetTenantId(), SiteID: s.GetSiteId(), SetID: s.GetSetId(), ProducerID: s.GetProducerId(), NetworkID: s.GetNetworkId()},
		Match:    preset.Match{DeviceID: camera.DeviceID(m.GetDeviceId()), Manufacturer: m.GetManufacturer(), Model: m.GetModel(), Hardware: m.GetHardware(), Firmware: m.GetFirmware(), Serial: m.GetSerial(), MAC: m.GetMac()},
		Settings: settings(v.GetSettings()), Required: slices.Clone(v.GetRequired()),
	}
	if alternatives := v.GetAlternatives(); alternatives != nil {
		out.Fallbacks = make([]camera.PresetSettings, len(alternatives.GetCandidates()))
		for i, candidate := range alternatives.GetCandidates() {
			out.Fallbacks[i] = settings(candidate)
		}
	}
	return out
}

// Provider uses the existing portable validator and resolver. The caller must
// first authorize access to this immutable row; conversion confers no grant.
func Provider(id, revision string, rules []*api.CameraPresetRule) (*preset.Provider, error) {
	config := preset.Config{ID: id, Revision: revision, Rules: make([]preset.Rule, len(rules))}
	for i, rule := range rules {
		if rule == nil {
			return nil, camera.ErrInvalid
		}
		for _, candidate := range rule.GetAlternatives().GetCandidates() {
			if candidate == nil {
				return nil, camera.ErrInvalid
			}
		}
		config.Rules[i] = decodeRule(rule)
	}
	return preset.New(config)
}

// EncodeRules returns a detached wire representation. Empty non-nil fallbacks
// deliberately produce a present wrapper with zero candidates.
func EncodeRules(rules []preset.Rule) ([]*api.CameraPresetRule, error) {
	// Validate the same source document before narrowing any Go int to int32.
	if _, err := preset.New(preset.Config{ID: "wire", Revision: "1", Rules: rules}); err != nil {
		return nil, err
	}
	out := make([]*api.CameraPresetRule, len(rules))
	for i, v := range rules {
		if !representable(v.Settings) {
			return nil, camera.ErrInvalid
		}
		s, m := v.Scope, v.Match
		r := api.CameraPresetRule_builder{
			Id:       v.ID,
			Scope:    api.CameraPresetScope_builder{TenantId: s.TenantID, SiteId: s.SiteID, SetId: s.SetID, ProducerId: s.ProducerID, NetworkId: s.NetworkID}.Build(),
			Match:    api.CameraPresetMatch_builder{DeviceId: string(m.DeviceID), Manufacturer: m.Manufacturer, Model: m.Model, Hardware: m.Hardware, Firmware: m.Firmware, Serial: m.Serial, Mac: m.MAC}.Build(),
			Settings: encodeSettings(v.Settings), Required: slices.Clone(v.Required),
		}.Build()
		if v.Fallbacks != nil {
			candidates := make([]*api.CameraPresetSettings, len(v.Fallbacks))
			for j, candidate := range v.Fallbacks {
				if !representable(candidate) {
					return nil, camera.ErrInvalid
				}
				candidates[j] = encodeSettings(candidate)
			}
			r.SetAlternatives(api.CameraPresetAlternatives_builder{Candidates: candidates}.Build())
		}
		out[i] = r
	}
	return out, nil
}

func representable(v camera.PresetSettings) bool {
	const max = 1<<31 - 1
	return v.Video.Width <= max && v.Video.Height <= max && v.Video.GOP <= max
}

func EncodeSnapshot(v camera.DevicePreset) (*api.CameraResolvedPreset, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if !representable(v.Requested) {
		return nil, camera.ErrInvalid
	}
	alternatives := make([]*api.CameraPresetSettings, len(v.Fallbacks))
	for i, candidate := range v.Fallbacks {
		if !representable(candidate) {
			return nil, camera.ErrInvalid
		}
		alternatives[i] = encodeSettings(candidate)
	}
	return api.CameraResolvedPreset_builder{Id: v.Ref.ID, Revision: v.Ref.Revision, MatchedRules: slices.Clone(v.MatchedRules), Requested: encodeSettings(v.Requested), Alternatives: alternatives, Required: slices.Clone(v.Required)}.Build(), nil
}

func DecodeSnapshot(v *api.CameraResolvedPreset) (camera.DevicePreset, error) {
	if v == nil {
		return camera.DevicePreset{}, camera.ErrInvalid
	}
	out := camera.DevicePreset{Ref: camera.PresetRef{ID: v.GetId(), Revision: v.GetRevision()}, MatchedRules: slices.Clone(v.GetMatchedRules()), Requested: settings(v.GetRequested()), Required: slices.Clone(v.GetRequired())}
	for _, candidate := range v.GetAlternatives() {
		if candidate == nil {
			return camera.DevicePreset{}, camera.ErrInvalid
		}
		out.Fallbacks = append(out.Fallbacks, settings(candidate))
	}
	if err := out.Validate(); err != nil {
		return camera.DevicePreset{}, err
	}
	return out, nil
}
