// Package preset provides a read-only, scoped implementation of the camera
// preset contract. A configuration is copied at construction and never reloaded
// during an enrollment; callers persist the returned snapshot for retries.
package preset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"

	"github.com/lesomnus/shale/camera"
	"go.yaml.in/yaml/v3"
)

const (
	maxFileBytes = 1 << 20
	maxRules     = 1024
)

type Config struct {
	ID       string `json:"id" yaml:"id"`
	Revision string `json:"revision" yaml:"revision"`
	Rules    []Rule `json:"rules" yaml:"rules"`
}

// Rule.Scope is a selector within an explicitly named tenant. Empty optional
// scope fields are wildcards, never a request to infer authorization from a
// device's discovery response. MAC rules must name a network scope.
//
// Rules are applied from least to most specific: tenant/site defaults,
// manufacturer, model/hardware/firmware, serial/MAC, then inventory device ID.
// Within a level, more scope and match fields are more specific. Different
// patches at equal specificity are a conflict rather than a file-order choice.
type Rule struct {
	ID        string                  `json:"id" yaml:"id"`
	Scope     camera.EnrollmentScope  `json:"scope" yaml:"scope"`
	Match     Match                   `json:"match,omitempty" yaml:"match,omitempty"`
	Settings  camera.PresetSettings   `json:"settings" yaml:"settings"`
	Fallbacks []camera.PresetSettings `json:"fallbacks,omitempty" yaml:"fallbacks,omitempty"`
	Required  []string                `json:"required,omitempty" yaml:"required,omitempty"`
}

type Match struct {
	DeviceID     camera.DeviceID `json:"device_id,omitempty" yaml:"device_id,omitempty"`
	Manufacturer string          `json:"manufacturer,omitempty" yaml:"manufacturer,omitempty"`
	Model        string          `json:"model,omitempty" yaml:"model,omitempty"`
	Hardware     string          `json:"hardware,omitempty" yaml:"hardware,omitempty"`
	Firmware     string          `json:"firmware,omitempty" yaml:"firmware,omitempty"`
	Serial       string          `json:"serial,omitempty" yaml:"serial,omitempty"`
	MAC          string          `json:"mac,omitempty" yaml:"mac,omitempty"`
}

type Provider struct {
	config Config
	ref    camera.PresetRef
}

var _ camera.DevicePresetProvider = (*Provider)(nil)

func New(config Config) (*Provider, error) {
	if strings.TrimSpace(config.ID) == "" || strings.TrimSpace(config.Revision) == "" || len(config.Rules) == 0 || len(config.Rules) > maxRules {
		return nil, fmt.Errorf("%w: invalid preset ruleset reference or rule count", camera.ErrInvalid)
	}
	config.Rules = slices.Clone(config.Rules)
	seen := make(map[string]bool, len(config.Rules))
	for i := range config.Rules {
		rule := &config.Rules[i]
		if strings.TrimSpace(rule.ID) == "" || seen[rule.ID] || strings.TrimSpace(rule.Scope.TenantID) == "" {
			return nil, fmt.Errorf("%w: preset rule needs a unique ID and tenant scope", camera.ErrInvalid)
		}
		seen[rule.ID] = true
		if rule.Match.MAC != "" {
			mac, err := NormalizeMAC(rule.Match.MAC)
			if err != nil || rule.Scope.NetworkID == "" {
				return nil, fmt.Errorf("%w: preset MAC rule requires an Ethernet MAC and network scope", camera.ErrInvalid)
			}
			rule.Match.MAC = mac
		}
		if err := camera.ValidatePresetSettings(rule.Settings); err != nil {
			return nil, err
		}
		if len(rule.Fallbacks) > 16 {
			return nil, fmt.Errorf("%w: too many preset fallback candidates", camera.ErrInvalid)
		}
		for _, s := range rule.Fallbacks {
			if err := camera.ValidatePresetSettings(s); err != nil {
				return nil, err
			}
		}
		seenRequired := make(map[string]bool, len(rule.Required))
		for _, field := range rule.Required {
			if seenRequired[field] || !knownField(field) {
				return nil, fmt.Errorf("%w: invalid required preset field", camera.ErrInvalid)
			}
			seenRequired[field] = true
		}
		rule.Fallbacks = slices.Clone(rule.Fallbacks)
		rule.Settings.Preserve = slices.Clone(rule.Settings.Preserve)
		slices.Sort(rule.Settings.Preserve)
		for i := range rule.Fallbacks {
			rule.Fallbacks[i].Preserve = slices.Clone(rule.Fallbacks[i].Preserve)
			slices.Sort(rule.Fallbacks[i].Preserve)
		}
		rule.Required = slices.Clone(rule.Required)
		slices.Sort(rule.Required)
	}
	// Rule file order is not policy. Required is a set, but candidate order and
	// the distinction between absent and explicitly empty fallbacks are policy.
	slices.SortFunc(config.Rules, func(a, b Rule) int { return strings.Compare(a.ID, b.ID) })
	type canonicalRule struct {
		Rule
		HasFallbacks bool `json:"has_fallbacks"`
	}
	canonical := struct {
		ID       string          `json:"id"`
		Revision string          `json:"revision"`
		Rules    []canonicalRule `json:"rules"`
	}{ID: config.ID, Revision: config.Revision}
	for _, rule := range config.Rules {
		canonical.Rules = append(canonical.Rules, canonicalRule{Rule: rule, HasFallbacks: rule.Fallbacks != nil})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot fingerprint preset configuration", camera.ErrInvalid)
	}
	digest := sha256.Sum256(encoded)
	ref := camera.PresetRef{ID: config.ID, Revision: config.Revision + "~" + hex.EncodeToString(digest[:])}
	return &Provider{config: config, ref: ref}, nil
}

// Load reads YAML or JSON (a YAML subset), rejecting unknown fields, trailing
// documents, and files larger than one MiB. The file contains policy only;
// credentials belong to an injected camera.AuthProvider.
func Load(path string) (*Provider, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxFileBytes {
		return nil, fmt.Errorf("%w: invalid preset file", camera.ErrInvalid)
	}
	return Decode(f)
}

func Decode(reader io.Reader) (*Provider, error) {
	bytes, err := io.ReadAll(io.LimitReader(reader, maxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(bytes) > maxFileBytes {
		return nil, fmt.Errorf("%w: preset file is too large", camera.ErrInvalid)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(bytes)))
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("%w: invalid preset document", camera.ErrInvalid)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: preset file must contain one document", camera.ErrInvalid)
	}
	return New(config)
}

func (p *Provider) Resolve(ctx context.Context, scope camera.EnrollmentScope, device camera.DeviceHint) (camera.DevicePreset, error) {
	if err := ctx.Err(); err != nil {
		return camera.DevicePreset{}, err
	}
	if p == nil || scope.TenantID == "" || scope.SetID == "" || scope.ProducerID == "" || scope.NetworkID == "" {
		return camera.DevicePreset{}, camera.ErrInvalid
	}
	var matching []Rule
	for _, rule := range p.config.Rules {
		if !scopeMatches(rule.Scope, scope) || !rule.Match.matches(device) {
			continue
		}
		matching = append(matching, rule)
	}
	if len(matching) == 0 {
		return camera.DevicePreset{}, camera.ErrPresetNotFound
	}
	slices.SortFunc(matching, func(a, b Rule) int {
		if c := compareRank(rank(a), rank(b)); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	out := camera.DevicePreset{Ref: p.ref}
	for i, rule := range matching {
		if i > 0 && compareRank(rank(matching[i-1]), rank(rule)) == 0 && !samePatch(matching[i-1], rule) {
			return camera.DevicePreset{}, fmt.Errorf("%w: equally specific camera preset rules", camera.ErrConflict)
		}
		out.MatchedRules = append(out.MatchedRules, rule.ID)
		out.Requested = camera.MergePresetSettings(out.Requested, rule.Settings)
		if rule.Fallbacks != nil {
			out.Fallbacks = camera.DevicePreset{Fallbacks: rule.Fallbacks}.Clone().Fallbacks
		}
		for _, field := range rule.Required {
			if !slices.Contains(out.Required, field) {
				out.Required = append(out.Required, field)
			}
		}
	}
	if err := out.Validate(); err != nil {
		return camera.DevicePreset{}, err
	}
	// A ruleset can select different presets for different devices. Include the
	// resolved match/settings in the snapshot reference while retaining the
	// entire ruleset digest, so both edits and a different match get new refs.
	encoded, err := json.Marshal(out)
	if err != nil {
		return camera.DevicePreset{}, fmt.Errorf("%w: cannot fingerprint resolved preset", camera.ErrInvalid)
	}
	digest := sha256.Sum256(encoded)
	out.Ref.Revision = p.config.Revision + "~" + hex.EncodeToString(digest[:])
	return out.Clone(), nil
}

// NormalizeMAC supports colon, hyphen and dotted Ethernet spellings. EUI-64
// and non-Ethernet identifiers are rejected instead of matching ambiguously.
func NormalizeMAC(raw string) (string, error) {
	mac, err := net.ParseMAC(strings.TrimSpace(raw))
	if err != nil || len(mac) != 6 {
		return "", camera.ErrInvalid
	}
	return mac.String(), nil
}

func scopeMatches(selector, scope camera.EnrollmentScope) bool {
	return selector.TenantID == scope.TenantID &&
		(selector.SiteID == "" || selector.SiteID == scope.SiteID) &&
		(selector.SetID == "" || selector.SetID == scope.SetID) &&
		(selector.ProducerID == "" || selector.ProducerID == scope.ProducerID) &&
		(selector.NetworkID == "" || selector.NetworkID == scope.NetworkID)
}

func (m Match) matches(d camera.DeviceHint) bool {
	if (m.DeviceID != "" && m.DeviceID != d.ID) ||
		(m.Manufacturer != "" && m.Manufacturer != d.Manufacturer) ||
		(m.Model != "" && m.Model != d.Model) ||
		(m.Hardware != "" && m.Hardware != d.Hardware) ||
		(m.Firmware != "" && m.Firmware != d.Firmware) ||
		(m.Serial != "" && m.Serial != d.Serial) {
		return false
	}
	if m.MAC != "" {
		mac, err := NormalizeMAC(d.MAC)
		if err != nil || mac != m.MAC {
			return false
		}
	}
	return true
}

// identity tier, scope specificity, device predicate specificity.
func rank(r Rule) [3]int {
	tier := 0
	switch {
	case r.Match.DeviceID != "":
		tier = 4
	case r.Match.Serial != "" || r.Match.MAC != "":
		tier = 3
	case r.Match.Model != "" || r.Match.Hardware != "" || r.Match.Firmware != "":
		tier = 2
	case r.Match.Manufacturer != "":
		tier = 1
	}
	nonempty := func(values ...string) int {
		n := 0
		for _, v := range values {
			if v != "" {
				n++
			}
		}
		return n
	}
	return [3]int{tier,
		nonempty(r.Scope.SiteID, r.Scope.SetID, r.Scope.ProducerID, r.Scope.NetworkID),
		nonempty(string(r.Match.DeviceID), r.Match.Manufacturer, r.Match.Model, r.Match.Hardware, r.Match.Firmware, r.Match.Serial, r.Match.MAC)}
}

func compareRank(a, b [3]int) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

func samePatch(a, b Rule) bool {
	return reflect.DeepEqual(a.Settings, b.Settings) && reflect.DeepEqual(a.Fallbacks, b.Fallbacks) && slices.Equal(a.Required, b.Required)
}

func knownField(field string) bool {
	return slices.Contains([]string{camera.PresetFieldFrequency, camera.PresetFieldProfile, camera.PresetFieldCodec, camera.PresetFieldResolution, camera.PresetFieldFPS, camera.PresetFieldBitrate, camera.PresetFieldGOP}, field)
}
