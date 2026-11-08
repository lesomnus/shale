// Package camera defines portable camera operations. It has no dependency on the
// Shale CLI, control-plane services or database. Implementations must not make
// camera writes as a side effect of inspection or authentication.
package camera

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"
)

type DeviceID string

// EnrollmentScope is authorization supplied by the caller, never inferred from
// an unauthenticated discovery response.
type EnrollmentScope struct {
	TenantID   string `json:"tenant_id" yaml:"tenant_id"`
	SiteID     string `json:"site_id,omitempty" yaml:"site_id,omitempty"`
	SetID      string `json:"set_id" yaml:"set_id"`
	ProducerID string `json:"producer_id" yaml:"producer_id"`
	NetworkID  string `json:"network_id" yaml:"network_id"`
}

type Endpoint struct {
	URL          string `json:"url" yaml:"url"`
	NetworkScope string `json:"network_scope,omitempty" yaml:"network_scope,omitempty"`
}

func (e Endpoint) MarshalJSON() ([]byte, error) {
	type plain Endpoint
	e.URL = SanitizeURI(e.URL)
	return json.Marshal(plain(e))
}

func (e Endpoint) MarshalYAML() (any, error) {
	type plain Endpoint
	e.URL = SanitizeURI(e.URL)
	return plain(e), nil
}

// Evidence describes a fact's origin; free-form protocol responses and secrets
// deliberately do not belong here.
type Evidence struct {
	Source        string    `json:"source"`
	At            time.Time `json:"at"`
	Firmware      string    `json:"firmware,omitempty"`
	Authenticated bool      `json:"authenticated"`
}

// DeviceHint is a candidate until authenticated evidence is explicitly claimed.
// ID, when set, is the inventory's assigned identity, not an IP or serial hash.
type DeviceHint struct {
	ID           DeviceID   `json:"id,omitempty"`
	CandidateID  string     `json:"candidate_id,omitempty"`
	Endpoints    []Endpoint `json:"endpoints"`
	Manufacturer string     `json:"manufacturer,omitempty"`
	Model        string     `json:"model,omitempty"`
	Hardware     string     `json:"hardware,omitempty"`
	Firmware     string     `json:"firmware,omitempty"`
	Serial       string     `json:"serial,omitempty"`
	MAC          string     `json:"mac,omitempty"`
	DiscoveryID  string     `json:"discovery_id,omitempty"`
	Evidence     []Evidence `json:"evidence,omitempty"`
}

// DiscoveryScope is selected by an authorized caller. An empty interface uses
// the OS routing table; explicit addresses work without multicast.
type DiscoveryScope struct {
	ID           string        `json:"id"`
	Interface    string        `json:"interface,omitempty"`
	LocalAddress string        `json:"local_address,omitempty"`
	Seeds        []Endpoint    `json:"seeds,omitempty"`
	Timeout      time.Duration `json:"timeout"`
	Multicast    bool          `json:"multicast"`
}

type Observation struct {
	Device     DeviceHint `json:"device"`
	Scope      string     `json:"scope"`
	ObservedAt time.Time  `json:"observed_at"`
}

type Discoverer interface {
	Discover(context.Context, DiscoveryScope) ([]Observation, error)
}

type ActivationState string

const (
	ActivationUnknown     ActivationState = "unknown"
	ActivationRequired    ActivationState = "activation_required"
	ActivationReady       ActivationState = "ready"
	ActivationUnsupported ActivationState = "unsupported"
)

type Resolution struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// ProfileCapability is what a profile can accept, distinct from its current
// configuration. Empty ranges/lists mean unknown rather than unlimited.
type ProfileCapability struct {
	Token        string       `json:"token"`
	Codecs       []string     `json:"codecs,omitempty"`
	Resolutions  []Resolution `json:"resolutions,omitempty"`
	FrameRates   []float64    `json:"frame_rates,omitempty"`
	Bitrates     []int64      `json:"bitrates,omitempty"`
	FrameRateMin float64      `json:"frame_rate_min,omitempty"`
	FrameRateMax float64      `json:"frame_rate_max,omitempty"`
	BitrateMin   int64        `json:"bitrate_min,omitempty"`
	BitrateMax   int64        `json:"bitrate_max,omitempty"`
	GOPMin       int          `json:"gop_min,omitempty"`
	GOPMax       int          `json:"gop_max,omitempty"`
	Evidence     []Evidence   `json:"evidence,omitempty"`
}

type Capabilities struct {
	Profiles []ProfileCapability `json:"profiles,omitempty"`
	Evidence []Evidence          `json:"evidence,omitempty"`
}

type MediaProfile struct {
	Token     string     `json:"token"`
	Name      string     `json:"name,omitempty"`
	Codec     string     `json:"codec,omitempty"`
	Width     int        `json:"width,omitempty"`
	Height    int        `json:"height,omitempty"`
	FPS       float64    `json:"fps,omitempty"`
	Bitrate   int64      `json:"bitrate,omitempty"`
	GOP       int        `json:"gop,omitempty"`
	StreamURI string     `json:"stream_uri,omitempty"`
	Evidence  []Evidence `json:"evidence,omitempty"`
}

func (p MediaProfile) MarshalJSON() ([]byte, error) {
	type plain MediaProfile
	p.StreamURI = SanitizeURI(p.StreamURI)
	return json.Marshal(plain(p))
}

func (p MediaProfile) MarshalYAML() (any, error) {
	type plain MediaProfile
	p.StreamURI = SanitizeURI(p.StreamURI)
	return plain(p), nil
}

type CurrentState struct {
	Profiles   []MediaProfile `json:"profiles,omitempty"`
	Firmware   string         `json:"firmware,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
}

type DesiredState struct {
	ProfileToken  string  `json:"profile_token,omitempty" yaml:"profile_token,omitempty"`
	Codec         string  `json:"codec,omitempty" yaml:"codec,omitempty"`
	Width         int     `json:"width,omitempty" yaml:"width,omitempty"`
	Height        int     `json:"height,omitempty" yaml:"height,omitempty"`
	FPS           float64 `json:"fps,omitempty" yaml:"fps,omitempty"`
	Bitrate       int64   `json:"bitrate,omitempty" yaml:"bitrate,omitempty"`
	GOP           int     `json:"gop,omitempty" yaml:"gop,omitempty"`
	AllowFallback bool    `json:"allow_fallback,omitempty" yaml:"allow_fallback,omitempty"`
	Revision      string  `json:"revision" yaml:"revision,omitempty"`
}

type Inspection struct {
	Device       DeviceHint      `json:"device"`
	Activation   ActivationState `json:"activation"`
	Capabilities Capabilities    `json:"capabilities"`
	Current      CurrentState    `json:"current"`
}

type ChangePlan struct {
	ProfileToken string       `json:"profile_token"`
	Before       MediaProfile `json:"before"`
	After        MediaProfile `json:"after"`
	Changes      []string     `json:"changes,omitempty"`
	Revision     string       `json:"revision"`
}

type Inspector interface {
	Inspect(context.Context, DeviceHint, CredentialLease) (Inspection, error)
}

// Bootstrapper only performs supported first-time activation. It deliberately
// contains neither a factory reset nor a password-guessing operation.
type Bootstrapper interface {
	Activation(context.Context, DeviceHint) (ActivationState, error)
	Activate(context.Context, DeviceHint, CredentialLease) error
}

type VideoConfigurator interface {
	Plan(context.Context, Inspection, DesiredState) (ChangePlan, error)
	Apply(context.Context, DeviceHint, CredentialLease, ChangePlan) (CurrentState, error)
}

type MediaRequest struct {
	Scope         EnrollmentScope `json:"scope"`
	Device        DeviceHint      `json:"device"`
	Profile       MediaProfile    `json:"profile"`
	Desired       DesiredState    `json:"desired"`
	CredentialRef *CredentialRef  `json:"credential_ref,omitempty"`
	NetworkScope  string          `json:"network_scope"`
	Duration      time.Duration   `json:"duration"`
	RequireDecode bool            `json:"require_decode"`
}

type VerifiedState struct {
	Profile        MediaProfile  `json:"profile"`
	AudioCodec     string        `json:"audio_codec,omitempty"`
	ArrivalFPS     float64       `json:"arrival_fps,omitempty"`
	ArrivalBitrate int64         `json:"arrival_bitrate,omitempty"`
	Duration       time.Duration `json:"duration"`
	MaxGap         time.Duration `json:"max_gap,omitempty"`
	Decoded        bool          `json:"decoded"`
	// Actual independently decoded video frames, not an FPS/duration estimate.
	DecodedFrames   int64      `json:"decoded_frames,omitempty"`
	Complete        bool       `json:"complete"`
	Evidence        []Evidence `json:"evidence,omitempty"`
	ObservedAt      time.Time  `json:"observed_at"`
	Firmware        string     `json:"firmware,omitempty"`
	ProfileRevision string     `json:"profile_revision,omitempty"`
}

type MediaVerifier interface {
	Verify(context.Context, MediaRequest) (VerifiedState, error)
}

var (
	ErrAuthRequired      = errors.New("camera credentials required")
	ErrNeedsCredentials  = ErrAuthRequired
	ErrNeedsUserInput    = errors.New("camera input required")
	ErrForbidden         = errors.New("camera operation forbidden")
	ErrUnsupported       = errors.New("camera operation unsupported")
	ErrManualActivation  = errors.New("manual camera activation required")
	ErrConflict          = errors.New("camera state changed")
	ErrExpired           = errors.New("camera credential lease expired")
	ErrInvalid           = errors.New("invalid camera request")
	ErrAmbiguousIdentity = errors.New("camera identity requires an explicit claim")
)

// SanitizeURI removes user information and common credential query parameters.
// Malformed inputs are not echoed because parser errors may contain a password.
func SanitizeURI(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "[invalid URI]"
	}
	u.User = nil
	q := u.Query()
	for key := range q {
		k := strings.ToLower(key)
		if strings.Contains(k, "password") || strings.Contains(k, "passwd") || strings.Contains(k, "secret") || strings.Contains(k, "token") || strings.Contains(k, "credential") || k == "user" || k == "username" || k == "auth" {
			q.Del(key)
		}
	}
	u.RawQuery = q.Encode()
	u.Fragment = ""
	return u.String()
}
