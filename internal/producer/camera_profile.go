package producer

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/camera/onvif"
)

// OnvifConfig names a management endpoint/profile independently of the streaming
// endpoint. Its credential is scoped for management, not implicitly borrowed
// from a stream-only reference.
type OnvifConfig struct {
	Endpoint      string                `yaml:"endpoint" json:"endpoint"`
	ProfileToken  string                `yaml:"profile_token" json:"profile_token"`
	CredentialRef *camera.CredentialRef `yaml:"credential_ref" json:"credential_ref,omitempty"`
}

func (p *Producer) refreshCeiling(ctx context.Context, s *source) error {
	if s.cfg.MaxBitrate > 0 {
		return nil
	}
	if s.cfg.MaxBitrateOnvif {
		profile, err := p.onvifProfile(ctx, s.cfg)
		if err != nil {
			return fmt.Errorf("source %s ONVIF profile: %w", s.cfg.Alias, err)
		}
		ceiling := profile.Bitrate
		if ceiling <= 0 {
			ceiling = StartingCeiling(fmt.Sprintf("%dx%d", profile.Width, profile.Height), int(math.Round(profile.FPS)), profile.Codec)
		}
		ceiling += s.cfg.audioShare()
		s.mu.Lock()
		s.inferredCeiling = ceiling
		s.mu.Unlock()
		return nil
	}
	if !strings.HasPrefix(strings.ToLower(s.cfg.Input), "rtsp") || s.cfg.Size != "" || s.cfg.Fps > 0 || s.cfg.Format != "" {
		return nil
	}
	s.mu.Lock()
	already := s.inferredCeiling > 0
	s.mu.Unlock()
	if already {
		return nil
	}
	verifier := p.cfg.MediaVerifier
	network := s.cfg.Scope.NetworkID
	if network == "" {
		network = "local"
	}
	scope := s.cfg.Scope
	scope.NetworkID = network
	if s.cfg.InputCredential != nil && scope != s.cfg.InputCredential.Scope {
		return camera.ErrForbidden
	}
	if verifier == nil {
		verifier = &media.Verifier{AuthProvider: p.sourceAuth(s), Ffmpeg: p.cfg.Ffmpeg, Ffprobe: p.cfg.Ffprobe, NetworkScope: network}
	}
	result, err := verifier.Verify(ctx, camera.MediaRequest{Scope: scope, Profile: camera.MediaProfile{StreamURI: s.cfg.Input}, CredentialRef: s.cfg.InputCredential, NetworkScope: network, Duration: 3 * time.Second})
	if err != nil {
		return fmt.Errorf("source %s RTSP profile: %w", s.cfg.Alias, err)
	}
	if !result.Complete {
		return media.ErrIncomplete
	}
	ceiling := StartingCeiling(fmt.Sprintf("%dx%d", result.Profile.Width, result.Profile.Height), int(math.Round(result.Profile.FPS)), result.Profile.Codec)
	if measured := result.Profile.Bitrate * 5 / 4; measured > ceiling {
		ceiling = measured
	}
	s.mu.Lock()
	s.inferredCeiling = ceiling + s.cfg.audioShare()
	s.mu.Unlock()
	return nil
}

func onvifEndpoint(source SourceConfig) (string, error) {
	cfg := source.Onvif
	if cfg == nil {
		cfg = &OnvifConfig{}
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		u, err := url.Parse(source.Input)
		if err != nil || u.Hostname() == "" {
			return "", camera.ErrInvalid
		}
		endpoint = (&url.URL{Scheme: "http", Host: net.JoinHostPort(u.Hostname(), "80"), Path: "/onvif/device_service"}).String()
	}
	if u, err := url.Parse(endpoint); err != nil || u.User != nil {
		return "", camera.ErrInvalid
	}
	return endpoint, nil
}

func (p *Producer) onvifProfile(ctx context.Context, source SourceConfig) (camera.MediaProfile, error) {
	var result camera.MediaProfile
	cfg := source.Onvif
	if cfg == nil {
		cfg = &OnvifConfig{}
	}
	endpoint, err := onvifEndpoint(source)
	if err != nil {
		return result, err
	}
	device := camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: endpoint, NetworkScope: source.Scope.NetworkID}}}
	var lease camera.CredentialLease
	sanitize := credential.InputSanitizer(source.Input)
	if cfg.CredentialRef != nil {
		lease, err = camera.Acquire(ctx, p.cfg.AuthProvider, camera.AuthRequest{Scope: source.Scope, Device: device, Purpose: camera.AuthManagement, Requirement: camera.AuthRequirement{Access: camera.AccessRead}, ExistingRef: cfg.CredentialRef})
		if err != nil {
			return result, err
		}
	} else if u, err := url.Parse(source.Input); err == nil && u.User != nil {
		password, _ := u.User.Password()
		// Legacy manually configured input explicitly supplied these credentials.
		// New reference inputs require a separately authorized management reference.
		lease = camera.NewCredentialLease(camera.CredentialRef{Provider: "manual", ID: source.Alias, Version: "1", Scope: source.Scope, Purpose: camera.AuthManagement}, camera.CredentialMaterial{Username: u.User.Username(), Password: password}, time.Now().Add(time.Minute))
	} else if source.InputCredential != nil {
		if private, ok := p.cfg.AuthProvider.(interface {
			legacyManagement(context.Context, camera.EnrollmentScope, camera.CredentialRef, string, string) (camera.CredentialLease, credential.Sanitizer, error)
		}); ok {
			lease, sanitize, err = private.legacyManagement(ctx, source.Scope, *source.InputCredential, source.Input, endpoint)
			if err != nil {
				return result, err
			}
		}
	}
	if lease != nil {
		defer lease.Close()
	}
	inspector := p.cfg.OnvifInspector
	if inspector == nil {
		inspector = &onvif.Client{}
	}
	inspected, err := inspector.Inspect(ctx, device, lease)
	if err != nil {
		return result, credential.SanitizeError(err, sanitize)
	}
	for _, profile := range inspected.Current.Profiles {
		if cfg.ProfileToken != "" && profile.Token == cfg.ProfileToken {
			return profile, nil
		}
		if cfg.ProfileToken == "" && camera.SanitizeURI(profile.StreamURI) == camera.SanitizeURI(source.Input) {
			return profile, nil
		}
	}
	if cfg.ProfileToken == "" && len(inspected.Current.Profiles) == 1 {
		return inspected.Current.Profiles[0], nil
	}
	return result, fmt.Errorf("%w: choose an explicit ONVIF profile token", camera.ErrNeedsUserInput)
}

func validateCameraInput(cfg Config, source SourceConfig) error {
	if source.InputCredential != nil {
		if !source.InputCredential.Authorizes(source.Scope, camera.AuthStreaming) {
			return camera.ErrForbidden
		}
		u, err := url.Parse(source.Input)
		if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "rtsp" && u.Scheme != "rtsps") {
			return camera.ErrInvalid
		}
		if cfg.AuthProvider == nil && cfg.AuthProviderFactory == nil {
			return camera.ErrAuthRequired
		}
	}
	if source.Onvif != nil && source.Onvif.CredentialRef != nil {
		if !source.Onvif.CredentialRef.Authorizes(source.Scope, camera.AuthManagement) {
			return camera.ErrForbidden
		}
		if cfg.AuthProvider == nil && cfg.AuthProviderFactory == nil {
			return camera.ErrAuthRequired
		}
	}
	if source.MaxBitrateOnvif && source.MaxBitrate > 0 {
		return camera.ErrInvalid
	}
	return nil
}

// A reference scoped to another authoritative Producer/Set is not accepted
// merely because the local YAML repeats that scope alongside it.
func (p *Producer) validateCameraAssignment() error {
	return p.validateCameraAssignmentSources(p.activeSources())
}
func (p *Producer) validateCameraAssignmentSources(sources []*source) error {
	for _, s := range sources {
		scope := s.cfg.Scope
		if s.cfg.InputCredential == nil && s.cfg.Onvif == nil && scope.TenantID == "" {
			continue
		}
		expected := map[string]string{"tenant": mustId(p.set.GetTenant().GetId()).String(), "set": mustId(p.set.GetId()).String(), "producer": p.id.String()}
		if scope.TenantID != "" && scope.TenantID != expected["tenant"] || scope.SetID != "" && scope.SetID != expected["set"] || scope.ProducerID != "" && scope.ProducerID != expected["producer"] {
			return camera.ErrForbidden
		}
		if scope.SiteID != "" && (p.set.GetSite() == nil || scope.SiteID != mustId(p.set.GetSite().GetId()).String()) {
			return camera.ErrForbidden
		}
	}
	return nil
}
