package producer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	catalog "github.com/lesomnus/shale/source"
)

// legacyCatalog is a private file/static compatibility boundary. Arbitrary local
// commands and encoder/USB/ONVIF settings never enter the shareable catalog.
func legacyCatalog(configs []SourceConfig, session SourceSession, fallback camera.AuthProvider) (catalog.Provider, map[string]SourceConfig, camera.AuthProvider, error) {
	auth := &legacyInputProvider{fallback: fallback, inputs: map[camera.CredentialRef]legacyInput{}}
	local := make(map[string]SourceConfig, len(configs))
	specs := make([]catalog.Spec, 0, len(configs))
	for _, original := range configs {
		cfg := cloneLocalConfig(original)
		clean, private, sanitize, material, err := splitLegacyInput(cfg.Input)
		if err != nil {
			return nil, nil, nil, err
		}
		if private {
			if cfg.InputCredential != nil {
				return nil, nil, nil, camera.ErrInvalid
			}
			scope := cfg.Scope
			if scope.TenantID == "" {
				scope.TenantID = session.TenantID
			}
			if scope.SiteID == "" {
				scope.SiteID = session.SiteID
			}
			if scope.SetID == "" {
				scope.SetID = session.SetID
			}
			if scope.ProducerID == "" {
				scope.ProducerID = session.ProducerID
			}
			if scope.NetworkID == "" {
				scope.NetworkID = "local"
			}
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return nil, nil, nil, catalog.ErrUnavailable
			}
			// These are process-local read references, not durable enrollment
			// credentials. Restart rereads the existing private configuration.
			ref := camera.CredentialRef{Provider: "legacy-input", ID: hex.EncodeToString(nonce[:]), Version: "1", Scope: scope, Purpose: camera.AuthStreaming, Recoverable: false}
			auth.inputs[ref] = legacyInput{clean: clean, input: cfg.Input, material: material, sanitize: sanitize}
			cfg.Input, cfg.InputCredential, cfg.Scope = clean, &ref, scope
		}
		spec := catalog.Spec{Alias: cfg.Alias, Name: cfg.Name, Input: clean, Labels: cfg.Labels, Zone: cfg.Zone, Profile: catalog.Profile{Codec: cfg.Format, FPS: float64(cfg.Fps), MaxBitrate: cfg.MaxBitrate, KeyframeInterval: cfg.KeyframeInterval}}
		// Unknown legacy sizes stay unknown; parseSize's automatic 1080p
		// default would turn a compatibility catalog into fabricated metadata.
		if size := strings.SplitN(strings.ToLower(cfg.Size), "x", 2); len(size) == 2 {
			w, we := strconv.Atoi(size[0])
			h, he := strconv.Atoi(size[1])
			if we == nil && he == nil && w > 0 && h > 0 {
				spec.Profile.Width, spec.Profile.Height = w, h
			}
		}
		if ref := cfg.InputCredential; ref != nil {
			spec.Credential = &catalog.CredentialRef{Provider: ref.Provider, ID: ref.ID, Version: ref.Version, Purpose: string(ref.Purpose)}
		}
		specs = append(specs, spec)
		local[cfg.Alias] = cfg
	}
	provider, err := catalog.NewStatic(specs)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(auth.inputs) == 0 {
		return provider, local, fallback, nil
	}
	return provider, local, auth, nil
}

func cloneLocalConfig(v SourceConfig) SourceConfig {
	out := v
	cloneMap := func(m map[string]string) map[string]string {
		if m == nil {
			return nil
		}
		n := make(map[string]string, len(m))
		for k, v := range m {
			n[k] = v
		}
		return n
	}
	out.Labels, out.Controls, out.EncoderOptions = cloneMap(v.Labels), cloneMap(v.Controls), cloneMap(v.EncoderOptions)
	out.ExtraInputArgs = append([]string(nil), v.ExtraInputArgs...)
	out.ExtraOutputArgs = append([]string(nil), v.ExtraOutputArgs...)
	if v.InputCredential != nil {
		r := *v.InputCredential
		out.InputCredential = &r
	}
	if v.Onvif != nil {
		r := *v.Onvif
		if r.CredentialRef != nil {
			ref := *r.CredentialRef
			r.CredentialRef = &ref
		}
		out.Onvif = &r
	}
	if v.Audio != nil {
		a := *v.Audio
		out.Audio = &a
	}
	if v.Idle != nil {
		i := *v.Idle
		out.Idle = &i
	}
	return out
}

func credentialQuery(key string) bool {
	k := strings.ToLower(key)
	for _, word := range []string{"password", "passwd", "secret", "token", "credential", "auth"} {
		if strings.Contains(k, word) {
			return true
		}
	}
	return k == "user" || k == "username" || k == "pwd" || k == "pass" || k == "apikey" || k == "api_key" || k == "sig" || k == "signature"
}

func splitLegacyInput(input string) (string, bool, credential.Sanitizer, camera.CredentialMaterial, error) {
	var material camera.CredentialMaterial
	u, err := url.Parse(input)
	if err != nil {
		return "", false, credential.Sanitizer{}, material, catalog.ErrInvalid
	}
	if u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return input, false, credential.Sanitizer{}, material, nil
	}
	if u.Host == "" {
		return "", false, credential.Sanitizer{}, material, catalog.ErrInvalid
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", false, credential.Sanitizer{}, material, catalog.ErrInvalid
	}
	private := u.User != nil || u.Fragment != ""
	var secrets []string
	if u.User != nil {
		material.Username = u.User.Username()
		material.Password, _ = u.User.Password()
		secrets = append(secrets, material.Username, material.Password)
		u.User = nil
	}
	for key, values := range query {
		if credentialQuery(key) {
			private = true
			secrets = append(secrets, values...)
			query.Del(key)
		}
	}
	if u.Fragment != "" {
		secrets = append(secrets, u.Fragment)
		u.Fragment = ""
	}
	if !private {
		return input, false, credential.Sanitizer{}, material, nil
	}
	u.RawQuery = query.Encode()
	return u.String(), true, credential.NewSanitizer(secrets...), material, nil
}

type legacyInput struct {
	clean, input string
	material     camera.CredentialMaterial
	sanitize     credential.Sanitizer
}

type legacyInputProvider struct {
	fallback camera.AuthProvider
	inputs   map[camera.CredentialRef]legacyInput
}

func (*legacyInputProvider) String() string     { return "[private legacy input provider]" }
func (p *legacyInputProvider) GoString() string { return p.String() }

func (p *legacyInputProvider) Acquire(ctx context.Context, q camera.AuthRequest) (camera.CredentialLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.ExistingRef != nil {
		if input, ok := p.inputs[*q.ExistingRef]; ok {
			ref := *q.ExistingRef
			if !ref.Authorizes(q.Scope, camera.AuthStreaming) || q.Purpose != camera.AuthStreaming || (q.Requirement.Access != "" && q.Requirement.Access != camera.AccessRead) {
				return nil, camera.ErrForbidden
			}
			if q.Requirement.Recoverable {
				return nil, camera.ErrNeedsUserInput
			}
			return &legacyInputLease{CredentialLease: camera.NewCredentialLease(ref, input.material, time.Now().Add(5*time.Minute)), legacy: input}, nil
		}
	}
	if p.fallback == nil {
		return nil, camera.ErrAuthRequired
	}
	return p.fallback.Acquire(ctx, q)
}

type legacyInputLease struct {
	camera.CredentialLease
	legacy legacyInput
}

func (l *legacyInputLease) WithInput(ctx context.Context, input string, use func(string, credential.Sanitizer) error) error {
	if input != l.legacy.clean {
		return camera.ErrForbidden
	}
	return l.CredentialLease.Use(ctx, func(camera.CredentialMaterial) error {
		return credential.SanitizeError(use(l.legacy.input, l.legacy.sanitize), l.legacy.sanitize)
	})
}

func (*legacyInputLease) String() string     { return "[private legacy input lease]" }
func (l *legacyInputLease) GoString() string { return l.String() }
