// Package source is the neutral, reference-only catalog consumed by a Producer.
// It has no dependency on camera discovery, vendor drivers, workers or the CLI.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
)

var (
	ErrInvalid     = errors.New("invalid source catalog")
	ErrConflict    = errors.New("source catalog conflicts with accepted state")
	ErrStale       = errors.New("source catalog revision is older than accepted state")
	ErrUnavailable = errors.New("source catalog unavailable")
)

// Revision identifies a full snapshot. Generation fences assignment changes;
// Catalog increases for changes to the source list or its specifications.
// An empty AssignmentID and zero Generation identify a local static catalog.
type Revision struct {
	AssignmentID string `json:"assignment_id,omitempty" yaml:"assignment_id,omitempty"`
	Generation   int64  `json:"generation" yaml:"generation"`
	Catalog      int64  `json:"catalog" yaml:"catalog"`
}

type Scope struct {
	TenantID   string `json:"tenant_id,omitempty" yaml:"tenant_id,omitempty"`
	SiteID     string `json:"site_id,omitempty" yaml:"site_id,omitempty"`
	SetID      string `json:"set_id,omitempty" yaml:"set_id,omitempty"`
	ProducerID string `json:"producer_id,omitempty" yaml:"producer_id,omitempty"`
	NetworkID  string `json:"network_id,omitempty" yaml:"network_id,omitempty"`
}

// CredentialRef is an immutable lookup key, never a grant. An AuthProvider must
// separately validate current assignment, exact runtime scope and recovery.
type CredentialRef struct {
	Provider string `json:"provider" yaml:"provider"`
	ID       string `json:"id" yaml:"id"`
	Version  string `json:"version" yaml:"version"`
	Purpose  string `json:"purpose" yaml:"purpose"`
}

// Profile preserves fractional source cadence. A zero KeyframeInterval means
// unknown; the catalog does not manufacture evidence of a random-access cadence.
type Profile struct {
	Codec            string        `json:"codec,omitempty" yaml:"codec,omitempty"`
	Width            int           `json:"width,omitempty" yaml:"width,omitempty"`
	Height           int           `json:"height,omitempty" yaml:"height,omitempty"`
	FPS              float64       `json:"fps,omitempty" yaml:"fps,omitempty"`
	MaxBitrate       int64         `json:"max_bitrate,omitempty" yaml:"max_bitrate,omitempty"`
	KeyframeInterval time.Duration `json:"keyframe_interval,omitempty" yaml:"keyframe_interval,omitempty"`
}

// Spec contains only shareable capture intent. Local command lines, vendor
// configuration and credential material belong to private compatibility adapters.
// Static aliases may lack SourceID before normal Producer registration; managed
// specifications always carry explicit binding and Source identifiers.
type Spec struct {
	BindingID  string            `json:"binding_id,omitempty" yaml:"binding_id,omitempty"`
	SourceID   string            `json:"source_id,omitempty" yaml:"source_id,omitempty"`
	Revision   int64             `json:"revision" yaml:"revision"`
	Alias      string            `json:"alias" yaml:"alias"`
	Name       string            `json:"name,omitempty" yaml:"name,omitempty"`
	Input      string            `json:"input,omitempty" yaml:"input,omitempty"`
	Profile    Profile           `json:"profile" yaml:"profile"`
	Credential *CredentialRef    `json:"credential,omitempty" yaml:"credential,omitempty"`
	Labels     map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Zone       string            `json:"zone,omitempty" yaml:"zone,omitempty"`
}

// Snapshot is authoritative only on a successful read. An empty Sources slice
// is a real empty catalog, not an error or an unavailable control plane.
type Snapshot struct {
	Revision Revision `json:"revision" yaml:"revision"`
	Scope    Scope    `json:"scope" yaml:"scope"`
	Sources  []Spec   `json:"sources" yaml:"sources"`
}

// Provider reads full snapshots and waits for a change hint. The hint carries no
// source delta: consumers re-read Snapshot, including periodically after a wait
// deadline, to recover lost notifications. WaitForChange must return when its
// context ends; no caller-owned channel or background goroutine is required.
type Provider interface {
	Snapshot(context.Context) (Snapshot, error)
	WaitForChange(context.Context, Revision) (Revision, error)
}

func normalizeRevision(r Revision) (Revision, error) {
	if r.Catalog < 0 || r.Generation < 0 {
		return Revision{}, invalid("revision")
	}
	if r.AssignmentID == "" {
		if r.Generation != 0 {
			return Revision{}, invalid("static assignment generation")
		}
		return r, nil
	}
	if r.Generation < 1 {
		return Revision{}, invalid("assignment generation")
	}
	id, err := canonicalID(r.AssignmentID)
	if err != nil {
		return Revision{}, err
	}
	r.AssignmentID = id
	return r, nil
}

// compare checks that after is not ahead of current. Assignment changes need a
// newly authorized provider rather than an implicit switch to another catalog.
func compare(after, current Revision) error {
	if after.AssignmentID != current.AssignmentID {
		return ErrConflict
	}
	if after.Generation > current.Generation || after.Catalog > current.Catalog {
		return ErrStale
	}
	return nil
}

// Clone returns caller-owned slices, maps and references.
func (s Snapshot) Clone() Snapshot {
	out := s
	out.Sources = make([]Spec, len(s.Sources))
	for i, spec := range s.Sources {
		out.Sources[i] = spec.Clone()
	}
	return out
}

func (s Spec) Clone() Spec {
	out := s
	if s.Credential != nil {
		ref := *s.Credential
		out.Credential = &ref
	}
	if s.Labels != nil {
		out.Labels = make(map[string]string, len(s.Labels))
		for k, v := range s.Labels {
			out.Labels[k] = v
		}
	}
	return out
}

func invalid(field string) error { return fmt.Errorf("%w: %s", ErrInvalid, field) }

func canonicalID(value string) (string, error) {
	id, err := pdid.Parse(value)
	if err != nil || id.IsZero() {
		return "", invalid("identifier")
	}
	return id.String(), nil
}

// Normalize validates a catalog and returns a deep copy with canonical IDs.
// It rejects credentials in endpoints instead of silently changing their meaning.
func Normalize(s Snapshot) (Snapshot, error) {
	out := s.Clone()
	managed := out.Revision.AssignmentID != ""
	if out.Revision.Catalog < 0 || out.Revision.Generation < 0 || len(out.Sources) > 500 {
		return Snapshot{}, invalid("revision or source count")
	}
	if managed {
		if out.Revision.Generation < 1 {
			return Snapshot{}, invalid("assignment generation")
		}
		for _, field := range []*string{&out.Revision.AssignmentID, &out.Scope.TenantID, &out.Scope.SetID, &out.Scope.ProducerID, &out.Scope.NetworkID} {
			value, err := canonicalID(*field)
			if err != nil {
				return Snapshot{}, err
			}
			*field = value
		}
		if out.Scope.SiteID != "" {
			value, err := canonicalID(out.Scope.SiteID)
			if err != nil {
				return Snapshot{}, err
			}
			out.Scope.SiteID = value
		}
	} else if out.Revision.Generation != 0 || out.Scope != (Scope{}) {
		return Snapshot{}, invalid("static assignment scope")
	}
	aliases, sources, bindings := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i := range out.Sources {
		spec := &out.Sources[i]
		if err := validateSpec(*spec, managed); err != nil {
			return Snapshot{}, err
		}
		if spec.Revision < 1 || spec.Revision > out.Revision.Catalog {
			return Snapshot{}, invalid("source revision")
		}
		if aliases[spec.Alias] {
			return Snapshot{}, invalid("duplicate alias")
		}
		aliases[spec.Alias] = true
		for _, field := range []struct {
			value *string
			seen  map[string]bool
		}{{&spec.SourceID, sources}, {&spec.BindingID, bindings}} {
			if *field.value == "" {
				continue
			}
			value, err := canonicalID(*field.value)
			if err != nil {
				return Snapshot{}, err
			}
			if field.seen[value] {
				return Snapshot{}, invalid("duplicate source or binding")
			}
			*field.value, field.seen[value] = value, true
		}
	}
	return out, nil
}

var referencePart = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)

func validateSpec(s Spec, managed bool) error {
	if s.Alias == "" || len(s.Alias) > 256 || (managed && (s.SourceID == "" || s.BindingID == "")) {
		return invalid("source identity")
	}
	for _, id := range []string{s.SourceID, s.BindingID} {
		if id != "" {
			if _, err := canonicalID(id); err != nil {
				return err
			}
		}
	}
	if err := validateInput(s.Input, managed); err != nil {
		return err
	}
	p := s.Profile
	if p.Width < 0 || p.Height < 0 || p.Width > 16384 || p.Height > 16384 || p.FPS < 0 || p.FPS > 1000 || math.IsNaN(p.FPS) || math.IsInf(p.FPS, 0) || p.MaxBitrate < 0 || p.MaxBitrate > 10_000_000_000 || p.KeyframeInterval < 0 {
		return invalid("capture profile")
	}
	if managed && ((p.Codec != "h264" && p.Codec != "h265") || p.Width == 0 || p.Height == 0 || p.FPS == 0 || p.MaxBitrate == 0) {
		return invalid("managed capture profile")
	}
	if ref := s.Credential; ref != nil {
		if !referencePart.MatchString(ref.Provider) || !referencePart.MatchString(ref.ID) || !referencePart.MatchString(ref.Version) || ref.Purpose != "streaming" {
			return invalid("streaming credential reference")
		}
		u, _ := url.Parse(s.Input)
		if u == nil || (u.Scheme != "rtsp" && u.Scheme != "rtsps") || u.Host == "" {
			return invalid("referenced capture input")
		}
	}
	return nil
}

func validateInput(input string, managed bool) error {
	if len(input) > 64<<10 || strings.ContainsAny(input, "\x00\r\n") {
		return invalid("capture input")
	}
	u, err := url.Parse(input)
	if err != nil || u.User != nil || u.Fragment != "" {
		return invalid("capture input must not contain credentials or a fragment")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return invalid("capture input query")
	}
	for field := range query {
		k := strings.ToLower(field)
		if strings.Contains(k, "password") || strings.Contains(k, "passwd") || strings.Contains(k, "secret") || strings.Contains(k, "token") || strings.Contains(k, "credential") || strings.Contains(k, "authorization") || k == "user" || k == "username" || k == "auth" || k == "pwd" || k == "pass" || k == "apikey" || k == "api_key" || k == "sig" || k == "signature" {
			return invalid("capture input query contains credentials")
		}
	}
	if managed && ((u.Scheme != "rtsp" && u.Scheme != "rtsps") || u.Host == "") {
		return invalid("managed capture input must be RTSP")
	}
	return nil
}

// Serialization validates even values constructed without a provider, so an
// invalid credential-bearing endpoint cannot leak through a status/error dump.
func (s Spec) MarshalJSON() ([]byte, error) {
	if err := validateSpec(s, false); err != nil {
		return nil, err
	}
	type plain Spec
	return json.Marshal(plain(s))
}

func (s Spec) MarshalYAML() (any, error) {
	if err := validateSpec(s, false); err != nil {
		return nil, err
	}
	type plain Spec
	return plain(s), nil
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	v, err := Normalize(s)
	if err != nil {
		return nil, err
	}
	type plain Snapshot
	return json.Marshal(plain(v))
}

func (s Snapshot) MarshalYAML() (any, error) {
	v, err := Normalize(s)
	if err != nil {
		return nil, err
	}
	type plain Snapshot
	return plain(v), nil
}

func (s Spec) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		return "[invalid source specification]"
	}
	return string(b)
}
func (s Spec) GoString() string { return s.String() }
func (s Snapshot) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		return "[invalid source snapshot]"
	}
	return string(b)
}
func (s Snapshot) GoString() string { return s.String() }
