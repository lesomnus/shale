package source_test

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func managed(catalog int64) source.Snapshot {
	id := func(domain pdid.Domain) string { return pdid.New(domain).String() }
	return source.Snapshot{
		Revision: source.Revision{AssignmentID: id(28), Generation: 1, Catalog: catalog},
		Scope:    source.Scope{TenantID: id(1), SetID: id(7), ProducerID: id(22), NetworkID: id(26)},
		Sources: []source.Spec{{
			BindingID: id(29), SourceID: id(8), Revision: catalog, Alias: "front-door", Input: "rtsp://192.0.2.10/stream?profile=main",
			Profile:    source.Profile{Codec: "h264", Width: 1280, Height: 720, FPS: 29.97, MaxBitrate: 2_000_000},
			Credential: &source.CredentialRef{Provider: "cp", ID: "camera-stream", Version: "1", Purpose: "streaming"},
			Labels:     map[string]string{"room": "entrance"},
		}},
	}
}

func TestNormalizeManagedContract(t *testing.T) {
	v := managed(1)
	v.Revision.AssignmentID = strings.ToUpper(v.Revision.AssignmentID)
	normalized, err := source.Normalize(v)
	require.NoError(t, err)
	require.Equal(t, strings.ToLower(v.Revision.AssignmentID), normalized.Revision.AssignmentID)
	require.Equal(t, 29.97, normalized.Sources[0].Profile.FPS)
	require.Zero(t, normalized.Sources[0].Profile.KeyframeInterval, "unknown cadence stays unknown")
	v.Sources[0].Labels["room"] = "changed"
	v.Sources[0].Credential.ID = "changed"
	require.Equal(t, "entrance", normalized.Sources[0].Labels["room"])
	require.Equal(t, "camera-stream", normalized.Sources[0].Credential.ID)

	empty := managed(0)
	empty.Sources = nil
	_, err = source.Normalize(empty)
	require.NoError(t, err, "revision zero with an explicitly empty managed catalog is valid")
}

func TestManagedIdentityAndRevisionValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*source.Snapshot)
	}{
		{"missing scope", func(v *source.Snapshot) { v.Scope.ProducerID = "" }},
		{"invalid ID", func(v *source.Snapshot) { v.Scope.SetID = "alias-is-not-an-id" }},
		{"zero generation", func(v *source.Snapshot) { v.Revision.Generation = 0 }},
		{"negative catalog", func(v *source.Snapshot) { v.Revision.Catalog = -1 }},
		{"missing binding", func(v *source.Snapshot) { v.Sources[0].BindingID = "" }},
		{"missing source", func(v *source.Snapshot) { v.Sources[0].SourceID = "" }},
		{"future spec", func(v *source.Snapshot) { v.Sources[0].Revision = 2 }},
		{"zero spec", func(v *source.Snapshot) { v.Sources[0].Revision = 0 }},
		{"incomplete profile", func(v *source.Snapshot) { v.Sources[0].Profile.Width = 0 }},
		{"nonfinite FPS", func(v *source.Snapshot) { v.Sources[0].Profile.FPS = math.NaN() }},
		{"wrong purpose", func(v *source.Snapshot) { v.Sources[0].Credential.Purpose = "management" }},
		{"non RTSP managed input", func(v *source.Snapshot) { v.Sources[0].Input = "v4l2:/dev/video0" }},
		{"duplicate alias", func(v *source.Snapshot) {
			x := v.Sources[0].Clone()
			x.SourceID = pdid.New(8).String()
			x.BindingID = pdid.New(29).String()
			v.Sources = append(v.Sources, x)
		}},
		{"duplicate source", func(v *source.Snapshot) {
			x := v.Sources[0].Clone()
			x.Alias = "other"
			x.BindingID = pdid.New(29).String()
			v.Sources = append(v.Sources, x)
		}},
		{"duplicate binding", func(v *source.Snapshot) {
			x := v.Sources[0].Clone()
			x.Alias = "other"
			x.SourceID = pdid.New(8).String()
			v.Sources = append(v.Sources, x)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := managed(1)
			tt.edit(&v)
			_, err := source.Normalize(v)
			require.ErrorIs(t, err, source.ErrInvalid)
		})
	}
}

func TestManagedIdentifierDomains(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*source.Snapshot)
	}{
		{"assignment", func(v *source.Snapshot) { v.Revision.AssignmentID = pdid.New(8).String() }},
		{"tenant", func(v *source.Snapshot) { v.Scope.TenantID = pdid.New(8).String() }},
		{"set", func(v *source.Snapshot) { v.Scope.SetID = pdid.New(8).String() }},
		{"producer", func(v *source.Snapshot) { v.Scope.ProducerID = pdid.New(8).String() }},
		{"network", func(v *source.Snapshot) { v.Scope.NetworkID = pdid.New(8).String() }},
		{"site", func(v *source.Snapshot) { v.Scope.SiteID = pdid.New(8).String() }},
		{"binding", func(v *source.Snapshot) { v.Sources[0].BindingID = pdid.New(8).String() }},
		{"source", func(v *source.Snapshot) { v.Sources[0].SourceID = pdid.New(22).String() }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := managed(1)
			tt.edit(&v)
			_, err := source.Normalize(v)
			require.ErrorIs(t, err, source.ErrInvalid, "another valid PDID is not this entity's identity")
		})
	}
	v := managed(1)
	v.Scope.SiteID = pdid.New(19).String()
	_, err := source.Normalize(v)
	require.NoError(t, err)
}

func TestSerializationNeverEchoesCredentialInputs(t *testing.T) {
	const material = "private fixture +:/? password"
	inputs := []string{
		"rtsp://operator:" + url.PathEscape(material) + "@192.0.2.10/stream",
		"rtsp://192.0.2.10/stream?%75sername=" + url.QueryEscape(material),
		"rtsp://192.0.2.10/stream?ACCESS_TOKEN=" + url.QueryEscape(material),
		"rtsp://192.0.2.10/stream?password=%not-encoding-" + material,
		"rtsp://192.0.2.10/stream#" + material,
	}
	for i, input := range inputs {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			v := managed(1)
			v.Sources[0].Input = input
			_, err := source.Normalize(v)
			require.ErrorIs(t, err, source.ErrInvalid)
			for _, value := range []string{material, url.QueryEscape(material), url.PathEscape(material)} {
				require.NotContains(t, err.Error(), value)
			}
			for _, object := range []any{v, v.Sources[0]} {
				b, err := json.Marshal(object)
				require.Error(t, err)
				require.Empty(t, b)
				require.NotContains(t, err.Error(), material)
				b, err = yaml.Marshal(object)
				require.Error(t, err)
				require.Empty(t, b)
				require.NotContains(t, err.Error(), material)
				require.NotContains(t, fmt.Sprintf("%v %#v", object, object), material)
			}
		})
	}
}
