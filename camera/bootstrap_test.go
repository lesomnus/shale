package camera

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapSettingsRequireExplicitPortableSelection(t *testing.T) {
	valid := BootstrapSettings{CompatibilityProfile: "installed/profile:v1", WebScheme: "https", WebPort: 8443}
	for _, edit := range []func(*BootstrapSettings){
		func(*BootstrapSettings) {},
		func(s *BootstrapSettings) { s.WebScheme, s.WebPort = "http", 1 },
		func(s *BootstrapSettings) { s.CompatibilityProfile, s.WebPort = strings.Repeat("p", 128), 65535 },
	} {
		s := valid
		edit(&s)
		require.NoError(t, s.Validate(), "syntax does not depend on an installed driver registry")
	}
	for _, test := range []struct {
		name string
		edit func(*BootstrapSettings)
	}{
		{"missing-profile", func(s *BootstrapSettings) { s.CompatibilityProfile = "" }},
		{"oversized-profile", func(s *BootstrapSettings) { s.CompatibilityProfile = strings.Repeat("p", 129) }},
		{"profile-space", func(s *BootstrapSettings) { s.CompatibilityProfile = "profile name" }},
		{"profile-secret", func(s *BootstrapSettings) { s.CompatibilityProfile = "profile?token=value" }},
		{"missing-scheme", func(s *BootstrapSettings) { s.WebScheme = "" }},
		{"upper-scheme", func(s *BootstrapSettings) { s.WebScheme = "HTTPS" }},
		{"unsupported-scheme", func(s *BootstrapSettings) { s.WebScheme = "rtsp" }},
		{"missing-port", func(s *BootstrapSettings) { s.WebPort = 0 }},
		{"negative-port", func(s *BootstrapSettings) { s.WebPort = -1 }},
		{"large-port", func(s *BootstrapSettings) { s.WebPort = 65536 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := valid
			test.edit(&s)
			require.ErrorIs(t, s.Validate(), ErrInvalid)
		})
	}
}
