package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Public download URLs must not accept credentials, scheme-relative hosts or
// non-HTTPS production destinations, even though they are operator settings.
func TestExportConfiguration(t *testing.T) {
	for _, endpoint := range []string{"", "/exports/recording.mp4", "https://downloads.example/exports/recording.mp4"} {
		c := ExportConfig{Enabled: true, PublicURL: endpoint}
		require.NoError(t, c.check(false), endpoint)
		require.NotEmpty(t, c.endpoint())
	}
	for _, endpoint := range []string{
		"http://downloads.example/exports/recording.mp4", "//downloads.example/exports/recording.mp4",
		"https://user:secret@downloads.example/export.mp4", "https://downloads.example/export.mp4?token=preset",
		"https://downloads.example/export.mp4#fragment", "javascript:alert(1)", "relative/export.mp4", "https://downloads.example",
	} {
		require.Error(t, (ExportConfig{Enabled: true, PublicURL: endpoint}).check(false), endpoint)
	}
	require.NoError(t, (ExportConfig{Enabled: true, PublicURL: "http://127.0.0.1:7402/exports/recording.mp4"}).check(true))
	for _, c := range []ExportConfig{{MaxConcurrent: -1}, {PerActor: -1}, {MaxDuration: -time.Second}} {
		require.Error(t, c.check(false), "invalid limits are refused even when disabled")
	}
}
