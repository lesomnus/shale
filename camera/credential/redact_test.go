package credential_test

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera/credential"
	"github.com/stretchr/testify/require"
)

func TestCredentialQueryRedactionAcrossRawEncodings(t *testing.T) {
	for _, key := range []string{"auth", "opaque_auth", "vendor_token", "api_key", "%61uth"} {
		for _, raw := range []string{"abc%2fdef%2btail", "abc%2Fdef%2btail", "%61bc%2fd%65f%2Btail"} {
			input := "rtsp://camera.invalid/live?profile=main&" + key + "=" + raw
			out := credential.Redact(input)
			require.NotContains(t, out, raw)
			require.NotContains(t, out, "abc/def+tail")
			require.Contains(t, out, "profile=main")
			sanitize := credential.InputSanitizer(input)
			require.Equal(t, "failure [redacted]", sanitize.Redact("failure "+raw), "exact vendor spelling must be safe outside its URL")
			require.Equal(t, "failure [redacted]", sanitize.Redact("failure abc/def+tail"))
		}
	}
	for _, input := range []string{"rtsp://camera/live?auth=bad%escape", "rtsp://camera/live#private-fragment"} {
		require.NotContains(t, credential.Redact(input), "bad%escape")
		require.NotContains(t, credential.Redact(input), "private-fragment")
	}
	require.Equal(t, "[invalid URL]", credential.Redact("rtsp://reader:bad%escape@camera/live?auth=private-token"), "a malformed userinfo must not leave a credential-bearing query tail")
}

func TestSanitizerPrefersFullOverlappingTokenAndPercentCases(t *testing.T) {
	sanitize := credential.NewSanitizer("abc", "abc/def+tail")
	for _, value := range []string{"abc/def+tail", "abc%2fdef%2Btail", "abc%2Fdef%2btail"} {
		require.Equal(t, "[redacted]", sanitize.Redact(value), "a username prefix must not leave a token suffix visible")
	}
	require.Equal(t, "Abc/DEF", sanitize.Redact("Abc/DEF"), "only percent-escape hex digits are case insensitive")
	require.NotContains(t, credential.NewSanitizer("abc/def").Redact("rtsp://camera.invalid/live?auth=abc%2fdef"), "abc%2fdef")
}

func TestSanitizerFailsClosedForNonUTF8OrOversizedCredential(t *testing.T) {
	for _, construct := range []func() credential.Sanitizer{
		func() credential.Sanitizer { return credential.InputSanitizer("rtsp://camera.invalid/live?auth=%ff") },
		func() credential.Sanitizer { return credential.NewSanitizer(string([]byte{0xff})) },
		func() credential.Sanitizer { return credential.NewSanitizer(strings.Repeat("fixture", 50_000)) },
	} {
		require.NotPanics(t, func() {
			sanitize := construct()
			for _, text := range []string{"failure rtsp://camera.invalid/live?auth=%ff", "failure " + string([]byte{0xff}), "capture failed"} {
				require.Equal(t, "[redacted]", sanitize.Redact(text))
			}
		})
	}
}
