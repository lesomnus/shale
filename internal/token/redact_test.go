package token

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A URL that reaches a log carries no token (§31).
func TestRedactURL(t *testing.T) {
	require.Equal(t, "https://10.1.2.74:7410/laminae/a/b?token=%3Credacted%3E", RedactURL("https://10.1.2.74:7410/laminae/a/b?token=Shale.abc.def"))
	require.Equal(t, "https://10.1.2.74:7410/laminae/a/b", RedactURL("https://10.1.2.74:7410/laminae/a/b"))
	require.Equal(t, "https://h:1/x?other=1&token=%3Credacted%3E", RedactURL("https://h:1/x?other=1&token=t"))
	require.Equal(t, "<unparseable url>", RedactURL("http://[::1"))
}
