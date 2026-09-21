package cli

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionCookie(t *testing.T) {
	// The server names the cookie (§40.1); the CLI takes what it is given.
	for _, name := range []string{"__Host-shale_tenant", "shale_cluster", "__Host-pd_session", "pd_session"} {
		ck := sessionCookie([]*http.Cookie{{Name: "other", Value: "1"}, {Name: name, Value: "v"}})
		require.NotNil(t, ck, name)
		require.Equal(t, name, ck.Name)
	}
	// One cookie of any name is the session; a cleared one is not.
	require.Equal(t, "x", sessionCookie([]*http.Cookie{{Name: "x", Value: "v"}}).Name)
	require.Nil(t, sessionCookie([]*http.Cookie{{Name: "x", Value: ""}}))
	require.Nil(t, sessionCookie(nil))
}
