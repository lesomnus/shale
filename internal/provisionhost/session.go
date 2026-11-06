package provisionhost

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
)

// readSession accepts the single tenant cookie pair saved by shale login, with
// one optional final newline. The sealed value and its expiry are checked by
// the CP; neither a bearer token nor another surface's session is substituted.
func readSession(path string) (string, error) {
	const limit = 4096
	invalid := errors.New("CP session requires a private regular file containing one tenant session cookie")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > limit {
		return "", invalid
	}
	f, err := os.Open(path)
	if err != nil {
		return "", invalid
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm()&0077 != 0 {
		return "", invalid
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(data) > limit {
		return "", invalid
	}
	pair := strings.TrimSuffix(string(data), "\n")
	name, value, ok := strings.Cut(pair, "=")
	if !ok || (name != "shale_tenant" && name != "__Host-shale_tenant") || value == "" {
		return "", invalid
	}
	for _, c := range []byte(value) {
		if c <= 0x20 || c >= 0x7f {
			return "", invalid
		}
	}
	if (&http.Cookie{Name: name, Value: value}).Valid() != nil {
		return "", invalid
	}
	return pair, nil
}

type sessionCredential struct {
	Path   string
	Secure bool
}

func (s sessionCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	// A renewed login at the same path takes effect without restarting the host.
	// A removed, public or malformed file stops the next authenticated call.
	cookie, err := readSession(s.Path)
	if err != nil {
		return nil, err
	}
	return map[string]string{"cookie": cookie}, nil
}

func (s sessionCredential) RequireTransportSecurity() bool { return s.Secure }
