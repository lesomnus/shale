package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writePair issues a server certificate for name and writes it and its key
// into dir as a Secret volume holds them.
func writePair(t *testing.T, ca *CA, dir, name string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert, err := ca.IssueServer(&key.PublicKey, name, Names{DNS: []string{name}}, time.Hour, time.Now())
	require.NoError(t, err)
	kb, err := EncodeKey(key)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), EncodeCerts(cert, ca.Cert), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.key"), kb, 0o600))
}

// swap points `..data` at dir the way the kubelet updates a mounted Secret:
// a new symlink renamed over the old one.
func swap(t *testing.T, root, dir string) {
	t.Helper()
	tmp := filepath.Join(root, "..data_tmp")
	require.NoError(t, os.Symlink(filepath.Base(dir), tmp))
	require.NoError(t, os.Rename(tmp, filepath.Join(root, "..data")))
}

func served(t *testing.T, k *KeyPairFile) string {
	t.Helper()
	c := k.Certificate()
	require.NotNil(t, c)
	require.NotNil(t, c.Leaf)

	return c.Leaf.Subject.CommonName
}

// An external certificate is read again when cert-manager rewrites the
// Secret, and a pair that does not load leaves the last good one in
// service (§33.5).
func TestKeyPairFileReload(t *testing.T) {
	ca, err := NewCA("test", time.Now())
	require.NoError(t, err)
	root := t.TempDir()
	writePair(t, ca, filepath.Join(root, "..v1"), "one.example.com")
	swap(t, root, filepath.Join(root, "..v1"))
	for _, f := range []string{"tls.crt", "tls.key"} {
		require.NoError(t, os.Symlink(filepath.Join("..data", f), filepath.Join(root, f)))
	}

	k, err := LoadKeyPairFile(filepath.Join(root, "tls.crt"), filepath.Join(root, "tls.key"))
	require.NoError(t, err)
	require.Equal(t, "one.example.com", served(t, k))

	// Within Every, nothing is looked at.
	writePair(t, ca, filepath.Join(root, "..v2"), "two.example.com")
	swap(t, root, filepath.Join(root, "..v2"))
	require.Equal(t, "one.example.com", served(t, k))

	k.Every = -1
	require.Equal(t, "two.example.com", served(t, k))

	// A certificate that does not match its key: the last good pair stays,
	// and the next look tries again.
	writePair(t, ca, filepath.Join(root, "..v3"), "three.example.com")
	other, err := os.ReadFile(filepath.Join(root, "..v1", "tls.key"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "..v3", "tls.key"), other, 0o600))
	swap(t, root, filepath.Join(root, "..v3"))
	require.Equal(t, "two.example.com", served(t, k))

	writePair(t, ca, filepath.Join(root, "..v3"), "three.example.com")
	require.Equal(t, "three.example.com", served(t, k))

	// Gone altogether: still the last good pair.
	require.NoError(t, os.Remove(filepath.Join(root, "..data")))
	require.Equal(t, "three.example.com", served(t, k))
}

func TestLoadKeyPairFile(t *testing.T) {
	_, err := LoadKeyPairFile("", "key.pem")
	require.Error(t, err)
	_, err = LoadKeyPairFile(filepath.Join(t.TempDir(), "missing.crt"), filepath.Join(t.TempDir(), "missing.key"))
	require.Error(t, err)
}
