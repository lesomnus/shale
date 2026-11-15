package cli

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
)

func TestReaderConfig(t *testing.T) {
	t.Run("the file maps onto the agent", func(t *testing.T) {
		c := cmd.Config{State: "/var/lib/shale", Cp: "cp.example.com:7400", CaHash: "sha256:00", Tenant: "acme"}
		c.Reader.HeartbeatInterval = 5 * time.Second
		cfg, err := ReaderConfig(&c)
		require.NoError(t, err)
		require.Equal(t, filepath.Join("/var/lib/shale", "reader"), cfg.StateDir)
		require.Equal(t, "cp.example.com:7400", cfg.Cp)
		require.Equal(t, "sha256:00", cfg.CaHash)
		require.Equal(t, "acme", cfg.Tenant)
		require.Equal(t, 5*time.Second, cfg.HeartbeatInterval)
		require.False(t, cfg.Dev)
		require.Nil(t, cfg.CpRoots, "the pinned bundle alone unless a CA is named")
	})
	t.Run("development mode dials the CP --dev describes", func(t *testing.T) {
		c := cmd.Config{}
		ApplyDev(&c, t.TempDir())
		cfg, err := ReaderConfig(&c)
		require.NoError(t, err)
		require.True(t, cfg.Dev)
		require.Equal(t, c.Client.Addr, cfg.Cp)
		require.Equal(t, filepath.Join(c.Dev, "reader"), cfg.StateDir)
	})
	t.Run("a reader needs a CP", func(t *testing.T) {
		_, err := ReaderConfig(&cmd.Config{})
		require.ErrorContains(t, err, "server.addr")
	})
	t.Run("the localhost listener is not built", func(t *testing.T) {
		c := cmd.Config{Cp: "cp.example.com"}
		c.Reader.Addr = "127.0.0.1:7450"
		_, err := ReaderConfig(&c)
		require.ErrorContains(t, err, "not built")
	})
	t.Run("a CA named for the CP, from a file", func(t *testing.T) {
		ca, err := pki.NewCA("public", time.Now())
		require.NoError(t, err)
		file := filepath.Join(t.TempDir(), "public.crt")
		require.NoError(t, os.WriteFile(file, ca.Bundle(), 0o644))
		c := cmd.Config{Cp: "cp.example.com"}
		c.Reader.CpCa = file
		cfg, err := ReaderConfig(&c)
		require.NoError(t, err)
		require.NotNil(t, cfg.CpRoots)
		_, err = ca.Cert.Verify(x509.VerifyOptions{Roots: cfg.CpRoots})
		require.NoError(t, err)

		c.Reader.CpCa = filepath.Join(t.TempDir(), "missing.crt")
		_, err = ReaderConfig(&c)
		require.ErrorContains(t, err, "reader.cp_ca")
	})
	t.Run("the system's roots, named", func(t *testing.T) {
		system := x509.NewCertPool()
		prev := pki.SystemRoots
		pki.SystemRoots = func() (*x509.CertPool, error) { return system, nil }
		t.Cleanup(func() { pki.SystemRoots = prev })
		c := cmd.Config{Cp: "cp.example.com"}
		c.Reader.CpCa = "system"
		cfg, err := ReaderConfig(&c)
		require.NoError(t, err)
		require.Same(t, system, cfg.CpRoots)
	})
}
