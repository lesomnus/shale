//go:build linux

package discovery

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaseFileOpenDoesNotFollowOrBlockAfterReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "leases")
	require.NoError(t, os.Symlink("another-file", path))
	_, err := openLeaseFile(path)
	require.Error(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, syscall.Mkfifo(path, 0600))
	f, err := openLeaseFile(path)
	require.NoError(t, err, "a replacement FIFO must not wait for a writer")
	defer f.Close()
	info, err := f.Stat()
	require.NoError(t, err)
	require.False(t, info.Mode().IsRegular(), "the caller refuses this descriptor before any read")
}
