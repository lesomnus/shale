//go:build linux

package discovery

import (
	"os"
	"syscall"
)

// Do not block on a FIFO or follow a symlink swapped in after Lstat. The caller
// checks the opened descriptor's regular-file type and identity before reading.
func openLeaseFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
}
