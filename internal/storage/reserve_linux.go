package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// reserve keeps one contiguous extent for the upload without changing the
// file size (§12.2).
func reserve(f *os.File, n int64) {
	if n <= 0 {
		return
	}
	unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, n)
}

// unreserve releases the reservation beyond EOF at completion.
func unreserve(f *os.File, size, hint int64) {
	if hint <= size {
		return
	}
	unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, size, hint-size)
}
