//go:build !linux

package discovery

import "os"

func openLeaseFile(path string) (*os.File, error) { return os.Open(path) }
