package pki

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// KeyPairEvery is how often a KeyPairFile looks at its files at most.
const KeyPairEvery = 5 * time.Second

// KeyPairFile is an external certificate and its key as files (§33.5),
// read again when they change: cert-manager rewrites a mounted Secret by
// swapping a symlink, and a listener serving it picks up the new pair at
// its next handshake without a restart. A pair that does not load keeps
// the last good one in service, and is tried again at the next look.
type KeyPairFile struct {
	CertFile string
	KeyFile  string
	// Every is how often the files are looked at at most; KeyPairEvery
	// when zero, every handshake when negative.
	Every time.Duration
	Log   *slog.Logger

	mu      sync.Mutex
	cur     *tls.Certificate
	stamp   [2]os.FileInfo
	checked time.Time
}

// LoadKeyPairFile reads the pair once; it fails when the files are not a
// certificate and its key.
func LoadKeyPairFile(certFile, keyFile string) (*KeyPairFile, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("pki: a certificate file and a key file, both")
	}
	k := &KeyPairFile{CertFile: certFile, KeyFile: keyFile}
	stamp, err := k.stat()
	if err != nil {
		return nil, err
	}
	c, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("pki: %s: %w", certFile, err)
	}
	k.cur, k.stamp, k.checked = &c, stamp, time.Now()

	return k, nil
}

// Certificate is the pair in service, read again first when the files
// changed since the last look.
func (k *KeyPairFile) Certificate() *tls.Certificate {
	k.mu.Lock()
	defer k.mu.Unlock()
	every := k.Every
	if every == 0 {
		every = KeyPairEvery
	}
	now := time.Now()
	if every > 0 && now.Sub(k.checked) < every {
		return k.cur
	}
	k.checked = now

	stamp, err := k.stat()
	if err != nil {
		k.warn(err)
		return k.cur
	}
	if same(stamp, k.stamp) {
		return k.cur
	}
	c, err := tls.LoadX509KeyPair(k.CertFile, k.KeyFile)
	if err != nil {
		// Half written, or a certificate beside the previous key: the
		// stamp stays, so the next look tries again.
		k.warn(err)
		return k.cur
	}
	k.cur, k.stamp = &c, stamp
	if k.Log != nil {
		k.Log.Info("certificate reloaded", "file", k.CertFile)
	}

	return k.cur
}

// GetCertificate is Certificate as tls.Config wants it.
func (k *KeyPairFile) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return k.Certificate(), nil
}

// stat follows the symlinks, so a swapped Secret volume reads as the new
// files it points at.
func (k *KeyPairFile) stat() ([2]os.FileInfo, error) {
	var out [2]os.FileInfo
	for i, f := range []string{k.CertFile, k.KeyFile} {
		fi, err := os.Stat(f)
		if err != nil {
			return out, err
		}
		out[i] = fi
	}

	return out, nil
}

func (k *KeyPairFile) warn(err error) {
	if k.Log != nil {
		k.Log.Warn("certificate not reloaded; the last good one stays", "file", k.CertFile, "err", err.Error())
	}
}

// same says the files are the ones read last: the same file, the same
// size, the same modification time.
func same(a, b [2]os.FileInfo) bool {
	for i := range a {
		if a[i] == nil || b[i] == nil || !os.SameFile(a[i], b[i]) || a[i].Size() != b[i].Size() || !a[i].ModTime().Equal(b[i].ModTime()) {
			return false
		}
	}

	return true
}
