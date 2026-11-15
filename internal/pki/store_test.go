package pki

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// issued is a host certificate for the store's key, as PEM, and its serial.
func issued(t *testing.T, ca *CA, st Store) ([]byte, string) {
	t.Helper()
	k, err := st.Key()
	require.NoError(t, err)
	csr, err := k.CSR("reader")
	require.NoError(t, err)
	c, err := ca.IssueClientHost(csr, pdid.New(23), time.Now())
	require.NoError(t, err)

	return EncodeCerts(c), Serial(c)
}

// A renewal replaces host.crt by a rename (§33.4): a program that opened
// the old file still reads all of the old one, the path names the new one
// at once, the modes are kept, and nothing is left behind.
func TestSetCertReplacesTheFileWhole(t *testing.T) {
	x := require.New(t)
	ca, err := NewCA("shale", time.Now())
	x.NoError(err)
	st := Store{Dir: t.TempDir()}
	first, _ := issued(t, ca, st)
	x.NoError(st.SetCert(first, ca.Bundle()))

	held, err := os.Open(st.CertPath())
	x.NoError(err)
	defer held.Close()
	before, err := os.Stat(st.CertPath())
	x.NoError(err)

	second, serial := issued(t, ca, st)
	x.NotEqual(first, second)
	x.NoError(st.SetCert(second, ca.Bundle()))

	var old bytes.Buffer
	_, err = old.ReadFrom(held)
	x.NoError(err)
	x.Equal(first, old.Bytes(), "the file a reader holds open is the old one, whole")
	got, err := os.ReadFile(st.CertPath())
	x.NoError(err)
	x.Equal(second, got)
	chain, err := st.Cert()
	x.NoError(err)
	x.Equal(serial, Serial(chain[0]), "the path names the new serial")

	after, err := os.Stat(st.CertPath())
	x.NoError(err)
	x.False(os.SameFile(before, after), "replaced, not rewritten in place")
	x.Equal(os.FileMode(0o644), after.Mode().Perm())
	key, err := os.Stat(st.KeyPath())
	x.NoError(err)
	x.Equal(os.FileMode(0o600), key.Mode().Perm())

	entries, err := os.ReadDir(st.Dir)
	x.NoError(err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	x.ElementsMatch([]string{"host.key", "host.crt", "ca.crt"}, names, "no temporary file is left")
}

// Whoever reads the files while the host renews again and again finds a
// whole certificate every time: the old one or the new one, never a
// truncated or interleaved file.
func TestSetCertIsNeverSeenHalfWritten(t *testing.T) {
	x := require.New(t)
	ca, err := NewCA("shale", time.Now())
	x.NoError(err)
	st := Store{Dir: t.TempDir()}
	a, serialA := issued(t, ca, st)
	b, serialB := issued(t, ca, st)
	x.NoError(st.SetCert(a, ca.Bundle()))

	var stop atomic.Bool
	var reads atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				chain, err := st.Cert()
				if err != nil || len(chain) != 1 {
					errs <- "a read found no whole certificate"
					return
				}
				if s := Serial(chain[0]); s != serialA && s != serialB {
					errs <- "a read found a certificate never written"
					return
				}
				reads.Add(1)
			}
		}()
	}
	for i := range 60 {
		v := a
		if i%2 == 0 {
			v = b
		}
		x.NoError(st.SetCert(v, ca.Bundle()))
	}
	stop.Store(true)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	x.Positive(reads.Load())
}

// A rotated key is written whole too, and the first one is made the same
// way.
func TestRotateReplacesTheKeyWhole(t *testing.T) {
	x := require.New(t)
	st := Store{Dir: filepath.Join(t.TempDir(), "reader")}
	k1, err := st.Key()
	x.NoError(err)
	held, err := os.Open(st.KeyPath())
	x.NoError(err)
	defer held.Close()

	k2, err := st.Rotate()
	x.NoError(err)
	x.NotEqual(k1.Fingerprint(), k2.Fingerprint())

	var old bytes.Buffer
	_, err = old.ReadFrom(held)
	x.NoError(err)
	pk, err := ParseKey(old.Bytes())
	x.NoError(err)
	x.True(pk.Equal(k1.Key), "the open file is the old key, whole")
	again, err := st.Key()
	x.NoError(err)
	x.Equal(k2.Fingerprint(), again.Fingerprint())
	info, err := os.Stat(st.KeyPath())
	x.NoError(err)
	x.Equal(os.FileMode(0o600), info.Mode().Perm())
}
