package cmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// cpPair writes a CP certificate from `ca`, issued `age` ago, and its key.
func cpPair(t *testing.T, ca *pki.CA, age time.Duration) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	c, err := ca.IssueServer(&key.PublicKey, pki.CpName, pki.Names{DNS: []string{"cp.example.com"}, IPs: []net.IP{net.ParseIP("192.0.2.10")}}, pki.CpLifetime, time.Now().Add(-age))
	require.NoError(t, err)
	kb, err := pki.EncodeKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, CpCertFile), filepath.Join(dir, CpKeyFile)
	require.NoError(t, os.WriteFile(certFile, pki.EncodeCerts(c, ca.Cert), 0o644))
	require.NoError(t, os.WriteFile(keyFile, kb, 0o600))

	return certFile, keyFile
}

// The CP presents the certificate `shale init` wrote while it is short of
// its renewal, and from two thirds of its life one it issues itself from
// the same CA, for the same key and names (§33.5). One no shale CA issued
// is presented as it is.
func TestCpCertificateRenewsItself(t *testing.T) {
	x := require.New(t)
	ca, err := pki.NewCA("shale-ca", time.Now().Add(-time.Hour*24*400))
	x.NoError(err)
	deps := &core.Deps{CA: ca}

	certFile, keyFile := cpPair(t, ca, 24*time.Hour)
	fresh, err := newCpCert(deps.Authorities(), certFile, keyFile, nil, nil, nil)
	x.NoError(err)
	x.Same(fresh.file, fresh.Certificate(), "the file, short of its renewal")

	certFile, keyFile = cpPair(t, ca, 300*24*time.Hour)
	old, err := newCpCert(deps.Authorities(), certFile, keyFile, []string{"cp.internal.example.com"}, nil, nil)
	x.NoError(err)
	got := old.Certificate()
	x.NotSame(old.file, got, "issued again two thirds of the way through")
	x.Same(got, old.Certificate(), "and kept")
	x.True(pki.IssuedBy(got.Leaf, ca.Cert))
	x.True(pki.IsCp(got.Leaf))
	x.True(time.Now().Before(pki.RenewAt(got.Leaf)))
	x.Equal(old.file.PrivateKey, got.PrivateKey, "for the same key")
	x.Contains(got.Leaf.DNSNames, "cp.example.com", "the names it had")
	x.Contains(got.Leaf.DNSNames, "cp.internal.example.com", "and control.names")
	x.Len(got.Certificate, 2, "with the CA, which a host pins on first contact")
	pool, err := pki.Pool(ca.Bundle())
	x.NoError(err)
	_, err = got.Leaf.Verify(x509VerifyServer(pool, "cp.example.com"))
	x.NoError(err)

	// A node of an earlier release that asks for this CA's certificates
	// gets the one presented now.
	cc, err := old.ClientCertificate(&tls.CertificateRequestInfo{AcceptableCAs: [][]byte{ca.Cert.RawSubject}, Version: tls.VersionTLS13, SignatureSchemes: []tls.SignatureScheme{tls.ECDSAWithP256AndSHA256}})
	x.NoError(err)
	x.Same(got, cc)

	public, err := pki.NewCA("public", time.Now())
	x.NoError(err)
	certFile, keyFile = cpPair(t, public, 300*24*time.Hour)
	external, err := newCpCert(deps.Authorities(), certFile, keyFile, nil, nil, nil)
	x.NoError(err)
	x.Same(external.file, external.Certificate(), "an external certificate is the operator's to renew")
}

func x509VerifyServer(pool *x509.CertPool, name string) x509.VerifyOptions {
	return x509.VerifyOptions{Roots: pool, DNSName: name, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
}
