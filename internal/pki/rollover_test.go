package pki

import (
	"crypto/rand"
	"crypto/x509"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// A CA rollover hands hosts a bundle with both CAs rather than a
// cross-signed certificate (§33.5), and this is why: every CA `shale init`
// made has a path length of zero, so the old root may sign leaves and
// nothing else. A chain from a leaf of the new CA through the new CA
// cross-signed by the old one breaks that limit, and every RFC 5280
// verifier, Go's among them, refuses it: a client that holds only the old
// root gains nothing from it. Both roots in the bundle verify either CA's
// leaves.
func TestCrossSigningCannotBridgeTheRollover(t *testing.T) {
	x := require.New(t)
	now := time.Now()

	old, err := NewCA("shale-ca", now)
	x.NoError(err)
	x.True(old.Cert.MaxPathLenZero, "what init has always made")
	next, err := NewCA("shale-ca next", now)
	x.NoError(err)

	// The new CA's subject and key, vouched for by the old one.
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		RawSubject:            next.Cert.RawSubject,
		SubjectKeyId:          next.Cert.SubjectKeyId,
		NotBefore:             next.Cert.NotBefore,
		NotAfter:              old.Cert.NotAfter,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}, old.Cert, &next.Key.PublicKey, old.Key)
	x.NoError(err)
	cross, err := x509.ParseCertificate(der)
	x.NoError(err)

	key, err := NewHostKey()
	x.NoError(err)
	csr, err := key.CSR("host")
	x.NoError(err)
	leaf, err := next.IssueHost(csr, pdid.New(22), Names{}, now)
	x.NoError(err)

	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	inter := x509.NewCertPool()
	inter.AddCert(cross)
	onlyOld, err := Pool(old.Bundle())
	x.NoError(err)
	_, err = leaf.Verify(x509.VerifyOptions{Roots: onlyOld, Intermediates: inter, KeyUsages: usage})
	var invalid x509.CertificateInvalidError
	x.True(errors.As(err, &invalid), "%v", err)
	x.Equal(x509.TooManyIntermediates, invalid.Reason, "the old root's path length forbids the cross-signed step")

	both, err := Pool(append(old.Bundle(), next.Bundle()...))
	x.NoError(err)
	_, err = leaf.Verify(x509.VerifyOptions{Roots: both, KeyUsages: usage})
	x.NoError(err, "the bundle with both roots verifies the new CA's leaves")
	oldLeaf, err := old.IssueHost(csr, pdid.New(22), Names{}, now)
	x.NoError(err)
	_, err = oldLeaf.Verify(x509.VerifyOptions{Roots: both, KeyUsages: usage})
	x.NoError(err, "and the old CA's")
}

func TestIssuedBy(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	a, err := NewCA("shale-ca", now)
	x.NoError(err)
	// The same name, another key: the name alone does not say.
	b, err := NewCA("shale-ca", now)
	x.NoError(err)
	key, err := NewHostKey()
	x.NoError(err)
	csr, err := key.CSR("host")
	x.NoError(err)
	leaf, err := a.IssueHost(csr, pdid.New(12), Names{}, now)
	x.NoError(err)

	x.True(IssuedBy(leaf, a.Cert))
	x.False(IssuedBy(leaf, b.Cert))
	x.True(IssuedBy(a.Cert, a.Cert), "a root signs itself")
	x.False(IssuedBy(nil, a.Cert))

	again, err := ParseCA(a.Bundle(), mustKey(t, a))
	x.NoError(err)
	x.Equal(a.Cert.Raw, again.Cert.Raw)
	_, err = ParseCA(a.Bundle(), mustKey(t, b))
	x.Error(err, "a key that is not the certificate's")
}

func mustKey(t *testing.T, ca *CA) []byte {
	t.Helper()
	b, err := EncodeKey(ca.Key)
	require.NoError(t, err)

	return b
}
