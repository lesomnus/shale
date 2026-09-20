package pki

import (
	"crypto/x509"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

func TestIssueAndVerify(t *testing.T) {
	x := require.New(t)
	now := time.Now()

	ca, err := NewCA("shale-ca", now)
	x.NoError(err)

	dir := t.TempDir()
	x.NoError(ca.Save(dir+"/ca.crt", dir+"/ca.key"))
	again, err := Load(dir+"/ca.crt", dir+"/ca.key")
	x.NoError(err)
	x.Equal(ca.Cert.Raw, again.Cert.Raw)

	st := Store{Dir: dir + "/host"}
	k, err := st.Key()
	x.NoError(err)
	k2, err := st.Key()
	x.NoError(err)
	x.Equal(k.Fingerprint(), k2.Fingerprint(), "the key is kept")

	csr, err := k.CSR("davy")
	x.NoError(err)

	id := pdid.New(12)
	c, err := ca.IssueHost(csr, id, Names{DNS: []string{"davy.nodes.example"}, IPs: []net.IP{net.ParseIP("10.1.2.73")}}, now)
	x.NoError(err)

	got, ok := IdOf(c)
	x.True(ok)
	x.Equal(id, got)
	x.False(IsCp(c))
	x.Equal([]string{"davy.nodes.example"}, c.DNSNames)

	pool, err := Pool(ca.Bundle())
	x.NoError(err)
	_, err = c.Verify(x509.VerifyOptions{Roots: pool, DNSName: "davy.nodes.example", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	x.NoError(err)
	_, err = c.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	x.NoError(err)

	// Stored and read back.
	x.NoError(st.SetCert(EncodeCerts(c), ca.Bundle()))
	chain, err := st.Cert()
	x.NoError(err)
	x.Len(chain, 1)
	x.Equal(Serial(c), Serial(chain[0]))

	renew := RenewAt(c)
	x.True(renew.After(now.Add(59 * 24 * time.Hour)))
	x.True(renew.Before(now.Add(61 * 24 * time.Hour)))

	cp, err := ca.IssueServer(&k.Key.PublicKey, CpName, Names{DNS: []string{"cp.example"}}, CpLifetime, now)
	x.NoError(err)
	x.True(IsCp(cp))
}

func TestBadCsr(t *testing.T) {
	x := require.New(t)
	ca, err := NewCA("ca", time.Now())
	x.NoError(err)
	_, err = ca.IssueHost([]byte("junk"), pdid.New(12), Names{}, time.Now())
	x.Error(err)
}
