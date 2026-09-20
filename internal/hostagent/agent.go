// Package hostagent is what every host does alike (§33.4): keep a key in its
// state directory, join the Control Plane until adopted, hold the certificate
// it was issued, renew it, and dial the CP with it.
package hostagent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostid"
	"github.com/lesomnus/shale/internal/pki"
)

// JoinInterval is how often a host asks again while pending (§33.4).
const JoinInterval = 10 * time.Second

// Version is stamped by the build.
var Version = "dev"

// Agent is one host's identity handling.
type Agent struct {
	// HardwareId overrides the machine's identity when set (§33.4).
	HardwareId string
	Kind       pdid.Domain
	Store      pki.Store
	// Cp is the Control Plane address: host:port, or a URL with scheme
	// `https` (the default) or `http` (development only).
	Cp string
	// CaHash makes the first-contact pin a check.
	CaHash string
	// Dev allows plaintext to the CP.
	Dev bool
	Log *slog.Logger

	key      *pki.HostKey
	id       pdid.Id
	hostname string
	hw       hostid.Identity
}

// Init reads the key and the hardware identity.
func (a *Agent) Init() error {
	k, err := a.Store.Key()
	if err != nil {
		return err
	}
	a.key = k

	hw, err := hostid.ReadOr(a.Store.Dir)
	if err != nil {
		return err
	}
	if a.HardwareId != "" {
		// Declared: a second host in one process (a test, a lab) must not
		// share the machine's identity.
		hw = hostid.Identity{Id: a.HardwareId, Kind: api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID}
	}
	a.hw = hw
	a.hostname, _ = os.Hostname()
	if a.Log == nil {
		a.Log = slog.Default()
	}

	// The certificate names the row, so a host that has one knows its id.
	if chain, err := a.Store.Cert(); err == nil && len(chain) > 0 {
		if id, ok := pki.IdOf(chain[0]); ok {
			a.id = id
		}
	}

	return nil
}

// Id is the host's row, Nil until adopted.
func (a *Agent) Id() pdid.Id { return a.id }

// Hostname is what the host calls itself.
func (a *Agent) Hostname() string { return a.hostname }

// Fingerprint is what the host prints beside "waiting for adoption".
func (a *Agent) Fingerprint() string { return a.key.Fingerprint() }

// HostJoin is the request every Join carries.
func (a *Agent) HostJoin() (*api.HostJoin, error) {
	csr, err := a.key.CSR(a.hostname)
	if err != nil {
		return nil, err
	}

	return api.HostJoin_builder{
		HardwareId:     a.hw.Id,
		HardwareIdKind: a.hw.Kind,
		Hostname:       a.hostname,
		Csr:            csr,
		KeyFingerprint: a.key.Fingerprint(),
		DateJoined:     timestamppb.Now(),
		Version:        Version,
	}.Build(), nil
}

// Certificate is the chain the host holds, nil before adoption.
func (a *Agent) Certificate() (*tls.Certificate, error) {
	pem, err := a.Store.CertPEM()
	if err != nil || len(pem) == 0 {
		return nil, err
	}
	kb, err := pki.EncodeKey(a.key.Key)
	if err != nil {
		return nil, err
	}
	c, err := tls.X509KeyPair(pem, kb)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// Leaf is the host certificate itself, for its serial and expiry.
func (a *Agent) Leaf() (*x509.Certificate, error) {
	chain, err := a.Store.Cert()
	if err != nil || len(chain) == 0 {
		return nil, err
	}

	return chain[0], nil
}

// endpoint splits the CP address into what to dial and whether to use TLS.
func (a *Agent) endpoint() (addr string, plain bool, err error) {
	v := a.Cp
	if v == "" {
		return "", false, errors.New("no control plane address; set `cp` or pass --cp")
	}
	if !strings.Contains(v, "://") {
		v = "https://" + v
	}
	u, err := url.Parse(v)
	if err != nil {
		return "", false, err
	}
	addr = u.Host
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "7400")
	}
	plain = u.Scheme == "http"
	if plain && !a.Dev {
		return "", false, errors.New("a plaintext control plane address needs --dev")
	}

	return addr, plain, nil
}

// tlsConfig verifies the CP against the pinned bundle, or pins on first
// contact (§33.4), and presents the host certificate when it has one.
func (a *Agent) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if cert, err := a.Certificate(); err == nil && cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}

	bundle, err := a.Store.Bundle()
	if err != nil {
		return nil, err
	}
	if len(bundle) > 0 {
		pool, err := pki.Pool(bundle)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool

		return cfg, nil
	}

	// First contact: accept what is presented, check it against --ca-hash
	// when given, and pin it.
	cfg.InsecureSkipVerify = true
	cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("no certificate presented")
		}
		var chain []*x509.Certificate
		for _, b := range raw {
			c, err := x509.ParseCertificate(b)
			if err != nil {
				return err
			}
			chain = append(chain, c)
		}
		root := chain[len(chain)-1]
		if !root.IsCA {
			return errors.New("the control plane presented no CA in its chain; set control.names or ca_hash")
		}
		if a.CaHash != "" && !strings.EqualFold(pki.Fingerprint(root), a.CaHash) {
			return fmt.Errorf("the control plane's CA is %s, not the expected %s", pki.Fingerprint(root), a.CaHash)
		}
		if err := a.Store.SetBundle(pki.EncodeCerts(root)); err != nil {
			return err
		}
		a.Log.Info("pinned the control plane's CA", "fingerprint", pki.Fingerprint(root))

		return nil
	}

	return cfg, nil
}

// Dial connects to the CP: mTLS with the host certificate when it has one,
// plaintext with the plain header in development.
func (a *Agent) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	addr, plain, err := a.endpoint()
	if err != nil {
		return nil, err
	}

	var opts []grpc.DialOption
	if plain {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if !a.id.IsZero() {
			opts = append(opts, auth.Inject(auth.PlainProvider(a.id.String()))...)
		}
	} else {
		cfg, err := a.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	}

	return grpc.NewClient(addr, opts...)
}

// DialAddr opens a gRPC connection to another host (a relay) with the
// pinned CA; `plain` is development mode.
func (a *Agent) DialAddr(ctx context.Context, addr string, plain bool) (*grpc.ClientConn, error) {
	if plain {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cfg, err := a.tlsConfig()
	if err != nil {
		return nil, err
	}

	return grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
}

// HTTPClient is a client for the data planes: it trusts the pinned CA and
// presents the host certificate, since a node verifies a client
// certificate when one is given (§33.5).
func (a *Agent) HTTPClient() (*http.Client, error) {
	tr := &http.Transport{
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	bundle, err := a.Store.Bundle()
	if err != nil {
		return nil, err
	}
	if len(bundle) > 0 {
		pool, err := pki.Pool(bundle)
		if err != nil {
			return nil, err
		}
		cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		if cert, err := a.Certificate(); err == nil && cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
		tr.TLSClientConfig = cfg
	}

	return &http.Client{Transport: tr}, nil
}

// Answer is what a kind's Join RPC hands back to the loop.
type Answer struct {
	*api.JoinAnswer
	Keys []*api.KeyEntry
}

// JoinFunc calls the kind's Join RPC with the request.
type JoinFunc func(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (Answer, error)

// Join asks until adopted, storing the certificate and the bundle, and
// answers the final adoption.
func (a *Agent) Join(ctx context.Context, call JoinFunc) (Answer, error) {
	said := false
	for {
		conn, err := a.Dial(ctx)
		if err != nil {
			return Answer{}, err
		}

		hj, err := a.HostJoin()
		if err != nil {
			conn.Close()
			return Answer{}, err
		}

		ans, err := call(ctx, conn, hj)
		conn.Close()
		if err != nil {
			a.Log.Warn("join", "err", err.Error())
		} else if ans.JoinAnswer.GetState() == api.HostState_HOST_STATE_ADOPTED && len(ans.JoinAnswer.GetCertificate()) > 0 {
			if err := a.Store.SetCert(ans.JoinAnswer.GetCertificate(), ans.JoinAnswer.GetCaBundle()); err != nil {
				return Answer{}, err
			}
			id, err := pdid.From(ans.JoinAnswer.GetId())
			if err != nil {
				return Answer{}, err
			}
			a.id = id
			a.Log.Info("adopted", "id", id.String(), "alias", ans.JoinAnswer.GetAlias())

			return ans, nil
		} else if !said {
			a.Log.Info("waiting for adoption",
				"alias", ans.JoinAnswer.GetAlias(),
				"hardware", a.hw.Id,
				"key", a.key.Fingerprint(),
				"message", ans.JoinAnswer.GetMessage())
			said = true
		}

		select {
		case <-ctx.Done():
			return Answer{}, ctx.Err()
		case <-time.After(JoinInterval):
		}
	}
}

// Ready says whether the host holds a certificate it can still use.
func (a *Agent) Ready(now time.Time) bool {
	leaf, err := a.Leaf()
	if err != nil || leaf == nil {
		return false
	}

	return now.Before(leaf.NotAfter) && !a.id.IsZero()
}

// NeedsRenewal is two thirds of the way through the certificate (§33.5).
func (a *Agent) NeedsRenewal(now time.Time) bool {
	leaf, err := a.Leaf()
	if err != nil || leaf == nil {
		return false
	}

	return now.After(pki.RenewAt(leaf))
}

// Renew asks the kind's RenewCertificate with a CSR for the current key and
// stores the answer.
func (a *Agent) Renew(ctx context.Context, conn *grpc.ClientConn, call func(ctx context.Context, conn *grpc.ClientConn, csr []byte) (cert, bundle []byte, err error)) error {
	csr, err := a.key.CSR(a.hostname)
	if err != nil {
		return err
	}
	cert, bundle, err := call(ctx, conn, csr)
	if err != nil {
		return err
	}

	return a.Store.SetCert(cert, bundle)
}

// Interfaces lists the machine's interfaces and addresses, which a node or
// relay reports (§34.10).
func Interfaces() []*api.HostInterface {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []*api.HostInterface
	for _, i := range ifs {
		if i.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		var vs []string
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && !n.IP.IsLinkLocalUnicast() {
				vs = append(vs, n.IP.String())
			}
		}
		if len(vs) > 0 {
			out = append(out, api.HostInterface_builder{Name: i.Name, Addresses: vs}.Build())
		}
	}

	return out
}

// BundleHash is what heartbeats report about the bundle held.
func (a *Agent) BundleHash() string {
	b, err := a.Store.Bundle()
	if err != nil || len(b) == 0 {
		return ""
	}

	return pki.BundleHash(b)
}
