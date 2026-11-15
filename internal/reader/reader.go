// Package reader is the Reader agent, `shale serve reader` (§33.4): it keeps
// a Reader host's credential current in its state directory -- the key, the
// certificate the Control Plane issued when an operator adopted it, and the
// CA bundle -- for whatever beside it reads with that credential: a media
// server, or a viewer of its own that calls the tenant API (Sources, Live,
// Timeline) with the certificate and sends its browsers to the relay and the
// nodes with the tokens the answers carry.
//
// It is what every host does alike (internal/hostagent) and nothing else:
// join with a CSR until adopted, heartbeat over mTLS, renew at two thirds of
// the certificate's life or when the CP asks, and go back to Join with a new
// key once the certificate has run out. It holds nothing a person has: no
// session, no signing key, no KEK.
//
// The localhost listener of §33.4, the tenant API without TLS for a media
// server that holds no key, is not built; `reader.addr` is refused.
package reader

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
)

// DomReader is the domain byte of a Reader (§35.3).
const DomReader pdid.Domain = 23

// DefaultHeartbeatInterval is how often a reader says it is there.
const DefaultHeartbeatInterval = 30 * time.Second

// Config is one reader agent's settings.
type Config struct {
	// StateDir holds host.key, host.crt and ca.crt (§34.3).
	StateDir string
	// Cp is the tenant API's gRPC address (`server.addr`): host:port, or a
	// URL with scheme `https` (the default) or `http` (development only).
	Cp     string
	CaHash string
	// CpRoots are CAs trusted explicitly for the CP's certificate, beside
	// the pinned bundle (`reader.cp_ca`, §33.5).
	CpRoots *x509.CertPool
	// Tenant is the tenant the reader joins, implied with only one.
	Tenant string
	Dev    bool
	// HardwareId overrides the machine's identity (tests, a second host on
	// one machine).
	HardwareId string

	HeartbeatInterval time.Duration
	// RetryInterval is how long a reader waits after the CP could not be
	// reached or refused it, and how often it asks while pending;
	// hostagent.JoinInterval when zero.
	RetryInterval time.Duration

	Log *slog.Logger
	// Now is the clock the reader decides renewal and expiry by.
	Now func() time.Time
}

func (c *Config) defaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = hostagent.JoinInterval
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Reader is one reader agent.
type Reader struct {
	cfg   Config
	log   *slog.Logger
	agent *hostagent.Agent
}

// New prepares a reader; Run does the work.
func New(cfg Config) (*Reader, error) {
	cfg.defaults()
	if cfg.StateDir == "" {
		return nil, errors.New("a reader needs a state directory")
	}
	if cfg.Cp == "" {
		return nil, errors.New("a reader needs cp: the tenant API's gRPC address to join (§33.4)")
	}

	return &Reader{
		cfg: cfg,
		log: cfg.Log,
		agent: &hostagent.Agent{
			Kind:       DomReader,
			HardwareId: cfg.HardwareId,
			Store:      pki.Store{Dir: cfg.StateDir},
			Cp:         cfg.Cp,
			CaHash:     cfg.CaHash,
			CpRoots:    cfg.CpRoots,
			JoinEvery:  cfg.RetryInterval,
			Dev:        cfg.Dev,
			Log:        cfg.Log,
		},
	}, nil
}

// Id is the reader's row, Nil until adopted.
func (r *Reader) Id() pdid.Id { return r.agent.Id() }

// Store is where the credential is kept.
func (r *Reader) Store() pki.Store { return r.agent.Store }

// errRenewed ends a session whose certificate was just renewed: the CP
// refuses the old serial from then on, on the connection already open too,
// so the next call is on a new connection with the new one.
var errRenewed = errors.New("certificate renewed")

// Run keeps the credential until the context is done.
func (r *Reader) Run(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.StateDir, 0o700); err != nil {
		return err
	}
	if err := r.agent.Init(); err != nil {
		return err
	}
	st := r.agent.Store
	r.log.Info("reader", "key", st.KeyPath(), "certificate", st.CertPath(), "bundle", st.BundlePath(), "key_fingerprint", r.agent.Fingerprint())

	rejoin := false
	for ctx.Err() == nil {
		now := r.cfg.Now()
		if r.agent.Expired(now) {
			// §33.4: an expired certificate cannot be renewed, and the CP
			// hands the same one back to a Join with the same key. A new
			// key is shown as "known host, new key" under this row, for an
			// operator to adopt again.
			leaf, _ := r.agent.Leaf()
			r.log.Warn("the certificate has expired; joining again with a new key for an operator to adopt",
				"serial", pki.Serial(leaf), "expired", leaf.NotAfter)
			if err := r.agent.Rekey(); err != nil {
				return err
			}
			rejoin = true
		}
		if rejoin || !r.agent.Ready(now) {
			// Without the certificate when it has expired, which the CP's
			// TLS would refuse; Join asks for none.
			if _, err := r.agent.JoinBootstrap(ctx, r.join); err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return err
			}
			rejoin = false
			if !r.agent.Ready(r.cfg.Now()) {
				// What came back has run out already: the top of the loop
				// says so and asks with a new key.
				continue
			}
		}

		conn, err := r.agent.Dial(ctx)
		if err == nil {
			err = r.session(ctx, conn)
			conn.Close()
		}
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errRenewed) {
			continue
		}
		if status.Code(err) == codes.Unauthenticated {
			// The CP knows this certificate and will not take it: the
			// reader was erased, or its certificate replaced, by a renewal
			// whose answer was lost or an adoption of a new key. Join says
			// which: the current certificate for this key, or a pending row.
			r.log.Warn("the control plane refused the certificate; joining again", "err", err.Error())
			rejoin = true
		} else {
			r.log.Warn("control plane", "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.RetryInterval):
		}
	}

	return nil
}

func (r *Reader) join(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (hostagent.Answer, error) {
	resp, err := api.NewReaderServiceClient(conn).Join(ctx, api.ReaderJoinRequest_builder{Host: hj, Tenant: r.cfg.Tenant}.Build())
	if err != nil {
		return hostagent.Answer{}, err
	}

	return hostagent.Answer{JoinAnswer: resp.GetAnswer()}, nil
}

// session heartbeats on one connection until it fails, the context ends,
// or the certificate is renewed.
func (r *Reader) session(ctx context.Context, conn *grpc.ClientConn) error {
	readers := api.NewReaderServiceClient(conn)
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	first := true
	for {
		hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		resp, err := readers.Heartbeat(hctx, api.ReaderHeartbeatRequest_builder{
			CaHash:  r.agent.BundleHash(),
			Version: hostagent.Version,
		}.Build())
		cancel()
		if err != nil {
			return err
		}
		if first {
			if leaf, _ := r.agent.Leaf(); leaf != nil {
				r.log.Info("reading", "id", r.agent.Id().String(), "serial", pki.Serial(leaf), "expires", leaf.NotAfter)
			}
			first = false
		}

		if resp.GetRenewCertificate() || r.agent.NeedsRenewal(r.cfg.Now()) {
			err := r.agent.Renew(ctx, conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
				v, err := api.NewReaderServiceClient(conn).RenewCertificate(ctx, api.ReaderRenewCertificateRequest_builder{Csr: csr}.Build())
				if err != nil {
					return nil, nil, err
				}

				return v.GetCertificate(), v.GetCaBundle(), nil
			})
			if err == nil {
				leaf, _ := r.agent.Leaf()
				r.log.Info("certificate renewed", "serial", pki.Serial(leaf), "expires", leaf.NotAfter)

				return errRenewed
			}
			if status.Code(err) == codes.Unauthenticated {
				return err
			}
			// Not this time: the certificate holds until it expires, and
			// the next heartbeat asks again.
			r.log.Warn("renew certificate", "err", err.Error())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
