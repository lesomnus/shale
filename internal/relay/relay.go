// Package relay is the live path (§39): it takes a producer's TS bytes over
// one gRPC stream and fans the video out to viewers over WebRTC (WHEP). It
// holds no state, reads no object, and trusts one thing: a CP signature on
// a token.
package relay

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
)

// DomRelay is the domain byte of a Relay (§35.3).
const DomRelay pdid.Domain = 24

// Config is a relay's own settings (§36.1, relay scope).
type Config struct {
	StateDir   string
	Cp         string
	CaHash     string
	Dev        bool
	HardwareId string

	// IngestAddr is the gRPC listener producers dial; WhepAddr the HTTPS
	// listener viewers dial. Advertise overrides the addresses reported.
	IngestAddr string
	WhepAddr   string
	Advertise  string

	IdleStop        time.Duration
	MaxViewers      int
	ViewersPerActor int
	// Ice servers, e.g. "stun:stun.l.google.com:19302".
	Ice []string
	// Nat1To1 are public IPs to announce as host candidates.
	Nat1To1    []string
	UdpPortMin int
	UdpPortMax int

	HeartbeatInterval time.Duration
	TokenSkew         time.Duration
	Log               *slog.Logger
}

func (c *Config) defaults() {
	if c.IngestAddr == "" {
		c.IngestAddr = ":7430"
	}
	if c.WhepAddr == "" {
		c.WhepAddr = ":7431"
	}
	if c.IdleStop == 0 {
		c.IdleStop = 10 * time.Second
	}
	if c.MaxViewers == 0 {
		c.MaxViewers = 500
	}
	if c.ViewersPerActor == 0 {
		c.ViewersPerActor = 16
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Relay is one relay process.
type Relay struct {
	cfg   Config
	log   *slog.Logger
	agent *hostagent.Agent
	keys  *hostagent.KeyRing
	id    pdid.Id

	connMu sync.Mutex
	conn   *grpc.ClientConn

	sources *sources
	whep    *whepServer

	// IngestAddr and WhepAddr are the addresses bound, once Run listens.
	IngestAddr string
	WhepAddr   string
	// Ready is closed once the relay serves.
	Ready chan struct{}
}

// New prepares a relay; Run does the work.
func New(cfg Config) (*Relay, error) {
	cfg.defaults()
	r := &Relay{cfg: cfg, log: cfg.Log, keys: hostagent.NewKeyRing(cfg.StateDir), Ready: make(chan struct{})}
	r.keys.Verifier.Skew = cfg.TokenSkew
	r.agent = &hostagent.Agent{Kind: DomRelay, Store: pki.Store{Dir: cfg.StateDir}, Cp: cfg.Cp, CaHash: cfg.CaHash, Dev: cfg.Dev, HardwareId: cfg.HardwareId, Log: cfg.Log}
	r.sources = newSources(r)

	return r, nil
}

// Id is the relay's row, Nil until adopted.
func (r *Relay) Id() pdid.Id { return r.id }

// Verifier checks tokens against the current key set.
func (r *Relay) Verifier() *token.Verifier { return r.keys.Verifier }

// Run joins, then serves ingest and WHEP until the context is done.
func (r *Relay) Run(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.StateDir, 0o700); err != nil {
		return err
	}
	if err := r.agent.Init(); err != nil {
		return err
	}
	r.keys.Load()

	// The listeners first, so the join reports the ports actually bound.
	il, err := net.Listen("tcp", r.cfg.IngestAddr)
	if err != nil {
		return err
	}
	r.IngestAddr = il.Addr().String()
	wl, err := net.Listen("tcp", r.cfg.WhepAddr)
	if err != nil {
		return err
	}
	r.WhepAddr = wl.Addr().String()

	if !r.agent.Ready(time.Now()) {
		ans, err := r.agent.Join(ctx, r.joinCall)
		if err != nil {
			return err
		}
		r.keys.Apply(ans.Keys)
	}
	r.id = r.agent.Id()
	r.log.Info("relay", "id", r.id.String())

	tlsCfg, err := r.tlsConfig()
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)

	// Ingest: producers dial with a publish token; TLS with the host
	// certificate, no client certificate needed (§39.7).
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		c := tlsCfg.Clone()
		c.NextProtos = []string{"h2"}
		opts = append(opts, grpc.Creds(credentials.NewTLS(c)))
	}
	gs := grpc.NewServer(opts...)
	api.RegisterRelayIngestServer(gs, &ingest{r: r})
	g.Go(func() error { return gs.Serve(il) })
	g.Go(func() error {
		<-ctx.Done()
		gs.GracefulStop()

		return nil
	})

	// WHEP.
	w, err := newWhepServer(r)
	if err != nil {
		return err
	}
	r.whep = w
	httpSrv := &http.Server{Handler: w, ReadHeaderTimeout: 30 * time.Second}
	g.Go(func() error {
		var err error
		if tlsCfg != nil {
			httpSrv.TLSConfig = tlsCfg
			err = httpSrv.ServeTLS(wl, "", "")
		} else {
			err = httpSrv.Serve(wl)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	})
	g.Go(func() error {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		w.closeAll()

		return httpSrv.Shutdown(shut)
	})

	r.log.Info("serving", "ingest", r.IngestAddr, "whep", r.WhepAddr, "tls", tlsCfg != nil)
	close(r.Ready)

	g.Go(func() error { return r.heartbeats(ctx) })
	g.Go(func() error {
		return r.keys.Poll(ctx, r.client, func(err error) { r.log.Warn("keys", "err", err.Error()) })
	})

	return g.Wait()
}

func (r *Relay) tlsConfig() (*tls.Config, error) {
	if r.cfg.Dev {
		return nil, nil
	}
	cert, err := r.agent.Certificate()
	if err != nil {
		return nil, err
	}
	if cert == nil {
		return nil, errors.New("no host certificate")
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c, err := r.agent.Certificate()
			if err != nil || c == nil {
				return cert, nil
			}

			return c, nil
		},
	}, nil
}

// ---- the CP connection --------------------------------------------------

func (r *Relay) client(ctx context.Context) (*grpc.ClientConn, error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.conn != nil {
		return r.conn, nil
	}
	c, err := r.agent.Dial(ctx)
	if err != nil {
		return nil, err
	}
	r.conn = c

	return c, nil
}

func (r *Relay) resetClient() {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

// advertised is an address as the CP should hand it out: the override, or
// the bound address.
func (r *Relay) advertised(bound string) string {
	if r.cfg.Advertise != "" {
		_, port, err := net.SplitHostPort(bound)
		if err == nil {
			if h, _, err := net.SplitHostPort(r.cfg.Advertise); err == nil && h != "" {
				return net.JoinHostPort(h, port)
			}

			return net.JoinHostPort(r.cfg.Advertise, port)
		}
	}

	return bound
}

func (r *Relay) joinCall(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (hostagent.Answer, error) {
	resp, err := api.NewRelayServiceClient(conn).Join(ctx, api.RelayJoinRequest_builder{
		Host:          hj,
		Interfaces:    hostagent.Interfaces(),
		IngestAddress: r.advertised(r.IngestAddr),
		WhepAddress:   r.advertised(r.WhepAddr),
	}.Build())
	if err != nil {
		return hostagent.Answer{}, err
	}

	return hostagent.Answer{JoinAnswer: resp.GetAnswer(), Keys: resp.GetKeys()}, nil
}

func (r *Relay) heartbeats(ctx context.Context) error {
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if err := r.heartbeat(ctx); err != nil {
			r.log.Warn("heartbeat", "err", err.Error())
			r.resetClient()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (r *Relay) heartbeat(ctx context.Context) error {
	conn, err := r.client(ctx)
	if err != nil {
		return err
	}
	leaf, _ := r.agent.Leaf()
	serial := ""
	if leaf != nil {
		serial = pki.Serial(leaf)
	}
	st := r.sources.status()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := api.NewRelayServiceClient(conn).Heartbeat(cctx, api.RelayHeartbeatRequest_builder{
		CertSerial:    serial,
		CaHash:        r.agent.BundleHash(),
		KeyIds:        r.keys.Kids(),
		Status:        st,
		Interfaces:    hostagent.Interfaces(),
		IngestAddress: r.advertised(r.IngestAddr),
		WhepAddress:   r.advertised(r.WhepAddr),
		Version:       hostagent.Version,
	}.Build())
	if err != nil {
		return err
	}
	if resp.GetRenewCertificate() || r.agent.NeedsRenewal(time.Now()) {
		if err := r.agent.Renew(ctx, conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
			v, err := api.NewRelayServiceClient(conn).RenewCertificate(ctx, api.RelayRenewCertificateRequest_builder{Csr: csr}.Build())
			if err != nil {
				return nil, nil, err
			}

			return v.GetCertificate(), v.GetCaBundle(), nil
		}); err != nil {
			r.log.Warn("renew certificate", "err", err.Error())
		} else {
			r.log.Info("certificate renewed")
			r.resetClient()
		}
	}

	return nil
}

// verify checks a token for this relay and one operation.
func (r *Relay) verify(tok string, op api.TokenOp) (*api.TokenClaims, error) {
	c, err := r.keys.Verifier.Verify(tok)
	if err != nil {
		return nil, err
	}
	if err := token.Check(c, r.id.Bytes(), op); err != nil {
		return nil, err
	}

	return c, nil
}

func errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }
