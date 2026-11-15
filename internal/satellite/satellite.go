// Package satellite is the host producers upload through (§41): it speaks
// a Storage Node's upload protocol to the producers the CP assigned to it,
// and forwards each request to the node its put token names, at the
// address the CP gives for that node. It holds no lamina and is no
// placement target; under `retain: committed` and `written` it moves
// bytes and nothing else, so a satellite that dies loses nothing a
// producer does not still hold. Under `retain: accepted` it takes a
// segment's bytes into its RAM and delivers them itself (§41.9): what it
// accepted and did not deliver is lost with it.
package satellite

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
)

// The domains a satellite reads off identifiers (§35.3).
const (
	DomSatellite pdid.Domain = 43
	DomProducer  pdid.Domain = 22
	DomNode      pdid.Domain = 12
)

// Config is a satellite's own settings (§36.1, satellite scope).
type Config struct {
	StateDir   string
	Cp         string
	CaHash     string
	Dev        bool
	HardwareId string

	// Addr is the upload listener producers dial; Advertise overrides the
	// address reported for it.
	Addr      string
	Advertise string
	// MemoryBudget is the one memory budget (§41.4): the retained pool of
	// `retain: accepted` takes RetainedBudget of it, and the in-flight
	// pool the rest. RequestBuffer is the buffer one request's body is
	// copied through; a request reserves twice it.
	MemoryBudget  int64
	RequestBuffer int
	// RefuseAccepted passes every request under `retain: accepted`
	// through, as under `committed` (`satellite.accept: false`, §41.9);
	// the in-flight pool then has the whole budget.
	RefuseAccepted bool
	// RetainedBudget is the retained pool's part of MemoryBudget, half of
	// it by default; RetainedPerProducer caps one producer's share of it,
	// none but the equal share by default (§41.9).
	RetainedBudget      int64
	RetainedPerProducer int64

	HeartbeatInterval time.Duration
	TokenSkew         time.Duration
	Log               *slog.Logger
}

// The defaults of §36.1.
const (
	DefaultAddr          = ":7460"
	DefaultMemoryBudget  = 256 << 20
	DefaultRequestBuffer = 256 << 10
	// DefaultHeartbeatInterval is a third of the CP's satellite_down_after
	// (§41.2), as a relay's is of relay_down_after.
	DefaultHeartbeatInterval = 2 * time.Second
	// minRequestBuffer keeps a request's buffer at least a few frames of
	// HTTP/2.
	minRequestBuffer = 64 << 10
)

func (c *Config) defaults() {
	if c.Addr == "" {
		c.Addr = DefaultAddr
	}
	if c.RequestBuffer <= 0 {
		c.RequestBuffer = DefaultRequestBuffer
	}
	if c.RequestBuffer < minRequestBuffer {
		c.RequestBuffer = minRequestBuffer
	}
	if c.MemoryBudget <= 0 {
		c.MemoryBudget = DefaultMemoryBudget
	}
	if c.RefuseAccepted {
		c.RetainedBudget = 0
	} else if c.RetainedBudget <= 0 {
		c.RetainedBudget = c.MemoryBudget / 2
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.TokenSkew <= 0 {
		c.TokenSkew = time.Minute
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Satellite is one satellite process.
type Satellite struct {
	cfg   Config
	log   *slog.Logger
	agent *hostagent.Agent
	keys  *hostagent.KeyRing
	id    pdid.Id
	m     *metrics
	proxy *Proxy
	// incarnation is drawn when the process starts: the CP tells a
	// satellite that restarted, and lost what it held, by it (§41.9).
	incarnation string

	connMu sync.Mutex
	conn   *grpc.ClientConn
	// beat holds heartbeats to one at a time: the ticker's, and those a
	// caller it does not know asks for (§41.3).
	beat sync.Mutex
	// wake has the next heartbeat go now: a live feed wants a fresher
	// token (§41.8).
	wake chan struct{}
	// live is every producer's stream to its relay (§41.8).
	live *liveFeeds

	// Addr is the upload listener's address, once Run listens.
	Addr string
	// Ready is closed once the satellite serves.
	Ready chan struct{}
}

// New prepares a satellite; Run does the work.
func New(cfg Config) (*Satellite, error) {
	cfg.defaults()
	if cfg.MemoryBudget-cfg.RetainedBudget < 2*int64(cfg.RequestBuffer) {
		return nil, errors.New("satellite.memory_budget, less satellite.retained_budget, is less than one request's share, twice satellite.request_buffer")
	}
	if cfg.RetainedPerProducer < 0 {
		return nil, errors.New("satellite.retained_per_producer is negative")
	}
	var inc [8]byte
	rand.Read(inc[:])
	s := &Satellite{cfg: cfg, log: cfg.Log, keys: hostagent.NewKeyRing(cfg.StateDir), Ready: make(chan struct{}), incarnation: hex.EncodeToString(inc[:])}
	s.keys.Verifier.Skew = cfg.TokenSkew
	s.m = newMetrics(context.Background())
	s.agent = &hostagent.Agent{Kind: DomSatellite, Store: pki.Store{Dir: cfg.StateDir}, Cp: cfg.Cp, CaHash: cfg.CaHash, Dev: cfg.Dev, HardwareId: cfg.HardwareId, Log: cfg.Log}
	s.proxy = &Proxy{
		Verifier: s.keys.Verifier,
		Dev:      cfg.Dev,
		Log:      cfg.Log,
		assigned: &assigned{dir: cfg.StateDir},
		pool:     newPool(cfg.MemoryBudget-cfg.RetainedBudget, cfg.RequestBuffer),
		Refuse:   cfg.RefuseAccepted,
		Laminae:  api.NewLaminaServiceClient(cpConn{s}),
	}
	if !cfg.RefuseAccepted {
		s.proxy.retained = newRetained(cfg.RetainedBudget, cfg.RetainedPerProducer)
	}
	s.proxy.record = s.recordHolder
	s.proxy.assigned.refresh = s.heartbeat
	s.proxy.routes = newRoutes(s.route, cfg.Dev)
	s.wake = make(chan struct{}, 1)
	s.live = newLiveFeeds(cfg.Log)
	s.live.dial = func(ctx context.Context, addr string, plain bool) (*grpc.ClientConn, error) {
		return s.agent.DialAddr(ctx, addr, plain)
	}
	s.live.refresh = func() { signal(s.wake) }
	s.proxy.live = s.live

	return s, nil
}

// Id is the satellite's row, Nil until adopted.
func (s *Satellite) Id() pdid.Id { return s.id }

// InFlight is what the in-flight pool holds now, the most it held at once,
// and its bound (§41.4).
func (s *Satellite) InFlight() (used, peak, budget int64) {
	p := s.proxy.pool

	return p.used.Load(), p.peak.Load(), p.budget
}

// Forwarded is the bytes forwarded to nodes since the satellite started.
func (s *Satellite) Forwarded() int64 { return s.proxy.forwarded.Load() }

// Retained is what the retained pool holds now, the most it held at once,
// and its bound (§41.9).
func (s *Satellite) Retained() (used, peak, budget int64) { return s.proxy.retained.usage() }

// RetainedBy is what producer p holds of the retained pool.
func (s *Satellite) RetainedBy(p pdid.Id) int64 { return s.proxy.retained.holding(p) }

// Accepted is what the satellite did under `retain: accepted` since it
// started: bytes taken, segments delivered and given up, and requests
// passed through instead.
func (s *Satellite) Accepted() (bytes, delivered, givenUp, passed int64) {
	p := s.proxy

	return p.acceptedBytes.Load(), p.delivered.Load(), p.givenUp.Load(), p.passed.Load()
}

// Held is the laminae whose bytes the satellite holds now, undelivered.
func (s *Satellite) Held() [][]byte { return s.proxy.holding() }

// Live is the live fan-out (§41.8): the sources it feeds relays now, and
// the bytes it sent them and the fragments it dropped since it started.
func (s *Satellite) Live() (sources int, sent, dropped int64) {
	return s.live.feeding(), s.live.bytes.Load(), s.live.dropped.Load()
}

// Run joins, then serves uploads until the context is done.
func (s *Satellite) Run(ctx context.Context) error {
	if err := os.MkdirAll(s.cfg.StateDir, 0o700); err != nil {
		return err
	}
	s.m = newMetrics(ctx)
	s.proxy.ctx = ctx
	if err := s.agent.Init(); err != nil {
		return err
	}
	s.keys.Load()
	s.proxy.assigned.load()

	// The listener first, so the join reports the port actually bound.
	l, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.Addr = l.Addr().String()

	if !s.agent.Ready(time.Now()) {
		ans, err := s.agent.Join(ctx, s.joinCall)
		if err != nil {
			l.Close()
			return err
		}
		s.keys.Apply(ans.Keys)
	}
	s.id = s.agent.Id()
	s.log.Info("satellite", "id", s.id.String())

	hc, err := s.agent.HTTPClient()
	if err != nil {
		l.Close()
		return err
	}
	s.proxy.Client = hc
	tlsCfg, err := s.tlsConfig()
	if err != nil {
		l.Close()
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	srv := &http.Server{
		Handler:           s.proxy,
		ReadHeaderTimeout: 30 * time.Second,
		// What HTTP/2 lets a producer send ahead of a request, the second
		// half of what a request reserves (§41.4).
		HTTP2:    &http.HTTP2Config{MaxReceiveBufferPerStream: s.cfg.RequestBuffer},
		ErrorLog: log.New(httpNoise{s.log}, "", 0),
	}
	g.Go(func() error {
		var err error
		if tlsCfg != nil {
			srv.TLSConfig = tlsCfg
			err = srv.ServeTLS(l, "", "")
		} else {
			err = srv.Serve(l)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	})
	g.Go(func() error {
		<-ctx.Done()
		// Graceful for a moment, then not: a live upload is open for as
		// long as its segment, and a satellite that waited for every one
		// would not stop. The producer resumes from the node's offset
		// (§41.6).
		shut, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(shut); err != nil {
			srv.Close()
		}
		// What it accepted and did not deliver goes with the process: said
		// to the CP while it can be, within a few seconds (§41.9).
		stop, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel2()
		s.proxy.stop(stop)

		return nil
	})
	g.Go(func() error {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				s.proxy.sweep(ctx)
			}
		}
	})

	s.log.Info("serving", "upload", s.Addr, "tls", tlsCfg != nil, "memory_budget", s.cfg.MemoryBudget, "request_buffer", s.cfg.RequestBuffer, "accept", !s.cfg.RefuseAccepted, "retained_budget", s.cfg.RetainedBudget)
	close(s.Ready)

	s.live.start(ctx)
	g.Go(func() error { return s.heartbeats(ctx) })
	g.Go(func() error {
		return s.keys.Poll(ctx, s.client, func(err error) { s.log.Warn("keys", "err", err.Error()) })
	})

	return g.Wait()
}

// tlsConfig is the upload listener's TLS: the host certificate, which
// producers verify against the shale CA as they do a node's (§33.5), and
// a client certificate required, verified against the bundle held at each
// handshake, so a producer renewed from the next CA of a rollover is
// trusted without a restart. Nil in development mode.
func (s *Satellite) tlsConfig() (*tls.Config, error) {
	if s.cfg.Dev {
		return nil, nil
	}
	cert, err := s.agent.Certificate()
	if err != nil {
		return nil, err
	}
	if cert == nil {
		return nil, errors.New("no host certificate")
	}
	pool, err := s.agent.TrustPool()
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, errors.New("no CA bundle to verify producers against")
	}
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			// Re-read so a renewed certificate is served without a restart.
			c, err := s.agent.Certificate()
			if err != nil || c == nil {
				return cert, nil
			}

			return c, nil
		},
	}
	cfg := base.Clone()
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		pool, err := s.agent.TrustPool()
		if err != nil || pool == nil {
			return nil, nil
		}
		c := base.Clone()
		c.ClientCAs = pool

		return c, nil
	}

	return cfg, nil
}

// httpNoise is where net/http's own log goes: a probe that connects and
// hangs up is not news, the rest is a warning.
type httpNoise struct{ log *slog.Logger }

func (w httpNoise) Write(p []byte) (int, error) {
	v := strings.TrimSpace(string(p))
	if strings.Contains(v, "TLS handshake error") && strings.HasSuffix(v, "EOF") {
		return len(p), nil
	}
	w.log.Warn("http", "msg", v)

	return len(p), nil
}

// ---- the CP connection --------------------------------------------------

func (s *Satellite) client(ctx context.Context) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != nil {
		return s.conn, nil
	}
	c, err := s.agent.Dial(ctx)
	if err != nil {
		return nil, err
	}
	s.conn = c

	return c, nil
}

func (s *Satellite) resetClient() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// advertised is the upload address as the CP should hand it out: the
// override, with the bound port when it names none, or the bound address.
func (s *Satellite) advertised() string {
	bound := s.Addr
	if s.cfg.Advertise == "" {
		return bound
	}
	_, port, err := net.SplitHostPort(bound)
	if err != nil {
		return bound
	}
	if h, p, err := net.SplitHostPort(s.cfg.Advertise); err == nil && h != "" {
		if p != "" {
			return s.cfg.Advertise
		}

		return net.JoinHostPort(h, port)
	}

	return net.JoinHostPort(strings.Trim(s.cfg.Advertise, "[]"), port)
}

func (s *Satellite) joinCall(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (hostagent.Answer, error) {
	resp, err := api.NewSatelliteServiceClient(conn).Join(ctx, api.SatelliteJoinRequest_builder{
		Host:          hj,
		Interfaces:    hostagent.Interfaces(),
		UploadAddress: s.advertised(),
	}.Build())
	if err != nil {
		return hostagent.Answer{}, err
	}

	return hostagent.Answer{JoinAnswer: resp.GetAnswer(), Keys: resp.GetKeys()}, nil
}

func (s *Satellite) heartbeats(ctx context.Context) error {
	t := time.NewTicker(s.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if err := s.heartbeat(ctx); err != nil {
			s.log.Warn("heartbeat", "err", err.Error())
			s.resetClient()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-s.wake:
		}
	}
}

// heartbeat reports the satellite's memory and load, and takes the list of
// producers it serves from the answer (§41.2, §41.3).
func (s *Satellite) heartbeat(ctx context.Context) error {
	s.beat.Lock()
	defer s.beat.Unlock()
	conn, err := s.client(ctx)
	if err != nil {
		return err
	}
	leaf, _ := s.agent.Leaf()
	serial := ""
	if leaf != nil {
		serial = pki.Serial(leaf)
	}
	p := s.proxy
	used, _, budget := s.InFlight()
	retainedUsed, _, retainedBudget := s.Retained()
	acceptedBytes, delivered, givenUp, passed := s.Accepted()
	load := hostagent.Load()
	st := api.SatelliteStatus_builder{
		InFlightBytes:  used,
		InFlightBudget: budget,
		Uploads:        int32(p.pool.requests.Load()),
		Producers:      int32(p.assigned.len()),
		ForwardedBytes: p.forwarded.Load(),
		Refused:        p.refused.Load(),
		LiveSources:    int32(s.live.feeding()),
		LiveBytes:      s.live.bytes.Load(),
		LiveDropped:    s.live.dropped.Load(),
		Load:           api.HostLoad_builder{Cpu: load, Temperature: hostagent.Temperature()}.Build(),
		RetainedBytes:  retainedUsed,
		RetainedBudget: retainedBudget,
		Retained:       p.retained.shares(),
		Held:           p.holding(),
		Incarnation:    s.incarnation,
		AcceptedBytes:  acceptedBytes,
		Delivered:      delivered,
		GivenUp:        givenUp,
		Passed:         passed,
		Accept:         !s.cfg.RefuseAccepted,
	}.Build()
	s.m.inFlight.Record(ctx, used)
	s.m.budget.Record(ctx, budget)
	s.m.uploads.Record(ctx, int64(st.GetUploads()))
	s.m.producers.Record(ctx, int64(st.GetProducers()))
	s.m.forwarded.Record(ctx, st.GetForwardedBytes())
	s.m.refused.Record(ctx, st.GetRefused())
	s.m.cpu.Record(ctx, load)
	s.m.retained.Record(ctx, retainedUsed)
	s.m.retainedBudget.Record(ctx, retainedBudget)
	s.m.held.Record(ctx, int64(len(st.GetHeld())))
	s.m.acceptedBytes.Record(ctx, acceptedBytes)
	s.m.delivered.Record(ctx, delivered)
	s.m.givenUp.Record(ctx, givenUp)
	s.m.passed.Record(ctx, passed)
	s.m.liveSources.Record(ctx, int64(st.GetLiveSources()))
	s.m.liveBytes.Record(ctx, st.GetLiveBytes())
	s.m.liveDropped.Record(ctx, st.GetLiveDropped())

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := api.NewSatelliteServiceClient(conn).Heartbeat(cctx, api.SatelliteHeartbeatRequest_builder{
		CertSerial:    serial,
		CaHash:        s.agent.BundleHash(),
		KeyIds:        s.keys.Kids(),
		Status:        st,
		Interfaces:    hostagent.Interfaces(),
		UploadAddress: s.advertised(),
		Version:       hostagent.Version,
	}.Build())
	if err != nil {
		return err
	}
	p.assigned.set(resp.GetProducers())
	s.live.set(resp.GetProducers())
	if resp.GetRenewCertificate() || s.agent.NeedsRenewal(time.Now()) {
		if err := s.agent.Renew(ctx, conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
			v, err := api.NewSatelliteServiceClient(conn).RenewCertificate(ctx, api.SatelliteRenewCertificateRequest_builder{Csr: csr}.Build())
			if err != nil {
				return nil, nil, err
			}

			return v.GetCertificate(), v.GetCaBundle(), nil
		}); err != nil {
			s.log.Warn("renew certificate", "err", err.Error())
		} else {
			s.log.Info("certificate renewed")
			s.resetClient()
		}
	}

	return nil
}

// recordHolder has the CP record the satellite as a lamina's holder
// before it takes the lamina's bytes (§41.9).
func (s *Satellite) recordHolder(ctx context.Context, lamina pdid.Id) error {
	_, err := api.NewSatelliteServiceClient(cpConn{s}).Accept(ctx, api.SatelliteAcceptRequest_builder{LaminaId: lamina.Bytes()}.Build())

	return err
}

// cpConn calls the CP on the connection the satellite holds at each
// call, which a failed heartbeat or a renewal replaces. A call the
// replacement cut short is made once more on the new one.
type cpConn struct{ s *Satellite }

func (c cpConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	var err error
	for range 2 {
		var conn *grpc.ClientConn
		conn, err = c.s.client(ctx)
		if err != nil {
			return err
		}
		err = conn.Invoke(ctx, method, args, reply, opts...)
		if err == nil || ctx.Err() != nil || status.Code(err) != codes.Canceled {
			return err
		}
	}

	return err
}

func (c cpConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := c.s.client(ctx)
	if err != nil {
		return nil, err
	}

	return conn.NewStream(ctx, desc, method, opts...)
}

// route asks the CP where a node's data plane is for this satellite.
func (s *Satellite) route(ctx context.Context, node pdid.Id) (*api.SatelliteRouteResponse, error) {
	conn, err := s.client(ctx)
	if err != nil {
		return nil, err
	}

	return api.NewSatelliteServiceClient(conn).Route(ctx, api.SatelliteRouteRequest_builder{NodeId: node.Bytes()}.Build())
}
