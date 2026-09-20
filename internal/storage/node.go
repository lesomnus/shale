package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
)

// DomNode is the domain byte of a Node (§35.3).
const DomNode pdid.Domain = 12

// Config is a Storage Node's own settings (§36.1, node scope).
type Config struct {
	StateDir string
	Cp       string
	CaHash   string
	Dev      bool
	// HardwareId overrides the machine's identity, for a second node in
	// one process.
	HardwareId string

	// Addr is the data plane listener; ControlAddr the control API (§35.7).
	Addr        string
	ControlAddr string
	// Advertise overrides the data address reported to the CP.
	Advertise string

	Sinks  []SinkConfig
	Limits Limits
	Marks  Watermarks

	HeartbeatInterval time.Duration
	EventReplayWindow time.Duration
	SweepInterval     time.Duration
	// GcInterval is how often a sink under pressure gets a round (§21).
	GcInterval       time.Duration
	TokenSkew        time.Duration
	GcPage           int
	GcProposalFactor float64
	// Quanta and Caps are the Device Queue's weights and backlog caps (§24).
	Quanta Quanta
	Caps   Caps

	Log *slog.Logger
}

func (c *Config) defaults() {
	if c.Addr == "" {
		c.Addr = ":7410"
	}
	if c.ControlAddr == "" {
		c.ControlAddr = ":7411"
	}
	if c.Limits.MaxUploads == 0 {
		c.Limits = DefaultLimits
	}
	if c.Marks.Low == 0 {
		c.Marks = DefaultWatermarks
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.EventReplayWindow == 0 {
		c.EventReplayWindow = 10 * time.Minute
	}
	if c.SweepInterval == 0 {
		c.SweepInterval = 24 * time.Hour
	}
	if c.TokenSkew == 0 {
		c.TokenSkew = time.Minute
	}
	if c.GcPage == 0 {
		c.GcPage = 5000
	}
	if c.GcProposalFactor == 0 {
		c.GcProposalFactor = 3
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.GcInterval == 0 {
		c.GcInterval = time.Minute
	}
}

// Node is one Storage Node process.
type Node struct {
	cfg   Config
	log   *slog.Logger
	agent *hostagent.Agent
	id    pdid.Id

	sinks []*Sink
	byId  map[pdid.Id]*Sink

	verifier *token.Verifier
	keys     *hostagent.KeyRing
	m        *metrics
	// queues is one Device Queue per device, shared by its sinks (§24).
	queues map[string]*Queue
	outbox *Outbox
	dp     *DataPlane

	connMu sync.Mutex
	conn   *grpc.ClientConn

	// Ready is closed once the node serves.
	Ready chan struct{}
	// DataAddr is where the data plane listens, once it does.
	DataAddr string
}

// New prepares a node; Run does the work.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	n := &Node{cfg: cfg, log: cfg.Log, byId: map[pdid.Id]*Sink{}, keys: hostagent.NewKeyRing(cfg.StateDir), Ready: make(chan struct{})}
	n.verifier = n.keys.Verifier
	n.verifier.Skew = cfg.TokenSkew
	n.m = newMetrics(context.Background())
	n.outbox = NewOutbox()
	n.dp = newDataPlane(n, cfg.Limits)
	n.agent = &hostagent.Agent{Kind: DomNode, Store: pki.Store{Dir: cfg.StateDir}, Cp: cfg.Cp, CaHash: cfg.CaHash, Dev: cfg.Dev, HardwareId: cfg.HardwareId, Log: cfg.Log}

	return n, nil
}

// Id is the node's row, Nil until adopted.
func (n *Node) Id() pdid.Id { return n.id }

// Verifier is the node's view of the key set.
func (n *Node) Verifier() *token.Verifier { return n.verifier }

func (n *Node) sinkOf(id pdid.Id) *Sink { return n.byId[id] }

// Run opens the sinks, joins, and serves until the context is done.
func (n *Node) Run(ctx context.Context) error {
	if err := os.MkdirAll(n.cfg.StateDir, 0o700); err != nil {
		return err
	}
	n.m = newMetrics(ctx)
	for _, sc := range n.cfg.Sinks {
		s, err := OpenSink(sc, n.cfg.Marks)
		if err != nil {
			return err
		}
		if _, dup := n.byId[s.Id]; dup {
			return fmt.Errorf("sink %s is listed twice", s.Path)
		}
		n.sinks = append(n.sinks, s)
		n.byId[s.Id] = s
		n.log.Info("sink", "id", s.Id.String(), "path", s.Path, "device", s.DeviceId, "fs", s.Caps.GetFilesystem(),
			"odirect", s.Caps.GetOdirect(), "fallocate", s.Caps.GetFallocate())
	}
	if len(n.sinks) == 0 {
		return errors.New("no sinks configured")
	}
	// Several sinks on one device: warned about in the heartbeat (§22.2).
	byDev := map[string]int{}
	for _, s := range n.sinks {
		byDev[s.DeviceId]++
	}
	for _, s := range n.sinks {
		if byDev[s.DeviceId] > 1 && !n.cfg.Dev {
			s.Warn("several sinks share device " + s.DeviceId)
		}
	}

	if err := n.agent.Init(); err != nil {
		return err
	}
	n.keys.Load()

	// One Device Queue per device (§24), then the startup scan as MAINT
	// work on it; the node serves during the scan (§29).
	g, ctx := errgroup.WithContext(ctx)
	n.queues = map[string]*Queue{}
	for _, s := range n.sinks {
		if _, ok := n.queues[s.DeviceId]; !ok {
			q := NewQueue(n.cfg.Quanta, n.cfg.Caps)
			n.queues[s.DeviceId] = q
			g.Go(func() error { q.Run(ctx); return nil })
		}
	}
	for _, s := range n.sinks {
		s := s
		g.Go(func() error { n.scan(ctx, s); return nil })
	}

	// The listeners first, so the join reports the ports actually bound.
	dl, err := net.Listen("tcp", n.cfg.Addr)
	if err != nil {
		return err
	}
	n.DataAddr = dl.Addr().String()
	cl, err := net.Listen("tcp", n.cfg.ControlAddr)
	if err != nil {
		return err
	}
	n.cfg.ControlAddr = cl.Addr().String()

	// Join unless the certificate says who we are (§33.4).
	if !n.agent.Ready(time.Now()) {
		ans, err := n.agent.Join(ctx, n.joinCall)
		if err != nil {
			return err
		}
		n.keys.Apply(ans.Keys)
	}
	n.id = n.agent.Id()
	n.log.Info("node", "id", n.id.String())

	tlsCfg, err := n.tlsConfig()
	if err != nil {
		return err
	}

	httpSrv := &http.Server{Handler: n.dp, ReadHeaderTimeout: 30 * time.Second, ErrorLog: log.New(httpNoise{n.log}, "", 0)}
	g.Go(func() error {
		var err error
		if tlsCfg != nil {
			httpSrv.TLSConfig = tlsCfg
			err = httpSrv.ServeTLS(dl, "", "")
		} else {
			err = httpSrv.Serve(dl)
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

		return httpSrv.Shutdown(shut)
	})

	gs := n.controlServer(tlsCfg)
	g.Go(func() error { return gs.Serve(cl) })
	g.Go(func() error {
		<-ctx.Done()
		gs.GracefulStop()

		return nil
	})

	n.log.Info("serving", "data", dl.Addr().String(), "control", cl.Addr().String(), "tls", tlsCfg != nil)
	close(n.Ready)

	g.Go(func() error { return n.heartbeats(ctx) })
	g.Go(func() error { return n.outbox.Drain(ctx, n.pushEvents, n.log) })
	g.Go(func() error { return n.keyPoll(ctx) })
	g.Go(func() error { return n.sweeps(ctx) })
	g.Go(func() error { return n.gcLoop(ctx) })

	return g.Wait()
}

// httpNoise is where net/http's own log goes: a TCP probe that connects
// and hangs up is a "TLS handshake error ... EOF" every few seconds, which
// is not news; the rest is a warning.
type httpNoise struct{ log *slog.Logger }

func (w httpNoise) Write(p []byte) (int, error) {
	s := strings.TrimSpace(string(p))
	if strings.Contains(s, "TLS handshake error") && strings.HasSuffix(s, "EOF") {
		return len(p), nil
	}
	w.log.Warn("http", "msg", s)

	return len(p), nil
}

// tlsConfig is the node's server TLS for both listeners: its host
// certificate; the control API also requires a client certificate that
// chains to the CA (§33.5). Nil is plaintext, development only.
func (n *Node) tlsConfig() (*tls.Config, error) {
	if n.cfg.Dev {
		// Development mode is plaintext everywhere (§33.5).
		return nil, nil
	}
	cert, err := n.agent.Certificate()
	if err != nil {
		return nil, err
	}
	if cert == nil {
		if n.cfg.Dev {
			return nil, nil
		}

		return nil, errors.New("no host certificate")
	}
	bundle, err := n.agent.Store.Bundle()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			// Re-read so a renewed certificate is served without a restart.
			c, err := n.agent.Certificate()
			if err != nil || c == nil {
				return cert, nil
			}

			return c, nil
		},
	}
	if len(bundle) > 0 {
		pool, err := pki.Pool(bundle)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}

	return cfg, nil
}

// controlServer is the NodeControl gRPC service: over mTLS it accepts one
// peer, the Control Plane (§35.7).
func (n *Node) controlServer(tlsCfg *tls.Config) *grpc.Server {
	var opts []grpc.ServerOption
	if tlsCfg != nil {
		c := tlsCfg.Clone()
		c.ClientAuth = tls.RequireAndVerifyClientCert
		c.NextProtos = []string{"h2"}
		opts = append(opts, grpc.Creds(credentials.NewTLS(c)))
		opts = append(opts, grpc.ChainUnaryInterceptor(cpOnlyUnary), grpc.ChainStreamInterceptor(cpOnlyStream))
	}
	gs := grpc.NewServer(opts...)
	api.RegisterNodeControlServer(gs, &control{n: n})

	return gs
}

func cpOnly(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "no client certificate")
	}
	if !pki.IsCp(info.State.VerifiedChains[0][0]) {
		return status.Error(codes.PermissionDenied, "the control API serves the control plane only")
	}

	return nil
}

func cpOnlyUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := cpOnly(ctx); err != nil {
		return nil, err
	}

	return h(ctx, req)
}

func cpOnlyStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := cpOnly(ss.Context()); err != nil {
		return err
	}

	return h(srv, ss)
}

// ---- the CP connection --------------------------------------------------

func (n *Node) client(ctx context.Context) (*grpc.ClientConn, error) {
	n.connMu.Lock()
	defer n.connMu.Unlock()
	if n.conn != nil {
		return n.conn, nil
	}
	c, err := n.agent.Dial(ctx)
	if err != nil {
		return nil, err
	}
	n.conn = c

	return c, nil
}

func (n *Node) resetClient() {
	n.connMu.Lock()
	defer n.connMu.Unlock()
	if n.conn != nil {
		n.conn.Close()
		n.conn = nil
	}
}

func (n *Node) dataAddress() string {
	if n.cfg.Advertise != "" {
		return n.cfg.Advertise
	}
	if n.DataAddr != "" {
		return n.DataAddr
	}

	return n.cfg.Addr
}

func (n *Node) sinkReports() []*api.SinkReport {
	var vs []*api.SinkReport
	for _, s := range n.sinks {
		vs = append(vs, s.Report())
	}

	return vs
}

func (n *Node) deviceReports() []*api.DeviceReport {
	seen := map[string]bool{}
	var vs []*api.DeviceReport
	for _, s := range n.sinks {
		if seen[s.DeviceId] {
			continue
		}
		seen[s.DeviceId] = true
		capacity, _, _ := s.Statfs()
		if s.Capacity > 0 {
			capacity = s.Capacity
		}
		dr := api.DeviceReport_builder{HardwareId: s.DeviceId, Capacity: capacity}
		if q := n.queues[s.DeviceId]; q != nil {
			dr.QueueWrite, dr.QueueRead, dr.QueueMaint = int32(q.Depth(ClassWrite)), int32(q.Depth(ClassRead)), int32(q.Depth(ClassMaint))
		}
		vs = append(vs, dr.Build())
	}

	return vs
}

// queue is the Device Queue of a sink's device; before Run, or for a sink
// of no known device, the job runs at once.
func (n *Node) queue(s *Sink) *Queue {
	if q := n.queues[s.DeviceId]; q != nil {
		return q
	}

	return nil
}

// onDevice runs fn as a job of the class on the sink's device (§24), or
// inline when the node has no queue for it yet.
func (n *Node) onDevice(ctx context.Context, s *Sink, class Class, cost int64, fn func() error) error {
	q := n.queue(s)
	if q == nil {
		return fn()
	}

	return q.Submit(ctx, class, cost, fn)
}

func (n *Node) joinCall(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (hostagent.Answer, error) {
	resp, err := api.NewNodeServiceClient(conn).Join(ctx, api.NodeJoinRequest_builder{
		Host:           hj,
		Interfaces:     hostagent.Interfaces(),
		Sinks:          n.sinkReports(),
		Devices:        n.deviceReports(),
		ControlAddress: n.cfg.ControlAddr,
		DataAddress:    n.dataAddress(),
	}.Build())
	if err != nil {
		return hostagent.Answer{}, err
	}

	return hostagent.Answer{JoinAnswer: resp.GetAnswer(), Keys: resp.GetKeys()}, nil
}

// heartbeats reports every heartbeat_interval and applies what the CP
// answers about the sinks (§27, §28.3).
func (n *Node) heartbeats(ctx context.Context) error {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if err := n.heartbeat(ctx); err != nil {
			n.log.Warn("heartbeat", "err", err.Error())
			n.resetClient()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (n *Node) heartbeat(ctx context.Context) error {
	conn, err := n.client(ctx)
	if err != nil {
		return err
	}
	leaf, _ := n.agent.Leaf()
	serial := ""
	if leaf != nil {
		serial = pki.Serial(leaf)
	}
	var uploads int64
	var objects int64
	for _, s := range n.sinks {
		uploads += s.uploads.Load()
		objects += int64(s.Index.Len())
		rep := s.Report()
		n.m.free.Record(ctx, rep.GetFree(), sinkAttr(s))
		n.m.pressure.Record(ctx, int64(rep.GetPressure()), sinkAttr(s))
		n.m.indexed.Record(ctx, int64(s.Index.Len()), sinkAttr(s))
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := api.NewNodeServiceClient(conn).Heartbeat(cctx, api.NodeHeartbeatRequest_builder{
		CertSerial:      serial,
		CaHash:          n.agent.BundleHash(),
		KeyIds:          n.verifier.Kids(),
		Devices:         n.deviceReports(),
		Sinks:           n.sinkReports(),
		Interfaces:      hostagent.Interfaces(),
		ControlAddress:  n.cfg.ControlAddr,
		DataAddress:     n.dataAddress(),
		Version:         hostagent.Version,
		UploadsInFlight: uploads,
		IndexObjects:    objects,
	}.Build())
	if err != nil {
		return err
	}
	for _, a := range resp.GetSinks() {
		id, err := pdid.From(a.GetSinkId())
		if err != nil {
			continue
		}
		if s := n.byId[id]; s != nil {
			if s.Serves() != a.GetServe() {
				n.log.Info("sink attachment", "sink", id.String(), "serve", a.GetServe(), "attachment", a.GetAttachment().String())
			}
			s.SetServe(a.GetServe())
			s.SetAccept(a.GetAcceptWrites())
		}
	}
	if resp.GetRenewCertificate() || n.agent.NeedsRenewal(time.Now()) {
		if err := n.agent.Renew(ctx, conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
			r, err := api.NewNodeServiceClient(conn).RenewCertificate(ctx, api.NodeRenewCertificateRequest_builder{Csr: csr}.Build())
			if err != nil {
				return nil, nil, err
			}

			return r.GetCertificate(), r.GetCaBundle(), nil
		}); err != nil {
			n.log.Warn("renew certificate", "err", err.Error())
		} else {
			n.log.Info("certificate renewed")
			n.resetClient()
		}
	}

	return nil
}

func (n *Node) pushEvents(ctx context.Context, evs []*api.Event) error {
	conn, err := n.client(ctx)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := api.NewNodeServiceClient(conn).PushEvents(cctx, api.NodePushEventsRequest_builder{Events: evs}.Build())
	if err != nil {
		n.resetClient()
		return err
	}
	if int(resp.GetApplied()) < len(evs) {
		n.log.Warn("events refused", "sent", len(evs), "applied", resp.GetApplied())
	}

	return nil
}

// ---- the key set ---------------------------------------------------------

// keyPoll keeps the key set current (§33.3).
func (n *Node) keyPoll(ctx context.Context) error {
	return n.keys.Poll(ctx, n.client, func(err error) { n.log.Warn("keys", "err", err.Error()) })
}

// ---- scans and sweeps ----------------------------------------------------

func (n *Node) scan(ctx context.Context, s *Sink) {
	start := time.Now()
	q := n.queue(s)
	res, err := s.Index.Scan(ctx, s, func(k int) { n.log.Info("scan", "sink", s.Id.String(), "files", k) }, func() error {
		return q.Submit(ctx, ClassMaint, 256*Align, func() error { return nil })
	})
	if err != nil {
		n.log.Warn("scan", "sink", s.Id.String(), "err", err.Error())
		return
	}
	n.log.Info("scan done", "sink", s.Id.String(), "complete", res.Complete, "open", len(res.Open), "damaged", len(res.Damaged), "took", time.Since(start).String())

	// Damaged files are removed and reported (§12.3).
	for _, key := range res.Damaged {
		path, _ := s.FilePath(key)
		rec, _ := ReadRecordPath(path)
		os.Remove(path)
		n.outbox.Push(api.Event_builder{Missing: api.ObjectMissing_builder{
			SinkId: s.Id.Bytes(), ObjectKey: key, ObjectId: rec.GetObjectId(), AttemptId: rec.GetAttemptId(),
			Reason: api.MissingReason_MISSING_REASON_DAMAGED, DateObserved: timestamppb.Now(),
		}.Build()}.Build())
	}

	// Open uploads follow the abandon rule from their own record (§12.2).
	n.dp.adoptOpen(s, res.Open)

	// Commits whose events a crash may have lost are replayed (§12.4).
	cutoff := time.Now().Add(-n.cfg.EventReplayWindow)
	for _, e := range s.Index.Snapshot() {
		if e.Committed.After(cutoff) {
			n.outbox.Push(api.Event_builder{Stored: storedEvent(s, e, e.Committed)}.Build())
		}
	}
}

func (n *Node) sweeps(ctx context.Context) error {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		n.dp.sweepAbandoned(ctx)
	}
}

// Outbox is the in-RAM queue of events waiting for the CP (§34.9).
type Outbox struct {
	mu   sync.Mutex
	q    []*api.Event
	wake chan struct{}
}

func NewOutbox() *Outbox {
	return &Outbox{wake: make(chan struct{}, 1)}
}

// Push queues an event.
func (o *Outbox) Push(ev *api.Event) {
	o.mu.Lock()
	o.q = append(o.q, ev)
	o.mu.Unlock()
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// Len is how many events wait.
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return len(o.q)
}

// Drain sends batches with backoff until the context is done.
func (o *Outbox) Drain(ctx context.Context, push func(ctx context.Context, evs []*api.Event) error, log *slog.Logger) error {
	backoff := time.Second
	for {
		o.mu.Lock()
		batch := o.q
		if len(batch) > 500 {
			batch = batch[:500]
		}
		o.mu.Unlock()

		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-o.wake:
			}
			continue
		}

		if err := push(ctx, batch); err != nil {
			log.Warn("push events", "n", len(batch), "err", err.Error(), "retry_in", backoff.String())
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		o.mu.Lock()
		o.q = o.q[len(batch):]
		o.mu.Unlock()
	}
}

// ---- the control API -----------------------------------------------------

type control struct {
	api.UnimplementedNodeControlServer
	n *Node
}

func (c *control) sink(id []byte) (*Sink, error) {
	sid, err := pdid.From(id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "sink_id")
	}
	s := c.n.byId[sid]
	if s == nil {
		return nil, status.Errorf(codes.NotFound, "sink %s is not on this node", sid)
	}

	return s, nil
}

// Delete unlinks keys now (§20.3, §35.7).
func (c *control) Delete(ctx context.Context, req *api.NodeDeleteRequest) (*api.NodeDeleteResponse, error) {
	s, err := c.sink(req.GetSinkId())
	if err != nil {
		return nil, err
	}
	var out []*api.NodeDeleteResponse_Key
	for _, key := range req.GetKeys() {
		res := c.n.unlink(s, key)
		out = append(out, api.NodeDeleteResponse_Key_builder{Key: key, Result: res}.Build())
	}

	return api.NodeDeleteResponse_builder{Keys: out}.Build(), nil
}

// unlink removes a file and reports it (§21.2).
func (n *Node) unlink(s *Sink, key string) api.DeleteResult {
	path, err := s.FilePath(key)
	if err != nil {
		return api.DeleteResult_DELETE_RESULT_FAILED
	}
	e, _ := s.Index.Get(key)
	var rec *api.ObjectRecord
	var size int64
	if e != nil {
		rec, size = e.Record, e.Size
	} else if st, err := os.Stat(path); err == nil {
		rec, _ = ReadRecordPath(path)
		size = st.Size()
	}
	// The unlink is MAINT work on the device (§21.2, §24).
	err = n.onDevice(context.Background(), s, ClassMaint, Align, func() error { return os.Remove(path) })
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.Index.Remove(key)
			return api.DeleteResult_DELETE_RESULT_ABSENT
		}
		n.log.Warn("unlink", "key", key, "err", err.Error())
		return api.DeleteResult_DELETE_RESULT_FAILED
	}
	s.Index.Remove(key)
	n.outbox.Push(api.Event_builder{Deleted: api.ObjectDeleted_builder{
		SinkId: s.Id.Bytes(), ObjectKey: key, ObjectId: rec.GetObjectId(), AttemptId: rec.GetAttemptId(),
		Size: size, DateDeleted: timestamppb.Now(),
	}.Build()}.Build())

	return api.DeleteResult_DELETE_RESULT_DELETED
}

// SetDates rewrites the dates in the xattrs (§20.3).
func (c *control) SetDates(ctx context.Context, req *api.NodeSetDatesRequest) (*api.NodeSetDatesResponse, error) {
	s, err := c.sink(req.GetSinkId())
	if err != nil {
		return nil, err
	}
	var absent []string
	for _, d := range req.GetDates() {
		if err := c.n.setDates(s, d.GetKey(), d.GetDateExpired(), d.GetDateDeleted()); err != nil {
			absent = append(absent, d.GetKey())
		}
	}

	return api.NodeSetDatesResponse_builder{Absent: absent}.Build(), nil
}

func (n *Node) setDates(s *Sink, key string, expired, deleted *timestamppb.Timestamp) error {
	path, err := s.FilePath(key)
	if err != nil {
		return err
	}
	rec, err := ReadRecordPath(path)
	if err != nil {
		return err
	}
	if expired != nil {
		rec.SetDateExpiredMs(expired.AsTime().UnixMilli())
	}
	if deleted != nil {
		rec.SetDateDeletedMs(deleted.AsTime().UnixMilli())
	} else {
		rec.SetDateDeletedMs(0)
	}
	if err := WriteRecordPath(path, rec); err != nil {
		return err
	}
	if e, ok := s.Index.Get(key); ok {
		s.Index.Add(EntryOf(key, rec, e.Size, e.Committed))
	}

	return nil
}

func (c *control) SetSinkState(ctx context.Context, req *api.NodeSetSinkStateRequest) (*api.NodeSetSinkStateResponse, error) {
	s, err := c.sink(req.GetSinkId())
	if err != nil {
		return nil, err
	}
	s.SetAccept(req.GetAcceptWrites())
	c.n.log.Info("sink state", "sink", s.Id.String(), "accept_writes", req.GetAcceptWrites())

	return &api.NodeSetSinkStateResponse{}, nil
}

func (c *control) Locate(ctx context.Context, req *api.NodeLocateRequest) (*api.NodeLocateResponse, error) {
	return api.NodeLocateResponse_builder{Supported: false}.Build(), nil
}

func (c *control) Gc(ctx context.Context, req *api.NodeGcRequest) (*api.NodeGcResponse, error) {
	s, err := c.sink(req.GetSinkId())
	if err != nil {
		return nil, err
	}
	st, err := c.n.gcRound(ctx, s, api.GcReason_GC_REASON_PRESSURE, true)
	if err != nil {
		return nil, err
	}

	return api.NodeGcResponse_builder{Proposed: st.proposed, Deleted: st.deleted, ReclaimedBytes: st.reclaimed}.Build(), nil
}

// Reconcile streams every complete file newer than `since` and says which
// of the DELETING keys are absent (§34.9).
func (c *control) Reconcile(req *api.NodeReconcileRequest, stream grpc.ServerStreamingServer[api.NodeReconcileResponse]) error {
	s, err := c.sink(req.GetSinkId())
	if err != nil {
		return err
	}
	var since time.Time
	if req.GetSince() != nil {
		since = req.GetSince().AsTime()
	}
	entries := s.Index.Snapshot()
	var total int64
	for _, e := range entries {
		if !e.Committed.After(since) {
			continue
		}
		total++
	}
	if err := stream.Send(api.NodeReconcileResponse_builder{Total: &total}.Build()); err != nil {
		return err
	}
	for _, e := range entries {
		if !e.Committed.After(since) {
			continue
		}
		if err := stream.Send(api.NodeReconcileResponse_builder{Record: storedEvent(s, e, e.Committed)}.Build()); err != nil {
			return err
		}
	}
	for _, key := range req.GetDeleting() {
		path, err := s.FilePath(key)
		if err != nil {
			continue
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			k := key
			if err := stream.Send(api.NodeReconcileResponse_builder{Absent: &k}.Build()); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *control) InstallCertificate(ctx context.Context, req *api.NodeInstallCertificateRequest) (*api.NodeInstallCertificateResponse, error) {
	if err := c.n.agent.Store.SetCert(req.GetCertificate(), req.GetCaBundle()); err != nil {
		return nil, err
	}
	certs, err := pki.ParseCerts(req.GetCertificate())
	if err != nil || len(certs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "certificate")
	}
	c.n.resetClient()

	return api.NodeInstallCertificateResponse_builder{CertSerial: pki.Serial(certs[0])}.Build(), nil
}

var _ = x509.ParseCertificate
