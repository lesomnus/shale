package producer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// DomProducer is the domain byte of a Producer (§35.3).
const DomProducer pdid.Domain = 22

// Config is a producer's own settings (§36.1, producer scope).
type Config struct {
	StateDir string
	Cp       string
	CaHash   string
	Dev      bool
	Tenant   string

	Sources []SourceConfig
	Ffmpeg  string

	Mode              api.UploadMode
	Retain            string
	SegmentDuration   time.Duration
	IdleTimeout       time.Duration
	AbandonTimeout    time.Duration
	AllocationHorizon time.Duration
	Upload            UploadConfig
	// Uplink bounds the sum of the ceilings × 1.2 (§38.5); 0 is none.
	Uplink int64
	// Buffer is the RAM budget for segments not yet stored (§16).
	Buffer            int64
	HeartbeatInterval time.Duration

	Log *slog.Logger
}

func (c *Config) defaults() {
	if c.Ffmpeg == "" {
		c.Ffmpeg = "ffmpeg"
	}
	if c.Buffer == 0 {
		c.Buffer = 512 << 20
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 30 * time.Second
	}
	if c.Mode == api.UploadMode_UPLOAD_MODE_UNSPECIFIED {
		c.Mode = api.UploadMode_UPLOAD_MODE_LIVE
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// Producer is one producer process.
type Producer struct {
	cfg   Config
	log   *slog.Logger
	agent *hostagent.Agent
	conn  *grpc.ClientConn
	id    pdid.Id

	set     *api.Set
	members int
	sources map[string]*source
	order   []*source

	uploader *Uploader
	profileV int64
	relay    *relayLink
	m        *metrics

	mu       sync.Mutex
	retained int64

	// Ready is closed once the producer records.
	Ready chan struct{}
}

// source is one camera's state.
type source struct {
	cfg     SourceConfig
	row     *api.Source
	capture *Capture
	cutter  *Cutter

	mu      sync.Mutex
	profile *api.SegmentProfile
	sched   Schedule
	ceiling int64
	allocs  map[int64]*api.Allocation
	pending []*Segment
	wake    chan struct{}
	// lastObj is the object of the segment uploaded last: the next segment
	// never reuses it, however the slot's allocation was cached (§15).
	lastObj []byte

	// Per-second accounting for the heartbeat (§38.5, §38.6).
	secBytes   [60]int64
	secFrames  [60]int64
	secIdx     int
	lastTick   time.Time
	atCap      int64
	episodes   int64
	seconds    int64
	run        int64
	dropped    int64
	lost       int64
	stored     int64
	lastReport time.Time
	raise      bool
}

// New prepares a producer; Run does the work.
func New(cfg Config) (*Producer, error) {
	cfg.defaults()
	if len(cfg.Sources) == 0 {
		return nil, errors.New("no sources configured")
	}
	p := &Producer{cfg: cfg, log: cfg.Log, sources: map[string]*source{}, Ready: make(chan struct{})}
	p.relay = newRelayLink(p)
	p.m = newMetrics(context.Background())
	p.agent = &hostagent.Agent{Kind: DomProducer, Store: pki.Store{Dir: cfg.StateDir}, Cp: cfg.Cp, CaHash: cfg.CaHash, Dev: cfg.Dev, Log: cfg.Log}
	for _, sc := range cfg.Sources {
		if sc.Alias == "" {
			return nil, errors.New("a source needs an alias")
		}
		if _, dup := p.sources[sc.Alias]; dup {
			return nil, fmt.Errorf("source %s is listed twice", sc.Alias)
		}
		s := &source{cfg: sc, allocs: map[int64]*api.Allocation{}, wake: make(chan struct{}, 1)}
		p.sources[sc.Alias] = s
		p.order = append(p.order, s)
	}

	return p, nil
}

// Run joins, registers its sources, negotiates, and records until the
// context is done.
func (p *Producer) Run(ctx context.Context) error {
	if err := os.MkdirAll(p.cfg.StateDir, 0o700); err != nil {
		return err
	}
	p.m = newMetrics(ctx)
	if err := p.agent.Init(); err != nil {
		return err
	}

	if !p.agent.Ready(time.Now()) {
		if _, err := p.agent.Join(ctx, p.joinCall); err != nil {
			return err
		}
	}
	p.id = p.agent.Id()
	p.log.Info("producer", "id", p.id.String())

	conn, err := p.agent.Dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	p.conn = conn

	if err := p.waitForSet(ctx); err != nil {
		return err
	}
	if err := p.register(ctx); err != nil {
		return err
	}
	if err := p.negotiate(ctx); err != nil {
		return err
	}

	p.uploader = &Uploader{Cfg: p.cfg.Upload, Objects: api.NewObjectServiceClient(conn), Log: p.log, Mode: p.cfg.Mode}
	if p.uploader.Cfg.Client == nil {
		// The data planes speak TLS from the same CA the producer pinned.
		hc, err := p.agent.HTTPClient()
		if err != nil {
			return err
		}
		p.uploader.Cfg.Client = hc
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, s := range p.order {
		s := s
		g.Go(func() error { return p.capture(ctx, s) })
		g.Go(func() error { return p.uploads(ctx, s) })
	}
	g.Go(func() error { return p.allocations(ctx) })
	g.Go(func() error { return p.heartbeats(ctx) })
	g.Go(func() error { return p.ticks(ctx) })
	g.Go(func() error { return p.relay.run(ctx) })
	close(p.Ready)

	return g.Wait()
}

func (p *Producer) joinCall(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (hostagent.Answer, error) {
	resp, err := api.NewProducerServiceClient(conn).Join(ctx, api.ProducerJoinRequest_builder{Host: hj, Tenant: p.cfg.Tenant}.Build())
	if err != nil {
		return hostagent.Answer{}, err
	}

	return hostagent.Answer{JoinAnswer: resp.GetAnswer()}, nil
}

// waitForSet polls the producer's own row until an admin has adopted it
// for a set (§33.4).
func (p *Producer) waitForSet(ctx context.Context) error {
	said := false
	for {
		me, err := api.NewProducerServiceClient(p.conn).Get(ctx, api.ProducerGetRequest_builder{
			Ref: api.ProducerRef_builder{Id: p.id.Bytes()}.Build(),
		}.Build())
		if err == nil && len(me.GetSet().GetId()) > 0 {
			set, err := api.NewSetServiceClient(p.conn).Get(ctx, api.SetGetRequest_builder{
				Ref: api.SetRef_builder{Id: me.GetSet().GetId()}.Build(),
			}.Build())
			if err == nil {
				p.set = set
				p.log.Info("set", "alias", set.GetAlias())
				return nil
			}
		}
		if err != nil {
			p.log.Warn("producer row", "err", err.Error())
		} else if !said {
			p.log.Info("adopted for no set yet; waiting")
			said = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(hostagent.JoinInterval):
		}
	}
}

// register makes sure every configured source exists in the set (§38.4).
func (p *Producer) register(ctx context.Context) error {
	sources := api.NewSourceServiceClient(p.conn)
	byAlias := map[string]*api.Source{}
	after := ""
	for {
		vs, err := sources.List(ctx, api.SourceListRequest_builder{
			Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: p.set.GetId()}.Build()}.Build()},
			Size:    500, After: after,
		}.Build())
		if err != nil {
			return err
		}
		for _, v := range vs.GetItems() {
			byAlias[v.GetAlias()] = v
		}
		if vs.GetNext() == "" {
			break
		}
		after = vs.GetNext()
	}
	p.members = len(byAlias)

	for _, s := range p.order {
		if v, ok := byAlias[s.cfg.Alias]; ok {
			s.row = v
			continue
		}
		v, err := sources.Add(ctx, api.SourceAddRequest_builder{
			Tenant:  api.TenantRef_builder{Id: p.set.GetTenant().GetId()}.Build(),
			Alias:   s.cfg.Alias,
			Name:    s.cfg.Name,
			Set:     api.SetRef_builder{Id: p.set.GetId()}.Build(),
			Zone:    s.cfg.Zone,
			Profile: api.SegmentProfile_builder{MaxBitrate: p.startingCeiling(s)}.Build(),
		}.Build())
		if err != nil {
			return fmt.Errorf("register source %s: %w", s.cfg.Alias, err)
		}
		p.log.Info("registered source", "alias", v.GetAlias(), "ordinal", v.GetOrdinal())
		s.row = v
		p.members++
	}

	return nil
}

// startingCeiling is the declared ceiling or the table of §38.5.
func (p *Producer) startingCeiling(s *source) int64 {
	if s.cfg.MaxBitrate > 0 {
		return s.cfg.MaxBitrate
	}

	return StartingCeiling(s.cfg.Size, s.cfg.Fps, s.cfg.Format)
}

// negotiate proposes the profile and applies what was agreed (§12.6).
func (p *Producer) negotiate(ctx context.Context) error {
	var proposals []*api.SourceProposal
	var total int64
	for _, s := range p.order {
		ceiling := s.ceiling
		if ceiling == 0 {
			ceiling = p.startingCeiling(s)
		}
		if s.raise {
			ceiling = ceiling * 5 / 4
			s.raise = false
		}
		total += ceiling
		prof := api.SegmentProfile_builder{MaxBitrate: ceiling}
		if p.cfg.SegmentDuration > 0 {
			prof.DurationSeconds = int64(p.cfg.SegmentDuration.Seconds())
		}
		if s.cfg.KeyframeInterval > 0 {
			prof.KeyframeIntervalMs = s.cfg.KeyframeInterval.Milliseconds()
		}
		proposals = append(proposals, api.SourceProposal_builder{
			Source:  api.SourceRef_builder{Id: s.row.GetId()}.Build(),
			Profile: prof.Build(),
		}.Build())
	}
	if p.cfg.Uplink > 0 && float64(total)*1.2 > float64(p.cfg.Uplink) {
		p.log.Warn("the sum of the ceilings × 1.2 exceeds the uplink", "ceilings", total, "uplink", p.cfg.Uplink)
	}

	link := api.LinkProfile_builder{Mode: p.cfg.Mode}
	if p.cfg.IdleTimeout > 0 {
		link.IdleTimeoutSeconds = int64(p.cfg.IdleTimeout.Seconds())
	}
	if p.cfg.AbandonTimeout > 0 {
		link.AbandonTimeoutSeconds = int64(p.cfg.AbandonTimeout.Seconds())
	}
	if p.cfg.AllocationHorizon > 0 {
		link.AllocationHorizonSeconds = int64(p.cfg.AllocationHorizon.Seconds())
	}

	resp, err := api.NewSetServiceClient(p.conn).Negotiate(ctx, api.SetNegotiateRequest_builder{
		Ref: api.SetRef_builder{Id: p.set.GetId()}.Build(), Link: link.Build(), Sources: proposals,
	}.Build())
	if err != nil {
		return fmt.Errorf("negotiate: %w", err)
	}
	for _, a := range resp.GetAdjustments() {
		p.log.Info("adjusted", "source", mustId(a.GetSourceId()).String(), "field", a.GetField(), "proposed", a.GetProposed(), "agreed", a.GetAgreed(), "why", a.GetReason())
	}
	p.profileV = resp.GetProfileVersion()
	p.relay.set(resp.GetRelay())
	p.set.SetLink(resp.GetLink())
	p.cfg.Mode = resp.GetLink().GetMode()
	if p.uploader != nil {
		p.uploader.Mode = p.cfg.Mode
	}
	for _, ag := range resp.GetSources() {
		for _, s := range p.order {
			if string(s.row.GetId()) != string(ag.GetSourceId()) {
				continue
			}
			s.mu.Lock()
			s.profile = ag.GetProfile()
			s.ceiling = ag.GetProfile().GetMaxBitrate()
			d := time.Duration(ag.GetProfile().GetDurationSeconds()) * time.Second
			s.sched = Schedule{
				Duration: d,
				Phase:    core.Phase(p.set.GetId(), int(s.row.GetOrdinal()), p.members, d),
				Ceiling:  ag.GetProfile().GetMaxBitrate() / 8 * ag.GetProfile().GetDurationSeconds(),
			}
			s.mu.Unlock()
			p.log.Info("profile", "source", s.cfg.Alias, "max_bitrate", s.ceiling, "duration", d.String(), "keyframe_ms", ag.GetProfile().GetKeyframeIntervalMs(), "phase", s.sched.Phase.String())
		}
	}

	return nil
}

// capture runs the source's capture process and cuts its stream.
func (p *Producer) capture(ctx context.Context, s *source) error {
	s.capture = &Capture{
		Ffmpeg:   p.cfg.Ffmpeg,
		Source:   s.cfg,
		Log:      p.log,
		RawLoops: s.cfg.RawLoops,
		Profile: func() (int64, time.Duration) {
			s.mu.Lock()
			defer s.mu.Unlock()
			kf := time.Duration(s.profile.GetKeyframeIntervalMs()) * time.Millisecond
			return s.ceiling, kf
		},
	}

	return s.capture.Run(ctx, func(r io.Reader) {
		reader := NewReader(r)
		s.cutter = &Cutter{
			Reader: reader,
			Schedule: func() Schedule {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.sched
			},
			Open: func(seg *Segment) {
				if p.cfg.Mode == api.UploadMode_UPLOAD_MODE_LIVE {
					p.enqueue(s, seg)
				}
			},
			Out: func(seg *Segment) {
				if p.cfg.Mode != api.UploadMode_UPLOAD_MODE_LIVE {
					p.enqueue(s, seg)
				}
				if seg.Early {
					s.mu.Lock()
					s.raise = true
					s.mu.Unlock()
				}
			},
		}
		var pk Packet
		started := time.Now()
		checked, tables := false, false
		for {
			if err := reader.Next(&pk); err != nil {
				if !errors.Is(err, io.EOF) && ctx.Err() == nil {
					p.log.Warn("stream", "source", s.cfg.Alias, "err", err.Error())
				}
				break
			}
			if !tables && len(reader.Streams().PMT) > 0 {
				// The first PMT says what the capture produces; audio TS
				// cannot carry restarts it encoding (§38.3), before any
				// segment opened.
				tables = true
				if s.capture.CheckAudio(reader.Streams()) {
					break
				}
			}
			s.cutter.Feed(&pk)
			p.relay.feed(s, &pk, reader)
			s.account(&pk, reader)
			if !checked && time.Since(started) > 10*time.Second {
				checked = true
				p.startupCheck(s)
			}
		}
		// The camera stopped: the segment closes where the recording did (§15).
		s.cutter.Stop()
	})
}

// startupCheck is the ten-second look at what a capture produces (§38.3).
func (p *Producer) startupCheck(s *source) {
	st := s.cutter.Stats()
	if st.KeyInterval > 2500*time.Millisecond {
		p.log.Warn("keyframe interval above 2 s; expect early cuts", "source", s.cfg.Alias, "interval", st.KeyInterval.String())
	}
	s.mu.Lock()
	rate := s.rate(10)
	ceiling := s.ceiling
	s.mu.Unlock()
	if ceiling > 0 && rate > int64(float64(ceiling)*0.95) {
		p.log.Warn("measured rate at the ceiling; expect early cuts or poor motion quality", "source", s.cfg.Alias, "rate", rate, "ceiling", ceiling)
	}
}

// account keeps the per-second counters (§38.5): bytes and frames per
// second, and whether the trailing keyframe interval sat at the cap.
func (s *source) account(pk *Packet, r *Reader) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.lastTick.IsZero() {
		s.lastTick = now.Truncate(time.Second)
	}
	for now.Sub(s.lastTick) >= time.Second {
		// Close the second: was its trailing keyframe interval at the cap?
		s.closeSecond()
		s.lastTick = s.lastTick.Add(time.Second)
		s.secIdx = (s.secIdx + 1) % 60
		s.secBytes[s.secIdx] = 0
		s.secFrames[s.secIdx] = 0
	}
	s.secBytes[s.secIdx] += PacketSize
	if r.IsVideoFrame(pk) {
		s.secFrames[s.secIdx]++
	}
}

func (s *source) closeSecond() {
	s.seconds++
	window := 2
	if s.cutter != nil {
		if kf := s.cutter.Stats().KeyInterval; kf > 0 {
			window = int((kf + time.Second - 1) / time.Second)
			if window < 1 {
				window = 1
			}
			if window > 10 {
				window = 10
			}
		}
	}
	var sum int64
	for i := 0; i < window; i++ {
		sum += s.secBytes[(s.secIdx-i+60)%60]
	}
	allowed := float64(s.ceiling) / 8 * float64(window)
	at := s.ceiling > 0 && float64(sum) >= 0.95*allowed
	if at {
		s.atCap++
		s.run++
	} else {
		if s.run >= 3 && s.run <= 60 {
			s.episodes++
		}
		s.run = 0
	}
}

// rate is the measured rate over the last n seconds, in bits per second.
func (s *source) rate(n int) int64 {
	if n > 60 {
		n = 60
	}
	var sum int64
	for i := 1; i <= n; i++ {
		sum += s.secBytes[(s.secIdx-i+60)%60]
	}
	if n == 0 {
		return 0
	}

	return sum * 8 / int64(n)
}

func (s *source) frameRate() float64 {
	var sum int64
	for i := 1; i <= 10; i++ {
		sum += s.secFrames[(s.secIdx-i+60)%60]
	}

	return float64(sum) / 10
}

// enqueue hands a segment to the source's uploader, dropping the oldest
// unstored one when the RAM budget is exceeded (§16).
func (p *Producer) enqueue(s *source, seg *Segment) {
	s.mu.Lock()
	s.pending = append(s.pending, seg)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// uploads is one source's upload worker: segments in order, one at a time.
func (p *Producer) uploads(ctx context.Context, s *source) error {
	for {
		s.mu.Lock()
		var seg *Segment
		if len(s.pending) > 0 {
			seg = s.pending[0]
			s.pending = s.pending[1:]
		}
		s.mu.Unlock()
		if seg == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-s.wake:
			}
			continue
		}

		if p.overBudget() {
			// The oldest unstored segment is this one: dropped (§16).
			s.mu.Lock()
			s.dropped++
			s.mu.Unlock()
			p.log.Warn("segment dropped: over the RAM budget", "source", s.cfg.Alias, "started", seg.Started)
			continue
		}

		al, err := p.allocationFor(ctx, s, seg)
		if err != nil {
			p.log.Warn("no allocation", "source", s.cfg.Alias, "started", seg.Started, "err", err.Error())
			s.mu.Lock()
			s.lost++
			s.mu.Unlock()
			continue
		}
		s.mu.Lock()
		s.lastObj = al.GetObjectId()
		s.mu.Unlock()
		res := p.uploader.Upload(ctx, al, seg)
		s.mu.Lock()
		if res.Stored {
			s.stored++
		} else {
			s.lost++
		}
		s.mu.Unlock()
		if res.Stored {
			p.log.Info("stored", "source", s.cfg.Alias, "key", al.GetObjectKey(), "bytes", seg.Len(), "attempts", res.Attempts)
			p.m.segments.Add(ctx, 1, metric.WithAttributes(attribute.String("source", s.cfg.Alias), attribute.String("outcome", "stored")))
		} else {
			p.log.Warn("lost", "source", s.cfg.Alias, "key", al.GetObjectKey(), "err", res.Err)
			p.m.segments.Add(ctx, 1, metric.WithAttributes(attribute.String("source", s.cfg.Alias), attribute.String("outcome", "lost")))
		}
	}
}

// overBudget says whether retained segments exceed the RAM budget.
func (p *Producer) overBudget() bool {
	var total int64
	for _, s := range p.order {
		s.mu.Lock()
		for _, seg := range s.pending {
			total += seg.Len()
		}
		s.mu.Unlock()
	}

	return total > p.cfg.Buffer
}

// allocationFor is the allocation of the slot a segment belongs to, from
// what was fetched ahead or on demand (§12.1).
func (p *Producer) allocationFor(ctx context.Context, s *source, seg *Segment) (*api.Allocation, error) {
	s.mu.Lock()
	slot := s.sched.SlotOf(seg.Started).Unix()
	al := s.allocs[slot]
	if al != nil {
		delete(s.allocs, slot)
	}
	last := s.lastObj
	s.mu.Unlock()
	if al != nil && len(last) > 0 && bytes.Equal(al.GetObjectId(), last) {
		// The slot's allocation was the segment before this one, which
		// the camera ended: this one needs an object of its own (§15).
		al = nil
	}
	if al != nil && al.GetDateExpires().AsTime().After(time.Now().Add(time.Minute)) && len(al.GetCandidates()) > 0 {
		return al, nil
	}

	actx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req := api.ObjectAllocateRequest_builder{
		Source:      api.SourceRef_builder{Id: s.row.GetId()}.Build(),
		DateStarted: timestamppb.New(seg.Started),
	}
	if len(last) > 0 {
		req.After = api.ObjectRef_builder{Id: last}.Build()
	}
	al, err := api.NewObjectServiceClient(p.conn).Allocate(actx, req.Build())
	if err != nil {
		return nil, err
	}
	if len(al.GetCandidates()) == 0 {
		return nil, errors.New("the slot was already stored")
	}

	return al, nil
}

// allocations fetches allocations ahead for every member (§12.1).
func (p *Producer) allocations(ctx context.Context) error {
	for {
		horizon := time.Duration(p.set.GetLink().GetAllocationHorizonSeconds()) * time.Second
		if horizon <= 0 {
			horizon = 10 * time.Minute
		}
		actx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := api.NewSetServiceClient(p.conn).Allocate(actx, api.SetAllocateRequest_builder{
			Ref: api.SetRef_builder{Id: p.set.GetId()}.Build(),
		}.Build())
		cancel()
		if err != nil {
			p.log.Warn("allocate", "err", err.Error())
		} else {
			if resp.GetProfileVersion() != p.profileV {
				p.log.Info("profile changed; negotiating again", "was", p.profileV, "now", resp.GetProfileVersion())
				if err := p.negotiate(ctx); err != nil {
					p.log.Warn("negotiate", "err", err.Error())
				}
			}
			for _, al := range resp.GetAllocations() {
				for _, s := range p.order {
					if string(s.row.GetId()) != string(al.GetSourceId()) {
						continue
					}
					s.mu.Lock()
					s.allocs[s.sched.SlotOf(al.GetDateStarted().AsTime()).Unix()] = al
					// Forget what has expired.
					for k, v := range s.allocs {
						if v.GetDateExpires().AsTime().Before(time.Now()) {
							delete(s.allocs, k)
						}
					}
					s.mu.Unlock()
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(horizon / 3):
		}
	}
}

// ticks closes seconds for idle sources and renegotiates after early cuts.
func (p *Producer) ticks(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		raise := false
		for _, s := range p.order {
			s.mu.Lock()
			if s.raise {
				raise = true
			}
			s.mu.Unlock()
		}
		if raise {
			// A ceiling exceeded was a declaration that was wrong: +25% at the
			// next negotiation (§38.5), applied by the encoder at its next
			// start.
			if err := p.negotiate(ctx); err != nil {
				p.log.Warn("negotiate after early cut", "err", err.Error())
			}
		}
	}
}

// heartbeats reports every source and the host (§38.6).
func (p *Producer) heartbeats(ctx context.Context) error {
	t := time.NewTicker(p.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if err := p.heartbeat(ctx); err != nil {
			p.log.Warn("heartbeat", "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (p *Producer) heartbeat(ctx context.Context) error {
	var reports []*api.SourceReport
	for _, s := range p.order {
		s.mu.Lock()
		var st CutStats
		if s.cutter != nil {
			st = s.cutter.Stats()
		}
		r := api.SourceReport_builder{
			SourceId:           s.row.GetId(),
			InputUp:            s.capture != nil && s.capture.Up() && time.Since(st.LastKey) < 10*time.Second,
			FrameRate:          s.frameRate(),
			MeasuredBitrate:    s.rate(60),
			MaxBitrate:         s.ceiling,
			KeyframeIntervalMs: st.KeyInterval.Milliseconds(),
			EarlyCuts:          st.EarlyCuts,
			SecondsAtCap:       s.atCap,
			Episodes:           s.episodes,
			SecondsTotal:       s.seconds,
		}
		if s.capture != nil {
			r.CaptureRestarts = s.capture.Restarts()
			if !s.capture.Up() {
				r.Error = s.capture.LastError()
			}
		}
		s.atCap, s.episodes, s.seconds = 0, 0, 0
		s.mu.Unlock()
		reports = append(reports, r.Build())
		up := int64(0)
		if r.InputUp {
			up = 1
		}
		p.m.input.Record(ctx, up, sourceAttr(s.cfg.Alias))
		p.m.rate.Record(ctx, r.MeasuredBitrate, sourceAttr(s.cfg.Alias))
		p.m.fps.Record(ctx, r.FrameRate, sourceAttr(s.cfg.Alias))
		p.m.keyframe.Record(ctx, r.KeyframeIntervalMs, sourceAttr(s.cfg.Alias))
		p.m.restarts.Record(ctx, r.CaptureRestarts, sourceAttr(s.cfg.Alias))
	}
	p.relay.mu.Lock()
	p.m.dropped.Record(ctx, p.relay.dropped)
	p.relay.mu.Unlock()
	p.m.transcodes.Record(ctx, p.relay.transcodes())

	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := api.NewProducerServiceClient(p.conn).Heartbeat(hctx, api.ProducerHeartbeatRequest_builder{
		Sources: reports,
		Load:    api.HostLoad_builder{Cpu: cpuLoad(), Temperature: socTemperature()}.Build(),
		CaHash:  p.agent.BundleHash(),
		Version: hostagent.Version,
	}.Build())
	if err != nil {
		return err
	}
	p.relay.set(resp.GetRelay())
	if resp.GetProfileVersion() != 0 && resp.GetProfileVersion() != p.profileV {
		if err := p.negotiate(ctx); err != nil {
			p.log.Warn("negotiate", "err", err.Error())
		}
	}
	for _, sg := range resp.GetSuggestions() {
		p.log.Info("suggestion", "source", mustId(sg.GetSourceId()).String(), "max_bitrate", sg.GetMaxBitrate(), "suggested", sg.GetSuggested(), "applied", sg.GetApplied(), "why", sg.GetReason())
	}
	if resp.GetRenewCertificate() || p.agent.NeedsRenewal(time.Now()) {
		if err := p.agent.Renew(ctx, p.conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
			r, err := api.NewProducerServiceClient(conn).RenewCertificate(ctx, api.ProducerRenewCertificateRequest_builder{Csr: csr}.Build())
			if err != nil {
				return nil, nil, err
			}

			return r.GetCertificate(), r.GetCaBundle(), nil
		}); err != nil {
			p.log.Warn("renew certificate", "err", err.Error())
		} else {
			p.log.Info("certificate renewed; reconnecting")
			if conn, err := p.agent.Dial(ctx); err == nil {
				old := p.conn
				p.conn = conn
				old.Close()
			}
		}
	}

	return nil
}

// cpuLoad is the one-minute load average over the CPU count.
func cpuLoad() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)

	return v / float64(runtime.NumCPU())
}

// socTemperature is the SoC temperature in °C, where Linux offers one.
func socTemperature() float64 {
	for _, p := range []string{"/sys/class/thermal/thermal_zone0/temp"} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
		if err == nil {
			return v / 1000
		}
	}

	return 0
}

// ParseBitrate reads "4Mbps", "64kbps", "4000000".
func ParseBitrate(v string) (int64, error) {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" || v == "auto" {
		return 0, nil
	}
	v = strings.TrimSuffix(v, "bps")
	v = strings.TrimSuffix(v, "b/s")
	mult := 1.0
	switch {
	case strings.HasSuffix(v, "k"):
		mult, v = 1e3, strings.TrimSuffix(v, "k")
	case strings.HasSuffix(v, "m"):
		mult, v = 1e6, strings.TrimSuffix(v, "m")
	case strings.HasSuffix(v, "g"):
		mult, v = 1e9, strings.TrimSuffix(v, "g")
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0, fmt.Errorf("bitrate %q: %w", v, err)
	}

	return int64(f * mult), nil
}

var _ = z.Ptr[int]
var _ = status.Code
var _ = codes.OK
