package cli

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/otx/log"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
	entschema "github.com/protobuf-orm/ent/dialect/sql/schema"
	"golang.org/x/sync/errgroup"

	"github.com/lesomnus/payday/spin"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	entmigrate "github.com/lesomnus/shale/internal/ent/migrate"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// NewCmdServe is `shale serve <role>` (§34.1): one binary, one command per
// role. `control` and `cluster` are separate entry points, not one server
// with a flag; `all` serves both on separate listeners with a node and a
// relay in the same process.
func NewCmdServe(c *cmd.Config) *xli.Command {
	common := func() flg.Flags {
		return flg.Flags{
			&flg.String{Name: "cp", Brief: "the control plane address this host dials"},
			&flg.String{Name: "dev", Brief: "development mode: everything under this directory, plaintext allowed"},
			&flg.String{Name: "state", Brief: "the state directory (/var/lib/shale)"},
		}
	}
	apply := func(self *xli.Command) {
		if v, ok := flg.Find[string](self, "dev"); ok && v != "" {
			ApplyDev(c, v)
		}
		if v, ok := flg.Find[string](self, "cp"); ok && v != "" {
			c.Cp = v
		}
		if v, ok := flg.Find[string](self, "state"); ok && v != "" {
			c.State = v
		}
	}

	return &xli.Command{
		Name:  "serve",
		Brief: "run one role",

		Commands: []*xli.Command{
			{
				Name: "control", Brief: "the Control Plane's tenant API", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return serveControlPlane(ctx, c, []cmd.Surface{cmd.SurfaceTenant}, false)
				}),
			},
			{
				Name: "cluster", Brief: "the Control Plane's cluster API", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return serveControlPlane(ctx, c, []cmd.Surface{cmd.SurfaceCluster}, false)
				}),
			},
			{
				Name: "storage", Brief: "a Storage Node", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					ctx, done, err := Telemetry(ctx, c)
					if err != nil {
						return err
					}
					defer done()
					n, err := storage.New(storageConfig(c))
					if err != nil {
						return err
					}
					return n.Run(ctx)
				}),
			},
			{
				Name: "producer", Brief: "a Producer: records the cameras of one set and uploads them", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return serveProducer(ctx, c)
				}),
			},
			{
				Name: "reader", Brief: "the Reader agent beside a media server", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return serveReader(ctx, c)
				}),
			},
			{
				Name: "relay", Brief: "a Relay: live streams to viewers over WebRTC", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return serveRelay(ctx, c)
				}),
			},
			{
				Name: "all", Brief: "control, cluster, one Storage Node, and one Relay in one process", Flags: common(),
				Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
					apply(self)
					return ServeAll(ctx, c, nil)
				}),
			},
		},

		Handler: xli.RequireSubcommand(),
	}
}

// serveControlPlane serves the given surfaces; `all` says which defaults
// the listeners take.
func serveControlPlane(ctx context.Context, c *cmd.Config, surfaces []cmd.Surface, all bool) error {
	ctx, done, err := Telemetry(ctx, c)
	if err != nil {
		return err
	}
	defer done()

	s, err := cmd.Build(ctx, *c)
	if err != nil {
		return err
	}
	defer s.Close()

	if c.Db.Migrate {
		if err := Migrate(ctx, s); err != nil {
			return err
		}
	} else if err := entschema.Check(ctx, s.Db, s.Dialect, entmigrate.Tables); err != nil {
		return err
	}
	if s.Kek == nil || s.CA == nil {
		return errNotInit
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return spin.Run(ctx, slices.Values(s.Spin)) })
	for _, surface := range surfaces {
		surface := surface
		l, err := net.Listen("tcp", c.ListenAddr(surface, all))
		if err != nil {
			return err
		}
		name := "tenant api"
		if surface == cmd.SurfaceCluster {
			name = "cluster api"
		}
		log.From(ctx).InfoContext(ctx, name, slog.String("addr", l.Addr().String()))
		g.Go(func() error { return s.Serve(ctx, surface, l) })
	}

	return g.Wait()
}

// Running is what ServeAll tells a caller once its listeners are up.
type Running struct {
	TenantAddr  string
	ClusterAddr string
	Node        *storage.Node
	CP          *cmd.Server
}

// ServeAll is §34.7: both APIs, a node, and a relay in one process. `ready`
// is told the addresses once they are bound.
func ServeAll(ctx context.Context, c *cmd.Config, ready func(Running)) error {
	ctx, done, err := Telemetry(ctx, c)
	if err != nil {
		return err
	}
	defer done()

	s, err := cmd.Build(ctx, *c)
	if err != nil {
		return err
	}
	defer s.Close()
	if c.Db.Migrate {
		if err := Migrate(ctx, s); err != nil {
			return err
		}
	} else if err := entschema.Check(ctx, s.Db, s.Dialect, entmigrate.Tables); err != nil {
		return err
	}
	if s.Kek == nil || s.CA == nil {
		return errNotInit
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return spin.Run(ctx, slices.Values(s.Spin)) })

	tl, err := net.Listen("tcp", c.ListenAddr(cmd.SurfaceTenant, true))
	if err != nil {
		return err
	}
	cl, err := net.Listen("tcp", c.ListenAddr(cmd.SurfaceCluster, true))
	if err != nil {
		return err
	}
	log.From(ctx).InfoContext(ctx, "tenant api", slog.String("addr", tl.Addr().String()))
	log.From(ctx).InfoContext(ctx, "cluster api", slog.String("addr", cl.Addr().String()))
	g.Go(func() error { return s.Serve(ctx, cmd.SurfaceTenant, tl) })
	g.Go(func() error { return s.Serve(ctx, cmd.SurfaceCluster, cl) })

	// The node in the same process dials the cluster listener, on loopback
	// when it listens on every interface.
	nc := storageConfig(c)
	if nc.Cp == "" || c.Cp == "" {
		scheme := "https://"
		if c.IsDev() {
			scheme = "http://"
		}
		addr := cl.Addr().String()
		if host, port, err := net.SplitHostPort(addr); err == nil {
			if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
				addr = net.JoinHostPort("127.0.0.1", port)
			}
		}
		nc.Cp = scheme + addr
	}
	n, err := storage.New(nc)
	if err != nil {
		return err
	}
	if ready != nil {
		ready(Running{TenantAddr: tl.Addr().String(), ClusterAddr: cl.Addr().String(), Node: n, CP: s})
	}
	g.Go(func() error {
		// Give the listeners a moment; the join loop retries anyway.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(200 * time.Millisecond):
		}

		return n.Run(ctx)
	})

	if relayInProcess != nil {
		g.Go(func() error { return relayInProcess(ctx, c, cl.Addr().String()) })
	}

	return g.Wait()
}

// storageConfig maps the file onto the node's settings.
func storageConfig(c *cmd.Config) storage.Config {
	sc := c.Storage
	cfg := storage.Config{
		StateDir:          c.StateDir("storage"),
		Cp:                c.Cp,
		CaHash:            c.CaHash,
		Dev:               c.IsDev(),
		Addr:              sc.Addr,
		ControlAddr:       sc.ControlAddr,
		Advertise:         sc.Advertise,
		HeartbeatInterval: sc.HeartbeatInterval,
		EventReplayWindow: sc.EventReplayWindow,
		SweepInterval:     sc.SweepInterval,
		TokenSkew:         sc.TokenSkew,
		GcPage:            sc.GcPage,
		GcProposalFactor:  sc.GcProposalFactor,
		Log:               slog.Default(),
	}
	if c.IsDev() {
		if cfg.Addr == "" {
			cfg.Addr = "127.0.0.1:7410"
		}
		if cfg.ControlAddr == "" {
			cfg.ControlAddr = "127.0.0.1:7411"
		}
	}
	cfg.Limits = storage.DefaultLimits
	if sc.MaxUploads > 0 {
		cfg.Limits.MaxUploads = sc.MaxUploads
	}
	if sc.UploadsPerActor > 0 {
		cfg.Limits.UploadsPerActor = sc.UploadsPerActor
	}
	if sc.MaxReadSessions > 0 {
		cfg.Limits.MaxReadSessions = sc.MaxReadSessions
	}
	if sc.SessionsPerActor > 0 {
		cfg.Limits.SessionsPerActor = sc.SessionsPerActor
	}
	cfg.Marks = storage.DefaultWatermarks
	if sc.WatermarkCritical > 0 {
		cfg.Marks.Critical = sc.WatermarkCritical
	}
	if sc.WatermarkLow > 0 {
		cfg.Marks.Low = sc.WatermarkLow
	}
	if sc.WatermarkTarget > 0 {
		cfg.Marks.Target = sc.WatermarkTarget
	}
	for _, s := range sc.Sinks {
		capacity, _ := storage.ParseCapacity(s.Capacity)
		cfg.Sinks = append(cfg.Sinks, storage.SinkConfig{Path: s.Path, Capacity: capacity, Device: s.Device})
	}

	return cfg
}

// serveProducer is `shale serve producer` (§38).
func serveProducer(ctx context.Context, c *cmd.Config) error {
	ctx, done, err := Telemetry(ctx, c)
	if err != nil {
		return err
	}
	defer done()
	cfg, err := ProducerConfig(c)
	if err != nil {
		return err
	}
	p, err := producer.New(cfg)
	if err != nil {
		return err
	}

	return p.Run(ctx)
}

// ProducerConfig maps the file onto the producer's settings.
func ProducerConfig(c *cmd.Config) (producer.Config, error) {
	pc := c.Producer
	cfg := producer.Config{
		StateDir:          c.StateDir("producer"),
		Cp:                c.Cp,
		CaHash:            c.CaHash,
		Dev:               c.IsDev(),
		Tenant:            c.Tenant,
		Ffmpeg:            pc.Ffmpeg,
		Retain:            pc.Retain,
		SegmentDuration:   pc.SegmentDuration,
		IdleTimeout:       pc.IdleTimeout,
		AbandonTimeout:    pc.AbandonTimeout,
		AllocationHorizon: pc.AllocationHorizon,
		HeartbeatInterval: pc.HeartbeatInterval,
		Log:               slog.Default(),
	}
	cfg.Upload = producer.UploadConfig{ResumeTimeout: pc.ResumeTimeout, RetryAfterCap: pc.RetryAfterCap, PlacementRetries: pc.PlacementRetries}
	switch strings.ToLower(pc.Mode) {
	case "buffered":
		cfg.Mode = api.UploadMode_UPLOAD_MODE_BUFFERED
	case "live", "":
		cfg.Mode = api.UploadMode_UPLOAD_MODE_LIVE
	default:
		return cfg, errors.New("producer.mode: live or buffered")
	}
	if pc.Uplink != "" {
		v, err := producer.ParseBitrate(pc.Uplink)
		if err != nil {
			return cfg, err
		}
		cfg.Uplink = v
	}
	if pc.Buffer != "" {
		v, err := storage.ParseCapacity(pc.Buffer)
		if err != nil {
			return cfg, err
		}
		cfg.Buffer = v
	}
	for _, sc := range pc.Sources {
		src := producer.SourceConfig{
			Alias: sc.Alias, Name: sc.Name, Input: sc.Input, Format: sc.Format, Size: sc.Size, Fps: sc.Fps,
			Encoder: sc.Encoder, KeyframeInterval: sc.KeyframeInterval, EncoderOptions: sc.EncoderOptions,
			ExtraInputArgs: sc.ExtraInputArgs, ExtraOutputArgs: sc.ExtraOutputArgs, Command: sc.Command, Zone: sc.Zone,
		}
		if sc.MaxBitrate == "" || strings.EqualFold(sc.MaxBitrate, "auto") {
			src.MaxBitrateAuto = true
		} else {
			v, err := producer.ParseBitrate(sc.MaxBitrate)
			if err != nil {
				return cfg, err
			}
			src.MaxBitrate = v
		}
		if sc.Audio != nil {
			a := &producer.AudioConfig{Device: sc.Audio.Device, Codec: sc.Audio.Codec}
			if sc.Audio.Bitrate != "" {
				v, err := producer.ParseBitrate(sc.Audio.Bitrate)
				if err != nil {
					return cfg, err
				}
				a.Bitrate = v
			}
			src.Audio = a
		}
		cfg.Sources = append(cfg.Sources, src)
	}

	return cfg, nil
}

// The reader agent and the relay arrive with their own packages; until
// then the commands say so.
var (
	serveReader = func(ctx context.Context, c *cmd.Config) error {
		return errors.New("the reader agent is not built yet")
	}
	serveRelay = func(ctx context.Context, c *cmd.Config) error {
		return errors.New("the relay is not built yet")
	}
	relayInProcess func(ctx context.Context, c *cmd.Config, clusterAddr string) error
)
