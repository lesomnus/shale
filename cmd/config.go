// Package cmd is this app's own wiring, and it is short on purpose.
//
// Everything that does not change from one app to the next is in payday. What
// is left is here, and it is deliberately **not** hidden behind a
// `payday.Serve(cfg)`: the stack, the order of the interceptors and which
// server the wall is on are the decisions a reader of an app most needs to be
// able to see, and a framework that hid them would be hiding the only part
// worth reading.
package cmd

import (
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso"
	"path/filepath"
	"time"

	"github.com/lesomnus/payday/config"
)

// Name is what this app is called, and it is the only place it is written.
// The environment prefix and the names of the configuration files are derived
// from it by the loader `cli.Cmd` makes with it: SHALE_DB_DSN, shale.yaml.
const Name = "shale"

// Config is what this app is configured with. Every role reads the same
// file and uses the sections that concern it.
type Config struct {
	// Server is the tenant API listener (`shale serve control`).
	Server config.ServerConfig `yaml:"server"`
	// Cluster is the cluster API listener (`shale serve cluster`), a separate
	// entry point on the internal network (§35.2).
	Cluster config.ServerConfig `yaml:"cluster"`
	Db      config.DbConfig     `yaml:"db"`
	Otel    config.OtelConfig   `yaml:"otel"`
	Watch   config.WatchConfig  `yaml:"watch"`

	// State is where every role keeps what it must not lose (§34.3):
	// `/var/lib/shale` by default, with a directory per role under it.
	State string `yaml:"state"`

	// Cp is the Control Plane address a host dials (§33.4): the tenant API
	// for producers and readers, the cluster API for nodes and relays.
	Cp string `yaml:"cp"`
	// CaHash turns the first-contact pin into a check: sha256:<hex> of the
	// CA certificate.
	CaHash string `yaml:"ca_hash"`
	// Tenant is the tenant a producer or reader joins, implied in a
	// single-organization cluster.
	Tenant string `yaml:"tenant"`

	Control  ControlConfig  `yaml:"control"`
	Storage  StorageConfig  `yaml:"storage"`
	Producer ProducerConfig `yaml:"producer"`
	Reader   ReaderConfig   `yaml:"reader"`
	Relay    RelayConfig    `yaml:"relay"`

	// Client is how this binary reaches a deployment when it is the CLI.
	Client ClientConfig `yaml:"client"`
	// Auth is who people are (§33.1): roster, in this process or elsewhere.
	Auth AuthConfig `yaml:"auth"`
	// Audit is how long the trail is kept and where what leaves it goes
	// (§26.5): payday's policy, a window per kind of thing, applied by the
	// leader. Empty keeps everything.
	Audit config.AuditConfig `yaml:"audit"`

	// Dev is development mode (`--dev <dir>`): everything in one directory,
	// plaintext allowed, one directory sink (§34.7).
	Dev string `yaml:"dev"`
}

// ClientConfig is the CLI's connection (§32).
type ClientConfig struct {
	// Addr is the tenant API; ClusterAddr the cluster API. Either may be a
	// URL with scheme http (plaintext, development) or https.
	Addr        string `yaml:"addr"`
	ClusterAddr string `yaml:"cluster_addr"`
	// As is who the CLI calls as with the plain header, development only:
	// `@tenant/alias`.
	As string `yaml:"as"`
	// Token is a session or API token.
	Token string `yaml:"token"`
	// CaFile is the CA to verify the CP against; the state directory's
	// bundle when empty.
	CaFile string `yaml:"ca_file"`
	// Web and ClusterWeb are where the two HTTP listeners answer -- the
	// sign-in endpoints -- when that is not the API's host two ports up:
	// an Ingress in front, e.g. https://shale.hday.dev and
	// https://ops.shale.hday.dev. Verified against the system's roots and
	// the CP's CA.
	Web        string `yaml:"web"`
	ClusterWeb string `yaml:"cluster_web"`
}

// ControlConfig is the Control Plane's own settings.
type ControlConfig struct {
	// ClusterTenant is the alias of the tenant that holds cluster operators.
	ClusterTenant string `yaml:"cluster_tenant"`
	// Readopt is `manual` (default) or `auto` (§33.4).
	Readopt string `yaml:"readopt"`
	// Names are extra names the CP certificate carries, for hosts that dial
	// it by a name the CP cannot guess.
	Names []string `yaml:"names"`
	// External certificates for the CP's listeners, instead of the built-in
	// CA's (§33.5).
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// AutoAdopt adopts every joining node and relay at once. `shale serve
	// all` sets it for the hosts in its own process; a lab may set it for
	// everything.
	AutoAdopt bool `yaml:"auto_adopt"`
	// ActorLimit is a rate limit per actor on both APIs, beside the
	// per-tenant one of `server.limit` (§35.1): a misbehaving host is
	// refused with RESOURCE_EXHAUSTED before its tenant is.
	ActorLimit config.LimitConfig `yaml:"actor_limit"`
	// Leader lease: how often the background jobs run.
	JobsEvery time.Duration `yaml:"jobs_every"`
	// DirectivesEvery is how often the leader compares the state with what
	// the nodes were told (§34.9); default 5 s.
	DirectivesEvery time.Duration `yaml:"directives_every"`
	// NodeDownAfter is how long a host may go unheard before it is down
	// (§27); default 30 s. Its laminae read as unavailable from then on and
	// placement stops offering its sinks, so a cluster whose heartbeats
	// cross a slow link raises it rather than flap.
	NodeDownAfter time.Duration `yaml:"node_down_after"`
	// SinkAutoAdoptAfter is how long a sink whose node is down waits before
	// the node that reports it takes it over (§28.3); default 10 min.
	SinkAutoAdoptAfter time.Duration `yaml:"sink_auto_adopt_after"`
}

// SinkConfig is one sink a node serves (§22.2).
type SinkConfig struct {
	Path string `yaml:"path"`
	// Capacity is required on a shared filesystem, e.g. "500GiB".
	Capacity string `yaml:"capacity"`
	// Device declares the device identity for a volume with no disk behind
	// it (a lab, a test); normally it is read from the block device.
	Device string `yaml:"device"`
}

// StorageConfig is a Storage Node's own settings (§36.1, node scope).
type StorageConfig struct {
	// Addr is the data plane listener.
	Addr string `yaml:"addr"`
	// ControlAddr is the control API listener the CP dials (§35.7).
	ControlAddr string `yaml:"control_addr"`
	// Advertise overrides the address a node reports for its data plane, for
	// a container whose interfaces are not the host's. A name here is handed
	// out as it is (§34.10).
	Advertise string `yaml:"advertise"`
	// External certificate for the data plane, instead of the host
	// certificate, e.g. one a browser trusts for the console's Playback
	// (§33.5); both or neither, read again when the files change. The
	// control API keeps the host certificate.
	CertFile string       `yaml:"cert_file"`
	KeyFile  string       `yaml:"key_file"`
	Sinks    []SinkConfig `yaml:"sinks"`

	PartSize        string `yaml:"part_size"`
	MaxUploads      int    `yaml:"max_uploads"`
	UploadsPerActor int    `yaml:"uploads_per_actor"`
	PartBufferPool  string `yaml:"part_buffer_pool"`
	ReadChunk       string `yaml:"read_chunk"`
	// GcInterval is how often a sink under pressure gets a GC round (1 min).
	GcInterval        time.Duration `yaml:"gc_interval"`
	MaxReadSessions   int           `yaml:"max_read_sessions"`
	SessionsPerActor  int           `yaml:"sessions_per_actor"`
	ReadBacklog       int           `yaml:"read_backlog"`
	EventReplayWindow time.Duration `yaml:"event_replay_window"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	GcPage            int           `yaml:"gc_page"`
	GcProposalFactor  float64       `yaml:"gc_proposal_factor"`
	TokenSkew         time.Duration `yaml:"token_skew"`
	SweepInterval     time.Duration `yaml:"sweep_interval"`
	// Watermarks as fractions of capacity (§21.1): 0.03 / 0.05 / 0.08.
	WatermarkCritical float64 `yaml:"watermark_critical"`
	WatermarkLow      float64 `yaml:"watermark_low"`
	WatermarkTarget   float64 `yaml:"watermark_target"`
	// Buffered forces buffered I/O even where O_DIRECT works.
	Buffered bool `yaml:"buffered"`
}

// SourceConfig is one camera a producer records (§38.3): tier 1 is the
// structured fields, tier 2 the options passed through, tier 3 a command of
// your own whose stdout is fragmented MP4.
type SourceConfig struct {
	// Alias of the Source row; registered from this configuration when
	// missing (§38.4).
	Alias string `yaml:"alias"`
	Name  string `yaml:"name"`
	// Input: `v4l2:/dev/video0`, `rtsp://...`, `file:/path.mp4`, or `push`
	// for a stream a process on this host writes to `producer.push`
	// (§38.9).
	Input string `yaml:"input"`
	// Kind of a pushed stream: `mp4` (the default), or `raw` for frames
	// that are not video, cut at frame boundaries (§38.9).
	Kind string `yaml:"kind"`
	// ContentType of a raw source's laminae, e.g. `application/x-mcap`,
	// told to the CP for whoever reads them (§38.9).
	ContentType string `yaml:"content_type"`
	// Format is what the camera delivers: mjpeg | yuyv | h264 | h265.
	Format string `yaml:"format"`
	Size   string `yaml:"size"`
	Fps    int    `yaml:"fps"`
	// Capture is the tool that runs the camera: `ffmpeg` (the default) or
	// `gstreamer`, for a V4L2 camera on the Raspberry Pi's encoder, with
	// its microphone (`audio.device`) or without (§38.3).
	Capture string `yaml:"capture"`
	// Encoder: auto | h264_v4l2m2m | h264_vaapi | h264_qsv | h264_nvenc |
	// libx264 | copy; under `capture: gstreamer` auto | v4l2h264enc |
	// x264enc | copy.
	Encoder string `yaml:"encoder"`
	// MaxBitrate is the declared ceiling, e.g. "4Mbps", or "auto" (§38.5).
	MaxBitrate string `yaml:"max_bitrate"`
	// KeyframeInterval, e.g. "2s"; negotiated, 2 s by default.
	KeyframeInterval time.Duration `yaml:"keyframe_interval"`
	Audio            *AudioConfig  `yaml:"audio"`
	// Controls are V4L2 controls set on a `v4l2:` device before every
	// capture start, by v4l2-ctl's names, e.g. `exposure_dynamic_framerate:
	// 0` so a Logitech camera keeps its frame rate in low light (§38.3).
	Controls map[string]string `yaml:"controls"`
	// Idle skips the segments of a scene that has been dark for
	// `dark_after` (10 min) until it is lit again (§38.10); absent, every
	// segment is stored.
	Idle *IdleConfig `yaml:"idle"`
	// Tier 2.
	EncoderOptions  map[string]string `yaml:"encoder_options"`
	ExtraInputArgs  []string          `yaml:"extra_input_args"`
	ExtraOutputArgs []string          `yaml:"extra_output_args"`
	// Tier 3: run with `sh -c`; stdout must be fragmented MP4 (§38.3).
	Command string `yaml:"command"`
	// Zone label (§7).
	Zone string `yaml:"zone"`
}

// IdleConfig is a source's `idle:` (§38.10).
type IdleConfig struct {
	// DarkAfter is how long the scene stays dark before its segments are
	// skipped; 10 minutes by default.
	DarkAfter time.Duration `yaml:"dark_after"`
	// Threshold is the luma, as a fraction of full scale (0.10 by
	// default), at or below which a pixel is dark; a frame is dark when
	// 98% of its pixels are.
	Threshold float64 `yaml:"threshold"`
}

// AudioConfig is a source's audio (§38.3): absent, a camera's own audio is
// recorded as the camera sends it, and a USB camera has none.
type AudioConfig struct {
	// Device is a microphone, e.g. `alsa:hw:1`, or the path of its USB
	// port, `alsa:/dev/snd/by-path/…`, resolved to its card at every start;
	// recorded in place of the camera's audio.
	Device string `yaml:"device"`
	// Bitrate, e.g. "64kbps": what a microphone is encoded at, and the
	// Opus track written beside the archive's for live (§38.7).
	Bitrate string `yaml:"bitrate"`
	// Codec: copy (the default for a camera's audio), aac (the default for
	// a microphone), opus (one track for the archive and live both), or
	// none. copy and aac get an Opus track beside them (§38.7).
	Codec string `yaml:"codec"`
}

// AuthConfig is how people are known (§33.1).
type AuthConfig struct {
	// Roster is the identity store: an external roster's address and the
	// key -- or the tenant keys -- this deployment acts with, or nothing,
	// which runs roster in the control plane's process on its own database
	// (§34.7).
	Roster identity.Config `yaml:"roster"`
	// Operators says that what people may change is what roster grants
	// them, and whose people operate the cluster, rather than the people of
	// `control.cluster_tenant` (§33.1). Set, every person of a tenant
	// reads, and calls the rest as far as a role at roster covers it.
	Operators identity.OperatorsConfig `yaml:"operators"`
	// Oidc is the issuer people sign in through (§33.1): each HTTP listener
	// is a relying party of it, and the CLI signs operators in with its
	// device flow.
	Oidc sso.Config `yaml:"oidc"`
	// SsoOnly refuses a password: `POST /session` with one is answered
	// with a refusal, and the console draws only the issuer's button.
	// Requires `auth.oidc`.
	SsoOnly bool `yaml:"sso_only"`
}

// ProducerConfig is a producer's own settings (§36.1, producer scope).
type ProducerConfig struct {
	// Set is the alias of the set this producer records; registered from
	// this configuration when missing and the producer is adopted for it.
	Set     string         `yaml:"set"`
	Sources []SourceConfig `yaml:"sources"`
	// Retain is `committed` (default), the whole segment in RAM until the
	// node's 201, or `written`, only what is above the offset the node
	// last reported durable: less RAM, and a segment whose node dies is
	// cut short there rather than sent elsewhere (§12.2).
	Retain string `yaml:"retain"`
	// Upload mode proposal: `live` or `buffered`.
	Mode              string        `yaml:"mode"`
	SegmentDuration   time.Duration `yaml:"segment_duration"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	AbandonTimeout    time.Duration `yaml:"abandon_timeout"`
	AllocationHorizon time.Duration `yaml:"allocation_horizon"`
	ResumeTimeout     time.Duration `yaml:"resume_timeout"`
	RetryAfterCap     time.Duration `yaml:"retry_after_cap"`
	PlacementRetries  int           `yaml:"placement_retries"`
	// Uplink is the link's capacity, e.g. "20Mbps", for the link check.
	Uplink string `yaml:"uplink"`
	// Buffer is the RAM budget for segments not yet stored, e.g. "512MiB"
	// (§16): under `committed` the fleet's segments in flight, sources ×
	// max_bitrate × segment_duration, plus headroom; under `written` about
	// a part per source, plus whatever an outage makes wait whole.
	Buffer            string        `yaml:"buffer"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// Ffmpeg is the capture binary; `ffmpeg` on PATH by default.
	Ffmpeg string `yaml:"ffmpeg"`
	// FragmentDuration bounds a fragment of every source's stream, e.g.
	// "200ms" (500 ms when unset): the live tee sends whole fragments, so
	// it is about how far behind the camera a live viewer is (§38.2).
	FragmentDuration time.Duration `yaml:"fragment_duration"`
	// Gstreamer is gst-launch, for the sources with `capture: gstreamer`
	// (§38.3); `gst-launch-1.0` on PATH by default.
	Gstreamer string `yaml:"gstreamer"`
	// Demo records this many sources of a picture and a tone ffmpeg draws
	// for itself, beside whatever `sources` lists (§38.1): a tutorial or a
	// walk-through that has no camera to hand. `--demo` on `serve producer`
	// sets it.
	Demo int `yaml:"demo"`
	// Push is the listener pushed sources are written to (§38.9):
	// `unix:/run/shale/push.sock` or `tcp://127.0.0.1:7450`. Off when
	// empty. PushIdle is how long a pushed stream may carry nothing
	// before its open segment closes as stopped (30 s).
	Push     string        `yaml:"push"`
	PushIdle time.Duration `yaml:"push_idle"`
}

// ReaderConfig is the reader agent's own settings (§33.4).
type ReaderConfig struct {
	// Addr is the plaintext localhost listener the media server uses.
	Addr string `yaml:"addr"`
}

// RelayConfig is a relay's own settings (§39).
type RelayConfig struct {
	IngestAddr string `yaml:"ingest_addr"`
	WhepAddr   string `yaml:"whep_addr"`
	// Advertise overrides the host of both addresses the relay reports,
	// keeping the ports bound.
	Advertise string `yaml:"advertise"`
	// WhepAdvertise is the WHEP address reported instead, a host or
	// host:port, e.g. the name a browser-trusted certificate carries
	// (§39.4). Its port, when it has one, is the one handed out.
	WhepAdvertise string `yaml:"whep_advertise"`
	// External certificate for the WHEP listener, instead of the host
	// certificate, which browsers do not trust; both or neither, read
	// again when the files change. Ingest keeps the host certificate
	// (§33.5).
	WhepCertFile    string        `yaml:"whep_cert_file"`
	WhepKeyFile     string        `yaml:"whep_key_file"`
	IdleStop        time.Duration `yaml:"idle_stop"`
	MaxViewers      int           `yaml:"max_viewers"`
	ViewersPerActor int           `yaml:"viewers_per_actor"`
	// RewindBudget bounds what the recent windows of every source hold
	// together, e.g. "1GiB", the default (§39.5).
	RewindBudget string `yaml:"rewind_budget"`
	// Ice servers, e.g. "stun:stun.l.google.com:19302".
	Ice []string `yaml:"ice"`
	// Public IPs to announce as host candidates, for a relay behind NAT.
	Nat1To1 []string `yaml:"nat_1to1"`
	// The UDP port range for ICE.
	UdpPortMin int `yaml:"udp_port_min"`
	UdpPortMax int `yaml:"udp_port_max"`
}

// StateDir is the directory a role keeps its state in (§34.3).
func (c Config) StateDir(role string) string {
	base := c.State
	if base == "" {
		if c.Dev != "" {
			base = c.Dev
		} else {
			base = "/var/lib/shale"
		}
	}

	return filepath.Join(base, role)
}

// IsDev is whether this process runs in development mode.
func (c Config) IsDev() bool { return c.Dev != "" }
