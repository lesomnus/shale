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
	"path/filepath"
	"time"

	"github.com/lesomnus/payday/config"
)

// Name is what this app is called, and it is the only place it is written.
// The environment prefix and the names of the configuration files are derived
// from it: SHALE_DB_DSN, shale.yaml.
const Name = "shale"

// Loader reads this app's configuration: a file, then the environment over
// the top of it (§34.1).
var Loader = config.For(Name)

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
	// a container whose interfaces are not the host's.
	Advertise string       `yaml:"advertise"`
	Sinks     []SinkConfig `yaml:"sinks"`

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
// your own whose stdout is MPEG-TS.
type SourceConfig struct {
	// Alias of the Source row; registered from this configuration when
	// missing (§38.4).
	Alias string `yaml:"alias"`
	Name  string `yaml:"name"`
	// Input: `v4l2:/dev/video0`, `rtsp://...`, `file:/path.ts`, or `push`
	// for a stream a process on this host writes to `producer.push`
	// (§38.9).
	Input string `yaml:"input"`
	// Kind of a pushed stream: `ts` (the default), or `raw` for frames
	// that are not video, cut at frame boundaries (§38.9).
	Kind string `yaml:"kind"`
	// ContentType of a raw source's laminae, e.g. `application/x-mcap`,
	// told to the CP for whoever reads them (§38.9).
	ContentType string `yaml:"content_type"`
	// Format is what the camera delivers: mjpeg | yuyv | h264 | h265.
	Format string `yaml:"format"`
	Size   string `yaml:"size"`
	Fps    int    `yaml:"fps"`
	// Encoder: auto | h264_v4l2m2m | h264_vaapi | h264_qsv | h264_nvenc |
	// libx264 | copy.
	Encoder string `yaml:"encoder"`
	// MaxBitrate is the declared ceiling, e.g. "4Mbps", or "auto" (§38.5).
	MaxBitrate string `yaml:"max_bitrate"`
	// KeyframeInterval, e.g. "2s"; negotiated, 2 s by default.
	KeyframeInterval time.Duration `yaml:"keyframe_interval"`
	Audio            *AudioConfig  `yaml:"audio"`
	// Tier 2.
	EncoderOptions  map[string]string `yaml:"encoder_options"`
	ExtraInputArgs  []string          `yaml:"extra_input_args"`
	ExtraOutputArgs []string          `yaml:"extra_output_args"`
	// Tier 3: run with `sh -c`; stdout must be MPEG-TS.
	Command string `yaml:"command"`
	// Zone label (§7).
	Zone string `yaml:"zone"`
}

// AudioConfig is a source's audio (§38.3): absent, a camera's own audio is
// recorded as the camera sends it, and a USB camera has none.
type AudioConfig struct {
	// Device is a microphone, e.g. `alsa:hw:1`, recorded in place of the
	// camera's audio.
	Device string `yaml:"device"`
	// Bitrate, e.g. "64kbps": what a microphone is encoded at, and the
	// Opus the live helper makes for the relay (§38.7).
	Bitrate string `yaml:"bitrate"`
	// Codec: copy (the default for a camera's audio), aac (the default for
	// a microphone), opus (plays live as it is), or none.
	Codec string `yaml:"codec"`
}

// AuthConfig is how people are known (§33.1).
type AuthConfig struct {
	// Roster is the identity store: an external roster's address and the
	// tenant keys this deployment acts with, or nothing, which runs roster
	// in the control plane's process on its own database (§34.7).
	Roster identity.Config `yaml:"roster"`
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
	IngestAddr      string        `yaml:"ingest_addr"`
	WhepAddr        string        `yaml:"whep_addr"`
	Advertise       string        `yaml:"advertise"`
	IdleStop        time.Duration `yaml:"idle_stop"`
	MaxViewers      int           `yaml:"max_viewers"`
	ViewersPerActor int           `yaml:"viewers_per_actor"`
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
