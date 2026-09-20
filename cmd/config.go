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
	// Leader lease: how often the background jobs run.
	JobsEvery time.Duration `yaml:"jobs_every"`
}

// SinkConfig is one sink a node serves (§22.2).
type SinkConfig struct {
	Path string `yaml:"path"`
	// Capacity is required on a shared filesystem, e.g. "500GiB".
	Capacity string `yaml:"capacity"`
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

	PartSize          string        `yaml:"part_size"`
	MaxUploads        int           `yaml:"max_uploads"`
	UploadsPerActor   int           `yaml:"uploads_per_actor"`
	PartBufferPool    string        `yaml:"part_buffer_pool"`
	ReadChunk         string        `yaml:"read_chunk"`
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

// SourceConfig is one camera a producer records (§38).
type SourceConfig struct {
	// Alias of the Source row; registered from this configuration when
	// missing.
	Alias string `yaml:"alias"`
	Name  string `yaml:"name"`
	// Input: an RTSP/ONVIF URL, a V4L2 device, or `-` for a stream on the
	// command's stdout.
	Input string `yaml:"input"`
	// Command is a custom capture command (tier 3); its stdout is the
	// MPEG-TS stream.
	Command []string `yaml:"command"`
	// Encoder options (tier 2), passed through to ffmpeg.
	EncoderOptions []string `yaml:"encoder_options"`
	// Structured settings (tier 1).
	Video VideoConfig `yaml:"video"`
	Audio AudioConfig `yaml:"audio"`
	// MaxBitrate is the declared ceiling, e.g. "4Mbps", or "auto" (§38.5).
	MaxBitrate string `yaml:"max_bitrate"`
	// PassThrough copies the camera's own stream without encoding.
	PassThrough bool `yaml:"pass_through"`
	// Zone label (§7).
	Zone string `yaml:"zone"`
}

// VideoConfig is the tier-1 video settings (§38.3).
type VideoConfig struct {
	Codec      string `yaml:"codec"`
	Resolution string `yaml:"resolution"`
	FrameRate  int    `yaml:"frame_rate"`
	Encoder    string `yaml:"encoder"`
	// Keyframe interval, e.g. "2s".
	KeyframeInterval time.Duration `yaml:"keyframe_interval"`
}

// AudioConfig is the tier-1 audio settings.
type AudioConfig struct {
	Codec   string `yaml:"codec"`
	Bitrate string `yaml:"bitrate"`
	Enabled bool   `yaml:"enabled"`
}

// ProducerConfig is a producer's own settings (§36.1, producer scope).
type ProducerConfig struct {
	// Set is the alias of the set this producer records; registered from
	// this configuration when missing and the producer is adopted for it.
	Set     string         `yaml:"set"`
	Sources []SourceConfig `yaml:"sources"`
	// Retain is `committed` (default) or `written` (§12.2).
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
	// Buffer is the RAM budget for segments, e.g. "512MiB".
	Buffer            string        `yaml:"buffer"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// Ffmpeg is the capture binary; `ffmpeg` on PATH by default.
	Ffmpeg string `yaml:"ffmpeg"`
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
