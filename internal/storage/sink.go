package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// DomSink is the domain byte of a Sink (§35.3), which the node mints sink
// identifiers with.
const DomSink pdid.Domain = 14

// LabelFile is the sink label at a sink's root (§23.1).
const LabelFile = ".shale-sink"

// Label is what the file holds.
type Label struct {
	SinkId   string    `json:"sink_id"`
	DeviceId string    `json:"device_id"`
	Created  time.Time `json:"created"`
	Version  int       `json:"version"`
}

// SinkConfig is one sink as configured (§22.2).
type SinkConfig struct {
	Path     string
	Capacity int64
	// Device declares the device identity instead of reading it from the
	// block device: for tests, and for volumes with no disk behind them.
	Device string
}

// Watermarks are the free-space thresholds as fractions of capacity (§21.1).
type Watermarks struct {
	Critical float64
	Low      float64
	Target   float64
}

// DefaultWatermarks are 3% / 5% / 8%.
var DefaultWatermarks = Watermarks{Critical: 0.03, Low: 0.05, Target: 0.08}

// Sink is one sink this node serves.
type Sink struct {
	Id       pdid.Id
	Path     string
	Label    Label
	DeviceId string
	// Capacity is the declared capacity, or zero for a whole filesystem.
	Capacity int64
	Caps     *api.SinkCapabilities
	Index    *Index
	Marks    Watermarks

	// accept is what the CP told the node, or what the node decided under
	// pressure; serve is whether the CP lets this node serve the sink at all
	// (§28.3).
	accept  atomic.Bool
	serve   atomic.Bool
	uploads atomic.Int64
	// critical is set by GC when nothing could be freed (§21.1).
	critical atomic.Bool

	// reserved is what open uploads may still write, counted against a
	// declared capacity.
	mu       sync.Mutex
	reserved int64
	warnings []string
}

// OpenSink prepares a sink: the directory, its label, its device, and the
// capability probe. A sink whose filesystem has no user xattrs is refused.
func OpenSink(c SinkConfig, marks Watermarks) (*Sink, error) {
	path, err := filepath.Abs(c.Path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(path, "laminae"), 0o755); err != nil {
		return nil, err
	}

	s := &Sink{Path: path, Capacity: c.Capacity, Marks: marks, Index: NewIndex()}
	s.accept.Store(true)
	s.serve.Store(true)

	dev, warn := deviceIdentity(path)
	// A ZFS dataset: the pool is the device (§22.2).
	z := zfsOf(path)
	if z != nil && z.Guid != "" {
		dev, warn = "zfs:"+z.Guid, nil
	}
	if c.Device != "" {
		dev, warn = c.Device, nil
	}
	s.DeviceId = dev
	s.warnings = append(s.warnings, warn...)

	if err := s.label(); err != nil {
		return nil, err
	}

	caps, err := probe(path)
	if err != nil {
		return nil, err
	}
	if z != nil {
		z.adjust(caps)
	}
	s.Caps = caps
	if !caps.GetXattr() {
		return nil, fmt.Errorf("sink %s: the filesystem has no user xattrs, which the record needs (§22.2)", path)
	}
	if c.Capacity == 0 {
		switch {
		case z != nil && z.Quota > 0:
			// The dataset's quota is the capacity (§22.2).
			s.Capacity = z.Quota
		default:
			if shared, why := looksShared(path); shared {
				s.warnings = append(s.warnings, "no capacity declared on "+why+"; the whole filesystem counts")
			}
		}
	}

	return s, nil
}

// label reads the label or writes a fresh one.
func (s *Sink) label() error {
	p := filepath.Join(s.Path, LabelFile)
	b, err := os.ReadFile(p)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.Label); err != nil {
			return fmt.Errorf("sink %s: label: %w", s.Path, err)
		}
		id, err := pdid.Parse(s.Label.SinkId)
		if err != nil || id.Domain() != DomSink {
			return fmt.Errorf("sink %s: label names %q, which is not a sink id", s.Path, s.Label.SinkId)
		}
		s.Id = id
		if s.Label.DeviceId != "" && s.Label.DeviceId != s.DeviceId {
			// A directory copied onto another device: flagged, not trusted (§9).
			s.warnings = append(s.warnings, fmt.Sprintf("label says device %s, this is %s", s.Label.DeviceId, s.DeviceId))
		}

		return nil
	case errors.Is(err, os.ErrNotExist):
		s.Id = pdid.New(DomSink)
		s.Label = Label{SinkId: s.Id.String(), DeviceId: s.DeviceId, Created: time.Now().UTC(), Version: 1}
		b, err := json.MarshalIndent(s.Label, "", "  ")
		if err != nil {
			return err
		}

		return os.WriteFile(p, b, 0o644)
	default:
		return err
	}
}

// Statfs is the filesystem's capacity and free space.
func (s *Sink) Statfs() (capacity, free int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(s.Path, &st); err != nil {
		return 0, 0, err
	}

	return int64(st.Blocks) * int64(st.Bsize), int64(st.Bavail) * int64(st.Bsize), nil
}

// Free is the sink's free space by its own measure (§22.2): the worse of
// headroom within a declared capacity and the filesystem's real free space.
func (s *Sink) Free() (capacity, free int64) {
	fsCap, fsFree, err := s.Statfs()
	if err != nil {
		return s.Capacity, 0
	}
	if s.Capacity <= 0 {
		return fsCap, fsFree
	}
	s.mu.Lock()
	reserved := s.reserved
	s.mu.Unlock()
	used := s.Index.Bytes() + reserved
	head := s.Capacity - used
	if head < 0 {
		head = 0
	}
	if fsFree < head {
		head = fsFree
	}

	return s.Capacity, head
}

// Pressure is where the sink stands against its watermarks (§21.1).
func (s *Sink) Pressure() api.Pressure {
	capacity, free := s.Free()
	if capacity <= 0 {
		return api.Pressure_PRESSURE_NORMAL
	}
	ratio := float64(free) / float64(capacity)
	switch {
	case s.critical.Load() || ratio < s.Marks.Critical:
		return api.Pressure_PRESSURE_CRITICAL
	case ratio < s.Marks.Low:
		return api.Pressure_PRESSURE_RECLAIM
	}

	return api.Pressure_PRESSURE_NORMAL
}

// AcceptsWrites is whether a new upload may open here.
func (s *Sink) AcceptsWrites() bool {
	return s.serve.Load() && s.accept.Load() && s.Pressure() != api.Pressure_PRESSURE_CRITICAL
}

// SetAccept is the CP's directive (§34.9).
func (s *Sink) SetAccept(v bool) { s.accept.Store(v) }

// SetServe is the CP's answer about attachment (§28.3).
func (s *Sink) SetServe(v bool) { s.serve.Store(v) }

// Serves is whether this node serves the sink.
func (s *Sink) Serves() bool { return s.serve.Load() }

// Reserve accounts for what an upload may write; negative releases.
func (s *Sink) Reserve(n int64) {
	s.mu.Lock()
	s.reserved += n
	if s.reserved < 0 {
		s.reserved = 0
	}
	s.mu.Unlock()
}

// Report is what a heartbeat says about the sink (§27).
func (s *Sink) Report() *api.SinkReport {
	capacity, free := s.Free()
	var newest *timestamppb.Timestamp
	if t := s.Index.Newest(); !t.IsZero() {
		newest = timestamppb.New(t)
	}
	s.mu.Lock()
	warnings := append([]string(nil), s.warnings...)
	s.mu.Unlock()

	return api.SinkReport_builder{
		SinkId:           s.Id.Bytes(),
		DeviceHardwareId: s.DeviceId,
		Path:             s.Path,
		Capacity:         capacity,
		Free:             free,
		Pressure:         s.Pressure(),
		UploadsInFlight:  s.uploads.Load(),
		Capabilities:     s.Caps,
		Warnings:         warnings,
		AcceptWrites:     s.AcceptsWrites(),
		Laminae:          int64(s.Index.Len()),
		DateNewest:       newest,
	}.Build()
}

// Warn adds a warning the heartbeat carries.
func (s *Sink) Warn(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.warnings {
		if w == v {
			return
		}
	}
	s.warnings = append(s.warnings, v)
}

// FilePath is the file of a key, refusing keys that leave the sink.
func (s *Sink) FilePath(key string) (string, error) {
	if !strings.HasPrefix(key, "laminae/") || strings.Contains(key, "..") || strings.Contains(key, "\x00") {
		return "", fmt.Errorf("bad lamina key %q", key)
	}
	p := filepath.Join(s.Path, filepath.FromSlash(key))
	if !strings.HasPrefix(p, s.Path+string(filepath.Separator)) {
		return "", fmt.Errorf("bad lamina key %q", key)
	}

	return p, nil
}

// ParseCapacity reads "500GiB", "16TB", "1073741824".
func ParseCapacity(v string) (int64, error) {
	v = strings.TrimSpace(strings.ToUpper(v))
	if v == "" {
		return 0, nil
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
		{"T", 1e12}, {"G", 1e9}, {"M", 1e6}, {"K", 1e3}, {"B", 1},
	}
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), 64)
			if err != nil {
				return 0, err
			}

			return int64(n * u.mult), nil
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("capacity %q: %w", v, err)
	}

	return n, nil
}

// probe tests what the sink's filesystem supports (§22.2).
func probe(path string) (*api.SinkCapabilities, error) {
	caps := api.SinkCapabilities_builder{}
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return nil, err
	}
	caps.Filesystem = fsName(st.Type)

	f, err := os.CreateTemp(path, ".shale-probe-*")
	if err != nil {
		return nil, err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()

	// user xattrs, with a record-sized value.
	v := make([]byte, 200)
	if err := unix.Fsetxattr(int(f.Fd()), XattrName, v, 0); err == nil {
		caps.Xattr = true
		// Whether it stays inline is a property of the inode size, which
		// no portable call answers; assume yes on XFS and ZFS with
		// xattr=sa, and warn on ext4 whose default inode is too small.
		switch caps.Filesystem {
		case "xfs", "zfs":
			caps.InlineRecord = true
		case "ext4":
			caps.InlineRecord = true
			caps.Warnings = append(caps.Warnings, "ext4: the record stays inline only with `mkfs.ext4 -I 512` or larger")
		case "tmpfs", "overlay", "btrfs":
			caps.InlineRecord = true
		}
	}

	// fallocate(KEEP_SIZE).
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, 1<<20); err == nil {
		caps.Fallocate = true
	}

	// O_DIRECT.
	if d, err := os.OpenFile(name, os.O_WRONLY|unix.O_DIRECT, 0); err == nil {
		caps.Odirect = true
		d.Close()
	}

	return caps.Build(), nil
}

func fsName(t int64) string {
	switch uint32(t) {
	case 0x58465342:
		return "xfs"
	case 0xEF53:
		return "ext4"
	case 0x2FC12FC1:
		return "zfs"
	case 0x01021994:
		return "tmpfs"
	case 0x794C7630:
		return "overlay"
	case 0x9123683E:
		return "btrfs"
	case 0x6969:
		return "nfs"
	}

	return fmt.Sprintf("0x%x", uint32(t))
}

// looksShared guesses whether a directory sink sits on a filesystem it
// shares with other things: the root filesystem, or one with a lot else on
// it.
func looksShared(path string) (bool, string) {
	var a, b unix.Stat_t
	if err := unix.Stat(path, &a); err != nil {
		return false, ""
	}
	if err := unix.Stat("/", &b); err == nil && a.Dev == b.Dev {
		return true, "the root filesystem"
	}

	return false, ""
}

// deviceIdentity is the hardware identity of the device under a path (§9):
// the WWN or serial from sysfs, else the filesystem id.
func deviceIdentity(path string) (string, []string) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return "", []string{"cannot stat the sink: " + err.Error()}
	}
	major, minor := unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev))

	sys := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	if target, err := filepath.EvalSymlinks(sys); err == nil {
		dir := target
		// A partition: its parent is the disk.
		if _, err := os.Stat(filepath.Join(dir, "partition")); err == nil {
			dir = filepath.Dir(dir)
		}
		for _, name := range []string{"device/wwid", "wwid", "device/serial", "serial"} {
			if b, err := os.ReadFile(filepath.Join(dir, name)); err == nil {
				if v := strings.TrimSpace(string(b)); v != "" {
					return "hw:" + strings.ToLower(strings.Join(strings.Fields(v), "_")), nil
				}
			}
		}
		if b, err := os.ReadFile(filepath.Join(dir, "dm/uuid")); err == nil {
			if v := strings.TrimSpace(string(b)); v != "" {
				return "dm:" + strings.ToLower(v), nil
			}
		}
	}

	// A volume Shale cannot see through: the filesystem's own id.
	var fs unix.Statfs_t
	if err := unix.Statfs(path, &fs); err == nil {
		id := fmt.Sprintf("fs:%08x%08x", uint32(fs.Fsid.Val[0]), uint32(fs.Fsid.Val[1]))

		return id, []string{"device identity is the filesystem id; no block device is visible under " + path}
	}

	return fmt.Sprintf("dev:%d:%d", major, minor), []string{"device identity is the device number, which does not survive a reboot"}
}
