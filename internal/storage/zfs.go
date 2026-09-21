package storage

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lesomnus/shale/api"
)

// Sinks on OpenZFS datasets (§22.2): a dataset is a directory sink, the
// pool is the device (its GUID the identity), the dataset's quota is the
// capacity when the sink declares none, and its properties decide what the
// probe can promise: `direct=standard` (OpenZFS 2.3) is direct I/O,
// anything else goes through the ARC; `xattr=sa` keeps the record in the
// dnode, anything else is a hidden directory per file.

// zfsDataset is what the tools say about the dataset under a sink.
type zfsDataset struct {
	Dataset string
	Pool    string
	Guid    string
	// Quota is `quota`, or `refquota` when there is none; 0 is none.
	Quota int64
	Props map[string]string
}

// zfsProps are the properties read, and the values lamina data wants.
var zfsProps = []struct{ name, want string }{
	{"xattr", "sa"},
	{"recordsize", "1M"},
	{"primarycache", "metadata"},
	{"compression", "off"},
	{"atime", "off"},
	{"logbias", "throughput"},
}

// runTool runs a ZFS tool with a short timeout; tests replace it.
var runTool = func(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "", err
	}

	return string(out), nil
}

// zfsOf answers the dataset under a path when its filesystem is ZFS and
// the tools answer, nil otherwise.
func zfsOf(path string) *zfsDataset {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil || fsName(st.Type) != "zfs" {
		return nil
	}

	return zfsFromTools(path)
}

// zfsFromTools reads the dataset, its pool's GUID, and its properties.
func zfsFromTools(path string) *zfsDataset {
	out, err := runTool("zfs", "list", "-H", "-o", "name", path)
	if err != nil {
		return nil
	}
	z := &zfsDataset{Dataset: strings.TrimSpace(out), Props: map[string]string{}}
	if z.Dataset == "" {
		return nil
	}
	z.Pool = z.Dataset
	if i := strings.IndexByte(z.Dataset, '/'); i > 0 {
		z.Pool = z.Dataset[:i]
	}
	if out, err := runTool("zpool", "get", "-H", "-o", "value", "guid", z.Pool); err == nil {
		z.Guid = strings.TrimSpace(out)
	}

	names := []string{"quota", "refquota"}
	for _, p := range zfsProps {
		names = append(names, p.name)
	}
	if out, err := runTool("zfs", "get", "-H", "-p", "-o", "property,value", strings.Join(names, ","), z.Dataset); err == nil {
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				z.Props[f[0]] = f[1]
			}
		}
	}
	// `direct` exists from OpenZFS 2.3; asked on its own since an unknown
	// property fails the whole request on older versions.
	if out, err := runTool("zfs", "get", "-H", "-o", "value", "direct", z.Dataset); err == nil {
		if v := strings.TrimSpace(out); v != "" && v != "-" {
			z.Props["direct"] = v
		}
	}
	for _, name := range []string{"quota", "refquota"} {
		if v, err := strconv.ParseInt(z.Props[name], 10, 64); err == nil && v > 0 {
			z.Quota = v
			break
		}
	}

	return z
}

// adjust makes the probe's answer the dataset's: direct I/O only with
// `direct=standard` or `always`, the record inline only with `xattr=sa`,
// and a line for every recommended property that is off, which the
// heartbeat carries and `sink get` shows.
func (z *zfsDataset) adjust(caps *api.SinkCapabilities) {
	switch z.Props["direct"] {
	case "standard", "always":
		caps.SetOdirect(true)
	default:
		caps.SetOdirect(false)
		v := z.Props["direct"]
		if v == "" {
			v = "not available before OpenZFS 2.3"
		}
		caps.SetWarnings(append(caps.GetWarnings(), "zfs: direct="+v+": reads and writes go through the ARC; direct=standard bypasses it"))
	}
	if z.Props["xattr"] != "sa" {
		caps.SetInlineRecord(false)
		caps.SetWarnings(append(caps.GetWarnings(), "zfs: xattr=sa keeps the record in the dnode; this dataset has xattr="+z.Props["xattr"]))
	}
	var off []string
	for _, p := range zfsProps {
		if p.name == "xattr" {
			continue
		}
		have := z.Props[p.name]
		if !zfsSame(p.name, have, p.want) {
			off = append(off, fmt.Sprintf("%s=%s (is %s)", p.name, p.want, zfsShow(p.name, have)))
		}
	}
	if len(off) > 0 {
		caps.SetWarnings(append(caps.GetWarnings(), "zfs: recommended for lamina data: "+strings.Join(off, ", ")))
	}
}

// zfsSame compares a property value from `zfs get -p` with the wanted one.
func zfsSame(name, have, want string) bool {
	if name == "recordsize" {
		return have == "1048576"
	}

	return have == want
}

// zfsShow prints a value as an operator would write it.
func zfsShow(name, v string) string {
	if name == "recordsize" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			switch {
			case n >= 1<<20 && n%(1<<20) == 0:
				return fmt.Sprintf("%dM", n>>20)
			case n >= 1<<10 && n%(1<<10) == 0:
				return fmt.Sprintf("%dK", n>>10)
			}
		}
	}
	if v == "" {
		return "unset"
	}

	return v
}
