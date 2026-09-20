package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/lesomnus/shale/api"
)

// The tools' answers for a dataset with the recommended properties and
// for one with the defaults, as `zfs get -H -p` prints them.
func fakeZfs(direct string, props map[string]string) func(string, ...string) (string, error) {
	return func(name string, args ...string) (string, error) {
		joined := name + " " + strings.Join(args, " ")
		switch {
		case strings.HasPrefix(joined, "zfs list"):
			return "tank/cctv\n", nil
		case strings.HasPrefix(joined, "zpool get"):
			return "5282494836601136202\n", nil
		case strings.Contains(joined, "get -H -o value direct"):
			if direct == "" {
				return "", errors.New("bad property")
			}
			return direct + "\n", nil
		case strings.HasPrefix(joined, "zfs get"):
			var b strings.Builder
			for k, v := range props {
				b.WriteString(k + "\t" + v + "\n")
			}
			return b.String(), nil
		}

		return "", errors.New("unexpected: " + joined)
	}
}

func TestZfsRecommendedDataset(t *testing.T) {
	saved := runTool
	defer func() { runTool = saved }()
	runTool = fakeZfs("standard", map[string]string{
		"quota": "8589934592", "refquota": "0", "xattr": "sa", "recordsize": "1048576",
		"primarycache": "metadata", "compression": "off", "atime": "off", "logbias": "throughput",
	})
	z := zfsFromTools("/tank/cctv")
	if z == nil || z.Pool != "tank" || z.Guid != "5282494836601136202" || z.Quota != 8589934592 {
		t.Fatalf("%+v", z)
	}
	caps := api.SinkCapabilities_builder{Xattr: true, InlineRecord: true, Odirect: true}.Build()
	z.adjust(caps)
	if !caps.GetOdirect() || !caps.GetInlineRecord() || len(caps.GetWarnings()) != 0 {
		t.Fatalf("odirect=%v inline=%v warnings=%v", caps.GetOdirect(), caps.GetInlineRecord(), caps.GetWarnings())
	}
}

func TestZfsDefaultDataset(t *testing.T) {
	saved := runTool
	defer func() { runTool = saved }()
	// An older OpenZFS: no `direct`, xattr as directories, 128K records.
	runTool = fakeZfs("", map[string]string{
		"quota": "0", "refquota": "0", "xattr": "on", "recordsize": "131072",
		"primarycache": "all", "compression": "lz4", "atime": "on", "logbias": "latency",
	})
	z := zfsFromTools("/tank/cctv")
	if z == nil || z.Quota != 0 {
		t.Fatalf("%+v", z)
	}
	caps := api.SinkCapabilities_builder{Xattr: true, InlineRecord: true, Odirect: true}.Build()
	z.adjust(caps)
	if caps.GetOdirect() || caps.GetInlineRecord() {
		t.Fatalf("odirect=%v inline=%v", caps.GetOdirect(), caps.GetInlineRecord())
	}
	all := strings.Join(caps.GetWarnings(), "\n")
	for _, want := range []string{"direct=not available", "xattr=on", "recordsize=1M (is 128K)", "primarycache=metadata (is all)", "compression=off (is lz4)"} {
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in:\n%s", want, all)
		}
	}
}
