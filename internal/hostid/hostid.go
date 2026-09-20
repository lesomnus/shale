// Package hostid reads the hardware identity a host joins with (§33.4).
//
// The order is fixed and not configurable (§36.1): the DMI product UUID, the
// device-tree serial number on boards without DMI such as a Raspberry Pi,
// and only then /etc/machine-id, which does not survive a reinstall and is
// reported as such.
package hostid

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/lesomnus/shale/api"
)

// Identity is what was read and where it came from.
type Identity struct {
	Id   string
	Kind api.HardwareIdKind
}

// Sources is the files tried, in order. A test may point them elsewhere.
var Sources = []struct {
	Path string
	Kind api.HardwareIdKind
}{
	{"/sys/class/dmi/id/product_uuid", api.HardwareIdKind_HARDWARE_ID_KIND_DMI},
	{"/sys/firmware/devicetree/base/serial-number", api.HardwareIdKind_HARDWARE_ID_KIND_DEVICE_TREE},
	{"/etc/machine-id", api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID},
}

// Read answers the first identity found.
func Read() (Identity, error) {
	for _, s := range Sources {
		b, err := os.ReadFile(s.Path)
		if err != nil {
			continue
		}

		// A device-tree string is NUL-terminated.
		v := strings.TrimSpace(strings.TrimRight(string(b), "\x00\n"))
		if v == "" {
			continue
		}

		// Some boards ship a DMI UUID of all zeros or all Fs, which names
		// nothing.
		if s.Kind == api.HardwareIdKind_HARDWARE_ID_KIND_DMI && placeholder(v) {
			continue
		}

		return Identity{Id: strings.ToLower(v), Kind: s.Kind}, nil
	}

	return Identity{}, errors.New("hostid: no hardware identity found")
}

// ReadOr answers the first identity found, and when the machine offers none
// at all (a container with no DMI and no machine-id), one made up once and
// kept in `dir`. It is reported as MACHINE_ID: it does not survive losing
// the directory, and the host says so.
func ReadOr(dir string) (Identity, error) {
	if v, err := Read(); err == nil {
		return v, nil
	}
	p := filepath.Join(dir, "host-id")
	if b, err := os.ReadFile(p); err == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			return Identity{Id: v, Kind: api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID}, nil
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return Identity{}, err
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return Identity{}, err
	}
	v := hex.EncodeToString(b)
	if err := os.WriteFile(p, []byte(v+"\n"), 0o600); err != nil {
		return Identity{}, err
	}

	return Identity{Id: v, Kind: api.HardwareIdKind_HARDWARE_ID_KIND_MACHINE_ID}, nil
}

func placeholder(v string) bool {
	t := strings.Trim(strings.ToLower(v), "0-f")
	if t == "" {
		u := strings.ReplaceAll(strings.ToLower(v), "-", "")
		return strings.Trim(u, "0") == "" || strings.Trim(u, "f") == ""
	}

	return false
}
