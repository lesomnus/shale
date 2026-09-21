package storage

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// netDev reads what the kernel counts per interface, received and sent
// bytes, from /proc/net/dev (§31). Elsewhere than Linux, or when the file
// is not there, it answers nothing and the NIC gauges stay silent.
func netDev() map[string][2]int64 {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil
	}
	defer f.Close()
	out := map[string][2]int64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, rest, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "lo" {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) < 9 {
			continue
		}
		rx, err1 := strconv.ParseInt(fields[0], 10, 64)
		tx, err2 := strconv.ParseInt(fields[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out[name] = [2]int64{rx, tx}
	}

	return out
}
