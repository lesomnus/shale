package hostagent

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

// What a host says about itself in its heartbeat (§31, §38.6): the load
// and the temperature, where Linux offers them; zero elsewhere.

// Load is the one-minute load average over the CPU count.
func Load() float64 {
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

// Temperature is the SoC temperature in °C, where Linux offers one.
func Temperature() float64 {
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
