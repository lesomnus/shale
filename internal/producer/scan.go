package producer

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Discovery (§38.4): what this host can see, as a configuration skeleton
// to edit, and a probe of the configured sources.

// Scan is what `shale producer scan` found.
type Scan struct {
	Cameras  []Camera
	Onvif    []string
	Audio    []string
	Encoders []string
}

// Camera is one V4L2 device with its modes.
type Camera struct {
	Device string
	Name   string
	Modes  []string
	H264   bool
}

// ScanHost enumerates cameras, ONVIF devices, audio devices, and encoders.
func ScanHost(ctx context.Context, ffmpeg string) Scan {
	var s Scan
	devs, _ := filepath.Glob("/dev/video*")
	sort.Strings(devs)
	for _, d := range devs {
		c := Camera{Device: d}
		if b, err := os.ReadFile(filepath.Join("/sys/class/video4linux", filepath.Base(d), "name")); err == nil {
			c.Name = strings.TrimSpace(string(b))
		}
		c.Modes, c.H264 = v4l2Modes(ctx, ffmpeg, d)
		if len(c.Modes) == 0 {
			// A metadata node or an output device: nothing to record from.
			continue
		}
		s.Cameras = append(s.Cameras, c)
	}
	s.Onvif = onvifDiscover(ctx, 2*time.Second)
	s.Audio = alsaDevices()
	if vs, err := AvailableEncoders(ctx, ffmpeg); err == nil {
		for _, v := range vs {
			for _, e := range EncoderOrder {
				if v == e {
					s.Encoders = append(s.Encoders, v)
				}
			}
		}
	}

	return s
}

// v4l2Modes lists a device's formats, sizes, and rates through ffmpeg,
// which every producer has, rather than through v4l2-ctl, which not every
// host has.
func v4l2Modes(ctx context.Context, ffmpeg, dev string) ([]string, bool) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-f", "v4l2", "-list_formats", "all", "-i", dev).CombinedOutput()
	// [video4linux2,v4l2 @ ...] Raw       :     yuyv422 :           YUYV 4:2:2 : 640x480 1280x720 1920x1080
	// [video4linux2,v4l2 @ ...] Compressed:       mjpeg :          Motion-JPEG : 640x480 1280x720 1920x1080
	re := regexp.MustCompile(`\]\s+(Raw|Compressed)\s*:\s*(\S+)\s*:\s*(.*?)\s*:\s*(.*)$`)
	var modes []string
	h264 := false
	for _, line := range strings.Split(string(out), "\n") {
		m := re.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		format := m[2]
		if format == "h264" {
			h264 = true
		}
		sizes := strings.Fields(m[4])
		modes = append(modes, fmt.Sprintf("%s: %s", format, strings.Join(sizes, " ")))
	}

	return modes, h264
}

// alsaDevices lists capture devices from /proc/asound.
func alsaDevices() []string {
	b, err := os.ReadFile("/proc/asound/cards")
	if err != nil {
		return nil
	}
	var out []string
	re := regexp.MustCompile(`^\s*(\d+)\s+\[(\S+)\s*\]:\s*(.*)$`)
	for _, line := range strings.Split(string(b), "\n") {
		if m := re.FindStringSubmatch(line); m != nil {
			out = append(out, fmt.Sprintf("alsa:hw:%s  (%s)", m[1], strings.TrimSpace(m[3])))
		}
	}

	return out
}

// onvifDiscover is WS-Discovery on the local network: one Probe to the
// multicast group, and the XAddrs of whatever answers.
func onvifDiscover(ctx context.Context, wait time.Duration) []string {
	probe := `<?xml version="1.0" encoding="UTF-8"?>
<e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope" xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
<e:Header><w:MessageID>uuid:shale-` + fmt.Sprint(time.Now().UnixNano()) + `</w:MessageID><w:To e:mustUnderstand="true">urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To><w:Action e:mustUnderstand="true">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action></e:Header>
<e:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></e:Body></e:Envelope>`
	conn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return nil
	}
	defer conn.Close()
	dst := &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 3702}
	if _, err := conn.WriteTo([]byte(probe), dst); err != nil {
		return nil
	}
	deadline := time.Now().Add(wait)
	conn.SetReadDeadline(deadline)
	seen := map[string]bool{}
	var out []string
	buf := make([]byte, 64<<10)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			break
		}
		for _, x := range xaddrs(buf[:n]) {
			if !seen[x] {
				seen[x] = true
				out = append(out, x)
			}
		}
	}
	sort.Strings(out)

	return out
}

// xaddrs pulls the XAddrs out of a ProbeMatch without caring about the
// namespaces.
func xaddrs(b []byte) []string {
	dec := xml.NewDecoder(strings.NewReader(string(b)))
	var out []string
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "XAddrs" {
			var v string
			if err := dec.DecodeElement(&v, &se); err == nil {
				out = append(out, strings.Fields(v)...)
			}
		}
	}

	return out
}

// WriteSkeleton prints the scan as a configuration skeleton (§38.4).
func (s Scan) WriteSkeleton(w io.Writer) {
	fmt.Fprintln(w, "# what this host can see; edit into `producer.sources`")
	if len(s.Encoders) > 0 {
		fmt.Fprintf(w, "# encoders: %s\n", strings.Join(s.Encoders, ", "))
	} else {
		fmt.Fprintln(w, "# encoders: none found (is ffmpeg installed?)")
	}
	for _, a := range s.Audio {
		fmt.Fprintf(w, "# audio: %s\n", a)
	}
	fmt.Fprintln(w, "producer:")
	fmt.Fprintln(w, "  sources:")
	for i, c := range s.Cameras {
		fmt.Fprintf(w, "    - alias: cam-%02d          # %s\n", i+1, c.Name)
		fmt.Fprintf(w, "      input: v4l2:%s\n", c.Device)
		for _, m := range c.Modes {
			fmt.Fprintf(w, "      # %s\n", m)
		}
		if c.H264 {
			fmt.Fprintln(w, "      format: h264         # the camera encodes; remuxed with -c copy")
		} else {
			fmt.Fprintln(w, "      format: mjpeg")
			fmt.Fprintln(w, "      encoder: auto")
		}
		fmt.Fprintln(w, "      size: 1920x1080")
		fmt.Fprintln(w, "      fps: 30")
		fmt.Fprintln(w, "      max_bitrate: auto")
	}
	for _, x := range s.Onvif {
		fmt.Fprintf(w, "    - alias: onvif           # %s\n", x)
		fmt.Fprintln(w, "      input: rtsp://<user>:<pass>@<camera>/stream1   # from the camera's media profile")
		fmt.Fprintln(w, "      format: h264")
		fmt.Fprintln(w, "      max_bitrate: 4Mbps   # the camera's configured limit × 1.05 plus audio")
	}
	if len(s.Cameras) == 0 && len(s.Onvif) == 0 {
		fmt.Fprintln(w, "    []   # no camera found")
	}
}

// ProbeResult is what a 30-second run of one source showed (§38.4).
type ProbeResult struct {
	Alias     string
	Encoder   string
	FrameRate float64
	Bitrate   int64
	Keyframe  time.Duration
	Cpu       float64
	Temp      float64
	Err       string
}

// Probe runs each source for `d` and reports what it sustains.
func Probe(ctx context.Context, ffmpeg string, sources []SourceConfig, d time.Duration, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) []ProbeResult {
	var out []ProbeResult
	for _, sc := range sources {
		res := ProbeResult{Alias: sc.Alias}
		ceiling := sc.MaxBitrate
		if ceiling == 0 {
			ceiling = StartingCeiling(sc.Size, sc.Fps, sc.Format)
		}
		cap := &Capture{Ffmpeg: ffmpeg, Source: sc, Log: slogOf(log), Profile: func() (int64, time.Duration) { return ceiling, 2 * time.Second }}
		pctx, cancel := context.WithTimeout(ctx, d)
		r := NewReader(nil)
		var frames, bytes int64
		var keys []time.Time
		start := time.Now()
		cap.Run(pctx, func(rd io.Reader) {
			r = NewReader(rd)
			var p Packet
			for {
				if err := r.Next(&p); err != nil {
					return
				}
				bytes += PacketSize
				if r.IsVideoFrame(&p) {
					frames++
				}
				if r.IsKeyframe(&p) {
					keys = append(keys, time.Now())
				}
			}
		})
		cancel()
		el := time.Since(start).Seconds()
		if el > 0 {
			res.FrameRate = float64(frames) / el
			res.Bitrate = int64(float64(bytes) * 8 / el)
		}
		if len(keys) > 1 {
			res.Keyframe = keys[len(keys)-1].Sub(keys[0]) / time.Duration(len(keys)-1)
		}
		res.Encoder = cap.Encoder
		res.Cpu = cpuLoad()
		res.Temp = socTemperature()
		res.Err = cap.LastError()
		out = append(out, res)
	}

	return out
}
