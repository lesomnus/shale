package e2e_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
)

// TestLiveFanOut is E6's last line: a hundred viewers on one camera of
// 4 Mbps hold on one relay (§39.5). The camera is a recording of that
// rate played as a camera would send it; a hundred receive-only WebRTC
// viewers, as many browsers, open their WHEP sessions at once, and each
// must have its first frame, then keep receiving the camera at its rate
// for a while with hardly a packet lost and no stall, while the relay
// keeps the producer attached and the source fed. Every viewer is the
// same person here, so the relay's viewers_per_actor (16) is raised for
// the test; max_viewers (500) is left as it is.
//
// The numbers are logged: how long the viewers took to come up and to
// reach the live edge, what each received in the window, packets lost,
// and the longest a viewer went without a packet. No frame after a
// viewer's first may come broken, joining or after: the relay sends the
// group of pictures at a few times real time rather than at once, and
// answers the viewers' NACKs (§39.4).
func TestLiveFanOut(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	const viewers = 100
	const window = 15 * time.Second
	c := start(t, func(cfg *cmd.Config) { cfg.Relay.ViewersPerActor = viewers })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admin := c.dial("@acme/admin")

	camera := fourMbps(t)
	index := indexFrames(t, camera)
	t.Logf("the camera: %d frames, %.2f Mbps of video", index.n, index.bitrate()/1e6)
	require.GreaterOrEqual(t, index.bitrate(), 4e6, "a camera of 4 Mbps")
	_, live, _ := liveProducer(t, ctx, c, admin, producer.SourceConfig{Alias: "lobby", Input: "raw:" + camera, Fps: 30, MaxBitrate: 5_000_000, Format: "h264"})
	relays := api.NewRelayServiceClient(c.dialCluster("@cluster/ops"))
	awaitActive(t, ctx, relays, 1, "always: the camera is on the relay")
	time.Sleep(2500 * time.Millisecond)

	// A hundred viewers, all at once.
	vs := make([]*fanViewer, viewers)
	var wg sync.WaitGroup
	for i := range vs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vs[i] = openFanViewer(live)
		}()
	}
	wg.Wait()
	defer func() {
		for _, v := range vs {
			if v.pc != nil {
				v.pc.Close()
			}
		}
	}()
	for i, v := range vs {
		require.NoError(t, v.err, "viewer %d opens its session", i+1)
	}
	require.Eventually(t, func() bool {
		for _, v := range vs {
			if v.snapshot().first.IsZero() {
				return false
			}
		}

		return true
	}, 20*time.Second, 100*time.Millisecond, "every viewer has its first frame")
	var joins []time.Duration
	for _, v := range vs {
		joins = append(joins, v.snapshot().first.Sub(v.offered))
	}
	t.Logf("%d viewers up, first frame after the offer: %s", viewers, spread(joins))

	// The window: what each viewer receives from here on, after the group
	// of pictures each got on joining has passed.
	time.Sleep(4 * time.Second)
	before := make([]fanStats, viewers)
	var edges []time.Duration
	var joinLost, joinBroken int64
	for i, v := range vs {
		before[i] = v.snapshot()
		v.resetGap()
		v.mu.Lock()
		edges = append(edges, liveEdge(v.arrivals))
		v.mu.Unlock()
		joinLost += before[i].lost
		joinBroken += before[i].broken
	}
	t.Logf("joining: first frame to the live edge %s; %d packets lost for good, %d frames after the first not whole, all viewers together",
		spread(edges), joinLost, joinBroken)
	cpu0 := cpuTime()
	begun := time.Now()
	time.Sleep(window)
	took := time.Since(begun).Seconds()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("the process, relay, producer and viewers together, in the window: %.2f CPUs; heap in use %d MiB",
		(cpuTime()-cpu0).Seconds()/took, ms.HeapInuse>>20)

	var rates []float64
	var gaps []time.Duration
	var packets, lost, bytes, broken int64
	for i, v := range vs {
		s := v.snapshot()
		d := s.minus(before[i])
		rates = append(rates, float64(d.bytes*8)/took)
		gaps = append(gaps, s.gap)
		packets += d.packets
		lost += d.lost
		bytes += d.bytes
		broken += d.broken
	}
	slices.Sort(rates)
	t.Logf("in %.0f s: %.0f Mbps to %d viewers together; per viewer min %.2f, median %.2f, max %.2f Mbps",
		took, float64(bytes*8)/took/1e6, viewers, rates[0]/1e6, rates[len(rates)/2]/1e6, rates[len(rates)-1]/1e6)
	t.Logf("packets %d, lost for good %d (%.3f%%), frames not whole %d; the longest a viewer went without a packet: %s",
		packets, lost, 100*float64(lost)/float64(packets+lost), broken, spread(gaps))

	require.Zero(t, joinBroken+broken, "every frame after the first comes whole, joining or not: the group of pictures paced, and what was lost sent again")

	st := relayStatus(t, ctx, relays)
	require.EqualValues(t, viewers, st.GetViewers(), "the relay still has every session")
	require.EqualValues(t, 1, st.GetAttachedProducers(), "and the producer attached")
	require.EqualValues(t, 1, st.GetActiveSources(), "and the camera fed")
	for i, v := range vs {
		s := v.snapshot().minus(before[i])
		rate := float64(s.bytes*8) / took
		require.Greater(t, rate, 0.9*index.bitrate(), "viewer %d receives the camera at its rate", i+1)
		require.LessOrEqual(t, float64(s.lost), 0.01*float64(s.packets+s.lost), "viewer %d loses hardly a packet", i+1)
		require.Less(t, v.snapshot().gap, time.Second, "viewer %d never stalls", i+1)
	}
}

// cpuTime is the CPU time this process has used, user and system.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// fourMbps makes a recording of a camera at 4 Mbps of H.264, 720p30 with
// a keyframe every two seconds, fragmented as the producer's capture
// fragments (§38.2), for a `raw:` source. Noise over a moving test
// pattern keeps the encoder at its rate, where a still picture would
// come to a fraction of it.
func fourMbps(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "4mbps.mp4")
	args := []string{
		"-v", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
		"-vf", "noise=alls=30:allf=t", "-t", "8",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-b:v", "4500k", "-maxrate", "4500k", "-bufsize", "2250k",
		"-g", "60", "-keyint_min", "60", "-sc_threshold", "0", "-an",
	}
	args = append(args, producer.RemuxMuxArgs(200*time.Millisecond)...)
	b, err := exec.Command("ffmpeg", append(args, out)...).CombinedOutput()
	require.NoError(t, err, string(b))

	return out
}

// fanViewer is one receive-only viewer that counts what it gets.
type fanViewer struct {
	pc      *webrtc.PeerConnection
	offered time.Time
	err     error

	mu    sync.Mutex
	stats fanStats
	last  time.Time
	// arrivals are the frames from the first decodable one on, for the
	// first seconds: when each came and its timestamp (liveEdge).
	arrivals []arrival
}

// fanStats is what a viewer received: video RTP packets, their payload
// bytes, packets that never came, frames that came whole, when the first
// decodable one did, and the longest wait between two packets since the
// gap was last reset.
type fanStats struct {
	packets, bytes, lost int64
	frames               int64
	// broken is the frames after the first decodable one that did not
	// come whole.
	broken int64
	first  time.Time
	gap    time.Duration
}

func (s fanStats) minus(o fanStats) fanStats {
	return fanStats{packets: s.packets - o.packets, bytes: s.bytes - o.bytes, lost: s.lost - o.lost, frames: s.frames - o.frames, broken: s.broken - o.broken, first: s.first, gap: s.gap}
}

func (v *fanViewer) snapshot() fanStats {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.stats
}

func (v *fanViewer) resetGap() {
	v.mu.Lock()
	v.stats.gap = 0
	v.mu.Unlock()
}

// openFanViewer opens a WHEP session and counts from then on.
func openFanViewer(live *api.LiveSource) *fanViewer {
	v := &fanViewer{}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		v.err = err
		return v
	}
	v.pc = pc
	onVideo(pc, func(p *rtp.Packet, at time.Time, asm *assembler) {
		lost := asm.lost
		asm.push(p, at, func(u accessUnit) {
			v.mu.Lock()
			if u.whole {
				v.stats.frames++
			}
			if v.stats.first.IsZero() && u.decodable() {
				v.stats.first = u.at
			}
			if !v.stats.first.IsZero() {
				if len(v.arrivals) > 0 && !u.whole {
					v.stats.broken++
				}
				if u.at.Sub(v.stats.first) < 4*time.Second {
					v.arrivals = append(v.arrivals, arrival{u.at, u.ts})
				}
			}
			v.mu.Unlock()
		})
		v.mu.Lock()
		v.stats.packets++
		v.stats.bytes += int64(len(p.Payload))
		v.stats.lost += asm.lost - lost
		if !v.last.IsZero() {
			v.stats.gap = max(v.stats.gap, at.Sub(v.last))
		}
		v.last = at
		v.mu.Unlock()
	})
	v.offered = time.Now()
	if _, _, err := offerWhep(pc, live); err != nil {
		v.err = fmt.Errorf("WHEP: %w", err)
	}

	return v
}
