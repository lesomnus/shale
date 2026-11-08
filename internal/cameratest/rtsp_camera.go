package cameratest

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

type RTSPOptions struct {
	ListenAddress      string // Explicit test interface, e.g. 127.0.0.2:0; no interface changes.
	Path               string
	Profile            camera.MediaProfile
	Username, Password string
	FFmpeg             string
	SetupTimeout       time.Duration
}

type rtspMedia struct {
	units    [][][]byte
	sps, pps []byte
	fps      float64
	profile  camera.MediaProfile
}

// RTSPCamera swaps immutable encoded media for new sessions. Existing sessions
// keep their original bitstream until Disconnect, Rebind or client teardown.
type RTSPCamera struct {
	mu       sync.Mutex
	options  RTSPOptions
	media    *rtspMedia
	endpoint *rtspEndpoint
	closed   bool
}

type rtspEndpoint struct {
	listener    net.Listener
	done        chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]bool
	wg          sync.WaitGroup
	once        sync.Once
}

func ServeRTSPCamera(t testing.TB, options RTSPOptions) *RTSPCamera {
	t.Helper()
	if options.ListenAddress == "" {
		options.ListenAddress = "127.0.0.1:0"
	}
	if options.Path == "" {
		options.Path = "/camera"
	}
	if !strings.HasPrefix(options.Path, "/") || strings.ContainsAny(options.Path, "?#\r\n ") {
		t.Fatal("invalid synthetic RTSP path")
	}
	if options.FFmpeg == "" {
		options.FFmpeg = "ffmpeg"
	}
	if options.SetupTimeout == 0 {
		options.SetupTimeout = 15 * time.Second
	}
	if options.SetupTimeout <= 0 || options.SetupTimeout > time.Minute {
		t.Fatal("invalid synthetic media setup bound")
	}
	c := &RTSPCamera{options: options}
	if err := c.SetProfile(context.Background(), options.Profile); err != nil {
		t.Fatalf("prepare synthetic media: %v", err)
	}
	if _, err := c.Rebind(options.ListenAddress); err != nil {
		t.Fatalf("bind synthetic RTSP: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func normalizeMedia(p camera.MediaProfile) (camera.MediaProfile, error) {
	if p.Codec == "" {
		p.Codec = "h264"
	}
	if p.Codec != "h264" || p.Width < 16 || p.Width > 1920 || p.Height < 16 || p.Height > 1080 || p.Width%2 != 0 || p.Height%2 != 0 || p.FPS < 1 || p.FPS > 60 || math.IsNaN(p.FPS) || math.IsInf(p.FPS, 0) || p.Bitrate < 32_000 || p.Bitrate > 8_000_000 {
		return p, fmt.Errorf("unsupported synthetic H264 profile")
	}
	if p.GOP == 0 {
		p.GOP = int(math.Round(p.FPS))
	}
	// The bounded clip is GOP-aligned and looped; reject longer GOPs rather
	// than growing a test capture without a bound.
	if p.GOP < 1 || p.GOP > int(math.Ceil(2*p.FPS)) {
		return p, fmt.Errorf("unsupported synthetic GOP")
	}
	if p.Token == "" {
		p.Token = "main"
	}
	if p.Name == "" {
		p.Name = "Main"
	}
	return p, nil
}

func (c *RTSPCamera) SetProfile(parent context.Context, p camera.MediaProfile) error {
	p, err := normalizeMedia(p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, c.options.SetupTimeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "shale-fixture-media-")
	if err != nil {
		return fmt.Errorf("prepare synthetic media directory")
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "stream.h264")
	rate := fmt.Sprint(p.Bitrate)
	frames := int(math.Ceil(2 * p.FPS))
	frames = ((frames + p.GOP - 1) / p.GOP) * p.GOP
	args := []string{"-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", fmt.Sprintf("testsrc2=s=%dx%d:r=%g", p.Width, p.Height, p.FPS), "-frames:v", fmt.Sprint(frames), "-an", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-tune", "zerolatency", "-g", fmt.Sprint(p.GOP), "-bf", "0", "-b:v", rate, "-minrate", rate, "-maxrate", rate, "-bufsize", fmt.Sprint(2 * p.Bitrate), "-x264-params", "repeat-headers=1:aud=1:nal-hrd=cbr:scenecut=0", "-f", "h264", "-y", path}
	cmd := exec.CommandContext(ctx, c.options.FFmpeg, args...)
	cmd.WaitDelay = time.Second
	if err = cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("synthetic H264 encoder failed")
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 6<<20 {
		return fmt.Errorf("invalid synthetic encoded media")
	}
	units, sps, pps := accessUnits(raw)
	if len(units) < 2 || len(sps) == 0 || len(pps) == 0 {
		return fmt.Errorf("invalid synthetic access units")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("synthetic RTSP is closed")
	}
	c.media = &rtspMedia{units: units, sps: sps, pps: pps, fps: p.FPS, profile: p}
	return nil
}

func (c *RTSPCamera) URL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.endpoint == nil {
		return ""
	}
	return "rtsp://" + c.endpoint.listener.Addr().String() + c.options.Path
}
func (c *RTSPCamera) Profile() camera.MediaProfile {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.media.profile
	if c.endpoint != nil {
		p.StreamURI = "rtsp://" + c.endpoint.listener.Addr().String() + c.options.Path
	}
	return p
}

// Rebind closes the previous listener and every old session before returning.
// It changes only test sockets; it does not configure DHCP, routes or interfaces.
func (c *RTSPCamera) Rebind(address string) (string, error) {
	lis, err := net.Listen("tcp4", address)
	if err != nil {
		return "", err
	}
	e := &rtspEndpoint{listener: lis, done: make(chan struct{}), connections: map[net.Conn]bool{}}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		lis.Close()
		return "", fmt.Errorf("synthetic RTSP is closed")
	}
	old := c.endpoint
	c.endpoint = e
	c.mu.Unlock()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			c.mu.Lock()
			media := c.media
			options := c.options
			c.mu.Unlock()
			e.mu.Lock()
			select {
			case <-e.done:
				e.mu.Unlock()
				conn.Close()
				continue
			default:
			}
			e.connections[conn] = true
			e.wg.Add(1)
			e.mu.Unlock()
			go func() {
				defer e.wg.Done()
				defer conn.Close()
				defer func() { e.mu.Lock(); delete(e.connections, conn); e.mu.Unlock() }()
				serveRTSPMedia(conn, lis.Addr().String(), options.Path, media, e.done, options.Username, options.Password)
			}()
		}
	}()
	if old != nil {
		old.close()
	}
	return c.URL(), nil
}

func (e *rtspEndpoint) disconnect() {
	e.mu.Lock()
	for conn := range e.connections {
		conn.Close()
	}
	e.mu.Unlock()
}
func (e *rtspEndpoint) close() {
	e.once.Do(func() { close(e.done); e.listener.Close(); e.disconnect(); e.wg.Wait() })
}
func (c *RTSPCamera) Disconnect() {
	c.mu.Lock()
	e := c.endpoint
	c.mu.Unlock()
	if e != nil {
		e.disconnect()
	}
}

func (c *RTSPCamera) Sessions() int {
	c.mu.Lock()
	e := c.endpoint
	c.mu.Unlock()
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.connections)
}
func (c *RTSPCamera) Close() {
	c.mu.Lock()
	c.closed = true
	e := c.endpoint
	c.mu.Unlock()
	if e != nil {
		e.close()
	}
}
