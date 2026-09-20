package producer

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"
)

// The live helper (§38.7): browsers play no audio but Opus over WebRTC,
// and a camera sends AAC or G.711. While such a camera is watched, the
// producer runs one ffmpeg that takes the tee, copies the video and encodes
// the audio as Opus, and sends its output to the relay in place of the
// bytes it stores. Nothing is decoded in this binary, nothing runs while
// nobody watches, and the recording keeps the camera's audio.

const (
	// helperQueue bounds the chunks waiting for the helper's input; bytes
	// beyond it are dropped, never held, so a stalled helper cannot slow
	// the capture loop.
	helperQueue = 64
	// helperGrace is how long a helper gets to flush after its input ends.
	helperGrace = 2 * time.Second
	// helperTries is how many times a helper that exits on its own is
	// started again before the tap sends the camera's bytes as they are.
	helperTries = 3
)

// needsOpus says whether a stream's audio has to be transcoded for the
// relay: there is audio, and it is not Opus.
func needsOpus(st Streams) bool { return st.HasAudio() && !st.AudioOpus }

// helper is one running transcoder.
type helper struct {
	id     pdid.Id
	cancel context.CancelFunc
	done   chan struct{}

	// in carries the tee's chunks to the process; closed says it was
	// closed, since the tap writes and the stop close from different
	// goroutines.
	mu     sync.Mutex
	in     chan []byte
	closed bool
	grace  sync.Once
}

// errNoFfmpeg is a host without ffmpeg: no helper, ever.
var errNoFfmpeg = errors.New("no ffmpeg for the live helper")

// startHelper runs a helper for a source. It is called with the link's
// lock held and takes it never; the caller decides what to do without one.
func (l *relayLink) startHelper(id pdid.Id, s *source) (*helper, error) {
	ctx := l.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ffmpeg := l.p.cfg.Ffmpeg
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	if _, err := exec.LookPath(ffmpeg); err != nil {
		return nil, errNoFfmpeg
	}

	hctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(hctx, ffmpeg, LiveArgs(s.cfg.audioBitrate())...)
	cmd.Env = append(os.Environ(), "AV_LOG_FORCE_NOCOLOR=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	// The output is a pipe of our own: Wait closes the pipes exec makes
	// the moment the process exits, with the last packets still in them.
	pr, pw, err := os.Pipe()
	if err != nil {
		cancel()
		return nil, err
	}
	cmd.Stdout = pw
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		pr.Close()
		pw.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		pr.Close()
		pw.Close()
		return nil, err
	}
	pw.Close()
	h := &helper{id: id, in: make(chan []byte, helperQueue), cancel: cancel, done: make(chan struct{})}
	l.p.log.Info("live helper started: the audio goes as Opus", "source", s.cfg.Alias, "bitrate", s.cfg.audioBitrate())

	// The tee into the helper.
	go func() {
		defer stdin.Close()
		for b := range h.in {
			if _, err := stdin.Write(b); err != nil {
				break
			}
		}
	}()
	// The helper's output to the relay, whole packets at a time, until the
	// process is gone and the pipe is drained.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		defer pr.Close()
		var buf []byte
		chunk := make([]byte, tapFlush)
		for {
			n, err := pr.Read(chunk)
			if n > 0 {
				buf = append(buf, chunk[:n]...)
				whole := len(buf) - len(buf)%PacketSize
				if whole > 0 {
					l.sendData(id, buf[:whole])
					buf = append(buf[:0], buf[whole:]...)
				}
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			if line := strings.TrimSpace(sc.Text()); line != "" {
				l.p.log.Info("live helper", "source", s.cfg.Alias, "line", line)
			}
		}
	}()
	go func() {
		err := cmd.Wait()
		<-drained
		close(h.done)
		l.helperExited(id, h, err, s.cfg.Alias)
	}()

	return h, nil
}

// write hands a chunk to the helper without waiting; false when it was
// dropped because the helper is behind, or is gone.
func (h *helper) write(b []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	select {
	case h.in <- b:
		return true
	default:
		return false
	}
}

// close ends the helper's input; it exits when it has flushed, and is
// killed after helperGrace if it has not.
func (h *helper) close() {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.in)
	}
	h.mu.Unlock()
	h.grace.Do(func() {
		go func() {
			select {
			case <-h.done:
			case <-time.After(helperGrace):
				h.cancel()
			}
		}()
	})
}

// helperExited is the end of a helper's process. One that was stopped on
// purpose is already gone from its tap; one that died is started again at
// the next keyframe, and after helperTries the tap sends the bytes as they
// are, silent for the viewer but alive.
func (l *relayLink) helperExited(id pdid.Id, h *helper, err error, alias string) {
	h.close()
	h.cancel()
	l.mu.Lock()
	t, ok := l.active[id]
	if !ok || t.h != h {
		l.mu.Unlock()
		return
	}
	t.h = nil
	t.keyed = false
	t.fails++
	t.raw = t.fails >= helperTries
	raw := t.raw
	l.mu.Unlock()
	msg := "no error"
	if err != nil {
		msg = err.Error()
	}
	if raw {
		l.p.log.Warn("live helper keeps exiting; sending the camera's bytes as they are", "source", alias, "err", msg)
	} else {
		l.p.log.Warn("live helper exited; starting it again at the next keyframe", "source", alias, "err", msg)
	}
}
