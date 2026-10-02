//go:build unix

package producer

import (
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// stopper ends a capture process the way it asks to be ended (§38.3): an
// interrupt to its process group, so a shell's pipeline hears it too, and
// a kill to the group if it is still there after the grace. A stage that
// reads another's output is given no signal and ends at the end of its
// input, after the stage before it flushed. A GStreamer pipeline killed
// mid-stream can leave the Pi's codec wedged until a reboot; interrupted
// (`gst-launch -e`) it drains and closes it.
type stopper struct {
	mu    sync.Mutex
	timer *time.Timer
	done  bool
}

// stopGracefully arranges cmd's stop, before it starts; `interrupt` says
// whether it is sent one. done is called once Wait returned, so the kill
// never reaches a process group that ended and whose number was reused.
func stopGracefully(cmd *exec.Cmd, interrupt bool, grace time.Duration) (done func()) {
	s := &stopper{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		pg := -cmd.Process.Pid
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.done {
			return nil
		}
		s.timer = time.AfterFunc(grace, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.done {
				syscall.Kill(pg, syscall.SIGKILL)
			}
		})
		if !interrupt {
			return nil
		}

		return syscall.Kill(pg, syscall.SIGINT)
	}
	// Wait stops waiting for the pipes this long after the stop, should a
	// process the group kill missed hold them.
	cmd.WaitDelay = grace + 2*time.Second

	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.done = true
		if s.timer != nil {
			s.timer.Stop()
		}
	}
}
