//go:build !unix

package producer

import (
	"os/exec"
	"time"
)

// stopGracefully is a kill where there are no process groups to interrupt.
func stopGracefully(cmd *exec.Cmd, interrupt bool, grace time.Duration) (done func()) {
	cmd.WaitDelay = grace

	return func() {}
}
