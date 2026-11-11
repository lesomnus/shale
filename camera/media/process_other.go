//go:build !unix

package media

import (
	"os/exec"
	"time"
)

func boundedProcess(cmd *exec.Cmd) { cmd.WaitDelay = 250 * time.Millisecond }
