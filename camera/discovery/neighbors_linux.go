//go:build linux

package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"time"

	"github.com/lesomnus/shale/camera"
)

// SystemNeighbors uses iproute2's ordinary, read-only neighbor query. The
// command is operator-owned configuration, never taken from a discovery job.
type SystemNeighbors struct{ IPCommand string }

type neighborBuffer struct{ bytes.Buffer }

func (b *neighborBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 128<<10 {
		return 0, camera.ErrInvalid
	}
	return b.Buffer.Write(p)
}

func (s SystemNeighbors) Neighbors(ctx context.Context, iface string) ([]Neighbor, error) {
	if !exactInterface.MatchString(iface) || iface == "lo" {
		return nil, camera.ErrInvalid
	}
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	command := s.IPCommand
	if command == "" {
		command = "ip"
	}
	cmd := exec.CommandContext(call, command, "-j", "neighbor", "show", "dev", iface)
	var output neighborBuffer
	cmd.Stdout, cmd.Stderr = &output, io.Discard
	if err := cmd.Run(); err != nil {
		if call.Err() != nil {
			return nil, call.Err()
		}
		return nil, camera.ErrInvalid
	}
	var rows []struct {
		Address string   `json:"dst"`
		MAC     string   `json:"lladdr"`
		States  []string `json:"state"`
	}
	if err := json.Unmarshal(output.Bytes(), &rows); err != nil || len(rows) > 1024 {
		return nil, camera.ErrInvalid
	}
	out := make([]Neighbor, len(rows))
	for i, row := range rows {
		out[i] = Neighbor{Address: row.Address, MAC: row.MAC, States: row.States}
	}
	return out, nil
}
