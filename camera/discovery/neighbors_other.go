//go:build !linux

package discovery

import (
	"context"

	"github.com/lesomnus/shale/camera"
)

type SystemNeighbors struct{ IPCommand string }

func (SystemNeighbors) Neighbors(context.Context, string) ([]Neighbor, error) {
	return nil, camera.ErrUnsupported
}
