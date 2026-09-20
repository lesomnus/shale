package core

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// Live viewing (§39) lands with the relay: assignment, publish and view
// tokens, and the Live RPCs. Until then the CP has no relay to assign.

// relayAssignment is the relay a producer sends to, or nil when none is
// assigned or available.
func (s Core) relayAssignment(ctx context.Context, producer pdid.Id, set *api.Set) (*api.RelayAssignment, error) {
	return nil, nil
}

func (s coreSet) Live(ctx context.Context, req *api.SetLiveRequest) (*api.SetLiveResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	return nil, status.Error(codes.Unimplemented, "live viewing arrives with the relay")
}

func (s coreSource) Live(ctx context.Context, req *api.SourceLiveRequest) (*api.SourceLiveResponse, error) {
	if _, err := actor(ctx); err != nil {
		return nil, err
	}

	return nil, status.Error(codes.Unimplemented, "live viewing arrives with the relay")
}

// starvation folds a producer's per-source report into the source rows
// (§38.5) and answers suggestions for ceilings that look too low.
func (s Core) starvation(ctx context.Context, p *api.Producer, reports []*api.SourceReport) ([]*api.Suggestion, error) {
	return nil, nil
}
