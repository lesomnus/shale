package cp

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/source"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Applications uses the Producer's authenticated connection. Publication and
// catalog reads confer no execution authority on another principal.
type Applications struct {
	client  api.CameraAssignmentServiceClient
	scope   source.Scope
	timeout time.Duration
}

func NewApplications(conn grpc.ClientConnInterface, scope source.Scope) *Applications {
	return &Applications{client: api.NewCameraAssignmentServiceClient(conn), scope: scope, timeout: 5 * time.Second}
}

func applyTarget(a source.Application) (*api.CameraApplyTarget, error) {
	if err := source.ValidateApplication(a); err != nil {
		return nil, err
	}
	assignment, _ := pdid.Parse(a.Revision.AssignmentID)
	binding, _ := pdid.Parse(a.BindingID)
	id, _ := pdid.Parse(a.SourceID)
	return api.CameraApplyTarget_builder{AssignmentId: assignment.Bytes(), BindingId: binding.Bytes(), SourceId: id.Bytes(), AssignmentGeneration: a.Revision.Generation, CatalogRevision: a.Revision.Catalog, SpecRevision: a.SpecRevision}.Build(), nil
}

func (p *Applications) Begin(ctx context.Context, a source.Application, intent string) (source.ApplyTicket, error) {
	target, err := applyTarget(a)
	if err != nil || a.Scope != p.scope || len(intent) < 1 || len(intent) > 128 {
		return source.ApplyTicket{}, source.ErrInvalid
	}
	call, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	state, err := p.client.Application(call, api.CameraApplicationRequest_builder{Target: target}.Build())
	if err != nil {
		return source.ApplyTicket{}, rpcError(ctx, err)
	}
	v, err := p.client.BeginApply(call, api.CameraBeginApplyRequest_builder{Target: target, ExpectedSequence: state.GetStarting().GetSequence(), IntentKey: intent}.Build())
	if err != nil {
		return source.ApplyTicket{}, rpcError(ctx, err)
	}
	if !proto.Equal(v.GetTarget(), target) || v.GetSequence() < 1 || len(v.GetCaptureEpoch()) != 32 || v.GetIntentKey() != intent {
		return source.ApplyTicket{}, source.ErrInvalid
	}
	if _, err := hex.DecodeString(v.GetCaptureEpoch()); err != nil {
		return source.ApplyTicket{}, source.ErrInvalid
	}
	return source.ApplyTicket{Application: a, Sequence: v.GetSequence(), Epoch: v.GetCaptureEpoch()}, nil
}

func (p *Applications) Acknowledge(ctx context.Context, proof source.AppliedProof) error {
	target, err := applyTarget(proof.Application)
	if err != nil || proof.Scope != p.scope || proof.Sequence < 1 || len(proof.Epoch) != 32 {
		return source.ErrInvalid
	}
	media := proof.Media
	request := api.CameraAppliedRequest_builder{Target: target, Sequence: proof.Sequence, CaptureEpoch: proof.Epoch, Media: api.CameraRuntimeMedia_builder{Profile: api.CameraCaptureProfile_builder{Codec: media.Codec, Width: int32(media.Width), Height: int32(media.Height), Fps: media.FPS, MaxBitrateBps: media.Bitrate, KeyframeIntervalMs: media.KeyframeInterval.Milliseconds()}.Build(), FirstMediaAt: timestamppb.New(media.FirstMediaAt), LastMediaAt: timestamppb.New(media.LastMediaAt), Frames: media.Frames, Keyframes: media.Keyframes}.Build()}
	if ref := proof.UsedCredentialRef; ref != nil {
		request.UsedCredential = api.CameraCredentialReference_builder{Provider: ref.Provider, Id: ref.ID, Version: ref.Version, Purpose: ref.Purpose}.Build()
	}
	call, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	v, err := p.client.Applied(call, request.Build())
	if err != nil {
		return rpcError(ctx, err)
	}
	if !proto.Equal(v.GetTarget(), target) || v.GetSequence() != proof.Sequence || v.GetCaptureEpoch() != proof.Epoch || v.GetDateApplied() == nil {
		return source.ErrInvalid
	}
	return nil
}

var _ source.ApplyController = (*Applications)(nil)
