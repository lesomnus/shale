package cameraworker

import (
	"context"
	"math"
	"strconv"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	publication "github.com/lesomnus/shale/provision/source"
	catalog "github.com/lesomnus/shale/source"
	"google.golang.org/protobuf/proto"
)

// JobCommitter publishes through the current Worker lease. It can be wrapped by
// provision/source.Publisher to notify a separately authenticated Producer's
// memory catalog only after the durable CP transaction succeeds.
type JobCommitter struct{ Execution *Execution }

var _ publication.Committer = JobCommitter{}

func publicationReceipt(w *api.CameraWorkerWork, operation string) publication.Receipt {
	j, b, a := w.GetJob(), w.GetBinding(), w.GetAssignment()
	return publication.Receipt{Binding: provision.BindingState{BindingID: b.GetLogicalKey(), Alias: b.GetSourceAlias(), SourceID: cameraID(b.GetSource().GetId()), DeploymentID: operation, DeployedAt: j.GetDatePublished().AsTime()}, Revision: catalog.Revision{AssignmentID: cameraID(a.GetId()), Generation: a.GetGeneration(), Catalog: j.GetPublishedRevision()}}
}

func (c JobCommitter) Commit(ctx context.Context, d provision.Deployment) (publication.Receipt, error) {
	if c.Execution == nil || d.OperationID == "" {
		return publication.Receipt{}, camera.ErrInvalid
	}
	w, err := c.Execution.Check(ctx)
	if err != nil {
		return publication.Receipt{}, err
	}
	j, b := w.GetJob(), w.GetBinding()
	q := jobRequest(w)
	if d.Request.ID != q.ID || d.Request.BindingID != q.BindingID || d.Request.Scope != q.Scope || d.Credential.Scope != q.Scope || d.Credential.Purpose != camera.AuthStreaming || q.StreamingRef == nil || d.Credential != *q.StreamingRef || d.Previous.Alias != b.GetSourceAlias() {
		return publication.Receipt{}, camera.ErrForbidden
	}
	r, err := cameraprogress.Decode(j.GetObserved().GetProgress())
	if err != nil || r.Verified == nil || r.Plan == nil || r.Stage != provision.Deploying || d.Profile.StreamURI != r.Verified.Profile.StreamURI {
		return publication.Receipt{}, camera.ErrInvalid
	}
	if r.Binding.DeploymentID != "" && r.Binding.DeploymentID != d.OperationID {
		return publication.Receipt{}, camera.ErrConflict
	}
	o, err := runObservation(r, j.GetObserved())
	if err != nil {
		return publication.Receipt{}, err
	}
	profile := captureProfile(d.Profile)
	profile.SetKeyframeIntervalMs(o.GetEffective().GetKeyframeIntervalMs())
	spec := api.CameraCaptureSpec_builder{BindingId: b.GetId(), SourceId: b.GetSource().GetId(), Alias: b.GetSourceAlias(), Input: d.Profile.StreamURI, Profile: profile, Credential: j.GetInputs().GetStreamingCredential(), Name: d.Request.Name}.Build()
	if j.GetDatePublished() != nil {
		// A previous Publish may have committed before its response was lost.
		// New independent probes have new timestamps; reuse its durable receipt
		// instead of resending the old operation with different evidence bytes.
		prior := b.GetDesired()
		if prior.GetProfile() == nil {
			return publication.Receipt{}, camera.ErrConflict
		}
		if j.GetPublishedRevision() != prior.GetRevision() || !b.GetCollect() || prior.GetInput() != spec.GetInput() || !publishedProfileMatches(r.Plan.After, d.Profile, prior.GetProfile()) || !proto.Equal(prior.GetCredential(), spec.GetCredential()) {
			return publication.Receipt{}, camera.ErrConflict
		}
		return publicationReceipt(w, d.OperationID), nil
	}
	if !proto.Equal(profile, o.GetEffective()) {
		return publication.Receipt{}, camera.ErrConflict
	}
	key := "publish/" + strconv.FormatUint(r.Version, 10) + "/" + provision.Key(d.OperationID, stateHash(o))
	_, err = c.Execution.Publish(ctx, key, spec, o)
	if err != nil {
		return publication.Receipt{}, privateExecutionError{err}
	}
	return publicationReceipt(c.Execution.Snapshot(), d.OperationID), nil
}

func publishedProfileMatches(configured, actual camera.MediaProfile, prior *api.CameraCaptureProfile) bool {
	if prior == nil || actual.Codec != prior.GetCodec() || actual.Width != int(prior.GetWidth()) || actual.Height != int(prior.GetHeight()) {
		return false
	}
	if configured.FPS > 0 {
		if actual.FPS != configured.FPS || prior.GetFps() != configured.FPS {
			return false
		}
	} else if actual.FPS <= 0 || math.Abs(actual.FPS-prior.GetFps()) > math.Max(0.5, prior.GetFps()*0.05) {
		return false
	}
	if configured.Bitrate > 0 {
		return actual.Bitrate == configured.Bitrate && prior.GetMaxBitrateBps() == configured.Bitrate
	}
	return actual.Bitrate > 0 && prior.GetMaxBitrateBps() > 0 && float64(actual.Bitrate) <= float64(prior.GetMaxBitrateBps())*1.3
}
