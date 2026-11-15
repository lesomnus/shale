package provision

import (
	"context"
	"errors"

	"github.com/lesomnus/shale/camera"
)

// errTLSUnobserved: a pin was refused, but the key in its place could not be
// read, or was the pin again. Nothing is recorded, and the run fails as after
// any refused connection.
var errTLSUnobserved = errors.New("changed camera TLS key could not be observed")

// changedTLS records the key a pinned device presented in place of its pin,
// so the run stops for an operator to approve exactly that key. The refused
// handshake sent no request, and observing sends no credential.
func (e *Engine) changedTLS(ctx context.Context, r *Run, cause error) error {
	pin := r.PinnedTLSIdentity()
	if pin == nil || e.TLSObserver == nil {
		return errTLSUnobserved
	}
	observed, err := e.TLSObserver.ObserveWebIdentity(ctx, r.Request.Device)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errTLSUnobserved
	}
	if observed.Validate() != nil || observed.DeviceID != pin.DeviceID {
		return camera.ErrConflict
	}
	if observed.SPKISHA256 == pin.SPKISHA256 {
		return errTLSUnobserved
	}
	r.ChangedTLSIdentity = &observed
	return cause
}
