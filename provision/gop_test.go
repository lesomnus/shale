package provision_test

import (
	"context"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

// A separately injected Verifier can claim successful decoding while returning
// no keyframe cadence. The engine must enforce the explicit GOP at publication,
// independently of the native verifier's own validation.
type observedGOPVerifier struct{ gop int }

func (v observedGOPVerifier) Verify(ctx context.Context, q camera.MediaRequest) (camera.VerifiedState, error) {
	observed, err := (verifier{}).Verify(ctx, q)
	observed.Profile.GOP = v.gop
	observed.DecodedFrames = 151
	return observed, err
}

func TestInjectedVerifierCannotPublishUnverifiedExplicitGOP(t *testing.T) {
	for _, tc := range []struct {
		name    string
		desired int
		actual  int
		publish bool
	}{
		{name: "unknown-explicit", desired: 25},
		{name: "above-explicit-bound", desired: 25, actual: 28},
		{name: "observed-explicit", desired: 25, actual: 25, publish: true},
		{name: "unknown-unconstrained", publish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, device, publisher, engine, _ := fixture(t)
			device.ready = true
			device.profile.GOP = 25 // Read-back configuration is not observation.
			q.Desired.GOP = tc.desired
			engine.Verifier = observedGOPVerifier{gop: tc.actual}
			run, err := engine.Execute(t.Context(), q, true)
			if tc.publish {
				require.NoError(t, err)
				require.Equal(t, provision.Active, run.Stage)
				require.Equal(t, 1, publisher.deploys)
			} else {
				require.ErrorIs(t, err, provision.ErrNotReady)
				require.NotEqual(t, provision.Active, run.Stage)
				require.Zero(t, publisher.deploys)
				require.Nil(t, run.Verified, "rejected cadence cannot become durable verified evidence")
			}
		})
	}
}
