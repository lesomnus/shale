package provision_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

// keyedCamera answers its web port with key. A driver pinning another key is
// refused in the handshake, before any request or credential is sent.
type keyedCamera struct {
	*device
	pin, key     string
	observations int
	observeErr   error
	observed     *camera.DeviceTLSIdentity
}

func (c *keyedCamera) web() error {
	if c.key != c.pin {
		return camera.ErrTLSIdentityChanged
	}
	return nil
}
func (c *keyedCamera) Activation(ctx context.Context, h camera.DeviceHint) (camera.ActivationState, error) {
	if err := c.web(); err != nil {
		return camera.ActivationUnknown, err
	}
	return c.device.Activation(ctx, h)
}
func (c *keyedCamera) Activate(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease) error {
	if err := c.web(); err != nil {
		return err
	}
	return c.device.Activate(ctx, h, l)
}
func (c *keyedCamera) ObserveWebIdentity(_ context.Context, h camera.DeviceHint) (camera.DeviceTLSIdentity, error) {
	c.observations++
	if c.observeErr != nil {
		return camera.DeviceTLSIdentity{}, c.observeErr
	}
	if c.observed != nil {
		return *c.observed, nil
	}
	return camera.DeviceTLSIdentity{DeviceID: string(h.ID), SPKISHA256: c.key}, nil
}

func key(digit string) string { return "sha256:" + strings.Repeat(digit, 64) }

func keyedEngine(t *testing.T, pin, presented string) (provision.Request, *keyedCamera, *provision.Engine) {
	t.Helper()
	q, d, _, e, _ := fixture(t)
	d.ready = true
	q.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-v1", WebScheme: "https", WebPort: 443}
	q.DeviceTLSIdentity = &camera.DeviceTLSIdentity{DeviceID: string(q.Device.ID), SPKISHA256: pin}
	c := &keyedCamera{device: d, pin: pin, key: presented}
	e.Bootstrapper, e.TLSObserver = c, c
	return q, c, e
}

func TestChangedTLSKeyStopsTheRunWithTheObservedKeyAndWritesNothing(t *testing.T) {
	q, c, e := keyedEngine(t, key("a"), key("b"))
	for range 2 {
		// Without an approval the key stays refused, attempt after attempt.
		r, err := e.Execute(t.Context(), q, true)
		require.ErrorIs(t, err, camera.ErrTLSIdentityChanged)
		require.NotContains(t, err.Error(), key("b"))
		require.Equal(t, provision.WaitingInput, r.Stage)
		require.Equal(t, "tls_key_changed", r.Events[len(r.Events)-1].Code)
		require.Equal(t, &camera.DeviceTLSIdentity{DeviceID: string(q.Device.ID), SPKISHA256: key("b")}, r.ChangedTLSIdentity)
		require.Equal(t, q.DeviceTLSIdentity, r.Request.DeviceTLSIdentity, "an observed key never replaces the pin")
		require.Nil(t, r.Inspection, "nothing past the refused handshake ran")
		require.Zero(t, c.activations)
		require.Zero(t, c.writes)
		persisted, err := e.Store.Load(t.Context(), q.ID)
		require.NoError(t, err)
		require.Equal(t, r.ChangedTLSIdentity, persisted.ChangedTLSIdentity)
	}
	require.Equal(t, 2, c.observations)

	// The device presents its pin again: the stop no longer applies.
	c.key = key("a")
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Nil(t, r.ChangedTLSIdentity)
}

func TestApprovedKeyIsTheOnlyKeyTheNextRunAccepts(t *testing.T) {
	// The job an approval creates pins the approved key. Any other key the
	// device presents after the approval stops that job again.
	q, c, e := keyedEngine(t, key("b"), key("c"))
	q.ID = "approved-successor"
	r, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, camera.ErrTLSIdentityChanged)
	require.Equal(t, key("c"), r.ChangedTLSIdentity.SPKISHA256)
	require.Zero(t, c.writes)
	c.key = key("b")
	r, err = e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
}

func TestChangedTLSKeyThatCannotBeObservedRecordsNothing(t *testing.T) {
	for name, edit := range map[string]func(*keyedCamera, *provision.Engine){
		"observer fails": func(c *keyedCamera, _ *provision.Engine) { c.observeErr = errors.New("handshake failed") },
		"pin again": func(c *keyedCamera, _ *provision.Engine) {
			c.observed = &camera.DeviceTLSIdentity{DeviceID: "camera-physical", SPKISHA256: key("a")}
		},
		"another device": func(c *keyedCamera, _ *provision.Engine) {
			c.observed = &camera.DeviceTLSIdentity{DeviceID: "another", SPKISHA256: key("b")}
		},
		"malformed": func(c *keyedCamera, _ *provision.Engine) {
			c.observed = &camera.DeviceTLSIdentity{DeviceID: "camera-physical", SPKISHA256: "sha256:B"}
		},
		"without observer": func(_ *keyedCamera, e *provision.Engine) { e.TLSObserver = nil },
	} {
		t.Run(name, func(t *testing.T) {
			q, c, e := keyedEngine(t, key("a"), key("b"))
			edit(c, e)
			r, err := e.Execute(t.Context(), q, true)
			require.Error(t, err)
			require.NotErrorIs(t, err, camera.ErrTLSIdentityChanged)
			require.Nil(t, r.ChangedTLSIdentity)
			require.NotEqual(t, "tls_key_changed", r.Events[len(r.Events)-1].Code)
			require.Zero(t, c.writes)
		})
	}
}

func TestUnpinnedRunNeverRecordsAChangedKey(t *testing.T) {
	q, c, e := keyedEngine(t, key("a"), key("b"))
	q.DeviceTLSIdentity = nil
	r, err := e.Execute(t.Context(), q, true)
	require.Error(t, err)
	require.NotErrorIs(t, err, camera.ErrTLSIdentityChanged)
	require.Nil(t, r.ChangedTLSIdentity)
	require.Zero(t, c.observations)
}
