package camera

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// DeviceTLSIdentity names an explicitly approved public key for one inventory
// device. It is public trust metadata, not a credential or an activation grant.
// The executing application must bind it to the current job and transport scope.
type DeviceTLSIdentity struct {
	DeviceID   string `json:"device_id" yaml:"device_id"`
	SPKISHA256 string `json:"spki_sha256" yaml:"spki_sha256"`
}

var deviceSPKIPin = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func (i DeviceTLSIdentity) Validate() error {
	if i.DeviceID == "" || len(i.DeviceID) > 256 || !utf8.ValidString(i.DeviceID) || strings.TrimSpace(i.DeviceID) != i.DeviceID || strings.ContainsFunc(i.DeviceID, unicode.IsControl) || !deviceSPKIPin.MatchString(i.SPKISHA256) {
		return ErrInvalid
	}
	return nil
}

// TLSVerificationSkip is the one value a camera network policy may set besides
// the default, empty, which verifies a device's HTTPS web key by its pin or by
// ordinary PKI. Skip checks no key there: the device is identified by its
// recorded MAC and serial only, and credentials go to an unverified endpoint.
const TLSVerificationSkip = "skip"

// ErrTLSIdentityChanged means a device's web port presented a key other than
// its pinned one. The handshake failed before any request was sent; nothing,
// including a credential, reached the device.
var ErrTLSIdentityChanged = errors.New("camera TLS key differs from its pin")

// WebIdentityObserver reads the key a pinned device's web port presents now,
// with a credential-free handshake on its current authorized endpoint. The
// result is an observation for an operator to approve, never trust by itself.
type WebIdentityObserver interface {
	ObserveWebIdentity(context.Context, DeviceHint) (DeviceTLSIdentity, error)
}
