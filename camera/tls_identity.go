package camera

import (
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
