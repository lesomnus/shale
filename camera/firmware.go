package camera

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrFirmwareImage means an image could not be fetched or no file in it has
// the required digest. Nothing was written to a device.
var ErrFirmwareImage = errors.New("camera firmware image unavailable")

// FirmwareImage names the exact firmware file written to a device. SHA256 is
// the lowercase hex digest of that file, not of an archive that contains it.
type FirmwareImage struct {
	URL    string `json:"url" yaml:"url"`
	SHA256 string `json:"sha256" yaml:"sha256"`
}

// FirmwareUpgrade is preset policy: a device that reports exactly one of From
// is upgraded to To with Image before any other configuration. Any other
// firmware, including an unknown or newer build, is left untouched. Builds are
// compared as exact strings because vendor version strings are not ordered.
type FirmwareUpgrade struct {
	From  []string      `json:"from" yaml:"from"`
	To    string        `json:"to" yaml:"to"`
	Image FirmwareImage `json:"image" yaml:"image"`
}

var firmwareDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

func firmwareBuild(v string) bool {
	return v != "" && len(v) <= 256 && utf8.ValidString(v) && strings.TrimSpace(v) == v && !strings.ContainsFunc(v, unicode.IsControl)
}

func (u FirmwareUpgrade) Validate() error {
	if !firmwareBuild(u.To) || len(u.From) == 0 || len(u.From) > 16 {
		return fmt.Errorf("%w: firmware upgrade needs a target and 1-16 exact source builds", ErrInvalid)
	}
	for i, from := range u.From {
		if !firmwareBuild(from) || from == u.To || slices.Contains(u.From[:i], from) {
			return fmt.Errorf("%w: invalid firmware upgrade source build", ErrInvalid)
		}
	}
	if !firmwareDigest.MatchString(u.Image.SHA256) {
		return fmt.Errorf("%w: firmware image needs a lowercase hex sha256", ErrInvalid)
	}
	parsed, err := url.Parse(u.Image.URL)
	if err != nil || len(u.Image.URL) > 2048 || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || strings.ContainsFunc(u.Image.URL, unicode.IsControl) {
		return fmt.Errorf("%w: firmware image needs an https URL without credentials", ErrInvalid)
	}
	return nil
}

// Applies reports whether a device currently on firmware is a qualified source.
func (u FirmwareUpgrade) Applies(firmware string) bool {
	return firmware != "" && slices.Contains(u.From, firmware)
}

func (u *FirmwareUpgrade) Clone() *FirmwareUpgrade {
	if u == nil {
		return nil
	}
	out := *u
	out.From = slices.Clone(u.From)
	return &out
}

// FirmwareFile is a verified image held in memory for one upload.
type FirmwareFile struct {
	Name   string
	SHA256 string
	Data   []byte
}

// FirmwareSource fetches an image and returns it only when its digest matches.
type FirmwareSource interface {
	Fetch(context.Context, FirmwareImage) (FirmwareFile, error)
}

// FirmwareUpgrader writes a verified image to an authenticated device. The
// device reboots afterwards; the caller waits and inspects it again. It must not
// reset the device or change any other setting.
//
// ConfirmWebIdentity reads the key a device presents after an upgrade and
// proves that the given credential logs in to that same key. It is only for
// the window right after an upgrade this Shale performed: the caller must
// check the physical identity and record the result before trusting it.
type FirmwareUpgrader interface {
	UpgradeFirmware(context.Context, DeviceHint, CredentialLease, FirmwareFile) error
	ConfirmWebIdentity(context.Context, DeviceHint, CredentialLease) (DeviceTLSIdentity, error)
}
