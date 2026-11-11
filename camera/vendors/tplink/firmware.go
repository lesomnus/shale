package tplink

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
	"unicode"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

// MaxFirmwareBytes bounds an image held in memory for one upload. The
// qualified C420 image is about 14 MiB.
const MaxFirmwareBytes = 64 << 20

const firmwareUploadPath = "/admin/system/firmware_upgrade"

// UpgradeFirmware repeats the camera UI's Local Update: it announces an upgrade,
// then uploads the image to the path the camera returns. The camera writes the
// image and reboots on its own; the caller waits and inspects it again. It does
// not reboot or reset the camera, including when the upload is refused.
func (d *Driver) UpgradeFirmware(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, file camera.FirmwareFile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(file.Data) == 0 || len(file.Data) > MaxFirmwareBytes {
		return camera.ErrInvalid
	}
	if err := d.initializationTarget(hint); err != nil {
		return err
	}
	return d.withWebSession(ctx, lease, func(session *webSession) error {
		reply, err := session.call(ctx, map[string]any{"method": "do", "system": map[string]any{"firmware_upgrade": map[string]string{"type": "0"}}})
		if err != nil {
			return err
		}
		if err := reply.success(); err != nil {
			return err
		}
		target, err := url.PathUnescape(reply.URL)
		if err != nil {
			return errProtocol
		}
		// The UI drops anything up to "..". Accept only the path it has
		// always returned rather than following an arbitrary one.
		if i := strings.Index(target, ".."); i >= 0 {
			target = target[i+2:]
		}
		if target != firmwareUploadPath {
			return errProtocol
		}
		return session.upload(ctx, target, firmwareName(file.Name), file.Data)
	})
}

func firmwareName(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || name == "/" || len(name) > 128 || strings.ContainsFunc(name, func(r rune) bool { return unicode.IsControl(r) || r == '"' }) {
		return "firmware.bin"
	}
	return name
}

// upload sends one multipart file field, as the UI's FormData does. Like the
// UI it waits longer than an ordinary RPC: the camera answers after it has
// received and checked the image.
func (s *webSession) upload(ctx context.Context, target, name string, data []byte) error {
	ep, err := s.driver.endpoint()
	if err != nil {
		return err
	}
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", name)
	if err != nil {
		return camera.ErrInvalid
	}
	if _, err := part.Write(data); err != nil {
		return camera.ErrInvalid
	}
	if err := form.Close(); err != nil {
		return camera.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(ep, "/")+s.prefix+target, &body)
	if err != nil {
		return camera.ErrInvalid
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("SourceType", "vigi_web")
	req.Close = true
	h := http.Client{}
	if s.driver.HTTPClient != nil {
		h = *s.driver.HTTPClient
	}
	// The context bounds this request; the client's short RPC timeout would
	// cut off a large upload.
	h.Timeout = 0
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := h.Do(req)
	if err != nil {
		// net/http errors include the URL, and so the session token.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return onvif.ErrTransport
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return errProtocol
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return camera.ErrAuthRequired
	}
	if response.StatusCode != http.StatusOK {
		return errProtocol
	}
	var reply webReply
	if json.Unmarshal(raw, &reply) != nil {
		return errProtocol
	}
	return reply.success()
}

// VerifyLogin opens and closes one web session with lease. It writes nothing.
func (d *Driver) VerifyLogin(ctx context.Context, lease camera.CredentialLease) error {
	return d.withWebSession(ctx, lease, func(*webSession) error { return nil })
}
