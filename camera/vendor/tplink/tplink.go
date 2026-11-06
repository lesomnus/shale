// Package tplink supplies narrowly selected TP-Link extensions to the standard
// ONVIF driver. It never registers Sources, resets devices or changes their clock.
package tplink

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

// ProfileC420V122 identifies the observed EasyCam C420 HW1.0 firmware 1.2.2
// Build260624 first-password protocol. A family name alone is insufficient to
// select it. CompatibilityProfile can be pinned by the enrollment administrator
// when an unactivated device cannot yet expose authenticated identity.
const ProfileC420V122 = "easycam-c420-hw1-1.2.2-260624"

// This public encryption key is distributed by the camera's public UI, not a
// camera credential. See PROTOCOL.md for the evidence and restricted scope.
const activationPublicKey = `-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC4D6i0oD/Ga5qb//RfSe8MrPVIrMIGecCxkcGWGj9kxxk74qQNq8XUuXoy2PczQ30BpiRHrlkbtBEPeWLpq85tfubTUjhBz1NPNvWrC88uaYVGvzNpgzZOqDC35961uPTuvdUa8vztcUQjEZy16WbmetRjURFIiWJgFCmemyYVbQIDAQAB
-----END PUBLIC KEY-----`

// Driver delegates standard operations to ONVIF. The web extension is only used
// for an explicitly supported first-password operation. It does not reuse the
// browser's login helper, which also calls boot_set_date.
type Driver struct {
	ONVIF                *onvif.Client
	HTTPClient           *http.Client
	WebEndpoint          string
	CompatibilityProfile string
	Language             string
	activationKey        *rsa.PublicKey // package fixture hook; defaults to firmware public key
}

type Adapter = Driver

func NewDriver(baseline *onvif.Client, webEndpoint string) *Driver {
	d := &Driver{ONVIF: baseline, WebEndpoint: webEndpoint}
	if baseline != nil {
		d.HTTPClient = baseline.HTTPClient
	}
	return d
}

var _ camera.Inspector = (*Driver)(nil)
var _ camera.VideoConfigurator = (*Driver)(nil)
var _ camera.Bootstrapper = (*Driver)(nil)

// Match is compatibility selection, not authorization to acquire credentials.
// Scope and access still come from the caller's enrollment policy/provider.
func Match(device camera.DeviceHint) bool {
	vendor := strings.ToLower(strings.ReplaceAll(device.Manufacturer, "-", ""))
	return strings.Contains(vendor, "tplink") && strings.EqualFold(device.Model, "EasyCam C420") && device.Hardware == "1.0" && strings.HasPrefix(device.Firmware, "1.2.2 Build260624")
}

func (d *Driver) baseline() *onvif.Client {
	if d.ONVIF != nil {
		return d.ONVIF
	}
	return onvif.NewClient(d.HTTPClient)
}

func (d *Driver) Inspect(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease) (camera.Inspection, error) {
	i, err := d.baseline().Inspect(ctx, hint, lease)
	if err != nil {
		return i, err
	}
	// Device responses are still assertions. The stream verifier must establish
	// the actual codec, including on firmware outside this compatibility entry.
	if strings.EqualFold(i.Device.Model, "EasyCam C420") {
		for p := range i.Current.Profiles {
			source := "tplink/c420/media2-preferred"
			for _, e := range i.Current.Profiles[p].Evidence {
				if e.Source == "onvif/media1" {
					source = "tplink/c420/media1-codec-unverified"
				}
			}
			i.Current.Profiles[p].Evidence = append(i.Current.Profiles[p].Evidence, camera.Evidence{Source: source, At: i.Current.ObservedAt, Firmware: i.Device.Firmware, Authenticated: lease != nil})
		}
	}
	return i, nil
}

func (d *Driver) Plan(ctx context.Context, inspection camera.Inspection, desired camera.DesiredState) (camera.ChangePlan, error) {
	return d.baseline().Plan(ctx, inspection, desired)
}
func (d *Driver) Apply(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, plan camera.ChangePlan) (camera.CurrentState, error) {
	return d.baseline().Apply(ctx, hint, lease, plan)
}

type rpcResult struct {
	ErrorCode int `json:"error_code"`
	Data      struct {
		Code int `json:"code"`
	} `json:"data"`
}

var errProtocol = errors.New("TP-Link protocol response not recognized")

func (d *Driver) endpoint() (string, error) {
	u, err := url.Parse(d.WebEndpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", camera.ErrInvalid
	}
	u.Path = "/"
	return u.String(), nil
}

func (d *Driver) rpc(ctx context.Context, payload any) (rpcResult, error) {
	var result rpcResult
	ep, err := d.endpoint()
	if err != nil {
		return result, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return result, camera.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, bytes.NewReader(data))
	if err != nil {
		return result, camera.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("SourceType", "vigi_web")
	h := http.Client{}
	if d.HTTPClient != nil {
		h = *d.HTTPClient
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := h.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, onvif.ErrTransport
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return result, errProtocol
	}
	if response.StatusCode != 200 && response.StatusCode != 401 {
		return result, errProtocol
	}
	// HTTP 401 with data.code is the normal anonymous challenge response.
	if err := json.Unmarshal(data, &result); err != nil {
		return result, errProtocol
	}
	return result, nil
}

// Activation reads an anonymous state challenge; it sends no password and never
// invokes login, clock, codec or service configuration methods.
func (d *Driver) Activation(ctx context.Context, hint camera.DeviceHint) (camera.ActivationState, error) {
	if d.CompatibilityProfile != "" && d.CompatibilityProfile != ProfileC420V122 {
		return camera.ActivationUnsupported, camera.ErrManualActivation
	}
	if d.CompatibilityProfile == "" && !Match(hint) {
		return camera.ActivationUnsupported, camera.ErrManualActivation
	}
	r, err := d.rpc(ctx, map[string]any{"method": "do", "user_management": map[string]any{"get_encrypt_info": nil}})
	if err != nil {
		return camera.ActivationUnknown, err
	}
	switch r.Data.Code {
	case -40405:
		return camera.ActivationRequired, nil // ESYSRESET, initial password unset
	case -40401, -40407, -40404, -40408, -40414:
		return camera.ActivationReady, nil // existing/locked account; never retry activation
	default:
		return camera.ActivationUnknown, errProtocol
	}
}

func (d *Driver) key() (*rsa.PublicKey, error) {
	if d.activationKey != nil {
		return d.activationKey, nil
	}
	block, _ := pem.Decode([]byte(activationPublicKey))
	if block == nil {
		return nil, errProtocol
	}
	k, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errProtocol
	}
	public, ok := k.(*rsa.PublicKey)
	if !ok {
		return nil, errProtocol
	}
	return public, nil
}

func encryptPassword(key *rsa.PublicKey, password string) (string, error) {
	data, err := rsa.EncryptPKCS1v15(rand.Reader, key, []byte(password))
	if err != nil {
		return "", camera.ErrInvalid
	}
	// encodeURIComponent(base64(...)); PathEscape leaves '+' unescaped in Go,
	// whereas the firmware browser sends %2B. QueryEscape uses '+' for spaces,
	// but base64 has no literal spaces.
	return url.QueryEscape(base64.StdEncoding.EncodeToString(data)), nil
}

// Activate requires the exact intended recoverable credential before any write.
// A resumed call rechecks ESYSRESET and skips initialized devices. Access must
// then be verified with an authenticated ONVIF inspection by the caller.
func (d *Driver) Activate(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease) error {
	state, err := d.Activation(ctx, hint)
	if err != nil {
		return err
	}
	if state == camera.ActivationReady {
		return nil
	}
	if state != camera.ActivationRequired {
		return camera.ErrManualActivation
	}
	if lease == nil {
		return camera.ErrAuthRequired
	}
	ref := lease.Ref()
	if !ref.Valid() || ref.Purpose != camera.AuthInitialAdmin || !ref.Recoverable {
		return camera.ErrNeedsUserInput
	}
	key, err := d.key()
	if err != nil {
		return err
	}
	return lease.Use(ctx, func(material camera.CredentialMaterial) error {
		if material.Username != "admin" || len(material.Password) < 6 || len(material.Password) > 32 {
			return camera.ErrInvalid
		}
		ciphertext, err := encryptPassword(key, material.Password)
		if err != nil {
			return err
		}
		language := d.Language
		if language == "" {
			language = "English"
		}
		result, err := d.rpc(ctx, map[string]any{"method": "do", "set_password": map[string]string{"username": "admin", "ciphertext": ciphertext, "reset_pwd_email": "", "sec_question_1": "", "sec_answer_1": "", "sec_question_2": "", "sec_answer_2": "", "sec_question_3": "", "sec_answer_3": "", "pwd_sec_type": "1", "language": language}})
		if err != nil {
			return err
		}
		if result.ErrorCode == -50932 { // another completed write; inspect, never reset
			state, err := d.Activation(ctx, hint)
			if err == nil && state == camera.ActivationReady {
				return nil
			}
			return camera.ErrConflict
		}
		if result.ErrorCode != 0 {
			return errProtocol
		}
		state, err := d.Activation(ctx, hint)
		if err != nil {
			return err
		}
		if state != camera.ActivationReady {
			return errProtocol
		}
		return nil
	})
}
