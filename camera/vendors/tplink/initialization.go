package tplink

import (
	"context"
	"crypto/md5" // The supported firmware's login protocol requires this digest.
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

const frequencyPlanRevision = "tplink/c420/image-frequency-v1"
const smartCodecPlanRevision = "tplink/c420/video-initialization-v1"

var _ camera.DeviceInitializer = (*Driver)(nil)

// PlanInitialization reads the image frequency without using the browser login
// helper: that helper also writes the camera's clock. Empty settings preserve
// device configuration and do not open a web session. Smart codec additionally
// records the complete bounded native main-stream state before a possible write.
func (d *Driver) PlanInitialization(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, desired camera.DeviceSettings) (camera.InitializationPlan, error) {
	if err := ctx.Err(); err != nil {
		return camera.InitializationPlan{}, err
	}
	if desired == (camera.DeviceSettings{}) {
		return camera.InitializationPlan{}, nil
	}
	if desired.PowerLineFrequency != 0 && !validFrequency(desired.PowerLineFrequency) || desired.SmartCodec != "" && !validSmartCodec(desired.SmartCodec) {
		return camera.InitializationPlan{}, camera.ErrInvalid
	}
	if err := d.initializationTarget(hint); err != nil {
		return camera.InitializationPlan{}, err
	}
	if desired.SmartCodec != "" && !smartCodecTarget(hint) {
		return camera.InitializationPlan{}, camera.ErrUnsupported
	}
	var before camera.DeviceSettings
	var video *camera.InitializationVideoMain
	err := d.withWebSession(ctx, lease, func(session *webSession) error {
		var err error
		if desired.PowerLineFrequency != 0 {
			before, err = session.frequency(ctx)
			if err != nil {
				return err
			}
		}
		if desired.SmartCodec != "" {
			v, err := session.videoMain(ctx)
			if err != nil {
				return err
			}
			before.SmartCodec = v.SmartCodec
			video = &v
		}
		return nil
	})
	if err != nil {
		return camera.InitializationPlan{}, err
	}
	plan := camera.InitializationPlan{Before: before, After: desired, Revision: frequencyPlanRevision, VideoMainBefore: video}
	if desired.SmartCodec != "" {
		plan.Revision = smartCodecPlanRevision
	}
	if before.PowerLineFrequency != desired.PowerLineFrequency {
		plan.Changes = append(plan.Changes, "power_line_frequency")
	}
	if before.SmartCodec != desired.SmartCodec {
		plan.Changes = append(plan.Changes, "smart_codec")
	}
	return plan, nil
}

// ApplyInitialization rechecks the read-only plan before writing one known image
// setting. An already-applied value is accepted, including after a lost write
// response. Other concurrent changes require a new plan; transport failures are
// deliberately returned without selecting another setting.
func (d *Driver) ApplyInitialization(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, plan camera.InitializationPlan) (camera.DeviceSettings, error) {
	if err := ctx.Err(); err != nil {
		return camera.DeviceSettings{}, err
	}
	if plan.Before == (camera.DeviceSettings{}) && plan.After == (camera.DeviceSettings{}) && plan.VideoMainBefore == nil {
		return camera.DeviceSettings{}, nil
	}
	if plan.After.PowerLineFrequency != 0 && (!validFrequency(plan.Before.PowerLineFrequency) || !validFrequency(plan.After.PowerLineFrequency)) || plan.After.PowerLineFrequency == 0 && plan.Before.PowerLineFrequency != 0 {
		return camera.DeviceSettings{}, camera.ErrInvalid
	}
	if plan.After.SmartCodec != "" {
		if !validSmartCodec(plan.Before.SmartCodec) || !validSmartCodec(plan.After.SmartCodec) || plan.VideoMainBefore == nil || !validNativeVideoMain(*plan.VideoMainBefore) || plan.VideoMainBefore.SmartCodec != plan.Before.SmartCodec {
			return camera.DeviceSettings{}, camera.ErrInvalid
		}
	} else if plan.Before.SmartCodec != "" || plan.VideoMainBefore != nil {
		return camera.DeviceSettings{}, camera.ErrInvalid
	}
	wantRevision := frequencyPlanRevision
	if plan.After.SmartCodec != "" {
		wantRevision = smartCodecPlanRevision
	}
	if plan.Revision != wantRevision {
		return camera.DeviceSettings{}, camera.ErrConflict
	}
	if err := d.initializationTarget(hint); err != nil {
		return camera.DeviceSettings{}, err
	}
	if plan.After.SmartCodec != "" && !smartCodecTarget(hint) {
		return camera.DeviceSettings{}, camera.ErrUnsupported
	}
	var observed camera.DeviceSettings
	err := d.withWebSession(ctx, lease, func(session *webSession) error {
		var err error
		// Check every selected prerequisite before the first setting write.
		// In particular a frequency conflict must not partially apply smart
		// codec first. Read again below immediately before a frequency write.
		if plan.After.PowerLineFrequency != 0 {
			frequency, err := session.frequency(ctx)
			if err != nil {
				return err
			}
			if frequency.PowerLineFrequency != plan.Before.PowerLineFrequency && frequency.PowerLineFrequency != plan.After.PowerLineFrequency {
				return camera.ErrConflict
			}
			observed.PowerLineFrequency = frequency.PowerLineFrequency
		}
		// Guard the full video write before frequency can legitimately alter the
		// encoder. A target mode already present needs no full-state write and
		// therefore never overwrites unrelated drift or post-frequency changes.
		if plan.After.SmartCodec != "" {
			if err = session.applySmartCodec(ctx, *plan.VideoMainBefore, plan.After.SmartCodec); err != nil {
				return err
			}
			observed.SmartCodec = plan.After.SmartCodec
		}
		if plan.After.PowerLineFrequency == 0 {
			return nil
		}
		frequency, err := session.frequency(ctx)
		if err != nil {
			return err
		}
		observed.PowerLineFrequency = frequency.PowerLineFrequency
		if observed.PowerLineFrequency == plan.After.PowerLineFrequency {
			return nil
		}
		if observed.PowerLineFrequency != plan.Before.PowerLineFrequency {
			return camera.ErrConflict
		}
		reply, err := session.call(ctx, map[string]any{"method": "set", "image": map[string]any{"switch": map[string]string{"flicker": strconv.Itoa(plan.After.PowerLineFrequency) + "hz"}}})
		if err != nil {
			return err
		}
		if err = reply.success(); err != nil {
			return err
		}
		frequency, err = session.frequency(ctx)
		if err != nil {
			return err
		}
		observed.PowerLineFrequency = frequency.PowerLineFrequency
		if observed.PowerLineFrequency != plan.After.PowerLineFrequency {
			return camera.ErrConflict
		}
		if plan.After.SmartCodec != "" {
			video, err := session.videoMain(ctx)
			if err != nil {
				return err
			}
			if video.SmartCodec != plan.After.SmartCodec {
				return camera.ErrConflict
			}
		}
		return nil
	})
	return observed, err
}

func validFrequency(hz int) bool { return hz == 50 || hz == 60 }

func (d *Driver) initializationTarget(hint camera.DeviceHint) error {
	// A compatibility pin can select an anonymous activation exchange. Image
	// writes additionally require the authenticated device's exact known identity.
	profile := deviceProfile(hint)
	if profile == "" || (d.CompatibilityProfile != "" && d.CompatibilityProfile != ProfileC420Known && d.CompatibilityProfile != profile) {
		return camera.ErrUnsupported
	}
	ep, err := d.endpoint()
	if err != nil {
		return err
	}
	web, _ := url.Parse(ep)
	for _, candidate := range hint.Endpoints {
		u, err := url.Parse(candidate.URL)
		if err == nil && strings.EqualFold(u.Hostname(), web.Hostname()) {
			return nil
		}
	}
	if len(hint.Endpoints) != 0 {
		return camera.ErrForbidden
	}
	return nil
}

type webReply struct {
	Status    int                        `json:"-"`
	ErrorCode *int                       `json:"error_code"`
	Data      json.RawMessage            `json:"data"`
	Stok      string                     `json:"stok"`
	Video     map[string]json.RawMessage `json:"video"`
	Image     struct {
		Switch struct {
			Flicker string `json:"flicker"`
		} `json:"switch"`
	} `json:"image"`
}

func (r webReply) success() error {
	if r.ErrorCode == nil {
		return errProtocol
	}
	if *r.ErrorCode == 0 {
		return nil
	}
	switch *r.ErrorCode {
	case -40401, -40404, -40407, -40408, -40414:
		return camera.ErrAuthRequired
	default:
		return errProtocol
	}
}

type webSession struct {
	driver *Driver
	path   string
}

func (s *webSession) call(ctx context.Context, payload any) (webReply, error) {
	return s.driver.webRPC(ctx, s.path, payload)
}

func (s *webSession) frequency(ctx context.Context) (camera.DeviceSettings, error) {
	reply, err := s.call(ctx, map[string]any{"method": "get", "image": map[string]string{"name": "switch"}})
	if err != nil {
		return camera.DeviceSettings{}, err
	}
	if err := reply.success(); err != nil {
		return camera.DeviceSettings{}, err
	}
	switch strings.ToLower(reply.Image.Switch.Flicker) {
	case "50hz":
		return camera.DeviceSettings{PowerLineFrequency: 50}, nil
	case "60hz":
		return camera.DeviceSettings{PowerLineFrequency: 60}, nil
	default:
		return camera.DeviceSettings{}, errProtocol
	}
}

func (d *Driver) withWebSession(ctx context.Context, lease camera.CredentialLease, use func(*webSession) error) error {
	if lease == nil {
		return camera.ErrAuthRequired
	}
	return lease.Use(ctx, func(material camera.CredentialMaterial) error {
		session, err := d.webLogin(ctx, material)
		if err != nil {
			return err
		}
		defer func() {
			// Session cleanup is bounded and still attempted when the caller's
			// context was canceled. Tokens are never persisted or included in errors.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			_, _ = session.call(cleanup, map[string]any{"method": "do", "system": map[string]string{"logout": "null"}})
		}()
		return use(session)
	})
}

func (d *Driver) webLogin(ctx context.Context, material camera.CredentialMaterial) (*webSession, error) {
	reply, err := d.webRPC(ctx, "/", map[string]any{"method": "do", "user_management": map[string]any{"get_encrypt_info": nil}})
	if err != nil {
		return nil, err
	}
	var challenge struct {
		Code               int               `json:"code"`
		EncryptType        []json.RawMessage `json:"encrypt_type"`
		CurrentEncryptType json.RawMessage   `json:"current_encrypt_type"`
		PasswordType       string            `json:"passwdType"`
		Key                string            `json:"key"`
		Key2               string            `json:"key_2"`
		Nonce              string            `json:"nonce"`
	}
	if json.Unmarshal(reply.Data, &challenge) != nil || reply.ErrorCode == nil || reply.Status != http.StatusUnauthorized {
		return nil, errProtocol
	}
	if challenge.Code != -40401 && challenge.Code != -40407 {
		return nil, camera.ErrAuthRequired
	}
	if *reply.ErrorCode != -40401 {
		return nil, errProtocol
	}
	kind := authKind(challenge.CurrentEncryptType)
	if kind == 0 {
		for _, supported := range challenge.EncryptType {
			if k := authKind(supported); k > kind {
				kind = k
			}
		}
	}
	if kind != 2 || challenge.PasswordType != "md5" {
		// An unexpected authentication exchange is not evidence that this
		// particular image setting is unsupported, and must not select a
		// lower-quality fallback.
		return nil, errProtocol
	}
	encodedKey := challenge.Key2
	if encodedKey == "" {
		encodedKey = challenge.Key
	}
	for range 2 {
		encodedKey, err = url.PathUnescape(encodedKey)
		if err != nil {
			return nil, errProtocol
		}
	}
	der, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, errProtocol
	}
	key, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, errProtocol
	}
	public, ok := key.(*rsa.PublicKey)
	if !ok || public.N.BitLen() < 1024 || public.N.BitLen() > 8192 || challenge.Nonce == "" || len(challenge.Nonce) > 512 {
		return nil, errProtocol
	}
	digest := md5.Sum([]byte("TPCQ75NF2Y:" + material.Password))
	secret := strings.ToUpper(hex.EncodeToString(digest[:])) + ":" + challenge.Nonce
	ciphertext, err := encryptPassword(public, secret)
	if err != nil {
		return nil, errProtocol
	}
	login := map[string]string{"encrypt_type": "2", "passwdType": "md5", "password": ciphertext, "username": strings.ReplaceAll(url.QueryEscape(material.Username), "+", "%20")}
	if challenge.Key2 != "" {
		login["keyType"] = "1"
	}
	reply, err = d.webRPC(ctx, "/", map[string]any{"method": "do", "login": login})
	if err != nil {
		return nil, err
	}
	if reply.Status != http.StatusOK || reply.success() != nil {
		return nil, camera.ErrAuthRequired
	}
	token, err := url.PathUnescape(reply.Stok)
	if err != nil || token == "" || len(token) > 1024 {
		return nil, errProtocol
	}
	return &webSession{driver: d, path: "/stok=" + url.PathEscape(token) + "/ds"}, nil
}

func authKind(raw json.RawMessage) int {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		n, _ := strconv.Atoi(s)
		return n
	}
	var n int
	_ = json.Unmarshal(raw, &n)
	return n
}

// webRPC shares the anonymous RPC's transport restrictions and deliberately
// replaces net/http errors, whose URLs would otherwise contain session tokens.
func (d *Driver) webRPC(ctx context.Context, path string, payload any) (webReply, error) {
	var result webReply
	ep, err := d.endpoint()
	if err != nil {
		return result, err
	}
	var raw json.RawMessage
	status, err := d.rpcJSON(ctx, strings.TrimRight(ep, "/")+path, payload, &raw)
	if err != nil {
		return result, err
	}
	if status == http.StatusForbidden || (path != "/" && status == http.StatusUnauthorized) {
		return result, camera.ErrAuthRequired
	}
	if json.Unmarshal(raw, &result) != nil {
		return result, errProtocol
	}
	result.Status = status
	return result, nil
}
