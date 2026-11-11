package drivers

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/camera"
)

// loginWeb answers only the C420 web login and logout. Any other call fails
// the test: confirming a key must not write the camera.
func loginWeb(t *testing.T, password string, logins, other *atomic.Int32) http.Handler {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	public := url.QueryEscape(url.QueryEscape(base64.StdEncoding.EncodeToString(der)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch {
		case r.URL.Path == "/" && req["user_management"] != nil:
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40401, "data": map[string]any{"code": -40401, "encrypt_type": []string{"2"}, "passwdType": "md5", "nonce": "n", "key_2": public}})
		case r.URL.Path == "/" && req["login"] != nil:
			var login map[string]string
			_ = json.Unmarshal(req["login"], &login)
			encoded, _ := url.QueryUnescape(login["password"])
			cipher, _ := base64.StdEncoding.DecodeString(encoded)
			plain, _ := rsa.DecryptPKCS1v15(rand.Reader, key, cipher)
			digest := md5.Sum([]byte("TPCQ75NF2Y:" + password))
			if string(plain) != strings.ToUpper(hex.EncodeToString(digest[:]))+":n" {
				_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40404})
				return
			}
			logins.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0, "stok": "token"})
		case r.URL.Path == "/stok=token/ds" && string(req["system"]) == `{"logout":"null"}`:
			_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0})
		default:
			other.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
}

// pinnedDriver uses the host clock for WS-Security: the ONVIF fixture does not
// serve the device time that pinned C420 drivers otherwise use.
func pinnedDriver(t *testing.T, o Options) *Driver {
	t.Helper()
	d, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	d.baseline.UseDeviceTimeForAuthentication = false
	return d
}

func TestConfirmWebIdentityReadsTheNewKeyAndLogsIn(t *testing.T) {
	c := fixtureCamera(t)
	var logins, other atomic.Int32
	web, rotated := factoryWeb(t, loginWeb(t, fixturePassword, &logins, &other))
	// The job still pins the key from before the upgrade.
	d := pinnedDriver(t, pinnedWebOptions(t, web, "sha256:"+strings.Repeat("1", 64)))
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: c.Hint("").Endpoints}
	identity, err := d.ConfirmWebIdentity(t.Context(), h, fixtureLease(t, camera.AuthManagement))
	if err != nil {
		t.Fatal(err)
	}
	if identity != (camera.DeviceTLSIdentity{DeviceID: "inventory-a", SPKISHA256: rotated}) || logins.Load() != 1 || other.Load() != 0 {
		t.Fatalf("identity=%+v logins=%d other=%d", identity, logins.Load(), other.Load())
	}
	if writes, activations := c.Writes(); writes != 0 || activations != 0 {
		t.Fatalf("writes=%d activations=%d", writes, activations)
	}
	// The driver's own pinned client still refuses that key.
	if _, err := d.webClient.Post(web.URL, "application/json", strings.NewReader("{}")); err == nil {
		t.Fatal("pinned client accepted the new key")
	}
}

func TestConfirmWebIdentityRefusals(t *testing.T) {
	c := fixtureCamera(t)
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: c.Hint("").Endpoints}
	t.Run("credential does not log in", func(t *testing.T) {
		var logins, other atomic.Int32
		web, _ := factoryWeb(t, loginWeb(t, "another password", &logins, &other))
		d := pinnedDriver(t, pinnedWebOptions(t, web, "sha256:"+strings.Repeat("1", 64)))
		if _, err := d.ConfirmWebIdentity(t.Context(), h, fixtureLease(t, camera.AuthManagement)); !errors.Is(err, camera.ErrAuthRequired) {
			t.Fatalf("refused login: %v", err)
		}
	})
	t.Run("no pinned device", func(t *testing.T) {
		var logins, other atomic.Int32
		web, _ := factoryWeb(t, loginWeb(t, fixturePassword, &logins, &other))
		d := scopedDriver(t, bootstrapFor(t, web.URL))
		if _, err := d.ConfirmWebIdentity(t.Context(), camera.DeviceHint{ID: "inventory-a", Endpoints: c.Hint("").Endpoints}, fixtureLease(t, camera.AuthManagement)); !errors.Is(err, camera.ErrInvalid) {
			t.Fatalf("unpinned driver: %v", err)
		}
		if logins.Load() != 0 {
			t.Fatal("unpinned driver logged in")
		}
	})
	t.Run("another device", func(t *testing.T) {
		var logins, other atomic.Int32
		web, _ := factoryWeb(t, loginWeb(t, fixturePassword, &logins, &other))
		d := pinnedDriver(t, pinnedWebOptions(t, web, "sha256:"+strings.Repeat("1", 64)))
		if _, err := d.ConfirmWebIdentity(t.Context(), camera.DeviceHint{ID: "inventory-b", Endpoints: h.Endpoints}, fixtureLease(t, camera.AuthManagement)); !errors.Is(err, camera.ErrForbidden) {
			t.Fatalf("another device: %v", err)
		}
	})
}
