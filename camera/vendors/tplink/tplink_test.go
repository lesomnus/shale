package tplink

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

type activationFixture struct {
	mu           sync.Mutex
	server       *httptest.Server
	key          *rsa.PrivateKey
	active       bool
	loseResponse bool
	writes       int
	unexpected   int
	password     string
}

func newActivationFixture(t *testing.T) *activationFixture {
	k, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	f := &activationFixture{key: k}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *activationFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var req map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		w.WriteHeader(400)
		return
	}
	if req["user_management"] != nil {
		var method map[string]any
		_ = json.Unmarshal(req["user_management"], &method)
		if _, ok := method["get_encrypt_info"]; !ok {
			f.unexpected++
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(401)
		code := -40405
		if f.active {
			code = -40401
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40401, "data": map[string]any{"code": code, "encrypt_type": []string{"2"}}})
		return
	}
	if req["set_password"] == nil {
		f.unexpected++
		w.WriteHeader(400)
		return
	}
	if f.active {
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -50932})
		return
	}
	var args map[string]string
	_ = json.Unmarshal(req["set_password"], &args)
	ciphertext, err := url.QueryUnescape(args["ciphertext"])
	if err != nil {
		w.WriteHeader(400)
		return
	}
	encrypted, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	password, err := rsa.DecryptPKCS1v15(rand.Reader, f.key, encrypted)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	if args["username"] != "admin" || args["pwd_sec_type"] != "1" || args["language"] != "English" {
		w.WriteHeader(400)
		return
	}
	f.password = string(password)
	f.active = true
	f.writes++
	if f.loseResponse {
		connection, _, _ := w.(http.Hijacker).Hijack()
		_ = connection.Close()
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]int{"error_code": 0})
}

func testHint() camera.DeviceHint {
	return camera.DeviceHint{Manufacturer: "TP-Link", Model: "EasyCam C420", Hardware: "1.0", Firmware: "1.2.2 Build260624 Rel.39352n"}
}
func initialLease(recoverable bool) camera.CredentialLease {
	return camera.NewCredentialLease(camera.CredentialRef{Provider: "vault", ID: "admin-c420", Version: "exact-1", Scope: camera.EnrollmentScope{TenantID: "a"}, Purpose: camera.AuthInitialAdmin, Recoverable: recoverable}, camera.CredentialMaterial{Username: "admin", Password: "test reserved +:/?# password"}, time.Now().Add(time.Minute))
}
func testDriver(f *activationFixture) *Driver {
	return &Driver{WebEndpoint: f.server.URL, activationKey: &f.key.PublicKey}
}

func TestFirstPasswordProtocolAndIdempotentResume(t *testing.T) {
	f := newActivationFixture(t)
	d := testDriver(f)
	lease := initialLease(true)
	defer lease.Close()
	state, err := d.Activation(context.Background(), testHint())
	if err != nil || state != camera.ActivationRequired {
		t.Fatalf("state %s: %v", state, err)
	}
	if f.writes != 0 {
		t.Fatal("inspection wrote")
	}
	if err := d.Activate(context.Background(), testHint(), lease); err != nil {
		t.Fatal(err)
	}
	if f.writes != 1 || f.password != "test reserved +:/?# password" {
		t.Fatal("intended password was not activated")
	}
	// A new worker can safely replay the run without overwriting the account.
	resumed := testDriver(f)
	if err := resumed.Activate(context.Background(), testHint(), nil); err != nil {
		t.Fatal(err)
	}
	if f.writes != 1 || f.unexpected != 0 {
		t.Fatal("replay changed device or called unrelated method")
	}
}

func TestInterruptionAfterPasswordWriteReinspects(t *testing.T) {
	f := newActivationFixture(t)
	f.loseResponse = true
	d := testDriver(f)
	lease := initialLease(true)
	defer lease.Close()
	if err := d.Activate(context.Background(), testHint(), lease); err == nil {
		t.Fatal("expected ambiguous transport failure")
	}
	f.mu.Lock()
	writes := f.writes
	f.mu.Unlock()
	if writes != 1 {
		t.Fatal("write did not happen")
	}
	// A new lease resolves the prepared exact version; it remains usable after
	// the first lease's local material was released.
	ref := lease.Ref()
	_ = lease.Close()
	again := initialLease(true)
	defer again.Close()
	if again.Ref() != ref {
		t.Fatal("credential version changed")
	}
	if err := testDriver(f).Activate(context.Background(), testHint(), again); err != nil {
		t.Fatal(err)
	}
	if f.writes != 1 {
		t.Fatal("ambiguous response caused another write")
	}
}

func TestUnrecoverableCredentialsAndUnknownFirmwareWaitWithoutWrite(t *testing.T) {
	f := newActivationFixture(t)
	d := testDriver(f)
	lease := initialLease(false)
	defer lease.Close()
	if err := d.Activate(context.Background(), testHint(), lease); !errors.Is(err, camera.ErrNeedsUserInput) {
		t.Fatal(err)
	}
	hint := testHint()
	hint.Firmware = "9.0.0 unknown"
	if _, err := d.Activation(context.Background(), hint); !errors.Is(err, camera.ErrManualActivation) {
		t.Fatal(err)
	}
	d.CompatibilityProfile = "unknown-vendor-profile"
	if err := d.Activate(context.Background(), testHint(), lease); !errors.Is(err, camera.ErrManualActivation) {
		t.Fatal(err)
	}
	if f.writes != 0 || f.unexpected != 0 {
		t.Fatal("unsupported device was changed")
	}
}

func TestAdministratorPinnedCompatibilityForFreshDevice(t *testing.T) {
	f := newActivationFixture(t)
	d := testDriver(f)
	d.CompatibilityProfile = ProfileC420V122
	lease := initialLease(true)
	defer lease.Close()
	if err := d.Activate(context.Background(), camera.DeviceHint{}, lease); err != nil {
		t.Fatal(err)
	}
	if f.writes != 1 {
		t.Fatal("pinned supported fresh device did not activate")
	}
}

func TestManagedEndpointMustAddressTheClaimedCamera(t *testing.T) {
	f := newActivationFixture(t)
	d := testDriver(f)
	hint := testHint()
	hint.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.10:2020/onvif/device_service"}}
	_, err := d.Activation(context.Background(), hint)
	if !errors.Is(err, camera.ErrForbidden) || f.writes != 0 {
		t.Fatalf("mismatched web endpoint accepted: %v", err)
	}
	hint = testHint()
	hint.Manufacturer = "not-tplink"
	if Match(hint) {
		t.Fatal("manufacturer substring accepted as compatibility evidence")
	}
}

func TestFirmwarePublicKeyAndCredentialFreeErrors(t *testing.T) {
	d := &Driver{}
	key, err := d.key()
	if err != nil {
		t.Fatal(err)
	}
	if key.N.BitLen() != 1024 || key.E != 65537 {
		t.Fatal("firmware public key changed")
	}
	if _, err := encryptPassword(key, "test password"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, "test reserved +:/?# password")
	}))
	defer server.Close()
	d.WebEndpoint = server.URL
	_, err = d.Activation(context.Background(), testHint())
	if err == nil || strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unsafe protocol error %v", err)
	}
}

func TestInitializedInspectionNeverUsesBrowserClockHelper(t *testing.T) {
	var operations []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		request := string(body)
		if strings.Contains(request, "boot_set_date") || strings.Contains(request, "set_password") || strings.Contains(request, "<t:Set") {
			t.Error("read-only inspection wrote camera")
			w.WriteHeader(400)
			return
		}
		op := ""
		response := ""
		switch {
		case strings.Contains(request, "<t:GetDeviceInformation>"):
			op = "GetDeviceInformation"
			response = "<Manufacturer>TP-Link</Manufacturer><Model>EasyCam C420</Model><FirmwareVersion>1.2.2 Build260624</FirmwareVersion><HardwareId>1.0</HardwareId>"
		case strings.Contains(request, "<t:GetServices>"):
			op = "GetServices"
			response = "<Service><Namespace>http://www.onvif.org/ver20/media/wsdl</Namespace><XAddr>" + server.URL + "/media</XAddr></Service>"
		case strings.Contains(request, "<t:GetProfiles>"):
			op = "GetProfiles"
			response = `<Profiles token="main"><Name>Main</Name><Configurations><VideoEncoder token="config"><Encoding>H265</Encoding><Resolution><Width>1920</Width><Height>1080</Height></Resolution><RateControl><FrameRateLimit>30</FrameRateLimit><BitrateLimit>4096</BitrateLimit></RateControl></VideoEncoder></Configurations></Profiles>`
		case strings.Contains(request, "<t:GetVideoEncoderConfigurations>"):
			op = "GetVideoEncoderConfigurations"
		case strings.Contains(request, "<t:GetStreamUri>"):
			op = "GetStreamUri"
			response = "<Uri>" + strings.Replace(server.URL, "http:", "rtsp:", 1) + "/main</Uri>"
		case strings.Contains(request, "<t:GetVideoEncoderConfigurationOptions>"):
			op = "GetVideoEncoderConfigurationOptions"
		default:
			t.Error("unexpected operation")
			w.WriteHeader(400)
			return
		}
		operations = append(operations, op)
		_, _ = io.WriteString(w, `<Envelope><Body><`+op+`Response>`+response+`</`+op+`Response></Body></Envelope>`)
	}))
	defer server.Close()
	d := NewDriver(onvif.NewClient(server.Client()), server.URL)
	lease := initialLease(true)
	defer lease.Close()
	hint := testHint()
	hint.Endpoints = []camera.Endpoint{{URL: server.URL + "/device"}}
	i, err := d.Inspect(context.Background(), hint, lease)
	if err != nil {
		t.Fatal(err)
	}
	if i.Current.Profiles[0].Codec != "h265" || len(operations) != 6 {
		t.Fatal("missing read-only normalized media2 result")
	}
	if i.Current.Profiles[0].Evidence[1].Source != "tplink/c420/media2-preferred" {
		t.Fatal("compatibility provenance missing")
	}
}
