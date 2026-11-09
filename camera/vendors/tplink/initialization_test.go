package tplink

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

type initializationFixture struct {
	mu              sync.Mutex
	server          *httptest.Server
	key             *rsa.PrivateKey
	frequency       string
	password        string
	loginRefused    bool
	ignoreWrite     bool
	loseWriteReply  bool
	unknownAuth     bool
	malformedReply  bool
	badKey          bool
	currentKind     string
	challengeCode   int
	challengeStatus int
	challengeOuter  int
	passwordType    string
	emptyNonce      bool
	firmware        string
	videoFPS        int
	videoWrites     int
	webCalls        int
	writes          int
	logins          int
	logouts         int
	unexpected      int
}

func newInitializationFixture(t *testing.T) *initializationFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	f := &initializationFixture{key: key, frequency: "50hz", password: "frequency fixture secret +:/?", currentKind: "2", challengeCode: -40401, challengeStatus: 401, challengeOuter: -40401, passwordType: "md5", firmware: "1.2.2 Build260624", videoFPS: 25}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *initializationFixture) driver() *Driver {
	return NewDriver(onvif.NewClient(f.server.Client()), f.server.URL)
}

func (f *initializationFixture) lease() camera.CredentialLease {
	return camera.NewCredentialLease(camera.CredentialRef{Provider: "fixture", ID: "c420-admin", Version: "1", Purpose: camera.AuthManagement, Scope: camera.EnrollmentScope{TenantID: "lab"}, Recoverable: true}, camera.CredentialMaterial{Username: "admin", Password: f.password}, time.Now().Add(time.Minute))
}

func (f *initializationFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/onvif/device_service" {
		f.serveONVIF(w, r)
		return
	}
	f.webCalls++
	var req map[string]json.RawMessage
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		w.WriteHeader(400)
		return
	}
	if r.Header.Get("SourceType") != "vigi_web" {
		f.unexpected++
		w.WriteHeader(400)
		return
	}
	if req["user_management"] != nil && r.URL.Path == "/" {
		der, _ := x509.MarshalPKIXPublicKey(&f.key.PublicKey)
		public := base64.StdEncoding.EncodeToString(der)
		if f.badKey {
			public = "unrecognized public key"
		}
		kind := f.currentKind
		if f.unknownAuth {
			kind = "9"
		}
		nonce := "fixture-nonce"
		if f.emptyNonce {
			nonce = ""
		}
		w.WriteHeader(f.challengeStatus)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": f.challengeOuter, "data": map[string]any{"code": f.challengeCode, "encrypt_type": []string{"1", "2"}, "current_encrypt_type": kind, "passwdType": f.passwordType, "nonce": nonce, "key_2": url.QueryEscape(url.QueryEscape(public))}})
		return
	}
	if req["login"] != nil && r.URL.Path == "/" {
		f.logins++
		var login map[string]string
		_ = json.Unmarshal(req["login"], &login)
		encoded, _ := url.PathUnescape(login["password"])
		cipher, _ := base64.StdEncoding.DecodeString(encoded)
		plain, _ := rsa.DecryptPKCS1v15(rand.Reader, f.key, cipher)
		digest := md5.Sum([]byte("TPCQ75NF2Y:" + f.password))
		want := strings.ToUpper(hex.EncodeToString(digest[:])) + ":fixture-nonce"
		if f.loginRefused || string(plain) != want || login["encrypt_type"] != "2" || login["passwdType"] != "md5" || login["keyType"] != "1" || login["username"] != "admin" {
			_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40404, "unsafe_detail": f.password})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0, "stok": "private-session-token"})
		return
	}
	if r.URL.Path != "/stok=private-session-token/ds" {
		f.unexpected++
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error_code":-40401}`)
		return
	}
	if req["system"] != nil {
		var system map[string]string
		_ = json.Unmarshal(req["system"], &system)
		if system["logout"] == "null" {
			f.logouts++
			_ = json.NewEncoder(w).Encode(map[string]int{"error_code": 0})
			return
		}
	}
	var method string
	_ = json.Unmarshal(req["method"], &method)
	if req["image"] != nil && method == "get" {
		if f.malformedReply {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0, "image": map[string]any{"switch": map[string]string{"flicker": f.frequency}}})
		return
	}
	if req["image"] != nil && method == "set" {
		var image struct {
			Switch map[string]string `json:"switch"`
		}
		_ = json.Unmarshal(req["image"], &image)
		if len(image.Switch) != 1 || (image.Switch["flicker"] != "50hz" && image.Switch["flicker"] != "60hz") {
			f.unexpected++
			w.WriteHeader(400)
			return
		}
		f.writes++
		if !f.ignoreWrite {
			f.frequency = image.Switch["flicker"]
		}
		if f.loseWriteReply {
			f.loseWriteReply = false
			connection, _, _ := w.(http.Hijacker).Hijack()
			_ = connection.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"error_code": 0})
		return
	}
	// A fixture that accepts only the intended operations also catches clock,
	// factory-reset, sensor and unrelated service-setting writes.
	f.unexpected++
	w.WriteHeader(400)
}

func (f *initializationFixture) serveONVIF(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	request := string(body)
	op, response := "", ""
	switch {
	case strings.Contains(request, "<t:GetDeviceInformation>"):
		op = "GetDeviceInformation"
		response = "<Manufacturer>TP-Link</Manufacturer><Model>EasyCam C420</Model><FirmwareVersion>" + f.firmware + "</FirmwareVersion><HardwareId>1.0</HardwareId>"
	case strings.Contains(request, "<t:GetServices>"):
		op = "GetServices"
		response = "<Service><Namespace>http://www.onvif.org/ver20/media/wsdl</Namespace><XAddr>" + f.server.URL + "/onvif/device_service</XAddr></Service>"
	case strings.Contains(request, "<t:GetNetworkInterfaces>"):
		op = "GetNetworkInterfaces"
		response = `<NetworkInterfaces token="eth0"><Enabled>true</Enabled><Info><HwAddress>02:00:00:00:00:42</HwAddress></Info></NetworkInterfaces>`
	case strings.Contains(request, "<t:GetProfiles>"):
		op = "GetProfiles"
		// The image prerequisite changes capabilities, not the requested
		// encoder profile. The regular ONVIF planner still has to select 30 fps.
		response = `<Profiles token="main"><Name>main</Name><Configurations><VideoEncoder token="enc"><Encoding>H264</Encoding><Resolution><Width>1920</Width><Height>1080</Height></Resolution><RateControl><FrameRateLimit>` + strconv.Itoa(f.videoFPS) + `</FrameRateLimit><BitrateLimit>4096</BitrateLimit></RateControl><GovLength>25</GovLength></VideoEncoder></Configurations></Profiles>`
	case strings.Contains(request, "<t:GetVideoEncoderConfigurations>"):
		op = "GetVideoEncoderConfigurations"
	case strings.Contains(request, "<t:GetStreamUri>"):
		op = "GetStreamUri"
		response = "<Uri>" + strings.Replace(f.server.URL, "http:", "rtsp:", 1) + "/main</Uri>"
	case strings.Contains(request, "<t:GetVideoEncoderConfigurationOptions>"):
		op = "GetVideoEncoderConfigurationOptions"
		fps := "1 5 10 15 20 25"
		if f.frequency == "60hz" {
			fps += " 30"
		}
		response = `<Options><Encoding>H264</Encoding><ResolutionsAvailable><Width>1920</Width><Height>1080</Height></ResolutionsAvailable><FrameRatesSupported>` + fps + `</FrameRatesSupported><BitrateRange><Min>256</Min><Max>4096</Max></BitrateRange><GovLengthRange><Min>1</Min><Max>150</Max></GovLengthRange></Options>`
	case strings.Contains(request, "<t:SetVideoEncoderConfiguration>"):
		op = "SetVideoEncoderConfiguration"
		var message struct {
			Body struct {
				Set struct {
					Configuration struct {
						Rate struct {
							FPS int `xml:"FrameRateLimit"`
						} `xml:"RateControl"`
					} `xml:"Configuration"`
				} `xml:"SetVideoEncoderConfiguration"`
			} `xml:"Body"`
		}
		if xml.Unmarshal(body, &message) != nil || message.Body.Set.Configuration.Rate.FPS != 30 || f.frequency != "60hz" {
			f.unexpected++
			w.WriteHeader(400)
			return
		}
		f.videoFPS = message.Body.Set.Configuration.Rate.FPS
		f.videoWrites++
	default:
		f.unexpected++
		w.WriteHeader(400)
		return
	}
	_, _ = io.WriteString(w, `<Envelope><Body><`+op+`Response>`+response+`</`+op+`Response></Body></Envelope>`)
}

func TestFrequencyInitializationIsPlannedAndRefreshesMedia2(t *testing.T) {
	f := newInitializationFixture(t)
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	hint := testHint()
	hint.Endpoints = []camera.Endpoint{{URL: f.server.URL + "/onvif/device_service"}}
	before, err := d.Inspect(context.Background(), hint, lease)
	if err != nil {
		t.Fatal(err)
	}
	desired := camera.DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 30, Revision: "preset-1"}
	if _, err := d.Plan(context.Background(), before, desired); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatalf("factory 50Hz must not accept 30fps: %v", err)
	}
	plan, err := d.PlanInitialization(context.Background(), hint, lease, camera.DeviceSettings{PowerLineFrequency: 60})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Before.PowerLineFrequency != 50 || plan.After.PowerLineFrequency != 60 || len(plan.Changes) != 1 || f.writes != 0 {
		t.Fatalf("plan was incomplete or wrote: %+v writes=%d", plan, f.writes)
	}
	observed, err := d.ApplyInitialization(context.Background(), hint, lease, plan)
	if err != nil || observed.PowerLineFrequency != 60 || f.writes != 1 {
		t.Fatalf("frequency not verified: %+v %v writes=%d", observed, err, f.writes)
	}
	after, err := d.Inspect(context.Background(), hint, lease)
	if err != nil {
		t.Fatal(err)
	}
	videoPlan, err := d.Plan(context.Background(), after, desired)
	if err != nil {
		t.Fatalf("refreshed 60Hz capabilities must permit 30fps: %v", err)
	}
	if videoPlan.Before.FPS != 25 || videoPlan.After.FPS != 30 || len(videoPlan.Changes) == 0 {
		t.Fatalf("image initialization bypassed normal encoder planning: %+v", videoPlan)
	}
	if f.logouts != 2 || f.unexpected != 0 {
		t.Fatalf("unexpected operations=%d, completed logouts=%d", f.unexpected, f.logouts)
	}
}

func TestFrequencyInitializationReadbackMismatchAndLostReply(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "readback-mismatch", true: "write-reply-lost"}[lost], func(t *testing.T) {
			f := newInitializationFixture(t)
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			plan, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60})
			if err != nil {
				t.Fatal(err)
			}
			f.ignoreWrite, f.loseWriteReply = !lost, lost
			_, err = d.ApplyInitialization(context.Background(), testHint(), lease, plan)
			want := camera.ErrConflict
			if lost {
				want = onvif.ErrTransport
			}
			if !errors.Is(err, want) || f.writes != 1 {
				t.Fatalf("ambiguous/mismatched response accepted: %v writes=%d", err, f.writes)
			}
			if lost {
				// Resume observes the first write, rather than repeating it or
				// moving to a fallback after an ambiguous transport error.
				observed, err := d.ApplyInitialization(context.Background(), testHint(), lease, plan)
				if err != nil || observed.PowerLineFrequency != 60 || f.writes != 1 {
					t.Fatalf("lost reply caused another write: %+v %v writes=%d", observed, err, f.writes)
				}
			}
			if f.unexpected != 0 || f.logins != f.logouts {
				t.Fatal("unexpected operations or session cleanup missing")
			}
		})
	}
}

func TestFrequencyInitializationNoOpAndCompatibilityGuards(t *testing.T) {
	f := newInitializationFixture(t)
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	if _, err := d.PlanInitialization(context.Background(), camera.DeviceHint{}, nil, camera.DeviceSettings{}); err != nil || f.logins != 0 {
		t.Fatalf("preserved settings contacted camera: %v", err)
	}
	if _, err := d.ApplyInitialization(context.Background(), camera.DeviceHint{}, nil, camera.InitializationPlan{}); err != nil || f.logins != 0 {
		t.Fatalf("empty plan contacted camera: %v", err)
	}
	for _, hint := range []camera.DeviceHint{{}, {Manufacturer: "TP-Link", Model: "EasyCam C420", Hardware: "1.0", Firmware: "unknown"}} {
		if _, err := d.PlanInitialization(context.Background(), hint, lease, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, camera.ErrUnsupported) || f.logins != 0 {
			t.Fatalf("unsupported identity reached web login: %v", err)
		}
	}
	hint := testHint()
	hint.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.9/onvif/device_service"}}
	if _, err := d.PlanInitialization(context.Background(), hint, lease, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, camera.ErrForbidden) || f.logins != 0 {
		t.Fatalf("wrong camera host reached login: %v", err)
	}
	if _, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 55}); !errors.Is(err, camera.ErrInvalid) || f.logins != 0 {
		t.Fatalf("invalid frequency reached login: %v", err)
	}
	f.frequency = "60hz"
	plan, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60})
	if err != nil || len(plan.Changes) != 0 {
		t.Fatal("already-selected frequency produced a change")
	}
	if _, err = d.ApplyInitialization(context.Background(), testHint(), lease, plan); err != nil || f.writes != 0 {
		t.Fatalf("already-selected frequency was written: %v", err)
	}
	plan.Revision = "different-initializer-version"
	logins := f.logins
	if _, err = d.ApplyInitialization(context.Background(), testHint(), lease, plan); !errors.Is(err, camera.ErrConflict) || f.logins != logins {
		t.Fatalf("unknown plan revision reached camera: %v", err)
	}
}

func TestFrequencyWebAuthenticationFailuresAreNotFallbackable(t *testing.T) {
	for _, kind := range []string{"login", "auth-kind", "malformed", "key"} {
		t.Run(kind, func(t *testing.T) {
			f := newInitializationFixture(t)
			f.loginRefused = kind == "login"
			f.unknownAuth = kind == "auth-kind"
			f.malformedReply = kind == "malformed"
			f.badKey = kind == "key"
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			_, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60})
			if err == nil || f.writes != 0 {
				t.Fatalf("failed protocol reached a write: %v", err)
			}
			if kind == "login" && !errors.Is(err, camera.ErrAuthRequired) {
				t.Fatalf("login failure changed class: %v", err)
			}
			if errors.Is(err, camera.ErrUnsupported) {
				t.Fatalf("authentication/protocol failure became a setting fallback: %v", err)
			}
			if strings.Contains(err.Error(), f.password) || strings.Contains(err.Error(), "private-session-token") {
				t.Fatal("credential or session token leaked")
			}
		})
	}
}
