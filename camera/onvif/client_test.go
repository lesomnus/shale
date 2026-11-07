package onvif

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

type cameraFixture struct {
	mu                sync.Mutex
	server            *httptest.Server
	media2            bool
	noMedia           bool
	noOptions         bool
	forbidden         bool
	failReadback      bool
	shared            bool
	digest            bool
	status            int
	endpoint          string
	ops               []string
	writes            int
	config            string
	configAttrs       string
	capabilitiesOnly  bool
	media2Unsupported bool
	interfaces        string
}

func newFixture(t *testing.T, media2 bool) *cameraFixture {
	f := &cameraFixture{media2: media2}
	f.config = `<tt:Name>Main encoder</tt:Name><tt:UseCount>1</tt:UseCount><tt:Encoding>H265</tt:Encoding><tt:Resolution><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:Resolution><tt:Quality>4.5</tt:Quality><tt:RateControl ConstantBitRate="true"><tt:FrameRateLimit>30</tt:FrameRateLimit><tt:BitrateLimit>4096</tt:BitrateLimit></tt:RateControl><tt:GovLength>30</tt:GovLength><tt:Profile>Main</tt:Profile><tt:Multicast><tt:TTL>1</tt:TTL></tt:Multicast>`
	if !media2 {
		f.config = strings.ReplaceAll(f.config, "H265", "H264")
		f.config = strings.ReplaceAll(f.config, "<tt:GovLength>30</tt:GovLength>", "<tt:H264><tt:GovLength>30</tt:GovLength><tt:H264Profile>Main</tt:H264Profile></tt:H264>")
	} else {
		f.config = strings.ReplaceAll(f.config, "<tt:GovLength>30</tt:GovLength>", "")
		f.config = strings.ReplaceAll(f.config, "<tt:Profile>Main</tt:Profile>", "")
		f.config = strings.ReplaceAll(f.config, "<tt:Quality>4.5</tt:Quality>", "") + "<tt:Quality>4.5</tt:Quality>"
		f.configAttrs = ` GovLength="30" Profile="Main"`
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func wrapResponse(operation, ns, inner string) string {
	return `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:tt="` + schemaNS + `" xmlns:t="` + ns + `"><s:Body><t:` + operation + `Response>` + inner + `</t:` + operation + `Response></s:Body></s:Envelope>`
}
func fault(w http.ResponseWriter, name string) {
	w.WriteHeader(500)
	_, _ = io.WriteString(w, `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><s:Fault><s:Code><s:Value>s:Sender</s:Value><s:Subcode><s:Value>ter:`+name+`</s:Value></s:Subcode></s:Code><s:Reason><s:Text>secret must not be echoed</s:Text></s:Reason></s:Fault></s:Body></s:Envelope>`)
}

func fixtureDigestValid(req *http.Request) bool {
	params := map[string]string{}
	for _, m := range challengeParam.FindAllStringSubmatch(req.Header.Get("Authorization"), -1) {
		v := m[2]
		if v == "" {
			v = m[3]
		}
		params[m[1]] = v
	}
	hash := func(s string) string { sum := md5.Sum([]byte(s)); return hex.EncodeToString(sum[:]) }
	want := hash(hash("operator:fixture:reserved @:/?#% password") + ":server-nonce:" + params["nc"] + ":" + params["cnonce"] + ":auth:" + hash(req.Method+":"+req.URL.RequestURI()))
	return params["response"] == want && params["username"] == "operator" && params["uri"] == req.URL.RequestURI()
}

func (f *cameraFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status > 0 {
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, "reserved @:/?#% password")
		return
	}
	if f.digest && !fixtureDigestValid(r) {
		w.Header().Set("WWW-Authenticate", `Digest realm="fixture", nonce="server-nonce", algorithm=MD5, qop="auth"`)
		w.WriteHeader(401)
		return
	}
	data, _ := io.ReadAll(r.Body)
	root, err := parseXML(data)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	body := root.find("Body")
	if body == nil || len(body.Children) != 1 {
		w.WriteHeader(400)
		return
	}
	op := body.Children[0]
	name := op.Name.Local
	ns := op.Name.Space
	f.ops = append(f.ops, name)
	var inner string
	switch name {
	case "GetDeviceInformation":
		inner = "<t:Manufacturer>TP-Link</t:Manufacturer><t:Model>EasyCam C420</t:Model><t:FirmwareVersion>1.2.2 Build260624</t:FirmwareVersion><t:SerialNumber>serial-A</t:SerialNumber><t:HardwareId>1.0</t:HardwareId>"
	case "GetNetworkInterfaces":
		if f.interfaces == "" {
			fault(w, "ActionNotSupported")
			return
		}
		inner = f.interfaces
	case "GetServices":
		if f.capabilitiesOnly {
			fault(w, "ActionNotSupported")
			return
		}
		if f.noMedia {
			break
		}
		ep := f.server.URL + "/media"
		if f.endpoint != "" {
			ep = f.endpoint
		}
		inner = "<t:Service><t:Namespace>" + media1NS + "</t:Namespace><t:XAddr>" + ep + "</t:XAddr></t:Service>"
		if f.media2 {
			inner += "<t:Service><t:Namespace>" + media2NS + "</t:Namespace><t:XAddr>" + ep + "</t:XAddr></t:Service>"
		}
	case "GetCapabilities":
		inner = "<t:Capabilities><tt:Media><tt:XAddr>" + f.server.URL + "/media</tt:XAddr></tt:Media></t:Capabilities>"
	case "GetProfiles":
		if ns == media2NS && f.media2Unsupported {
			fault(w, "ActionNotSupported")
			return
		}
		if ns == media2NS {
			inner = `<t:Profiles token="profile_1"><t:Name>Main</t:Name><t:Configurations><t:VideoEncoder token="config_1"` + f.configAttrs + `>` + f.config + `</t:VideoEncoder></t:Configurations></t:Profiles>`
		} else {
			inner = `<t:Profiles token="profile_1"><tt:Name>Main</tt:Name><tt:VideoEncoderConfiguration token="config_1">` + strings.ReplaceAll(f.config, "H265", "H264") + `</tt:VideoEncoderConfiguration></t:Profiles>`
		}
		if f.shared {
			inner += strings.ReplaceAll(inner, "profile_1", "profile_2")
		}
	case "GetVideoEncoderConfigurations":
		inner = `<t:Configurations token="config_1"` + f.configAttrs + `>` + f.config + `</t:Configurations>`
	case "GetStreamUri":
		uri := strings.Replace(f.server.URL, "http:", "rtsp:", 1) + "/stream1"
		if ns == media2NS {
			inner = "<t:Uri>" + uri + "</t:Uri>"
		} else {
			inner = "<t:MediaUri><tt:Uri>" + uri + "</tt:Uri></t:MediaUri>"
		}
	case "GetVideoEncoderConfigurationOptions":
		if f.noOptions {
			fault(w, "ActionNotSupported")
			return
		}
		if ns == media2NS {
			for _, codec := range []string{"H264", "H265"} {
				inner += `<t:Options FrameRatesSupported="30 25 15" GovLengthRange="1 150"><tt:Encoding>` + codec + "</tt:Encoding><tt:QualityRange><tt:Min>1</tt:Min><tt:Max>5</tt:Max></tt:QualityRange><tt:ResolutionsAvailable><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:ResolutionsAvailable><tt:ResolutionsAvailable><tt:Width>1280</tt:Width><tt:Height>720</tt:Height></tt:ResolutionsAvailable><tt:BitrateRange><tt:Min>64</tt:Min><tt:Max>8192</tt:Max></tt:BitrateRange></t:Options>"
			}
		} else {
			inner = "<t:Options><tt:H264><tt:ResolutionsAvailable><tt:Width>1920</tt:Width><tt:Height>1080</tt:Height></tt:ResolutionsAvailable><tt:FrameRateRange><tt:Min>1</tt:Min><tt:Max>30</tt:Max></tt:FrameRateRange><tt:GovLengthRange><tt:Min>1</tt:Min><tt:Max>150</tt:Max></tt:GovLengthRange></tt:H264><tt:Extension><tt:H264><tt:BitrateRange><tt:Min>64</tt:Min><tt:Max>8192</tt:Max></tt:BitrateRange></tt:H264></tt:Extension></t:Options>"
		}
	case "SetVideoEncoderConfiguration":
		if f.forbidden {
			fault(w, "OperationProhibited")
			return
		}
		f.writes++
		if !f.failReadback {
			config := op.child("Configuration")
			var b strings.Builder
			for _, child := range config.Children {
				data, _ := xml.Marshal(child)
				b.Write(data)
			}
			f.config = b.String()
			if ns == media2NS {
				f.configAttrs = ` GovLength="` + config.attr("GovLength") + `" Profile="` + config.attr("Profile") + `"`
			}
		}
	default:
		fault(w, "ActionNotSupported")
		return
	}
	w.Header().Set("Content-Type", "application/soap+xml")
	_, _ = io.WriteString(w, wrapResponse(name, ns, inner))
}

func fixtureLease() camera.CredentialLease {
	return camera.NewCredentialLease(camera.CredentialRef{Provider: "fixture", ID: "secret", Version: "1", Purpose: camera.AuthManagement}, camera.CredentialMaterial{Username: "operator", Password: "reserved @:/?#% password"}, time.Now().Add(time.Minute))
}
func (f *cameraFixture) device() camera.DeviceHint {
	return camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: f.server.URL + "/device", NetworkScope: "lan-a"}}}
}

func TestMedia2PreferredOverMisreportedMedia1(t *testing.T) {
	f := newFixture(t, true)
	f.digest = true
	client := &Client{}
	lease := fixtureLease()
	defer lease.Close()
	inspection, err := client.Inspect(context.Background(), f.device(), lease)
	if err != nil {
		t.Fatal(err)
	}
	profile := inspection.Current.Profiles[0]
	if profile.Codec != "h265" || profile.Width != 1920 || profile.FPS != 30 || profile.Bitrate != 4096000 || profile.GOP != 30 {
		t.Fatalf("wrong normalized profile: %+v", profile)
	}
	if profile.Evidence[0].Source != "onvif/media2" || !profile.Evidence[0].Authenticated {
		t.Fatal("missing evidence")
	}
	if inspection.Device.Model != "EasyCam C420" || inspection.Device.Firmware != "1.2.2 Build260624" {
		t.Fatal("missing identity")
	}
	if f.writes != 0 {
		t.Fatal("inspection performed a write")
	}
	for _, operation := range f.ops {
		if !strings.HasPrefix(operation, "Get") {
			t.Fatalf("inspection write: %s", operation)
		}
	}
	if len(inspection.Capabilities.Profiles) != 2 {
		t.Fatal("missing codec-specific options")
	}
}

func TestMedia1AndPartialCapabilities(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			f := newFixture(t, false)
			f.noOptions = partial
			client := &Client{}
			lease := fixtureLease()
			defer lease.Close()
			i, err := client.Inspect(context.Background(), f.device(), lease)
			if err != nil {
				t.Fatal(err)
			}
			if i.Current.Profiles[0].Codec != "h264" || i.Current.Profiles[0].Evidence[0].Source != "onvif/media1" {
				t.Fatal("missing deliberate Media1 fallback")
			}
			_, err = client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1", FPS: 25, Bitrate: 2048000})
			if partial && !errors.Is(err, camera.ErrUnsupported) {
				t.Fatalf("unknown options allowed write: %v", err)
			}
			if !partial && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMedia1EncodingIntervalIsPartOfActualFrameRate(t *testing.T) {
	f := newFixture(t, false)
	f.config = strings.ReplaceAll(f.config, "<tt:FrameRateLimit>30</tt:FrameRateLimit>", "<tt:FrameRateLimit>30</tt:FrameRateLimit><tt:EncodingInterval>2</tt:EncodingInterval>")
	client := &Client{}
	lease := fixtureLease()
	defer lease.Close()
	i, err := client.Inspect(context.Background(), f.device(), lease)
	if err != nil {
		t.Fatal(err)
	}
	if i.Current.Profiles[0].FPS != 15 {
		t.Fatal("encoded frame rate ignores EncodingInterval")
	}
	if _, err := client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1", FPS: 25}); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatal("plan silently changes encoding interval")
	}
	p, err := client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1", FPS: 10})
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.Apply(context.Background(), f.device(), lease, p)
	if err != nil {
		t.Fatal(err)
	}
	if state.Profiles[0].FPS != 10 || !strings.Contains(f.config, ">20</") || !strings.Contains(f.config, ">2</") {
		t.Fatal("FrameRateLimit was not adjusted while preserving EncodingInterval")
	}
}

func TestDeliberateFallbacksForOlderServices(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			f := newFixture(t, !legacy)
			f.capabilitiesOnly, f.media2Unsupported = legacy, !legacy
			lease := fixtureLease()
			defer lease.Close()
			i, err := (&Client{}).Inspect(context.Background(), f.device(), lease)
			if err != nil {
				t.Fatal(err)
			}
			if i.Current.Profiles[0].Evidence[0].Source != "onvif/media1" || i.Current.Profiles[0].Codec != "h264" {
				t.Fatal("legacy service fallback did not preserve Media1 provenance")
			}
		})
	}
}

func TestApplyReadbackNoopStaleAndPreserveFields(t *testing.T) {
	f := newFixture(t, true)
	client := &Client{}
	lease := fixtureLease()
	defer lease.Close()
	i, err := client.Inspect(context.Background(), f.device(), lease)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1", Codec: "h264", Width: 1280, Height: 720, FPS: 25, Bitrate: 2048000, GOP: 50, Revision: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.writes != 0 {
		t.Fatal("planning wrote config")
	}
	state, err := client.Apply(context.Background(), f.device(), lease, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !equalVideo(state.Profiles[0], plan.After) || f.writes != 1 {
		t.Fatal("not applied/read back")
	}
	if !strings.Contains(f.config, "4.5") || !strings.Contains(f.config, `ConstantBitRate="true"`) || !strings.Contains(f.config, "Multicast") {
		t.Fatal("unrelated configuration lost")
	}
	_, err = client.Apply(context.Background(), f.device(), lease, plan)
	if err != nil || f.writes != 1 {
		t.Fatalf("repeated apply was not idempotent: %v", err)
	}
	stale := plan
	stale.After.FPS = 15
	_, err = client.Apply(context.Background(), f.device(), lease, stale)
	if !errors.Is(err, camera.ErrConflict) || f.writes != 1 {
		t.Fatalf("stale plan accepted: %v", err)
	}
}

func TestFailedAuthorizationMissingServicesAndSafeErrors(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*cameraFixture)
		want  error
	}{
		{"auth", func(f *cameraFixture) { f.status = 401 }, camera.ErrAuthRequired},
		{"forbidden", func(f *cameraFixture) { f.status = 403 }, camera.ErrForbidden},
		{"no media", func(f *cameraFixture) { f.noMedia = true }, camera.ErrUnsupported},
		{"cross host", func(f *cameraFixture) { f.endpoint = "http://other-camera.invalid/media" }, ErrResponse},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, true)
			test.setup(f)
			lease := fixtureLease()
			defer lease.Close()
			_, err := (&Client{}).Inspect(context.Background(), f.device(), lease)
			if !errors.Is(err, test.want) {
				t.Fatalf("want %v got %v", test.want, err)
			}
			if strings.Contains(err.Error(), "reserved") {
				t.Fatal("secret in error")
			}
		})
	}
}

func TestWriteForbiddenSharedEncoderAndReadbackMismatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*cameraFixture)
		want  error
	}{
		{"forbidden", func(f *cameraFixture) { f.forbidden = true }, camera.ErrForbidden},
		{"shared", func(f *cameraFixture) { f.shared = true }, camera.ErrUnsupported},
		{"readback", func(f *cameraFixture) { f.failReadback = true }, ErrVerification},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, true)
			test.setup(f)
			client := &Client{}
			lease := fixtureLease()
			defer lease.Close()
			i, err := client.Inspect(context.Background(), f.device(), lease)
			if err != nil {
				t.Fatal(err)
			}
			p, err := client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1", FPS: 25})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Apply(context.Background(), f.device(), lease, p)
			if !errors.Is(err, test.want) {
				t.Fatalf("want %v got %v", test.want, err)
			}
		})
	}
}

func TestWSSecurityDigestAndRequestCancellation(t *testing.T) {
	m := camera.CredentialMaterial{Username: "operator", Password: "reserved @:/?#% password"}
	x, err := security(m)
	if err != nil {
		t.Fatal(err)
	}
	n, err := parseXML([]byte(x))
	if err != nil {
		t.Fatal(err)
	}
	securityToken := n.child("UsernameToken")
	nonce, _ := base64.StdEncoding.DecodeString(securityToken.text("Nonce"))
	created := securityToken.text("Created")
	hash := sha1.New()
	hash.Write(nonce)
	hash.Write([]byte(created))
	hash.Write([]byte(m.Password))
	if securityToken.text("Password") != base64.StdEncoding.EncodeToString(hash.Sum(nil)) {
		t.Fatal("invalid WS-Security digest")
	}
	if strings.Contains(x, m.Password) {
		t.Fatal("password sent in WSSE plaintext")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFixture(t, true)
	_, err = (&Client{}).Inspect(ctx, f.device(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSetNotAuthorizedIsPermissionFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fault(w, "NotAuthorized") }))
	defer server.Close()
	client := &Client{}
	_, err := client.request(context.Background(), server.URL, media2NS, "SetVideoEncoderConfiguration", "", camera.CredentialMaterial{Username: "operator", Password: "reserved password"})
	if !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("write permission failure misclassified: %v", err)
	}
	_, err = client.request(context.Background(), server.URL, media2NS, "GetProfiles", "", camera.CredentialMaterial{Username: "operator", Password: "reserved password"})
	if !errors.Is(err, camera.ErrAuthRequired) {
		t.Fatalf("read authentication failure misclassified: %v", err)
	}
}

func TestPreservedEncoderExtensionsRetainQNameNamespaceDeclarations(t *testing.T) {
	config, err := parseXML([]byte(`<Configurations xmlns="http://www.onvif.org/ver20/media/wsdl" xmlns:vnd="urn:synthetic-vendor" token="encoder" GovLength="30"><Extension xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="vnd:EncoderMode"><vnd:Behavior>unchanged</vnd:Behavior></Extension></Configurations>`))
	if err != nil {
		t.Fatal(err)
	}
	config.Name = xml.Name{Space: media2NS, Local: "Configuration"}
	config.setAttr("GovLength", "50")
	body, err := xml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `xmlns:vnd="urn:synthetic-vendor"`) || !strings.Contains(string(body), `type="vnd:EncoderMode"`) {
		t.Fatal("encoder update discarded a namespace used by a vendor QName")
	}
	parsed, err := parseXML(body)
	if err != nil || parsed.find("Behavior").Name.Space != "urn:synthetic-vendor" {
		t.Fatal("preserved vendor extension changed namespace")
	}
}

func TestPlansAreCredentialFreeAndRejectUnsupportedValues(t *testing.T) {
	f := newFixture(t, true)
	client := &Client{}
	lease := fixtureLease()
	defer lease.Close()
	i, err := client.Inspect(context.Background(), f.device(), lease)
	if err != nil {
		t.Fatal(err)
	}
	for _, desired := range []camera.DesiredState{{FPS: 60}, {Width: 64, Height: 64}, {Codec: "av1"}, {Bitrate: 1500}, {GOP: 1000}} {
		desired.ProfileToken = "profile_1"
		_, err := client.Plan(context.Background(), i, desired)
		if !errors.Is(err, camera.ErrUnsupported) {
			t.Fatalf("unsupported accepted: %+v %v", desired, err)
		}
	}
	p, err := client.Plan(context.Background(), i, camera.DesiredState{ProfileToken: "profile_1"})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(p)
	if strings.Contains(string(data), "reserved") || strings.Contains(string(data), "operator") {
		t.Fatal("plan contains credentials")
	}
	if !slices.Equal(p.Changes, []string(nil)) {
		t.Fatal("unexpected no-op changes")
	}
}
