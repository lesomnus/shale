package cameratest

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lesomnus/shale/camera"
)

// ONVIFCamera is a protocol-backed synthetic camera. Its media endpoint is
// independent: SetVideo changes reported state, not the RTSP fixture's content.
// This makes mismatched advertised/actual profiles observable in integration.
type ONVIFCamera struct {
	URL, WebURL        string
	mu                 sync.Mutex
	server             *httptest.Server
	profile            camera.MediaProfile
	active             bool
	loseActivation     bool
	media2             bool
	media1Codec        string
	username, password string
	calls              []string
	writes             int
	activations        int
	heldSet            *heldConfiguration
	identity           CameraIdentity
	media              *RTSPCamera
	modes              []camera.MediaProfile
	customIdentity     bool
}

type ONVIFOptions struct {
	ListenAddress string
	Identity      CameraIdentity
	Media         *RTSPCamera
	Modes         []camera.MediaProfile
}

type heldConfiguration struct {
	accepted chan struct{}
	release  chan struct{}
	settled  chan struct{}
}

func ServeONVIF(t testing.TB, profile camera.MediaProfile) *ONVIFCamera {
	return ServeONVIFWithOptions(t, profile, ONVIFOptions{})
}

func ServeONVIFWithOptions(t testing.TB, profile camera.MediaProfile, options ONVIFOptions) *ONVIFCamera {
	t.Helper()
	if profile.Token == "" {
		profile.Token = "main"
	}
	if profile.Name == "" {
		profile.Name = "Main"
	}
	if options.ListenAddress == "" {
		options.ListenAddress = "127.0.0.1:0"
	}
	if options.Media != nil {
		profile = options.Media.Profile()
	}
	c := &ONVIFCamera{profile: profile, active: true, media2: true, media1Codec: profile.Codec, identity: defaultIdentity(options.Identity), media: options.Media, modes: append([]camera.MediaProfile(nil), options.Modes...), customIdentity: options.Identity != (CameraIdentity{})}
	lis, err := net.Listen("tcp4", options.ListenAddress)
	if err != nil {
		t.Fatalf("bind synthetic ONVIF: %v", err)
	}
	c.server = httptest.NewUnstartedServer(http.HandlerFunc(c.serve))
	c.server.Listener = lis
	c.server.Start()
	c.URL, c.WebURL = c.server.URL+"/onvif/device_service", c.server.URL
	t.Cleanup(c.close)
	return c
}

func (c *ONVIFCamera) Hint(id camera.DeviceID) camera.DeviceHint {
	c.mu.Lock()
	defer c.mu.Unlock()
	hint := camera.DeviceHint{ID: id, Manufacturer: c.identity.Manufacturer, Model: c.identity.Model, Hardware: c.identity.Hardware, Firmware: c.identity.Firmware, Endpoints: []camera.Endpoint{{URL: c.server.URL + "/onvif/device_service", NetworkScope: "lan"}}}
	if c.customIdentity {
		hint.Serial = c.identity.Serial
		hint.MAC = c.identity.MAC
		hint.DiscoveryID = c.identity.DiscoveryID
	}
	return hint
}
func (c *ONVIFCamera) SetVideo(p camera.MediaProfile) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.profile = p
}
func (c *ONVIFCamera) SetUnactivated(value bool) { c.mu.Lock(); defer c.mu.Unlock(); c.active = !value }
func (c *ONVIFCamera) LoseActivationResponseOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loseActivation = true
}

// HoldConfigurationResponseOnce models firmware which accepted a Set but still
// commits after the HTTP caller has lost its response. Another request can read
// the old state while the device-side operation remains unsettled.
func (c *ONVIFCamera) HoldConfigurationResponseOnce(t testing.TB) (<-chan struct{}, func(), <-chan struct{}) {
	t.Helper()
	h := &heldConfiguration{accepted: make(chan struct{}), release: make(chan struct{}), settled: make(chan struct{})}
	c.mu.Lock()
	c.heldSet = h
	c.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() { close(h.release) })
		select {
		case <-h.accepted:
			<-h.settled
		default:
		}
	}
	t.Cleanup(release)
	return h.accepted, release, h.settled
}
func (c *ONVIFCamera) SetMedia1Codec(codec string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.media1Codec = codec
}
func (c *ONVIFCamera) EnableMedia2(value bool) { c.mu.Lock(); defer c.mu.Unlock(); c.media2 = value }
func (c *ONVIFCamera) RequireCredentials(user, password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.username, c.password = user, password
}
func (c *ONVIFCamera) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}
func (c *ONVIFCamera) Writes() (configuration, activation int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes, c.activations
}

type soapRequest struct {
	Header struct {
		Security struct {
			Token struct{ Username, Password, Nonce, Created string } `xml:"UsernameToken"`
		} `xml:"Security"`
	} `xml:"Header"`
	Body struct {
		Operation struct {
			XMLName       xml.Name
			Configuration encoderConfig `xml:"Configuration"`
		} `xml:",any"`
	} `xml:"Body"`
}
type encoderConfig struct {
	Encoding    string
	Resolution  struct{ Width, Height int }
	RateControl struct {
		FrameRateLimit float64
		BitrateLimit   int64
	}
	GovLength int `xml:"GovLength,attr"`
	H264      struct{ GovLength int }
}

func (c *ONVIFCamera) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(400)
		return
	}
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		c.json(w, data)
		return
	}
	var request soapRequest
	if err := xml.Unmarshal(data, &request); err != nil {
		w.WriteHeader(400)
		return
	}
	name, ns := request.Body.Operation.XMLName.Local, request.Body.Operation.XMLName.Space
	c.calls = append(c.calls, name)
	if !c.active {
		w.WriteHeader(401)
		return
	}
	if c.username != "" {
		token := request.Header.Security.Token
		nonce, err := base64.StdEncoding.DecodeString(token.Nonce)
		h := sha1.New()
		_, _ = h.Write(nonce)
		_, _ = h.Write([]byte(token.Created))
		_, _ = h.Write([]byte(c.password))
		if err != nil || token.Username != c.username || token.Password != base64.StdEncoding.EncodeToString(h.Sum(nil)) {
			w.WriteHeader(401)
			return
		}
	}
	profile := c.profile
	var response string
	const media1 = "http://www.onvif.org/ver10/media/wsdl"
	const media2 = "http://www.onvif.org/ver20/media/wsdl"
	switch name {
	case "GetDeviceInformation":
		response = "<Manufacturer>" + xmlText(c.identity.Manufacturer) + "</Manufacturer><Model>" + xmlText(c.identity.Model) + "</Model><FirmwareVersion>" + xmlText(c.identity.Firmware) + "</FirmwareVersion><SerialNumber>" + xmlText(c.identity.Serial) + "</SerialNumber><HardwareId>" + xmlText(c.identity.Hardware) + "</HardwareId>"
	case "GetNetworkInterfaces":
		response = `<NetworkInterfaces token="eth0"><Enabled>true</Enabled><Info><HwAddress>` + xmlText(c.identity.MAC) + `</HwAddress></Info></NetworkInterfaces>`
	case "GetServices":
		for _, service := range []string{media1, media2} {
			if service == media2 && !c.media2 {
				continue
			}
			response += "<Service><Namespace>" + service + "</Namespace><XAddr>" + c.server.URL + "/onvif/media</XAddr></Service>"
		}
	case "GetProfiles":
		if ns == media1 {
			profile.Codec = c.media1Codec
			response = `<Profiles token="` + xmlText(profile.Token) + `"><Name>` + xmlText(profile.Name) + `</Name><VideoEncoderConfiguration token="encoder">` + encoderXML(profile, false) + `</VideoEncoderConfiguration></Profiles>`
		} else {
			response = `<Profiles token="` + xmlText(profile.Token) + `"><Name>` + xmlText(profile.Name) + `</Name><Configurations><VideoEncoder token="encoder" GovLength="` + strconv.Itoa(profile.GOP) + `" Profile="Main">` + encoderXML(profile, true) + `</VideoEncoder></Configurations></Profiles>`
		}
	case "GetVideoEncoderConfigurations":
		response = `<Configurations token="encoder" GovLength="` + strconv.Itoa(profile.GOP) + `" Profile="Main">` + encoderXML(profile, true) + `</Configurations>`
	case "GetStreamUri":
		if ns == media1 {
			response = "<MediaUri><Uri>" + xmlText(profile.StreamURI) + "</Uri></MediaUri>"
		} else {
			response = "<Uri>" + xmlText(profile.StreamURI) + "</Uri>"
		}
	case "GetVideoEncoderConfigurationOptions":
		resolutions := resolutionXML(profile)
		frames := "30 25 15 10"
		bitrateRange := "<BitrateRange><Min>1</Min><Max>16384</Max></BitrateRange>"
		if c.media != nil {
			frames = strconv.FormatFloat(profile.FPS, 'f', -1, 64)
			bitrateRange = "<BitrateRange><Min>32</Min><Max>8000</Max></BitrateRange>"
		}
		if len(c.modes) > 0 {
			resolutions = ""
			rates := []string{}
			for _, mode := range c.modes {
				resolutions += resolutionXML(mode)
				rates = append(rates, strconv.FormatFloat(mode.FPS, 'f', -1, 64))
			}
			frames = strings.Join(rates, " ")
		}
		if ns == media2 {
			codecs := []string{"H264", "H265"}
			if c.media != nil {
				codecs = []string{"H264"}
			}
			for _, codec := range codecs {
				response += `<Options FrameRatesSupported="` + frames + `" GovLengthRange="1 150"><Encoding>` + codec + "</Encoding><QualityRange><Min>1</Min><Max>5</Max></QualityRange>" + resolutions + bitrateRange + "</Options>"
			}
		} else {
			response = "<Options><H264>" + resolutions + "<FrameRateRange><Min>1</Min><Max>30</Max></FrameRateRange><GovLengthRange><Min>1</Min><Max>150</Max></GovLengthRange></H264><Extension><H264>" + bitrateRange + "</H264></Extension></Options>"
		}
	case "SetVideoEncoderConfiguration":
		x := request.Body.Operation.Configuration
		after := c.profile
		after.Codec = strings.ToLower(x.Encoding)
		if after.Codec == "hevc" {
			after.Codec = "h265"
		}
		after.Width, after.Height = x.Resolution.Width, x.Resolution.Height
		after.FPS = x.RateControl.FrameRateLimit
		after.Bitrate = x.RateControl.BitrateLimit * 1000
		after.GOP = x.GovLength
		if after.GOP == 0 {
			after.GOP = x.H264.GovLength
		}
		if held := c.heldSet; held != nil {
			c.heldSet = nil
			close(held.accepted)
			go func() {
				<-held.release
				c.mu.Lock()
				_ = c.configure(context.Background(), after)
				c.mu.Unlock()
				close(held.settled)
			}()
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		if err := c.configure(r.Context(), after); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	default:
		w.WriteHeader(400)
		return
	}
	w.Header().Set("Content-Type", "application/soap+xml")
	_, _ = fmt.Fprintf(w, `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><t:%sResponse xmlns:t="%s">%s</t:%sResponse></s:Body></s:Envelope>`, name, ns, response, name)
}

func (c *ONVIFCamera) json(w http.ResponseWriter, data []byte) {
	var request struct {
		UserManagement map[string]json.RawMessage `json:"user_management"`
		SetPassword    map[string]string          `json:"set_password"`
	}
	if json.Unmarshal(data, &request) != nil {
		w.WriteHeader(400)
		return
	}
	if _, ok := request.UserManagement["get_encrypt_info"]; ok {
		c.calls = append(c.calls, "get_encrypt_info")
		code := -40405
		if c.active {
			code = -40401
		}
		w.WriteHeader(401)
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -40401, "data": map[string]any{"code": code}})
		return
	}
	if request.SetPassword == nil {
		c.calls = append(c.calls, "unexpected_vendor_write")
		w.WriteHeader(400)
		return
	}
	c.calls = append(c.calls, "set_password")
	if c.active {
		_ = json.NewEncoder(w).Encode(map[string]any{"error_code": -50932})
		return
	}
	encoded, err := url.QueryUnescape(request.SetPassword["ciphertext"])
	if err != nil {
		w.WriteHeader(400)
		return
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	// The real firmware key's private half is not available to a fixture. RSA
	// plaintext fidelity is checked by the driver's private-key protocol unit
	// fixture; this integration fixture validates its 1024-bit wire shape and
	// then requires the prepared credential through actual WS-Security reads.
	if err != nil || len(ciphertext) != 128 || request.SetPassword["username"] != "admin" || request.SetPassword["pwd_sec_type"] != "1" {
		w.WriteHeader(400)
		return
	}
	c.active = true
	c.activations++
	if c.loseActivation {
		c.loseActivation = false
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 0})
}

func xmlText(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
func resolutionXML(p camera.MediaProfile) string {
	return fmt.Sprintf("<ResolutionsAvailable><Width>%d</Width><Height>%d</Height></ResolutionsAvailable>", p.Width, p.Height)
}
func encoderXML(p camera.MediaProfile, media2 bool) string {
	codec := strings.ToUpper(p.Codec)
	if codec == "HEVC" {
		codec = "H265"
	}
	gop := "<GovLength>" + strconv.Itoa(p.GOP) + "</GovLength>"
	if !media2 {
		gop = "<H264>" + gop + "<H264Profile>Main</H264Profile></H264>"
	} else {
		gop = ""
	}
	base := fmt.Sprintf("<Name>%s</Name><UseCount>1</UseCount><Encoding>%s</Encoding><Resolution><Width>%d</Width><Height>%d</Height></Resolution>", xmlText(p.Name), codec, p.Width, p.Height)
	interval := ""
	if !media2 {
		base += "<Quality>4</Quality>"
		interval = "<EncodingInterval>1</EncodingInterval>"
	}
	base += fmt.Sprintf("<RateControl><FrameRateLimit>%s</FrameRateLimit>%s<BitrateLimit>%d</BitrateLimit></RateControl>", strconv.FormatFloat(p.FPS, 'f', -1, 64), interval, p.Bitrate/1000)
	if media2 {
		return base + "<Quality>4</Quality>"
	}
	return base + gop + "<Multicast><Address><Type>IPv4</Type><IPv4Address>0.0.0.0</IPv4Address></Address><Port>0</Port><TTL>1</TTL><AutoStart>false</AutoStart></Multicast><SessionTimeout>PT60S</SessionTimeout>"
}
