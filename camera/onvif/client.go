package onvif

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

var _ camera.Inspector = (*Client)(nil)
var _ camera.VideoConfigurator = (*Client)(nil)

type snapshot struct {
	inspection         camera.Inspection
	service, namespace string
	configs            map[string]*node // profile token -> original encoder configuration
	uses               map[string]int
}

func deviceEndpoint(device camera.DeviceHint) (*url.URL, error) {
	for _, ep := range device.Endpoints {
		if u, err := validatedEndpoint(ep.URL); err == nil {
			return u, nil
		}
	}
	return nil, camera.ErrInvalid
}

// Inspect performs only Get operations. Media2 is authoritative when available;
// a Media1 label is never promoted to independently verified media evidence.
func (c *Client) Inspect(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease) (camera.Inspection, error) {
	var result camera.Inspection
	fn := func(material camera.CredentialMaterial) error {
		s, err := c.inspect(ctx, device, material)
		if err == nil {
			result = s.inspection
		}
		return err
	}
	if lease == nil {
		return result, fn(camera.CredentialMaterial{})
	}
	err := lease.Use(ctx, fn)
	return result, err
}

func (c *Client) inspect(ctx context.Context, device camera.DeviceHint, material camera.CredentialMaterial) (snapshot, error) {
	s := snapshot{configs: map[string]*node{}, uses: map[string]int{}}
	base, err := deviceEndpoint(device)
	if err != nil {
		return s, err
	}
	info, err := c.request(ctx, base.String(), deviceNS, "GetDeviceInformation", "", material)
	if err != nil {
		return s, err
	}
	device.Manufacturer = info.text("Manufacturer")
	device.Model = info.text("Model")
	device.Firmware = info.text("FirmwareVersion")
	device.Serial = info.text("SerialNumber")
	device.Hardware = info.text("HardwareId")
	now := time.Now().UTC()
	evidence := camera.Evidence{Source: "onvif/device", At: now, Firmware: device.Firmware, Authenticated: material.Username != ""}
	device.Evidence = append(device.Evidence, evidence)
	s.inspection = camera.Inspection{Device: device, Activation: camera.ActivationReady, Current: camera.CurrentState{Firmware: device.Firmware, ObservedAt: now}}
	services, err := c.services(ctx, base, material)
	if err != nil {
		return s, err
	}
	for _, ns := range []string{media2NS, media1NS} {
		endpoint := services[ns]
		if endpoint == "" {
			continue
		}
		body := ""
		if ns == media2NS {
			body = "<t:Type>All</t:Type>"
		}
		profiles, e := c.request(ctx, endpoint, ns, "GetProfiles", body, material)
		if errors.Is(e, camera.ErrUnsupported) {
			continue
		}
		if e != nil {
			return s, e
		}
		s.service, s.namespace = endpoint, ns
		var encoders *node
		if ns == media2NS {
			encoders, e = c.request(ctx, endpoint, ns, "GetVideoEncoderConfigurations", "", material)
			if e != nil && !errors.Is(e, camera.ErrUnsupported) {
				return s, e
			}
		}
		for _, p := range profiles.all("Profiles") {
			token := p.attr("token")
			if token == "" {
				continue
			}
			config := p.child("VideoEncoderConfiguration")
			if ns == media2NS {
				config = p.child("Configurations").child("VideoEncoder")
				if config != nil && encoders != nil {
					for _, n := range encoders.all("Configurations") {
						if n.attr("token") == config.attr("token") {
							config = n
							break
						}
					}
				}
			}
			if config == nil {
				continue
			}
			profile := profileFromNode(token, p.text("Name"), config)
			profile.Evidence = []camera.Evidence{{Source: "onvif/" + mediaVersion(ns), At: now, Firmware: device.Firmware, Authenticated: material.Username != ""}}
			uriBody := "<t:Protocol>RTSP</t:Protocol><t:ProfileToken>" + escape(token) + "</t:ProfileToken>"
			if ns == media1NS {
				uriBody = "<t:StreamSetup><tt:Stream>RTP-Unicast</tt:Stream><tt:Transport><tt:Protocol>RTSP</tt:Protocol></tt:Transport></t:StreamSetup><t:ProfileToken>" + escape(token) + "</t:ProfileToken>"
			}
			stream, e := c.request(ctx, endpoint, ns, "GetStreamUri", uriBody, material)
			if e != nil && !errors.Is(e, camera.ErrUnsupported) {
				return s, e
			}
			if e == nil {
				u := stream.text("Uri")
				if u == "" {
					u = stream.child("MediaUri").text("Uri")
				}
				if u != "" {
					profile.StreamURI, e = sameCamera(base, u, true)
					if e != nil {
						return s, e
					}
				}
			}
			s.configs[token] = config
			s.uses[config.attr("token")]++
			s.inspection.Current.Profiles = append(s.inspection.Current.Profiles, profile)
			optionBody := "<t:ConfigurationToken>" + escape(config.attr("token")) + "</t:ConfigurationToken><t:ProfileToken>" + escape(token) + "</t:ProfileToken>"
			if ns == media2NS {
				optionBody = "<t:ConfigurationToken>" + escape(config.attr("token")) + "</t:ConfigurationToken><t:ProfileToken>" + escape(token) + "</t:ProfileToken>"
			}
			options, e := c.request(ctx, endpoint, ns, "GetVideoEncoderConfigurationOptions", optionBody, material)
			if e != nil && !errors.Is(e, camera.ErrUnsupported) {
				return s, e
			}
			if e == nil {
				s.inspection.Capabilities.Profiles = append(s.inspection.Capabilities.Profiles, capabilities(token, options, profile.Evidence)...)
			}
		}
		if len(s.inspection.Current.Profiles) == 0 {
			return s, camera.ErrUnsupported
		}
		return s, nil
	}
	return s, camera.ErrUnsupported
}

func mediaVersion(ns string) string {
	if ns == media2NS {
		return "media2"
	}
	return "media1"
}

func (c *Client) services(ctx context.Context, base *url.URL, material camera.CredentialMaterial) (map[string]string, error) {
	result := map[string]string{}
	r, err := c.request(ctx, base.String(), deviceNS, "GetServices", "<t:IncludeCapability>false</t:IncludeCapability>", material)
	if err == nil {
		for _, service := range r.all("Service") {
			ns := service.text("Namespace")
			if ns != media1NS && ns != media2NS {
				continue
			}
			ep, e := sameCamera(base, service.text("XAddr"), false)
			if e != nil {
				return nil, e
			}
			result[ns] = ep
		}
		return result, nil
	}
	if !errors.Is(err, camera.ErrUnsupported) {
		return nil, err
	}
	r, err = c.request(ctx, base.String(), deviceNS, "GetCapabilities", "<t:Category>Media</t:Category>", material)
	if err != nil {
		return nil, err
	}
	if media := r.find("Media"); media != nil {
		ep, e := sameCamera(base, media.text("XAddr"), false)
		if e != nil {
			return nil, e
		}
		result[media1NS] = ep
	}
	return result, nil
}

func profileFromNode(token, name string, config *node) camera.MediaProfile {
	r := config.child("Resolution")
	rate := config.child("RateControl")
	codec := normalizeCodec(config.text("Encoding"))
	gop := integer(config.text("GovLength"))
	if gop == 0 {
		for _, kind := range []string{"H264", "H265", "MPEG4"} {
			if x := config.child(kind); x != nil {
				gop = integer(x.text("GovLength"))
				break
			}
		}
	}
	return camera.MediaProfile{Token: token, Name: name, Codec: codec, Width: integer(r.text("Width")), Height: integer(r.text("Height")), FPS: decimal(rate.text("FrameRateLimit")), Bitrate: int64(integer(rate.text("BitrateLimit"))) * 1000, GOP: gop}
}

func normalizeCodec(s string) string {
	switch strings.ToUpper(s) {
	case "H264", "H.264", "AVC":
		return "h264"
	case "H265", "H.265", "HEVC":
		return "h265"
	case "JPEG", "MJPEG":
		return "mjpeg"
	default:
		return strings.ToLower(s)
	}
}
func wireCodec(s string) string {
	switch normalizeCodec(s) {
	case "h264":
		return "H264"
	case "h265":
		return "H265"
	case "mjpeg":
		return "JPEG"
	default:
		return strings.ToUpper(s)
	}
}
func integer(s string) int     { v, _ := strconv.Atoi(s); return v }
func decimal(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }

func capabilities(token string, response *node, evidence []camera.Evidence) []camera.ProfileCapability {
	var result []camera.ProfileCapability
	for _, options := range response.all("Options") {
		if codec := options.text("Encoding"); codec != "" {
			result = append(result, capability(token, normalizeCodec(codec), options, evidence))
			continue
		}
		for _, kind := range []string{"H264", "H265", "JPEG", "MPEG4"} {
			if codecOptions := options.child(kind); codecOptions != nil {
				cap := capability(token, normalizeCodec(kind), codecOptions, evidence)
				if extension := options.child("Extension").child(kind); extension != nil {
					if rate := extension.child("BitrateRange"); rate != nil {
						cap.BitrateMin = int64(integer(rate.text("Min"))) * 1000
						cap.BitrateMax = int64(integer(rate.text("Max"))) * 1000
					}
				}
				result = append(result, cap)
			}
		}
	}
	return result
}

func capability(token, codec string, options *node, evidence []camera.Evidence) camera.ProfileCapability {
	c := camera.ProfileCapability{Token: token, Codecs: []string{codec}, Evidence: evidence}
	for _, r := range options.all("ResolutionsAvailable") {
		c.Resolutions = append(c.Resolutions, camera.Resolution{Width: integer(r.text("Width")), Height: integer(r.text("Height"))})
	}
	for _, n := range options.all("FrameRatesSupported") {
		for _, v := range strings.Fields(n.Text) {
			if f := decimal(v); f > 0 {
				c.FrameRates = append(c.FrameRates, f)
			}
		}
	}
	if r := options.child("FrameRateRange"); r != nil {
		c.FrameRateMin = decimal(r.text("Min"))
		c.FrameRateMax = decimal(r.text("Max"))
	}
	if r := options.child("BitrateRange"); r != nil {
		c.BitrateMin = int64(integer(r.text("Min"))) * 1000
		c.BitrateMax = int64(integer(r.text("Max"))) * 1000
	}
	if r := options.child("GovLengthRange"); r != nil {
		c.GOPMin = integer(r.text("Min"))
		c.GOPMax = integer(r.text("Max"))
	}
	return c
}

// Plan is read-only. Unknown option ranges are not treated as permission to
// change a setting. No lower-quality fallback is selected implicitly.
func (c *Client) Plan(ctx context.Context, inspection camera.Inspection, desired camera.DesiredState) (camera.ChangePlan, error) {
	if err := ctx.Err(); err != nil {
		return camera.ChangePlan{}, err
	}
	if desired.Width < 0 || desired.Height < 0 || desired.FPS < 0 || desired.Bitrate < 0 || desired.GOP < 0 || math.IsNaN(desired.FPS) || math.IsInf(desired.FPS, 0) {
		return camera.ChangePlan{}, camera.ErrInvalid
	}
	token := desired.ProfileToken
	if token == "" && len(inspection.Current.Profiles) == 1 {
		token = inspection.Current.Profiles[0].Token
	}
	var before *camera.MediaProfile
	for _, p := range inspection.Current.Profiles {
		if p.Token == token {
			copy := p
			before = &copy
			break
		}
	}
	if before == nil {
		return camera.ChangePlan{}, camera.ErrInvalid
	}
	after := *before
	var changes []string
	if desired.Codec != "" && normalizeCodec(desired.Codec) != before.Codec {
		after.Codec = normalizeCodec(desired.Codec)
		changes = append(changes, "codec")
	}
	if desired.Width != 0 && desired.Width != before.Width {
		after.Width = desired.Width
		changes = append(changes, "width")
	}
	if desired.Height != 0 && desired.Height != before.Height {
		after.Height = desired.Height
		changes = append(changes, "height")
	}
	if desired.FPS != 0 && desired.FPS != before.FPS {
		after.FPS = desired.FPS
		changes = append(changes, "fps")
	}
	if desired.Bitrate != 0 && desired.Bitrate != before.Bitrate {
		after.Bitrate = desired.Bitrate
		changes = append(changes, "bitrate")
	}
	if desired.GOP != 0 && desired.GOP != before.GOP {
		after.GOP = desired.GOP
		changes = append(changes, "gop")
	}
	if len(changes) > 0 {
		valid := false
		for _, cap := range inspection.Capabilities.Profiles {
			if cap.Token == token && slices.Contains(cap.Codecs, after.Codec) && supports(cap, *before, after, changes) {
				valid = true
				break
			}
		}
		if !valid {
			return camera.ChangePlan{}, camera.ErrUnsupported
		}
	}
	return camera.ChangePlan{ProfileToken: token, Before: *before, After: after, Changes: changes, Revision: desired.Revision}, nil
}

func supports(cap camera.ProfileCapability, before, after camera.MediaProfile, changes []string) bool {
	codecChange := slices.Contains(changes, "codec")
	if codecChange || slices.Contains(changes, "width") || slices.Contains(changes, "height") {
		if !slices.Contains(cap.Resolutions, camera.Resolution{Width: after.Width, Height: after.Height}) {
			return false
		}
	}
	if codecChange || slices.Contains(changes, "fps") {
		if !slices.Contains(cap.FrameRates, after.FPS) && (cap.FrameRateMax <= 0 || after.FPS < cap.FrameRateMin || after.FPS > cap.FrameRateMax) {
			return false
		}
	}
	if codecChange || slices.Contains(changes, "bitrate") {
		if after.Bitrate%1000 != 0 {
			return false
		}
		if !slices.Contains(cap.Bitrates, after.Bitrate) && (cap.BitrateMax <= 0 || after.Bitrate < cap.BitrateMin || after.Bitrate > cap.BitrateMax) {
			return false
		}
	}
	if (codecChange && after.GOP > 0) || slices.Contains(changes, "gop") {
		if cap.GOPMax <= 0 || after.GOP < cap.GOPMin || after.GOP > cap.GOPMax {
			return false
		}
	}
	return true
}

func equalVideo(a, b camera.MediaProfile) bool {
	return a.Token == b.Token && a.Codec == b.Codec && a.Width == b.Width && a.Height == b.Height && a.FPS == b.FPS && a.Bitrate == b.Bitrate && a.GOP == b.GOP
}

// Apply rechecks the plan's precondition, preserves unspecified encoder fields,
// writes one encoder only, then reads back. An ambiguous transport failure is
// returned to the workflow; retries re-inspect before attempting another write.
func (c *Client) Apply(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease, plan camera.ChangePlan) (camera.CurrentState, error) {
	var result camera.CurrentState
	if lease == nil {
		return result, camera.ErrAuthRequired
	}
	err := lease.Use(ctx, func(material camera.CredentialMaterial) error {
		s, err := c.inspect(ctx, device, material)
		if err != nil {
			return err
		}
		var actual camera.MediaProfile
		found := false
		for _, p := range s.inspection.Current.Profiles {
			if p.Token == plan.ProfileToken {
				actual = p
				found = true
			}
		}
		if !found {
			return camera.ErrInvalid
		}
		if equalVideo(actual, plan.After) {
			result = s.inspection.Current
			return nil
		}
		if !equalVideo(actual, plan.Before) {
			return camera.ErrConflict
		}
		replanned, err := c.Plan(ctx, s.inspection, camera.DesiredState{ProfileToken: plan.ProfileToken, Codec: plan.After.Codec, Width: plan.After.Width, Height: plan.After.Height, FPS: plan.After.FPS, Bitrate: plan.After.Bitrate, GOP: plan.After.GOP, Revision: plan.Revision})
		if err != nil {
			return err
		}
		if !equalVideo(replanned.After, plan.After) || !slices.Equal(replanned.Changes, plan.Changes) {
			return camera.ErrInvalid
		}
		config := s.configs[plan.ProfileToken]
		if config == nil {
			return camera.ErrUnsupported
		}
		if s.uses[config.attr("token")] > 1 || integer(config.text("UseCount")) > 1 {
			return fmt.Errorf("shared ONVIF encoder: %w", camera.ErrUnsupported)
		}
		if (slices.Contains(plan.Changes, "width") || slices.Contains(plan.Changes, "height")) && config.child("Resolution") == nil {
			return camera.ErrUnsupported
		}
		if (slices.Contains(plan.Changes, "fps") || slices.Contains(plan.Changes, "bitrate")) && config.child("RateControl") == nil {
			return camera.ErrUnsupported
		}
		for _, field := range plan.Changes {
			switch field {
			case "codec":
				if s.namespace == media1NS && config.child(wireCodec(plan.After.Codec)) == nil {
					return camera.ErrUnsupported
				}
				config.set("Encoding", wireCodec(plan.After.Codec))
			case "width":
				config.child("Resolution").set("Width", strconv.Itoa(plan.After.Width))
			case "height":
				config.child("Resolution").set("Height", strconv.Itoa(plan.After.Height))
			case "fps":
				config.child("RateControl").set("FrameRateLimit", strconv.FormatFloat(plan.After.FPS, 'f', -1, 64))
			case "bitrate":
				config.child("RateControl").set("BitrateLimit", strconv.FormatInt(plan.After.Bitrate/1000, 10))
			case "gop":
				if s.namespace == media2NS {
					config.set("GovLength", strconv.Itoa(plan.After.GOP))
				} else {
					x := config.child(wireCodec(plan.After.Codec))
					if x == nil {
						return camera.ErrUnsupported
					}
					x.set("GovLength", strconv.Itoa(plan.After.GOP))
				}
			}
		}
		config.Name = xml.Name{Space: s.namespace, Local: "Configuration"}
		body, err := xml.Marshal(config)
		if err != nil {
			return ErrResponse
		}
		wire := string(body)
		if s.namespace == media1NS {
			wire += "<t:ForcePersistence>true</t:ForcePersistence>"
		}
		if _, err = c.request(ctx, s.service, s.namespace, "SetVideoEncoderConfiguration", wire, material); err != nil {
			return err
		}
		verified, err := c.inspect(ctx, device, material)
		if err != nil {
			return err
		}
		for _, p := range verified.inspection.Current.Profiles {
			if equalVideo(p, plan.After) {
				result = verified.inspection.Current
				return nil
			}
		}
		return ErrVerification
	})
	return result, err
}
