package core

import (
	"bytes"
	"context"
	"strconv"

	"github.com/lesomnus/shale/api"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
)

func (s Core) cameraPresetRules(ctx context.Context, tenant, site []byte, rules []*api.CameraPresetRule) error {
	if _, err := presetwire.Provider("validation", "1", rules); err != nil {
		return invalid("rules", "preset rules violate the portable preset contract")
	}
	for _, rule := range rules {
		if err := cameraToken("rule.id", rule.GetId()); err != nil {
			return err
		}
		q, match := rule.GetScope(), rule.GetMatch()
		if q.GetTenantId() != mustId(tenant).String() || (len(site) != 0 && q.GetSiteId() != mustId(site).String()) {
			return denied("preset scope", "rule crosses the preset's tenant or site")
		}
		for _, text := range []string{match.GetManufacturer(), match.GetModel(), match.GetHardware(), match.GetFirmware(), match.GetSerial(), match.GetMac(), rule.GetSettings().GetVideo().GetProfileToken()} {
			if !cameraPresetText(text) {
				return invalid("rule", "preset matching or profile value is too long or malformed")
			}
		}
		for _, settings := range append([]*api.CameraPresetSettings{rule.GetSettings()}, rule.GetAlternatives().GetCandidates()...) {
			v := settings.GetVideo()
			if !cameraPresetText(v.GetProfileToken()) || !cameraPresetText(v.GetRevision()) || v.GetWidth() > 16384 || v.GetHeight() > 16384 || v.GetFps() > 1000 || v.GetBitrateBps() > 10_000_000_000 || v.GetGopFrames() > 1_000_000 {
				return invalid("settings", "bounded camera settings are required")
			}
		}
		if err := s.cameraPresetSelectors(ctx, tenant, q, match); err != nil {
			return err
		}
	}
	return nil
}

func cameraRuleParent(tenant, site []byte, scope *api.CameraPresetScope) error {
	if scope.GetTenantId() != mustId(tenant).String() || (scope.GetSiteId() != "" && scope.GetSiteId() != cameraPresetOptionalID(site)) {
		return denied("preset scope", "selector belongs to another tenant or site")
	}
	return nil
}

// Every inventory selector is resolved through the operator's normal wall when
// the revision is created. Machine resolution later reads only this validated
// immutable revision under its own job's exact authority.
func (s Core) cameraPresetSelectors(ctx context.Context, tenant []byte, q *api.CameraPresetScope, match *api.CameraPresetMatch) error {
	nx := s.Next()
	var inferredSite []byte
	hasSite := false
	parent := func(parentTenant, parentSite []byte) error {
		if err := cameraRuleParent(parentTenant, parentSite, q); err != nil {
			return err
		}
		if hasSite && !bytes.Equal(inferredSite, parentSite) {
			return denied("preset scope", "inventory selectors belong to different sites")
		}
		hasSite, inferredSite = true, bytes.Clone(parentSite)
		return nil
	}
	if q.GetSiteId() != "" {
		id, err := cameraPresetID(q.GetSiteId(), DomSite)
		if err != nil {
			return err
		}
		site, err := nx.Site().Get(ctx, api.SiteGetRequest_builder{Ref: api.SiteRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(site.GetTenant().GetId(), tenant) {
			return denied("preset scope", "selector belongs to another tenant")
		}
		hasSite, inferredSite = true, bytes.Clone(site.GetId())
	}
	if q.GetSetId() != "" {
		id, err := cameraPresetID(q.GetSetId(), DomSet)
		if err != nil {
			return err
		}
		row, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := parent(row.GetTenant().GetId(), row.GetSite().GetId()); err != nil {
			return err
		}
	}
	if q.GetProducerId() != "" {
		id, err := cameraPresetID(q.GetProducerId(), DomProducer)
		if err != nil {
			return err
		}
		row, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := parent(row.GetTenant().GetId(), row.GetSite().GetId()); err != nil {
			return err
		}
		if q.GetSetId() != "" && q.GetSetId() != cameraPresetOptionalID(row.GetSet().GetId()) {
			return denied("preset scope", "producer and Set selectors disagree")
		}
	}
	if q.GetNetworkId() != "" {
		id, err := cameraPresetID(q.GetNetworkId(), DomCameraNetwork)
		if err != nil {
			return err
		}
		row, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := parent(row.GetTenant().GetId(), row.GetSite().GetId()); err != nil {
			return err
		}
	}
	if match.GetDeviceId() != "" {
		id, err := cameraPresetID(match.GetDeviceId(), DomCameraDevice)
		if err != nil {
			return err
		}
		row, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := parent(row.GetTenant().GetId(), row.GetSite().GetId()); err != nil {
			return err
		}
		if q.GetNetworkId() != "" && q.GetNetworkId() != mustId(row.GetNetwork().GetId()).String() {
			return denied("preset scope", "device and network selectors disagree")
		}
	}
	return nil
}

func cameraPresetOptionalID(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return mustId(id).String()
}

func (s Core) cameraEnrollmentPolicy(ctx context.Context, n *api.CameraNetwork, p *api.CameraEnrollmentPolicy) error {
	if p == nil || p.GetMaxDevices() < 0 || p.GetMaxDevices() > 1024 || len(p.GetEligible()) > 128 {
		return invalid("enrollment", "bounded enrollment policy is required")
	}
	if !cameraPresetText(p.GetInitialUsername()) {
		return invalid("initial_username", "bounded activation username is required")
	}
	if p.GetAutomatic() && (p.GetMaxDevices() == 0 || (!p.GetAllowAny() && len(p.GetEligible()) == 0)) {
		return invalid("enrollment", "automatic enrollment requires a device limit and explicit eligibility")
	}
	if p.GetAllowAny() && len(p.GetEligible()) != 0 {
		return invalid("eligible", "choose selectors or explicit allow_any")
	}
	for i, match := range p.GetEligible() {
		if match == nil || (match.GetDeviceId() == "" && match.GetManufacturer() == "" && match.GetModel() == "" && match.GetHardware() == "" && match.GetFirmware() == "" && match.GetSerial() == "" && match.GetMac() == "") {
			return invalid("eligible", "empty eligibility selectors require explicit allow_any")
		}
		q := api.CameraPresetScope_builder{TenantId: mustId(n.GetTenant().GetId()).String(), NetworkId: mustId(n.GetId()).String()}.Build()
		if len(n.GetSite().GetId()) != 0 {
			q.SetSiteId(mustId(n.GetSite().GetId()).String())
		}
		if err := s.cameraPresetRules(ctx, n.GetTenant().GetId(), n.GetSite().GetId(), []*api.CameraPresetRule{api.CameraPresetRule_builder{Id: "eligible-" + strconv.Itoa(i), Scope: q, Match: match}.Build()}); err != nil {
			return err
		}
	}
	return nil
}
