package core

import (
	"context"
	"strings"
	"sync"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/reader"
	"github.com/lesomnus/shale/internal/ent/sitemember"
)

// The two surfaces' policies (§35.2). payday supplies the wall; what each
// kind of caller may do on each surface is Shale's to say, and it is said
// here rather than in a configuration file.

// clusterServices are served on the cluster API only. No flag mounts them
// into `shale serve control`.
var clusterServices = []string{
	"/shale.NodeService/", "/shale.RelayService/", "/shale.DeviceService/", "/shale.SinkService/",
	"/shale.SigningKeyService/", "/shale.PlacementPolicyService/", "/shale.UploadPolicyService/",
	"/shale.AddressPolicyService/", "/shale.TenantService/", "/shale.OutboxService/",
}

// tenantServices are the tenant API.
var tenantServices = []string{
	"/shale.SetService/", "/shale.SourceService/", "/shale.ObjectService/", "/shale.AttemptService/",
	"/shale.SiteService/", "/shale.SiteMemberService/", "/shale.ProducerService/", "/shale.ReaderService/",
	"/shale.HolderService/", "/shale.AuditService/",
}

// IsClusterService says whether a method belongs to the cluster surface.
func IsClusterService(method string) bool {
	for _, p := range clusterServices {
		if strings.HasPrefix(method, p) {
			return true
		}
	}

	return false
}

// IsTenantService says whether a method belongs to the tenant surface.
func IsTenantService(method string) bool {
	for _, p := range tenantServices {
		if strings.HasPrefix(method, p) {
			return true
		}
	}

	return false
}

// Public are the methods served without a credential: health, reflection,
// and every host's Join (§35.2).
func Public(method string) bool {
	switch method {
	case api.ProducerService_Join_FullMethodName, api.ReaderService_Join_FullMethodName,
		api.NodeService_Join_FullMethodName, api.RelayService_Join_FullMethodName:
		return true
	}

	return strings.HasPrefix(method, "/grpc.health.v1.Health/") || strings.HasPrefix(method, "/grpc.reflection.")
}

func denied(method, why string) error {
	return status.Errorf(codes.PermissionDenied, "%s: %s", method, why)
}

// closedToEveryone are generated verbs no caller may use on the tenant
// surface: rows the system writes (§35.3).
func closedToEveryone(m string) bool {
	switch {
	case strings.HasPrefix(m, "/shale.ObjectService/"):
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Apply") || strings.HasSuffix(m, "/Erase")
	case strings.HasPrefix(m, "/shale.AttemptService/"):
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Apply") || strings.HasSuffix(m, "/Erase")
	case strings.HasPrefix(m, "/shale.ProducerService/"), strings.HasPrefix(m, "/shale.ReaderService/"),
		strings.HasPrefix(m, "/shale.NodeService/"), strings.HasPrefix(m, "/shale.DeviceService/"),
		strings.HasPrefix(m, "/shale.SinkService/"), strings.HasPrefix(m, "/shale.SigningKeyService/"):
		// Hosts, devices, sinks, and keys are written by the system and by
		// the custom verbs of §32 (adopt, quarantine, retire, rotate, ...);
		// a general write could set a state nothing else agrees with.
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Apply") || strings.HasSuffix(m, "/Patch")
	case strings.HasPrefix(m, "/shale.RelayService/"):
		// `relay patch` is the operator's, for labels (§39.2).
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Apply")
	}

	return false
}

// TenantPolicy is the tenant API: people see their tenant; a producer or a
// reader sees its tenant and only the calls it needs.
type TenantPolicy struct{}

var producerMay = map[string]bool{
	api.SetService_Get_FullMethodName: true, api.SetService_List_FullMethodName: true,
	api.SetService_Negotiate_FullMethodName: true, api.SetService_Allocate_FullMethodName: true,
	api.SourceService_Get_FullMethodName: true, api.SourceService_List_FullMethodName: true,
	api.SourceService_Add_FullMethodName: true,
	api.ObjectService_Get_FullMethodName: true, api.ObjectService_Allocate_FullMethodName: true,
	api.ObjectService_Reallocate_FullMethodName: true, api.ObjectService_Renew_FullMethodName: true,
	api.ObjectService_ReportAttempt_FullMethodName: true, api.ObjectService_ReportFailure_FullMethodName: true,
	api.AttemptService_Get_FullMethodName:  true,
	api.ProducerService_Get_FullMethodName: true, api.ProducerService_Heartbeat_FullMethodName: true,
	api.ProducerService_Relay_FullMethodName: true, api.ProducerService_RenewCertificate_FullMethodName: true,
}

var readerMay = map[string]bool{
	api.SetService_Get_FullMethodName: true, api.SetService_List_FullMethodName: true, api.SetService_Live_FullMethodName: true,
	api.SourceService_Get_FullMethodName: true, api.SourceService_List_FullMethodName: true, api.SourceService_Live_FullMethodName: true,
	api.ObjectService_Get_FullMethodName: true, api.ObjectService_List_FullMethodName: true, api.ObjectService_Timeline_FullMethodName: true,
	api.SiteService_Get_FullMethodName: true, api.SiteService_List_FullMethodName: true,
	api.ReaderService_Get_FullMethodName: true, api.ReaderService_Heartbeat_FullMethodName: true,
	api.ReaderService_RenewCertificate_FullMethodName: true,
}

func (TenantPolicy) May(ctx context.Context, c gate.Call) error {
	m := c.Action
	if strings.HasPrefix(m, "/payday.BatchService/") {
		return nil
	}
	if IsClusterService(m) {
		return denied(m, "not served on the tenant API")
	}
	if closedToEveryone(m) {
		return denied(m, "written by the system, not by a caller")
	}

	switch kindOf(c.Actor) {
	case DomProducer:
		if !producerMay[m] {
			return denied(m, "not something a producer does")
		}
	case DomReader:
		if !readerMay[m] {
			return denied(m, "not something a reader does")
		}
	case DomHolder:
		// A person may do the rest, inside their tenant; giving somebody a
		// password is an administrator's act (§33.1).
		if m == api.HolderService_IssuePassword_FullMethodName && !seesEverySite(ctx) {
			return denied(m, "only a person who sees every site issues a password")
		}
	default:
		return denied(m, "not served to this kind of host")
	}

	return nil
}

// seesEverySite says whether the caller is a person with all_sites, from
// the row the resolver put in the frame.
func seesEverySite(ctx context.Context) bool {
	f, ok := frame.From(ctx)
	if !ok {
		return false
	}
	row, ok := f.Row.(*api.Holder)

	return ok && row.GetAllSites()
}

func (TenantPolicy) Where(_ context.Context, c gate.Call) (frame.Tenants, error) {
	return frame.Only(c.Tenant), nil
}

// ClusterPolicy is the cluster API: operators from the cluster's tenant see
// everything; nodes and relays do their own work and nothing more.
type ClusterPolicy struct {
	ClusterTenant pdid.Id
}

var nodeMay = map[string]bool{
	api.NodeService_Get_FullMethodName: true, api.NodeService_Heartbeat_FullMethodName: true,
	api.NodeService_PushEvents_FullMethodName: true, api.NodeService_RenewCertificate_FullMethodName: true,
	api.NodeService_Resolve_FullMethodName: true,
	api.SinkService_Get_FullMethodName:     true, api.SinkService_List_FullMethodName: true, api.SinkService_ProposeGc_FullMethodName: true,
	api.DeviceService_Get_FullMethodName: true, api.DeviceService_List_FullMethodName: true,
	api.SigningKeyService_Get_FullMethodName: true, api.SigningKeyService_List_FullMethodName: true, api.SigningKeyService_Watch_FullMethodName: true,
}

var relayMay = map[string]bool{
	api.RelayService_Get_FullMethodName: true, api.RelayService_Heartbeat_FullMethodName: true,
	api.RelayService_RenewCertificate_FullMethodName: true,
	api.SigningKeyService_Get_FullMethodName:         true, api.SigningKeyService_List_FullMethodName: true, api.SigningKeyService_Watch_FullMethodName: true,
}

func (p ClusterPolicy) May(_ context.Context, c gate.Call) error {
	m := c.Action
	if strings.HasPrefix(m, "/payday.BatchService/") {
		return nil
	}
	switch kindOf(c.Actor) {
	case DomNode:
		if !nodeMay[m] {
			return denied(m, "not something a node does")
		}
	case DomRelay:
		if !relayMay[m] {
			return denied(m, "not something a relay does")
		}
	case DomHolder:
		if !p.ClusterTenant.IsZero() && c.Tenant != p.ClusterTenant {
			return denied(m, "the cluster API serves cluster operators")
		}
		if strings.HasPrefix(m, "/shale.SigningKeyService/") && (strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Apply")) {
			return denied(m, "keys are made by Rotate")
		}
	default:
		return denied(m, "not served to this kind of host")
	}

	return nil
}

func (ClusterPolicy) Where(_ context.Context, c gate.Call) (frame.Tenants, error) {
	return frame.Everything, nil
}

// Sites answers which sites a caller may see (§33.1): a person or a reader
// with all_sites, or a node, sees every site; otherwise the SiteMember
// rows; a producer sees its set's site.
type Sites struct {
	d *Deps

	mu    sync.Mutex
	cache map[pdid.Id]sitesEntry
}

type sitesEntry struct {
	at  time.Time
	all bool
	ids []pdid.Id
}

const sitesTTL = 10 * time.Second

// NewSites makes the membership lookup.
func NewSites(d *Deps) *Sites {
	return &Sites{d: d, cache: map[pdid.Id]sitesEntry{}}
}

// Of is the frame.Sets payday's Grouped scope reads.
func (s *Sites) Of(ctx context.Context) (vs []uuid.UUID, all bool, err error) {
	f, ok := frame.From(ctx)
	if !ok {
		return nil, false, status.Error(codes.Unauthenticated, "no frame")
	}
	now := s.d.now()

	s.mu.Lock()
	e, ok := s.cache[f.Actor]
	s.mu.Unlock()
	if ok && now.Sub(e.at) < sitesTTL {
		return uuidsOf(e.ids, pdid.Id.Uuid), e.all, nil
	}

	e = sitesEntry{at: now}
	switch kindOf(f.Actor) {
	case DomHolder:
		row, ok := f.Row.(*api.Holder)
		if ok && row.GetAllSites() {
			e.all = true
			break
		}
		if !ok {
			h, err := s.d.Own.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
			if err != nil {
				return nil, false, err
			}
			if h.GetAllSites() {
				e.all = true
				break
			}
		}
		ms, err := s.d.Ent.SiteMember.Query().Where(sitemember.HolderIdEQ(f.Actor.Uuid())).All(ctx)
		if err != nil {
			return nil, false, err
		}
		for _, m := range ms {
			e.ids = append(e.ids, pdid.Id(m.SiteId))
		}
	case DomReader:
		r, err := s.d.Ent.Reader.Query().Where(reader.IdEQ(f.Actor.Uuid())).Only(ctx)
		if err != nil {
			return nil, false, err
		}
		if r.AllSites {
			e.all = true
			break
		}
		ms, err := s.d.Ent.SiteMember.Query().Where(sitemember.ReaderIdEQ(f.Actor.Uuid())).All(ctx)
		if err != nil {
			return nil, false, err
		}
		for _, m := range ms {
			e.ids = append(e.ids, pdid.Id(m.SiteId))
		}
	case DomProducer:
		p, err := s.d.Own.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, false, err
		}
		if len(p.GetSite().GetId()) == 0 {
			e.all = true
		} else {
			e.ids = []pdid.Id{mustId(p.GetSite().GetId())}
		}
	default:
		e.all = true
	}

	s.mu.Lock()
	s.cache[f.Actor] = e
	s.mu.Unlock()

	return uuidsOf(e.ids, pdid.Id.Uuid), e.all, nil
}
