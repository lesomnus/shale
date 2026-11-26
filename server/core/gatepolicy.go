package core

import (
	"context"
	"slices"
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
	"github.com/lesomnus/shale/internal/identity"
)

// The two surfaces' policies (§35.2). payday supplies the wall; what each
// kind of caller may do on each surface is Shale's to say, and it is said
// here rather than in a configuration file.

// clusterServices are served on the cluster API only. No flag mounts them
// into `shale serve control`.
var clusterServices = []string{
	"/shale.NodeService/", "/shale.RelayService/", "/shale.SatelliteService/", "/shale.DeviceService/", "/shale.SinkService/",
	"/shale.SigningKeyService/", "/shale.PlacementPolicyService/", "/shale.UploadPolicyService/",
	"/shale.AddressPolicyService/", "/shale.TenantService/", "/shale.OutboxService/",
	"/shale.CertificateAuthorityService/",
}

// tenantServices are the tenant API.
var tenantServices = []string{
	"/shale.SetService/", "/shale.SourceService/", "/shale.LaminaService/", "/shale.AttemptService/",
	"/shale.SiteService/", "/shale.SiteMemberService/", "/shale.ProducerService/", "/shale.ReaderService/",
	"/shale.HolderService/", "/shale.AuditService/", "/shale.DelegationService/",
	"/shale.CameraNetworkService/", "/shale.CameraDeviceService/", "/shale.CameraAssignmentService/",
	"/shale.CameraBindingService/", "/shale.CameraJobService/", "/shale.CameraWorkerService/",
	"/shale.CameraPresetService/", "/shale.CameraPolicyService/",
	"/shale.CameraCredentialService/", "/shale.CameraCredentialVersionService/",
	"/shale.ReachService/",
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
	case api.ProducerService_Join_FullMethodName, api.ReaderService_Join_FullMethodName, api.CameraWorkerService_Join_FullMethodName,
		api.NodeService_Join_FullMethodName, api.RelayService_Join_FullMethodName, api.SatelliteService_Join_FullMethodName:
		return true
	}

	return strings.HasPrefix(method, "/grpc.health.v1.Health/") || strings.HasPrefix(method, "/grpc.reflection.")
}

func denied(method, why string) error {
	return status.Errorf(codes.PermissionDenied, "%s: %s", method, why)
}

// closedToEveryone are generated verbs no caller may use on either
// surface: rows the system writes (§35.3). An operator holding all of Shale
// on the cluster API is no exception: what a general write would set there
// is a state nothing else agrees with just the same.
//
// No entity has an Apply (each names the verbs it has, in `proto/shale/`), so
// none is listed: a patch document would be a general write past every rule
// below, and an AddressPolicy's TSIG secret stored as written (§34.10).
func closedToEveryone(m string) bool {
	// Camera authority rows change only through validated, fenced operations.
	if strings.HasPrefix(m, "/shale.Camera") {
		for _, verb := range []string{"/Add", "/Patch", "/Erase"} {
			if strings.HasSuffix(m, verb) {
				return true
			}
		}
	}
	switch {
	case strings.HasPrefix(m, "/shale.LaminaService/"):
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Erase")
	case strings.HasPrefix(m, "/shale.AttemptService/"):
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Erase")
	case strings.HasPrefix(m, "/shale.ReaderService/"),
		strings.HasPrefix(m, "/shale.NodeService/"), strings.HasPrefix(m, "/shale.DeviceService/"),
		strings.HasPrefix(m, "/shale.SinkService/"), strings.HasPrefix(m, "/shale.SigningKeyService/"):
		// Hosts, devices, sinks, and keys are written by the system and by
		// the custom verbs of §32 (adopt, quarantine, retire, rotate, ...);
		// a general write could set a state nothing else agrees with. Keys
		// are made by Rotate.
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch")
	case strings.HasPrefix(m, "/shale.CertificateAuthorityService/"):
		// The CA set changes by Rotate and the leader's steps alone (§33.5):
		// a CA written, patched or erased by hand is a bundle no host holds.
		return strings.HasSuffix(m, "/Add") || strings.HasSuffix(m, "/Patch") || strings.HasSuffix(m, "/Erase")
	case strings.HasPrefix(m, "/shale.ProducerService/"):
		// `producer patch` is the admin's, for the name, the labels and the
		// live policy (§39.3); the core layer refuses the rest of the row.
		return strings.HasSuffix(m, "/Add")
	case strings.HasPrefix(m, "/shale.RelayService/"):
		// `relay patch` is the operator's, for labels (§39.2).
		return strings.HasSuffix(m, "/Add")
	case strings.HasPrefix(m, "/shale.SatelliteService/"):
		// `satellite patch` is the operator's, for labels (§41.2); the core
		// layer refuses the rest of the row. A satellite row is made by its
		// own Join.
		return strings.HasSuffix(m, "/Add")
	}

	return false
}

// Operators says what a person may change (§33.1), by what roster grants
// them: `identity.Operators` is the one a deployment runs, which asks
// `HolderService.Reaches`. An answer it could not get is an error, and an
// error is no.
type Operators interface {
	// May says whether holder of tenant may call method there.
	May(ctx context.Context, tenant, holder pdid.Id, method string) (bool, error)
	// MayOperate says whether holder may call method on the cluster API:
	// a person of the operators' tenant, granted it there.
	MayOperate(ctx context.Context, tenant, holder pdid.Id, method string) (bool, error)
	// Administers says whether holder of tenant holds all of Shale there,
	// which is what sees every site.
	Administers(ctx context.Context, tenant, holder pdid.Id) (bool, error)
}

// granted asks, failing closed: a lookup that failed is a refusal that
// says so, as Unavailable, so a caller retries rather than concluding it
// lost the right.
func granted(ctx context.Context, may func(context.Context, pdid.Id, pdid.Id, string) (bool, error), c gate.Call) error {
	ok, err := may(ctx, c.Tenant, c.Actor, c.Action)
	if err != nil {
		return status.Errorf(codes.Unavailable, "%s: cannot tell what roster grants you right now: %v", c.Action, err)
	}
	if !ok {
		return denied(c.Action, "roster grants you nothing that covers this; a role of Shale's at roster naming it, bound to you or to a group of Shale's you are in, does")
	}

	return nil
}

// TenantPolicy is the tenant API: people see their tenant; a producer or a
// reader sees its tenant and only the calls it needs.
//
// With Operators, a person reads what the console's pages read (personMay)
// and calls the rest as far as roster grants them; with Reads too, the
// reads are as far as roster grants them as well. Without Operators, the
// rule before it: a person may do anything inside their tenant, but for
// what hands somebody a secret (May).
//
// DelegationService is an app's (§33.8): its four entries are served to
// nobody with a credential the interceptor knows, and its generated verbs
// to nobody at all.
type TenantPolicy struct {
	Operators Operators
	Reads     bool
}

var producerMay = map[string]bool{
	// The custom Snapshot rechecks current Producer/Set ownership. This does
	// not grant generated inventory Get/List/Watch or Worker job operations.
	api.CameraAssignmentService_Snapshot_FullMethodName:    true,
	api.CameraAssignmentService_Application_FullMethodName: true,
	api.CameraAssignmentService_BeginApply_FullMethodName:  true,
	api.CameraAssignmentService_Applied_FullMethodName:     true,
	api.SetService_Get_FullMethodName:                      true, api.SetService_List_FullMethodName: true,
	api.CameraCredentialService_Resolve_FullMethodName: true,
	api.SetService_Negotiate_FullMethodName:            true, api.SetService_Allocate_FullMethodName: true,
	api.SourceService_Get_FullMethodName: true, api.SourceService_List_FullMethodName: true,
	api.SourceService_Add_FullMethodName: true,
	api.LaminaService_Get_FullMethodName: true, api.LaminaService_Allocate_FullMethodName: true,
	api.LaminaService_Reallocate_FullMethodName: true, api.LaminaService_Renew_FullMethodName: true,
	api.LaminaService_ReportAttempt_FullMethodName: true, api.LaminaService_ReportFailure_FullMethodName: true,
	api.LaminaService_Skip_FullMethodName:  true,
	api.AttemptService_Get_FullMethodName:  true,
	api.ProducerService_Get_FullMethodName: true, api.ProducerService_Heartbeat_FullMethodName: true,
	api.ProducerService_Relay_FullMethodName: true, api.ProducerService_RenewCertificate_FullMethodName: true,
}

// A worker uses only bounded assignment/job operations. Generic Source/Lamina
// reads and tenant-wide camera CRUD never become worker capabilities.
var cameraWorkerMay = map[string]bool{
	api.CameraCredentialService_Resolve_FullMethodName:      true,
	api.CameraWorkerService_Heartbeat_FullMethodName:        true,
	api.CameraWorkerService_RenewCertificate_FullMethodName: true,
	api.CameraWorkerService_Poll_FullMethodName:             true,
	api.CameraWorkerService_CheckJob_FullMethodName:         true,
	api.CameraWorkerService_ObserveJob_FullMethodName:       true,
	api.CameraJobService_Claim_FullMethodName:               true,
	api.CameraJobService_Report_FullMethodName:              true,
	api.CameraJobService_Publish_FullMethodName:             true,
	api.CameraPresetService_ResolveForJob_FullMethodName:    true,
	api.CameraPolicyService_Observe_FullMethodName:          true,
	api.CameraPolicyService_TrustFactoryTLS_FullMethodName:  true,
	api.CameraPolicyService_VerifyTransport_FullMethodName:  true,
	api.CameraPolicyService_ObserveClock_FullMethodName:     true,
}

var readerMay = map[string]bool{
	api.SetService_Get_FullMethodName: true, api.SetService_List_FullMethodName: true, api.SetService_Live_FullMethodName: true,
	api.SourceService_Get_FullMethodName: true, api.SourceService_List_FullMethodName: true, api.SourceService_Live_FullMethodName: true,
	api.LaminaService_Get_FullMethodName: true, api.LaminaService_List_FullMethodName: true, api.LaminaService_Timeline_FullMethodName: true,
	api.LaminaService_Export_FullMethodName: true,
	api.SiteService_Get_FullMethodName:      true, api.SiteService_List_FullMethodName: true,
	api.ReaderService_Get_FullMethodName: true, api.ReaderService_Heartbeat_FullMethodName: true,
	api.ReaderService_RenewCertificate_FullMethodName: true,
}

// personMay is what every person of a tenant may call where roster says
// what people may change (§33.1): the reads behind the console's pages --
// sets, cameras, live, the laminae and their timeline (which is playback
// and export), sites, producers and readers, attempts -- and the Watch
// that keeps each current. Not people, site membership, the trail, a
// password, or any write: those are what roster grants.
var personMay = map[string]bool{
	api.SetService_Get_FullMethodName:         true,
	api.SetService_List_FullMethodName:        true,
	api.SetService_Watch_FullMethodName:       true,
	api.SetService_Live_FullMethodName:        true,
	api.SourceService_Get_FullMethodName:      true,
	api.SourceService_List_FullMethodName:     true,
	api.SourceService_Watch_FullMethodName:    true,
	api.SourceService_Live_FullMethodName:     true,
	api.LaminaService_Get_FullMethodName:      true,
	api.LaminaService_List_FullMethodName:     true,
	api.LaminaService_Watch_FullMethodName:    true,
	api.LaminaService_Timeline_FullMethodName: true,
	api.LaminaService_Export_FullMethodName:   true,
	api.AttemptService_Get_FullMethodName:     true,
	api.AttemptService_List_FullMethodName:    true,
	api.SiteService_Get_FullMethodName:        true,
	api.SiteService_List_FullMethodName:       true,
	api.SiteService_Watch_FullMethodName:      true,
	api.ProducerService_Get_FullMethodName:    true,
	api.ProducerService_List_FullMethodName:   true,
	api.ProducerService_Watch_FullMethodName:  true,
	api.ReaderService_Get_FullMethodName:      true,
	api.ReaderService_List_FullMethodName:     true,
	api.ReaderService_Watch_FullMethodName:    true,
}

// ownKeys are a person's keys for Shale, made, listed and ended by the
// person for themselves (§33.1): every person may call them on the tenant
// API whatever roles they hold, and `reads: granted` too, since what they
// are about is the caller's own and nobody else's. A key may call them
// where its methods name them. The cluster API serves none of them: an
// operator ends somebody else's key at roster.
var ownKeys = map[string]bool{
	api.HolderService_BeginKey_FullMethodName: true,
	api.HolderService_PollKey_FullMethodName:  true,
	api.HolderService_ListKeys_FullMethodName: true,
	api.HolderService_EndKey_FullMethodName:   true,
}

// ownReaches is a person's own lists of cameras (Reach, §33.1): every
// person adds, reads, changes and erases their own, whatever roles they
// hold, and nobody else's -- which the scope holds them to (Reaches), as
// the core layer holds what they put in one to what they see. The cluster
// API serves none of it.
func ownReaches(m string) bool { return strings.HasPrefix(m, "/shale.ReachService/") }

// keyScopeMay is what a key that carries a list of cameras may call
// (§33.1, *A key narrowed to some cameras*): the reads and views a list
// confines, each held to it by the scope beside the wall (Reaches) and,
// where a token is signed, by what the scope answers when it is signed.
//
// A key that carries a list is refused everything else, at BeginKey and at
// the door, whatever its methods say: a key said to reach two cameras that
// could erase a third source, read a producer's inventory or change a
// policy would not be narrowed. The scope confines what it can be shown to
// confine, and nothing else is let through on its word.
//
// A method is added here only with a test of its row in §33.1's table of
// paths (TestKeyNarrowedToCameras): the scope covers more entities than
// this offers -- a camera's binding, by its source -- and holds them if the
// table grows, but a path with no test is not offered to a narrowed key.
var keyScopeMay = map[string]bool{
	api.SourceService_Get_FullMethodName:      true,
	api.SourceService_List_FullMethodName:     true,
	api.SourceService_Watch_FullMethodName:    true,
	api.SourceService_Live_FullMethodName:     true,
	api.SetService_Get_FullMethodName:         true,
	api.SetService_List_FullMethodName:        true,
	api.SetService_Watch_FullMethodName:       true,
	api.SetService_Live_FullMethodName:        true,
	api.LaminaService_Get_FullMethodName:      true,
	api.LaminaService_List_FullMethodName:     true,
	api.LaminaService_Watch_FullMethodName:    true,
	api.LaminaService_Timeline_FullMethodName: true,
	api.LaminaService_Export_FullMethodName:   true,
	api.AttemptService_Get_FullMethodName:     true,
	api.AttemptService_List_FullMethodName:    true,
}

// KeyScopeMay says whether a key that carries a list of cameras may call a
// method (keyScopeMay).
func KeyScopeMay(method string) bool { return keyScopeMay[method] }

// KeyScopeMethods is every method a key that carries a list of cameras may
// call, sorted.
func KeyScopeMethods() []string {
	out := make([]string, 0, len(keyScopeMay))
	for m := range keyScopeMay {
		out = append(out, m)
	}
	slices.Sort(out)

	return out
}

// scopedKey says whether a call came with a key that carries a scope.
func scopedKey(ctx context.Context) bool {
	f, ok := frame.From(ctx)

	return ok && f.TokenScope() != ""
}

// OwnKeys says whether a method is one of a person's own keys' (ownKeys).
func OwnKeys(method string) bool { return ownKeys[method] }

// PersonMay says whether every person may call a method on the tenant API
// where roster says what people may change.
func PersonMay(method string) bool { return personMay[method] }

func (p TenantPolicy) May(ctx context.Context, c gate.Call) error {
	m := c.Action
	if IsClusterService(m) {
		return denied(m, "not served on the tenant API")
	}
	if scopedKey(ctx) && !keyScopeMay[m] {
		// Whatever the key's methods say: what a list of cameras cannot
		// confine, a key that carries one does not call.
		return denied(m, "this key carries a list of cameras, and such a key calls only the reads and views a list confines: "+
			"sources, sets, laminae and attempts, their live view, timeline and export")
	}
	if closedToEveryone(m) {
		return denied(m, "written by the system, not by a caller")
	}
	if strings.HasPrefix(m, "/shale.DelegationService/") {
		return denied(m, "an app calls this with its exchange token and nothing else (§33.8)")
	}

	switch kindOf(c.Actor) {
	case DomProducer:
		if !producerMay[m] {
			return denied(m, "not something a producer does")
		}
	case DomCameraWorker:
		if !cameraWorkerMay[m] {
			return denied(m, "not something a camera worker does")
		}
	case DomReader:
		if !readerMay[m] {
			return denied(m, "not something a reader does")
		}
	case DomHolder:
		if ownKeys[m] || ownReaches(m) {
			return nil
		}
		if p.Operators != nil {
			// Reading is everybody's, unless reads are granted too; the
			// rest is what roster grants (§33.1).
			if personMay[m] && !p.Reads {
				return nil
			}

			return granted(ctx, p.Operators.May, c)
		}
		// A person may do the rest, inside their tenant, except what
		// hands somebody a secret (§33.1). Giving somebody a password is an
		// administrator's act. Recovering a camera's credential hands the
		// caller its plaintext -- the administrator, management or
		// streaming password of a camera -- and it is an operator's: with
		// nobody named one, nobody has it, whoever they are of the tenant.
		// A camera worker's and a producer's delivery are decided above, by
		// their kind.
		switch m {
		case api.HolderService_IssuePassword_FullMethodName:
			if !seesEverySite(ctx) {
				return denied(m, "only a person who sees every site issues a password")
			}
		case api.CameraCredentialService_Resolve_FullMethodName:
			return denied(m, "recovering a camera's credential is an operator's act, and this deployment names no operators (auth.operators)")
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

// ClusterPolicy is the cluster API: operators see everything; nodes,
// relays and satellites do their own work and nothing more.
//
// Operators are the people of the operators' tenant whom roster grants all
// of Shale there, when Operators is set; the people of ClusterTenant when it
// is not (§33.1). It is all of Shale on every call and not as far as roster
// grants: Where is every tenant here, so a grant of one tenant service
// would be that service in every tenant at once. A narrower grant is used
// on the tenant API, inside the person's own tenant.
//
// What no caller may write on the tenant API is closed here too, and so is
// DelegationService, which is an app's on the tenant API alone (§33.8).
type ClusterPolicy struct {
	ClusterTenant pdid.Id
	Operators     Operators
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

// satelliteMay is what a satellite does on the cluster API (§41): its own
// heartbeat and certificate, the routes to the nodes its producers' tokens
// name, and the key set it verifies those tokens with. Nothing that reads
// a tenant's rows: the producers it serves are in its heartbeat's answer.
//
// Under `retain: accepted` it delivers what it took in a producer's stead
// (§41.9): it records itself as a lamina's holder (Accept), and makes the
// producer's lamina calls, Renew, ReportAttempt, Reallocate and
// ReportFailure. The core layer takes them only for a lamina whose set's
// producer is assigned to the satellite now, and in that producer's name.
var satelliteMay = map[string]bool{
	api.SatelliteService_Get_FullMethodName: true, api.SatelliteService_Heartbeat_FullMethodName: true,
	api.SatelliteService_RenewCertificate_FullMethodName: true, api.SatelliteService_Route_FullMethodName: true,
	api.SatelliteService_Accept_FullMethodName: true,
	api.SigningKeyService_Get_FullMethodName:   true, api.SigningKeyService_List_FullMethodName: true, api.SigningKeyService_Watch_FullMethodName: true,
	api.LaminaService_Renew_FullMethodName: true, api.LaminaService_ReportAttempt_FullMethodName: true,
	api.LaminaService_Reallocate_FullMethodName: true, api.LaminaService_ReportFailure_FullMethodName: true,
}

func (p ClusterPolicy) May(ctx context.Context, c gate.Call) error {
	m := c.Action
	if closedToEveryone(m) {
		return denied(m, "written by the system, not by a caller")
	}
	if strings.HasPrefix(m, "/shale.DelegationService/") {
		return denied(m, "an app calls this on the tenant API with its exchange token and nothing else (§33.8)")
	}
	if ownKeys[m] {
		return denied(m, "a person's keys for Shale are their own, on the tenant API; an operator ends somebody else's at roster")
	}
	if ownReaches(m) {
		return denied(m, "a person's lists of cameras are their own, on the tenant API")
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
	case DomSatellite:
		if !satelliteMay[m] {
			return denied(m, "not something a satellite does")
		}
	case DomHolder:
		switch {
		case p.Operators != nil:
			// All of Shale, asked on every call rather than only at sign-in:
			// a grant taken away ends here within the TTL, and a session that
			// came by some other road than the cluster sign-in is still asked.
			ok, err := p.Operators.MayOperate(ctx, c.Tenant, c.Actor, identity.Everything)
			if err != nil {
				return status.Errorf(codes.Unavailable, "%s: cannot tell what roster grants you right now: %v", m, err)
			}
			if !ok {
				return denied(m, "the cluster API serves operators: people of the operators' tenant whom roster grants all of Shale ("+identity.Everything+"), a role of Shale's")
			}
		case p.ClusterTenant.IsZero():
			// No cluster tenant is known here yet: nobody is an operator,
			// rather than everybody (§33.7).
			return denied(m, "the cluster tenant is not known yet; there are no cluster operators until it is")
		case c.Tenant != p.ClusterTenant:
			return denied(m, "the cluster API serves cluster operators")
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
// rows; a producer sees its set's site. Where roster says what people may
// change, a person it grants all of Shale sees every site whatever their
// row says.
type Sites struct {
	d *Deps
	// Operators, where roster says what people may change.
	Operators Operators

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
		if s.Operators != nil {
			// Whoever holds all of Shale administers the tenant, so sees
			// all of it; a failed lookup is no, as everywhere (§33.1).
			if is, _ := s.Operators.Administers(ctx, f.Tenant, f.Actor); is {
				e.all = true
				break
			}
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
	case DomCameraWorker:
		// The site approved at adoption is the maximum scope; assignments narrow it
		// further inside each execution RPC. An unassigned worker has no all-sites flag.
		w, err := s.d.Own.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, false, err
		}
		if len(w.GetSite().GetId()) > 0 {
			e.ids = []pdid.Id{mustId(w.GetSite().GetId())}
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
