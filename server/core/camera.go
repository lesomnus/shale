package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/cameranetwork"
	"github.com/protobuf-orm/ent/dialect"
	"github.com/protobuf-orm/ent/dialect/sql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Camera authority is initially an operator API. #195 adds adopted Worker
// identities to narrowly scoped execution methods; an arbitrary UUID is never
// a machine credential. The outer gate still checks the operator's grant.
func cameraOperator(ctx context.Context) (pdid.Id, error) {
	f, err := actor(ctx)
	if err != nil {
		return pdid.Id{}, err
	}
	if kindOf(f.Actor) != DomHolder {
		return pdid.Id{}, denied("camera authority", "only an authorized person may operate cameras")
	}
	return f.Actor, nil
}

// cameraExecutor does not broaden cameraOperator: configuration, inventory,
// assignment and handover remain person/operator-only.
func (s Core) cameraExecutor(ctx context.Context) (pdid.Id, error) {
	f, err := actor(ctx)
	if err != nil {
		return pdid.Id{}, err
	}
	if kindOf(f.Actor) == DomHolder {
		return f.Actor, nil
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return pdid.Id{}, err
	}
	return f.Actor, nil
}

// The framework's grouped wall cannot express only Site=NULL without an
// all-sites flag. Worker custom execution instead uses the recorded private
// stack, then validates the exact current authority graph before any effect or
// response. Generic RPCs never receive this path. Operators retain their wall.
func (s Core) cameraExecutionTx(ctx context.Context, fn func(context.Context, api.Server) error) error {
	f, err := actor(ctx)
	if err != nil {
		return err
	}
	if kindOf(f.Actor) == DomCameraWorker {
		if _, err := s.cameraWorkerActor(ctx); err != nil {
			return err
		}
		return s.ownTx(ctx, fn)
	}
	if _, err := cameraOperator(ctx); err != nil {
		return err
	}
	return s.tx(ctx, fn)
}
func (s Core) cameraExecutionScope(ctx context.Context, a *api.CameraAssignment) error {
	f, err := actor(ctx)
	if err != nil {
		return err
	}
	if kindOf(f.Actor) == DomHolder {
		return nil
	}
	w, err := s.cameraWorkerActor(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(a.GetWorkerId(), w.GetId()) {
		return denied("camera job", "job is not assigned to this worker")
	}
	return cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), w.GetTenant().GetId(), w.GetSite().GetId())
}

func (s Core) cameraExecutionParents(ctx context.Context, a *api.CameraAssignment) error {
	f, err := actor(ctx)
	if err != nil {
		return err
	}
	if kindOf(f.Actor) != DomCameraWorker {
		return nil
	}
	if err := s.cameraExecutionScope(ctx, a); err != nil {
		return err
	}
	nx := s.own(ctx)
	set, err := nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: a.GetSet().GetId()}.Build()}.Build())
	if err != nil {
		return err
	}
	if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), set.GetTenant().GetId(), set.GetSite().GetId()); err != nil {
		return err
	}
	p, err := nx.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
	if err != nil {
		return err
	}
	if p.GetState() != api.HostState_HOST_STATE_ADOPTED || !bytes.Equal(p.GetSet().GetId(), set.GetId()) {
		return failed("assigned producer is no longer adopted for this set")
	}
	return cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), p.GetTenant().GetId(), p.GetSite().GetId())
}

func cameraVersion(want, got *timestamppb.Timestamp) error {
	if want == nil || got == nil || want.CheckValid() != nil || !want.AsTime().Equal(got.AsTime()) {
		return status.Error(codes.Aborted, "camera state changed; read the current version before retrying")
	}
	return nil
}

var cameraKey = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)

func cameraToken(field, v string) error {
	if !cameraKey.MatchString(v) {
		return invalid(field, "a nonempty bounded identifier is required")
	}
	return nil
}
func cameraHash(m proto.Message) []byte {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	h := sha256.Sum256(b)
	return h[:]
}
func cameraSite(site *api.Site) *api.SiteRef {
	if len(site.GetId()) == 0 {
		return nil
	}
	return api.SiteRef_builder{Id: site.GetId()}.Build()
}
func cameraSameScope(tenantA, siteA, tenantB, siteB []byte) error {
	if !bytes.Equal(tenantA, tenantB) || !bytes.Equal(siteA, siteB) {
		return status.Error(codes.PermissionDenied, "camera assignment crosses tenant or site")
	}
	return nil
}
func cameraNetworks(cidrs []string) ([]netip.Prefix, error) {
	if len(cidrs) == 0 || len(cidrs) > 32 {
		return nil, invalid("cidrs", "one to 32 explicit camera prefixes are required")
	}
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, raw := range cidrs {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p.Bits() == 0 || p.Addr().IsMulticast() || p.Addr().IsUnspecified() || p != p.Masked() {
			return nil, invalid("cidrs", "canonical, bounded unicast prefixes are required")
		}
		out = append(out, p)
	}
	return out, nil
}
func cameraURI(raw string, cidrs []string, media bool) error {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.Hostname() == "" {
		return invalid("endpoint", "a credential-free camera URI is required")
	}
	if media {
		if u.Scheme != "rtsp" && u.Scheme != "rtsps" {
			return invalid("input", "an RTSP camera URI is required")
		}
	} else if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return invalid("endpoint", "unsupported camera URI scheme")
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() {
		return invalid("endpoint", "an explicit camera IP address is required")
	}
	ps, err := cameraNetworks(cidrs)
	if err != nil {
		return err
	}
	found := false
	for _, p := range ps {
		if p.Contains(ip.Unmap()) {
			found = true
		}
	}
	if !found {
		return invalid("endpoint", "camera endpoint is outside its assigned network")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return invalid("endpoint", "invalid camera query parameters")
	}
	for key, values := range query {
		switch strings.ToLower(key) {
		case "channel", "subtype", "stream", "streamid", "profile", "transport", "unicast", "proto":
		default:
			return invalid("endpoint", "only noncredential camera stream selectors are allowed in the URI")
		}
		for _, value := range values {
			if len(value) > 128 || strings.ContainsAny(value, "@:/\\\x00\r\n") {
				return invalid("endpoint", "invalid stream selector")
			}
		}
	}
	return nil
}
func cameraEndpoints(vs []*api.CameraObservedEndpoint, cidrs []string) error {
	if len(vs) > 32 {
		return invalid("endpoints", "too many camera endpoints")
	}
	for _, v := range vs {
		if v == nil {
			return invalid("endpoints", "an endpoint is missing")
		}
		if err := cameraURI(v.GetUri(), cidrs, false); err != nil {
			return err
		}
		if v.GetProvenance() != "" {
			if err := cameraToken("provenance", v.GetProvenance()); err != nil {
				return err
			}
		}
		if v.GetObservedAt() != nil && v.GetObservedAt().CheckValid() != nil {
			return invalid("observed_at", "invalid timestamp")
		}
	}
	return nil
}
func cameraCredential(v *api.CameraCredentialReference, purpose string) error {
	if v == nil {
		return nil
	}
	if err := cameraToken("credential.provider", v.GetProvider()); err != nil {
		return err
	}
	if err := cameraToken("credential.id", v.GetId()); err != nil {
		return err
	}
	if err := cameraToken("credential.version", v.GetVersion()); err != nil {
		return err
	}
	if v.GetPurpose() != purpose {
		return invalid("credential.purpose", "credential purpose does not match the operation")
	}
	return nil
}
func cameraProfile(p *api.CameraCaptureProfile, complete bool) error {
	if p == nil {
		if complete {
			return invalid("profile", "verified media profile is required")
		}
		return nil
	}
	if p.GetCodec() != "" && p.GetCodec() != "h264" && p.GetCodec() != "h265" {
		return invalid("profile.codec", "unsupported camera codec")
	}
	if p.GetWidth() < 0 || p.GetHeight() < 0 || p.GetWidth() > 16384 || p.GetHeight() > 16384 || p.GetFps() < 0 || p.GetFps() > 1000 || math.IsNaN(p.GetFps()) || math.IsInf(p.GetFps(), 0) || p.GetMaxBitrateBps() < 0 || p.GetMaxBitrateBps() > 10_000_000_000 || p.GetKeyframeIntervalMs() < 0 {
		return invalid("profile", "invalid capture profile")
	}
	if complete && (p.GetCodec() == "" || p.GetWidth() == 0 || p.GetHeight() == 0 || p.GetFps() == 0 || p.GetMaxBitrateBps() == 0) {
		return invalid("profile", "a complete verified media profile is required")
	}
	return nil
}
func cameraObservation(v *api.CameraJobObservation, verified bool) error {
	if v == nil {
		if verified {
			return invalid("observed", "independent media evidence is required")
		}
		return nil
	}
	if v.GetCode() != "" {
		if err := cameraToken("code", v.GetCode()); err != nil {
			return err
		}
	}
	if len(v.GetWarnings()) > 32 {
		return invalid("warnings", "too many warnings")
	}
	for _, w := range v.GetWarnings() {
		if w == nil {
			return invalid("warnings", "missing warning")
		}
		if err := cameraToken("warning.code", w.GetCode()); err != nil {
			return err
		}
		if w.GetField() != "" {
			if err := cameraToken("warning.field", w.GetField()); err != nil {
				return err
			}
		}
	}
	if err := cameraProfile(v.GetEffective(), verified); err != nil {
		return err
	}
	m := v.GetVerified()
	if m != nil {
		if err := cameraProfile(m.GetProfile(), true); err != nil {
			return err
		}
		if m.GetDurationSeconds() <= 0 || m.GetDurationSeconds() > 300 || math.IsNaN(m.GetDurationSeconds()) || m.GetDecodedFrames() < 1 || m.GetObservedAt() == nil || m.GetObservedAt().CheckValid() != nil || !m.GetDecoded() {
			return invalid("verified", "bounded independently decoded media evidence is required")
		}
	}
	if verified && m == nil {
		return invalid("verified", "independent media evidence is required")
	}
	return nil
}
func (s Core) cameraLockAssignment(ctx context.Context, id []byte) error {
	q := s.ent(ctx).CameraAssignment.Query().Where(cameraassignment.IdEQ(mustId(id).Uuid()))
	if s.driver().Dialect() != dialect.SQLite {
		q.Modify(func(q *sql.Selector) { q.ForUpdate() })
	}
	_, err := q.Only(ctx)
	return err
}
func (s Core) cameraLockDevice(ctx context.Context, id []byte) error {
	q := s.ent(ctx).CameraDevice.Query().Where(cameradevice.IdEQ(mustId(id).Uuid()))
	if s.driver().Dialect() != dialect.SQLite {
		q.Modify(func(q *sql.Selector) { q.ForUpdate() })
	}
	_, err := q.Only(ctx)
	return err
}
func cameraConflict(err error) error {
	if ent.IsConstraintError(err) {
		return status.Error(codes.AlreadyExists, "camera identity, binding or request is already reserved")
	}
	return err
}
func cameraFresh(t *timestamppb.Timestamp, after time.Time) bool {
	return t != nil && t.CheckValid() == nil && !t.AsTime().Before(after)
}

// Reject unknown wire fields instead of preserving an untyped side channel in
// CP catalog/job JSON. New managed fields need an explicit versioned contract.
func cameraMessage(m proto.Message) error {
	if m == nil || proto.Size(m) > 64<<10 {
		return invalid("request", "camera request is missing or too large")
	}
	var visit func(protoreflect.Message, int) bool
	visit = func(v protoreflect.Message, depth int) bool {
		if depth > 16 || len(v.GetUnknown()) != 0 {
			return false
		}
		ok := true
		v.Range(func(f protoreflect.FieldDescriptor, x protoreflect.Value) bool {
			if f.IsMap() {
				if f.MapValue().Kind() == protoreflect.MessageKind {
					x.Map().Range(func(_ protoreflect.MapKey, y protoreflect.Value) bool { ok = visit(y.Message(), depth+1); return ok })
				}
			} else if f.IsList() {
				if f.Kind() == protoreflect.MessageKind {
					for i := 0; i < x.List().Len(); i++ {
						if !visit(x.List().Get(i).Message(), depth+1) {
							ok = false
							break
						}
					}
				}
			} else if f.Kind() == protoreflect.MessageKind {
				ok = visit(x.Message(), depth+1)
			}
			return ok
		})
		return ok
	}
	if !visit(m.ProtoReflect(), 0) {
		return invalid("request", "unknown or excessively nested camera fields are not allowed")
	}
	return nil
}
func (s Core) cameraLockNetwork(ctx context.Context, id []byte) error {
	q := s.ent(ctx).CameraNetwork.Query().Where(cameranetwork.IdEQ(mustId(id).Uuid()))
	if s.driver().Dialect() != dialect.SQLite {
		q.Modify(func(q *sql.Selector) { q.ForUpdate() })
	}
	_, err := q.Only(ctx)
	return err
}
