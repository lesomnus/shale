package shale

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/grpc"
)

// RPCAuthority uses a configured scoped service/operator identity, never an
// impersonated Producer. Source creation still uses the Producer's own identity.
// This identity must be allowed to enumerate the tenant-wide alias namespace.
type RPCAuthority struct {
	Conn           grpc.ClientConnInterface
	PollInterval   time.Duration
	ObserveTimeout time.Duration
	MaxReportAge   time.Duration
	LiveHTTPClient *http.Client
	// LiveCheck is injectable for controlled integration transports. The normal
	// implementation opens a bounded WHEP session and requires video to arrive.
	LiveCheck func(context.Context, *api.LiveSource) error
}

func (a *RPCAuthority) assignment(ctx context.Context, s camera.EnrollmentScope) (*api.Producer, error) {
	if a.Conn == nil {
		return nil, camera.ErrInvalid
	}
	pid, err := pdid.Parse(s.ProducerID)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	set, err := pdid.Parse(s.SetID)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	tenant, err := pdid.Parse(s.TenantID)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	p, err := api.NewProducerServiceClient(a.Conn).Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: pid.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if p.GetState() != api.HostState_HOST_STATE_ADOPTED || !bytes.Equal(p.GetTenant().GetId(), tenant.Bytes()) || !bytes.Equal(p.GetSet().GetId(), set.Bytes()) || p.GetDateErased() != nil {
		return nil, camera.ErrForbidden
	}
	row, err := api.NewSetServiceClient(a.Conn).Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(row.GetTenant().GetId(), tenant.Bytes()) || row.GetDateErased() != nil {
		return nil, camera.ErrForbidden
	}
	if s.SiteID != "" {
		site, err := pdid.Parse(s.SiteID)
		if err != nil {
			return nil, camera.ErrInvalid
		}
		if !bytes.Equal(row.GetSite().GetId(), site.Bytes()) || !bytes.Equal(p.GetSite().GetId(), site.Bytes()) {
			return nil, camera.ErrForbidden
		}
	}
	return p, nil
}
func (a *RPCAuthority) find(ctx context.Context, d provision.Deployment, alias string) (*api.Source, error) {
	tenant, err := pdid.Parse(d.Request.Scope.TenantID)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	set, err := pdid.Parse(d.Request.Scope.SetID)
	if err != nil {
		return nil, camera.ErrInvalid
	}
	after := ""
	var found *api.Source
	for {
		resp, err := api.NewSourceServiceClient(a.Conn).List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Tenant: api.TenantRef_builder{Id: tenant.Bytes()}.Build()}.Build()}, Size: 500, After: after}.Build())
		if err != nil {
			return nil, err
		}
		for _, s := range resp.GetItems() {
			if s.GetAlias() != alias {
				continue
			}
			if found != nil || !bytes.Equal(s.GetTenant().GetId(), tenant.Bytes()) || !bytes.Equal(s.GetSet().GetId(), set.Bytes()) || s.GetLabels()[BindingLabel] != d.Request.BindingID || s.GetLabels()[DeviceLabel] != string(d.Request.Device.ID) || s.GetDateErased() != nil {
				return nil, provision.ErrConflict
			}
			found = s
		}
		if resp.GetNext() == "" {
			return found, nil
		}
		if resp.GetNext() == after {
			return nil, provision.ErrConflict
		}
		after = resp.GetNext()
	}
}
func (a *RPCAuthority) Check(ctx context.Context, d provision.Deployment, alias string) error {
	if _, err := a.assignment(ctx, d.Request.Scope); err != nil {
		return err
	}
	_, err := a.find(ctx, d, alias)
	return err
}
func (a *RPCAuthority) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	parent := ctx
	timeout := a.ObserveTimeout
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	every := a.PollInterval
	if every <= 0 {
		every = time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		out, err := a.observeOnce(ctx, d, b)
		if err == nil {
			return out, nil
		}
		if parent.Err() != nil {
			return out, parent.Err()
		}
		if ctx.Err() != nil {
			return out, provision.ErrNotReady
		}
		if !errors.Is(err, provision.ErrNotReady) {
			return out, err
		}
		b = out
		select {
		case <-ctx.Done():
			return b, provision.ErrNotReady
		case <-tick.C:
		}
	}
}
func (a *RPCAuthority) observeOnce(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	p, err := a.assignment(ctx, d.Request.Scope)
	if err != nil {
		return b, err
	}
	s, err := a.find(ctx, d, b.Alias)
	if err != nil {
		return b, err
	}
	if s == nil {
		return b, provision.ErrNotReady
	}
	id, err := pdid.From(s.GetId())
	if err != nil {
		return b, camera.ErrInvalid
	}
	if b.SourceID != "" && b.SourceID != id.String() {
		return b, provision.ErrConflict
	}
	b.SourceID = id.String()
	b.Ingest = false
	b.Live = false
	b.Committed = false
	age := a.MaxReportAge
	if age <= 0 {
		age = 30 * time.Second
	}
	status := p.GetStatus()
	if at := status.GetDateReported(); at != nil && !at.AsTime().Before(b.DeployedAt) && time.Since(at.AsTime()) <= age && time.Until(at.AsTime()) < age {
		for _, r := range status.GetSources() {
			if bytes.Equal(r.GetSourceId(), s.GetId()) && r.GetInputUp() && !r.GetDark() && r.GetFrameRate() > 0 && r.GetMeasuredBitrate() > 0 {
				b.Ingest = true
			}
		}
	}
	if !b.Ingest {
		return b, provision.ErrNotReady
	}
	// Inspect newest rows first; a new complete recording must have both
	// started and committed after deployment. An old archive is not evidence.
	after := ""
	for {
		resp, err := api.NewLaminaServiceClient(a.Conn).List(ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: s.GetId()}.Build()}.Build()}, Size: 100, Reverse: true, After: after}.Build())
		if err != nil {
			return b, err
		}
		past := false
		for _, l := range resp.GetItems() {
			if at := l.GetDateStarted(); at != nil && at.AsTime().Before(b.DeployedAt) {
				past = true
				continue
			}
			if !bytes.Equal(l.GetSource().GetId(), s.GetId()) || !bytes.Equal(l.GetSet().GetId(), s.GetSet().GetId()) || l.GetState() != api.LaminaState_LAMINA_STATE_COMMITTED || l.GetIncomplete() || l.GetSize() <= 0 || l.GetDateCommitted() == nil || l.GetDateStarted() == nil || l.GetDateEnded() == nil || l.GetDateEnded().AsTime().Before(l.GetDateStarted().AsTime()) || l.GetDateCommitted().AsTime().Before(b.DeployedAt) {
				continue
			}
			b.Committed = true
			b.CommittedAt = l.GetDateCommitted().AsTime()
			break
		}
		if b.Committed || past || resp.GetNext() == "" {
			break
		}
		if after == resp.GetNext() {
			return b, provision.ErrConflict
		}
		after = resp.GetNext()
	}
	if !b.Committed {
		return b, provision.ErrNotReady
	}
	live, err := api.NewSourceServiceClient(a.Conn).Live(ctx, api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: s.GetId()}.Build()}.Build())
	if err != nil {
		return b, err
	}
	ls := live.GetSource()
	if ls == nil || !bytes.Equal(ls.GetSourceId(), s.GetId()) || ls.GetWhepUrl() == "" || ls.GetViewToken() == "" || ls.GetDateExpires() == nil || !ls.GetDateExpires().AsTime().After(time.Now()) {
		return b, provision.ErrNotReady
	}
	check := a.LiveCheck
	if check == nil {
		check = func(ctx context.Context, s *api.LiveSource) error { return checkLive(ctx, a.LiveHTTPClient, s) }
	}
	if err = check(ctx, ls); err != nil {
		return b, provision.ErrNotReady
	}
	b.Live = true
	return b, nil
}
