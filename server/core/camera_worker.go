package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraworker"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type coreCameraWorker struct {
	Core
	api.CameraWorkerServiceServer
}

func (s Core) CameraWorker() api.CameraWorkerServiceServer {
	return coreCameraWorker{s, s.Next().CameraWorker()}
}

func (s Core) issueWorker(hj *api.HostJoin, id pdid.Id) ([]byte, string, time.Time, error) {
	if s.d.CA == nil {
		return nil, "", time.Time{}, failed("this control plane has no CA")
	}
	c, err := s.d.CA.IssueClientHost(hj.GetCsr(), id, s.d.now())
	if err != nil {
		return nil, "", time.Time{}, err
	}
	return pki.EncodeCerts(c), pki.Serial(c), c.NotAfter, nil
}

func workerCSRKey(csr []byte) (string, error) {
	r, err := x509.ParseCertificateRequest(csr)
	if err != nil {
		return "", invalid("csr", "invalid certificate request")
	}
	if err := r.CheckSignature(); err != nil {
		return "", invalid("csr", "invalid certificate request signature")
	}
	h := sha256.Sum256(r.RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

// cameraWorkerActor rechecks even an already-open long poll against current CP
// state. The original frame row is the certificate authenticated at RPC entry.
func (s Core) cameraWorkerActor(ctx context.Context) (*api.CameraWorker, error) {
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomCameraWorker {
		return nil, denied("camera worker", "an adopted worker is required")
	}
	prior, ok := f.Row.(*api.CameraWorker)
	if !ok || !bytes.Equal(prior.GetId(), f.Actor.Bytes()) {
		return nil, denied("camera worker", "worker identity was not resolved")
	}
	current, err := s.own(ctx).CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if current.GetState() != api.HostState_HOST_STATE_ADOPTED || current.GetCertSerial() == "" || current.GetCertSerial() != prior.GetCertSerial() || !bytes.Equal(current.GetTenant().GetId(), f.Tenant.Bytes()) {
		return nil, denied("camera worker", "worker adoption or certificate was revoked")
	}
	return current, nil
}

func (s coreCameraWorker) Adopt(ctx context.Context, r *api.CameraWorkerAdoptRequest) (*api.CameraWorker, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraWorker
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		w, err := nx.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		var site *api.SiteRef
		if r.GetSite() != nil {
			v, err := nx.Site().Get(ctx, api.SiteGetRequest_builder{Ref: r.GetSite()}.Build())
			if err != nil {
				return err
			}
			if !bytes.Equal(w.GetTenant().GetId(), v.GetTenant().GetId()) {
				return denied("worker adopt", "site belongs to another tenant")
			}
			site = cameraSite(v)
		}
		if w.GetDateAdopted() != nil && !bytes.Equal(w.GetSite().GetId(), site.GetId()) {
			return failed("an adopted worker site is fixed; approve a replacement identity for another site")
		}
		if len(w.GetJoin().GetCsr()) == 0 {
			return failed("this worker has no pending join request")
		}
		rec := proto.Clone(w.GetJoin()).(*api.HostJoin)
		key, err := workerCSRKey(rec.GetCsr())
		if err != nil {
			return err
		}
		cert, serial, exp, err := s.issueWorker(rec, mustId(w.GetId()))
		if err != nil {
			return err
		}
		rec.SetCertificate(cert)
		rec.SetRejoin(false)
		patch := api.CameraWorkerPatchRequest_builder{Ref: api.CameraWorkerRef_builder{Id: w.GetId()}.Build(), State: z.Ptr(api.HostState_HOST_STATE_ADOPTED), Join: rec, CertSerial: z.Ptr(serial), DateAdopted: timestamppb.New(s.d.now()), DateCertIssued: timestamppb.New(s.d.now()), DateCertExpires: timestamppb.New(exp), DateUpdated: w.GetDateUpdated(), ApprovedKeyFingerprint: z.Ptr(key)}
		if site != nil {
			patch.Site = site
		} else {
			patch.SiteNull = z.Ptr(true)
		}
		if a := strings.TrimSpace(r.GetAlias()); a != "" {
			patch.Alias = z.Ptr(a)
		}
		if _, err = nx.CameraWorker().Patch(ctx, patch.Build()); err != nil {
			return err
		}
		out, err = nx.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: w.GetId()}.Build()}.Build())
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s coreCameraWorker) Heartbeat(ctx context.Context, r *api.CameraWorkerHeartbeatRequest) (*api.CameraWorkerHeartbeatResponse, error) {
	w, err := s.cameraWorkerActor(ctx)
	if err != nil {
		return nil, err
	}
	if len(r.GetVersion()) > 256 || strings.ContainsFunc(r.GetVersion(), unprintable) {
		return nil, invalid("version", "a bounded printable version is required")
	}
	now := s.d.now()
	_, err = s.own(ctx).CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{Ref: api.CameraWorkerRef_builder{Id: w.GetId()}.Build(), DateSeen: timestamppb.New(now), Version: z.Ptr(r.GetVersion()), DateUpdatedForce: z.Ptr(true)}.Build())
	if err != nil {
		return nil, err
	}
	return api.CameraWorkerHeartbeatResponse_builder{RenewCertificate: w.GetDateCertExpires() != nil && now.After(renewAt(w.GetDateCertIssued(), w.GetDateCertExpires()))}.Build(), nil
}

func (s coreCameraWorker) Revoke(ctx context.Context, r *api.CameraWorkerRevokeRequest) (*api.CameraWorker, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraWorker
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		w, err := nx.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), w.GetDateUpdated()); err != nil {
			return err
		}
		if _, err := nx.CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{Ref: r.GetRef(), State: z.Ptr(api.HostState_HOST_STATE_PENDING), CertSerial: z.Ptr(""), DateUpdated: w.GetDateUpdated()}.Build()); err != nil {
			return err
		}
		if _, err := nx.CameraWorker().Erase(ctx, r.GetRef()); err != nil {
			return err
		}
		out = proto.Clone(w).(*api.CameraWorker)
		out.SetState(api.HostState_HOST_STATE_PENDING)
		out.SetCertSerial("")
		out.SetDateErased(timestamppb.New(s.d.now()))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s coreCameraWorker) Join(ctx context.Context, req *api.CameraWorkerJoinRequest) (*api.CameraWorkerJoinResponse, error) {
	hj := req.GetHost()
	if err := validateJoin(hj); err != nil {
		return nil, err
	}
	key, err := workerCSRKey(hj.GetCsr())
	if err != nil {
		return nil, err
	}
	hj = proto.Clone(hj).(*api.HostJoin)
	hj.SetKeyFingerprint(key[:23]) // Same display fingerprint as HostKey.Fingerprint.
	from, _ := peerAddr(ctx)

	t, err := s.tenantFor(ctx, req.GetTenant())
	if err != nil {
		return nil, err
	}
	tid := mustId(t.GetId())

	row, err := s.ent(ctx).CameraWorker.Query().
		Where(cameraworker.HardwareIdEQ(strings.ToLower(hj.GetHardwareId())), cameraworker.TenantIdEQ(tid.Uuid()), cameraworker.DateErasedIsNil()).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	var js *joinState
	if row != nil {
		js = &joinState{id: pdid.Id(row.Id), state: api.HostState(row.State), join: row.Join, certSerial: row.CertSerial}
	}

	answer := api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING, CaBundle: s.bundle()}
	switch s.decide(js, hj) {
	case decCreate:
		id := pdid.New(DomCameraWorker)
		alias := aliasFor(hj.GetHostname(), func(a string) bool {
			n, _ := s.ent(ctx).CameraWorker.Query().Where(cameraworker.AliasEQ(a), cameraworker.TenantIdEQ(tid.Uuid()), cameraworker.DateErasedIsNil()).Count(ctx)
			return n > 0
		})
		if _, err := s.own(ctx).CameraWorker().Add(ctx, api.CameraWorkerAddRequest_builder{
			Id:         id.Bytes(),
			Tenant:     tenantRef(tid),
			Alias:      alias,
			HardwareId: strings.ToLower(hj.GetHardwareId()),
			Hostname:   hj.GetHostname(),
			State:      api.HostState_HOST_STATE_PENDING,
			Join:       s.joinRecord(hj, from, false),
			Version:    hj.GetVersion(),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = id.Bytes()
		answer.Alias = alias
		answer.Message = "waiting for adoption: shale camera-worker adopt " + alias
	case decPending:
		if _, err := s.own(ctx).CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{
			Ref:              api.CameraWorkerRef_builder{Id: js.id.Bytes()}.Build(),
			Join:             s.joinRecord(hj, from, js.join.GetRejoin()),
			Hostname:         z.Ptr(hj.GetHostname()),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Message = "waiting for adoption"
	case decIssued:
		answer.State = api.HostState_HOST_STATE_ADOPTED
		answer.Id = js.id.Bytes()
		answer.Alias = row.Alias
		answer.Certificate = js.join.GetCertificate()
	case decRejoin:
		// A public join cannot replace an approved key or broaden the adopted site.
		// A new key always requires a separate operator adoption, even when other
		// host kinds use an automatic readoption policy.
		rec := s.joinRecord(hj, from, true)
		if _, err := s.own(ctx).CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{
			Ref: api.CameraWorkerRef_builder{Id: js.id.Bytes()}.Build(), Join: rec, DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return nil, err
		}
		answer.Id, answer.Alias, answer.Rejoin = js.id.Bytes(), row.Alias, true
		answer.Message = "known worker, new key: waiting for adoption"
	}

	return api.CameraWorkerJoinResponse_builder{Answer: answer.Build()}.Build(), nil
}

func (s coreCameraWorker) RenewCertificate(ctx context.Context, req *api.CameraWorkerRenewCertificateRequest) (*api.CameraWorkerRenewCertificateResponse, error) {
	_, currentErr := s.cameraWorkerActor(ctx)
	if currentErr != nil {
		return nil, currentErr
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomCameraWorker {
		return nil, status.Error(codes.PermissionDenied, "only a camera worker renews its own certificate")
	}
	key, err := workerCSRKey(req.GetCsr())
	if err != nil {
		return nil, err
	}
	r, err := s.own(ctx).CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: f.Actor.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if r.GetApprovedKeyFingerprint() == "" || key != r.GetApprovedKeyFingerprint() {
		return nil, denied("worker renew", "a new key requires explicit operator adoption")
	}
	rec := r.GetJoin()
	if rec == nil {
		rec = &api.HostJoin{}
	}
	rec.SetCsr(req.GetCsr())
	rec.SetKeyFingerprint(key[:23])
	rec.SetRejoin(false)
	pem, serial, exp, err := s.issueWorker(rec, f.Actor)
	if err != nil {
		return nil, err
	}
	rec.SetCertificate(pem)
	if _, err := s.own(ctx).CameraWorker().Patch(ctx, api.CameraWorkerPatchRequest_builder{
		Ref:              api.CameraWorkerRef_builder{Id: r.GetId()}.Build(),
		Join:             rec,
		CertSerial:       z.Ptr(serial),
		DateCertIssued:   timestamppb.New(s.d.now()),
		DateCertExpires:  timestamppb.New(exp),
		DateUpdatedForce: z.Ptr(true),
	}.Build()); err != nil {
		return nil, err
	}

	return api.CameraWorkerRenewCertificateResponse_builder{Certificate: pem, CaBundle: s.bundle()}.Build(), nil
}
