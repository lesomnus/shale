package producer

import (
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	catalog "github.com/lesomnus/shale/source"
)

// admissionRefused is a managed catalog this producer cannot take up yet,
// for a reason it knows the amounts of (§38.5): err is what is logged and
// what the reconciliation retries on.
type admissionRefused struct {
	reason              api.AdmissionWaitReason
	required, available int64
	err                 error
}

func (e *admissionRefused) Error() string { return e.err.Error() }
func (e *admissionRefused) Unwrap() error { return e.err }

func refused(reason api.AdmissionWaitReason, required, available int64, err error) error {
	return &admissionRefused{reason: reason, required: required, available: available, err: err}
}

// waitOf is why a reconciliation is waiting: this producer's own refusal,
// the AdmissionWait the CP attached to its own, or else what the status
// code says. Never the error's text.
func waitOf(err error) *api.AdmissionWait {
	var r *admissionRefused
	if errors.As(err, &r) {
		return api.AdmissionWait_builder{Reason: r.reason, Required: r.required, Available: r.available}.Build()
	}
	if st, ok := status.FromError(err); ok {
		for _, d := range st.Details() {
			if w, ok := d.(*api.AdmissionWait); ok && w.GetReason() != api.AdmissionWaitReason_ADMISSION_WAIT_REASON_UNSPECIFIED {
				return api.AdmissionWait_builder{Reason: w.GetReason(), Required: w.GetRequired(), Available: w.GetAvailable()}.Build()
			}
		}
	}
	reason := api.AdmissionWaitReason_ADMISSION_WAIT_REASON_OTHER
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated:
		reason = api.AdmissionWaitReason_ADMISSION_WAIT_REASON_AUTHORIZATION
	case codes.Aborted:
		reason = api.AdmissionWaitReason_ADMISSION_WAIT_REASON_STALE
	}

	return api.AdmissionWait_builder{Reason: reason}.Build()
}

// waiting is the cameras of a catalog not recording as it names them: no
// incarnation of theirs runs this exact capture.
func (p *Producer) waiting(next catalog.Snapshot) [][]byte {
	active := map[string]*source{}
	for _, s := range p.activeSources() {
		if s.spec != nil {
			active[s.spec.SourceID] = s
		}
	}
	var out [][]byte
	for _, v := range next.Sources {
		if s := active[v.SourceID]; s != nil && sameCapture(s, v, next.Revision) {
			continue
		}
		if id, err := pdid.Parse(v.SourceID); err == nil {
			out = append(out, id.Bytes())
		}
	}

	return out
}

// awaitAdmission keeps why the catalog's cameras wait, for the heartbeat to
// report (§38.5). The wait keeps its start while the reason and the catalog
// revision stay the same.
func (p *Producer) awaitAdmission(next catalog.Snapshot, err error, now time.Time) *api.AdmissionWait {
	w := waitOf(err)
	w.SetSourceIds(p.waiting(next))
	w.SetCatalogRevision(next.Revision.Catalog)
	since := now
	if was := p.admission.Load(); was != nil && was.GetReason() == w.GetReason() && was.GetCatalogRevision() == w.GetCatalogRevision() {
		since = was.GetDateSince().AsTime()
	}
	w.SetDateSince(timestamppb.New(since))
	p.admission.Store(w)

	return w
}
