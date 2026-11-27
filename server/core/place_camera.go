package core

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"uuid"

	"github.com/protobuf-orm/ent/dialect/sql"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameracredential"
	"github.com/lesomnus/shale/internal/ent/cameracredentialversion"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/internal/ent/cameranetwork"
	"github.com/lesomnus/shale/internal/ent/camerapolicy"
	"github.com/lesomnus/shale/internal/ent/camerapreset"
	"github.com/lesomnus/shale/internal/ent/cameraworker"
	entset "github.com/lesomnus/shale/internal/ent/set"
)

// A set whose cameras Shale manages is placed with its camera network
// (§7), because every camera operation checks that the network, its
// devices, the assignment and its bindings and jobs, the worker, the
// policy, the preset and the credentials are of one site with the set, its
// sources and its producer. What ties each kind to the set:
//
//   - CameraAssignment: names the set (set_id);
//   - CameraBinding: names an assignment of the set (assignment_id);
//   - CameraJob: names an assignment of the set (assignment_id);
//   - CameraNetwork: named by a live assignment of the set (network_id);
//   - CameraDevice: of the network (network_id);
//   - CameraCredential: of a device of the network (device_id);
//   - CameraCredentialVersion: of such a credential (credential_id);
//   - CameraPolicy: of the network (network_id);
//   - CameraWorker: named by a live assignment of the set (worker_id); its
//     site is its adoption's;
//   - CameraPreset: of the site alone, named by a policy of the network
//     (preset_id) or by a job of the set (inputs.preset). One in the site
//     the set leaves moves when nothing outside the network names it; one in
//     no site while the set leaves a site is the tenant's, usable from any
//     site (cameraPresetScope), and stays.
//
// Refused, each in a sentence the dry run answers too: a network another
// live set is assigned to, a worker another live set is assigned, a preset
// another network's policy or another set's job names, a preset whose
// rules name a site, and a job of the set under way, since a job checks at
// every step the scope it began with.
type cameraPlan struct {
	assignments []uuid.UUID
	networks    []uuid.UUID
	workers     []uuid.UUID
	presets     []uuid.UUID
	refused     []string
}

func (p *cameraPlan) managed() bool { return len(p.assignments) > 0 }

func uuidsOfSet(vs map[uuid.UUID]bool) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(vs))
	for v := range vs {
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b uuid.UUID) int { return strings.Compare(string(a[:]), string(b[:])) })

	return out
}

// setAliases is the aliases of the sets ids names, for a sentence.
func (s Core) setAliases(ctx context.Context, ids map[uuid.UUID]bool) string {
	vs, err := s.ent(ctx).Set.Query().Where(entset.IdIn(uuidsOfSet(ids)...)).All(ctx)
	if err != nil || len(vs) == 0 {
		return fmt.Sprintf("%d other set(s)", len(ids))
	}
	names := make([]string, 0, len(vs))
	for _, v := range vs {
		names = append(names, v.Alias)
	}
	slices.Sort(names)

	return strings.Join(names, ", ")
}

// cameraPlanOf is what of a set's cameras a placing moves, and what it
// refuses. It reads only: inside the placing's transaction it is asked
// once the set's row and the network's are held (placeRows), and for a
// dry run with nothing held.
func (s Core) cameraPlanOf(ctx context.Context, set, tenant pdid.Id, from []byte) (*cameraPlan, error) {
	db := s.ent(ctx)
	t := tenant.Uuid()
	as, err := db.CameraAssignment.Query().Where(cameraassignment.TenantIdEQ(t), cameraassignment.SetIdEQ(set.Uuid())).All(ctx)
	if err != nil || len(as) == 0 {
		return &cameraPlan{}, err
	}
	p := &cameraPlan{}
	networks, workers := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
	for _, a := range as {
		p.assignments = append(p.assignments, a.Id)
		if a.DateErased != nil {
			continue
		}
		networks[a.NetworkId] = true
		if a.WorkerId != nil && !isZero(*a.WorkerId) {
			workers[*a.WorkerId] = true
		}
	}
	p.networks, p.workers = uuidsOfSet(networks), uuidsOfSet(workers)
	var was uuid.UUID
	if len(from) > 0 {
		was = mustId(from).Uuid()
	}

	for _, n := range p.networks {
		row, err := db.CameraNetwork.Get(ctx, n)
		if err != nil {
			return nil, err
		}
		if row.SiteId != was {
			p.refused = append(p.refused, fmt.Sprintf("camera network %s is not in the site the set is in; its cameras are not working as it is", row.Alias))
		}
		others, err := db.CameraAssignment.Query().Where(cameraassignment.TenantIdEQ(t), cameraassignment.NetworkIdEQ(n), cameraassignment.SetIdNEQ(set.Uuid()), cameraassignment.DateErasedIsNil()).All(ctx)
		if err != nil {
			return nil, err
		}
		if len(others) > 0 {
			sets := map[uuid.UUID]bool{}
			for _, o := range others {
				sets[o.SetId] = true
			}
			p.refused = append(p.refused, fmt.Sprintf("camera network %s also serves %s: a network moves with every set on it or not at all, and placing several sets at once is not built; "+
				"give this set's cameras a network of their own first, or leave them where they are", row.Alias, s.setAliases(ctx, sets)))
		}
	}
	for _, w := range p.workers {
		row, err := db.CameraWorker.Get(ctx, w)
		if err != nil {
			return nil, err
		}
		if row.SiteId != was {
			p.refused = append(p.refused, fmt.Sprintf("camera worker %s is not in the site the set is in", row.Alias))
		}
		others, err := db.CameraAssignment.Query().Where(cameraassignment.TenantIdEQ(t), cameraassignment.WorkerIdEQ(w), cameraassignment.SetIdNEQ(set.Uuid()), cameraassignment.DateErasedIsNil()).All(ctx)
		if err != nil {
			return nil, err
		}
		if len(others) > 0 {
			sets := map[uuid.UUID]bool{}
			for _, o := range others {
				sets[o.SetId] = true
			}
			p.refused = append(p.refused, fmt.Sprintf("camera worker %s also works for %s: a worker is adopted in one site; "+
				"adopt another worker for the other sets, or for this one, first", row.Alias, s.setAliases(ctx, sets)))
		}
	}

	jobs, err := db.CameraJob.Query().Where(camerajob.TenantIdEQ(t)).All(ctx)
	if err != nil {
		return nil, err
	}
	ours := map[uuid.UUID]bool{}
	for _, a := range p.assignments {
		ours[a] = true
	}
	running := 0
	named := map[uuid.UUID]bool{}
	for _, j := range jobs {
		if !ours[j.AssignmentId] {
			continue
		}
		if !cameraTerminalStage(api.CameraJobStage(j.Stage)) {
			running++
		}
		if id, ok := jobPreset(j); ok {
			named[id] = true
		}
	}
	if running > 0 {
		p.refused = append(p.refused, fmt.Sprintf("%d camera job(s) of the set are under way, and a job checks at every step the site it began in: "+
			"wait for them to finish, or cancel them, and place the set then", running))
	}
	if len(p.networks) > 0 {
		pols, err := db.CameraPolicy.Query().Where(camerapolicy.TenantIdEQ(t), camerapolicy.NetworkIdIn(p.networks...)).All(ctx)
		if err != nil {
			return nil, err
		}
		for _, v := range pols {
			named[v.PresetId] = true
		}
	}
	for _, id := range uuidsOfSet(named) {
		pr, err := db.CameraPreset.Get(ctx, id)
		if ent.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if pr.SiteId != was {
			// Not of the site the set leaves: a preset in no site, which
			// any site's cameras may use, and which stays.
			continue
		}
		elsewhere, err := db.CameraPolicy.Query().Where(camerapolicy.TenantIdEQ(t), camerapolicy.PresetIdEQ(id), camerapolicy.NetworkIdNotIn(p.networks...)).Count(ctx)
		if err != nil {
			return nil, err
		}
		theirs := 0
		for _, j := range jobs {
			if v, ok := jobPreset(j); ok && v == id && !ours[j.AssignmentId] {
				theirs++
			}
		}
		if elsewhere > 0 || theirs > 0 {
			p.refused = append(p.refused, fmt.Sprintf("camera preset %s (revision %s) is also named by %d policy(ies) of other camera networks and %d job(s) of other sets: "+
				"a preset is of its site and moves only with everything that names it; publish a revision of it for this network and activate it, and place the set then",
				pr.PresetKey, pr.Revision, elsewhere, theirs))
			continue
		}
		if slices.ContainsFunc(pr.Rules, func(r *api.CameraPresetRule) bool { return r.GetScope().GetSiteId() != "" }) {
			p.refused = append(p.refused, fmt.Sprintf("camera preset %s (revision %s) has rules that name a site, which a move would change the meaning of: "+
				"publish a revision whose rules name none, activate it, and place the set then", pr.PresetKey, pr.Revision))
			continue
		}
		p.presets = append(p.presets, id)
	}

	return p, nil
}

// jobPreset is the preset a job's inputs name, if any.
func jobPreset(j *ent.CameraJob) (uuid.UUID, bool) {
	ref := j.Inputs.GetPreset()
	if ref == nil || ref.GetId() == "" {
		return uuid.UUID{}, false
	}
	id, err := pdid.Parse(ref.GetId())
	if err != nil {
		return uuid.UUID{}, false
	}

	return id.Uuid(), true
}

// placeCameraCount counts what placeCameraRows would rewrite.
func (s Core) placeCameraCount(ctx context.Context, p *cameraPlan, tenant pdid.Id, out *api.SetPlaced) error {
	if !p.managed() {
		return nil
	}
	db, t := s.ent(ctx), tenant.Uuid()
	counts := []struct {
		n   func() (int, error)
		set func(int64)
	}{
		{func() (int, error) {
			return db.CameraAssignment.Query().Where(cameraassignment.TenantIdEQ(t), cameraassignment.IdIn(p.assignments...)).Count(ctx)
		}, out.SetCameraAssignments},
		{func() (int, error) {
			return db.CameraBinding.Query().Where(camerabinding.TenantIdEQ(t), camerabinding.AssignmentIdIn(p.assignments...)).Count(ctx)
		}, out.SetCameraBindings},
		{func() (int, error) {
			return db.CameraJob.Query().Where(camerajob.TenantIdEQ(t), camerajob.AssignmentIdIn(p.assignments...)).Count(ctx)
		}, out.SetCameraJobs},
		{func() (int, error) {
			return db.CameraNetwork.Query().Where(cameranetwork.TenantIdEQ(t), cameranetwork.IdIn(p.networks...)).Count(ctx)
		}, out.SetCameraNetworks},
		{func() (int, error) {
			return db.CameraDevice.Query().Where(cameradevice.TenantIdEQ(t), cameradevice.NetworkIdIn(p.networks...)).Count(ctx)
		}, out.SetCameraDevices},
		{func() (int, error) {
			return db.CameraCredential.Query().Where(cameracredential.TenantIdEQ(t), cameracredential.HasDeviceWith(cameradevice.NetworkIdIn(p.networks...))).Count(ctx)
		}, out.SetCameraCredentials},
		{func() (int, error) {
			return db.CameraCredentialVersion.Query().Where(cameracredentialversion.TenantIdEQ(t),
				cameracredentialversion.HasCredentialWith(cameracredential.HasDeviceWith(cameradevice.NetworkIdIn(p.networks...)))).Count(ctx)
		}, out.SetCameraCredentialVersions},
		{func() (int, error) {
			return db.CameraPolicy.Query().Where(camerapolicy.TenantIdEQ(t), camerapolicy.NetworkIdIn(p.networks...)).Count(ctx)
		}, out.SetCameraPolicies},
		{func() (int, error) {
			return db.CameraPreset.Query().Where(camerapreset.TenantIdEQ(t), camerapreset.IdIn(p.presets...)).Count(ctx)
		}, out.SetCameraPresets},
		{func() (int, error) {
			return db.CameraWorker.Query().Where(cameraworker.TenantIdEQ(t), cameraworker.IdIn(p.workers...)).Count(ctx)
		}, out.SetCameraWorkers},
	}
	for _, c := range counts {
		n, err := c.n()
		if err != nil {
			return err
		}
		c.set(int64(n))
	}

	return nil
}

// placeCameraNetworks writes the networks' site first of the camera rows,
// which holds them: every camera operation on an assignment takes the
// set's row and then the network's (cameraAssignmentRowChecked), adding a
// device or a policy takes the network's, and none of them writes a row of
// the network until this transaction ends.
func (s Core) placeCameraNetworks(ctx context.Context, p *cameraPlan, tenant, to pdid.Id) (int, error) {
	if len(p.networks) == 0 {
		return 0, nil
	}
	site := to.Uuid()

	return s.ent(ctx).CameraNetwork.Update().Where(cameranetwork.TenantIdEQ(tenant.Uuid()), cameranetwork.IdIn(p.networks...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(cameranetwork.FieldSiteId, site) }).Save(ctx)
}

// placeCameraRows rewrites the rest of the plan's camera rows, a statement
// a kind, each narrowed to the tenant first: devices (by network), their
// credentials and the versions (by device), assignments (by set), their
// bindings and jobs (by assignment), the networks' policies, the presets
// only they name, and the worker. Nothing but the site is written: no
// version moves, so no operation under way is refused for a row changed
// under it.
func (s Core) placeCameraRows(ctx context.Context, p *cameraPlan, tenant, to pdid.Id, out *api.SetPlaced) error {
	db, t, site := s.ent(ctx), tenant.Uuid(), to.Uuid()
	n, err := db.CameraDevice.Update().Where(cameradevice.TenantIdEQ(t), cameradevice.NetworkIdIn(p.networks...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(cameradevice.FieldSiteId, site) }).Save(ctx)
	if err != nil {
		return err
	}
	out.SetCameraDevices(int64(n))
	devices := cameradevice.NetworkIdIn(p.networks...)
	if n, err = db.CameraCredential.Update().Where(cameracredential.TenantIdEQ(t), cameracredential.HasDeviceWith(devices)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(cameracredential.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraCredentials(int64(n))
	if n, err = db.CameraCredentialVersion.Update().Where(cameracredentialversion.TenantIdEQ(t), cameracredentialversion.HasCredentialWith(cameracredential.HasDeviceWith(devices))).
		Modify(func(u *sql.UpdateBuilder) { u.Set(cameracredentialversion.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraCredentialVersions(int64(n))
	if n, err = db.CameraAssignment.Update().Where(cameraassignment.TenantIdEQ(t), cameraassignment.IdIn(p.assignments...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(cameraassignment.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraAssignments(int64(n))
	if n, err = db.CameraBinding.Update().Where(camerabinding.TenantIdEQ(t), camerabinding.AssignmentIdIn(p.assignments...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(camerabinding.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraBindings(int64(n))
	if n, err = db.CameraJob.Update().Where(camerajob.TenantIdEQ(t), camerajob.AssignmentIdIn(p.assignments...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(camerajob.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraJobs(int64(n))
	if n, err = db.CameraPolicy.Update().Where(camerapolicy.TenantIdEQ(t), camerapolicy.NetworkIdIn(p.networks...)).
		Modify(func(u *sql.UpdateBuilder) { u.Set(camerapolicy.FieldSiteId, site) }).Save(ctx); err != nil {
		return err
	}
	out.SetCameraPolicies(int64(n))
	if len(p.presets) > 0 {
		if n, err = db.CameraPreset.Update().Where(camerapreset.TenantIdEQ(t), camerapreset.IdIn(p.presets...)).
			Modify(func(u *sql.UpdateBuilder) { u.Set(camerapreset.FieldSiteId, site) }).Save(ctx); err != nil {
			return err
		}
		out.SetCameraPresets(int64(n))
	}
	if len(p.workers) > 0 {
		// A worker's site is mutable: it is its adoption's.
		if n, err = db.CameraWorker.Update().Where(cameraworker.TenantIdEQ(t), cameraworker.IdIn(p.workers...)).SetSiteId(site).Save(ctx); err != nil {
			return err
		}
		out.SetCameraWorkers(int64(n))
	}

	return nil
}
