package core

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbpgx"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/internal/ent/producer"
	"github.com/lesomnus/shale/internal/ent/source"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// placeFixture is the allocation fixture (three cameras, three sinks) with
// its set in no site, a site to put it in, a producer adopted for it, and a
// person who sees every site.
type placeFixture struct {
	*capFixture
	site     *api.Site
	producer pdid.Id
	person   context.Context
}

func newPlaceFixture(t *testing.T, f *capFixture) *placeFixture {
	t.Helper()
	ctx := t.Context()
	site, err := f.sink.Site().Add(ctx, api.SiteAddRequest_builder{Tenant: f.tenant, Alias: "hall"}.Build())
	require.NoError(t, err)
	p := pdid.New(DomProducer)
	_, err = f.sink.Producer().Add(ctx, api.ProducerAddRequest_builder{
		Id: p.Bytes(), Tenant: f.tenant, Alias: "pi", HardwareId: "pi", State: api.HostState_HOST_STATE_ADOPTED,
		Set: api.SetRef_builder{Id: f.set.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
	h, err := f.sink.Holder().Add(ctx, api.HolderAddRequest_builder{Id: pdid.New(DomHolder).Bytes(), Tenant: f.tenant, Alias: "admin", AllSites: true}.Build())
	require.NoError(t, err)
	person := frame.Into(context.Background(), frame.New(mustId(h.GetId()), mustId(f.tenant.GetId()), frame.Whole()).WithRow(h))

	return &placeFixture{capFixture: f, site: site, producer: p, person: person}
}

func (f *placeFixture) place(dry bool) (*api.SetPlaceResponse, error) {
	return f.core.Set().Place(f.person, api.SetPlaceRequest_builder{
		Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Site: api.SiteRef_builder{Id: f.site.GetId()}.Build(), DryRun: dry,
	}.Build())
}

// sites is the site of every row of the set, by kind, as the database
// holds it: "" for none.
func (f *placeFixture) sites(t *testing.T) map[string][]string {
	t.Helper()
	ctx := t.Context()
	of := func(v [16]byte) string {
		if isZero(v) {
			return ""
		}
		return pdid.Id(v).String()
	}
	out := map[string][]string{}
	set, err := f.client.Set.Get(ctx, mustId(f.set.GetId()).Uuid())
	require.NoError(t, err)
	out["set"] = []string{of(set.SiteId)}
	srcs, err := f.client.Source.Query().Where(source.SetIdEQ(set.Id)).All(ctx)
	require.NoError(t, err)
	for _, v := range srcs {
		out["source"] = append(out["source"], of(v.SiteId))
	}
	ls, err := f.client.Lamina.Query().Where(lamina.SetIdEQ(set.Id)).All(ctx)
	require.NoError(t, err)
	for _, v := range ls {
		out["lamina"] = append(out["lamina"], of(v.SiteId))
	}
	as, err := f.client.Attempt.Query().Where(attempt.HasLaminaWith(lamina.SetIdEQ(set.Id))).All(ctx)
	require.NoError(t, err)
	for _, v := range as {
		out["attempt"] = append(out["attempt"], of(v.SiteId))
	}
	ps, err := f.client.Producer.Query().Where(producer.SetIdEQ(set.Id)).All(ctx)
	require.NoError(t, err)
	for _, v := range ps {
		out["producer"] = append(out["producer"], of(v.SiteId))
	}

	return out
}

// every says each row of each kind carries site.
func every(t *testing.T, got map[string][]string, site string) {
	t.Helper()
	for kind, vs := range got {
		for _, v := range vs {
			require.Equal(t, site, v, "a %s", kind)
		}
	}
}

// pgConfig is the PostgreSQL database SHALE_E2E_DB_DSN names, emptied
// first, as the end-to-end suite's run on PostgreSQL is; the test skips
// without one.
func pgConfig(t *testing.T) config.DbConfig {
	t.Helper()
	dsn := os.Getenv("SHALE_E2E_DB_DSN")
	if dsn == "" {
		t.Skip("SHALE_E2E_DB_DSN names no PostgreSQL database")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	_, err = db.Exec("drop schema public cascade; create schema public")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	return config.DbConfig{Driver: "pgx", Dsn: dsn}
}

// A set placed into a site carries it on every row it has: the set, its
// sources, their laminae and attempts, its producer; the counts are the
// rows rewritten; a dry run counts the same and writes nothing; placing it
// where it is writes nothing and counts nothing.
func TestPlaceMovesEveryRowOfTheSet(t *testing.T) {
	f := newPlaceFixture(t, newCapFixture(t))
	f.allocate()
	before := f.sites(t)
	every(t, before, "")
	require.NotEmpty(t, before["lamina"])

	// The producer, with no site, sees every site; the answer is asked
	// again below, within the time a person's is kept.
	pctx := frame.Into(context.Background(), frame.New(f.producer, mustId(f.tenant.GetId()), frame.Whole()))
	sites := NewSites(f.core.d)
	_, all, err := sites.Of(pctx)
	require.NoError(t, err)
	require.True(t, all)

	dry, err := f.place(true)
	require.NoError(t, err)
	require.True(t, dry.GetDryRun())
	require.Equal(t, before, f.sites(t), "a dry run writes nothing")

	got, err := f.place(false)
	require.NoError(t, err)
	site := mustId(f.site.GetId()).String()
	after := f.sites(t)
	every(t, after, site)
	require.Equal(t, int64(len(after["source"])), got.GetMoved().GetSources())
	require.Equal(t, int64(len(after["lamina"])), got.GetMoved().GetLaminae())
	require.Equal(t, int64(len(after["attempt"])), got.GetMoved().GetAttempts())
	require.Equal(t, int64(1), got.GetMoved().GetProducers())
	require.Equal(t, got.GetMoved().GetLaminae(), dry.GetMoved().GetLaminae(), "the dry run counted the same")
	require.Equal(t, got.GetMoved().GetAttempts(), dry.GetMoved().GetAttempts())
	require.Equal(t, got.GetMoved().GetSources(), dry.GetMoved().GetSources())
	require.Equal(t, got.GetMoved().GetProducers(), dry.GetMoved().GetProducers())
	require.Empty(t, got.GetFromSiteId())
	require.Equal(t, f.site.GetId(), got.GetSet().GetSite().GetId())

	// The producer sees its set's new site at once: what it reads of its
	// own set is not held back by a remembered answer.
	vs, all, err := sites.Of(pctx)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, [][16]byte{mustId(f.site.GetId()).Uuid()}, uuidArrays(vs))

	again, err := f.place(false)
	require.NoError(t, err)
	require.Zero(t, again.GetMoved().GetSources()+again.GetMoved().GetLaminae()+again.GetMoved().GetAttempts()+again.GetMoved().GetProducers())
	require.Equal(t, got.GetSet().GetDateUpdated().AsTime(), again.GetSet().GetDateUpdated().AsTime(), "already there: nothing written")
	require.Equal(t, after, f.sites(t))
}

// managedCameras is what a set whose cameras Shale manages has, every
// kind once: a worker, a network, a device, a preset and the network's
// policy naming it, an assignment of the set's producer to the network
// with the worker, a binding of the set's first source to the device, a
// job of the binding naming the policy and the preset, and a credential of
// the device with a version. All in the site the set is in, none.
type managedCameras struct {
	worker, network, device, preset, policy, assignment, binding, job, credential, version pdid.Id
}

func (f *placeFixture) managed(t *testing.T, alias string, stage api.CameraJobStage) managedCameras {
	t.Helper()
	ctx := t.Context()
	m := managedCameras{worker: pdid.New(DomCameraWorker), network: pdid.New(DomCameraNetwork), device: pdid.New(DomCameraDevice), preset: pdid.New(DomCameraPreset),
		policy: pdid.New(DomCameraPolicy), assignment: pdid.New(DomCameraAssignment), binding: pdid.New(DomCameraBinding), job: pdid.New(DomCameraJob),
		credential: pdid.New(DomCameraCredential), version: pdid.New(DomCameraCredentialVersion)}
	_, err := f.sink.CameraWorker().Add(ctx, api.CameraWorkerAddRequest_builder{Id: m.worker.Bytes(), Tenant: f.tenant, Alias: alias + "-worker", HardwareId: alias + "-worker",
		State: api.HostState_HOST_STATE_ADOPTED, CertSerial: "serial", DateCertExpires: timestamppb.New(time.Now().Add(time.Hour))}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraNetwork().Add(ctx, api.CameraNetworkAddRequest_builder{Id: m.network.Bytes(), Tenant: f.tenant, Alias: alias + "-lan", Cidrs: []string{"10.0.0.0/24"}, Revision: 1}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraDevice().Add(ctx, api.CameraDeviceAddRequest_builder{Id: m.device.Bytes(), Tenant: f.tenant, Network: api.CameraNetworkRef_builder{Id: m.network.Bytes()}.Build(),
		DeviceKey: alias + "-cam", ObservationRevision: 1}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraPreset().Add(ctx, api.CameraPresetAddRequest_builder{Id: m.preset.Bytes(), Tenant: f.tenant, PresetKey: alias + "-preset", Revision: "1"}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraPolicy().Add(ctx, api.CameraPolicyAddRequest_builder{Id: m.policy.Bytes(), Tenant: f.tenant, Network: api.CameraNetworkRef_builder{Id: m.network.Bytes()}.Build(),
		Preset: api.CameraPresetRef_builder{Id: m.preset.Bytes()}.Build(), PolicyKey: alias + "-policy", Revision: "1", Enrollment: &api.CameraEnrollmentPolicy{}}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraAssignment().Add(ctx, api.CameraAssignmentAddRequest_builder{Id: m.assignment.Bytes(), Tenant: f.tenant,
		Network: api.CameraNetworkRef_builder{Id: m.network.Bytes()}.Build(), Set: api.SetRef_builder{Id: f.set.GetId()}.Build(),
		Producer: api.ProducerRef_builder{Id: f.producer.Bytes()}.Build(), WorkerSlot: "slot", WorkerId: m.worker.Bytes(), Generation: 1}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraBinding().Add(ctx, api.CameraBindingAddRequest_builder{Id: m.binding.Bytes(), Tenant: f.tenant,
		Assignment: api.CameraAssignmentRef_builder{Id: m.assignment.Bytes()}.Build(), Device: api.CameraDeviceRef_builder{Id: m.device.Bytes()}.Build(),
		Source: api.SourceRef_builder{Id: f.srcs[0].GetId()}.Build(), LogicalKey: alias + "-cam", SourceAlias: f.srcs[0].GetAlias()}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraJob().Add(ctx, api.CameraJobAddRequest_builder{Id: m.job.Bytes(), Tenant: f.tenant,
		Binding: api.CameraBindingRef_builder{Id: m.binding.Bytes()}.Build(), Assignment: api.CameraAssignmentRef_builder{Id: m.assignment.Bytes()}.Build(),
		RequestKey: alias + "-job", RequestHash: []byte{1}, Stage: stage, AssignmentGeneration: 1, Inputs: api.CameraJobInputs_builder{
			Policy: api.CameraRevisionReference_builder{Id: m.policy.String(), Revision: "1"}.Build(),
			Preset: api.CameraRevisionReference_builder{Id: m.preset.String(), Revision: "1"}.Build(),
		}.Build()}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraCredential().Add(ctx, api.CameraCredentialAddRequest_builder{Id: m.credential.Bytes(), Tenant: f.tenant,
		Device: api.CameraDeviceRef_builder{Id: m.device.Bytes()}.Build(), BindingKey: "device", Purpose: "management", CurrentVersion: "v1"}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraCredentialVersion().Add(ctx, api.CameraCredentialVersionAddRequest_builder{Id: m.version.Bytes(), Tenant: f.tenant,
		Credential: api.CameraCredentialRef_builder{Id: m.credential.Bytes()}.Build(), Version: "v1", IntentKey: "intent", Access: "read", Backend: "test",
		SealedEnvelope: []byte{1}, RequestHash: []byte{1}, Origin: api.CameraCredentialOrigin_CAMERA_CREDENTIAL_ORIGIN_GENERATED}.Build())
	require.NoError(t, err)

	return m
}

// cameraSites is the site of each camera row m names, by kind: "" for none.
func (f *placeFixture) cameraSites(t *testing.T, m managedCameras) map[string]string {
	t.Helper()
	ctx := t.Context()
	of := func(v [16]byte) string {
		if isZero(v) {
			return ""
		}
		return pdid.Id(v).String()
	}
	out := map[string]string{}
	get := func(kind string, v [16]byte, err error) {
		require.NoError(t, err, kind)
		out[kind] = of(v)
	}
	w, err := f.client.CameraWorker.Get(ctx, m.worker.Uuid())
	get("worker", w.SiteId, err)
	n, err := f.client.CameraNetwork.Get(ctx, m.network.Uuid())
	get("network", n.SiteId, err)
	d, err := f.client.CameraDevice.Get(ctx, m.device.Uuid())
	get("device", d.SiteId, err)
	pr, err := f.client.CameraPreset.Get(ctx, m.preset.Uuid())
	get("preset", pr.SiteId, err)
	po, err := f.client.CameraPolicy.Get(ctx, m.policy.Uuid())
	get("policy", po.SiteId, err)
	a, err := f.client.CameraAssignment.Get(ctx, m.assignment.Uuid())
	get("assignment", a.SiteId, err)
	b, err := f.client.CameraBinding.Get(ctx, m.binding.Uuid())
	get("binding", b.SiteId, err)
	j, err := f.client.CameraJob.Get(ctx, m.job.Uuid())
	get("job", j.SiteId, err)
	c, err := f.client.CameraCredential.Get(ctx, m.credential.Uuid())
	get("credential", c.SiteId, err)
	v, err := f.client.CameraCredentialVersion.Get(ctx, m.version.Uuid())
	get("credential version", v.SiteId, err)

	return out
}

func everyCamera(t *testing.T, got map[string]string, site string) {
	t.Helper()
	for kind, v := range got {
		require.Equal(t, site, v, "a camera %s", kind)
	}
}

// A set whose cameras Shale manages, on a network of its own, in no site,
// is placed with its network and everything of it: every camera kind is in
// the new site, the checks a worker's job makes of them hold there, the
// worker sees its rows at once, and the dry run counted each kind.
func TestPlaceMovesAManagedSetWithItsNetwork(t *testing.T) {
	f := newPlaceFixture(t, newCapFixture(t))
	m := f.managed(t, "yard", api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE)
	everyCamera(t, f.cameraSites(t, m), "")

	dry, err := f.place(true)
	require.NoError(t, err)
	require.Empty(t, dry.GetRefused())
	d := dry.GetMoved()
	for kind, n := range map[string]int64{
		"assignments": d.GetCameraAssignments(), "bindings": d.GetCameraBindings(), "jobs": d.GetCameraJobs(), "networks": d.GetCameraNetworks(),
		"devices": d.GetCameraDevices(), "credentials": d.GetCameraCredentials(), "credential versions": d.GetCameraCredentialVersions(),
		"policies": d.GetCameraPolicies(), "presets": d.GetCameraPresets(), "workers": d.GetCameraWorkers(),
	} {
		require.Equal(t, int64(1), n, "the dry run's camera %s", kind)
	}
	everyCamera(t, f.cameraSites(t, m), "")

	got, err := f.place(false)
	require.NoError(t, err)
	site := mustId(f.site.GetId()).String()
	everyCamera(t, f.cameraSites(t, m), site)
	every(t, f.sites(t), site)
	require.Equal(t, d.GetCameraDevices(), got.GetMoved().GetCameraDevices())
	require.Equal(t, d.GetCameraWorkers(), got.GetMoved().GetCameraWorkers())
	require.Equal(t, d.GetCameraPresets(), got.GetMoved().GetCameraPresets())

	// The worker, as its certificate makes it the caller: its sites are
	// the new one at once, the wall shows it its assignment, and a job's
	// rows pass every check of one site -- assignment, network, set,
	// producer, job, binding and device.
	row, err := f.sink.CameraWorker().Get(t.Context(), api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: m.worker.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	wctx := frame.Into(context.Background(), frame.New(m.worker, mustId(f.tenant.GetId()), frame.Whole()).WithScope(frame.Only(mustId(f.tenant.GetId()))).WithRow(row))
	sites := NewSites(f.core.d)
	vs, all, err := sites.Of(wctx)
	require.NoError(t, err)
	require.False(t, all)
	require.Equal(t, [][16]byte{mustId(f.site.GetId()).Uuid()}, uuidArrays(vs))
	walled, err := pd.NewSink(f.client, bare.WithScope(bare.Scopes{pd.Wall(), pd.Grouped(sites.Of)}))
	require.NoError(t, err)
	_, err = walled.CameraAssignment().Get(wctx, api.CameraAssignmentGetRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: m.assignment.Bytes()}.Build()}.Build())
	require.NoError(t, err, "the worker reads its assignment through the wall")
	j, a, _, dev, err := f.core.cameraJobRows(wctx, api.CameraJobRef_builder{Id: m.job.Bytes()}.Build())
	require.NoError(t, err, "a worker's job: every row of one site")
	_, err = f.core.cameraPinnedInputs(f.person, a, dev, j.GetInputs())
	require.NoError(t, err, "the job's policy and preset are of the assignment's site")
	require.NoError(t, cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), f.tenant.GetId(), f.site.GetId()))
}

// A network another set is assigned to moves with it or not at all, and
// placing several sets at once is not built: refused, naming the other
// set, by the dry run as by the placing, and nothing moves.
func TestPlaceRefusesANetworkOfTwoSets(t *testing.T) {
	f := newPlaceFixture(t, newCapFixture(t))
	m := f.managed(t, "yard", api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE)
	ctx := t.Context()
	other, err := f.sink.Set().Add(ctx, api.SetAddRequest_builder{Tenant: f.tenant, Alias: "barn"}.Build())
	require.NoError(t, err)
	pi := pdid.New(DomProducer)
	_, err = f.sink.Producer().Add(ctx, api.ProducerAddRequest_builder{Id: pi.Bytes(), Tenant: f.tenant, Alias: "barn-pi", HardwareId: "barn-pi", State: api.HostState_HOST_STATE_ADOPTED,
		Set: api.SetRef_builder{Id: other.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraAssignment().Add(ctx, api.CameraAssignmentAddRequest_builder{Tenant: f.tenant, Network: api.CameraNetworkRef_builder{Id: m.network.Bytes()}.Build(),
		Set: api.SetRef_builder{Id: other.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: pi.Bytes()}.Build(), WorkerSlot: "slot", Generation: 1}.Build())
	require.NoError(t, err)

	dry, err := f.place(true)
	require.NoError(t, err)
	require.Len(t, dry.GetRefused(), 1)
	require.Contains(t, dry.GetRefused()[0], "yard-lan also serves barn")
	_, err = f.place(false)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "yard-lan also serves barn")
	everyCamera(t, f.cameraSites(t, m), "")
	every(t, f.sites(t), "")
}

// A preset is of its site alone: one another network's policy also names
// moves only with it, so the placing is refused, saying how many name it
// and what an operator does; so is a set whose job is under way.
func TestPlaceRefusesWhatItsCamerasShare(t *testing.T) {
	t.Run("a preset of another network", func(t *testing.T) {
		f := newPlaceFixture(t, newCapFixture(t))
		m := f.managed(t, "yard", api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE)
		ctx := t.Context()
		lan := pdid.New(DomCameraNetwork)
		_, err := f.sink.CameraNetwork().Add(ctx, api.CameraNetworkAddRequest_builder{Id: lan.Bytes(), Tenant: f.tenant, Alias: "barn-lan", Cidrs: []string{"10.0.1.0/24"}, Revision: 1}.Build())
		require.NoError(t, err)
		_, err = f.sink.CameraPolicy().Add(ctx, api.CameraPolicyAddRequest_builder{Tenant: f.tenant, Network: api.CameraNetworkRef_builder{Id: lan.Bytes()}.Build(),
			Preset: api.CameraPresetRef_builder{Id: m.preset.Bytes()}.Build(), PolicyKey: "barn-policy", Revision: "1", Enrollment: &api.CameraEnrollmentPolicy{}}.Build())
		require.NoError(t, err)

		dry, err := f.place(true)
		require.NoError(t, err)
		require.Len(t, dry.GetRefused(), 1)
		require.Contains(t, dry.GetRefused()[0], "camera preset yard-preset (revision 1) is also named by 1 policy(ies) of other camera networks")
		_, err = f.place(false)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		everyCamera(t, f.cameraSites(t, m), "")
	})
	t.Run("a job under way", func(t *testing.T) {
		f := newPlaceFixture(t, newCapFixture(t))
		m := f.managed(t, "yard", api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING)
		_, err := f.place(false)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		require.Contains(t, status.Convert(err).Message(), "1 camera job(s) of the set are under way")
		everyCamera(t, f.cameraSites(t, m), "")
	})
}

// An allocation made while a move is held open is written with the site
// the move gives the set, not the one it read before: the move holds the
// set's row, the allocation waits for it and reads the site again inside
// its own transaction. So is a source added meanwhile. On SQLite and, with
// SHALE_E2E_DB_DSN, on PostgreSQL.
func TestPlaceWhileAllocating(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testPlaceWhileAllocating(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testPlaceWhileAllocating(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testPlaceWhileAllocating(t *testing.T, cf *capFixture) {
	f := newPlaceFixture(t, cf)
	// A recording before the move.
	_, err := f.allocateOne(f.srcs[0], f.slot(f.srcs[0]))
	require.NoError(t, err)

	held, release := make(chan struct{}), make(chan struct{})
	f.core.d.placeHeld = func(context.Context) {
		close(held)
		<-release
	}
	placed := make(chan error, 1)
	go func() {
		_, err := f.place(false)
		placed <- err
	}()
	select {
	case <-held:
	case err := <-placed:
		t.Fatalf("the move ended without being held open: %v", err)
	}

	type result struct {
		al  *api.Allocation
		err error
	}
	allocated := make(chan result, 1)
	go func() {
		al, err := f.allocateOne(f.srcs[1], f.slot(f.srcs[1]))
		allocated <- result{al, err}
	}()
	added := make(chan error, 1)
	go func() {
		_, err := f.core.Source().Add(f.ctx, api.SourceAddRequest_builder{Tenant: f.tenant, Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Alias: "porch"}.Build())
		added <- err
	}()
	select {
	case r := <-allocated:
		t.Fatalf("the allocation was answered while the move was held open: %v", r.err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-placed)
	r := <-allocated
	require.NoError(t, r.err)
	require.NotEmpty(t, r.al.GetCandidates())
	require.NoError(t, <-added)

	every(t, f.sites(t), mustId(f.site.GetId()).String())
}

func uuidArrays[T ~[16]byte](vs []T) [][16]byte {
	out := make([][16]byte, 0, len(vs))
	for _, v := range vs {
		out = append(out, [16]byte(v))
	}
	return out
}
