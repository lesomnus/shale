package core

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
)

// A write that holds nothing -- a producer's heartbeat storing a source's
// starvation -- attempted while a transaction that read the source holds it
// waits for that transaction, which then patches the source with the
// version it read, and the write lands after it. On SQLite every
// transaction holds the database from its BEGIN, so the write waits there
// with or without the hold; on PostgreSQL (SHALE_E2E_DB_DSN) it commits in
// between unless the row is held, and the patch is refused.
func TestPublishHoldsTheSource(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testPublishHoldsTheSource(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testPublishHoldsTheSource(t, newCapFixtureOn(t, pgConfig(t))) })
}

// Negotiate for a set whose cameras Shale does not manage reads the set and
// its sources inside the transaction that patches them, and holds them: a
// heartbeat's write meanwhile waits, on SQLite as on PostgreSQL. Before, it
// read them before its transaction, and the write landed in between on
// either.
func TestNegotiateHoldsTheSources(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testNegotiateHoldsTheSources(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testNegotiateHoldsTheSources(t, newCapFixtureOn(t, pgConfig(t))) })
}

// heldOpen runs `call` until it reaches the hook `hold` installs, then
// `write` beside it, and requires the write to wait for the call: neither
// is answered while the call is held, and both succeed once it goes on.
func heldOpen(t *testing.T, hold func(func(context.Context)), call func() error, write func() error) {
	t.Helper()
	held, release := make(chan struct{}), make(chan struct{})
	hold(func(context.Context) {
		close(held)
		<-release
	})
	called := make(chan error, 1)
	go func() { called <- call() }()
	select {
	case <-held:
	case err := <-called:
		t.Fatalf("answered without being held open: %v", err)
	}
	hold(nil)

	wrote := make(chan error, 1)
	go func() { wrote <- write() }()
	select {
	case err := <-wrote:
		close(release)
		<-called
		t.Fatalf("the write landed while the row was held: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-called)
	require.NoError(t, <-wrote)
}

// starve is what a producer's heartbeat writes of a source (keepStarvation),
// through the server below, so that a refusal is seen rather than logged.
func (f *capFixture) starve(src []byte) error {
	ctx := context.Background()
	v, err := f.sink.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src}.Build()}.Build())
	if err != nil {
		return err
	}
	st := counted(v.GetStarvation(), api.SourceReport_builder{SourceId: src, Episodes: 1, SecondsTotal: 1}.Build(), f.now)
	_, err = f.sink.Source().Patch(ctx, starvationPatch(v, st))

	return err
}

func testPublishHoldsTheSource(t *testing.T, cf *capFixture) {
	f := newPlaceFixture(t, cf)
	ctx := t.Context()
	who := pdid.New(DomHolder)
	operator := frame.Into(context.Background(), frame.New(who, mustId(f.tenant.GetId()), frame.Whole()))

	// A job of the set's first source, verified and leased to the operator.
	network, device, assignment, binding, job := pdid.New(DomCameraNetwork), pdid.New(DomCameraDevice), pdid.New(DomCameraAssignment), pdid.New(DomCameraBinding), pdid.New(DomCameraJob)
	_, err := f.sink.CameraNetwork().Add(ctx, api.CameraNetworkAddRequest_builder{Id: network.Bytes(), Tenant: f.tenant, Alias: "lan", Cidrs: []string{"10.0.0.0/24"}, Revision: 1}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraAssignment().Add(ctx, api.CameraAssignmentAddRequest_builder{Id: assignment.Bytes(), Tenant: f.tenant,
		Network: api.CameraNetworkRef_builder{Id: network.Bytes()}.Build(), Set: api.SetRef_builder{Id: f.set.GetId()}.Build(),
		Producer: api.ProducerRef_builder{Id: f.producer.Bytes()}.Build(), WorkerSlot: "slot", Generation: 1}.Build())
	require.NoError(t, err)
	src := f.srcs[0]
	_, err = f.sink.CameraDevice().Add(ctx, api.CameraDeviceAddRequest_builder{Id: device.Bytes(), Tenant: f.tenant, Network: api.CameraNetworkRef_builder{Id: network.Bytes()}.Build(),
		DeviceKey: "door-cam", ObservationRevision: 1, ActiveJobId: job.Bytes(), ClaimGeneration: 1}.Build())
	require.NoError(t, err)
	_, err = f.sink.CameraBinding().Add(ctx, api.CameraBindingAddRequest_builder{Id: binding.Bytes(), Tenant: f.tenant,
		Assignment: api.CameraAssignmentRef_builder{Id: assignment.Bytes()}.Build(), Device: api.CameraDeviceRef_builder{Id: device.Bytes()}.Build(),
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), LogicalKey: "door-cam", SourceAlias: src.GetAlias()}.Build())
	require.NoError(t, err)
	j, err := f.sink.CameraJob().Add(ctx, api.CameraJobAddRequest_builder{Id: job.Bytes(), Tenant: f.tenant,
		Binding: api.CameraBindingRef_builder{Id: binding.Bytes()}.Build(), Assignment: api.CameraAssignmentRef_builder{Id: assignment.Bytes()}.Build(),
		RequestKey: "door-job", RequestHash: []byte{1}, Stage: api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING, AssignmentGeneration: 1,
		LeaseOwner: who.Bytes(), LeaseGeneration: 1, LeaseExpires: timestamppb.New(f.now.Add(time.Hour))}.Build())
	require.NoError(t, err)
	// The version is taken from a read, as a caller takes it. A generated
	// Add answers the row as it built it, stamped by the sink's clock with
	// nanoseconds; PostgreSQL keeps a microsecond, so that answer is not
	// the version Publish reads and compares, and it is refused there.
	// SQLite keeps the nanoseconds and hides it.
	j, err = f.sink.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job.Bytes()}.Build()}.Build())
	require.NoError(t, err)

	profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 160, Height: 90, Fps: 10, MaxBitrateBps: 1_000_000}.Build()
	observed := api.CameraJobObservation_builder{Effective: profile, Verified: api.CameraMediaEvidence_builder{
		Profile: profile, Decoded: true, DurationSeconds: 2, DecodedFrames: 20, ObservedAt: timestamppb.New(f.now),
	}.Build()}.Build()
	var published *api.CameraJobPublishResponse
	heldOpen(t, func(fn func(context.Context)) { f.core.d.publishHeld = fn }, func() error {
		var err error
		published, err = f.core.CameraJob().Publish(operator, api.CameraJobPublishRequest_builder{
			Ref: api.CameraJobRef_builder{Id: job.Bytes()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: "door-publish",
			LeaseGeneration: 1, AssignmentGeneration: 1, Observed: observed,
			Spec: api.CameraCaptureSpec_builder{BindingId: binding.Bytes(), SourceId: src.GetId(), Alias: src.GetAlias(), Input: "rtsp://10.0.0.5/main", Profile: profile, Name: "Front door"}.Build(),
		}.Build())
		return err
	}, func() error { return f.starve(src.GetId()) })
	require.Equal(t, api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY, published.GetJob().GetStage())

	// Both are on the row: the publication's, and the heartbeat's after it.
	got, err := f.sink.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "Front door", got.GetName())
	require.Equal(t, "door-cam", got.GetLabels()[cameraBindingLabel])
	require.Equal(t, int64(1), got.GetStarvation().GetEpisodes())
}

func testNegotiateHoldsTheSources(t *testing.T, f *capFixture) {
	src := f.srcs[0]
	var agreed *api.SetNegotiateResponse
	heldOpen(t, func(fn func(context.Context)) { f.core.d.negotiateHeld = fn }, func() error {
		var err error
		agreed, err = f.core.Set().Negotiate(f.ctx, api.SetNegotiateRequest_builder{
			Ref: api.SetRef_builder{Id: f.set.GetId()}.Build(),
			Sources: []*api.SourceProposal{api.SourceProposal_builder{
				Source:      api.SourceRef_builder{Id: src.GetId()}.Build(),
				Profile:     api.SegmentProfile_builder{MaxBitrate: 2_000_000, DurationSeconds: int64(capSegment / time.Second), KeyframeIntervalMs: 2000}.Build(),
				ContentType: "video/mp4",
			}.Build()},
		}.Build())
		return err
	}, func() error { return f.starve(src.GetId()) })
	require.Greater(t, agreed.GetProfileVersion(), f.set.GetProfileVersion())

	got, err := f.sink.Source().Get(t.Context(), api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, int64(2_000_000), got.GetProfile().GetMaxBitrate())
	require.Equal(t, "video/mp4", got.GetContentType())
	require.Equal(t, int64(1), got.GetStarvation().GetEpisodes())
}

// heldBeside runs `call` until it reaches the hook `hold` installs, then
// `write` to its end beside it, and answers what the call answers once it
// goes on.
func heldBeside(t *testing.T, hold func(func(context.Context)), call func() error, write func() error) error {
	t.Helper()
	held, release := make(chan struct{}), make(chan struct{})
	hold(func(context.Context) {
		close(held)
		<-release
	})
	called := make(chan error, 1)
	go func() { called <- call() }()
	select {
	case <-held:
	case err := <-called:
		t.Fatalf("answered without being held open: %v", err)
	}
	hold(nil)

	err := write()
	close(release)
	require.NoError(t, err)

	return <-called
}

// joiner is a kind of host as a join test drives it: a poll with a key,
// an operator's adoption, and the certificate the row's join record
// keeps.
type joiner struct {
	kind  string
	join  func(key *pki.HostKey) (*api.JoinAnswer, error)
	adopt func(id []byte) error
	kept  func(id []byte) []byte
}

// joiners are the six kinds of host that join, over the fixture, which is
// given a CA to issue from.
func joiners(t *testing.T, f *capFixture) []joiner {
	ca, err := pki.NewCA("shale", f.now)
	require.NoError(t, err)
	f.core.d.CA = ca
	ctx := context.Background()
	host := func(kind string, key *pki.HostKey) *api.HostJoin {
		csr, err := key.CSR(kind)
		require.NoError(t, err)
		return api.HostJoin_builder{HardwareId: "held-" + kind, Hostname: kind, Csr: csr, KeyFingerprint: key.Fingerprint()}.Build()
	}
	return []joiner{{
		kind: "producer",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.Producer().Join(ctx, api.ProducerJoinRequest_builder{Tenant: "acme", Host: host("producer", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.Producer().Adopt(f.ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: id}.Build(), Set: api.SetRef_builder{Id: f.set.GetId()}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			return v.GetJoin().GetCertificate()
		},
	}, {
		kind: "reader",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.Reader().Join(ctx, api.ReaderJoinRequest_builder{Tenant: "acme", Host: host("reader", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.Reader().Adopt(f.ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: id}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.Reader().Get(ctx, api.ReaderGetRequest_builder{Ref: api.ReaderRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			return v.GetJoin().GetCertificate()
		},
	}, {
		kind: "node",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.Node().Join(ctx, api.NodeJoinRequest_builder{Host: host("node", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.Node().Adopt(f.ctx, api.NodeAdoptRequest_builder{Ref: api.NodeRef_builder{Id: id}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.Node().Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			return v.GetJoin().GetCertificate()
		},
	}, {
		kind: "relay",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.Relay().Join(ctx, api.RelayJoinRequest_builder{Host: host("relay", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.Relay().Adopt(f.ctx, api.RelayAdoptRequest_builder{Ref: api.RelayRef_builder{Id: id}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.Relay().Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			return v.GetJoin().GetCertificate()
		},
	}, {
		kind: "satellite",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.Satellite().Join(ctx, api.SatelliteJoinRequest_builder{Host: host("satellite", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.Satellite().Adopt(f.ctx, api.SatelliteAdoptRequest_builder{Ref: api.SatelliteRef_builder{Id: id}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.Satellite().Get(ctx, api.SatelliteGetRequest_builder{Ref: api.SatelliteRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			return v.GetJoin().GetCertificate()
		},
	}, {
		kind: "camera-worker",
		join: func(key *pki.HostKey) (*api.JoinAnswer, error) {
			v, err := f.core.CameraWorker().Join(ctx, api.CameraWorkerJoinRequest_builder{Tenant: "acme", Host: host("camera-worker", key)}.Build())
			return v.GetAnswer(), err
		},
		adopt: func(id []byte) error {
			_, err := f.core.CameraWorker().Adopt(f.ctx, api.CameraWorkerAdoptRequest_builder{Ref: api.CameraWorkerRef_builder{Id: id}.Build()}.Build())
			return err
		},
		kept: func(id []byte) []byte {
			v, err := f.sink.CameraWorker().Get(ctx, api.CameraWorkerGetRequest_builder{Ref: api.CameraWorkerRef_builder{Id: id}.Build()}.Build())
			require.NoError(t, err)
			require.Equal(t, v.GetCertificate(), v.GetJoin().GetCertificate())
			return v.GetJoin().GetCertificate()
		},
	}}
}

// A host's join poll that read its row before an operator adopted it and
// writes after does not write over the adoption: its write tests the
// version it read, is refused, and the poll reads the row again and
// answers the adoption's certificate in the same call. The same when the
// host is adopted already and polls with a new key, and is adopted again
// meanwhile. Before, the poll's record replaced the adoption's, the
// certificate with it, and the host waited for an adoption the console
// said it had (#340). The poll's read is outside any transaction, so this
// happens on SQLite as on PostgreSQL.
func TestJoinKeepsTheAdoption(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testJoinKeepsTheAdoption(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testJoinKeepsTheAdoption(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testJoinKeepsTheAdoption(t *testing.T, f *capFixture) {
	hold := func(fn func(context.Context)) { f.core.d.joinHeld = fn }
	for _, h := range joiners(t, f) {
		t.Run(h.kind, func(t *testing.T) {
			// Adopted while a poll of the pending host is held after its read.
			first, err := pki.NewHostKey()
			require.NoError(t, err)
			pending, err := h.join(first)
			require.NoError(t, err)
			require.Equal(t, api.HostState_HOST_STATE_PENDING, pending.GetState())
			id := pending.GetId()
			var held *api.JoinAnswer
			require.NoError(t, heldBeside(t, hold, func() error {
				var err error
				held, err = h.join(first)
				return err
			}, func() error { return h.adopt(id) }))
			cert := h.kept(id)
			require.NotEmpty(t, cert, "the row keeps the adoption's certificate")
			require.Equal(t, api.HostState_HOST_STATE_ADOPTED, held.GetState(), "the poll that was held answers the adoption")
			require.Equal(t, cert, held.GetCertificate())
			again, err := h.join(first)
			require.NoError(t, err)
			require.Equal(t, api.HostState_HOST_STATE_ADOPTED, again.GetState())
			require.Equal(t, cert, again.GetCertificate())

			// A new key, and adopted again while its poll is held.
			second, err := pki.NewHostKey()
			require.NoError(t, err)
			rejoin, err := h.join(second)
			require.NoError(t, err)
			require.Equal(t, api.HostState_HOST_STATE_PENDING, rejoin.GetState())
			require.True(t, rejoin.GetRejoin())
			require.NoError(t, heldBeside(t, hold, func() error {
				var err error
				held, err = h.join(second)
				return err
			}, func() error { return h.adopt(id) }))
			renewed := h.kept(id)
			require.NotEmpty(t, renewed, "the row keeps the re-adoption's certificate")
			require.NotEqual(t, cert, renewed)
			require.Equal(t, api.HostState_HOST_STATE_ADOPTED, held.GetState())
			require.Equal(t, renewed, held.GetCertificate())
			again, err = h.join(second)
			require.NoError(t, err)
			require.Equal(t, api.HostState_HOST_STATE_ADOPTED, again.GetState())
			require.Equal(t, renewed, again.GetCertificate())
		})
	}
}

// A poll whose row moves under it twice in one call answers what it
// would have answered had its write landed, waiting, and no error: the
// host asks again in ten seconds. The CP logs it once.
func TestJoinMovedTwiceWaits(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testJoinMovedTwiceWaits(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testJoinMovedTwiceWaits(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testJoinMovedTwiceWaits(t *testing.T, f *capFixture) {
	h := joiners(t, f)[0]
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	pending, err := h.join(key)
	require.NoError(t, err)

	moves := 0
	f.core.d.joinHeld = func(ctx context.Context) {
		moves++
		_, err := f.sink.Producer().Patch(ctx, api.ProducerPatchRequest_builder{
			Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Labels: map[string]string{"moved": strconv.Itoa(moves)}, DateUpdatedForce: z.Ptr(true),
		}.Build())
		require.NoError(t, err)
	}
	got, err := h.join(key)
	f.core.d.joinHeld = nil
	require.NoError(t, err)
	require.Equal(t, 2, moves, "read again once")
	require.Equal(t, api.HostState_HOST_STATE_PENDING, got.GetState())
	require.Equal(t, pending.GetId(), got.GetId())
	require.Equal(t, "waiting for adoption", got.GetMessage())
	require.Empty(t, got.GetCertificate())
	require.Equal(t, 1, strings.Count(f.logs.String(), "moved twice while it polled"))
}
