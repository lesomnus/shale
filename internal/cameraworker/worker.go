package cameraworker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/hostagent"
	"github.com/lesomnus/shale/internal/pki"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Domain pdid.Domain = 31

var errReconnect = errors.New("camera worker certificate renewed")

// Config contains only machine enrollment and execution settings. No bearer or
// human session fallback is supported. The state directory must be independent
// of a colocated Producer's key and certificate.
type Config struct {
	StateDir          string
	Cp                string
	CaHash            string
	Tenant            string
	Dev               bool
	HardwareID        string
	HeartbeatInterval time.Duration
	RetryInterval     time.Duration
	LeaseSeconds      int64
	Executor          Executor
	// Discovery is opt-in. The composition root supplies its LAN transport;
	// assignments and network intent always come from the authenticated CP.
	Discoverer        camera.Discoverer
	DiscoveryInterval time.Duration
	DiscoveryTimeout  time.Duration
	Log               *slog.Logger
}

type Worker struct {
	cfg   Config
	agent *hostagent.Agent
	// Run owns one serial poll loop at a time. Preserve only its last attempted
	// job across session reconnects so a failing camera cannot starve its peers.
	lastAttempted string
}

func New(c Config) (*Worker, error) {
	if c.StateDir == "" || c.Cp == "" {
		return nil, errors.New("camera worker needs a private state directory and CP address")
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = time.Second
	}
	if c.LeaseSeconds == 0 {
		c.LeaseSeconds = 30
	}
	if c.DiscoveryInterval == 0 {
		c.DiscoveryInterval = 30 * time.Second
	}
	if c.DiscoveryTimeout == 0 {
		c.DiscoveryTimeout = 2 * time.Second
	}
	if c.HeartbeatInterval < time.Millisecond || c.RetryInterval < time.Millisecond || c.LeaseSeconds < 3 || c.LeaseSeconds > 300 {
		return nil, errors.New("invalid camera worker timing")
	}
	if c.DiscoveryInterval < time.Second || c.DiscoveryInterval > time.Hour || c.DiscoveryTimeout < time.Millisecond || c.DiscoveryTimeout > 30*time.Second {
		return nil, errors.New("invalid camera worker discovery timing")
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Executor == nil {
		c.Executor = ExecutorFunc(pendingExecutor)
	}
	a := &hostagent.Agent{Kind: Domain, HardwareId: c.HardwareID, Store: pki.Store{Dir: c.StateDir}, Cp: c.Cp, CaHash: c.CaHash, Dev: c.Dev, Log: c.Log}
	if err := a.Init(); err != nil {
		return nil, err
	}
	return &Worker{cfg: c, agent: a}, nil
}
func (w *Worker) ID() pdid.Id { return w.agent.Id() }

// HTTPClient uses this Worker's current machine certificate and pinned CP CA
// for native data-plane verification. Camera-device trust is configured
// separately; callers own and close this client's idle connections.
func (w *Worker) HTTPClient() (*http.Client, error) { return w.agent.HTTPClient() }

func operationKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return "worker:" + hex.EncodeToString(b[:])
}

func (w *Worker) Run(ctx context.Context) error {
	bootstrap := false
	for ctx.Err() == nil {
		if bootstrap || !w.agent.Ready(time.Now()) {
			_, err := w.agent.JoinBootstrap(ctx, func(ctx context.Context, conn *grpc.ClientConn, host *api.HostJoin) (hostagent.Answer, error) {
				v, err := api.NewCameraWorkerServiceClient(conn).Join(ctx, api.CameraWorkerJoinRequest_builder{Host: host, Tenant: w.cfg.Tenant}.Build())
				if err != nil {
					return hostagent.Answer{}, err
				}
				return hostagent.Answer{JoinAnswer: v.GetAnswer()}, nil
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			bootstrap = false
		}
		conn, err := w.agent.Dial(ctx)
		if err == nil {
			err = w.session(ctx, conn)
			conn.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A committed renewal may have lost its response. Recover only through
		// the public approved-key Join; never substitute an anonymous or human
		// credential for an ordinary machine request.
		if status.Code(err) == codes.Unauthenticated {
			bootstrap = true
		}
		if !errors.Is(err, errReconnect) {
			w.logSessionFailure(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.cfg.RetryInterval):
		}
	}
	return ctx.Err()
}

// Device drivers may include authenticated URLs, credentials or response bodies
// in errors. Preserve the error for control flow, but never serialize its text or
// details to the Worker log, even when it is wrapped in a gRPC status.
func (w *Worker) logSessionFailure(err error) {
	code := status.Code(err)
	kind := "none"
	switch {
	case errors.Is(err, context.Canceled):
		code, kind = codes.Canceled, "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code, kind = codes.DeadlineExceeded, "deadline_exceeded"
	}
	w.cfg.Log.Warn("camera worker connection stopped; device work canceled", "code", code.String(), "context", kind)
}

func (w *Worker) session(ctx context.Context, conn *grpc.ClientConn) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return w.heartbeat(ctx, conn) })
	g.Go(func() error { return w.poll(ctx, conn) })
	if w.cfg.Discoverer != nil {
		g.Go(func() error {
			return w.discover(ctx, api.NewCameraWorkerServiceClient(conn), api.NewCameraPolicyServiceClient(conn))
		})
	}
	return g.Wait()
}
func (w *Worker) heartbeat(ctx context.Context, conn *grpc.ClientConn) error {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		call, done := context.WithTimeout(ctx, 10*time.Second)
		v, err := api.NewCameraWorkerServiceClient(conn).Heartbeat(call, api.CameraWorkerHeartbeatRequest_builder{CaHash: w.agent.BundleHash(), Version: hostagent.Version}.Build())
		done()
		if err != nil {
			return err
		}
		if v.GetRenewCertificate() || w.agent.NeedsRenewal(time.Now()) {
			err := w.agent.Renew(ctx, conn, func(ctx context.Context, conn *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
				v, err := api.NewCameraWorkerServiceClient(conn).RenewCertificate(ctx, api.CameraWorkerRenewCertificateRequest_builder{Csr: csr}.Build())
				if err != nil {
					return nil, nil, err
				}
				return v.GetCertificate(), v.GetCaBundle(), nil
			})
			if err != nil {
				return err
			}
			return errReconnect
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) execute(ctx context.Context, conn *grpc.ClientConn, work *api.CameraWorkerWork, finished chan<- error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e := &Execution{life: ctx, work: work, workers: api.NewCameraWorkerServiceClient(conn), jobs: api.NewCameraJobServiceClient(conn), presets: api.NewCameraPresetServiceClient(conn), credentials: api.NewCameraCredentialServiceClient(conn), leaseSeconds: w.cfg.LeaseSeconds, cancel: cancel}
	keepDone := make(chan error, 1)
	go func() { keepDone <- e.keep(ctx) }()
	err := w.cfg.Executor.Execute(ctx, e)
	cancel()
	keepErr := <-keepDone
	if (err == nil || errors.Is(err, context.Canceled)) && keepErr != nil && !errors.Is(keepErr, context.Canceled) {
		err = keepErr
	}
	finished <- err
}

func (w *Worker) startExecution(ctx context.Context, conn *grpc.ClientConn, work *api.CameraWorkerWork, finished chan<- error) context.CancelFunc {
	execCtx, stop := context.WithCancel(ctx)
	go w.execute(execCtx, conn, work, finished)
	return stop
}

// pollStart rotates a fresh bounded CP snapshot after the previous attempt.
// Missing jobs carry no local scheduling state; yielded remains bounded by the
// current snapshot (the CP permits at most 500 jobs), never by process lifetime.
// Neither the cursor nor a snapshot authorizes execution: Claim and CheckJob do.
func (w *Worker) pollStart(work []*api.CameraWorkerWork, yielded map[string]bool) int {
	start, found := 0, false
	present := make(map[string]bool, len(work))
	for i, v := range work {
		id := string(v.GetJob().GetId())
		present[id] = true
		if id == w.lastAttempted {
			start, found = (i+1)%len(work), true
		}
	}
	if !found {
		w.lastAttempted = ""
	}
	for id := range yielded {
		if !present[id] {
			delete(yielded, id)
		}
	}
	return start
}

// One serial execution per Worker avoids local overlap; the CP's physical-device
// fence also protects against competing Workers and restarts. Terminal or
// superseded work is canceled before another executor starts.
func (w *Worker) poll(ctx context.Context, conn *grpc.ClientConn) error {
	client := api.NewCameraWorkerServiceClient(conn)
	jobs := api.NewCameraJobServiceClient(conn)
	var revision string
	var active *api.CameraWorkerWork
	var stop context.CancelFunc
	finished := make(chan error, 1)
	// An executor which yielded WAITING is resumed on reconnect/restart. It must
	// not busy-loop against a job whose pinned inputs have not changed.
	yielded := map[string]bool{}
	defer func() {
		if stop != nil {
			stop()
			<-finished
		}
	}()
	for {
		select {
		case err := <-finished:
			if stop != nil {
				stop()
			}
			stop = nil
			if err != nil {
				return err
			}
			if active != nil {
				yielded[string(active.GetJob().GetId())] = true
			}
			active = nil
		default:
		}
		snapshot, err := client.Poll(ctx, api.CameraWorkerPollRequest_builder{AfterRevision: revision, WaitSeconds: 5}.Build())
		if err != nil {
			return err
		}
		revision = snapshot.GetRevision()
		pending := snapshot.GetJobs()
		start := w.pollStart(pending, yielded)
		if active != nil {
			valid := false
			for _, v := range snapshot.GetJobs() {
				if bytes.Equal(v.GetJob().GetId(), active.GetJob().GetId()) && v.GetAssignment().GetGeneration() == active.GetAssignment().GetGeneration() && bytes.Equal(v.GetAssignment().GetWorkerId(), w.ID().Bytes()) {
					valid = true
					break
				}
			}
			if !valid {
				stop()
				err := <-finished
				stop = nil
				active = nil
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
			}
		}
		if active != nil {
			continue
		}
		for offset := range pending {
			v := pending[(start+offset)%len(pending)]
			j := v.GetJob()
			if yielded[string(j.GetId())] {
				continue
			}
			// Never take over another live lease, even within this assigned location.
			if len(j.GetLeaseOwner()) > 0 && !bytes.Equal(j.GetLeaseOwner(), w.ID().Bytes()) && j.GetLeaseExpires() != nil && j.GetLeaseExpires().AsTime().After(time.Now()) {
				continue
			}
			// Advance before Claim/CheckJob, including when either response fails.
			// Reconnection must not make one unsettled/denied job the first again.
			w.lastAttempted = string(j.GetId())
			claimed, err := jobs.Claim(ctx, api.CameraJobClaimRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), ExpectedVersion: j.GetDateUpdated(), OperationKey: operationKey(), LeaseSeconds: w.cfg.LeaseSeconds}.Build())
			if err != nil {
				// A changed snapshot or another unsettled intent for this physical
				// device is expected contention. Keep polling other assigned work
				// instead of reconnecting and reclaiming every yielded WAITING job.
				if code := status.Code(err); code == codes.Aborted || code == codes.FailedPrecondition {
					continue
				}
				return err
			}
			v.SetJob(claimed)
			// CheckJob is the fresh grant after Claim; it also prevents carrying a
			// catalog snapshot's stale assignment into a device operation.
			v, err = client.CheckJob(ctx, api.CameraWorkerCheckJobRequest_builder{Ref: api.CameraJobRef_builder{Id: j.GetId()}.Build(), LeaseGeneration: claimed.GetLeaseGeneration(), AssignmentGeneration: claimed.GetAssignmentGeneration()}.Build())
			if err != nil {
				return err
			}
			active = v
			stop = w.startExecution(ctx, conn, v, finished)
			break
		}
	}
}
