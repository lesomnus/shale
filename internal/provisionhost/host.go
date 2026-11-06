// Package provisionhost hosts the portable provisioning engine. Jobs contain
// scoped references and normalized evidence; credential callbacks and media never
// cross this service's API. Workers own their local secret/configuration stores.
package provisionhost

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/web"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"google.golang.org/grpc"
)

type Executor interface {
	Execute(context.Context, provision.Request, bool) (provision.Run, error)
}

type Target struct {
	Scope      camera.EnrollmentScope
	Provider   string
	Executor   Executor
	Checkpoint func(context.Context, provision.Request) (provision.Run, error)
}

// Client is process configuration, never accepted in a job request.
type Client struct {
	Token    string `json:"-" yaml:"-"`
	Scopes   []camera.EnrollmentScope
	Networks []string
	Worker   bool
}

type Options struct {
	StateDir   string
	Targets    []Target
	Networks   []Network
	Clients    []Client
	Discoverer camera.Discoverer
}

type Job struct {
	ID           string                 `json:"id"`
	Kind         string                 `json:"kind"`
	Request      provision.Request      `json:"request,omitempty"`
	Discovery    *camera.DiscoveryScope `json:"discovery,omitempty"`
	Apply        bool                   `json:"apply"`
	State        string                 `json:"state"`
	Code         string                 `json:"code,omitempty"`
	Run          *provision.Run         `json:"run,omitempty"`
	Observations []camera.Observation   `json:"observations,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

type Host struct {
	options Options
	dir     string
	locks   *filestore.Store
	mu      sync.Mutex
	wake    chan struct{}
	running map[string]context.CancelFunc
}

var jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func New(options Options) (*Host, error) {
	if options.StateDir == "" || len(options.Clients) == 0 {
		return nil, camera.ErrInvalid
	}
	for _, client := range options.Clients {
		if len(client.Token) < 32 {
			return nil, camera.ErrInvalid
		}
	}
	networkIDs := map[string]bool{}
	for _, network := range options.Networks {
		if networkIDs[network.ID] {
			return nil, camera.ErrInvalid
		}
		networkIDs[network.ID] = true
		if err := network.Validate(); err != nil {
			return nil, err
		}
	}
	for _, target := range options.Targets {
		if target.Executor == nil || target.Provider == "" {
			return nil, camera.ErrInvalid
		}
		found := false
		for _, network := range options.Networks {
			if network.ID == target.Scope.NetworkID {
				found = true
			}
		}
		if !found {
			return nil, camera.ErrInvalid
		}
	}
	locks, err := filestore.Open(options.StateDir)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(options.StateDir, "jobs")
	if _, err = filestore.Open(dir); err != nil {
		return nil, err
	}
	if options.Discoverer == nil {
		options.Discoverer = discovery.Discoverer{}
	}
	return &Host{options: options, dir: dir, locks: locks, wake: make(chan struct{}, 1), running: map[string]context.CancelFunc{}}, nil
}

func (h *Host) path(id string) string { return filepath.Join(h.dir, provision.Key(id)+".json") }

func (h *Host) load(id string) (Job, error) {
	data, err := os.ReadFile(h.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return Job{}, provision.ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	var job Job
	if err = json.Unmarshal(data, &job); err != nil || job.ID != id {
		return Job{}, camera.ErrInvalid
	}
	return job, nil
}

func (h *Host) save(job *Job) error {
	job.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return filestore.WriteAtomic(h.path(job.ID), data, 0600)
}

func (h *Host) list() ([]Job, error) {
	paths, err := filepath.Glob(filepath.Join(h.dir, "*.json"))
	if err != nil {
		return nil, err
	}
	var out []Job
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var job Job
		if err = json.Unmarshal(data, &job); err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, nil
}

func (h *Host) notify() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *Host) target(q provision.Request) (Target, error) {
	if err := q.Validate(); err != nil {
		return Target{}, err
	}
	for _, target := range h.options.Targets {
		if target.Scope == q.Scope && target.Provider == q.Provider {
			network, err := h.network(q.Scope.NetworkID)
			if err != nil {
				return Target{}, err
			}
			for _, endpoint := range q.Device.Endpoints {
				if err = network.AllowURI(endpoint.URL); err != nil {
					return Target{}, err
				}
			}
			return target, nil
		}
	}
	return Target{}, camera.ErrForbidden
}

func (h *Host) network(id string) (Network, error) {
	for _, network := range h.options.Networks {
		if network.ID == id {
			return network, nil
		}
	}
	return Network{}, camera.ErrForbidden
}

func (h *Host) submit(q provision.Request, apply bool) (Job, error) {
	if !jobIDPattern.MatchString(q.ID) {
		return Job{}, camera.ErrInvalid
	}
	if _, err := h.target(q); err != nil {
		return Job{}, err
	}
	reservation, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.locks.Reserve(reservation, q); err != nil {
		return Job{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, err := h.load(q.ID); err == nil {
		if old.Kind != "enroll" || !reflect.DeepEqual(old.Request, q) || old.Apply != apply {
			return Job{}, provision.ErrConflict
		}
		return old, nil
	} else if !errors.Is(err, provision.ErrNotFound) {
		return Job{}, err
	}
	job := Job{ID: q.ID, Kind: "enroll", Request: q, Apply: apply, State: "queued", CreatedAt: time.Now().UTC()}
	if err := h.save(&job); err != nil {
		return Job{}, err
	}
	h.notify()
	return job, nil
}

// Run owns the durable queue. Restart replays interrupted jobs, while the core's
// device lock and exact references protect partially completed camera writes.
// Multiple controller processes must not share one queue concurrently.
func (h *Host) Run(ctx context.Context) error {
	unlock, err := h.locks.Lock(ctx, "host-coordinator")
	if err != nil {
		return err
	}
	defer unlock()
	h.mu.Lock()
	jobs, err := h.list()
	if err == nil {
		for n := range jobs {
			if jobs[n].State == "running" {
				jobs[n].State = "queued"
				if err = h.save(&jobs[n]); err != nil {
					break
				}
			}
		}
	}
	h.mu.Unlock()
	if err != nil {
		return err
	}
	h.notify()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.wake:
		}
		for ctx.Err() == nil {
			h.mu.Lock()
			jobs, err = h.list()
			if err != nil {
				h.mu.Unlock()
				return err
			}
			var next *Job
			for n := range jobs {
				if jobs[n].State == "queued" {
					next = &jobs[n]
					break
				}
			}
			if next == nil {
				h.mu.Unlock()
				break
			}
			next.State = "running"
			if err = h.save(next); err != nil {
				h.mu.Unlock()
				return err
			}
			runctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			h.running[next.ID] = cancel
			h.mu.Unlock()
			result, observations, executeErr := h.execute(runctx, *next)
			cancel()
			h.mu.Lock()
			delete(h.running, next.ID)
			current, loadErr := h.load(next.ID)
			if loadErr != nil {
				h.mu.Unlock()
				return loadErr
			}
			if current.State != "canceled" {
				if result.Request.ID != "" {
					current.Run = &result
				}
				current.Observations = observations
				current.State = "done"
				current.Code = ""
				if executeErr != nil {
					current.Code = errorCode(executeErr)
					current.State = "waiting"
					if ctx.Err() != nil {
						current.State = "queued"
					}
					if current.Code == "forbidden" || current.Code == "invalid" || current.Code == "conflict" {
						current.State = "failed"
					}
				}
			}
			err = h.save(&current)
			h.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

func (h *Host) execute(ctx context.Context, job Job) (provision.Run, []camera.Observation, error) {
	if job.Kind == "discover" && job.Discovery != nil {
		observations, err := h.discover(ctx, *job.Discovery)
		return provision.Run{}, observations, err
	}
	target, err := h.target(job.Request)
	if err != nil {
		return provision.Run{}, nil, err
	}
	if err = h.locks.Reserve(ctx, job.Request); err != nil {
		return provision.Run{}, nil, err
	}
	unlock, err := h.locks.Lock(ctx, provision.Key("controller-device", job.Request.Scope.TenantID, string(job.Request.Device.ID)))
	if err != nil {
		return provision.Run{}, nil, err
	}
	defer unlock()
	run, err := target.Executor.Execute(ctx, job.Request, job.Apply)
	return run, nil, err
}

func (h *Host) discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	network, err := h.network(scope.ID)
	if err != nil {
		return nil, err
	}
	if network.Worker.URL == "" {
		return h.options.Discoverer.Discover(ctx, scope)
	}
	client, token, err := connectionHTTP(network.Worker)
	if err != nil {
		return nil, err
	}
	if transport, ok := client.Transport.(*http.Transport); ok {
		defer transport.CloseIdleConnections()
	}
	remote := &RemoteExecutor{URL: network.Worker.URL, Token: token, HTTPClient: client, AllowLoopbackHTTP: network.Worker.AllowLoopbackHTTP}
	observations, err := remote.Discover(ctx, scope)
	if err != nil {
		return nil, err
	}
	for _, observation := range observations {
		if observation.Scope != scope.ID {
			return nil, camera.ErrForbidden
		}
		for _, endpoint := range observation.Device.Endpoints {
			if endpoint.NetworkScope != scope.ID || network.AllowURI(endpoint.URL) != nil {
				return nil, camera.ErrForbidden
			}
		}
	}
	return observations, nil
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, camera.ErrForbidden):
		return "forbidden"
	case errors.Is(err, camera.ErrAuthRequired), errors.Is(err, camera.ErrExpired):
		return "credentials_required"
	case errors.Is(err, camera.ErrNeedsUserInput), errors.Is(err, camera.ErrManualActivation), errors.Is(err, camera.ErrAmbiguousIdentity):
		return "input_required"
	case errors.Is(err, camera.ErrUnsupported):
		return "unsupported"
	case errors.Is(err, camera.ErrInvalid):
		return "invalid"
	case errors.Is(err, camera.ErrConflict), errors.Is(err, provision.ErrConflict):
		return "conflict"
	case errors.Is(err, provision.ErrNotFound):
		return "not_found"
	case errors.Is(err, provision.ErrNotReady):
		return "media_not_ready"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "interrupted"
	default:
		return "operation_failed"
	}
}

// Handler uses Payday's HTTP mux/CORS support. These deliberately explicit HTTP
// routes authenticate independently; no CP permission is bypassed by this mux.
func (h *Host) Handler() (http.Handler, error) {
	mux, err := web.New(config.HttpConfig{}, grpc.NewServer())
	if err != nil {
		return nil, err
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { respond(w, http.StatusOK, map[string]bool{"ok": true}) })
	mux.Handle("/v1/", http.HandlerFunc(h.route))
	return mux, nil
}

func (h *Host) authenticate(r *http.Request) (Client, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return Client{}, false
	}
	hash := sha256.Sum256([]byte(token))
	for _, client := range h.options.Clients {
		other := sha256.Sum256([]byte(client.Token))
		if subtle.ConstantTimeCompare(hash[:], other[:]) == 1 {
			return client, true
		}
	}
	return Client{}, false
}

func allowsScope(client Client, scope camera.EnrollmentScope) bool {
	for _, allowed := range client.Scopes {
		if allowed == scope {
			return true
		}
	}
	return false
}
func allowsNetwork(client Client, id string) bool {
	for _, allowed := range client.Networks {
		if allowed == id {
			return true
		}
	}
	for _, allowed := range client.Scopes {
		if allowed.NetworkID == id {
			return true
		}
	}
	return false
}
func allowsJob(client Client, job Job) bool {
	if job.Kind == "discover" && job.Discovery != nil {
		return allowsNetwork(client, job.Discovery.ID)
	}
	return allowsScope(client, job.Request.Scope)
}

func (h *Host) route(w http.ResponseWriter, r *http.Request) {
	client, ok := h.authenticate(r)
	if !ok {
		respond(w, http.StatusUnauthorized, map[string]string{"code": "unauthorized"})
		return
	}
	if r.URL.Path == "/v1/discover" && r.Method == http.MethodPost {
		var request struct {
			ID        string            `json:"id"`
			NetworkID string            `json:"network_id"`
			Seeds     []camera.Endpoint `json:"seeds,omitempty"`
		}
		if !decode(w, r, &request) {
			return
		}
		if !jobIDPattern.MatchString(request.ID) {
			fail(w, camera.ErrInvalid)
			return
		}
		if !allowsNetwork(client, request.NetworkID) {
			fail(w, camera.ErrForbidden)
			return
		}
		network, err := h.network(request.NetworkID)
		if err != nil {
			fail(w, err)
			return
		}
		scope := camera.DiscoveryScope{ID: network.ID, Interface: network.Interface, LocalAddress: network.LocalAddress, Timeout: 2 * time.Second, Multicast: network.Multicast, Seeds: append([]camera.Endpoint(nil), network.Seeds...)}
		for _, seed := range request.Seeds {
			if err = network.AllowURI(seed.URL); err != nil {
				fail(w, err)
				return
			}
			scope.Seeds = append(scope.Seeds, seed)
		}
		h.mu.Lock()
		old, loadErr := h.load(request.ID)
		if loadErr == nil {
			h.mu.Unlock()
			if !allowsJob(client, old) || old.Kind != "discover" || !reflect.DeepEqual(old.Discovery, &scope) {
				fail(w, provision.ErrConflict)
				return
			}
			respond(w, http.StatusAccepted, old)
			return
		}
		if !errors.Is(loadErr, provision.ErrNotFound) {
			h.mu.Unlock()
			fail(w, loadErr)
			return
		}
		job := Job{ID: request.ID, Kind: "discover", Discovery: &scope, State: "queued", CreatedAt: time.Now().UTC()}
		err = h.save(&job)
		h.mu.Unlock()
		if err != nil {
			fail(w, err)
			return
		}
		h.notify()
		respond(w, http.StatusAccepted, job)
		return
	}
	if r.URL.Path == "/v1/execute" && r.Method == http.MethodPost {
		if !client.Worker {
			fail(w, camera.ErrForbidden)
			return
		}
		var request struct {
			Request provision.Request `json:"request"`
			Apply   bool              `json:"apply"`
		}
		if !decode(w, r, &request) {
			return
		}
		if !allowsScope(client, request.Request.Scope) {
			fail(w, camera.ErrForbidden)
			return
		}
		target, err := h.target(request.Request)
		if err != nil {
			fail(w, err)
			return
		}
		if err = h.locks.Reserve(r.Context(), request.Request); err != nil {
			fail(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		run, err := target.Executor.Execute(ctx, request.Request, request.Apply)
		// Return the durable checkpoint even on a recoverable operation error.
		respond(w, http.StatusOK, ExecutionResult{Run: run, Code: codeOrEmpty(err)})
		return
	}
	if r.URL.Path == "/v1/discovery-execute" && r.Method == http.MethodPost {
		if !client.Worker {
			fail(w, camera.ErrForbidden)
			return
		}
		var request struct {
			NetworkID string            `json:"network_id"`
			Seeds     []camera.Endpoint `json:"seeds,omitempty"`
		}
		if !decode(w, r, &request) {
			return
		}
		if !allowsNetwork(client, request.NetworkID) {
			fail(w, camera.ErrForbidden)
			return
		}
		network, err := h.network(request.NetworkID)
		if err != nil || network.Worker.URL != "" {
			fail(w, camera.ErrForbidden)
			return
		}
		seeds := append([]camera.Endpoint(nil), network.Seeds...)
		for _, seed := range request.Seeds {
			if err = network.AllowURI(seed.URL); err != nil {
				fail(w, err)
				return
			}
			seeds = append(seeds, seed)
		}
		observations, err := h.options.Discoverer.Discover(r.Context(), camera.DiscoveryScope{ID: network.ID, Interface: network.Interface, LocalAddress: network.LocalAddress, Seeds: seeds, Timeout: 2 * time.Second, Multicast: network.Multicast})
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusOK, observations)
		return
	}
	if r.URL.Path == "/v1/checkpoint" && r.Method == http.MethodPost {
		if !client.Worker {
			fail(w, camera.ErrForbidden)
			return
		}
		var request struct {
			Request provision.Request `json:"request"`
		}
		if !decode(w, r, &request) {
			return
		}
		if !allowsScope(client, request.Request.Scope) {
			fail(w, camera.ErrForbidden)
			return
		}
		target, err := h.target(request.Request)
		if err != nil {
			fail(w, err)
			return
		}
		if target.Checkpoint == nil {
			fail(w, provision.ErrNotFound)
			return
		}
		run, err := target.Checkpoint(r.Context(), request.Request)
		if err != nil {
			fail(w, err)
			return
		}
		if !reflectRequest(run.Request, request.Request) {
			fail(w, camera.ErrForbidden)
			return
		}
		respond(w, http.StatusOK, run)
		return
	}
	if r.URL.Path == "/v1/runs" {
		switch r.Method {
		case http.MethodPost:
			var request struct {
				Request provision.Request `json:"request"`
				Apply   bool              `json:"apply"`
			}
			if !decode(w, r, &request) {
				return
			}
			if !allowsScope(client, request.Request.Scope) {
				fail(w, camera.ErrForbidden)
				return
			}
			job, err := h.submit(request.Request, request.Apply)
			if err != nil {
				fail(w, err)
				return
			}
			respond(w, http.StatusAccepted, job)
			return
		case http.MethodGet:
			h.mu.Lock()
			jobs, err := h.list()
			h.mu.Unlock()
			if err != nil {
				fail(w, err)
				return
			}
			visible := []Job{}
			for _, job := range jobs {
				if allowsJob(client, job) {
					visible = append(visible, job)
				}
			}
			respond(w, http.StatusOK, visible)
			return
		}
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	if path == r.URL.Path || path == "" {
		http.NotFound(w, r)
		return
	}
	id, action, _ := strings.Cut(path, "/")
	h.mu.Lock()
	job, err := h.load(id)
	if err != nil {
		h.mu.Unlock()
		fail(w, err)
		return
	}
	if !allowsJob(client, job) {
		h.mu.Unlock()
		fail(w, camera.ErrForbidden)
		return
	}
	if action == "" && r.Method == http.MethodGet {
		h.mu.Unlock()
		if job.State == "running" && job.Kind == "enroll" {
			if target, er := h.target(job.Request); er == nil && target.Checkpoint != nil {
				if run, er := target.Checkpoint(r.Context(), job.Request); er == nil && reflectRequest(run.Request, job.Request) {
					job.Run = &run
				}
			}
		}
		respond(w, http.StatusOK, job)
		return
	}
	if action == "cancel" && r.Method == http.MethodPost {
		job.State = "canceled"
		job.Code = "canceled"
		err = h.save(&job)
		if cancel := h.running[id]; cancel != nil {
			cancel()
		}
		h.mu.Unlock()
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, http.StatusOK, job)
		return
	}
	if action == "resume" && r.Method == http.MethodPost {
		var request struct {
			Apply *bool `json:"apply,omitempty"`
		}
		if !decode(w, r, &request) {
			h.mu.Unlock()
			return
		}
		if job.State == "running" || h.running[id] != nil {
			h.mu.Unlock()
			fail(w, provision.ErrConflict)
			return
		}
		if request.Apply != nil {
			job.Apply = *request.Apply
		}
		job.State = "queued"
		job.Code = ""
		err = h.save(&job)
		h.mu.Unlock()
		if err != nil {
			fail(w, err)
			return
		}
		h.notify()
		respond(w, http.StatusAccepted, job)
		return
	}
	h.mu.Unlock()
	http.NotFound(w, r)
}

func codeOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return errorCode(err)
}

func decode(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil {
		fail(w, camera.ErrInvalid)
		return false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		fail(w, camera.ErrInvalid)
		return false
	}
	return true
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, err error) {
	code := errorCode(err)
	status := http.StatusBadRequest
	switch code {
	case "forbidden":
		status = http.StatusForbidden
	case "not_found":
		status = http.StatusNotFound
	case "conflict":
		status = http.StatusConflict
	case "operation_failed":
		status = http.StatusInternalServerError
	}
	respond(w, status, map[string]string{"code": code})
}
