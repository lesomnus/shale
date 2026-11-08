package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/edge"
	"github.com/lesomnus/shale/internal/pki"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

const edgeRuntimeProcessOptIn = "SHALE_EDGE_RUNTIME_PROCESS"

var edgeRuntimeProtocolOutput *os.File

// Run before testing prints even a -test.v prelude. Only the explicit sidecar
// opt-in changes process descriptors; ordinary E2Es retain their normal output.
func init() {
	if os.Getenv(edgeRuntimeProcessOptIn) != "1" {
		return
	}
	failed := func() {
		_, _ = os.Stderr.WriteString("prepare edge runtime protocol output failed\n")
		os.Exit(2)
	}
	fd, err := unix.Dup(int(os.Stdout.Fd()))
	if err != nil {
		failed()
	}
	unix.CloseOnExec(fd)
	if unix.SetNonblock(fd, true) != nil {
		unix.Close(fd)
		failed()
	}
	edgeRuntimeProtocolOutput = os.NewFile(uintptr(fd), "edge-runtime-public-json")
	if unix.Dup2(int(os.Stderr.Fd()), int(os.Stdout.Fd())) != nil {
		failed()
	}
}

// TestEdgeRuntimeProcess is an opt-in production-runtime sidecar, not an
// assembled CP/network E2E. Run a compiled test binary with
// SHALE_EDGE_RUNTIME_PROCESS=1 -test.run '^TestEdgeRuntimeProcess$'. The topology
// coordinator owns exec_prefix, UID/capabilities, DHCP and network configuration.
//
// Private stdin accepts JSONL {version:1,id:positive,op:start|status|stop}.
// Start additionally requires config:{state,cp,ca_hash,tenant,dev,edge,producer,
// camera_worker}, using the ordinary YAML setting names and duration strings.
// The limited DTO excludes human authentication, camera policy and credentials.
// cli.EdgeConfig selects only the CP-managed default providers and transports.
// Original stdout carries JSONL only; testing/runtime diagnostics use stderr.
//
// "started" means the runtime loop was launched, including pre-adoption. Status
// exposes selected roles' STORED identity/key facts, never connection or media
// readiness. It reads existing files without creating keys. The coordinator
// must check CP and independently decoded media for readiness. EOF, stop and
// SIGTERM/SIGINT cancel and join both the runtime and its owned input reader.
func TestEdgeRuntimeProcess(t *testing.T) {
	if os.Getenv(edgeRuntimeProcessOptIn) != "1" {
		t.Skip("opt-in isolated managed edge subprocess")
	}
	public := edgeRuntimeProtocolOutput
	defer public.Close()
	// Inherited os.Stdin can be a blocking descriptor outside Go's poller.
	// A fresh nonblocking os.File makes Close interrupt an outstanding pipe
	// read, so stop/SIGTERM can join the scanner without waiting for parent EOF.
	inputFD, err := unix.Dup(int(os.Stdin.Fd()))
	if err != nil {
		t.Fatal("prepare runtime protocol input")
	}
	unix.CloseOnExec(inputFD)
	if unix.SetNonblock(inputFD, true) != nil {
		unix.Close(inputFD)
		t.Fatal("prepare runtime protocol input")
	}
	input := os.NewFile(uintptr(inputFD), "edge-runtime-private-input")
	defer input.Close()
	defer os.Stdin.Close()
	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	if runEdgeRuntimeProcess(ctx, input, public) != nil {
		t.Error("edge runtime process stopped after a protocol or runtime failure")
	}
}

type edgeRuntimeSettings struct {
	State        string                 `yaml:"state"`
	CP           string                 `yaml:"cp"`
	CAHash       string                 `yaml:"ca_hash"`
	Tenant       string                 `yaml:"tenant"`
	Dev          bool                   `yaml:"dev"`
	Edge         cmd.EdgeConfig         `yaml:"edge"`
	Producer     cmd.ProducerConfig     `yaml:"producer"`
	CameraWorker cmd.CameraWorkerConfig `yaml:"camera_worker"`
}

type edgeRuntimeRequest struct {
	Version int             `json:"version"`
	ID      int64           `json:"id"`
	Op      string          `json:"op"`
	Config  json.RawMessage `json:"config,omitempty"`
}

type edgeRuntimeStoredRole struct {
	IdentityState string `json:"stored_identity"`
	SPKISHA256    string `json:"spki_sha256,omitempty"`
	ID            string `json:"id,omitempty"`
}

type edgeRuntimeStatus struct {
	Runtime    string                           `json:"runtime"`
	Roles      map[string]edgeRuntimeStoredRole `json:"roles"`
	PID        int                              `json:"pid"`
	EUID       int                              `json:"euid"`
	EGID       int                              `json:"egid"`
	CapInh     string                           `json:"cap_inh"`
	CapPrm     string                           `json:"cap_prm"`
	CapEff     string                           `json:"cap_eff"`
	CapBnd     string                           `json:"cap_bnd"`
	CapAmb     string                           `json:"cap_amb"`
	NoNewPrivs int                              `json:"no_new_privs"`
}

type edgeRuntimeResponse struct {
	Version int                `json:"version"`
	ID      int64              `json:"id,omitempty"`
	Event   string             `json:"event"`
	Code    string             `json:"code,omitempty"`
	Status  *edgeRuntimeStatus `json:"status,omitempty"`
}

// encoding/json accepts duplicate object keys. Walk every nested object before
// decoding settings so a later spelling cannot replace an earlier declaration.
func edgeRuntimeUniqueJSON(raw []byte) bool {
	if len(raw) >= 64<<10 || !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	var walk func(int) bool
	nodes := 0
	walk = func(depth int) bool {
		nodes++
		if depth > 32 || nodes > 8192 {
			return false
		}
		v, err := d.Token()
		if err != nil {
			return false
		}
		if delimiter, ok := v.(json.Delim); ok {
			switch delimiter {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					k, err := d.Token()
					key, ok := k.(string)
					if err != nil || !ok || seen[key] || !walk(depth+1) {
						return false
					}
					seen[key] = true
				}
				end, err := d.Token()
				return err == nil && end == json.Delim('}')
			case '[':
				for d.More() {
					if !walk(depth + 1) {
						return false
					}
				}
				end, err := d.Token()
				return err == nil && end == json.Delim(']')
			default:
				return false
			}
		}
		return true
	}
	return walk(0) && d.Decode(new(any)) == io.EOF
}

func edgeRuntimeRequestFrom(raw []byte) (edgeRuntimeRequest, bool) {
	var r edgeRuntimeRequest
	if !edgeRuntimeUniqueJSON(raw) {
		return r, false
	}
	// Struct decoding is case-insensitive. Require exact protocol spellings so
	// Version/version cannot act as two declarations of the same envelope field.
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return r, false
	}
	for key := range fields {
		if key != "version" && key != "id" && key != "op" && key != "config" {
			return r, false
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || r.Version != 1 || r.ID <= 0 {
		return edgeRuntimeRequest{}, false
	}
	switch r.Op {
	case "start":
		return r, len(r.Config) > 0 && !bytes.Equal(r.Config, []byte("null"))
	case "status", "stop":
		return r, len(r.Config) == 0
	default:
		return edgeRuntimeRequest{}, false
	}
}

func edgeRuntimeText(s string, limit int) bool {
	return len(s) <= limit && !strings.ContainsFunc(s, unicode.IsControl)
}

func edgeRuntimeConfig(raw []byte) (*cmd.Config, edge.Config, error) {
	var q edgeRuntimeSettings
	d := yaml.NewDecoder(bytes.NewReader(raw)) // JSON uses the ordinary YAML tags.
	d.KnownFields(true)
	if d.Decode(&q) != nil || d.Decode(new(any)) != io.EOF || !filepath.IsAbs(q.State) || !edgeRuntimeText(q.State, 4096) || !edgeRuntimeText(q.Tenant, 128) || !edgeRuntimeText(q.CAHash, 128) {
		return nil, edge.Config{}, edge.ErrInvalidConfig
	}
	u, err := url.Parse(q.CP)
	if err != nil || len(q.CP) > 2048 || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.Path != "" && u.Path != "/" || u.Scheme != "https" && (u.Scheme != "http" || !q.Dev) {
		return nil, edge.Config{}, edge.ErrInvalidConfig
	}
	// Reject local compatibility even when Producer is disabled. The private
	// protocol carries transport/runtime tuning, not a dormant camera or secret.
	p := q.Producer
	if len(p.Sources) != 0 || p.Demo != 0 || p.Set != "" || p.Push != "" || p.PushIdle != 0 || p.Credentials.Directory != "" || p.Credentials.Provider != "" && p.Credentials.Provider != "cp" || p.Catalog.Provider != "" && p.Catalog.Provider != "cp" {
		return nil, edge.Config{}, edge.ErrInvalidConfig
	}
	c := &cmd.Config{State: q.State, Cp: q.CP, CaHash: q.CAHash, Tenant: q.Tenant, Edge: q.Edge, Producer: q.Producer, CameraWorker: q.CameraWorker}
	if q.Dev {
		c.Dev = q.State
	}
	cfg, err := cli.EdgeConfig(c)
	if err != nil {
		return nil, edge.Config{}, edge.ErrInvalidConfig
	}
	return c, cfg, nil
}

func edgeRuntimeFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		clear(data)
		return nil, errors.New("unavailable runtime identity")
	}
	return data, nil
}

func edgeRuntimeRole(dir string, domain byte, now time.Time) edgeRuntimeStoredRole {
	r := edgeRuntimeStoredRole{IdentityState: "awaiting_identity"}
	raw, err := edgeRuntimeFile(filepath.Join(dir, "host.key"), 16<<10)
	if err != nil {
		return r
	}
	defer clear(raw)
	key, err := pki.ParseKey(raw)
	if err != nil {
		r.IdentityState = "invalid_key"
		return r
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		r.IdentityState = "invalid_key"
		return r
	}
	hash := sha256.Sum256(spki)
	r.SPKISHA256 = hex.EncodeToString(hash[:])
	r.IdentityState = "awaiting_adoption"
	certificate, err := edgeRuntimeFile(filepath.Join(dir, "host.crt"), 64<<10)
	if err != nil {
		return r
	}
	chain, err := pki.ParseCerts(certificate)
	if err != nil || len(chain) == 0 {
		r.IdentityState = "invalid_certificate"
		return r
	}
	leaf := chain[0]
	id, ok := pki.IdOf(leaf)
	if !ok || byte(id.Domain()) != domain || !bytes.Equal(spki, leaf.RawSubjectPublicKeyInfo) {
		r.IdentityState = "invalid_certificate"
		return r
	}
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		r.IdentityState = "expired_certificate"
		return r
	}
	r.IdentityState, r.ID = "adopted", id.String()
	return r
}

func edgeRuntimeProcessStatus(configuration *cmd.Config, running bool) (*edgeRuntimeStatus, error) {
	r := &edgeRuntimeStatus{Runtime: "idle", Roles: map[string]edgeRuntimeStoredRole{}, PID: os.Getpid(), EUID: os.Geteuid(), EGID: os.Getegid()}
	if running {
		r.Runtime = "running"
	}
	data, err := edgeRuntimeFile("/proc/self/status", 64<<10)
	if err != nil {
		return nil, err
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			fields[key] = strings.TrimSpace(value)
		}
	}
	for _, value := range []struct {
		name string
		dest *string
	}{{"CapInh", &r.CapInh}, {"CapPrm", &r.CapPrm}, {"CapEff", &r.CapEff}, {"CapBnd", &r.CapBnd}, {"CapAmb", &r.CapAmb}} {
		bits := fields[value.name]
		if len(bits) != 16 {
			return nil, errors.New("unavailable runtime status")
		}
		if _, err := strconv.ParseUint(bits, 16, 64); err != nil {
			return nil, errors.New("unavailable runtime status")
		}
		*value.dest = bits
	}
	if fields["NoNewPrivs"] != "0" && fields["NoNewPrivs"] != "1" {
		return nil, errors.New("unavailable runtime status")
	}
	r.NoNewPrivs, _ = strconv.Atoi(fields["NoNewPrivs"])
	if configuration != nil {
		roles := configuration.Edge.Roles
		if roles == nil {
			roles = []string{"worker", "producer"}
		}
		for _, role := range roles {
			if role == "worker" {
				r.Roles[role] = edgeRuntimeRole(configuration.StateDir("camera-worker"), 31, time.Now())
			} else {
				r.Roles[role] = edgeRuntimeRole(configuration.StateDir("producer"), 22, time.Now())
			}
		}
	}
	return r, nil
}

func runEdgeRuntimeProcess(parent context.Context, input io.ReadCloser, output io.Writer) error {
	ctx, cancel := context.WithCancel(parent)
	encoder := json.NewEncoder(output)
	respond := func(id int64, event, code string, state *edgeRuntimeStatus) error {
		// A coordinator that stops reading cannot keep a signal/cancellation
		// blocked forever in a pipe write. The subprocess output is pollable.
		if f, ok := output.(*os.File); ok {
			if err := f.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return errors.New("runtime output unavailable")
			}
		}
		return encoder.Encode(edgeRuntimeResponse{Version: 1, ID: id, Event: event, Code: code, Status: state})
	}
	requests := make(chan []byte)
	readError := make(chan error, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			line := bytes.Clone(scanner.Bytes())
			select {
			case requests <- line:
			case <-ctx.Done():
				return
			}
		}
		readError <- scanner.Err()
		close(requests)
	}()
	var runtimeDone chan error
	var configuration *cmd.Config
	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			cancel()
			_ = input.Close()
			<-readerDone
			if runtimeDone != nil {
				<-runtimeDone
			}
		})
	}
	defer shutdown()
	finish := func(id int64, code string) error {
		shutdown()
		return respond(id, "stopped", code, nil)
	}
	for {
		select {
		case <-ctx.Done():
			return finish(0, "")
		case <-runtimeDone:
			runtimeDone = nil
			_ = finish(0, "runtime-failed")
			return errors.New("edge runtime unavailable")
		case line, ok := <-requests:
			if !ok {
				if <-readError != nil {
					_ = finish(0, "invalid-input")
					return errors.New("invalid runtime input")
				}
				return finish(0, "")
			}
			r, valid := edgeRuntimeRequestFrom(line)
			if !valid {
				if err := respond(0, "error", "invalid-input", nil); err != nil {
					return err
				}
				continue
			}
			if r.Op == "stop" {
				return finish(r.ID, "")
			}
			if r.Op == "start" {
				if configuration != nil {
					if err := respond(r.ID, "error", "already-started", nil); err != nil {
						return err
					}
					continue
				}
				c, cfg, err := edgeRuntimeConfig(r.Config)
				if err != nil {
					if err := respond(r.ID, "error", "invalid-config", nil); err != nil {
						return err
					}
					continue
				}
				configuration = c
				runtimeDone = make(chan error, 1)
				result := runtimeDone
				go func() { result <- edge.Run(ctx, cfg) }()
			}
			state, err := edgeRuntimeProcessStatus(configuration, configuration != nil)
			if err != nil {
				if err := respond(r.ID, "error", "status-unavailable", nil); err != nil {
					return err
				}
				continue
			}
			event := "status"
			if r.Op == "start" {
				event = "started"
			}
			if err := respond(r.ID, event, "", state); err != nil {
				return err
			}
		}
	}
}
