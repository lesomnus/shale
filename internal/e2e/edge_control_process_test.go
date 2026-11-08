//go:build linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const edgeControlProcessOptIn = "SHALE_EDGE_CONTROL_PROCESS"

var edgeControlProtocolOutput *os.File

// Separate protocol output before testing can print a verbose test prelude.
// The opt-in is confined to the dedicated subprocess, never ordinary tests.
func init() {
	if os.Getenv(edgeControlProcessOptIn) != "1" {
		return
	}
	failed := func() { _, _ = os.Stderr.WriteString("prepare control protocol output failed\n"); os.Exit(2) }
	var err error
	edgeControlProtocolOutput, err = edgeControlOwnedFile(os.Stdout, "control-public-json")
	if err != nil {
		failed()
	}
	if unix.Dup2(int(os.Stderr.Fd()), int(os.Stdout.Fd())) != nil {
		failed()
	}
}

// TestEdgeControlProcess is an opt-in development cluster sidecar. Launch its
// compiled test binary with SHALE_EDGE_CONTROL_PROCESS=1 and -test.run
// '^TestEdgeControlProcess$', as UID/GID 65534 with all capability sets empty
// and no_new_privs. Private stdin owns a bounded version=1 JSONL protocol:
// start (state_dir, bind_ip), status, stop_cp, restart_cp and stop. Start requires
// an existing empty private writable directory and an explicitly assigned IPv4
// address, e.g. 10.23.0.1 in the coordinator's isolated CP namespace. All eight
// service listeners and advertised addresses use that IP; no wildcard address
// or loopback fallback is advertised. stop_cp stops CP, Node and Relay together;
// restart_cp retains their state, keys and every public port.
//
// This deliberately uses start's synthetic SQLite/keys and development plain
// actor transport, including @acme/admin for the coordinator. It is separate
// from the real TLS authority fixtures, which remain required coverage. External
// SHALE_E2E_DB_DSN is forbidden. No credentials, tokens, input/configuration or
// arbitrary RPC/SQL controls are exposed. The duplicated original stdout is
// JSONL only; testing diagnostics/PASS/FAIL go to stderr. EOF, stop and signals
// close/join the owned input reader and stop/join all cluster services.
func TestEdgeControlProcess(t *testing.T) {
	if os.Getenv(edgeControlProcessOptIn) != "1" {
		t.Skip("opt-in isolated control subprocess")
	}
	public := edgeControlProtocolOutput
	defer public.Close()
	if !edgeControlPrivileges().unprivileged() {
		_ = public.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_ = json.NewEncoder(public).Encode(edgeControlResponse{Version: 1, Event: "error", Code: "invalid-process-identity"})
		t.Fatal("control process requires its unprivileged test identity")
	}
	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	input, err := edgeControlOwnedFile(os.Stdin, "control-private-input")
	if err != nil {
		t.Fatal("prepare control protocol input")
	}
	defer input.Close()
	defer os.Stdin.Close()
	if err := runEdgeControlProcess(t, ctx, input, public); err != nil {
		t.Error("control process stopped after a protocol or fixture failure")
	}
}

// Inherited stdin can be a blocking file unknown to Go's poller. Set the dup
// nonblocking before NewFile, so closing our owned descriptor cancels its Read.
func edgeControlOwnedFile(source *os.File, name string) (*os.File, error) {
	fd, err := unix.Dup(int(source.Fd()))
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

type edgeControlRequest struct {
	Version  int    `json:"version"`
	ID       int64  `json:"id"`
	Op       string `json:"op"`
	StateDir string `json:"state_dir,omitempty"`
	BindIP   string `json:"bind_ip,omitempty"`
}

type edgeControlResponse struct {
	Version int                `json:"version"`
	ID      int64              `json:"id,omitempty"`
	Event   string             `json:"event"`
	Code    string             `json:"code,omitempty"`
	Status  *edgeControlStatus `json:"status,omitempty"`
}

type edgeControlIdentity struct {
	PID         int    `json:"pid"`
	UID         int    `json:"uid"`
	GID         int    `json:"gid"`
	Effective   string `json:"cap_effective"`
	Permitted   string `json:"cap_permitted"`
	Inheritable string `json:"cap_inheritable"`
	Bounding    string `json:"cap_bounding"`
	Ambient     string `json:"cap_ambient"`
	NoNewPrivs  bool   `json:"no_new_privs"`
}

func edgeControlPrivileges() edgeControlIdentity {
	p := edgeControlIdentity{PID: os.Getpid(), UID: os.Geteuid(), GID: os.Getegid()}
	body, err := os.ReadFile("/proc/self/status")
	if err != nil || len(body) > 64<<10 {
		return p
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "CapEff:":
			p.Effective = fields[1]
		case "CapPrm:":
			p.Permitted = fields[1]
		case "CapInh:":
			p.Inheritable = fields[1]
		case "CapBnd:":
			p.Bounding = fields[1]
		case "CapAmb:":
			p.Ambient = fields[1]
		case "NoNewPrivs:":
			p.NoNewPrivs = fields[1] == "1"
		}
	}
	return p
}

func (p edgeControlIdentity) unprivileged() bool {
	if p.UID != 65534 || p.GID != 65534 || !p.NoNewPrivs {
		return false
	}
	for _, value := range []string{p.Effective, p.Permitted, p.Inheritable, p.Bounding, p.Ambient} {
		capabilities, err := strconv.ParseUint(value, 16, 64)
		if err != nil || capabilities != 0 {
			return false
		}
	}
	return true
}

type edgeControlEndpoints struct {
	TenantAPI      string `json:"tenant_api"`
	ClusterAPI     string `json:"cluster_api"`
	TenantHTTP     string `json:"tenant_http"`
	ClusterHTTP    string `json:"cluster_http"`
	StorageData    string `json:"storage_data"`
	StorageControl string `json:"storage_control"`
	RelayIngest    string `json:"relay_ingest"`
	RelayWHEP      string `json:"relay_whep"`
}

type edgeControlStatus struct {
	Process   edgeControlIdentity  `json:"process"`
	Running   bool                 `json:"running"`
	Endpoints edgeControlEndpoints `json:"endpoints"`
}

func edgeControlRequestFrom(line []byte) (edgeControlRequest, bool) {
	var request edgeControlRequest
	if len(line) >= 64<<10 {
		return request, false
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || request.Version != 1 || request.ID < 1 {
		return edgeControlRequest{}, false
	}
	allowed := map[string]bool{"version": true, "id": true, "op": true}
	switch request.Op {
	case "start":
		allowed["state_dir"], allowed["bind_ip"] = true, true
	case "status", "stop_cp", "restart_cp", "stop":
	default:
		return edgeControlRequest{}, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(line, &fields) != nil {
		return edgeControlRequest{}, false
	}
	for name := range fields {
		if !allowed[name] {
			return edgeControlRequest{}, false
		}
	}
	return request, true
}

func edgeControlStartValid(r edgeControlRequest) bool {
	ip, err := netip.ParseAddr(r.BindIP)
	if err != nil || !ip.Is4() || ip.IsUnspecified() || ip.IsMulticast() || ip.String() != r.BindIP || r.Op != "start" || len(r.StateDir) > 1024 || !filepath.IsAbs(r.StateDir) || filepath.Clean(r.StateDir) != r.StateDir || strings.ContainsFunc(r.StateDir, unicode.IsControl) {
		return false
	}
	info, err := os.Lstat(r.StateDir)
	if err != nil || !info.IsDir() || info.Mode()&0077 != 0 || info.Mode()&0200 == 0 {
		return false
	}
	resolved, err := filepath.EvalSymlinks(r.StateDir)
	if err != nil || resolved != r.StateDir {
		return false
	}
	entries, err := os.ReadDir(r.StateDir)
	if err != nil || len(entries) != 0 {
		return false
	}
	probe, err := os.CreateTemp(r.StateDir, ".control-write-")
	if err != nil {
		return false
	}
	name := probe.Name()
	defer os.Remove(name)
	return probe.Close() == nil
}

func TestEdgeControlProcessInputBoundary(t *testing.T) {
	t.Setenv("SHALE_E2E_DB_DSN", "")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal("private fixture directory unavailable")
	}
	base := edgeControlRequest{Version: 1, ID: 1, Op: "start", StateDir: dir, BindIP: "127.0.0.1"}
	if !edgeControlStartValid(base) {
		t.Fatal("empty private fixture directory rejected")
	}
	existing := filepath.Join(dir, "existing-state")
	if os.WriteFile(existing, []byte("synthetic-state"), 0600) != nil {
		t.Fatal("existing fixture state unavailable")
	}
	if edgeControlStartValid(base) {
		t.Fatal("existing state could be mistaken for a fresh fixture")
	}
	if os.Remove(existing) != nil {
		t.Fatal("existing fixture state cleanup failed")
	}
	if os.Chmod(dir, 0755) != nil {
		t.Fatal("fixture permission change unavailable")
	}
	if edgeControlStartValid(base) {
		t.Fatal("publicly traversable state accepted")
	}
	if os.Chmod(dir, 0700) != nil {
		t.Fatal("private fixture directory unavailable")
	}
	for _, change := range []func(*edgeControlRequest){
		func(r *edgeControlRequest) { r.BindIP = "0.0.0.0" },
		func(r *edgeControlRequest) { r.BindIP = "localhost" },
		func(r *edgeControlRequest) { r.BindIP = "::1" },
		func(r *edgeControlRequest) { r.BindIP = "224.0.0.1" },
		func(r *edgeControlRequest) { r.StateDir = "relative/private" },
		func(r *edgeControlRequest) { r.StateDir = dir + "/../private" },
	} {
		r := base
		change(&r)
		if edgeControlStartValid(r) {
			t.Fatal("invalid isolated start accepted")
		}
	}
	const secret = "private-control-input-sentinel"
	input := `{"version":1,"id":1,"op":"status","password":"` + secret + `"}` + "\n" +
		`{"version":1,"id":2,"op":"` + secret + `"}` + "\n" +
		`{"version":1,"id":3,"op":"start","state_dir":"` + secret + `","bind_ip":"0.0.0.0"}` + "\n" +
		`{"version":1,"id":4,"op":"status","state_dir":"` + secret + `"}` + "\n"
	var output bytes.Buffer
	if err := runEdgeControlProcess(t, t.Context(), strings.NewReader(input), &output); err != nil {
		t.Fatal("invalid input did not close cleanly")
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), "password") {
		t.Fatal("private input appeared in status")
	}
	scanner := bufio.NewScanner(&output)
	count := 0
	for scanner.Scan() {
		var response edgeControlResponse
		if json.Unmarshal(scanner.Bytes(), &response) != nil || response.Version != 1 || response.Event != "error" || response.Status != nil {
			t.Fatal("invalid error envelope")
		}
		count++
	}
	if count != 4 {
		t.Fatal("unexpected response count")
	}
	output.Reset()
	if err := runEdgeControlProcess(t, t.Context(), strings.NewReader(strings.Repeat("x", 64<<10)), &output); err == nil {
		t.Fatal("oversized private input accepted")
	}
	if output.String() != "{\"version\":1,\"event\":\"error\",\"code\":\"invalid-input\"}\n" {
		t.Fatal("oversized input was echoed")
	}
	t.Setenv("SHALE_E2E_DB_DSN", secret)
	output.Reset()
	if err := runEdgeControlProcess(t, t.Context(), strings.NewReader(""), &output); err == nil {
		t.Fatal("external database environment accepted")
	}
	if strings.Contains(output.String(), secret) {
		t.Fatal("external database was echoed")
	}
}

func TestEdgeControlProcessOwnedReaderCancellation(t *testing.T) {
	t.Setenv("SHALE_E2E_DB_DSN", "")
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal("private pipe unavailable")
	}
	defer reader.Close()
	defer writer.Close()
	owned, err := edgeControlOwnedFile(reader, "test-control-input")
	if err != nil {
		t.Fatal("owned pipe unavailable")
	}
	defer owned.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- runEdgeControlProcess(t, ctx, owned, &output) }()
	if _, err := io.WriteString(writer, "{\"version\":1,\"id\":1,\"op\":\"status\"}\n"); err != nil {
		t.Fatal("private request unavailable")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("owned input cancellation failed")
		}
	case <-time.After(time.Second):
		t.Fatal("owned input reader outlived cancellation")
	}
}

func TestEdgeControlProcessPrivilegeAndEndpointBoundary(t *testing.T) {
	p := edgeControlIdentity{UID: 65534, GID: 65534, Effective: "0", Permitted: "0", Inheritable: "0", Bounding: "0", Ambient: "0", NoNewPrivs: true}
	if !p.unprivileged() {
		t.Fatal("unprivileged identity rejected")
	}
	for _, change := range []func(*edgeControlIdentity){func(p *edgeControlIdentity) { p.UID = 0 }, func(p *edgeControlIdentity) { p.GID = 0 }, func(p *edgeControlIdentity) { p.Effective = "1" }, func(p *edgeControlIdentity) { p.Bounding = "1" }, func(p *edgeControlIdentity) { p.NoNewPrivs = false }} {
		bad := p
		change(&bad)
		if bad.unprivileged() {
			t.Fatal("privileged process accepted")
		}
	}
	e := edgeControlEndpoints{TenantAPI: "10.23.0.1:4001", ClusterAPI: "10.23.0.1:4002", TenantHTTP: "10.23.0.1:4003", ClusterHTTP: "10.23.0.1:4004", StorageData: "10.23.0.1:4005", StorageControl: "10.23.0.1:4006", RelayIngest: "10.23.0.1:4007", RelayWHEP: "10.23.0.1:4008"}
	if !e.onIP("10.23.0.1") {
		t.Fatal("isolated public endpoints rejected")
	}
	for _, address := range []string{"0.0.0.0:4005", "127.0.0.1:4005", "10.23.0.1:0", "private:4005"} {
		bad := e
		bad.StorageData = address
		if bad.onIP("10.23.0.1") {
			t.Fatal("unreachable or wildcard endpoint accepted")
		}
	}
}

type edgeControlCluster struct {
	cluster   *cluster
	bindIP    string
	startedAt time.Time
	endpoints edgeControlEndpoints
}

func startEdgeControlCluster(t *testing.T, ctx context.Context, r edgeControlRequest) (*edgeControlCluster, error) {
	address := net.JoinHostPort(r.BindIP, "0")
	startedAt := time.Now()
	c := start(t, func(c *cmd.Config) {
		c.State, c.Dev = r.StateDir, r.StateDir
		c.Db.Driver = "sqlite3"
		c.Db.Dsn = "file:" + filepath.Join(r.StateDir, "control", "shale.db") + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme"}
		// Use the native authorized download path for later independent MP4
		// qualification. The private protocol never accepts exporter settings.
		c.Export.Enabled = true
		c.Server.Addr, c.Cluster.Addr = address, address
		c.Server.Http.Addr, c.Cluster.Http.Addr = address, address
		// Storage.Advertise is a complete address, unlike Relay's host
		// override. An empty override reports this explicitly bound listener.
		c.Storage.Addr, c.Storage.ControlAddr, c.Storage.Advertise = address, address, ""
		c.Storage.Sinks = []cmd.SinkConfig{{Path: filepath.Join(r.StateDir, "sink"), Capacity: "4GiB"}}
		c.Relay.IngestAddr, c.Relay.WhepAddr = address, address
		c.Relay.Advertise, c.Relay.WhepAdvertise = r.BindIP, r.BindIP
		c.Relay.HeartbeatInterval = time.Second
	})
	s := &edgeControlCluster{cluster: c, bindIP: r.BindIP, startedAt: startedAt}
	if err := s.waitReady(ctx); err != nil {
		c.stopCP()
		return nil, err
	}
	return s, nil
}

func TestEdgeControlProcessExportAvailable(t *testing.T) {
	t.Setenv("SHALE_E2E_DB_DSN", "")
	dir := t.TempDir()
	if os.Chmod(dir, 0700) != nil {
		t.Fatal("private fixture directory unavailable")
	}
	fixture, err := startEdgeControlCluster(t, t.Context(), edgeControlRequest{Version: 1, ID: 1, Op: "start", StateDir: dir, BindIP: "127.0.0.1"})
	if err != nil {
		t.Fatal("control fixture unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = api.NewLaminaServiceClient(fixture.cluster.dial("@acme/admin")).Export(ctx, &api.LaminaExportRequest{})
	// The handler checks the enabled export endpoint before validating source
	// input. This requires real RPC admission, but no media, token or download.
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("native export was not available for request validation")
	}
}

func (s *edgeControlCluster) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	options := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, auth.Inject(auth.PlainProvider("@acme/admin"))...)
	conn, err := grpc.NewClient(s.cluster.running.ClusterAddr, options...)
	if err != nil {
		return errors.New("control status unavailable")
	}
	defer conn.Close()
	for ctx.Err() == nil {
		call, done := context.WithTimeout(ctx, time.Second)
		node, nodeErr := api.NewNodeServiceClient(conn).Get(call, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: s.cluster.running.Node.Id().Bytes()}.Build()}.Build())
		relays, relayErr := api.NewRelayServiceClient(conn).List(call, api.RelayListRequest_builder{}.Build())
		done()
		if nodeErr == nil && relayErr == nil && len(relays.GetItems()) == 1 {
			relay := relays.GetItems()[0]
			endpoints := edgeControlEndpoints{TenantAPI: s.cluster.running.TenantAddr, ClusterAPI: s.cluster.running.ClusterAddr,
				TenantHTTP: s.cluster.running.CP.HttpAddr(cmd.SurfaceTenant), ClusterHTTP: s.cluster.running.CP.HttpAddr(cmd.SurfaceCluster),
				StorageData: node.GetDataAddress(), StorageControl: node.GetControlAddress(), RelayIngest: relay.GetIngestAddress(), RelayWHEP: relay.GetWhepAddress()}
			if endpoints.onIP(s.bindIP) && relay.GetState() == api.HostState_HOST_STATE_ADOPTED && relay.GetDateSeen() != nil && !relay.GetDateSeen().AsTime().Before(s.startedAt) && node.GetDateSeen() != nil && !node.GetDateSeen().AsTime().Before(s.startedAt) {
				s.endpoints = endpoints
				// The common harness keeps only API ports by default. This fixed
				// sidecar also pins HTTP/data/control/relay ports before restart.
				c := s.cluster.cfg
				c.Server.Http.Addr, c.Cluster.Http.Addr = endpoints.TenantHTTP, endpoints.ClusterHTTP
				c.Storage.Addr, c.Storage.ControlAddr = endpoints.StorageData, endpoints.StorageControl
				c.Storage.Advertise = endpoints.StorageData
				c.Relay.IngestAddr, c.Relay.WhepAddr = endpoints.RelayIngest, endpoints.RelayWHEP
				return nil
			}
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return errors.New("control services did not become ready")
}

func (e edgeControlEndpoints) onIP(ip string) bool {
	for _, address := range []string{e.TenantAPI, e.ClusterAPI, e.TenantHTTP, e.ClusterHTTP, e.StorageData, e.StorageControl, e.RelayIngest, e.RelayWHEP} {
		host, rawPort, err := net.SplitHostPort(address)
		port, portErr := strconv.Atoi(rawPort)
		if err != nil || portErr != nil || host != ip || port < 1 || port > 65535 {
			return false
		}
	}
	return true
}

func (s *edgeControlCluster) status() *edgeControlStatus {
	return &edgeControlStatus{Process: edgeControlPrivileges(), Running: s.cluster.cancel != nil, Endpoints: s.endpoints}
}

func runEdgeControlProcess(t *testing.T, ctx context.Context, input io.Reader, output io.Writer) error {
	t.Helper()
	encoder := json.NewEncoder(output)
	respond := func(id int64, event, code string, status *edgeControlStatus) error {
		if bounded, ok := output.(interface{ SetWriteDeadline(time.Time) error }); ok {
			if err := bounded.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
		}
		return encoder.Encode(edgeControlResponse{Version: 1, ID: id, Event: event, Code: code, Status: status})
	}
	if os.Getenv("SHALE_E2E_DB_DSN") != "" {
		_ = respond(0, "error", "external-database-forbidden", nil)
		return errors.New("external database is forbidden")
	}
	ctx, cancel := context.WithCancel(ctx)
	requests, readError, readerDone := make(chan []byte), make(chan error, 1), make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case requests <- line:
			case <-ctx.Done():
				return
			}
		}
		readError <- scanner.Err()
		close(requests)
	}()
	var fixture *edgeControlCluster
	defer func() {
		cancel()
		if closer, ok := input.(io.Closer); ok {
			_ = closer.Close()
		}
		<-readerDone
		if fixture != nil {
			fixture.cluster.stopCP()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-requests:
			if ctx.Err() != nil {
				return nil
			}
			if !ok {
				if <-readError != nil {
					_ = respond(0, "error", "invalid-input", nil)
					return errors.New("invalid control input")
				}
				return nil
			}
			r, valid := edgeControlRequestFrom(line)
			if !valid {
				if err := respond(0, "error", "invalid-input", nil); err != nil {
					return err
				}
				continue
			}
			if fixture == nil {
				if !edgeControlStartValid(r) {
					if err := respond(r.ID, "error", "invalid-start", nil); err != nil {
						return err
					}
					continue
				}
				var err error
				fixture, err = startEdgeControlCluster(t, ctx, r)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					_ = respond(r.ID, "error", "start-failed", nil)
					return errors.New("control start failed")
				}
				if err = respond(r.ID, "ready", "", fixture.status()); err != nil {
					return err
				}
				continue
			}
			code, event := "", "status"
			switch r.Op {
			case "status":
			case "start":
				code = "already-started"
			case "stop_cp":
				fixture.cluster.stopCP()
			case "restart_cp":
				if fixture.cluster.cancel != nil {
					code = "already-running"
					break
				}
				prior := fixture.endpoints
				fixture.startedAt = time.Now()
				fixture.cluster.startCP()
				if fixture.waitReady(ctx) != nil || fixture.endpoints != prior {
					if ctx.Err() != nil {
						return nil
					}
					_ = respond(r.ID, "error", "restart-failed", nil)
					return errors.New("control restart failed")
				}
				event = "ready"
			case "stop":
				fixture.cluster.stopCP()
				return respond(r.ID, "stopped", "", nil)
			}
			if code != "" {
				event = "error"
			}
			if err := respond(r.ID, event, code, fixture.status()); err != nil {
				return err
			}
		}
	}
}
