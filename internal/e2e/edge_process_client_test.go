package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type isolatedRole struct {
	Address         string   `json:"address"`
	Interface       string   `json:"interface"`
	CameraInterface string   `json:"camera_interface"`
	CameraAddress   string   `json:"camera_address"`
	CameraCIDR      string   `json:"camera_cidr"`
	UplinkInterface string   `json:"uplink_interface"`
	UplinkAddress   string   `json:"uplink_address"`
	ExecPrefix      []string `json:"exec_prefix"`
}

type isolatedTopology struct {
	Version        int                     `json:"version"`
	ApplicationUID int                     `json:"application_uid"`
	ApplicationGID int                     `json:"application_gid"`
	Roles          map[string]isolatedRole `json:"roles"`
	DHCP           struct {
		Lease     map[string]any `json:"lease"`
		LeaseFile string         `json:"lease_file"`
	} `json:"dhcp"`
}

func qualificationTopology(t *testing.T) isolatedTopology {
	t.Helper()
	path := os.Getenv("SHALE_TOPOLOGY_FILE")
	if path == "" {
		t.Skip("opt-in isolated camera topology")
	}
	require.Equal(t, "1", os.Getenv("SHALE_CAMERA_LAN_NETNS_TEST"))
	require.Zero(t, os.Geteuid(), "only the disposable coordinator may manipulate its namespaces")
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	var topology isolatedTopology
	require.NoError(t, json.NewDecoder(io.LimitReader(file, 64<<10)).Decode(&topology))
	require.Equal(t, 1, topology.Version)
	require.Equal(t, 65534, topology.ApplicationUID)
	require.Equal(t, 65534, topology.ApplicationGID)
	for _, role := range []string{"cp", "edge", "camera"} {
		require.NotEmpty(t, topology.Roles[role].ExecPrefix)
	}
	return topology
}

// Every byte of public sidecar output is retained within this explicit budget
// for later secret scanning. Overflow is a qualification failure, not silent
// truncation of potentially sensitive diagnostics.
type qualificationLog struct {
	mu       sync.Mutex
	data     bytes.Buffer
	overflow bool
}

func (l *qualificationLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.data.Len()+len(p) <= 8<<20 {
		_, _ = l.data.Write(p)
	} else {
		l.overflow = true
	}
	return len(p), nil
}
func (l *qualificationLog) snapshot() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.data.String(), l.overflow
}

type qualificationResponse struct {
	Version int             `json:"version"`
	ID      int64           `json:"id"`
	Event   string          `json:"event"`
	Code    string          `json:"code"`
	Status  json.RawMessage `json:"status"`
}

type qualificationProcess struct {
	command      *exec.Cmd
	input        io.WriteCloser
	output       *os.File
	replies      chan qualificationResponse
	readDone     chan struct{}
	readerCancel context.CancelFunc
	done         chan struct{}
	sequence     int64
	mu           sync.Mutex
	logs         qualificationLog
	waitError    error
	readError    error
	expectedKill bool
}

func startQualificationProcess(t *testing.T, ctx context.Context, role isolatedRole, testName, optIn string) *qualificationProcess {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	arguments := append(append([]string{}, role.ExecPrefix...), binary, "-test.run", "^"+testName+"$", "-test.timeout=20m")
	require.NotEmpty(t, arguments)
	command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "TMPDIR=/tmp", "GOMAXPROCS=2", optIn + "=1"}
	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, writer, err := os.Pipe()
	require.NoError(t, err)
	readerCtx, readerCancel := context.WithCancel(ctx)
	p := &qualificationProcess{command: command, input: input, output: output, replies: make(chan qualificationResponse, 32), readDone: make(chan struct{}), done: make(chan struct{}), readerCancel: readerCancel}
	command.Stdout = writer
	command.Stderr = &p.logs
	require.NoError(t, command.Start())
	require.NoError(t, writer.Close())
	go func() {
		defer close(p.readDone)
		defer close(p.replies)
		scanner := bufio.NewScanner(io.TeeReader(output, &p.logs))
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			var response qualificationResponse
			if err := json.Unmarshal(scanner.Bytes(), &response); err != nil || response.Version != 1 {
				p.readError = errors.New("invalid sidecar response")
				return
			}
			select {
			case p.replies <- response:
			case <-readerCtx.Done():
				return
			}
		}
		if scanner.Err() != nil {
			p.readError = errors.New("sidecar response stream failed")
		}
	}()
	go func() { p.waitError = command.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = p.input.Close()
		select {
		case <-p.done:
		case <-time.After(15 * time.Second):
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Error("qualification sidecar did not stop")
			}
		}
		readerCancel()
		_ = output.Close()
		select {
		case <-p.readDone:
		case <-time.After(5 * time.Second):
			t.Error("qualification response reader did not join")
		}
		_, overflow := p.logs.snapshot()
		require.False(t, overflow, "sidecar diagnostic budget exceeded")
	})
	return p
}

func (p *qualificationProcess) request(ctx context.Context, op string, fields map[string]any) (qualificationResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return qualificationResponse{}, err
	}
	p.sequence++
	request := map[string]any{"version": 1, "id": p.sequence, "op": op}
	for key, value := range fields {
		if key != "version" && key != "id" && key != "op" {
			request[key] = value
		}
	}
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) >= 64<<10 {
		return qualificationResponse{}, errors.New("invalid sidecar command")
	}
	deadline := time.Now().Add(30 * time.Second)
	if bounded, ok := ctx.Deadline(); ok && bounded.Before(deadline) {
		deadline = bounded
	}
	pipe := p.input.(*os.File) // exec.StdinPipe creates an owned pollable pipe.
	if err = pipe.SetWriteDeadline(deadline); err != nil {
		return qualificationResponse{}, errors.New("sidecar command deadline failed")
	}
	defer pipe.SetWriteDeadline(time.Time{})
	if _, err = p.input.Write(append(encoded, '\n')); err != nil {
		return qualificationResponse{}, errors.New("sidecar command write failed")
	}
	select {
	case response, ok := <-p.replies:
		if !ok || response.ID != p.sequence {
			return qualificationResponse{}, errors.New("sidecar response correlation failed")
		}
		if response.Event == "error" || response.Code != "" {
			return qualificationResponse{}, errors.New("sidecar command rejected")
		}
		return response, nil
	case <-ctx.Done():
		return qualificationResponse{}, ctx.Err()
	}
}

// A protocol acknowledgment cannot attest a clean process exit. Drain stdout
// and stderr before checking errors or scanning every incarnation's diagnostics.
func (p *qualificationProcess) finalize(ctx context.Context) error {
	select {
	case <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-p.readDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	expectedKill := p.expectedKill
	p.mu.Unlock()
	if expectedKill {
		exited, ok := p.waitError.(*exec.ExitError)
		if !ok {
			return errors.New("sidecar crash did not terminate its process")
		}
		status, ok := exited.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return errors.New("sidecar crash had an unexpected exit status")
		}
	} else if p.waitError != nil {
		return errors.New("sidecar process did not exit successfully")
	}
	if p.readError != nil {
		return errors.New("sidecar response stream did not finish cleanly")
	}
	if _, overflow := p.logs.snapshot(); overflow {
		return errors.New("sidecar diagnostic budget exceeded")
	}
	return nil
}

// Crash deliberately kills this owned process group between protocol requests.
// The signal and complete reader/diagnostic drain are verified before recovery.
func (p *qualificationProcess) Crash(ctx context.Context) error {
	p.mu.Lock()
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		return err
	}
	select {
	case <-p.done:
		p.mu.Unlock()
		return errors.New("sidecar ended before the requested crash")
	default:
	}
	p.expectedKill = true
	err := syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
	p.mu.Unlock()
	if err != nil {
		return errors.New("sidecar crash signal failed")
	}
	return p.finalize(ctx)
}

func qualificationState(t *testing.T, uid, gid int) string {
	t.Helper()
	// Testing's own parent directories are private to root. These dedicated
	// disposable paths give each unprivileged actor its own writable state.
	root, err := os.MkdirTemp("/tmp", "shale-qualification-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(root, 0755))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	state := filepath.Join(root, "state")
	require.NoError(t, os.Mkdir(state, 0700))
	require.NoError(t, os.Chown(state, uid, gid))
	return state
}

func osReadQualificationProc(pid int) (map[string]string, error) {
	file, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return nil, errors.New("application process status unavailable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return nil, errors.New("application process status unavailable")
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		values := strings.Fields(value)
		if len(values) > 0 {
			fields[key] = values[0]
		}
	}
	return fields, nil
}
