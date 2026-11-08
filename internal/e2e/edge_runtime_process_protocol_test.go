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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/stretchr/testify/require"
)

func TestEdgeRuntimeProcessInputBoundary(t *testing.T) {
	for _, raw := range []string{
		`{"version":1,"version":1,"id":1,"op":"status"}`,
		`{"version":2,"Version":1,"id":1,"op":"status"}`,
		`{"version":1,"id":1,"op":"start","config":{"edge":{"roles":["worker"],"roles":["producer"]}}}`,
		`{"version":1,"id":1,"op":"start","config":{"edge":{"ro\u006ces":[],"roles":[]}}}`,
		`{"version":1,"id":1,"op":"status","config":null}`,
		`{"version":1,"id":0,"op":"status"}`,
		`{"version":1,"id":1,"op":"status","password":"private-password-sentinel"}`,
		`{"version":1,"id":1,"op":"private-password-sentinel"}`,
		strings.Repeat("[", 33) + strings.Repeat("]", 33),
		strings.Repeat("x", 64<<10),
	} {
		_, ok := edgeRuntimeRequestFrom([]byte(raw))
		require.False(t, ok, "invalid bounded protocol declaration")
	}
	root := t.TempDir()
	valid := map[string]any{"state": root, "cp": "https://127.0.0.1:1", "tenant": "acme", "edge": map[string]any{"roles": []string{"worker"}}, "camera_worker": map[string]any{"discovery_interval": "1s", "discovery_timeout": "200ms"}}
	raw, err := json.Marshal(valid)
	require.NoError(t, err)
	c, cfg, err := edgeRuntimeConfig(raw)
	require.NoError(t, err)
	require.Nil(t, cfg.Producer)
	require.NotNil(t, cfg.Worker)
	require.Nil(t, cfg.Worker.Discoverer, "production composition selects the discoverer")
	require.Nil(t, cfg.Worker.Executor, "production composition selects the leased executor")
	require.Equal(t, time.Second, c.CameraWorker.DiscoveryInterval)
	for _, change := range []func(map[string]any){
		func(v map[string]any) { v["cp"] = "https://user:private-password-sentinel@127.0.0.1:1" },
		func(v map[string]any) { v["cp"] = "https://127.0.0.1:1?token=private-password-sentinel" },
		func(v map[string]any) { v["auth"] = map[string]any{"password": "private-password-sentinel"} },
		func(v map[string]any) { v["policy"] = map[string]any{} },
		func(v map[string]any) { v["state"] = "relative-state" },
		func(v map[string]any) {
			v["producer"] = map[string]any{"sources": []any{map[string]any{"input": "rtsp://user:private-password-sentinel@127.0.0.1"}}}
		},
		func(v map[string]any) {
			v["producer"] = map[string]any{"credentials": map[string]any{"directory": "/private-password-sentinel"}}
		},
	} {
		var v map[string]any
		require.NoError(t, json.Unmarshal(raw, &v))
		change(v)
		input, err := json.Marshal(v)
		require.NoError(t, err)
		_, _, err = edgeRuntimeConfig(input)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-password-sentinel")
	}
	var output bytes.Buffer
	input := `{"version":1,"id":1,"op":"status","password":"private-password-sentinel"}` + "\n" +
		`{"version":1,"id":2,"op":"start","config":{"auth":{"password":"private-password-sentinel"}}}` + "\n" +
		`{"version":1,"id":3,"op":"stop"}` + "\n"
	require.NoError(t, runEdgeRuntimeProcess(t.Context(), io.NopCloser(strings.NewReader(input)), &output))
	require.NotContains(t, output.String(), "private-password-sentinel")
	require.NotContains(t, output.String(), "password")
	scanner := bufio.NewScanner(&output)
	var events []string
	for scanner.Scan() {
		var response edgeRuntimeResponse
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &response))
		events = append(events, response.Event)
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, []string{"error", "error", "stopped"}, events)
	output.Reset()
	require.Error(t, runEdgeRuntimeProcess(t.Context(), io.NopCloser(strings.NewReader(strings.Repeat("x", 64<<10))), &output))
	require.Equal(t, "{\"version\":1,\"event\":\"stopped\",\"code\":\"invalid-input\"}\n", output.String())
}

func TestEdgeRuntimeProcessStoredIdentityIsReadOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent-role")
	require.Equal(t, "awaiting_identity", edgeRuntimeRole(dir, 31, time.Now()).IdentityState)
	_, err := os.Stat(dir)
	require.True(t, os.IsNotExist(err), "status cannot create a key or directory")
	store := pki.Store{Dir: dir}
	key, err := store.Key() // fixture setup; the status implementation never calls Key.
	require.NoError(t, err)
	state := edgeRuntimeRole(dir, 31, time.Now())
	require.Equal(t, "awaiting_adoption", state.IdentityState)
	require.Len(t, state.SPKISHA256, 64)
	now := time.Now()
	ca, err := pki.NewCA("runtime-process-test", now)
	require.NoError(t, err)
	csr, err := key.CSR("worker")
	require.NoError(t, err)
	id := pdid.New(31)
	cert, err := ca.IssueClientHost(csr, id, now)
	require.NoError(t, err)
	require.NoError(t, store.SetCert(pki.EncodeCerts(cert), ca.Bundle()))
	state = edgeRuntimeRole(dir, 31, now)
	require.Equal(t, "adopted", state.IdentityState)
	require.Equal(t, id.String(), state.ID)
	require.Equal(t, "invalid_certificate", edgeRuntimeRole(dir, 22, now).IdentityState)
	require.Equal(t, "expired_certificate", edgeRuntimeRole(dir, 31, cert.NotAfter.Add(time.Second)).IdentityState)
	require.Equal(t, "expired_certificate", edgeRuntimeRole(dir, 31, cert.NotBefore.Add(-time.Second)).IdentityState)
	other, err := pki.NewHostKey()
	require.NoError(t, err)
	otherCSR, err := other.CSR("worker")
	require.NoError(t, err)
	otherCert, err := ca.IssueClientHost(otherCSR, id, now)
	require.NoError(t, err)
	require.NoError(t, store.SetCert(pki.EncodeCerts(otherCert), ca.Bundle()))
	require.Equal(t, "invalid_certificate", edgeRuntimeRole(dir, 31, now).IdentityState)
}

func TestEdgeRuntimeProcessReaderCancellation(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- runEdgeRuntimeProcess(ctx, reader, &output) }()
	_, err := io.WriteString(writer, "{\"version\":1,\"id\":1,\"op\":\"status\"}\n")
	require.NoError(t, err)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("owned runtime input reader did not join")
	}
	_, err = writer.Write([]byte("private"))
	require.True(t, errors.Is(err, io.ErrClosedPipe))
}

// This is a bounded process/protocol smoke against a refused loopback port. It
// starts no CP, camera, encoder, decoder, DHCP service or network namespace.
// Privileged Linux runners additionally drop to the topology's UID/GID 65534.
func TestEdgeRuntimeProcessLifecycle(t *testing.T) {
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Skip("setpriv required for the unprivileged runtime smoke")
	}
	root, err := os.MkdirTemp("", "shale-edge-runtime-process-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(root) })
	require.NoError(t, os.Chmod(root, 0755))
	source, err := os.Executable()
	require.NoError(t, err)
	in, err := os.Open(source)
	require.NoError(t, err)
	defer in.Close()
	binary := filepath.Join(root, "e2e.test")
	out, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	require.NoError(t, err)
	_, err = io.Copy(out, in)
	require.NoError(t, out.Close())
	require.NoError(t, err)
	for _, scenario := range []struct {
		roles       []string
		termination string
	}{{[]string{"worker"}, "stop"}, {[]string{"producer"}, "eof"}, {[]string{"worker", "producer"}, "signal"}} {
		t.Run(scenario.termination, func(t *testing.T) {
			state := filepath.Join(root, scenario.termination)
			require.NoError(t, os.Mkdir(state, 0700))
			expectedUID, expectedGID := os.Geteuid(), os.Getegid()
			prefix := []string{"--no-new-privs", "--inh-caps=-all", "--ambient-caps=-all"}
			if os.Geteuid() == 0 {
				expectedUID, expectedGID = 65534, 65534
				require.NoError(t, os.Chown(state, expectedUID, expectedGID))
				prefix = append(prefix, "--bounding-set=-all", "--reuid=65534", "--regid=65534", "--clear-groups")
			}
			call, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			args := append(prefix, binary, "-test.run=^TestEdgeRuntimeProcess$", "-test.timeout=18s", "-test.v")
			process := exec.CommandContext(call, setpriv, args...)
			process.Env = append(os.Environ(), edgeRuntimeProcessOptIn+"=1")
			stdin, err := process.StdinPipe()
			require.NoError(t, err)
			stdout, publicWriter, err := os.Pipe()
			require.NoError(t, err)
			defer stdout.Close()
			defer publicWriter.Close()
			process.Stdout = publicWriter
			var stderr bytes.Buffer
			process.Stderr = &stderr
			require.NoError(t, process.Start())
			require.NoError(t, publicWriter.Close())
			replies := make(chan edgeRuntimeResponse, 8)
			readDone := make(chan error, 1)
			readerExited := make(chan struct{})
			go func() {
				defer close(readerExited)
				scanner := bufio.NewScanner(stdout)
				scanner.Buffer(make([]byte, 4096), 64<<10)
				for scanner.Scan() {
					var r edgeRuntimeResponse
					if json.Unmarshal(scanner.Bytes(), &r) != nil {
						readDone <- errors.New("non-JSON protocol output")
						return
					}
					select {
					case replies <- r:
					case <-call.Done():
						readDone <- call.Err()
						return
					}
				}
				readDone <- scanner.Err()
			}()
			finished := make(chan error, 1)
			processExited := make(chan struct{})
			go func() { defer close(processExited); finished <- process.Wait() }()
			t.Cleanup(func() {
				cancel()
				stdin.Close()
				stdout.Close()
				select {
				case <-processExited:
				case <-time.After(3 * time.Second):
					t.Error("runtime child not reaped")
				}
				select {
				case <-readerExited:
				case <-time.After(time.Second):
					t.Error("runtime output reader not joined")
				}
			})
			pending := "start"
			receive := func() edgeRuntimeResponse {
				select {
				case r := <-replies:
					return r
				case <-call.Done():
					t.Fatalf("bounded runtime protocol response missing for %s", pending)
					return edgeRuntimeResponse{}
				}
			}
			send := func(v any) edgeRuntimeResponse {
				require.NoError(t, json.NewEncoder(stdin).Encode(v))
				return receive()
			}
			first := send(map[string]any{"version": 1, "id": 1, "op": "start", "config": map[string]any{"state": state, "cp": "https://127.0.0.1:1", "tenant": "acme", "edge": map[string]any{"roles": scenario.roles, "retry_interval": "100ms"}, "producer": map[string]any{"buffer": "16MiB"}, "camera_worker": map[string]any{"retry_interval": "100ms"}}})
			require.Equal(t, "started", first.Event)
			require.Equal(t, "running", first.Status.Runtime)
			require.Equal(t, expectedUID, first.Status.EUID)
			require.Equal(t, expectedGID, first.Status.EGID)
			require.Equal(t, 1, first.Status.NoNewPrivs)
			for _, bits := range []string{first.Status.CapInh, first.Status.CapPrm, first.Status.CapEff, first.Status.CapAmb} {
				require.Equal(t, "0000000000000000", bits)
			}
			if os.Geteuid() == 0 {
				require.Equal(t, "0000000000000000", first.Status.CapBnd)
			}
			require.Eventually(t, func() bool {
				pending = "status"
				r := send(map[string]any{"version": 1, "id": 2, "op": "status"})
				if r.Event != "status" || len(r.Status.Roles) != len(scenario.roles) {
					return false
				}
				for _, role := range r.Status.Roles {
					if role.IdentityState != "awaiting_adoption" || len(role.SPKISHA256) != 64 || role.ID != "" {
						return false
					}
				}
				return true
			}, 3*time.Second, 20*time.Millisecond, "real runtime creates only each selected machine key before adoption")
			switch scenario.termination {
			case "stop":
				pending = "stop"
				require.Equal(t, "stopped", send(map[string]any{"version": 1, "id": 3, "op": "stop"}).Event)
			case "eof":
				pending = "eof"
				require.NoError(t, stdin.Close())
				require.Equal(t, "stopped", receive().Event)
			case "signal":
				pending = "signal"
				require.NoError(t, process.Process.Signal(syscall.SIGTERM))
				require.Equal(t, "stopped", receive().Event)
			}
			select {
			case err := <-finished:
				require.NoError(t, err, "runtime subprocess did not stop cleanly")
			case <-call.Done():
				t.Fatal("runtime subprocess did not join")
			}
			select {
			case err := <-readDone:
				require.NoError(t, err)
			case <-call.Done():
				t.Fatal("public reader did not join")
			}
			require.NotContains(t, stderr.String(), "private-password-sentinel")
		})
	}
}
