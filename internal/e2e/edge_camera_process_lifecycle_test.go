//go:build linux

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
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestEdgeCameraProcessOwnedInputCancellation(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer writer.Close() // Keep the producer open throughout cancellation/join.
	input, err := edgeCameraOwnedFile(reader, "camera-test-private-input")
	require.NoError(t, err)
	defer input.Close()
	require.NoError(t, reader.Close())
	raw, err := input.SyscallConn()
	require.NoError(t, err)
	var flags, mode int
	var flagErr error
	require.NoError(t, raw.Control(func(fd uintptr) {
		flags, flagErr = unix.FcntlInt(fd, unix.F_GETFD, 0)
		if flagErr == nil {
			mode, flagErr = unix.FcntlInt(fd, unix.F_GETFL, 0)
		}
	}))
	require.NoError(t, flagErr)
	require.NotZero(t, flags&unix.FD_CLOEXEC)
	require.NotZero(t, mode&unix.O_NONBLOCK)
	observed := &edgeCameraObservedInput{File: input, nextRead: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- runEdgeCameraProcess(t, ctx, observed, &output) }()
	_, err = io.WriteString(writer, "{\"version\":1,\"id\":1,\"op\":\"status\"}\n")
	require.NoError(t, err)
	select {
	case <-observed.nextRead:
	case <-time.After(time.Second):
		t.Fatal("camera scanner did not return to its pending pipe read")
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("owned camera pipe reader did not join without producer EOF")
	}
	_, err = writer.Write([]byte("private-input-after-cancellation"))
	require.Error(t, err, "cancellation closes the owned pipe reader")
}

type edgeCameraObservedInput struct {
	*os.File
	reads    int
	nextRead chan struct{}
}

func (r *edgeCameraObservedInput) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 2 {
		close(r.nextRead)
	}
	return r.File.Read(p)
}

type edgeCameraDeadlineOutput struct {
	bytes.Buffer
	deadline time.Time
	fail     bool
}

func (w *edgeCameraDeadlineOutput) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	if w.fail {
		return errors.New("private-output-error-sentinel")
	}
	return nil
}

func TestEdgeCameraProcessOutputDeadline(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(strconv.FormatBool(fail), func(t *testing.T) {
			output := &edgeCameraDeadlineOutput{fail: fail}
			before := time.Now()
			err := runEdgeCameraProcess(t, t.Context(), strings.NewReader("{\"version\":1,\"id\":1,\"op\":\"status\"}\n"), output)
			require.WithinRange(t, output.deadline, before.Add(5*time.Second), time.Now().Add(5*time.Second))
			if fail {
				require.EqualError(t, err, "camera output unavailable")
				require.Zero(t, output.Len(), "failed deadline setup must prevent the write")
			} else {
				require.NoError(t, err)
				require.JSONEq(t, `{"version":1,"id":1,"event":"error","code":"invalid-start"}`, output.String())
			}
		})
	}
}

// No camera, encoder, discovery listener or network namespace is started here.
// The assembled isolated workflow separately proves initialized stop/session
// cleanup while the coordinator keeps the private stdin producer open.
func TestEdgeCameraProcessLifecycle(t *testing.T) {
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Skip("setpriv required for the unprivileged camera protocol smoke")
	}
	root, err := os.MkdirTemp("", "shale-edge-camera-lifecycle-")
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
	_, copyErr := io.Copy(out, in)
	require.NoError(t, out.Close())
	require.NoError(t, copyErr)
	for _, ending := range []string{"eof", "signal"} {
		t.Run(ending, func(t *testing.T) {
			prefix := []string{"--no-new-privs", "--inh-caps=-all", "--ambient-caps=-all"}
			if os.Geteuid() == 0 {
				prefix = append(prefix, "--bounding-set=-all", "--reuid=65534", "--regid=65534", "--clear-groups")
			}
			call, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			args := append(prefix, binary, "-test.run=^TestEdgeCameraProcess$", "-test.timeout=8s", "-test.v")
			process := exec.CommandContext(call, setpriv, args...)
			process.Env = append(os.Environ(), edgeCameraProcessOptIn+"=1")
			private, stdin, err := os.Pipe()
			require.NoError(t, err)
			defer private.Close()
			defer stdin.Close()
			stdout, public, err := os.Pipe()
			require.NoError(t, err)
			defer stdout.Close()
			defer public.Close()
			process.Stdin, process.Stdout = private, public
			var diagnostics bytes.Buffer
			process.Stderr = &diagnostics
			require.NoError(t, process.Start())
			require.NoError(t, private.Close())
			require.NoError(t, public.Close())
			finished := make(chan error, 1)
			processExited := make(chan struct{})
			go func() { defer close(processExited); finished <- process.Wait() }()
			replies := make(chan edgeCameraResponse, 4)
			readDone := make(chan error, 1)
			readerExited := make(chan struct{})
			go func() {
				defer close(readerExited)
				scanner := bufio.NewScanner(stdout)
				scanner.Buffer(make([]byte, 4096), 64<<10)
				for scanner.Scan() {
					var reply edgeCameraResponse
					if json.Unmarshal(scanner.Bytes(), &reply) != nil || reply.Version != 1 {
						readDone <- errors.New("non-JSON camera protocol output")
						return
					}
					select {
					case replies <- reply:
					case <-call.Done():
						readDone <- call.Err()
						return
					}
				}
				readDone <- scanner.Err()
			}()
			t.Cleanup(func() {
				cancel()
				stdin.Close()
				stdout.Close()
				select {
				case <-processExited:
				case <-time.After(3 * time.Second):
					t.Error("camera protocol child not reaped")
				}
				select {
				case <-readerExited:
				case <-time.After(time.Second):
					t.Error("camera protocol output reader did not join")
				}
			})
			require.NoError(t, json.NewEncoder(stdin).Encode(map[string]any{"version": 1, "id": 1, "op": "status"}))
			select {
			case reply := <-replies:
				require.Equal(t, int64(1), reply.ID)
				require.Equal(t, "error", reply.Event)
				require.Equal(t, "invalid-start", reply.Code)
				require.Nil(t, reply.Status)
			case err := <-readDone:
				t.Fatalf("camera protocol ended before its first response: %v", err)
			case <-call.Done():
				t.Fatal("camera protocol response did not arrive")
			}
			if os.Geteuid() == 0 {
				privileges, err := os.ReadFile("/proc/" + strconv.Itoa(process.Process.Pid) + "/status")
				require.NoError(t, err)
				fields := map[string][]string{}
				for line := range strings.SplitSeq(string(privileges), "\n") {
					key, value, ok := strings.Cut(line, ":")
					if ok {
						fields[key] = strings.Fields(value)
					}
				}
				require.Equal(t, []string{"65534", "65534", "65534", "65534"}, fields["Uid"])
				require.Equal(t, []string{"65534", "65534", "65534", "65534"}, fields["Gid"])
				require.Equal(t, []string{"1"}, fields["NoNewPrivs"])
				for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
					require.Equal(t, []string{"0000000000000000"}, fields[key])
				}
			}
			if ending == "eof" {
				require.NoError(t, stdin.Close())
			} else {
				// Intentionally retain stdin through Wait. EOF must not be what
				// unblocks the scanner when the signal requests cancellation.
				require.NoError(t, process.Process.Signal(syscall.SIGTERM))
			}
			select {
			case err := <-finished:
				require.NoError(t, err, "camera protocol child did not exit cleanly")
			case <-call.Done():
				t.Fatal("camera protocol child did not join")
			}
			select {
			case err := <-readDone:
				require.NoError(t, err)
			case <-call.Done():
				t.Fatal("camera protocol output reader did not join")
			}
			require.Empty(t, replies, "testing prelude/PASS output must stay on stderr")
			require.Contains(t, diagnostics.String(), "--- PASS: TestEdgeCameraProcess")
		})
	}
}
