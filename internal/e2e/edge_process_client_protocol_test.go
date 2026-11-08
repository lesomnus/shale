package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A deliberately tiny subprocess proves that a valid final acknowledgment is
// insufficient when the process or its remaining output subsequently fails.
func TestQualificationClientProcessHelper(t *testing.T) {
	if os.Getenv("SHALE_QUALIFICATION_CLIENT_PROCESS") != "1" {
		t.Skip("opt-in qualification protocol subprocess")
	}
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		os.Exit(4)
	}
	var request struct {
		Version int    `json:"version"`
		ID      int64  `json:"id"`
		Mode    string `json:"mode"`
	}
	if json.Unmarshal(scanner.Bytes(), &request) != nil {
		os.Exit(4)
	}
	event := "stopped"
	if request.Mode == "wait_for_kill" {
		event = "started"
	}
	_ = json.NewEncoder(os.Stdout).Encode(qualificationResponse{Version: 1, ID: request.ID, Event: event})
	switch request.Mode {
	case "wait_for_kill":
		for {
			time.Sleep(time.Second)
		}
	case "bad_exit":
		os.Exit(3)
	case "bad_json":
		_, _ = os.Stdout.WriteString("malformed-terminal-output\n")
	}
	os.Exit(0)
}

func TestQualificationCrashChecksExpectedSignalAndJoins(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p := startQualificationProcess(t, ctx, isolatedRole{ExecPrefix: []string{"/usr/bin/env"}}, "TestQualificationClientProcessHelper", "SHALE_QUALIFICATION_CLIENT_PROCESS")
	response, err := p.request(ctx, "start", map[string]any{"mode": "wait_for_kill"})
	require.NoError(t, err)
	require.Equal(t, "started", response.Event)
	require.NoError(t, p.Crash(ctx))
	require.NoError(t, p.finalize(ctx), "later log scans must recognize only this explicit verified crash")
}

func TestQualificationFinalizationRejectsAcknowledgedFailures(t *testing.T) {
	for _, mode := range []string{"ok", "bad_exit", "bad_json"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			p := startQualificationProcess(t, ctx, isolatedRole{ExecPrefix: []string{"/usr/bin/env"}}, "TestQualificationClientProcessHelper", "SHALE_QUALIFICATION_CLIENT_PROCESS")
			response, err := p.request(ctx, "stop", map[string]any{"mode": mode})
			require.NoError(t, err)
			require.Equal(t, "stopped", response.Event)
			err = p.finalize(ctx)
			if mode == "ok" {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "acknowledged terminal failure must reject qualification")
			}
		})
	}
}

func TestQualificationClientCannotBlockOnUnreadCommandPipe(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close(); writer.Close() })
	raw, err := writer.SyscallConn()
	require.NoError(t, err)
	var sizeError error
	require.NoError(t, raw.Control(func(fd uintptr) { _, sizeError = unix.FcntlInt(fd, unix.F_SETPIPE_SZ, 4096) }))
	require.NoError(t, sizeError)
	client := &qualificationProcess{input: writer, replies: make(chan qualificationResponse)}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = client.request(ctx, "start", map[string]any{"config": strings.Repeat("bounded", 4096)})
	require.Error(t, err)
	require.GreaterOrEqual(t, time.Since(started), 20*time.Millisecond, "the write must actually wait for its pipe deadline")
	require.Less(t, time.Since(started), time.Second, "a child that stops reading must not hang the coordinator")
}
