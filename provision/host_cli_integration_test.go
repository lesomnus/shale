package provision_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/internal/provisionhost"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

// Process-boundary host startup is covered in cli's subprocess fixture. This
// test connects the real remote CLI and Payday HTTP queue to a real protocol,
// media-qualification and deployment engine rather than a fake executor.
func TestProtocolRemoteCLIApplyStatusResumeAndEmbeddedReplay(t *testing.T) {
	f := protocolFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine := f.engine(t)
	token := "camera-cli-conformance-token-at-least-32-bytes"
	host, err := provisionhost.New(provisionhost.Options{
		StateDir: filepath.Join(f.dir, "host"),
		Networks: []provisionhost.Network{{ID: "lan", CIDRs: []string{"127.0.0.0/8"}}},
		Targets:  []provisionhost.Target{{Scope: f.request.Scope, Provider: "file", Executor: engine}},
		Clients:  []provisionhost.Client{{Token: token, Scopes: []camera.EnrollmentScope{f.request.Scope}}},
	})
	require.NoError(t, err)
	handler, err := host.Handler()
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	defer server.Close()
	runctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- host.Run(runctx) }()
	defer func() {
		stop()
		if done != nil {
			require.NoError(t, <-done)
		}
	}()
	tokenPath := filepath.Join(f.dir, "cli.token")
	require.NoError(t, os.WriteFile(tokenPath, []byte(token), 0600))
	requestPath := filepath.Join(f.dir, "cli-request.json")
	writeRequest := func(q provision.Request) {
		body, err := json.Marshal(q)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(requestPath, body, 0600))
	}
	writeRequest(f.request)
	call := func(name string, args ...string) provisionhost.Job {
		var out bytes.Buffer
		command := cli.NewCmdCamera()
		command.Writer, command.ErrWriter = &out, io.Discard
		argv := []string{name, "--server", server.URL, "--token-file", tokenPath, "--allow-loopback-http"}
		argv = append(argv, args...)
		require.NoError(t, command.Run(ctx, argv))
		require.NotContains(t, out.String(), "prepared +:/?#% password")
		require.NotContains(t, out.String(), "stream +:/?#% password")
		var job provisionhost.Job
		require.NoError(t, json.Unmarshal(out.Bytes(), &job), out.String())
		return job
	}
	active := call("apply", "--request", requestPath, "--wait")
	require.Equal(t, "done", active.State)
	require.NotNil(t, active.Run)
	require.Equal(t, provision.Active, active.Run.Stage)
	require.True(t, active.Run.Verified.Complete && active.Run.Verified.Decoded)
	require.Equal(t, "h264", active.Run.Verified.Profile.Codec)
	require.Equal(t, "stable-logical-source", active.Run.Binding.SourceID)
	status := call("status", "--id", f.request.ID)
	require.Equal(t, active.Run.Binding.SourceID, status.Run.Binding.SourceID)
	replay := call("apply", "--request", requestPath, "--wait")
	require.Equal(t, active.Run.Binding.SourceID, replay.Run.Binding.SourceID)
	// A second request for the same desired target first plans read-only; the
	// explicit resume --apply follows the identical workflow and binding.
	followup := f.request
	followup.ID = "cli-plan-and-resume"
	writeRequest(followup)
	planned := call("plan", "--request", requestPath, "--wait")
	require.Equal(t, provision.Planned, planned.Run.Stage)
	resumed := call("resume", "--id", followup.ID, "--apply", "--wait")
	require.Equal(t, provision.Active, resumed.Run.Stage)
	require.Equal(t, active.Run.Binding.SourceID, resumed.Run.Binding.SourceID)
	// Stop the queue before inspecting counters or embedding another caller.
	stop()
	require.NoError(t, <-done)
	done = nil
	embedded, err := f.engine(t).Execute(ctx, f.request, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, embedded.Stage)
	require.Equal(t, active.Run.Binding.SourceID, embedded.Binding.SourceID)
	require.Equal(t, 1, f.restarts)
	f.assertNoSecrets(t)
}
