package producer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/internal/cameratest"
)

// A camera's capture never puts its credential in ffmpeg's command line,
// which every local user of a producer host can read (#249): ffmpeg records
// through a loopback proxy that signs in for it.
func TestCaptureKeepsTheCredentialOutOfArgv(t *testing.T) {
	real, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "argv.log")
	ffmpeg := filepath.Join(dir, "ffmpeg")
	require.NoError(t, os.WriteFile(ffmpeg, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nexec %q \"$@\"\n", log, real)), 0o700))

	material := camera.CredentialMaterial{Username: "viewer", Password: "s3cret:@/?% pass"}
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	provider := credential.FileProvider{Dir: filepath.Join(dir, "private")}
	ref, err := provider.Prepare(context.Background(), scope, camera.AuthStreaming, "stream", material)
	require.NoError(t, err)
	c := &Capture{
		Ffmpeg:       ffmpeg,
		AuthProvider: provider,
		Source:       SourceConfig{Alias: "door", Input: cameratest.ServeRTSPDigest(t, material.Username, material.Password), Format: "h264", InputCredential: &ref, Scope: scope, Audio: &AudioConfig{Codec: "none"}},
		Log:          slog.New(slog.DiscardHandler),
		Profile:      func() (int64, time.Duration) { return 2_000_000, time.Second },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) {
			b := make([]byte, 64<<10)
			n, _ := io.ReadAtLeast(r, b, 1024)
			select {
			case got <- n:
			default:
			}
			cancel()
			_, _ = io.Copy(io.Discard, r)
		})
	}()
	select {
	case n := <-got:
		require.GreaterOrEqual(t, n, 1024, "the capture recorded through the proxy")
	case <-time.After(20 * time.Second):
		t.Fatal("nothing was recorded through the proxy")
	}
	cancel()
	<-done

	b, err := os.ReadFile(log)
	require.NoError(t, err)
	require.Contains(t, string(b), "rtsp://127.0.0.1:")
	for _, secret := range []string{material.Password, url.PathEscape(material.Password), url.QueryEscape(material.Password), url.UserPassword("", material.Password).String()[1:], material.Username + ":"} {
		require.False(t, strings.Contains(string(b), secret), "ffmpeg's command line carries the credential (%q)", secret)
	}
}
