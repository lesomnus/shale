package media_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/internal/cameratest"
)

// recordingTools are ffprobe and ffmpeg behind wrappers that write down
// every command line they are given, as /proc/<pid>/cmdline shows it to any
// local user, and then run the real tool.
func recordingTools(t *testing.T) (ffprobe, ffmpeg, log string) {
	t.Helper()
	dir := t.TempDir()
	log = filepath.Join(dir, "argv.log")
	wrap := func(name string) string {
		real, err := exec.LookPath(name)
		if err != nil {
			t.Skip(name + " not installed")
		}
		path := filepath.Join(dir, name)
		script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %q\nexec %q \"$@\"\n", log, real)
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}

	return wrap("ffprobe"), wrap("ffmpeg"), log
}

// No command line a verification runs carries the camera's credential
// (#249): the tools read the stream through a loopback proxy that signs in
// for them, Basic or Digest, and they still decode it.
func TestToolsNeverSeeTheCredential(t *testing.T) {
	material := camera.CredentialMaterial{Username: "viewer", Password: "s3cret:@/?% pass"}
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	for _, tc := range []struct {
		name   string
		serve  func(testing.TB, string, string) string
		legacy bool
	}{
		{"referenced, basic", cameratest.ServeRTSPAuth, false},
		{"referenced, digest", cameratest.ServeRTSPDigest, false},
		{"in the URL, digest", cameratest.ServeRTSPDigest, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ffprobe, ffmpeg, log := recordingTools(t)
			address := tc.serve(t, material.Username, material.Password)
			req := camera.MediaRequest{Scope: scope, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: address, Codec: "h264", Width: 160, Height: 90, FPS: 10}, Duration: 2 * time.Second, RequireDecode: true}
			v := media.Verifier{Ffprobe: ffprobe, Ffmpeg: ffmpeg, NetworkScope: "lan", StartupTimeout: 3 * time.Second}
			if tc.legacy {
				u, err := url.Parse(address)
				if err != nil {
					t.Fatal(err)
				}
				u.User = url.UserPassword(material.Username, material.Password)
				req.Profile.StreamURI = u.String()
			} else {
				provider := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "private")}
				ref, err := provider.Prepare(context.Background(), scope, camera.AuthStreaming, "stream", material)
				if err != nil {
					t.Fatal(err)
				}
				req.CredentialRef, v.AuthProvider = &ref, provider
			}
			got, err := v.Verify(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Complete || !got.Decoded {
				t.Fatalf("the stream was not read through the proxy: %+v", got)
			}
			b, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if len(lines) != 2 {
				t.Fatalf("%d command lines, want ffprobe's and ffmpeg's", len(lines))
			}
			for _, secret := range []string{material.Password, url.PathEscape(material.Password), url.QueryEscape(material.Password), url.UserPassword("", material.Password).String()[1:], material.Username + ":", "@127.0.0.1"} {
				if strings.Contains(string(b), secret) {
					t.Fatalf("a command line carries the credential (%q)", secret)
				}
			}
			for _, line := range lines {
				if !strings.Contains(line, "rtsp://127.0.0.1:") {
					t.Fatalf("a tool did not read the loopback proxy: %s", line)
				}
			}
		})
	}
}
