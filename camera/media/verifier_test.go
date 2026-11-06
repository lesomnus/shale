package media_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
)

func fixtureRequest(path string) camera.MediaRequest {
	return camera.MediaRequest{Scope: camera.EnrollmentScope{TenantID: "tenant", NetworkID: "lan"}, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: (&url.URL{Scheme: "file", Path: path}).String()}, Duration: time.Second, RequireDecode: true}
}
func requireMediaTools(t *testing.T) {
	t.Helper()
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if _, e := exec.LookPath(name); e != nil {
			t.Skip(name + " not installed")
		}
	}
}
func TestActualH264AudioAndIndependentDecode(t *testing.T) {
	requireMediaTools(t)
	path, e := filepath.Abs("../../internal/producer/testdata/av.mp4")
	if e != nil {
		t.Fatal(e)
	}
	v := media.Verifier{NetworkScope: "lan", AllowFile: true}
	req := fixtureRequest(path)
	got, e := v.Verify(context.Background(), req)
	if e != nil {
		t.Fatal(e)
	}
	if !got.Complete || !got.Decoded || got.Profile.Codec != "h264" || got.Profile.Width != 640 || got.Profile.Height != 360 || got.Profile.FPS < 25 || got.AudioCodec == "" {
		t.Fatalf("wrong normalized facts: %+v", got)
	}
	if len(got.Evidence) < 2 {
		t.Fatal("packet and independent decode evidence absent")
	}
	req.Desired.Codec = "hevc"
	got, e = v.Verify(context.Background(), req)
	if !errors.Is(e, media.ErrMismatch) || got.Profile.Codec != "h264" || got.Decoded {
		t.Fatal("advertised HEVC mismatch passed")
	}
	req.NetworkScope = "other"
	if _, e = v.Verify(context.Background(), req); !errors.Is(e, camera.ErrForbidden) {
		t.Fatal("wrong network accepted")
	}
}
func TestActualHEVCAndEarlyEOF(t *testing.T) {
	requireMediaTools(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "hevc.mp4")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "color=c=black:s=160x90:r=10", "-t", "1.5", "-c:v", "libx265", "-preset", "ultrafast", "-x265-params", "pools=1:frame-threads=1:log-level=error", "-y", path)
	if out, e := c.CombinedOutput(); e != nil {
		t.Skipf("controlled HEVC encoder unavailable: %s", out)
	}
	v := media.Verifier{NetworkScope: "lan", AllowFile: true}
	req := fixtureRequest(path)
	req.Desired.Codec = "hevc"
	got, e := v.Verify(context.Background(), req)
	if e != nil {
		t.Fatal(e)
	}
	if got.Profile.Codec != "hevc" || !got.Decoded {
		t.Fatal("HEVC not independently decoded")
	}
	req.Duration = 4 * time.Second
	got, e = v.Verify(context.Background(), req)
	if !errors.Is(e, media.ErrIncomplete) || got.Complete {
		t.Fatal("short EOF reported as complete low FPS")
	}
}
func script(t *testing.T, content string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX process fixture")
	}
	path := filepath.Join(t.TempDir(), "fixture")
	if e := os.WriteFile(path, []byte("#!/bin/sh\n"+content+"\n"), 0700); e != nil {
		t.Fatal(e)
	}
	return path
}
func TestProbeTerminationAndIndependentDecodeFailure(t *testing.T) {
	packets := "printf '%s\\n' 'packet|stream_index=0|pts_time=0|dts_time=0|size=100|flags=K_' 'packet|stream_index=0|pts_time=1|dts_time=1|size=100|flags=__' 'stream|index=0|codec_type=video|codec_name=h264|width=160|height=90'"
	req := fixtureRequest("/controlled")
	req.Duration = time.Second
	for _, tc := range []struct {
		name, body string
		decode     bool
		want       error
	}{
		{"expiry", "sleep 2", false, media.ErrIncomplete},
		{"short-eof", "printf '%s\\n' 'packet|stream_index=0|pts_time=0|dts_time=0|size=100|flags=K_' 'stream|index=0|codec_type=video|codec_name=h264|width=160|height=90'", false, media.ErrIncomplete},
		{"decode-refused", packets, true, media.ErrDecode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := media.Verifier{NetworkScope: "lan", AllowFile: true, Ffprobe: script(t, tc.body), Ffmpeg: script(t, "exit 1"), StartupTimeout: 30 * time.Millisecond}
			r := req
			r.RequireDecode = tc.decode
			got, e := v.Verify(context.Background(), r)
			if !errors.Is(e, tc.want) || got.Decoded {
				t.Fatalf("wrong termination classification: %v", e)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v := media.Verifier{NetworkScope: "lan", AllowFile: true}
	if _, e := v.Verify(ctx, req); !errors.Is(e, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
func TestAuthenticationFailureDoesNotLeak(t *testing.T) {
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "s", ProducerID: "p", NetworkID: "lan"}
	store := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "private")}
	material := camera.CredentialMaterial{Username: "camera", Password: "reserved:@/? p%"}
	ref, e := store.Prepare(context.Background(), scope, camera.AuthStreaming, "input", material)
	if e != nil {
		t.Fatal(e)
	}
	tool := script(t, "printf '%s\\n' \"$*\" >&2\nexit 1")
	v := media.Verifier{AuthProvider: store, NetworkScope: "lan", Ffprobe: tool}
	req := camera.MediaRequest{Scope: scope, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: "rtsp://camera/stream"}, CredentialRef: &ref, Duration: time.Second}
	got, e := v.Verify(context.Background(), req)
	if e == nil {
		t.Fatal("authentication error missing")
	}
	if strings.Contains(e.Error(), material.Password) || strings.Contains(e.Error(), url.QueryEscape(material.Password)) || strings.Contains(e.Error(), url.UserPassword(material.Username, material.Password).String()) || strings.Contains(got.Profile.StreamURI, "@") {
		t.Fatal("credential leaked")
	}
}
