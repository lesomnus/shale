package media_test

import (
	"context"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/internal/cameratest"
	"path/filepath"
	"testing"
	"time"
)

// This fixture serves synthetic H.264 over real RTSP/RTP interleaved TCP. It
// records no workplace media and exercises both the observer and independent
// decoder's actual network sessions, not a mocked ffprobe JSON result.
func TestActualRTSPQualification(t *testing.T) {
	requireMediaTools(t)
	address := cameratest.ServeRTSP(t)
	req := camera.MediaRequest{Scope: camera.EnrollmentScope{TenantID: "tenant", NetworkID: "lan"}, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: address, Codec: "h264", Width: 160, Height: 90, FPS: 10}, Desired: camera.DesiredState{GOP: 10}, Duration: 2 * time.Second, RequireDecode: true}
	v := media.Verifier{NetworkScope: "lan", StartupTimeout: 3 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, e := v.Verify(ctx, req)
	if e != nil {
		t.Fatalf("%v: %+v", e, got)
	}
	if !got.Complete || !got.Decoded || got.ArrivalFPS < 7 || got.ArrivalFPS > 15 || got.MaxGap > 500*time.Millisecond || got.Profile.GOP < 8 || got.Profile.GOP > 12 {
		t.Fatalf("bad network observation: %+v", got)
	}
}

func TestActualRTSPAuthenticationReservedPassword(t *testing.T) {
	requireMediaTools(t)
	material := camera.CredentialMaterial{Username: "camera", Password: "reserved:@/?% password"}
	address := cameratest.ServeRTSPAuth(t, material.Username, material.Password)
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	provider := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "private")}
	ref, e := provider.Prepare(context.Background(), scope, camera.AuthStreaming, "stream", material)
	if e != nil {
		t.Fatal(e)
	}
	req := camera.MediaRequest{Scope: scope, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: address, Codec: "h264", Width: 160, Height: 90, FPS: 10}, CredentialRef: &ref, Duration: 2 * time.Second, RequireDecode: true}
	v := media.Verifier{AuthProvider: provider, NetworkScope: "lan", StartupTimeout: 3 * time.Second}
	got, e := v.Verify(context.Background(), req)
	if e != nil {
		t.Fatal(e)
	}
	if !got.Complete || !got.Decoded {
		t.Fatal("authenticated RTSP stream not independently decoded")
	}
}
