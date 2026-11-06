package media_test

import (
	"context"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/internal/cameratest"
	"testing"
	"time"
)

// This fixture serves synthetic H.264 over real RTSP/RTP interleaved TCP. It
// records no workplace media and exercises both the observer and independent
// decoder's actual network sessions, not a mocked ffprobe JSON result.
func TestActualRTSPQualification(t *testing.T) {
	requireMediaTools(t)
	address := cameratest.ServeRTSP(t)
	req := camera.MediaRequest{Scope: camera.EnrollmentScope{TenantID: "tenant", NetworkID: "lan"}, NetworkScope: "lan", Profile: camera.MediaProfile{StreamURI: address, Codec: "h264", Width: 160, Height: 90, FPS: 10}, Duration: 2 * time.Second, RequireDecode: true}
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
