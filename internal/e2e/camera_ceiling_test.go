package e2e_test

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The #128 cases exercise actual recording, not just the proposed RPC number:
// no-declaration RTSP must infer a usable ceiling and max_bitrate:onvif must
// reread the real SOAP profile at the next negotiation while capture continues.
func TestCameraDerivedCeilingsRecord(t *testing.T) {
	for _, binary := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Skip(binary + " unavailable")
		}
	}
	for _, kind := range []string{"undeclared-rtsp", "onvif-profile"} {
		t.Run(kind, func(t *testing.T) {
			c := start(t)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			admin := c.dial("@acme/admin")
			smallLaminae(t, ctx, c)
			sets := api.NewSetServiceClient(admin)
			set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "camera-derived"}.Build())
			require.NoError(t, err)
			input := cameratest.ServeRTSP(t)
			source := producer.SourceConfig{Alias: "camera", Input: input}
			var device *cameratest.ONVIFCamera
			profile := cameramodel.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 1_000_000, GOP: 10, StreamURI: input}
			if kind == "onvif-profile" {
				device = cameratest.ServeONVIF(t, profile)
				source.Onvif = &producer.OnvifConfig{Endpoint: device.URL, ProfileToken: "main"}
				source.MaxBitrateOnvif = true
			}
			// In particular, neither case supplies Size, Fps, Format, Encoder or a
			// numeric MaxBitrate. The RTSP case supplies no ONVIF metadata at all.
			p, err := producer.New(producer.Config{StateDir: filepath.Join(t.TempDir(), "producer"), Cp: "http://" + c.running.TenantAddr, Dev: true, Sources: []producer.SourceConfig{source}, Mode: api.UploadMode_UPLOAD_MODE_LIVE, HeartbeatInterval: time.Second, SegmentDuration: 4 * time.Second, AllocationHorizon: time.Minute, Buffer: 64 << 20})
			require.NoError(t, err)
			pctx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- p.Run(pctx) }()
			t.Cleanup(func() {
				stop()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
					t.Error("Producer outlived cancellation")
				}
			})
			joined := adoptProducer(t, ctx, api.NewProducerServiceClient(admin), set)
			select {
			case <-p.Ready:
			case err := <-done:
				t.Fatalf("Producer: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			sources := api.NewSourceServiceClient(admin)
			rows, err := sources.List(ctx, api.SourceListRequest_builder{Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()}}.Build())
			require.NoError(t, err)
			require.Len(t, rows.GetItems(), 1)
			row := rows.GetItems()[0]
			require.Greater(t, row.GetProfile().GetMaxBitrate(), int64(100_000), "initial bitrate must come from actual mode, not the black scene's tiny VBR output")
			if device != nil {
				require.Equal(t, int64(1_128_000), row.GetProfile().GetMaxBitrate(), "ONVIF video ceiling plus default audio budget")
				profile.Bitrate = 2_000_000
				device.SetVideo(profile)
				// Reactivation is a supported policy operation which bumps Set version;
				// the actual Producer notices it on heartbeat and negotiates again.
				policies := api.NewUploadPolicyServiceClient(c.dialCluster(c.operator()))
				listed, err := policies.List(ctx, api.UploadPolicyListRequest_builder{Size: 100}.Build())
				require.NoError(t, err)
				var active *api.UploadPolicy
				for _, policy := range listed.GetItems() {
					if policy.GetActive() {
						active = policy
						break
					}
				}
				require.NotNil(t, active)
				_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: active.GetId()}.Build()}.Build())
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					current, err := sources.Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: row.GetId()}.Build()}.Build())
					return err == nil && current.GetProfile().GetMaxBitrate() == 2_128_000
				}, 15*time.Second, 200*time.Millisecond, "next negotiation must read the changed ONVIF profile")
			}
			// Observe actual capture as well as a complete recording started after the
			// ceiling was settled. A Source metadata row alone does not satisfy #128.
			after := time.Now()
			producers := api.NewProducerServiceClient(admin)
			require.Eventually(t, func() bool {
				current, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: joined.GetId()}.Build()}.Build())
				if err != nil {
					return false
				}
				for _, report := range current.GetStatus().GetSources() {
					if bytes.Equal(report.GetSourceId(), row.GetId()) && report.GetInputUp() && report.GetFrameRate() > 0 && report.GetMeasuredBitrate() > 0 {
						return true
					}
				}
				return false
			}, 20*time.Second, 200*time.Millisecond, "inferred camera must be ingesting")
			laminae := api.NewLaminaServiceClient(admin)
			var complete *api.Lamina
			require.Eventually(t, func() bool {
				listed, err := laminae.List(ctx, api.LaminaListRequest_builder{Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Source: api.SourceRef_builder{Id: row.GetId()}.Build()}.Build()}, Size: 100, Reverse: true}.Build())
				if err != nil {
					return false
				}
				for _, lamina := range listed.GetItems() {
					if lamina.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && !lamina.GetIncomplete() && lamina.GetSize() > 0 && lamina.GetDateStarted() != nil && lamina.GetDateStarted().AsTime().After(after) {
						complete = lamina
						return true
					}
				}
				return false
			}, 50*time.Second, 250*time.Millisecond, "camera-derived ceiling must produce complete committed media")
			timeline, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{Source: api.SourceRef_builder{Id: row.GetId()}.Build(), From: timestamppb.New(complete.GetDateStarted().AsTime().Add(-time.Second)), To: timestamppb.New(complete.GetDateEnded().AsTime().Add(time.Second)), Size: 100}.Build())
			require.NoError(t, err)
			require.Len(t, timeline.GetSources(), 1)
			fetched := false
			for _, object := range timeline.GetSources()[0].GetLaminae() {
				if !bytes.Equal(object.GetLaminaId(), complete.GetId()) {
					continue
				}
				raw := fetch(t, object.GetUrl())
				playsOnItsOwn(t, raw)
				decode := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-xerror", "-i", "pipe:0", "-f", "null", "-")
				decode.Stdin = bytes.NewReader(raw)
				decode.Stdout = io.Discard
				require.NoError(t, decode.Run())
				fetched = true
				break
			}
			require.True(t, fetched, "committed inferred-profile recording must be readable and independently decodable")
		})
	}
}
