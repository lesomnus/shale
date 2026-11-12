package producer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// producer.audio: none reaches the cameras a catalog hands the producer,
// whose specifications carry no audio: no sound is recorded, and its share
// leaves the ceiling the producer proposes (§38.3).
func TestProducerAudioNoneReachesManagedCameras(t *testing.T) {
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}, Audio: &AudioConfig{Codec: "none"}})
	cfg := p.configForSpec(v, v.Sources[0])
	require.Equal(t, "none", cfg.audioCodec())
	require.Equal(t, v.Sources[0].Profile.MaxBitrate, cfg.MaxBitrate, "no audio share above the video ceiling")
	args := strings.Join(Args(cfg, cfg.Encoder, cfg.MaxBitrate, 2*time.Second), " ")
	require.Contains(t, args, "-an")
	require.NotContains(t, args, "-map")
	require.NotContains(t, args, "-c:a")

	p.cfg.SourceProvider = testCatalog{value: v}
	require.NoError(t, p.loadSourceCatalog(t.Context()))
	require.Equal(t, "none", p.stagedSources()[0].cfg.audioCodec())

	// Without it the camera's audio is recorded, with Opus beside it, as before.
	q, w := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
	cfg = q.configForSpec(w, w.Sources[0])
	require.Nil(t, cfg.Audio)
	require.Equal(t, w.Sources[0].Profile.MaxBitrate+2*DefaultAudioBitrate, cfg.MaxBitrate)
	require.Contains(t, strings.Join(Args(cfg, cfg.Encoder, cfg.MaxBitrate, 2*time.Second), " "), "-c:a:0 copy")
}

// A source that names its own audio keeps it; one that names none takes the
// producer's.
func TestProducerAudioYieldsToASourcesOwn(t *testing.T) {
	p, err := New(Config{
		Audio: &AudioConfig{Codec: "none"},
		Sources: []SourceConfig{
			{Alias: "yard", Input: "rtsp://cam/1", Format: "h264", MaxBitrate: 2_000_000},
			{Alias: "door", Input: "v4l2:/dev/video0", Format: "mjpeg", MaxBitrate: 2_000_000, Audio: &AudioConfig{Device: "alsa:hw:1", Codec: "aac"}},
			{Alias: "demo", Input: DemoPrefix + DemoPatterns[0].Pattern, Size: DemoSize, Fps: DemoFps},
		},
	})
	require.NoError(t, err)
	got := map[string]string{}
	for _, s := range p.order {
		got[s.cfg.Alias] = s.cfg.audioCodec()
	}
	require.Equal(t, map[string]string{"yard": "none", "door": "aac", "demo": "none"}, got)
	require.True(t, p.order[1].cfg.hasMic())
}

// Only none: a microphone is one source's, and a codec that keeps sound is a
// choice each source makes for itself.
func TestProducerAudioTakesOnlyNone(t *testing.T) {
	src := []SourceConfig{{Alias: "yard", Input: "rtsp://cam/1", Format: "h264", MaxBitrate: 2_000_000}}
	for _, a := range []*AudioConfig{{Codec: "aac"}, {Codec: "copy"}, {Codec: ""}, {Device: "alsa:hw:1"}, {Codec: "none", Bitrate: 64_000}, {Codec: "none", Device: "alsa:hw:1"}} {
		_, err := New(Config{Audio: a, Sources: src})
		require.Error(t, err, "%+v", *a)
	}
	_, err := New(Config{Audio: &AudioConfig{Codec: " None "}, Sources: src})
	require.NoError(t, err)
}

func TestAudioShareOfNoneIsNothing(t *testing.T) {
	require.Zero(t, SourceConfig{Audio: &AudioConfig{Codec: "none"}}.audioShare())
	require.Equal(t, int64(2*DefaultAudioBitrate), SourceConfig{}.audioShare())
	require.Equal(t, int64(DefaultAudioBitrate), SourceConfig{Audio: &AudioConfig{Codec: "opus"}}.audioShare())
}
