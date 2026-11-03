package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type exportedPlayback struct {
	File  string
	Media exportProbe
}

type exportPacket struct {
	Stream   int    `json:"stream_index"`
	PTS      string `json:"pts_time"`
	DTS      string `json:"dts_time"`
	Duration string `json:"duration_time"`
	Hash     string `json:"data_hash"`
	Flags    string `json:"flags"`
}

type exportStream struct {
	Index  int    `json:"index"`
	Codec  string `json:"codec_name"`
	Kind   string `json:"codec_type"`
	Frames string `json:"nb_read_frames"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

type exportProbe struct {
	Packets []exportPacket `json:"packets"`
	Streams []exportStream `json:"streams"`
	Format  struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

func exportPlaybackTools(t *testing.T) (ffprobe, ffmpeg string) {
	t.Helper()
	var err error
	ffprobe, err = exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is not installed; CI installs the independent playback tools")
	}
	ffmpeg, err = exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed; CI installs the independent playback tools")
	}
	return
}

// Only local synthetic/public fixture files are passed to these commands.
// The download capability and delegation handle never reach a subprocess.
func exportMediaCommand(t *testing.T, executable string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, args...)
	var output, diagnostics bytes.Buffer
	command.Stdout, command.Stderr = &output, &diagnostics
	err := command.Run()
	require.NoError(t, err, "%s failed: %s", filepath.Base(executable), diagnostics.String())
	require.Empty(t, diagnostics.String(), "%s reported a media error", filepath.Base(executable))
	return output.Bytes()
}

func probeExport(t *testing.T, ffprobe, file string) exportProbe {
	t.Helper()
	b := exportMediaCommand(t, ffprobe, "-v", "error", "-count_frames", "-show_packets",
		"-show_entries", "stream=index,codec_name,codec_type,nb_read_frames,width,height:packet=stream_index,pts_time,dts_time,duration_time,data_hash,flags:format=duration",
		"-show_data_hash", "sha256", "-of", "json", file)
	var out exportProbe
	require.NoError(t, json.Unmarshal(b, &out), "the downloaded file must be independently demuxable")
	return out
}

func exportTime(t *testing.T, value string) float64 {
	t.Helper()
	n, err := strconv.ParseFloat(value, 64)
	require.NoError(t, err)
	require.False(t, math.IsNaN(n) || math.IsInf(n, 0))
	return n
}

func checkExportPlayback(t *testing.T, ffmpeg string, file exportedPlayback) {
	t.Helper()
	require.NotEmpty(t, file.Media.Streams)
	require.NotEmpty(t, file.Media.Packets)
	require.Greater(t, exportTime(t, file.Media.Format.Duration), float64(0))
	video := false
	for _, stream := range file.Media.Streams {
		count, err := strconv.Atoi(stream.Frames)
		require.NoError(t, err, "track %d (%s) must decode", stream.Index, stream.Codec)
		require.Positive(t, count, "track %d (%s) must contain decoded frames", stream.Index, stream.Codec)
		if stream.Kind == "video" {
			video = true
			require.Positive(t, stream.Width)
			require.Positive(t, stream.Height)
		}
	}
	require.True(t, video, "the completed export must contain playable video")
	exportMediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-xerror", "-i", file.File, "-map", "0", "-f", "null", "-")
}
