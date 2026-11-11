package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/producer"
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
	Position string `json:"pos"`
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

func probeExport(t *testing.T, ffprobe, file string, options ...string) exportProbe {
	t.Helper()
	args := append([]string{"-v", "error", "-count_frames", "-show_packets"}, options...)
	args = append(args, "-show_entries", "stream=index,codec_name,codec_type,nb_read_frames,width,height:packet=stream_index,pts_time,dts_time,duration_time,pos,data_hash,flags:format=duration",
		"-show_data_hash", "sha256", "-of", "json", file)
	b := exportMediaCommand(t, ffprobe, args...)
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

func videoExportPackets(t *testing.T, media exportProbe) (exportStream, []exportPacket) {
	t.Helper()
	for _, stream := range media.Streams {
		if stream.Kind != "video" {
			continue
		}
		var packets []exportPacket
		for _, packet := range media.Packets {
			if packet.Stream == stream.Index {
				packets = append(packets, packet)
			}
		}
		require.NotEmpty(t, packets)
		return stream, packets
	}
	t.Fatal("the independent demuxer found no video track")
	return exportStream{}, nil
}

// Mirror the producer's two-stage RTSP/remux path without using camera footage:
// H.264 with B-frames and AAC in TS, then its real delay_moov muxer arguments.
func remuxExportFixture(t *testing.T, ffprobe, ffmpeg string) ([]byte, exportProbe) {
	t.Helper()
	encoders := exportMediaCommand(t, ffmpeg, "-hide_banner", "-v", "error", "-encoders")
	if !strings.Contains(string(encoders), "libx264") {
		t.Skip("libx264 is not available for the synthetic producer fixture")
	}
	dir := t.TempDir()
	ts := filepath.Join(dir, "capture.ts")
	exportMediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=10",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
		"-t", "6", "-c:v", "libx264", "-threads", "1", "-profile:v", "main", "-pix_fmt", "yuv420p",
		"-g", "20", "-keyint_min", "20", "-sc_threshold", "0", "-bf", "2",
		"-c:a", "aac", "-f", "mpegts", ts)
	file := filepath.Join(dir, "remux.mp4")
	args := []string{"-nostdin", "-v", "error", "-y", "-f", "mpegts", "-i", ts,
		"-map", "0", "-c", "copy", "-bsf:a:0", "aac_adtstoasc"}
	args = append(args, producer.RemuxMuxArgs(200*time.Millisecond)...)
	exportMediaCommand(t, ffmpeg, append(args, file)...)
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	b = indexLamina(t, b)
	moov, found := fmp4.Find(b, "moov")
	require.True(t, found)
	tracks, err := fmp4.Children(moov.Payload(b))
	require.NoError(t, err)
	edits := 0
	for _, track := range tracks {
		if track.Type == "trak" {
			if _, found := fmp4.Find(track.Payload(moov.Payload(b)), "edts"); found {
				edits++
			}
		}
	}
	require.Positive(t, edits, "the producer-style fixture must exercise input edit lists")
	// Source.Start is the first video keyframe's decode wall clock. Ignore
	// the input movie's edit list only for this raw-clock reference; probe
	// the exported file normally, exactly as a player interprets its edits.
	media := probeExport(t, ffprobe, file, "-ignore_editlist", "1")
	require.Len(t, media.Streams, 2, "the fixture contains H.264 and AAC")
	require.Equal(t, "h264", media.Streams[0].Codec)
	require.Equal(t, "aac", media.Streams[1].Codec)
	_, video := videoExportPackets(t, media)
	require.Len(t, video, 60)
	require.Greater(t, exportTime(t, video[0].PTS)-exportTime(t, video[0].DTS), 0.15, "the fixture must exercise B-frame presentation lead")
	boxes, err := fmp4.Children(b)
	require.NoError(t, err)
	var fragmentStarts []int64
	for _, box := range boxes {
		if box.Type == "mdat" {
			fragmentStarts = append(fragmentStarts, int64(box.Off+box.Hdr))
		}
	}
	for _, i := range []int{0, 20, 40} {
		require.Contains(t, video[i].Flags, "K", "the synthetic GOP boundaries must be stable")
		require.Contains(t, fragmentStarts, exportPosition(t, video[i]), "video byte cuts must include every track in the selected fragment")
	}
	return b, media
}

// indexLamina ends a recording with the index a producer ends a lamina with,
// in place of FFmpeg's: where each fragment that begins with a keyframe is,
// and its decode time.
func indexLamina(t *testing.T, b []byte) []byte {
	t.Helper()
	boxes, err := fmp4.Children(b)
	require.NoError(t, err)
	var init *fmp4.Init
	var keys []fmp4.KeyAt
	for i, box := range boxes {
		if box.Type == "mfra" {
			require.Equal(t, len(b), box.End, "FFmpeg's index ends the file")
			b = b[:box.Off]
			break
		}
		if box.Type != "moof" {
			continue
		}
		if init == nil {
			init, err = fmp4.ParseInit(b[:box.Off])
			require.NoError(t, err)
		}
		end := box.End
		if i+1 < len(boxes) && boxes[i+1].Type == "mdat" {
			end = boxes[i+1].End
		}
		f, err := fmp4.ParseFragment(b[box.Off:end], init)
		require.NoError(t, err)
		if f.Key(init) {
			keys = append(keys, fmp4.KeyAt{Time: f.Video(init).Time, Offset: int64(box.Off)})
		}
	}
	require.NotEmpty(t, keys)
	return append(b, fmp4.Mfra(init.Video().ID, keys)...)
}

func exportPosition(t *testing.T, packet exportPacket) int64 {
	t.Helper()
	n, err := strconv.ParseInt(packet.Position, 10, 64)
	require.NoError(t, err)
	return n
}

func selectedExportPackets(t *testing.T, input exportProbe, from, to int64) map[int][]exportPacket {
	t.Helper()
	selected := map[int][]exportPacket{}
	for _, packet := range input.Packets {
		position := exportPosition(t, packet)
		if position >= from && position < to {
			selected[packet.Stream] = append(selected[packet.Stream], packet)
		}
	}
	return selected
}

func TestNativeExportPlaybackAcrossRemuxRestarts(t *testing.T) {
	ffprobe, ffmpeg := exportPlaybackTools(t)
	body, original := remuxExportFixture(t, ffprobe, ffmpeg)
	c := start(t, func(c *cmd.Config) { c.Export.Enabled = true })
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	src := exportSource(t, admin)
	laminae := api.NewLaminaServiceClient(admin)
	start := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, offset := range []time.Duration{0, 10 * time.Second} {
		at := start.Add(offset)
		allocation, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
			Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(at),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, put(t, allocation, body, at.Add(6*time.Second)))
		require.Eventually(t, func() bool {
			row, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: allocation.GetLaminaId()}.Build()}.Build())
			return err == nil && row.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
		}, 10*time.Second, 100*time.Millisecond, "the independently restarted recording committed")
	}
	req := api.LaminaExportRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
		From:   timestamppb.New(start.Add(2500 * time.Millisecond)),
		To:     timestamppb.New(start.Add(13200 * time.Millisecond)),
	}.Build()
	out, err := api.NewLaminaServiceClient(exportReader(t, c, admin)).Export(ctx, req)
	require.NoError(t, err)
	download := exportDownload(t, exportHTTPBase(t, c, cmd.SurfaceTenant), out)
	stream, actual := videoExportPackets(t, download.Media)
	_, input := videoExportPackets(t, original)
	// A start inside GOP 2-4 includes 2-6 from the first recording; an end
	// inside GOP 12-14 includes 10-14 from the restarted recording. Packet
	// byte positions select both tracks by fragment membership, rather than
	// incorrectly dropping AAC which precedes its fragment's video timestamp.
	first := selectedExportPackets(t, original, exportPosition(t, input[20]), int64(len(body)))
	second := selectedExportPackets(t, original, exportPosition(t, input[0]), exportPosition(t, input[40]))
	origin := math.Inf(1)
	for _, packets := range first {
		require.NotEmpty(t, packets)
		origin = min(origin, exportTime(t, packets[0].DTS))
	}
	require.Len(t, download.Media.Streams, len(original.Streams), "both H.264 and AAC tracks must survive the API/HTTP export")
	for i, sourceStream := range original.Streams {
		outputStream := download.Media.Streams[i]
		require.Equal(t, sourceStream.Codec, outputStream.Codec)
		require.Equal(t, sourceStream.Kind, outputStream.Kind)
		var output []exportPacket
		for _, packet := range download.Media.Packets {
			if packet.Stream == outputStream.Index {
				output = append(output, packet)
			}
		}
		blocks := [][]exportPacket{first[sourceStream.Index], second[sourceStream.Index]}
		require.NotEmpty(t, blocks[0])
		require.NotEmpty(t, blocks[1])
		require.Len(t, output, len(blocks[0])+len(blocks[1]), "%s packet count changed", sourceStream.Codec)
		shift := -origin
		if sourceStream.Kind == "video" {
			// Applying a movie edit to reordered video can place its first
			// decode timestamp before zero. Bound that initial adjustment by
			// the input's presentation lead, then use ONE track-wide anchor:
			// a separate anchor at each restart would hide a misplaced epoch.
			first := blocks[0][0]
			lead := exportTime(t, first.PTS) - exportTime(t, first.DTS)
			shift = exportTime(t, output[0].DTS) - exportTime(t, first.DTS)
			require.InDelta(t, -origin, shift, lead+0.000002, "the initial video edit must stay within its B-frame presentation lead")
			require.GreaterOrEqual(t, exportTime(t, output[0].PTS), float64(0), "the first selected video frame must be presentable")
		}
		at := 0
		for block, packets := range blocks {
			for _, packet := range packets {
				got := output[at]
				require.Equal(t, packet.Hash, got.Hash, "%s packet %d changed, repeated or moved", sourceStream.Codec, at)
				// Allow rounding of the movie clock and ffprobe's decimal times.
				placement := float64(block)*10 + shift
				require.InDelta(t, exportTime(t, packet.DTS)+placement, exportTime(t, got.DTS), 0.000002, "%s packet %d decode time", sourceStream.Codec, at)
				require.InDelta(t, exportTime(t, packet.PTS)+placement, exportTime(t, got.PTS), 0.000002, "%s packet %d presentation time", sourceStream.Codec, at)
				if at > 0 {
					require.Greater(t, exportTime(t, got.DTS), exportTime(t, output[at-1].DTS), "%s decode clock overlapped at the restart", sourceStream.Codec)
				}
				at++
			}
		}
	}
	decoded, err := strconv.Atoi(stream.Frames)
	require.NoError(t, err)
	require.Equal(t, 80, decoded, "the complete HTTP file must decode each selected frame exactly once")
	require.InDelta(t, 8, exportTime(t, actual[40].DTS)-exportTime(t, actual[0].DTS), 0.000002, "the encoder restart must be placed on wall-clock time")
	duration := exportTime(t, download.Media.Format.Duration)
	require.GreaterOrEqual(t, duration, float64(12))
	require.Less(t, duration, 12.5)
	t.Run("a download cut short still opens", func(t *testing.T) {
		complete, err := os.ReadFile(download.File)
		require.NoError(t, err)
		truncated := filepath.Join(t.TempDir(), "truncated.mp4")
		require.NoError(t, os.WriteFile(truncated, complete[:len(complete)/2], 0600))
		// The init segment comes first, so what arrived plays up to its last
		// whole fragment; the browser still reports the download failed,
		// short of its Content-Length.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, ffprobe, "-v", "quiet", "-select_streams", "v", "-count_frames",
			"-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", truncated).Output()
		require.NoError(t, err)
		frames, err := strconv.Atoi(strings.TrimSpace(string(out)))
		require.NoError(t, err)
		require.Greater(t, frames, 20, "the first half's frames decode")
	})
}
