package mp4export

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These checks use an independent demuxer and decoder. Walking our own sample
// tables cannot catch a valid-looking edit list that a player interprets by
// repeating samples or shifting only the video presentation clock.
// CI installs FFmpeg; local environments without it keep the pure Go tests.
func demuxTools(t *testing.T) (ffprobe, ffmpeg string) {
	t.Helper()
	var err error
	ffprobe, err = exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is not installed")
	}
	ffmpeg, err = exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	return
}

type demuxPacket struct {
	Stream int    `json:"stream_index"`
	PTS    string `json:"pts_time"`
	DTS    string `json:"dts_time"`
	Hash   string `json:"data_hash"`
}

type demuxFile struct {
	Packets []demuxPacket `json:"packets"`
	Streams []struct {
		Index int    `json:"index"`
		Codec string `json:"codec_name"`
		Kind  string `json:"codec_type"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
		Tags     struct {
			Created string `json:"creation_time"`
		} `json:"tags"`
	} `json:"format"`
}

func mediaCommand(t *testing.T, executable string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, executable, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", filepath.Base(executable), err, out)
	}
	return out
}

func demux(t *testing.T, ffprobe, file string) demuxFile {
	t.Helper()
	b := mediaCommand(t, ffprobe, "-v", "error", "-show_entries",
		"packet=stream_index,pts_time,dts_time,data_hash:stream=index,codec_name,codec_type:format=duration:format_tags=creation_time",
		"-show_data_hash", "sha256", "-of", "json", file)
	var out demuxFile
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("ffprobe JSON: %v\n%s", err, b)
	}
	return out
}

func packetTime(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		t.Fatalf("invalid packet time %q: %v", s, err)
	}
	return v
}

func nearTime(t *testing.T, what string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.6f, want %.6f", what, got, want)
	}
}

func streamPackets(file demuxFile) map[int][]demuxPacket {
	out := make(map[int][]demuxPacket)
	for _, p := range file.Packets {
		out[p.Stream] = append(out[p.Stream], p)
	}
	return out
}

// compareDemux requires byte-for-byte sample preservation and the same
// presentation/decode timestamps, with only the requested wall-clock placement.
func compareDemux(t *testing.T, input, output demuxFile, clip float64, copies int, restart float64) {
	t.Helper()
	wants := streamPackets(input)
	gots := streamPackets(output)
	if len(gots) != len(wants) || len(output.Streams) != len(input.Streams) {
		t.Fatal("export changed the number of tracks")
	}
	for _, tr := range input.Streams {
		want := wants[tr.Index]
		if clip > 0 {
			first := 0
			for first < len(want) && packetTime(t, want[first].DTS) < clip-0.000001 {
				first++
			}
			want = want[first:]
		}
		got := gots[tr.Index]
		if len(want) == 0 || len(got) != len(want)*copies {
			t.Fatalf("track %d (%s): %d packets, want %d", tr.Index, tr.Codec, len(got), len(want)*copies)
		}
		for i, p := range got {
			original := want[i%len(want)]
			if p.Hash == "" || p.Hash != original.Hash {
				t.Fatalf("track %d packet %d payload changed", tr.Index, i)
			}
			shift := float64(i/len(want))*restart - clip
			nearTime(t, "PTS", packetTime(t, p.PTS), packetTime(t, original.PTS)+shift, 0.000002)
			nearTime(t, "DTS", packetTime(t, p.DTS), packetTime(t, original.DTS)+shift, 0.000002)
			if i > 0 && packetTime(t, p.DTS) <= packetTime(t, got[i-1].DTS) {
				t.Fatalf("track %d packet %d has non-increasing DTS", tr.Index, i)
			}
		}
	}
}

func writeMedia(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	file := filepath.Join(dir, name+".mp4")
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestFFmpegDemuxAndDecode(t *testing.T) {
	ffprobe, ffmpeg := demuxTools(t)
	dir := t.TempDir()
	b := fixture(t, "av") // H.264 with reordered frames, AAC and Opus.
	original := demux(t, ffprobe, writeMedia(t, dir, "input", b))
	if len(original.Streams) != 3 {
		t.Fatal("the AV fixture must exercise both audio tracks")
	}
	for _, tc := range []struct {
		name               string
		clip, end, restart float64
		copies             int
		wantDuration       float64
	}{
		{"full", 0, 3600, 0, 1, 6.066667},
		{"gap", 0, 3600, 10, 2, 16.066667},
		{"clip", 2, 4.2, 0, 1, 4.066667},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sources := []Source{source(b, testStart)}
			if tc.copies == 2 {
				sources = append(sources, source(b, testStart.Add(time.Duration(tc.restart*float64(time.Second)))))
			}
			from := testStart
			if tc.clip > 0 {
				from = testStart.Add(2500 * time.Millisecond) // preceding keyframe is 2 s.
			}
			plan := prepare(t, sources, from, testStart.Add(time.Duration(tc.end*float64(time.Second))))
			file := writeMedia(t, dir, tc.name, writePlan(t, plan))
			out := demux(t, ffprobe, file)
			compareDemux(t, original, out, tc.clip, tc.copies, tc.restart)
			nearTime(t, "file duration", packetTime(t, out.Format.Duration), tc.wantDuration, 0.002)
			created, err := time.Parse(time.RFC3339Nano, out.Format.Tags.Created)
			if err != nil || !created.Equal(testStart.Add(time.Duration(tc.clip*float64(time.Second)))) {
				t.Fatalf("creation time %q: %v", out.Format.Tags.Created, err)
			}
			mediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-xerror", "-i", file, "-map", "0", "-f", "null", "-")
		})
	}
}

func TestFFmpegBFramePresentation(t *testing.T) {
	ffprobe, ffmpeg := demuxTools(t)
	if !strings.Contains(string(mediaCommand(t, ffmpeg, "-hide_banner", "-encoders")), "libx264") {
		t.Skip("ffmpeg has no libx264 encoder for the generated B-frame fixture")
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "bframes.mp4")
	mediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=10",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
		"-t", "6", "-c:v", "libx264", "-profile:v", "main", "-pix_fmt", "yuv420p", "-g", "10", "-bf", "2",
		"-c:a", "aac", "-movflags", "+frag_keyframe+empty_moov+default_base_moof", file)
	original := demux(t, ffprobe, file)
	video := streamPackets(original)[0]
	if len(video) != 60 || packetTime(t, video[0].PTS)-packetTime(t, video[0].DTS) < 0.15 {
		t.Fatal("fixture must have a nonzero video presentation lead")
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	p := prepare(t, []Source{source(b, testStart)}, testStart, testStart.Add(time.Hour))
	outFile := writeMedia(t, dir, "export", writePlan(t, p))
	out := demux(t, ffprobe, outFile)
	compareDemux(t, original, out, 0, 1, 0)
	nearTime(t, "B-frame duration", packetTime(t, out.Format.Duration), packetTime(t, original.Format.Duration), 0.002)
	mediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-xerror", "-i", outFile, "-map", "0", "-f", "null", "-")
}
