package mp4export

import (
	"context"
	"encoding/json"
	"fmt"
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
	Stream   int    `json:"stream_index"`
	PTS      string `json:"pts_time"`
	DTS      string `json:"dts_time"`
	Duration string `json:"duration_time"`
	Flags    string `json:"flags"`
	Hash     string `json:"data_hash"`
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
		"packet=stream_index,pts_time,dts_time,duration_time,flags,data_hash:stream=index,codec_name,codec_type:format=duration:format_tags=creation_time",
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

// compareDemux requires each track's packets to be a run of the input's,
// byte for byte, copies times over, every copy moved by one shift for all
// tracks, and decode times that only go on. It is the shift of each copy.
func compareDemux(t *testing.T, input, output demuxFile, copies int) []float64 {
	t.Helper()
	wants := streamPackets(input)
	gots := streamPackets(output)
	if len(gots) != len(wants) || len(output.Streams) != len(input.Streams) {
		t.Fatal("export changed the number of tracks")
	}
	var shifts []float64
	for _, tr := range input.Streams {
		want, got := wants[tr.Index], gots[tr.Index]
		n := len(got) / copies
		if n == 0 || len(got)%copies != 0 {
			t.Fatalf("track %d (%s): %d packets in %d copies", tr.Index, tr.Codec, len(got), copies)
		}
		k := -1
		for at := 0; at+n <= len(want) && k < 0; at++ {
			k = at
			for i := range n {
				if got[i].Hash == "" || got[i].Hash != want[at+i].Hash {
					k = -1
					break
				}
			}
		}
		if k < 0 {
			t.Fatalf("track %d (%s): its packets are not a run of the input's", tr.Index, tr.Codec)
		}
		for i, p := range got {
			original := want[k+i%n]
			if p.Hash != original.Hash {
				t.Fatalf("track %d packet %d payload changed", tr.Index, i)
			}
			shift := packetTime(t, p.DTS) - packetTime(t, original.DTS)
			if c := i / n; c == len(shifts) {
				shifts = append(shifts, shift)
			} else {
				// One tick of the coarsest track's clock.
				nearTime(t, fmt.Sprintf("track %d copy %d shift", tr.Index, c), shift, shifts[c], 0.0001)
			}
			nearTime(t, "PTS", packetTime(t, p.PTS)-packetTime(t, original.PTS), shift, 0.000002)
			if i > 0 && packetTime(t, p.DTS) <= packetTime(t, got[i-1].DTS) {
				t.Fatalf("track %d packet %d has non-increasing DTS", tr.Index, i)
			}
		}
	}
	return shifts
}

func writeMedia(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	file := filepath.Join(dir, name+".mp4")
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

// checkPlays has FFmpeg decode every stream, and ffprobe read the length and
// creation time the file says.
func checkPlays(t *testing.T, ffmpeg string, out demuxFile, file string, p *Plan, length float64) {
	t.Helper()
	nearTime(t, "file duration", packetTime(t, out.Format.Duration), length, 0.1)
	created, err := time.Parse(time.RFC3339Nano, out.Format.Tags.Created)
	if err != nil || !created.Equal(p.Start().Truncate(time.Second)) {
		t.Fatalf("creation time %q, want %s: %v", out.Format.Tags.Created, p.Start(), err)
	}
	mediaCommand(t, ffmpeg, "-nostdin", "-v", "error", "-xerror", "-i", file, "-map", "0", "-f", "null", "-")
}

func TestFFmpegDemuxAndDecode(t *testing.T) {
	ffprobe, ffmpeg := demuxTools(t)
	dir := t.TempDir()
	b := fixture(t, "av") // H.264 with reordered frames, AAC and Opus.
	original := demux(t, ffprobe, writeMedia(t, dir, "input", b))
	if len(original.Streams) != 3 {
		t.Fatal("the AV fixture must exercise both audio tracks")
	}
	end := ended(t, b)
	for _, tc := range []struct {
		name     string
		from, to time.Duration
		restart  time.Duration
		copies   int
		length   float64
	}{
		{"full", 0, time.Hour, 0, 1, 0},
		{"gap", 0, time.Hour, 10 * time.Second, 2, 10},
		// The fixture indexes every fragment; those at 0, 2 and 4 s begin with a keyframe.
		{"clip", 2100 * time.Millisecond, 4200 * time.Millisecond, 0, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := source(b, testStart)
			s.End = end
			sources := []Source{s}
			if tc.copies == 2 {
				s.Start, s.End = s.Start.Add(tc.restart), s.End.Add(tc.restart)
				sources = append(sources, s)
			}
			p := prepare(t, sources, testStart.Add(tc.from), testStart.Add(tc.to))
			file := writeMedia(t, dir, tc.name, written(t, p))
			out := demux(t, ffprobe, file)
			shifts := compareDemux(t, original, out, tc.copies)
			if tc.copies == 2 {
				nearTime(t, "restart", shifts[1]-shifts[0], tc.restart.Seconds(), 0.0001)
			}
			video := streamPackets(out)[0]
			if !strings.HasPrefix(video[0].Flags, "K") {
				t.Fatal("does not begin at a keyframe")
			}
			// To date_ended, or to the keyframe after the range's end.
			length := tc.length + end.Sub(p.Start()).Seconds()
			if tc.to < time.Hour {
				last := video[len(video)-1]
				length = packetTime(t, last.DTS) + packetTime(t, last.Duration) - packetTime(t, video[0].DTS)
			}
			checkPlays(t, ffmpeg, out, file, p, length)
		})
	}
}

// A file of FFmpeg's own fragmenting, with B-frames and without an index.
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
	s := source(b, testStart)
	s.End = ended(t, b)
	p := prepare(t, []Source{s}, testStart, testStart.Add(time.Hour))
	outFile := writeMedia(t, dir, "export", written(t, p))
	out := demux(t, ffprobe, outFile)
	compareDemux(t, original, out, 1)
	// The tracks' durations are the export's length, and ffprobe adds the
	// video's presentation lead to the file's.
	checkPlays(t, ffmpeg, out, outFile, p, packetTime(t, original.Format.Duration)+packetTime(t, video[0].PTS))
}
