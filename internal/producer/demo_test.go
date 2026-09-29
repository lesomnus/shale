package producer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDemoSources is what `--demo 4` hands a producer: numbered aliases,
// the patterns in turn, and the first one again once they run out.
func TestDemoSources(t *testing.T) {
	srcs := DemoSources(4)
	require.Len(t, srcs, 4)

	aliases := make([]string, len(srcs))
	for i, s := range srcs {
		aliases[i] = s.Alias
		require.Equal(t, DemoSize, s.Size)
		require.Equal(t, DemoFps, s.Fps)
		require.True(t, isDemo(s.Input), "%s: %s", s.Alias, s.Input)
		p, err := demoOf(s.Input)
		require.NoError(t, err)
		require.Equal(t, DemoPatterns[i%len(DemoPatterns)], p)
	}
	require.Equal(t, []string{"demo-01", "demo-02", "demo-03", "demo-04"}, aliases)
	require.Equal(t, srcs[0].Input, srcs[3].Input, "the patterns go round")
	require.Empty(t, DemoSources(0))
}

// TestDemoOf is the pattern a `demo:` input names, and the refusal of one
// this build does not draw.
func TestDemoOf(t *testing.T) {
	p, err := demoOf("demo:")
	require.NoError(t, err)
	require.Equal(t, DemoPatterns[0], p)

	p, err = demoOf("demo:cellauto")
	require.NoError(t, err)
	require.Equal(t, "cellauto", p.Pattern)
	require.Equal(t, "cellauto=size=640x480:rate=15", p.Filter("640x480", 15))

	_, err = demoOf("demo:rickroll")
	require.ErrorContains(t, err, "not a pattern this build draws")
	require.ErrorContains(t, err, "testsrc2", "and it says which ones are")
}

// TestArgsDemo is the capture of a demo source (§38.1): the picture and
// the tone are two inputs ffmpeg makes for itself, paced to the clock,
// and the tone is encoded and mapped as a microphone's is.
func TestArgsDemo(t *testing.T) {
	cases := []struct {
		name string
		cfg  SourceConfig
		want []string
		not  []string
	}{
		{"the picture and the tone", DemoSources(1)[0],
			[]string{
				"-re -f lavfi -i testsrc2=size=1280x720:rate=30",
				"-f lavfi -i sine=frequency=440:sample_rate=48000",
				"-map 0:v:0 -map 1:a:0", "-c:a aac -b:a 64000", "-c:v libx264",
			},
			[]string{"-an", "-c:a copy"}},
		{"the second pattern has a tone of its own", DemoSources(2)[1],
			[]string{"-i cellauto=size=1280x720:rate=30", "sine=frequency=554:"}, nil},
		{"a pattern's own settings come after the size and the rate", DemoSources(3)[2],
			[]string{"-i life=size=1280x720:rate=30:mold=10:", "sine=frequency=659:"}, nil},
		{"`demo:` alone is the first pattern", SourceConfig{Alias: "d", Input: "demo:"},
			[]string{"-i testsrc2=size=1280x720:rate=30"}, nil},
		{"a size and a rate of its own", SourceConfig{Alias: "d", Input: "demo:", Size: "640x480", Fps: 15},
			[]string{"-i testsrc2=size=640x480:rate=15"}, nil},
		{"as Opus", SourceConfig{Alias: "d", Input: "demo:", Audio: &AudioConfig{Codec: "opus", Bitrate: 48000}},
			[]string{"-c:a libopus -b:a 48000", "-map 0:v:0 -map 1:a:0"}, []string{"-an"}},
		{"silent", SourceConfig{Alias: "d", Input: "demo:", Audio: &AudioConfig{Codec: "none"}},
			[]string{"-an"}, []string{"sine=", "-c:a"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := strings.Join(Args(c.cfg, "libx264", 2_000_000, 2*time.Second), " ")
			for _, w := range c.want {
				require.Contains(t, args, w)
			}
			for _, n := range c.not {
				require.NotContains(t, args, n)
			}
		})
	}
}

// TestDemoRefused is what a producer says before it starts: a pattern it
// cannot draw, and sound it cannot copy from a stream nobody sends.
func TestDemoRefused(t *testing.T) {
	_, err := New(Config{
		StateDir: t.TempDir(), Cp: "http://127.0.0.1:1",
		Sources: []SourceConfig{{Alias: "bad", Input: "demo:nosuch"}},
	})
	require.ErrorContains(t, err, "source bad")
	require.ErrorContains(t, err, "not a pattern this build draws")

	_, err = New(Config{
		StateDir: t.TempDir(), Cp: "http://127.0.0.1:1",
		Sources: []SourceConfig{{Alias: "copied", Input: "demo:", Audio: &AudioConfig{Codec: "copy"}}},
	})
	require.ErrorContains(t, err, "aac, opus or none")

	p, err := New(Config{
		StateDir: t.TempDir(), Cp: "http://127.0.0.1:1",
		Sources: DemoSources(3),
	})
	require.NoError(t, err)
	require.NotNil(t, p)
}
