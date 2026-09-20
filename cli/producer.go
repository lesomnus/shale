package cli

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"

	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
)

// The producer's own commands (§38.4): `scan` prints what this host can
// see as a configuration skeleton, `probe` runs the configured sources for
// a while and reports what they sustain. They are mounted under `producer`
// beside the generated verbs.
func addProducerCommands(t *pdcmd.Tree, c *cmd.Config) {
	t.Add("producer/scan", &xli.Command{
		Name:  "scan",
		Brief: "what this host can see: cameras, ONVIF devices, audio, encoders",
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			ffmpeg := c.Producer.Ffmpeg
			if ffmpeg == "" {
				ffmpeg = "ffmpeg"
			}
			producer.ScanHost(ctx, ffmpeg).WriteSkeleton(self)

			return nil
		}),
	})
	t.Add("producer/probe", &xli.Command{
		Name:  "probe",
		Brief: "run the configured sources for a while and report what they sustain",
		Flags: flg.Flags{
			&flg.Duration{Name: "for", Brief: "how long to run each source (30s)"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			d, ok := flg.Find[time.Duration](self, "for")
			if !ok || d <= 0 {
				d = 30 * time.Second
			}
			cfg, err := ProducerConfig(c)
			if err != nil {
				return err
			}
			if len(cfg.Sources) == 0 {
				return fmt.Errorf("no sources configured; run `shale producer scan` first")
			}
			fmt.Fprintf(self, "%-12s %-14s %8s %12s %10s %6s %6s  %s\n", "SOURCE", "ENCODER", "FPS", "BITRATE", "KEYFRAME", "CPU", "TEMP", "LAST STDERR")
			for _, r := range producer.Probe(ctx, cfg.Ffmpeg, cfg.Sources, d, slog.Default()) {
				fmt.Fprintf(self, "%-12s %-14s %8.1f %12d %10s %6.2f %6.1f  %s\n", r.Alias, r.Encoder, r.FrameRate, r.Bitrate, r.Keyframe.Round(time.Millisecond), r.Cpu, r.Temp, r.Err)
			}

			return nil
		}),
	})
}
