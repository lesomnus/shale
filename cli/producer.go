package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"

	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
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
	t.Add("producer/push", &xli.Command{
		Name:  "push",
		Brief: "write a stream to a pushed source of the producer on this host (§38.9)",
		Flags: flg.Flags{
			&flg.String{Name: "to", Brief: "the producer's listener, unix:/path or tcp://host:port (producer.push)"},
			&flg.String{Name: "part", Brief: "bytes per request (4MiB)"},
			&flg.Switch{Name: "open", Brief: "leave the stream open at the end instead of completing it"},
		},
		Args: arg.Args{
			&arg.String{Name: "ALIAS", Brief: "the pushed source"},
			&arg.String{Name: "FILE", Brief: "the stream; stdin when absent", Optional: true},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			alias, _ := arg.Get[string](self, "ALIAS")
			to, _ := flg.Find[string](self, "to")
			if to == "" {
				to = c.Producer.Push
			}
			if to == "" {
				return errors.New("--to: the producer's listener, or producer.push in the configuration")
			}
			p := &producer.Pusher{Addr: to, Alias: alias, Log: slog.Default()}
			if v, ok := flg.Find[string](self, "part"); ok && v != "" {
				n, err := storage.ParseCapacity(v)
				if err != nil {
					return err
				}
				p.Part = int(n)
			}
			open, _ := flg.Find[bool](self, "open")
			off, _, err := p.Offset(ctx)
			if err != nil {
				return err
			}
			var in io.Reader = os.Stdin
			if path, ok := arg.Get[string](self, "FILE"); ok && path != "" {
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer f.Close()
				if _, err := f.Seek(off, io.SeekStart); err != nil {
					return err
				}
				in = f
			} else if off != 0 {
				return fmt.Errorf("the producer holds %d bytes of this stream already; give the file to resume, or complete the stream first", off)
			}
			end, err := p.Push(ctx, in, off, !open)
			if err != nil {
				return err
			}
			self.Printf("%s: at %d\n", alias, end)

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
