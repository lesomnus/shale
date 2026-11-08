package cli

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"os"
	"strings"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/edge"
)

// EdgeConfig maps only the enabled managed roles. Standalone ProducerConfig
// retains its explicit static/file compatibility and never selects this path.
func EdgeConfig(c *cmd.Config) (edge.Config, error) { return edgeConfig(c, nil) }

func edgeConfig(c *cmd.Config, override []string) (edge.Config, error) {
	if c == nil {
		return edge.Config{}, edge.ErrInvalidConfig
	}
	roles := c.Edge.Roles
	if override != nil {
		roles = override
	}
	if roles == nil {
		roles = []string{"worker", "producer"}
	}
	if len(roles) == 0 {
		return edge.Config{}, errors.New("edge.roles must enable worker and/or producer")
	}
	selected := make(map[string]bool, len(roles))
	for _, role := range roles {
		if role != "worker" && role != "producer" || selected[role] {
			return edge.Config{}, errors.New("edge.roles requires distinct worker and/or producer values")
		}
		selected[role] = true
	}
	cfg := edge.Config{RetryInterval: c.Edge.RetryInterval, Log: slog.Default()}
	if c.Edge.StreamingCache != nil {
		cfg.Cache = &edge.CacheOptions{MaxAge: c.Edge.StreamingCache.MaxAge, MaxEntries: c.Edge.StreamingCache.MaxEntries}
	}
	if selected["producer"] {
		pc := c.Producer
		catalog := strings.ToLower(pc.Catalog.Provider)
		if len(pc.Sources) != 0 || pc.Demo != 0 || pc.Push != "" || pc.PushIdle != 0 || pc.Set != "" || pc.Credentials.Directory != "" || (catalog != "" && catalog != "cp") || (pc.Credentials.Provider != "" && pc.Credentials.Provider != "cp") {
			return edge.Config{}, errors.New("managed edge Producer requires only CP catalog and credentials")
		}
		if pc.Catalog.Assignment != "" {
			id, err := pdid.Parse(pc.Catalog.Assignment)
			if err != nil || id.Domain() != 28 {
				return edge.Config{}, errors.New("managed edge assignment selector is invalid")
			}
		}
		copy := *c
		copy.Producer.Catalog.Provider = "cp"
		copy.Producer.Credentials.Provider = "cp"
		base, err := ProducerConfig(&copy)
		if err != nil {
			return edge.Config{}, errors.New("invalid managed edge Producer settings")
		}
		// The composition root installs the machine-bound central provider and
		// optional cache after its private host key is ready. No local fallback.
		base.AuthProviderFactory = nil
		cfg.Producer = &base
	}
	if selected["worker"] {
		base := CameraWorkerConfig(c)
		if base.Cp == "" && c.IsDev() {
			base.Cp = c.Client.Addr
		}
		cfg.Worker = &base
		cfg.Camera.FFmpeg, cfg.Camera.FFprobe = c.CameraWorker.Ffmpeg, c.CameraWorker.Ffprobe
		if c.CameraWorker.CameraCAFile != "" {
			roots, err := cameraRoots(c.CameraWorker.CameraCAFile)
			if err != nil {
				return edge.Config{}, err
			}
			cfg.Camera.Roots = roots
		}
	}
	if err := edge.Validate(cfg); err != nil {
		return edge.Config{}, err
	}
	return cfg, nil
}

func cameraRoots(path string) (*x509.CertPool, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("camera CA file cannot be read")
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(body) {
		return nil, errors.New("camera CA file contains no trusted certificate")
	}
	return roots, nil
}

func serveEdge(ctx context.Context, c *cmd.Config, roles []string) error {
	configuration, err := edgeConfig(c, roles)
	if err != nil {
		return err
	}
	ctx, done, err := Telemetry(ctx, c)
	if err != nil {
		return err
	}
	defer done()
	return edge.Run(ctx, configuration)
}
