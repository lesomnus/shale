// Package edge composes independent outbound Worker and Producer machine roles.
package edge

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/producer"
)

type Config struct {
	Producer      *producer.Config
	Worker        *cameraworker.Config
	Camera        CameraOptions
	Cache         *CacheOptions
	RetryInterval time.Duration
	Log           *slog.Logger
}

// CameraOptions contains transport/tool settings, never camera policy or
// credential material. Roots supplements the camera driver's TLS trust.
type CameraOptions struct {
	FFmpeg, FFprobe string
	Roots           *x509.CertPool
}

// CacheOptions explicitly enables a bounded streaming credential cache. It does
// not authorize a new capture, replace the catalog, or cache management access.
// Nil Config.Cache keeps caching off; a zero MaxEntries selects 32 entries.
type CacheOptions struct {
	MaxAge     time.Duration
	MaxEntries int
}

var ErrInvalidConfig = errors.New("invalid edge configuration")

type runner interface{ Run(context.Context) error }

type runtimeFactories struct {
	broker   func() *sourceBroker
	producer func(producer.Config, *sourceBroker, *CacheOptions) (runner, error)
	worker   func(cameraworker.Config, *sourceBroker, CameraOptions) (runner, error)
}

type rolePlan struct {
	name    string
	first   runner
	rebuild func() (runner, error)
}

// Run validates and constructs all enabled roles before starting their network
// loops. Constructors may initialize private machine keys/state; they do not
// dial CP or discover LAN devices. Every role owns its own attempt context and
// fresh instances. A recoverable return never cancels a healthy sibling.
// Validate checks the managed role selection, bounds and private-directory
// separation without constructing a host, creating keys or dialing a service.
func Validate(c Config) error {
	_, err := normalizedConfig(c)
	return err
}

func Run(ctx context.Context, c Config) error {
	return runWithFactories(ctx, c, runtimeFactories{broker: newSourceBroker, producer: buildProducer, worker: buildWorker})
}

func runWithFactories(ctx context.Context, c Config, factories runtimeFactories) error {
	c, err := normalizedConfig(c)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	broker := factories.broker()
	var plans []rolePlan
	if c.Producer != nil {
		base := *c.Producer
		build := func() (runner, error) { return factories.producer(base, broker, c.Cache) }
		first, err := build()
		if err != nil || first == nil {
			return invalidRole("producer")
		}
		plans = append(plans, rolePlan{name: "producer", first: first, rebuild: build})
	}
	if c.Worker != nil {
		base := *c.Worker
		build := func() (runner, error) { return factories.worker(base, broker, c.Camera) }
		first, err := build()
		if err != nil || first == nil {
			return invalidRole("worker")
		}
		plans = append(plans, rolePlan{name: "worker", first: first, rebuild: build})
	}
	var roles sync.WaitGroup
	for _, plan := range plans {
		roles.Go(func() { superviseRole(ctx, plan, c.RetryInterval, c.Log) })
	}
	roles.Wait()
	return nil
}

func superviseRole(ctx context.Context, plan rolePlan, retry time.Duration, log *slog.Logger) {
	current := plan.first
	for ctx.Err() == nil {
		if current != nil {
			attempt, cancel := context.WithCancel(ctx)
			err := current.Run(attempt)
			cancel()
			if ctx.Err() != nil {
				return
			}
			code := "run_failed"
			if err == nil {
				code = "run_ended"
			}
			log.WarnContext(ctx, "edge role retry", "role", plan.name, "code", code)
			current = nil
		}
		timer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		next, err := plan.rebuild()
		if err != nil || next == nil {
			log.WarnContext(ctx, "edge role retry", "role", plan.name, "code", "construct_failed")
			continue
		}
		current = next
	}
}

func invalidRole(role string) error {
	// Drop constructor errors: third-party errors may contain URLs or material.
	return fmt.Errorf("%w: %s", ErrInvalidConfig, role)
}

func normalizedConfig(c Config) (Config, error) {
	if c.Producer == nil && c.Worker == nil || c.RetryInterval < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.Cache != nil {
		cache := *c.Cache
		if c.Producer == nil || cache.MaxAge < time.Second || cache.MaxAge > 5*time.Minute || cache.MaxEntries < 0 || cache.MaxEntries > 64 {
			return Config{}, ErrInvalidConfig
		}
		if cache.MaxEntries == 0 {
			cache.MaxEntries = 32
		}
		c.Cache = &cache
	}
	var producerDir, workerDir string
	var err error
	if c.Producer != nil {
		value := *c.Producer
		if value.Cp == "" || value.HeartbeatInterval < 0 {
			return Config{}, invalidRole("producer")
		}
		producerDir, err = privateStatePath(value.StateDir)
		if err != nil {
			return Config{}, invalidRole("producer")
		}
		c.Producer = &value
	}
	if c.Worker != nil {
		value := *c.Worker
		if value.Cp == "" {
			return Config{}, invalidRole("worker")
		}
		workerDir, err = privateStatePath(value.StateDir)
		if err != nil {
			return Config{}, invalidRole("worker")
		}
		c.Worker = &value
	}
	if producerDir != "" && producerDir == workerDir {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

// Resolve the closest existing parent too: two different, not-yet-created leaf
// paths can still name the same directory through an existing symlink. A
// dangling or cyclic existing link is a configuration error, not a new root.
func privateStatePath(path string) (string, error) {
	if path == "" {
		return "", ErrInvalidConfig
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", ErrInvalidConfig
	}
	parent := filepath.Clean(abs)
	var missing []string
	for {
		_, err := os.Lstat(parent)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(parent)
			if err != nil {
				return "", ErrInvalidConfig
			}
			info, err := os.Stat(resolved)
			if err != nil || !info.IsDir() {
				return "", ErrInvalidConfig
			}
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(err) || filepath.Dir(parent) == parent {
			return "", ErrInvalidConfig
		}
		missing = append(missing, filepath.Base(parent))
		parent = filepath.Dir(parent)
	}
}
