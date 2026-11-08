package edge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/stretchr/testify/require"
)

type runtimeRunnerFunc func(context.Context) error

func (f runtimeRunnerFunc) Run(ctx context.Context) error { return f(ctx) }

type runtimeSafeLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *runtimeSafeLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *runtimeSafeLog) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

func runtimeTestConfig(t *testing.T) Config {
	t.Helper()
	state := t.TempDir()
	return Config{
		Producer:      &producer.Config{StateDir: filepath.Join(state, "producer"), Cp: "https://cp.invalid"},
		Worker:        &cameraworker.Config{StateDir: filepath.Join(state, "worker"), Cp: "https://cp.invalid"},
		RetryInterval: time.Millisecond,
		Log:           slog.New(slog.DiscardHandler),
	}
}

func runtimeWaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("role did not reach its expected state")
	}
}

func TestSupervisorRetriesOnlyFailedRoleWithFreshInstance(t *testing.T) {
	c := runtimeTestConfig(t)
	logs := &runtimeSafeLog{}
	c.Log = slog.New(slog.NewJSONHandler(logs, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	healthyStarted, healthyStopped := make(chan struct{}), make(chan struct{})
	restarted := make(chan struct{})
	var producerBuilds, workerBuilds atomic.Int32
	var firstContext context.Context
	factories := runtimeFactories{
		broker: newSourceBroker,
		producer: func(producer.Config, *sourceBroker, *CacheOptions) (runner, error) {
			producerBuilds.Add(1)
			return runtimeRunnerFunc(func(ctx context.Context) error {
				close(healthyStarted)
				<-ctx.Done()
				close(healthyStopped)
				return ctx.Err()
			}), nil
		},
		worker: func(cameraworker.Config, *sourceBroker, CameraOptions) (runner, error) {
			switch workerBuilds.Add(1) {
			case 1:
				return runtimeRunnerFunc(func(ctx context.Context) error {
					firstContext = ctx
					<-healthyStarted
					return errors.New("fixture-secret rtsp://user:private@camera.invalid/live")
				}), nil
			case 2:
				return nil, errors.New("fixture-secret constructor URL")
			default:
				return runtimeRunnerFunc(func(ctx context.Context) error { close(restarted); <-ctx.Done(); return ctx.Err() }), nil
			}
		},
	}
	done := make(chan error, 1)
	go func() { done <- runWithFactories(ctx, c, factories) }()
	runtimeWaitSignal(t, restarted)
	require.Equal(t, int32(1), producerBuilds.Load())
	require.Equal(t, int32(3), workerBuilds.Load())
	require.ErrorIs(t, firstContext.Err(), context.Canceled, "an ended attempt's context is closed before its replacement")
	select {
	case <-healthyStopped:
		t.Fatal("a worker failure canceled the Producer")
	default:
	}
	cancel()
	runtimeWaitSignal(t, healthyStopped)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not join both roles")
	}
	output := logs.text()
	require.Contains(t, output, `"role":"worker"`)
	require.Contains(t, output, `"code":"run_failed"`)
	require.Contains(t, output, `"code":"construct_failed"`)
	for _, forbidden := range []string{"fixture-secret", "rtsp://", "private", "cp.invalid"} {
		require.NotContains(t, output, forbidden)
	}
}

func TestSupervisorConstructsBothRolesBeforeAnyNetworkLoop(t *testing.T) {
	c := runtimeTestConfig(t)
	var started atomic.Bool
	err := runWithFactories(t.Context(), c, runtimeFactories{
		broker: newSourceBroker,
		producer: func(producer.Config, *sourceBroker, *CacheOptions) (runner, error) {
			return runtimeRunnerFunc(func(context.Context) error { started.Store(true); return nil }), nil
		},
		worker: func(cameraworker.Config, *sourceBroker, CameraOptions) (runner, error) {
			return nil, errors.New("fixture private key/path secret")
		},
	})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "worker")
	require.NotContains(t, err.Error(), "secret")
	require.False(t, started.Load())
}

func TestSupervisorUnexpectedReturnAndCancellationJoin(t *testing.T) {
	for _, result := range []error{nil, context.Canceled} {
		name := "nil"
		if result != nil {
			name = "internal cancellation"
		}
		t.Run(name, func(t *testing.T) {
			c := runtimeTestConfig(t)
			c.Worker = nil
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started, ending, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseCleanup := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseCleanup()
			var builds atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- runWithFactories(ctx, c, runtimeFactories{broker: newSourceBroker, producer: func(producer.Config, *sourceBroker, *CacheOptions) (runner, error) {
					if builds.Add(1) == 1 {
						return runtimeRunnerFunc(func(context.Context) error { return result }), nil
					}
					return runtimeRunnerFunc(func(ctx context.Context) error { close(started); <-ctx.Done(); close(ending); <-release; return nil }), nil
				}})
			}()
			runtimeWaitSignal(t, started)
			cancel()
			runtimeWaitSignal(t, ending)
			select {
			case <-done:
				t.Fatal("Run returned before the role's cleanup joined")
			default:
			}
			releaseCleanup()
			require.NoError(t, <-done)
			require.Equal(t, int32(2), builds.Load())
		})
	}
}

func TestSupervisorCanceledParentDoesNotConstructHost(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, runWithFactories(ctx, runtimeTestConfig(t), runtimeFactories{}))
}

func TestSupervisorRejectsInvalidProducerHeartbeatBeforeConstructingRoles(t *testing.T) {
	c := runtimeTestConfig(t)
	c.Producer.HeartbeatInterval = -time.Second
	require.ErrorIs(t, Validate(c), ErrInvalidConfig)
	// Empty factories would panic if either constructor were reached. In
	// particular a delayed time.NewTicker panic must not kill a healthy Worker.
	require.ErrorIs(t, runWithFactories(t.Context(), c, runtimeFactories{}), ErrInvalidConfig)
	c.Producer.HeartbeatInterval = 0
	require.NoError(t, Validate(c), "zero selects the Producer's existing default")
}

func TestEdgeConfigurationSeparatesResolvedPrivateState(t *testing.T) {
	c := runtimeTestConfig(t)
	root := t.TempDir()
	real, link := filepath.Join(root, "real"), filepath.Join(root, "alias")
	require.NoError(t, os.Mkdir(real, 0700))
	require.NoError(t, os.Symlink(real, link))
	c.Producer.StateDir = filepath.Join(real, "not-created", "host")
	c.Worker.StateDir = filepath.Join(link, "not-created", "host")
	require.ErrorIs(t, Validate(c), ErrInvalidConfig, "existing parent symlinks cannot alias absent role directories")
	c.Worker.StateDir = filepath.Join(link, "not-created", "other")
	require.NoError(t, Validate(c))
	c.Worker.StateDir = filepath.Join(root, "loop")
	require.NoError(t, os.Symlink(c.Worker.StateDir, c.Worker.StateDir))
	require.ErrorIs(t, Validate(c), ErrInvalidConfig)
	c.Worker.StateDir = filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(c.Worker.StateDir, []byte("not a private state directory"), 0600))
	require.ErrorIs(t, Validate(c), ErrInvalidConfig)
}

func TestEdgeCacheIsExplicitAndBounded(t *testing.T) {
	c := runtimeTestConfig(t)
	n, err := normalizedConfig(c)
	require.NoError(t, err)
	require.Nil(t, n.Cache)
	c.Cache = &CacheOptions{MaxAge: time.Second}
	n, err = normalizedConfig(c)
	require.NoError(t, err)
	require.Equal(t, 32, n.Cache.MaxEntries)
	require.Zero(t, c.Cache.MaxEntries, "normalization does not mutate the caller's options")
	for _, value := range []CacheOptions{{MaxAge: 0}, {MaxAge: time.Millisecond}, {MaxAge: 301 * time.Second}, {MaxAge: time.Second, MaxEntries: 65}, {MaxAge: time.Second, MaxEntries: -1}} {
		c.Cache = &value
		require.ErrorIs(t, Validate(c), ErrInvalidConfig)
	}
	c.Cache = &CacheOptions{MaxAge: 300 * time.Second, MaxEntries: 64}
	require.NoError(t, Validate(c))
	c.Producer = nil
	require.ErrorIs(t, Validate(c), ErrInvalidConfig)
	c.Cache = nil
	require.NoError(t, Validate(c), "a disabled Producer and its section are independent of the Worker")
	c.Worker = nil
	require.ErrorIs(t, Validate(c), ErrInvalidConfig)
	require.False(t, strings.Contains(ErrInvalidConfig.Error(), "private"))
}
