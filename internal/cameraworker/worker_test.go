package cameraworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecutorErrorLogsNeverContainCameraSecrets(t *testing.T) {
	const secret = "worker-password:/@ value"
	rawURL := "rtsp://operator:" + secret + "@camera.example/stream"
	escapedURL := "rtsp://operator:" + url.QueryEscape(secret) + "@camera.example/stream"
	for _, makeError := range []struct {
		name  string
		error func(string) error
	}{
		{"driver", func(message string) error { return fmt.Errorf("camera probe failed: %s", message) }},
		{"grpc_status", func(message string) error { return status.Error(codes.Unauthenticated, message) }},
		{"wrapped_context", func(message string) error { return fmt.Errorf("%s: %w", message, context.DeadlineExceeded) }},
	} {
		t.Run(makeError.name, func(t *testing.T) {
			var logs bytes.Buffer
			injected := makeError.error(rawURL + " " + escapedURL)
			w := &Worker{cfg: Config{LeaseSeconds: 30, Log: slog.New(slog.NewJSONHandler(&logs, nil)), Executor: ExecutorFunc(func(context.Context, *Execution) error { return injected })}}
			finished := make(chan error, 1)
			// The executor result travels through the same execution completion path
			// consumed by poll/session/Run. No device/CP operation is needed here.
			w.execute(context.Background(), nil, api.CameraWorkerWork_builder{}.Build(), finished)
			returned := <-finished
			require.True(t, errors.Is(returned, injected), "logging must not replace the error used for auth/retry control flow")
			require.Equal(t, status.Code(injected), status.Code(returned))
			w.logSessionFailure(returned)
			output := logs.String()
			require.Contains(t, output, "camera worker connection stopped")
			for _, sensitive := range []string{secret, url.QueryEscape(secret), rawURL, escapedURL, "operator", "camera.example", "camera probe failed"} {
				require.NotContains(t, output, sensitive)
			}
			if errors.Is(injected, context.DeadlineExceeded) {
				require.Contains(t, output, `"context":"deadline_exceeded"`)
			} else {
				require.Contains(t, output, `"code":"`+status.Code(injected).String()+`"`)
			}
		})
	}
}
