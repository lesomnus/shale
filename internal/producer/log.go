package producer

import "log/slog"

// slogOf adapts a minimal logger to *slog.Logger for the capture.
func slogOf(l interface {
	Info(string, ...any)
	Warn(string, ...any)
}) *slog.Logger {
	if s, ok := l.(*slog.Logger); ok {
		return s
	}

	return slog.Default()
}
