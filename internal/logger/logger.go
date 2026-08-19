package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	With(args ...any) Logger
}

type SLogger struct {
	base *slog.Logger
}

func New(level, format string) Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	return &SLogger{base: slog.New(handler)}
}

func NewWithWriter(level, format string, w io.Writer) Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		handler = slog.NewJSONHandler(w, opts)
	}
	return &SLogger{base: slog.New(handler)}
}

func (l *SLogger) With(args ...any) Logger {
	return &SLogger{base: l.base.With(args...)}
}

func (l *SLogger) Debug(ctx context.Context, msg string, args ...any) {
	l.base.DebugContext(ctx, msg, args...)
}

func (l *SLogger) Info(ctx context.Context, msg string, args ...any) {
	l.base.InfoContext(ctx, msg, args...)
}

func (l *SLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.base.WarnContext(ctx, msg, args...)
}

func (l *SLogger) Error(ctx context.Context, msg string, args ...any) {
	l.base.ErrorContext(ctx, msg, args...)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func JSONField(name string, value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return json.RawMessage(raw)
}
