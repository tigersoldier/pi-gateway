// Package gwlog is the gateway's structured logger, shared by the daemon and
// the session actors. Text output suits journald; JSON suits log aggregators
// (docs/design.md §14 M3).
package gwlog

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Logger is the logging surface used across the gateway. Implementations are
// safe for concurrent use.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	// With returns a logger that adds args to every record.
	With(args ...any) Logger
}

// New returns a logger writing to w. level is one of debug, info, warn, or
// error (case-insensitive); format is "text" (the default) or "json".
func New(w io.Writer, level, format string) (Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "text":
		h = slog.NewTextHandler(w, opts)
	case "json":
		h = slog.NewJSONHandler(w, opts)
	default:
		return nil, fmt.Errorf("gwlog: unknown format %q (want text or json)", format)
	}
	return &slogLogger{log: slog.New(h)}, nil
}

// ParseLevel maps a level name to its slog level.
func ParseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("gwlog: unknown level %q (want debug, info, warn, or error)", level)
}

func (s *slogLogger) Debug(msg string, args ...any) { s.log.Debug(msg, args...) }
func (s *slogLogger) Info(msg string, args ...any)  { s.log.Info(msg, args...) }
func (s *slogLogger) Warn(msg string, args ...any)  { s.log.Warn(msg, args...) }
func (s *slogLogger) Error(msg string, args ...any) { s.log.Error(msg, args...) }
func (s *slogLogger) With(args ...any) Logger       { return &slogLogger{log: s.log.With(args...)} }

type slogLogger struct{ log *slog.Logger }

// Nop returns a logger that discards every record.
func Nop() Logger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
func (nopLogger) With(...any) Logger   { return nopLogger{} }

// FromLogf adapts a printf-style function (typically t.Logf in tests) to the
// Logger interface, rendering structured attributes as key=value pairs.
func FromLogf(f func(format string, args ...any)) Logger {
	if f == nil {
		return Nop()
	}
	return &logfLogger{f: f}
}

type logfLogger struct {
	f      func(format string, args ...any)
	prefix string
}

func (l *logfLogger) log(msg string, args ...any) {
	line := msg
	if attrs := formatAttrs(args); attrs != "" {
		line += " " + attrs
	}
	if l.prefix != "" {
		line = l.prefix + " " + line
	}
	l.f("%s", line)
}

func (l *logfLogger) Debug(msg string, args ...any) { l.log(msg, args...) }
func (l *logfLogger) Info(msg string, args ...any)  { l.log(msg, args...) }
func (l *logfLogger) Warn(msg string, args ...any)  { l.log(msg, args...) }
func (l *logfLogger) Error(msg string, args ...any) { l.log(msg, args...) }

func (l *logfLogger) With(args ...any) Logger {
	prefix := formatAttrs(args)
	if l.prefix != "" {
		prefix = l.prefix + " " + prefix
	}
	return &logfLogger{f: l.f, prefix: prefix}
}

// formatAttrs renders slog-style key/value pairs for printf adapters.
func formatAttrs(args []any) string {
	if len(args) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(args); i += 2 {
		if i > 0 {
			b.WriteByte(' ')
		}
		key, ok := args[i].(string)
		if !ok {
			key = fmt.Sprint(args[i])
		}
		val := any("(MISSING)")
		if i+1 < len(args) {
			val = args[i+1]
		}
		fmt.Fprintf(&b, "%s=%v", key, val)
	}
	return b.String()
}
