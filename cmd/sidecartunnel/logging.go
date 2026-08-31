package main

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// newLogger builds the structured logger docs/08-config.md §3's log block describes: one
// of four levels, in JSON or text, writing to w.
//
// It returns an error naming the key rather than falling back to a default. A gateway
// that was asked for "warn" and silently logs at info is a gateway whose logs an operator
// will read as evidence of quiet (NFR-5). config.Validate enforces both rules already;
// the check stays because a logger is also built from a hand-written config in a test,
// and because the alternative to an error here is a silent default.
//
// Nothing this logger is given anywhere in the process carries a cookie, an Authorization
// header, a webhook body or a message payload, at any level including debug — that rule
// is enforced at the call sites, not here (NFR-7, docs/14-coding-standards.md §9).
func newLogger(cfg config.Log, w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("log.level is %q, want one of debug, info, warn, error (docs/08-config.md §3)", cfg.Level)
	}

	opts := &slog.HandlerOptions{Level: level}
	switch cfg.Format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("log.format is %q, want json or text (docs/08-config.md §3)", cfg.Format)
	}
}
