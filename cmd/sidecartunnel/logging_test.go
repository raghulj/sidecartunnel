package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestNewLogger_LevelsAndFormats walks docs/08-config.md §3's log block. Each level is
// asserted by what it suppresses, because a level that logs everything is
// indistinguishable from a correct one until an operator reads a quiet log as evidence of
// quiet.
func TestNewLogger_LevelsAndFormats(t *testing.T) {
	tests := []struct {
		name      string
		cfg       config.Log
		wantDebug bool
		wantWarn  bool
		contains  string
	}{
		{"debug json", config.Log{Level: "debug", Format: "json"}, true, true, `"msg":"warned"`},
		{"info json", config.Log{Level: "info", Format: "json"}, false, true, `"msg":"warned"`},
		{"warn text", config.Log{Level: "warn", Format: "text"}, false, true, "msg=warned"},
		{"error text", config.Log{Level: "error", Format: "text"}, false, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log, err := newLogger(tt.cfg, &buf)
			if err != nil {
				t.Fatalf("newLogger(%+v) = %v", tt.cfg, err)
			}
			log.Debug("debugged")
			log.Warn("warned")

			if got := strings.Contains(buf.String(), "debugged"); got != tt.wantDebug {
				t.Fatalf("debug emitted = %v at level %q, want %v", got, tt.cfg.Level, tt.wantDebug)
			}
			if got := strings.Contains(buf.String(), "warned"); got != tt.wantWarn {
				t.Fatalf("warn emitted = %v at level %q, want %v", got, tt.cfg.Level, tt.wantWarn)
			}
			if tt.contains != "" && !strings.Contains(buf.String(), tt.contains) {
				t.Fatalf("output %q is not %s", buf.String(), tt.cfg.Format)
			}
		})
	}
}

// TestNewLogger_Rejections keeps NFR-5's rule at the logger too: an unusable value names
// its key and refuses, rather than defaulting silently to info.
func TestNewLogger_Rejections(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Log
		want string
	}{
		{"unknown level", config.Log{Level: "trace", Format: "json"}, "log.level"},
		{"empty level", config.Log{Format: "json"}, "log.level"},
		{"unknown format", config.Log{Level: "info", Format: "logfmt"}, "log.format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log, err := newLogger(tt.cfg, &buf)
			if err == nil {
				t.Fatalf("newLogger(%+v) = %v, nil; want an error naming %s", tt.cfg, log, tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("newLogger error = %q, want it to name %q (NFR-5)", err, tt.want)
			}
		})
	}
}

// TestNewLogger_WritesWhereItIsTold is the property the startup path depends on: the
// logger writes to the stream run was given, not to a package-level default
// (docs/14-coding-standards.md §7).
func TestNewLogger_WritesWhereItIsTold(t *testing.T) {
	var buf bytes.Buffer
	log, err := newLogger(config.Log{Level: "info", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	log.Info("hello", slog.String("k", "v"))
	if !strings.Contains(buf.String(), `"k":"v"`) {
		t.Fatalf("output = %q, want the attribute", buf.String())
	}
}
