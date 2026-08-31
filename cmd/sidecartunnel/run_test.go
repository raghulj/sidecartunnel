package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newHeldPort binds a loopback port and holds it for the test, so that a startup can be
// made to fail on a port clash rather than on something the test invented.
func newHeldPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold a port: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String()
}

// TestRun_Version prints the three symbols the release build injects with -X.
//
// docs/15-releasing.md §5: the linker silently ignores an -X for a symbol it cannot find,
// so a release built without them succeeds and reports nothing. This is the check that
// notices, which is only worth anything if the values actually reach the output.
func TestRun_Version(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), []string{"--version"}, &stdout, &stderr, signalChan(1)); got != exitOK {
		t.Fatalf("run(--version) = %d, want %d", got, exitOK)
	}
	for _, want := range []string{version, commit, date} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("--version output %q does not report %q", stdout.String(), want)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("--version wrote to stderr: %q", stderr.String())
	}
}

// TestRun_Help answers -h with the usage text and exit 0. A help request is not a failure.
func TestRun_Help(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), []string{"-h"}, &stdout, &stderr, signalChan(1)); got != exitOK {
		t.Fatalf("run(-h) = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stderr.String(), "healthcheck") {
		t.Fatalf("usage does not mention the healthcheck subcommand: %q", stderr.String())
	}
}

// TestRun_ArgumentErrors covers every way the command line can be wrong. Each is exit 1
// with something on stderr an operator can act on.
func TestRun_ArgumentErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"--nope"}, "flag provided but not defined"},
		{"unknown subcommand", []string{"serve"}, `unknown subcommand "serve"`},
		{"healthcheck with an argument", []string{"healthcheck", "now"}, "takes no arguments"},
		{"unknown flag after the subcommand", []string{"healthcheck", "--nope"}, "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(context.Background(), tt.args, &stdout, &stderr, signalChan(1)); got != exitFailure {
				t.Fatalf("run(%v) = %d, want %d", tt.args, got, exitFailure)
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tt.want)
			}
		})
	}
}

// TestRun_InvalidConfigNamesTheKey_NFR5 is the acceptance criterion for NFR-5: invalid
// configuration fails startup with a message naming the offending key, and the process
// does not start in a partially-configured state.
//
// server.allowed_origins is the rule chosen because it is the one most likely to be
// relaxed for convenience by someone in a hurry, and because a default of ["*"] would be
// a security hole shipped as a convenience (docs/05-authorization.md §5).
func TestRun_InvalidConfigNamesTheKey_NFR5(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "no allowed origins",
			env:  map[string]string{"ST_SERVER__ALLOWED_ORIGINS": ""},
			want: "server.allowed_origins",
		},
		{
			name: "no connect url",
			env: map[string]string{
				"ST_SERVER__ALLOWED_ORIGINS": "https://app.example.com",
				"ST_APP__CONNECT_URL":        "",
			},
			want: "app.connect_url",
		},
		{
			name: "short control secret",
			env: map[string]string{
				"ST_SERVER__ALLOWED_ORIGINS": "https://app.example.com",
				"ST_APP__CONNECT_URL":        "http://webapp:5000/_st/connect",
				"ST_APP__WEBHOOK_SECRETS":    testWebhookSecret,
				"ST_CONTROL__SECRET":         "too-short",
			},
			want: "control.secret",
		},
		{
			name: "server.listen has no port",
			env: map[string]string{
				"ST_SERVER__ALLOWED_ORIGINS": "https://app.example.com",
				"ST_APP__CONNECT_URL":        "http://webapp:5000/_st/connect",
				"ST_APP__WEBHOOK_SECRETS":    testWebhookSecret,
				"ST_CONTROL__SECRET":         testControlSecret,
				"ST_SERVER__LISTEN":          "0.0.0.0",
			},
			want: "server.listen",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			var stdout, stderr bytes.Buffer
			if got := run(context.Background(), nil, &stdout, &stderr, signalChan(1)); got != exitFailure {
				t.Fatalf("run() = %d on invalid configuration, want %d", got, exitFailure)
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr = %q, want it to name %q (NFR-5)", stderr.String(), tt.want)
			}
			if !strings.HasPrefix(stderr.String(), "FATAL ") {
				t.Fatalf("stderr = %q, want it to begin FATAL (docs/08-config.md §2)", stderr.String())
			}
		})
	}
}

// TestRun_InvalidConfigNeverQuotesASecret is docs/14-coding-standards.md §9 applied to the
// startup path: an error from internal/config names the key and must not quote the value
// of app.webhook_secrets or control.secret (NFR-7).
func TestRun_InvalidConfigNeverQuotesASecret(t *testing.T) {
	env := testEnv("http://webapp:5000/_st/connect")
	env["ST_APP__WEBHOOK_SECRETS"] = "short"
	for k, v := range env {
		t.Setenv(k, v)
	}
	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), nil, &stdout, &stderr, signalChan(1)); got != exitFailure {
		t.Fatalf("run() = %d, want %d", got, exitFailure)
	}
	for _, secret := range []string{"short", testControlSecret} {
		if strings.Contains(stderr.String(), secret) {
			t.Fatalf("startup error quoted a secret value: %q", stderr.String())
		}
	}
}

// TestRun_ConfigFlagReadsTheFile proves --config is wired to config.Load and that a file
// error names the file, because an operator with three config files and a crash loop
// cannot act on a message that names only the key.
func TestRun_ConfigFlagReadsTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  no_such_key: 1\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), []string{"--config", path}, &stdout, &stderr, signalChan(1)); got != exitFailure {
		t.Fatalf("run(--config) = %d on an unknown key, want %d", got, exitFailure)
	}
	if !strings.Contains(stderr.String(), path) || !strings.Contains(stderr.String(), "no_such_key") {
		t.Fatalf("stderr = %q, want it to name the file and the unknown key", stderr.String())
	}
}

// TestRun_HealthcheckAcceptsTheFlagOnEitherSide covers both spellings a deployment ends
// up with: the flag before the subcommand, as a person types it, and after it, as a
// compose healthcheck grows one.
func TestRun_HealthcheckAcceptsTheFlagOnEitherSide(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  no_such_key: 1\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	for _, args := range [][]string{
		{"--config", path, "healthcheck"},
		{"healthcheck", "--config", path},
	} {
		var stdout, stderr bytes.Buffer
		if got := run(context.Background(), args, &stdout, &stderr, signalChan(1)); got != exitFailure {
			t.Fatalf("run(%v) = %d, want %d", args, got, exitFailure)
		}
		if !strings.Contains(stderr.String(), path) {
			t.Fatalf("run(%v) stderr = %q, want it to name the config file", args, stderr.String())
		}
	}
}

// TestRun_StartupFailureIsReportedAndExitsOne covers the wiring failure that survives
// validation: a configuration that validates but names a port already in use. A bind
// failure must be a startup error, never a listener that silently is not there.
func TestRun_StartupFailureIsReportedAndExitsOne(t *testing.T) {
	stub := newStubWebhook(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600})

	// Hold a port, then ask the gateway for the same one.
	held := newHeldPort(t)
	env := testEnv(stub.URL)
	env["ST_SERVER__LISTEN"] = held
	env["ST_LOG__FORMAT"] = "text"
	for k, v := range env {
		t.Setenv(k, v)
	}

	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), nil, &stdout, &stderr, signalChan(1)); got != exitFailure {
		t.Fatalf("run() = %d with server.listen already bound, want %d", got, exitFailure)
	}
	if !strings.Contains(stderr.String(), "server.listen") {
		t.Fatalf("stderr = %q, want it to name server.listen", stderr.String())
	}
}

// TestRun_ServeStopsOnAContextCancellation is the smallest whole-binary run: it loads the
// configuration, builds the graph, serves, drains and returns 0 when its context ends.
//
// It also asserts NFR-7 across the whole startup path at log.level debug: neither secret
// reaches a log line, at any level, including this one.
func TestRun_ServeStopsOnAContextCancellation(t *testing.T) {
	stub := newStubWebhook(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600})
	for k, v := range testEnv(stub.URL) {
		t.Setenv(k, v)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stderr := &syncBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(ctx, nil, io.Discard, stderr, signalChan(1)) }()
	cancel()

	select {
	case got := <-done:
		if got != exitOK {
			t.Fatalf("run() = %d after cancellation, want %d\n%s", got, exitOK, stderr.String())
		}
	case <-time.After(waitFor):
		t.Fatalf("run did not return within %s", waitFor)
	}
	for _, secret := range []string{testWebhookSecret, testControlSecret} {
		if strings.Contains(stderr.String(), secret) {
			t.Fatalf("a secret reached the log at debug level (NFR-7): %q", stderr.String())
		}
	}
}

// TestRun_HealthcheckSubcommand is the subcommand as a container runtime invokes it:
// `sidecartunnel healthcheck`, configured from the environment alone, exiting 0 against a
// live listener and 1 against nothing.
//
// It is the whole point of the subcommand existing — a distroless image has no shell and
// no curl, so the binary probes itself (docs/10-operations.md §1, §3).
func TestRun_HealthcheckSubcommand(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("healthcheck probed %q, want /health — never /ready (FR-20)", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer live.Close()

	stub := newStubWebhook(t, grant{user: "u-7", expiresIn: 3600})
	env := testEnv(stub.URL)
	env["ST_SERVER__LISTEN"] = strings.TrimPrefix(live.URL, "http://")
	for k, v := range env {
		t.Setenv(k, v)
	}

	var stdout, stderr bytes.Buffer
	if got := run(context.Background(), []string{"healthcheck"}, &stdout, &stderr, signalChan(1)); got != exitOK {
		t.Fatalf("healthcheck against a live listener = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}

	// The same configuration with nothing listening is exit 1, which is what makes the
	// container healthcheck mean anything.
	t.Setenv("ST_SERVER__LISTEN", newClosedPort(t))
	stderr.Reset()
	if got := run(context.Background(), []string{"healthcheck"}, &stdout, &stderr, signalChan(1)); got != exitFailure {
		t.Fatalf("healthcheck against nothing = %d, want %d", got, exitFailure)
	}
	if stderr.Len() == 0 {
		t.Fatal("a failed healthcheck said nothing on stderr")
	}
}

// newClosedPort returns an address that was bound and then released, so nothing is
// listening on it.
func newClosedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}
