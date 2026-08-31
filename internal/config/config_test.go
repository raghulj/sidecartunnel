package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// testSecret is exactly 32 bytes, the minimum docs/08-config.md §3 requires of
// app.webhook_secrets and control.secret.
const testSecret = "0123456789abcdef0123456789abcdef"

// isolateEnv removes every ST_ variable from the process environment for the duration of
// the test, so a developer with ST_BUS__URL exported does not silently change what these
// tests assert. Restored by t.Cleanup.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		key, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(key, "ST_") {
			continue
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unsetenv(%q) = %v", key, err)
		}
		t.Cleanup(func() { os.Setenv(key, value) })
	}
}

// minimalEnv sets the four keys docs/08-config.md §4 calls the equivalent minimum by
// environment alone, minus bus.url which has a default.
func minimalEnv(t *testing.T) {
	t.Helper()
	isolateEnv(t)
	t.Setenv("ST_SERVER__ALLOWED_ORIGINS", "https://app.example.com")
	t.Setenv("ST_APP__CONNECT_URL", "http://webapp:5000/_st/connect")
	t.Setenv("ST_APP__WEBHOOK_SECRETS", testSecret)
	t.Setenv("ST_CONTROL__SECRET", testSecret)
}

// loadMinimal returns a valid configuration built from defaults plus the documented
// environment-only minimum. Validation tests mutate it one key at a time.
func loadMinimal(t *testing.T) *Config {
	t.Helper()
	minimalEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") = %v, want no error", err)
	}
	return cfg
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) = %v", path, err)
	}
	return path
}

// resolveNamespace applies docs/06-channels.md §1 and §3 so a test can assert that a
// subscribe would find a block: the substring before the first separator selects the
// block, a channel with no separator resolves to the reserved name "", and a namespace
// with no block of its own falls back to the built-in default block — which is the one
// named "". No block at all is error 102, the closed failure.
func resolveNamespace(c *Config, channel string) (Namespace, bool) {
	name := ""
	if i := strings.Index(channel, c.Channels.Separator); i >= 0 {
		name = channel[:i]
	}
	var fallback *Namespace
	for i := range c.Namespaces {
		if c.Namespaces[i].Name == name {
			return c.Namespaces[i], true
		}
		if c.Namespaces[i].Name == "" {
			fallback = &c.Namespaces[i]
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return Namespace{}, false
}

// TestLoad_Defaults asserts every default in docs/08-config.md §3, key by key. A default
// that drifts from the table is a bug in one of the two, and this table is how it is
// found.
func TestLoad_Defaults(t *testing.T) {
	cfg := loadMinimal(t)

	tests := []struct {
		key  string
		got  any
		want any
	}{
		{"server.listen", cfg.Server.Listen, ":8000"},
		{"server.path", cfg.Server.Path, "/ws"},
		{"server.allow_missing_origin", cfg.Server.AllowMissingOrigin, false},
		{"server.handshake_timeout", cfg.Server.HandshakeTimeout.Duration(), 5 * time.Second},
		{"server.ping_interval", cfg.Server.PingInterval.Duration(), 25 * time.Second},
		{"server.pong_timeout", cfg.Server.PongTimeout.Duration(), 10 * time.Second},
		{"server.drain_timeout", cfg.Server.DrainTimeout.Duration(), 20 * time.Second},
		{"server.drain_spread", cfg.Server.DrainSpread.Duration(), 60 * time.Second},
		{"server.read_header_timeout", cfg.Server.ReadHeaderTimeout.Duration(), 5 * time.Second},
		{"server.trusted_proxies", len(cfg.Server.TrustedProxies), 0},

		{"app.name", cfg.App.Name, "app"},
		{"app.connect_timeout", cfg.App.ConnectTimeout.Duration(), 10 * time.Second},
		{"app.connect_queue", cfg.App.ConnectQueue, 4096},
		{"app.cookie_names", len(cfg.App.CookieNames), 0},
		{"app.webhook_timeout", cfg.App.WebhookTimeout.Duration(), 3 * time.Second},
		{"app.webhook_concurrency", cfg.App.WebhookConcurrency, 32},
		{"app.webhook_retries", cfg.App.WebhookRetries, 1},
		{"app.cache_ttl", cfg.App.CacheTTL.Duration(), time.Duration(0)},
		{"app.min_expiry", cfg.App.MinExpiry.Duration(), 60 * time.Second},
		{"app.max_expiry", cfg.App.MaxExpiry.Duration(), 6 * time.Hour},

		{"bus.kind", cfg.Bus.Kind, "redis"},
		{"bus.url", cfg.Bus.URL, "redis://localhost:6379/0"},
		{"bus.dial_timeout", cfg.Bus.DialTimeout.Duration(), 3 * time.Second},
		{"bus.reconnect_min", cfg.Bus.ReconnectMin.Duration(), 200 * time.Millisecond},
		{"bus.reconnect_max", cfg.Bus.ReconnectMax.Duration(), 10 * time.Second},
		{"bus.intake_queue", cfg.Bus.IntakeQueue, 4096},
		{"bus.dispatch_workers", cfg.Bus.DispatchWorkers, 2},
		{"bus.ready_grace", cfg.Bus.ReadyGrace.Duration(), 30 * time.Second},
		{"bus.prefix", cfg.Bus.Prefix, "st:"},

		{"channels.separator", cfg.Channels.Separator, "-"},

		{"limits.max_connections", cfg.Limits.MaxConnections, 25000},
		{"limits.max_subscriptions_per_conn", cfg.Limits.MaxSubscriptionsPerConn, 500},
		{"limits.max_connections_per_user", cfg.Limits.MaxConnectionsPerUser, 20},
		{"limits.read_buffer", cfg.Limits.ReadBuffer, 2048},
		{"limits.write_buffer", cfg.Limits.WriteBuffer, 2048},
		{"limits.compression", cfg.Limits.Compression, false},
		{"limits.outbound_queue", cfg.Limits.OutboundQueue, 256},
		{"limits.max_message_size", cfg.Limits.MaxMessageSize, 32768},
		{"limits.max_frame_size", cfg.Limits.MaxFrameSize, 16384},
		{"limits.max_channel_length", cfg.Limits.MaxChannelLength, 255},

		{"control.refresh_spread", cfg.Control.RefreshSpread.Duration(), 60 * time.Second},

		{"log.level", cfg.Log.Level, "info"},
		{"log.format", cfg.Log.Format, "json"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Fatalf("%s = %v, want %v", tt.key, tt.got, tt.want)
			}
		})
	}
}

// TestLoad_NoDefaultForAllowedOrigins is the rule most likely to be relaxed for
// convenience: there is no default, and an absent one refuses to start
// (docs/08-config.md §3, docs/05-authorization.md §5).
func TestLoad_NoDefaultForAllowedOrigins(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ST_APP__CONNECT_URL", "http://webapp:5000/_st/connect")
	t.Setenv("ST_APP__WEBHOOK_SECRETS", testSecret)
	t.Setenv("ST_CONTROL__SECRET", testSecret)

	cfg, err := Load("")
	if err == nil {
		t.Fatalf("Load(\"\") = %+v, want an error", cfg)
	}
	if cfg != nil {
		t.Fatalf("Load returned %+v alongside an error, want nil", cfg)
	}
	if !strings.Contains(err.Error(), "server.allowed_origins") {
		t.Fatalf("error = %q, want it to name server.allowed_origins", err)
	}
}

// TestLoad_Precedence asserts the three layers of docs/08-config.md §1 in order:
// built-in defaults, then the YAML file, then the environment.
func TestLoad_Precedence(t *testing.T) {
	yaml := "server:\n  path: /from-file\n  ping_interval: 40s\n  allowed_origins: [\"https://file.example.com\"]\n"
	path := writeFile(t, "config.yaml", yaml)

	t.Run("defaults only", func(t *testing.T) {
		cfg := loadMinimal(t)
		if cfg.Server.Path != "/ws" {
			t.Fatalf("server.path = %q, want the default %q", cfg.Server.Path, "/ws")
		}
	})

	t.Run("file beats defaults", func(t *testing.T) {
		minimalEnv(t)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Server.Path != "/from-file" {
			t.Fatalf("server.path = %q, want %q", cfg.Server.Path, "/from-file")
		}
		if cfg.Server.PingInterval.Duration() != 40*time.Second {
			t.Fatalf("server.ping_interval = %v, want 40s", cfg.Server.PingInterval)
		}
		// The environment set allowed_origins, so it wins over the file's value even
		// here; the file value is asserted absent to prove ordering, not merging.
		if got := cfg.Server.AllowedOrigins; !reflect.DeepEqual(got, []string{"https://app.example.com"}) {
			t.Fatalf("server.allowed_origins = %v, want the environment's value", got)
		}
	})

	t.Run("env beats file", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_SERVER__PATH", "/from-env")
		t.Setenv("ST_SERVER__PING_INTERVAL", "45s")
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Server.Path != "/from-env" {
			t.Fatalf("server.path = %q, want %q", cfg.Server.Path, "/from-env")
		}
		if cfg.Server.PingInterval.Duration() != 45*time.Second {
			t.Fatalf("server.ping_interval = %v, want 45s", cfg.Server.PingInterval)
		}
	})
}

// TestLoad_FileErrors covers the file layer's failure paths. Decoding is strict, so an
// unrecognised key is a startup error naming it — including auth_required, which
// docs/13-review-findings.md S4 removed and which must not be silently honoured.
func TestLoad_FileErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		path    string
		wantErr string
	}{
		{name: "missing file", path: filepath.Join(t.TempDir(), "absent.yaml"), wantErr: "absent.yaml"},
		{name: "malformed yaml", content: "server: [\n", wantErr: "yaml"},
		{name: "unknown key", content: "server:\n  listen_addr: \":9000\"\n", wantErr: "listen_addr"},
		{name: "unknown top level", content: "servers: {}\n", wantErr: "servers"},
		{name: "removed auth_required", content: "namespaces:\n  - name: room\n    auth_required: true\n", wantErr: "auth_required"},
		{name: "bad duration", content: "server:\n  ping_interval: banana\n", wantErr: "banana"},
		{name: "wrong type", content: "limits:\n  max_connections: lots\n", wantErr: "lots"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minimalEnv(t)
			path := tt.path
			if path == "" {
				path = writeFile(t, "config.yaml", tt.content)
			}
			cfg, err := Load(path)
			if err == nil {
				t.Fatalf("Load = %+v, want an error", cfg)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestLoad_EmptyFile: an empty document is not an error, it just contributes nothing.
func TestLoad_EmptyFile(t *testing.T) {
	minimalEnv(t)
	cfg, err := Load(writeFile(t, "empty.yaml", ""))
	if err != nil {
		t.Fatalf("Load = %v, want no error", err)
	}
	if cfg.Server.Path != "/ws" {
		t.Fatalf("server.path = %q, want the default", cfg.Server.Path)
	}
}

// TestLoad_EnvTypes covers one environment override per field kind in the tree: string,
// bool, int, duration and comma-separated scalar list (docs/08-config.md §1).
func TestLoad_EnvTypes(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		check func(*testing.T, *Config)
	}{
		{
			name: "string", key: "ST_BUS__PREFIX", value: "other:",
			check: func(t *testing.T, c *Config) {
				if c.Bus.Prefix != "other:" {
					t.Fatalf("bus.prefix = %q", c.Bus.Prefix)
				}
			},
		},
		{
			name: "bool", key: "ST_LIMITS__COMPRESSION", value: "true",
			check: func(t *testing.T, c *Config) {
				if !c.Limits.Compression {
					t.Fatalf("limits.compression = false, want true")
				}
			},
		},
		{
			name: "int", key: "ST_BUS__DISPATCH_WORKERS", value: "8",
			check: func(t *testing.T, c *Config) {
				if c.Bus.DispatchWorkers != 8 {
					t.Fatalf("bus.dispatch_workers = %d", c.Bus.DispatchWorkers)
				}
			},
		},
		{
			name: "duration", key: "ST_APP__MAX_EXPIRY", value: "90m",
			check: func(t *testing.T, c *Config) {
				if c.App.MaxExpiry.Duration() != 90*time.Minute {
					t.Fatalf("app.max_expiry = %v", c.App.MaxExpiry)
				}
			},
		},
		{
			name: "list", key: "ST_SERVER__TRUSTED_PROXIES", value: "10.0.0.0/8, 192.168.0.0/16",
			check: func(t *testing.T, c *Config) {
				want := []string{"10.0.0.0/8", "192.168.0.0/16"}
				if !reflect.DeepEqual(c.Server.TrustedProxies, want) {
					t.Fatalf("server.trusted_proxies = %v, want %v", c.Server.TrustedProxies, want)
				}
			},
		},
		{
			name: "empty list clears", key: "ST_APP__COOKIE_NAMES", value: "",
			check: func(t *testing.T, c *Config) {
				if len(c.App.CookieNames) != 0 {
					t.Fatalf("app.cookie_names = %v, want empty", c.App.CookieNames)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minimalEnv(t)
			t.Setenv(tt.key, tt.value)
			cfg, err := Load("")
			if err != nil {
				t.Fatalf("Load = %v", err)
			}
			tt.check(t, cfg)
		})
	}
}

// TestLoad_EnvParseErrors: an environment value that cannot be parsed names its own
// variable, because that is the only thing an operator can act on (NFR-5).
func TestLoad_EnvParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "bool", key: "ST_LIMITS__COMPRESSION", value: "yes-please"},
		{name: "int", key: "ST_BUS__INTAKE_QUEUE", value: "many"},
		{name: "duration", key: "ST_SERVER__PING_INTERVAL", value: "25 seconds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minimalEnv(t)
			t.Setenv(tt.key, tt.value)
			_, err := Load("")
			if err == nil {
				t.Fatalf("Load = nil error, want one")
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("error = %q, want it to name %q", err, tt.key)
			}
		})
	}
}

// TestLoad_EnvFileSuffix covers the Docker and Swarm secret convention from
// docs/08-config.md §1: any key may be supplied from a file by appending _FILE, and the
// trailing newline every editor and `echo` adds is not part of the secret.
func TestLoad_EnvFileSuffix(t *testing.T) {
	t.Run("value read from file", func(t *testing.T) {
		minimalEnv(t)
		secret := testSecret + "-from-file"
		t.Setenv("ST_CONTROL__SECRET_FILE", writeFile(t, "secret", secret))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Control.Secret != secret {
			t.Fatalf("control.secret = %q, want %q", cfg.Control.Secret, secret)
		}
	})

	t.Run("trailing newline trimmed", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_CONTROL__SECRET_FILE", writeFile(t, "secret", testSecret+"\n"))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Control.Secret != testSecret {
			t.Fatalf("control.secret = %q, want the newline trimmed", cfg.Control.Secret)
		}
	})

	t.Run("trailing crlf trimmed", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_CONTROL__SECRET_FILE", writeFile(t, "secret", testSecret+"\r\n"))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Control.Secret != testSecret {
			t.Fatalf("control.secret = %q, want the newline trimmed", cfg.Control.Secret)
		}
	})

	t.Run("list from file", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_APP__WEBHOOK_SECRETS_FILE", writeFile(t, "secrets", testSecret+","+testSecret+"b\n"))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		want := []string{testSecret, testSecret + "b"}
		if !reflect.DeepEqual(cfg.App.WebhookSecrets, want) {
			t.Fatalf("app.webhook_secrets = %d entries, want %d", len(cfg.App.WebhookSecrets), len(want))
		}
	})

	t.Run("file wins over the plain variable", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_BUS__PREFIX", "from-env:")
		t.Setenv("ST_BUS__PREFIX_FILE", writeFile(t, "prefix", "from-file:\n"))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if cfg.Bus.Prefix != "from-file:" {
			t.Fatalf("bus.prefix = %q, want the _FILE value", cfg.Bus.Prefix)
		}
	})

	t.Run("missing file names the variable", func(t *testing.T) {
		minimalEnv(t)
		missing := filepath.Join(t.TempDir(), "absent")
		t.Setenv("ST_CONTROL__SECRET_FILE", missing)
		_, err := Load("")
		if err == nil {
			t.Fatalf("Load = nil error, want one")
		}
		if !strings.Contains(err.Error(), "ST_CONTROL__SECRET_FILE") {
			t.Fatalf("error = %q, want it to name ST_CONTROL__SECRET_FILE", err)
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("error = %q, want it to name the unreadable path", err)
		}
	})
}

// TestLoad_NamespacesJSON covers ST_NAMESPACES_JSON, which exists because a list of
// objects cannot be expressed as environment scalars (docs/08-config.md §1).
func TestLoad_NamespacesJSON(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_NAMESPACES_JSON", `[{"name":"room"},{"name":"desk","client_events":true,"rate_limit":"5/m"}]`)
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if len(cfg.Namespaces) != 2 {
			t.Fatalf("namespaces = %d blocks, want 2", len(cfg.Namespaces))
		}
		if cfg.Namespaces[0].Name != "room" || cfg.Namespaces[0].RateLimit != "10/s" {
			t.Fatalf("namespaces[0] = %+v, want name room and the default rate limit", cfg.Namespaces[0])
		}
		if !cfg.Namespaces[1].ClientEvents || cfg.Namespaces[1].RateLimit != "5/m" {
			t.Fatalf("namespaces[1] = %+v", cfg.Namespaces[1])
		}
	})

	t.Run("beats the file", func(t *testing.T) {
		minimalEnv(t)
		path := writeFile(t, "config.yaml", "namespaces:\n  - name: fromfile\n")
		t.Setenv("ST_NAMESPACES_JSON", `[{"name":"fromenv"}]`)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if len(cfg.Namespaces) != 1 || cfg.Namespaces[0].Name != "fromenv" {
			t.Fatalf("namespaces = %+v, want the environment's list", cfg.Namespaces)
		}
	})

	t.Run("from a file", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_NAMESPACES_JSON_FILE", writeFile(t, "ns.json", `[{"name":"room"}]`+"\n"))
		cfg, err := Load("")
		if err != nil {
			t.Fatalf("Load = %v", err)
		}
		if len(cfg.Namespaces) != 1 || cfg.Namespaces[0].Name != "room" {
			t.Fatalf("namespaces = %+v", cfg.Namespaces)
		}
	})

	errs := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "malformed", value: `[{"name":`, wantErr: "ST_NAMESPACES_JSON"},
		{name: "not a list", value: `{"name":"room"}`, wantErr: "ST_NAMESPACES_JSON"},
		{name: "unknown key", value: `[{"name":"room","auth_required":true}]`, wantErr: "auth_required"},
		{name: "invalid content", value: `[{"name":"default"}]`, wantErr: "namespaces[0].name"},
	}
	for _, tt := range errs {
		t.Run(tt.name, func(t *testing.T) {
			minimalEnv(t)
			t.Setenv("ST_NAMESPACES_JSON", tt.value)
			_, err := Load("")
			if err == nil {
				t.Fatalf("Load = nil error, want one")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}

	t.Run("missing file", func(t *testing.T) {
		minimalEnv(t)
		t.Setenv("ST_NAMESPACES_JSON_FILE", filepath.Join(t.TempDir(), "absent.json"))
		_, err := Load("")
		if err == nil {
			t.Fatalf("Load = nil error, want one")
		}
		if !strings.Contains(err.Error(), "ST_NAMESPACES_JSON_FILE") {
			t.Fatalf("error = %q, want it to name the variable", err)
		}
	})
}

// TestLoad_EmptyNamespacesInstallsDefault is docs/13-review-findings.md M11: without the
// built-in block the environment-only configuration starts cleanly, reports healthy, and
// refuses every subscribe.
func TestLoad_EmptyNamespacesInstallsDefault(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  string
	}{
		{name: "no file", yaml: ""},
		{name: "file omits namespaces", yaml: "server:\n  path: /ws\n"},
		{name: "file has an empty list", yaml: "namespaces: []\n"},
		{name: "env has an empty list", env: "[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minimalEnv(t)
			path := ""
			if tt.yaml != "" {
				path = writeFile(t, "config.yaml", tt.yaml)
			}
			if tt.env != "" {
				t.Setenv("ST_NAMESPACES_JSON", tt.env)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load = %v", err)
			}
			if len(cfg.Namespaces) != 1 {
				t.Fatalf("namespaces = %+v, want exactly the built-in default block", cfg.Namespaces)
			}
			ns := cfg.Namespaces[0]
			if ns.Name != "" {
				t.Fatalf("built-in block name = %q, want the reserved %q (docs/06-channels.md §1)", ns.Name, "")
			}
			if ns.RateLimit != "10/s" || ns.ClientEvents || ns.Presence || ns.HistorySize != 0 || ns.MaxMessageSize != nil {
				t.Fatalf("built-in block = %+v, want the documented namespace defaults", ns)
			}
			// It applies to every channel, separator or not.
			for _, channel := range []string{"room-4410", "standalone", "org-42-alerts"} {
				if _, ok := resolveNamespace(cfg, channel); !ok {
					t.Fatalf("resolveNamespace(%q) = not found, want the built-in default block", channel)
				}
			}
		})
	}
}

// TestLoad_ExplicitNamespacesFailClosed: once namespaces are configured, a channel whose
// namespace has no block and where no default block exists is refused
// (docs/06-channels.md §3). The built-in block is installed only for an empty list.
func TestLoad_ExplicitNamespacesFailClosed(t *testing.T) {
	minimalEnv(t)
	t.Setenv("ST_NAMESPACES_JSON", `[{"name":"room"}]`)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if _, ok := resolveNamespace(cfg, "room-4410"); !ok {
		t.Fatalf("resolveNamespace(room-4410) = not found, want the room block")
	}
	if _, ok := resolveNamespace(cfg, "desk-1"); ok {
		t.Fatalf("resolveNamespace(desk-1) = found, want fail-closed with no default block")
	}
}

// goldenYAML is the worked example from docs/08-config.md §4, verbatim.
const goldenYAML = `server:
  listen: ":8000"
  path: "/ws"
  allowed_origins:
    - "https://app.example.com"
  ping_interval: 25s

app:
  name: main
  connect_url: "http://webapp:5000/_st/connect"
  webhook_secrets: ["${ST_WEBHOOK_SECRET}"]
  webhook_concurrency: 32
  connect_timeout: 10s
  max_expiry: 6h

bus:
  kind: redis
  url: "redis://redis:6379/3"
  prefix: "st:"

control:
  secret: "${ST_CONTROL_SECRET}"

namespaces:
  - name: room
  - name: user

limits:
  max_connections: 25000
  outbound_queue: 256
`

// TestLoad_GoldenExample loads the worked example from docs/08-config.md §4 as written.
//
// Its ${...} placeholders are not expanded by this package — §1 lists exactly three
// sources and shell-style substitution is not one of them — so the two secrets arrive the
// way a deployment actually supplies them, through the environment. That is asserted in
// both directions: with the environment the example loads and validates, and without it
// the too-short placeholder is a startup error naming app.webhook_secrets.
func TestLoad_GoldenExample(t *testing.T) {
	path := writeFile(t, "config.yaml", goldenYAML)

	t.Run("loads and validates", func(t *testing.T) {
		isolateEnv(t)
		t.Setenv("ST_APP__WEBHOOK_SECRETS", testSecret)
		t.Setenv("ST_CONTROL__SECRET", testSecret)

		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load = %v, want no error", err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate = %v, want no error", err)
		}

		checks := []struct {
			key  string
			got  any
			want any
		}{
			{"server.listen", cfg.Server.Listen, ":8000"},
			{"server.allowed_origins", cfg.Server.AllowedOrigins, []string{"https://app.example.com"}},
			{"server.ping_interval", cfg.Server.PingInterval.Duration(), 25 * time.Second},
			{"app.name", cfg.App.Name, "main"},
			{"app.connect_url", cfg.App.ConnectURL, "http://webapp:5000/_st/connect"},
			{"app.max_expiry", cfg.App.MaxExpiry.Duration(), 6 * time.Hour},
			{"bus.url", cfg.Bus.URL, "redis://redis:6379/3"},
			{"bus.prefix", cfg.Bus.Prefix, "st:"},
			{"namespaces", []string{cfg.Namespaces[0].Name, cfg.Namespaces[1].Name}, []string{"room", "user"}},
			{"namespaces[0].rate_limit", cfg.Namespaces[0].RateLimit, "10/s"},
			{"limits.max_connections", cfg.Limits.MaxConnections, 25000},
			{"limits.outbound_queue", cfg.Limits.OutboundQueue, 256},
			{"log.level", cfg.Log.Level, "info"},
		}
		for _, c := range checks {
			if !reflect.DeepEqual(c.got, c.want) {
				t.Errorf("%s = %v, want %v", c.key, c.got, c.want)
			}
		}
	})

	t.Run("unsubstituted secret is a startup error", func(t *testing.T) {
		isolateEnv(t)
		_, err := Load(path)
		if err == nil {
			t.Fatalf("Load = nil error, want one")
		}
		if !strings.Contains(err.Error(), "app.webhook_secrets") {
			t.Fatalf("error = %q, want it to name app.webhook_secrets", err)
		}
		if strings.Contains(err.Error(), "${ST_WEBHOOK_SECRET}") {
			t.Fatalf("error = %q, must not quote the value of a secret key (NFR-7)", err)
		}
	})
}

// TestLoad_EnvOnlyExample is the second half of docs/08-config.md §4: the equivalent
// minimum by environment alone. It must load, validate, and permit a subscribe to
// room-4410 through the built-in default block — the failure docs/13-review-findings.md
// M11 records is a gateway that starts, reports healthy, and refuses every subscribe.
func TestLoad_EnvOnlyExample(t *testing.T) {
	isolateEnv(t)
	t.Setenv("ST_SERVER__ALLOWED_ORIGINS", "https://app.example.com")
	t.Setenv("ST_APP__CONNECT_URL", "http://webapp:5000/_st/connect")
	t.Setenv("ST_APP__WEBHOOK_SECRETS", testSecret)
	t.Setenv("ST_CONTROL__SECRET", testSecret)
	t.Setenv("ST_BUS__URL", "redis://redis:6379/3")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load = %v, want no error", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate = %v, want no error", err)
	}
	if cfg.Bus.URL != "redis://redis:6379/3" {
		t.Fatalf("bus.url = %q", cfg.Bus.URL)
	}
	ns, ok := resolveNamespace(cfg, "room-4410")
	if !ok {
		t.Fatalf("resolveNamespace(room-4410) = not found; the env-only example must permit a subscribe")
	}
	if ns.Name != "" {
		t.Fatalf("room-4410 resolved to namespace %q, want the built-in default block", ns.Name)
	}
}

// TestLoad_ErrorNamesTheFile: a validation failure from a file-backed configuration says
// which file it came from, because an operator with three of them cannot act otherwise.
func TestLoad_ErrorNamesTheFile(t *testing.T) {
	minimalEnv(t)
	path := writeFile(t, "config.yaml", "log:\n  level: chatty\n")
	_, err := Load(path)
	if err == nil {
		t.Fatalf("Load = nil error, want one")
	}
	if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "log.level") {
		t.Fatalf("error = %q, want it to name both %q and log.level", err, path)
	}
}

// TestSetField_UnsupportedKind guards the reflective environment walker: a field kind it
// cannot decode is an error, never a silently skipped key. It is unreachable through the
// Config tree today, which is the point — it is what makes adding a float or a map field
// fail loudly rather than quietly.
func TestSetField_UnsupportedKind(t *testing.T) {
	var target struct {
		X float64
		M map[string]string
	}
	v := reflect.ValueOf(&target).Elem()

	for i, name := range []string{"float64", "map"} {
		t.Run(name, func(t *testing.T) {
			if err := setField(v.Field(i), "1.5"); err == nil {
				t.Fatalf("setField(%s) = nil error, want one", name)
			}
		})
	}
}
