package config

import (
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

// TestValidate is one case per rule in docs/08-config.md §3. Every failing case asserts
// that the message names the offending key, which is what NFR-5 requires and what an
// operator reading a crash loop has to work from.
func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Config)
		// want is the substring the error must contain; empty means the mutation must
		// still validate.
		want string
		// notWant, when set, must NOT appear: no error may quote a secret (NFR-7).
		notWant string
	}{
		// --- server ---
		{name: "server.listen empty", mut: func(c *Config) { c.Server.Listen = "" }, want: "server.listen"},
		{name: "server.listen has no port", mut: func(c *Config) { c.Server.Listen = "localhost" }, want: "server.listen"},
		{name: "server.listen port is not numeric", mut: func(c *Config) { c.Server.Listen = "localhost:http" }, want: "server.listen"},
		{name: "server.listen port out of range", mut: func(c *Config) { c.Server.Listen = ":99999" }, want: "server.listen"},
		{name: "server.path empty", mut: func(c *Config) { c.Server.Path = "" }, want: "server.path"},
		{name: "server.path is relative", mut: func(c *Config) { c.Server.Path = "ws" }, want: "server.path"},
		{name: "server.allowed_origins empty", mut: func(c *Config) { c.Server.AllowedOrigins = nil }, want: "server.allowed_origins"},
		{name: "server.allowed_origins wildcard", mut: func(c *Config) { c.Server.AllowedOrigins = []string{"*"} }, want: "server.allowed_origins"},
		{name: "server.allowed_origins subdomain wildcard", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"https://*.example.com"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins without scheme", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"app.example.com"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins wrong scheme", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"ftp://app.example.com"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins with a path", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"https://app.example.com/app"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins without a host", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"https://"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins unparseable", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"https://exa mple.com"}
		}, want: "server.allowed_origins[0]"},
		{name: "server.allowed_origins second entry", mut: func(c *Config) {
			c.Server.AllowedOrigins = []string{"https://a.example.com", "b.example.com"}
		}, want: "server.allowed_origins[1]"},
		{name: "server.handshake_timeout too small", mut: func(c *Config) { c.Server.HandshakeTimeout = Duration(0) }, want: "server.handshake_timeout"},
		{name: "server.handshake_timeout too large", mut: func(c *Config) { c.Server.HandshakeTimeout = Duration(61 * time.Second) }, want: "server.handshake_timeout"},
		{name: "server.ping_interval too small", mut: func(c *Config) { c.Server.PingInterval = Duration(time.Second) }, want: "server.ping_interval"},
		{name: "server.ping_interval too large", mut: func(c *Config) { c.Server.PingInterval = Duration(301 * time.Second) }, want: "server.ping_interval"},
		{name: "server.pong_timeout too small", mut: func(c *Config) { c.Server.PongTimeout = Duration(0) }, want: "server.pong_timeout"},
		{name: "server.pong_timeout too large", mut: func(c *Config) {
			c.Server.PingInterval = Duration(300 * time.Second)
			c.Server.PongTimeout = Duration(61 * time.Second)
		}, want: "server.pong_timeout"},
		{name: "server.pong_timeout not below ping_interval", mut: func(c *Config) {
			c.Server.PingInterval = Duration(10 * time.Second)
			c.Server.PongTimeout = Duration(10 * time.Second)
		}, want: "server.pong_timeout"},
		{name: "server.drain_timeout too small", mut: func(c *Config) { c.Server.DrainTimeout = Duration(0) }, want: "server.drain_timeout"},
		{name: "server.drain_timeout too large", mut: func(c *Config) { c.Server.DrainTimeout = Duration(301 * time.Second) }, want: "server.drain_timeout"},
		{name: "server.drain_spread negative", mut: func(c *Config) { c.Server.DrainSpread = Duration(-time.Second) }, want: "server.drain_spread"},
		// A spread below a millisecond is not a spread: the consuming code works in
		// whole milliseconds, so 500us is arithmetically zero. Every neighbouring
		// duration key has a documented range and this one did not, so it started
		// cleanly and produced a value the connection layer cannot express.
		{name: "server.drain_spread sub-millisecond", mut: func(c *Config) {
			c.Server.DrainSpread = Duration(500 * time.Microsecond)
		}, want: "server.drain_spread"},
		{name: "server.drain_spread zero", mut: func(c *Config) { c.Server.DrainSpread = Duration(0) }, want: "server.drain_spread"},
		{name: "server.drain_spread too large", mut: func(c *Config) {
			c.Server.DrainSpread = Duration(301 * time.Second)
		}, want: "server.drain_spread"},
		{name: "server.drain_spread at the floor", mut: func(c *Config) { c.Server.DrainSpread = Duration(time.Second) }},
		{name: "server.drain_spread at the ceiling", mut: func(c *Config) {
			c.Server.DrainSpread = Duration(300 * time.Second)
		}},
		{name: "server.read_header_timeout zero", mut: func(c *Config) { c.Server.ReadHeaderTimeout = Duration(0) }, want: "server.read_header_timeout"},
		{name: "server.trusted_proxies not a cidr", mut: func(c *Config) {
			c.Server.TrustedProxies = []string{"10.0.0.1"}
		}, want: "server.trusted_proxies[0]"},
		{name: "server.trusted_proxies valid", mut: func(c *Config) {
			c.Server.TrustedProxies = []string{"10.0.0.0/8", "::1/128"}
		}},

		// --- app ---
		{name: "app.name empty", mut: func(c *Config) { c.App.Name = "" }, want: "app.name"},
		{name: "app.connect_url missing", mut: func(c *Config) { c.App.ConnectURL = "" }, want: "app.connect_url"},
		{name: "app.connect_url relative", mut: func(c *Config) { c.App.ConnectURL = "/_st/connect" }, want: "app.connect_url"},
		{name: "app.connect_url wrong scheme", mut: func(c *Config) { c.App.ConnectURL = "ws://webapp:5000/x" }, want: "app.connect_url"},
		{name: "app.connect_url without host", mut: func(c *Config) { c.App.ConnectURL = "http:///x" }, want: "app.connect_url"},
		{name: "app.connect_url unparseable", mut: func(c *Config) { c.App.ConnectURL = "http://%zz/" }, want: "app.connect_url"},
		// NFR-7. https://gw:hunter2@webapp.internal/_st/connect is an ordinary shape for
		// an internal endpoint, and the gateway formats the configured URL into its own
		// errors. Refused at startup, and refused without quoting the value — including
		// on the unparseable path, where url.Parse's own error text echoes the whole
		// string back, credentials and all.
		{name: "app.connect_url carries credentials", mut: func(c *Config) {
			c.App.ConnectURL = "https://gw:hunter2@webapp.internal/_st/connect"
		}, want: "app.connect_url", notWant: "hunter2"},
		{name: "app.connect_url carries a bare username", mut: func(c *Config) {
			c.App.ConnectURL = "https://gw@webapp.internal/_st/connect"
		}, want: "app.connect_url", notWant: "gw@"},
		{name: "app.connect_url unparseable with credentials", mut: func(c *Config) {
			c.App.ConnectURL = "ht tp://gw:hunter2@webapp.internal/_st/connect"
		}, want: "app.connect_url", notWant: "hunter2"},
		{name: "app.webhook_secrets empty", mut: func(c *Config) { c.App.WebhookSecrets = nil }, want: "app.webhook_secrets"},
		{name: "app.webhook_secrets too short", mut: func(c *Config) {
			c.App.WebhookSecrets = []string{"tooshort"}
		}, want: "app.webhook_secrets[0]", notWant: "tooshort"},
		{name: "app.webhook_secrets second too short", mut: func(c *Config) {
			c.App.WebhookSecrets = []string{testSecret, "s3cr3t"}
		}, want: "app.webhook_secrets[1]", notWant: "s3cr3t"},
		{name: "app.connect_timeout zero", mut: func(c *Config) { c.App.ConnectTimeout = Duration(0) }, want: "app.connect_timeout"},
		{name: "app.connect_queue zero", mut: func(c *Config) { c.App.ConnectQueue = 0 }, want: "app.connect_queue"},
		{name: "app.cookie_names empty entry", mut: func(c *Config) {
			c.App.CookieNames = []string{"session", ""}
		}, want: "app.cookie_names[1]"},
		{name: "app.webhook_timeout too small", mut: func(c *Config) { c.App.WebhookTimeout = Duration(50 * time.Millisecond) }, want: "app.webhook_timeout"},
		{name: "app.webhook_timeout too large", mut: func(c *Config) { c.App.WebhookTimeout = Duration(31 * time.Second) }, want: "app.webhook_timeout"},
		{name: "app.webhook_concurrency too small", mut: func(c *Config) { c.App.WebhookConcurrency = 0 }, want: "app.webhook_concurrency"},
		{name: "app.webhook_concurrency too large", mut: func(c *Config) { c.App.WebhookConcurrency = 4097 }, want: "app.webhook_concurrency"},
		{name: "app.webhook_retries negative", mut: func(c *Config) { c.App.WebhookRetries = -1 }, want: "app.webhook_retries"},
		{name: "app.webhook_retries too large", mut: func(c *Config) { c.App.WebhookRetries = 6 }, want: "app.webhook_retries"},
		{name: "app.cache_ttl negative", mut: func(c *Config) { c.App.CacheTTL = Duration(-time.Second) }, want: "app.cache_ttl"},
		{name: "app.cache_ttl enabled", mut: func(c *Config) { c.App.CacheTTL = Duration(30 * time.Second) }},
		{name: "app.min_expiry zero", mut: func(c *Config) { c.App.MinExpiry = Duration(0) }, want: "app.min_expiry"},
		{name: "app.max_expiry zero", mut: func(c *Config) { c.App.MaxExpiry = Duration(0) }, want: "app.max_expiry"},
		{name: "app.max_expiry below min_expiry", mut: func(c *Config) {
			c.App.MinExpiry = Duration(2 * time.Hour)
			c.App.MaxExpiry = Duration(time.Hour)
		}, want: "app.max_expiry"},

		// --- bus ---
		{name: "bus.kind unknown", mut: func(c *Config) { c.Bus.Kind = "kafka" }, want: "bus.kind"},
		{name: "bus.kind memory needs no url", mut: func(c *Config) {
			c.Bus.Kind = "memory"
			c.Bus.URL = ""
		}},
		{name: "bus.url missing for redis", mut: func(c *Config) { c.Bus.URL = "" }, want: "bus.url"},
		{name: "bus.url wrong scheme", mut: func(c *Config) { c.Bus.URL = "http://redis:6379/0" }, want: "bus.url"},
		{name: "bus.url unparseable", mut: func(c *Config) { c.Bus.URL = "redis://%zz" }, want: "bus.url"},
		// redis://:password@host is the documented way to give the bus a password, so
		// unlike app.connect_url the userinfo is accepted — but it must not reach the
		// error text of a key that is wrong for some other reason (NFR-7).
		{name: "bus.url wrong scheme with a password", mut: func(c *Config) {
			c.Bus.URL = "http://:hunter2@redis:6379/0"
		}, want: "bus.url", notWant: "hunter2"},
		{name: "bus.url unparseable with a password", mut: func(c *Config) {
			c.Bus.URL = "red is://:hunter2@redis:6379/0"
		}, want: "bus.url", notWant: "hunter2"},
		{name: "bus.url with a password", mut: func(c *Config) { c.Bus.URL = "redis://:hunter2@redis:6379/0" }},
		{name: "bus.url rediss", mut: func(c *Config) { c.Bus.URL = "rediss://redis:6379/0" }},
		{name: "bus.dial_timeout zero", mut: func(c *Config) { c.Bus.DialTimeout = Duration(0) }, want: "bus.dial_timeout"},
		{name: "bus.reconnect_min zero", mut: func(c *Config) { c.Bus.ReconnectMin = Duration(0) }, want: "bus.reconnect_min"},
		{name: "bus.reconnect_max zero", mut: func(c *Config) { c.Bus.ReconnectMax = Duration(0) }, want: "bus.reconnect_max"},
		{name: "bus.reconnect_max below min", mut: func(c *Config) {
			c.Bus.ReconnectMin = Duration(10 * time.Second)
			c.Bus.ReconnectMax = Duration(time.Second)
		}, want: "bus.reconnect_max"},
		{name: "bus.intake_queue zero", mut: func(c *Config) { c.Bus.IntakeQueue = 0 }, want: "bus.intake_queue"},
		{name: "bus.dispatch_workers zero", mut: func(c *Config) { c.Bus.DispatchWorkers = 0 }, want: "bus.dispatch_workers"},
		{name: "bus.dispatch_workers too many", mut: func(c *Config) { c.Bus.DispatchWorkers = 65 }, want: "bus.dispatch_workers"},
		{name: "bus.ready_grace negative", mut: func(c *Config) { c.Bus.ReadyGrace = Duration(-time.Second) }, want: "bus.ready_grace"},
		{name: "bus.prefix empty", mut: func(c *Config) { c.Bus.Prefix = "" }, want: "bus.prefix"},
		{name: "bus.prefix with whitespace", mut: func(c *Config) { c.Bus.Prefix = "st :" }, want: "bus.prefix"},

		// --- channels ---
		{name: "channels.separator empty", mut: func(c *Config) { c.Channels.Separator = "" }, want: "channels.separator"},
		{name: "channels.separator too long", mut: func(c *Config) { c.Channels.Separator = "--" }, want: "channels.separator"},
		{name: "channels.separator not printable", mut: func(c *Config) { c.Channels.Separator = "\n" }, want: "channels.separator"},
		{name: "channels.separator is a space", mut: func(c *Config) { c.Channels.Separator = " " }, want: "channels.separator"},
		{name: "channels.separator not ascii", mut: func(c *Config) { c.Channels.Separator = "é" }, want: "channels.separator"},
		{name: "channels.separator colon", mut: func(c *Config) { c.Channels.Separator = ":" }},

		// --- namespaces ---
		{name: "namespaces name default", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "default", RateLimit: "10/s"}}
		}, want: "namespaces[0].name"},
		{name: "namespaces duplicate names", mut: func(c *Config) {
			c.Namespaces = []Namespace{
				{Name: "room", RateLimit: "10/s"},
				{Name: "room", RateLimit: "10/s"},
			}
		}, want: "namespaces[1].name"},
		{name: "namespaces duplicate default blocks", mut: func(c *Config) {
			c.Namespaces = []Namespace{{RateLimit: "10/s"}, {RateLimit: "10/s"}}
		}, want: "namespaces[1].name"},
		{name: "namespaces name contains the separator", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room-a", RateLimit: "10/s"}}
		}, want: "namespaces[0].name"},
		{name: "namespaces name is reserved", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "_control", RateLimit: "10/s"}}
		}, want: "namespaces[0].name"},
		{name: "namespaces name not printable ascii", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "ro om", RateLimit: "10/s"}}
		}, want: "namespaces[0].name"},
		{name: "namespaces reserved empty name is the default block", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "", RateLimit: "10/s"}, {Name: "room", RateLimit: "10/s"}}
		}},
		{name: "namespaces rate_limit empty", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room"}}
		}, want: "namespaces[0].rate_limit"},
		{name: "namespaces rate_limit has no unit", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10"}}
		}, want: "namespaces[0].rate_limit"},
		{name: "namespaces rate_limit bad unit", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/h"}}
		}, want: "namespaces[0].rate_limit"},
		{name: "namespaces rate_limit not a number", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "ten/s"}}
		}, want: "namespaces[0].rate_limit"},
		{name: "namespaces rate_limit zero", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "0/s"}}
		}, want: "namespaces[0].rate_limit"},
		{name: "namespaces rate_limit per minute", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "600/m"}}
		}},
		{name: "namespaces max_message_size zero", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", MaxMessageSize: ptr(0)}}
		}, want: "namespaces[0].max_message_size"},
		{name: "namespaces max_message_size too large", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", MaxMessageSize: ptr(1 << 21)}}
		}, want: "namespaces[0].max_message_size"},
		{name: "namespaces max_message_size set", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", MaxMessageSize: ptr(4096)}}
		}},
		{name: "namespaces presence", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", Presence: true}}
		}, want: "namespaces[0].presence"},
		{name: "namespaces history_size", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", HistorySize: 10}}
		}, want: "namespaces[0].history_size"},
		{name: "namespaces client_events", mut: func(c *Config) {
			c.Namespaces = []Namespace{{Name: "room", RateLimit: "10/s", ClientEvents: true}}
		}},
		{name: "namespaces empty list", mut: func(c *Config) { c.Namespaces = nil }, want: "namespaces"},

		// --- limits ---
		{name: "limits.max_connections negative", mut: func(c *Config) { c.Limits.MaxConnections = -1 }, want: "limits.max_connections"},
		{name: "limits.max_connections unlimited", mut: func(c *Config) { c.Limits.MaxConnections = 0 }},
		{name: "limits.max_subscriptions_per_conn zero", mut: func(c *Config) { c.Limits.MaxSubscriptionsPerConn = 0 }, want: "limits.max_subscriptions_per_conn"},
		{name: "limits.max_subscriptions_per_conn too large", mut: func(c *Config) { c.Limits.MaxSubscriptionsPerConn = 10001 }, want: "limits.max_subscriptions_per_conn"},
		{name: "limits.max_connections_per_user negative", mut: func(c *Config) { c.Limits.MaxConnectionsPerUser = -1 }, want: "limits.max_connections_per_user"},
		{name: "limits.max_connections_per_user unlimited", mut: func(c *Config) { c.Limits.MaxConnectionsPerUser = 0 }},
		{name: "limits.read_buffer zero", mut: func(c *Config) { c.Limits.ReadBuffer = 0 }, want: "limits.read_buffer"},
		{name: "limits.read_buffer too large", mut: func(c *Config) { c.Limits.ReadBuffer = 1 << 21 }, want: "limits.read_buffer"},
		{name: "limits.write_buffer zero", mut: func(c *Config) { c.Limits.WriteBuffer = 0 }, want: "limits.write_buffer"},
		{name: "limits.write_buffer too large", mut: func(c *Config) { c.Limits.WriteBuffer = 1 << 21 }, want: "limits.write_buffer"},
		{name: "limits.outbound_queue too small", mut: func(c *Config) { c.Limits.OutboundQueue = 15 }, want: "limits.outbound_queue"},
		{name: "limits.outbound_queue too large", mut: func(c *Config) { c.Limits.OutboundQueue = 65537 }, want: "limits.outbound_queue"},
		{name: "limits.max_message_size zero", mut: func(c *Config) { c.Limits.MaxMessageSize = 0 }, want: "limits.max_message_size"},
		{name: "limits.max_message_size too large", mut: func(c *Config) { c.Limits.MaxMessageSize = 1 << 21 }, want: "limits.max_message_size"},
		{name: "limits.max_frame_size zero", mut: func(c *Config) { c.Limits.MaxFrameSize = 0 }, want: "limits.max_frame_size"},
		{name: "limits.max_frame_size too large", mut: func(c *Config) { c.Limits.MaxFrameSize = 1 << 21 }, want: "limits.max_frame_size"},
		{name: "limits.max_channel_length too small", mut: func(c *Config) { c.Limits.MaxChannelLength = 15 }, want: "limits.max_channel_length"},
		{name: "limits.max_channel_length too large", mut: func(c *Config) { c.Limits.MaxChannelLength = 1025 }, want: "limits.max_channel_length"},

		// --- control ---
		{name: "control.secret missing", mut: func(c *Config) { c.Control.Secret = "" }, want: "control.secret"},
		{name: "control.secret too short", mut: func(c *Config) {
			c.Control.Secret = "0123456789abcdef0123456789abcde"
		}, want: "control.secret", notWant: "0123456789abcdef0123456789abcde"},

		// --- server.listen ---
		//
		// There is one listener. The rule that admin.listen had to be a different socket
		// from server.listen went with it (docs/12-roadmap.md §2), so what is left is
		// that the address is bindable at all.
		{name: "server.listen empty", mut: func(c *Config) { c.Server.Listen = "" }, want: "server.listen"},
		{name: "server.listen has no port", mut: func(c *Config) { c.Server.Listen = "0.0.0.0" }, want: "server.listen"},
		{name: "server.listen port is not a number", mut: func(c *Config) { c.Server.Listen = ":http" }, want: "server.listen"},
		{name: "server.listen port out of range", mut: func(c *Config) { c.Server.Listen = ":70000" }, want: "server.listen"},
		{name: "server.listen ephemeral", mut: func(c *Config) { c.Server.Listen = ":0" }},
		{name: "server.listen explicit host", mut: func(c *Config) { c.Server.Listen = "10.0.0.1:8000" }},

		// --- log ---
		{name: "log.level unknown", mut: func(c *Config) { c.Log.Level = "chatty" }, want: "log.level"},
		{name: "log.level debug", mut: func(c *Config) { c.Log.Level = "debug" }},
		{name: "log.level warn", mut: func(c *Config) { c.Log.Level = "warn" }},
		{name: "log.level error", mut: func(c *Config) { c.Log.Level = "error" }},
		{name: "log.format unknown", mut: func(c *Config) { c.Log.Format = "xml" }, want: "log.format"},
		{name: "log.format text", mut: func(c *Config) { c.Log.Format = "text" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadMinimal(t)
			tt.mut(cfg)
			err := cfg.Validate()

			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want no error", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate = nil error, want one naming %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate = %q, want it to name %q", err, tt.want)
			}
			if tt.notWant != "" && strings.Contains(err.Error(), tt.notWant) {
				t.Fatalf("Validate = %q, must not quote the value of a secret key (NFR-7)", err)
			}
		})
	}
}

// TestValidate_ValidBaseline is the positive control: the documented minimum validates,
// so every failure above is caused by its own mutation and nothing else.
func TestValidate_ValidBaseline(t *testing.T) {
	cfg := loadMinimal(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate = %v, want no error", err)
	}
}

// TestRedactedURL is the NFR-7 helper on its own, because every caller of it is a place a
// credential would otherwise reach a log line, and "some other package's test happens to
// cover this branch" is not a property worth depending on.
func TestRedactedURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"no credentials is unchanged", "https://webapp.internal/_st/connect", "https://webapp.internal/_st/connect"},
		{"password", "https://gw:hunter2@webapp.internal/_st/connect", "https://redacted@webapp.internal/_st/connect"},
		{"username alone", "https://gw@webapp.internal/_st/connect", "https://redacted@webapp.internal/_st/connect"},
		{"redis password with no username", "redis://:hunter2@redis:6379/0", "redis://redacted@redis:6379/0"},
		// Not echoed: the reason it does not parse may be the credential in it.
		{"unparseable", "ht tp://gw:hunter2@webapp.internal/x", "[unparseable URL]"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RedactedURL(tt.raw); got != tt.want {
				t.Fatalf("RedactedURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
