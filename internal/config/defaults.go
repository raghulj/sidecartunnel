package config

import "time"

// Built-in namespace values. docs/08-config.md §3 documents them on the namespaces table;
// they are constants here so the built-in default block and a partially specified block
// from a file cannot drift apart.
const (
	// defaultNamespaceName is the reserved namespace a channel with no separator resolves
	// to, and the name the built-in default block carries. It is deliberately "" and not
	// "default": a namespace may not be *named* default, because one block would then
	// govern two disjoint populations — separator-less channels, and channels literally
	// beginning "default-" (docs/06-channels.md §1, docs/13-review-findings.md m5).
	defaultNamespaceName = ""

	// defaultRateLimit is the client-event rate per connection (docs/08-config.md §3).
	defaultRateLimit = "10/s"
)

// defaults returns a Config carrying every default in docs/08-config.md §3.
//
// Three keys have no default and are left zero on purpose — server.allowed_origins,
// app.connect_url, app.webhook_secrets and control.secret. Each is required, and each
// refuses to start when absent rather than being filled in with something convenient
// (docs/05-authorization.md §5, NFR-5).
func defaults() *Config {
	return &Config{
		Server: Server{
			Listen:            ":8000",
			Path:              "/ws",
			HandshakeTimeout:  Duration(5 * time.Second),
			PingInterval:      Duration(25 * time.Second),
			PongTimeout:       Duration(10 * time.Second),
			DrainTimeout:      Duration(20 * time.Second),
			DrainSpread:       Duration(60 * time.Second),
			ReadHeaderTimeout: Duration(5 * time.Second),
		},
		App: App{
			Name:               "app",
			ConnectTimeout:     Duration(10 * time.Second),
			ConnectQueue:       4096,
			WebhookTimeout:     Duration(3 * time.Second),
			WebhookConcurrency: 32,
			WebhookRetries:     1,
			CacheTTL:           Duration(0),
			MinExpiry:          Duration(60 * time.Second),
			MaxExpiry:          Duration(6 * time.Hour),
		},
		Bus: Bus{
			Kind:            "redis",
			URL:             "redis://localhost:6379/0",
			DialTimeout:     Duration(3 * time.Second),
			ReconnectMin:    Duration(200 * time.Millisecond),
			ReconnectMax:    Duration(10 * time.Second),
			IntakeQueue:     4096,
			DispatchWorkers: 2,
			ReadyGrace:      Duration(30 * time.Second),
			Prefix:          "st:",
		},
		Channels: Channels{
			Separator: "-",
		},
		Limits: Limits{
			MaxConnections:          25000,
			MaxSubscriptionsPerConn: 500,
			MaxConnectionsPerUser:   20,
			ReadBuffer:              2048,
			WriteBuffer:             2048,
			OutboundQueue:           256,
			MaxMessageSize:          32768,
			MaxFrameSize:            16384,
			MaxChannelLength:        255,
		},
		Control: Control{
			RefreshSpread: Duration(60 * time.Second),
		},
		Admin: Admin{
			Listen: "127.0.0.1:9001",
		},
		Log: Log{
			Level:  "info",
			Format: "json",
		},
	}
}

// normalizeNamespaces fills in the namespace-level defaults, and installs the built-in
// default block when the list is empty.
//
// The built-in block is what makes the environment-only deployment in docs/08-config.md
// §4 usable. Without it that configuration starts cleanly, reports healthy, accepts
// connections and refuses every single subscribe, because no namespace block matches any
// channel and a channel with no block fails closed (docs/06-channels.md §3,
// docs/13-review-findings.md M11).
//
// It is installed only for a wholly empty list. Once an operator has written namespaces,
// a channel outside them is an error rather than a silently permissive channel — adding a
// catch-all to a configured list would undo exactly the fail-closed behaviour that section
// asks for.
func normalizeNamespaces(c *Config) {
	if len(c.Namespaces) == 0 {
		c.Namespaces = []Namespace{{Name: defaultNamespaceName, RateLimit: defaultRateLimit}}
		return
	}
	for i := range c.Namespaces {
		if c.Namespaces[i].RateLimit == "" {
			c.Namespaces[i].RateLimit = defaultRateLimit
		}
	}
}
