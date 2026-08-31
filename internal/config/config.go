package config

import "fmt"

// Config is the whole configuration surface. Every key in docs/08-config.md §3 appears
// here exactly once, with its documented default recorded on the field.
//
// It is passed explicitly to whatever needs it. There is no package-level instance and
// there must not be one: a global makes two tests that configure differently impossible
// to run in the same process, and it hides which component depends on which key.
type Config struct {
	// Server is the client-facing listener and the websocket lifecycle.
	Server Server `yaml:"server"`

	// App is the single consuming application. One gateway process serves exactly one
	// application: multi-app was cut because it was not merely unfinished but unsound —
	// the hub cross-delivered between apps, limits and origins were global while app was
	// a list, and nothing specified how an inbound socket picked one. A second
	// application is a second container, which is free with a static binary
	// (docs/13-review-findings.md S1).
	App App `yaml:"app"`

	// Bus is the replica-to-replica transport.
	Bus Bus `yaml:"bus"`

	// Channels is channel-name parsing.
	Channels Channels `yaml:"channels"`

	// Namespaces are the per-namespace configuration blocks, selected by the substring
	// before a channel's first separator.
	//
	// This is a list of objects, which cannot be expressed in environment variables, so
	// it needs either the YAML file or ST_NAMESPACES_JSON carrying the whole list as
	// JSON. When it is empty a built-in default block applies to every channel — without
	// that, the minimal environment-only configuration starts cleanly, reports healthy,
	// and refuses every single subscribe (docs/13-review-findings.md M11).
	Namespaces []Namespace `yaml:"namespaces"`

	// Limits are the resource ceilings.
	Limits Limits `yaml:"limits"`

	// Control is the signed control channel.
	Control Control `yaml:"control"`

	// Log is logging output.
	Log Log `yaml:"log"`
}

// Server configures the client-facing listener and the websocket lifecycle.
type Server struct {
	// Listen is the client listen address. Default ":8000". Must be a valid listen
	// address.
	Listen string `yaml:"listen"`

	// Path is the websocket endpoint path. Default "/ws". Must begin with "/". FR-1.
	Path string `yaml:"path"`

	// AllowedOrigins is the exact-match Origin allowlist, including scheme. Required and
	// non-empty; there is no default. No wildcards, no suffix matching, no "ends with
	// .example.com" — subdomain wildcards are how an attacker who controls one forgotten
	// subdomain gets everything.
	//
	// This is the most important key in the file. Browsers do not apply CORS to websocket
	// handshakes but do attach cookies, so without this check a page on evil.example
	// opens a socket to the gateway, the browser attaches the victim's session cookie,
	// the application answers correctly, and the attacker's page receives that user's
	// entire realtime stream. FR-2, docs/05-authorization.md §5.
	AllowedOrigins []string `yaml:"allowed_origins"`

	// AllowMissingOrigin accepts an upgrade carrying no Origin header. Default false.
	//
	// It exists for non-browser clients, which send none. Turning it on removes the
	// defense for browsers too, so it should be paired with a source-address allowlist at
	// the proxy (docs/05-authorization.md §5).
	AllowMissingOrigin bool `yaml:"allow_missing_origin"`

	// HandshakeTimeout is how long the gateway waits for the connect frame after a
	// successful upgrade. Default 5s, range 1s–60s.
	//
	// It covers only receipt of that frame — the part the client controls. The
	// authorization that follows has its own, longer budget in App.ConnectTimeout, and
	// exceeding that closes retryably. Conflating the two turns a slow application into a
	// permanent, non-retryable lockout of every reconnecting user, which is why FR-4
	// asserts both paths separately.
	HandshakeTimeout Duration `yaml:"handshake_timeout"`

	// PingInterval is the gap between protocol-level pings. Default 25s, range 5s–300s.
	// It must be below any proxy idle timeout, or the proxy reaps healthy sockets every
	// interval and you will chase it for an afternoon. FR-7.
	PingInterval Duration `yaml:"ping_interval"`

	// PongTimeout is how long a pong may take before the connection is closed with
	// proto.ClosePingTimeout. Default 10s, range 1s–60s, and must be less than
	// PingInterval. FR-7.
	PongTimeout Duration `yaml:"pong_timeout"`

	// DrainTimeout is the ceiling on graceful shutdown. Default 20s, range 1s–300s.
	// FR-19.
	DrainTimeout Duration `yaml:"drain_timeout"`

	// DrainSpread is the window across which drain retry_after values are spread
	// uniformly. Default 60s.
	//
	// At 10,000 connections per replica and 40 ms auth latency, 60s yields ~7 concurrent
	// requests at the application; 30s yields ~13, which is tight against a 16-worker
	// pool; no spread at all yields ~400 in a one-second window, which is a full
	// application outage. This key is not an optimization — it is the thing that makes
	// this scale work on a request/response application (docs/10-operations.md §8).
	//
	// docs/13-review-findings.md S5 says 30s; docs/08-config.md §3 says 60s and is
	// normative, so 60s.
	DrainSpread Duration `yaml:"drain_spread"`

	// ReadHeaderTimeout guards the upgrade against slowloris. Default 5s.
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"`

	// TrustedProxies are CIDRs whose X-Forwarded-For is believed. Default empty, which
	// trusts nothing, so X-St-Forwarded-For is the socket peer address.
	//
	// A client-supplied X-Forwarded-For from an untrusted peer is discarded, never
	// forwarded. Passing it through would let an attacker send X-Forwarded-For: 127.0.0.1
	// and hit an application's localhost trust path from the public internet — an auth
	// bypass in the application, delivered by the gateway, under a header prefix implying
	// the gateway vouched for it. FR-24.
	TrustedProxies []string `yaml:"trusted_proxies"`
}

// App configures the single consuming application and its connect webhook.
type App struct {
	// Name is used in log lines. Default "app".
	Name string `yaml:"name"`

	// ConnectURL is the connect webhook. Required; must be an absolute http or https URL.
	// FR-3.
	ConnectURL string `yaml:"connect_url"`

	// WebhookSecrets sign the connect webhook request. Required, minimum 32 bytes each.
	//
	// The gateway signs with the first; the list exists so a secret can be rotated
	// without simultaneous restarts of the gateway and the application
	// (docs/13-review-findings.md m10).
	//
	// Never log an element of this slice.
	WebhookSecrets []string `yaml:"webhook_secrets"`

	// ConnectTimeout is the whole authorization budget: queue wait plus the call.
	// Default 10s. Exceeding it closes with proto.CloseAuthUnavailable, which is
	// retryable. NFR-4.
	ConnectTimeout Duration `yaml:"connect_timeout"`

	// ConnectQueue caps how many connections may wait for authorization. Default 4096.
	// Overflow closes with proto.CloseAuthUnavailable.
	//
	// An unbounded queue is 25,000 half-open sockets each holding a captured cookie,
	// which is both a memory problem and a security one (NFR-4).
	ConnectQueue int `yaml:"connect_queue"`

	// CookieNames are the cookies that form the webhook cache key. Default empty, meaning
	// the whole Cookie header.
	//
	// Hashing the whole header is why the cache did not deduplicate tabs: _ga, _fbp and
	// CSRF tokens differ per tab, so two tabs of one user miss each other anyway
	// (docs/13-review-findings.md C4).
	CookieNames []string `yaml:"cookie_names"`

	// WebhookTimeout is the per-call timeout. Default 3s, range 100ms–30s.
	WebhookTimeout Duration `yaml:"webhook_timeout"`

	// WebhookConcurrency caps concurrent outbound webhook calls. Default 32, range
	// 1–4096.
	//
	// Excess connections wait inside the gateway, where waiting is cheap, rather than
	// being issued at an application with a fixed worker pool. A reconnect after a
	// replica restart is N simultaneous authentications and N is every connected user
	// (NFR-4).
	WebhookConcurrency int `yaml:"webhook_concurrency"`

	// WebhookRetries is the retry count for a failed call. Default 1, range 0–5.
	//
	// It applies to 5xx and timeout only, never to 401 or 403. Retrying a refusal turns a
	// revocation into a denial-of-service against the application (FR-6).
	WebhookRetries int `yaml:"webhook_retries"`

	// CacheTTL caches webhook answers, keyed on a hash of the cookies named by
	// CookieNames. Default 0, off.
	//
	// Off is right. It is worth less than it sounds — N reconnecting users are N distinct
	// cookies and therefore N cache misses — and it costs revocation latency, because a
	// cached entry otherwise survives a revocation and a suspended user reconnecting
	// within the TTL gets their pre-revocation grants back. When enabled, any control
	// disconnect flushes the entire cache (docs/13-review-findings.md C4).
	CacheTTL Duration `yaml:"cache_ttl"`

	// MinExpiry clamps the webhook's expires_in from below. Default 60s.
	MinExpiry Duration `yaml:"min_expiry"`

	// MaxExpiry clamps the webhook's expires_in from above. Default 6h. The clamped value
	// is what the client is told, not the application's raw one.
	//
	// 6h rather than an hour is safe because revocation no longer depends on expiry — the
	// control channel is immediate — and each expiry costs a full reconnect. Long expiry
	// plus immediate revocation is strictly better than short expiry, which the earlier
	// design was quietly using as a revocation mechanism it was bad at
	// (docs/13-review-findings.md S3).
	MaxExpiry Duration `yaml:"max_expiry"`
}

// Bus configures the replica-to-replica transport.
type Bus struct {
	// Kind is "redis" or "memory". Default "redis".
	//
	// memory is single-process only and exists for tests and single-node development.
	// Starting with memory and more than one replica is undetectable by the gateway and
	// produces messages that arrive for some users and not others, so it logs a prominent
	// warning at startup every time.
	Kind string `yaml:"kind"`

	// URL is the Redis URL. Default "redis://localhost:6379/0". Required when Kind is
	// "redis".
	//
	// Use a dedicated database index: sharing one with a cache means someone's FLUSHDB
	// takes realtime down with it, or at least takes the blame for it
	// (docs/10-operations.md §3).
	URL string `yaml:"url"`

	// DialTimeout is the connection timeout. Default 3s.
	DialTimeout Duration `yaml:"dial_timeout"`

	// ReconnectMin is the backoff floor after bus loss. Default 200ms. NFR-8.
	ReconnectMin Duration `yaml:"reconnect_min"`

	// ReconnectMax is the backoff ceiling after bus loss. Default 10s. NFR-8.
	ReconnectMax Duration `yaml:"reconnect_max"`

	// IntakeQueue is the depth between the bus reader goroutine and the dispatch workers.
	// Default 4096. A full intake drops rather than blocks, and every drop is logged;
	// a run of those lines means the workers are behind the reader.
	IntakeQueue int `yaml:"intake_queue"`

	// DispatchWorkers is the number of fan-out workers. Default 2, range 1–64.
	//
	// They are kept separate from the bus reader so Redis never evicts us for a slow
	// subscriber: the reader does nothing but drain the socket into IntakeQueue
	// (docs/13-review-findings.md M8).
	//
	// docs/09-internals.md §5 and docs/13-review-findings.md M8 say 4;
	// docs/08-config.md §3 says 2 and is normative, so 2.
	DispatchWorkers int `yaml:"dispatch_workers"`

	// ReadyGrace is how long the bus may be down before /ready reports 503. Default 30s.
	//
	// It exists so a short blip does not pull the whole fleet from the load balancer at
	// once. An eight-second Redis restart should be invisible
	// (docs/13-review-findings.md M20).
	ReadyGrace Duration `yaml:"ready_grace"`

	// Prefix is the Redis channel prefix, and also the hub's internal key prefix.
	// Default "st:".
	//
	// The hub keys subscriptions by {prefix}{channel}, never the bare channel name. That
	// is what makes cross-delivery structurally impossible rather than a filter someone
	// has to remember to apply, and it is what keeps restoring multi-app cheap (FR-21).
	Prefix string `yaml:"prefix"`
}

// Channels configures channel-name parsing.
type Channels struct {
	// Separator splits the namespace from the rest of a channel name at its first
	// occurrence. Default "-". Exactly one printable ASCII character.
	Separator string `yaml:"separator"`
}

// Namespace is one namespace configuration block. docs/06-channels.md §3.
//
// There is deliberately no auth_required key. An earlier draft had auth_required: false
// for genuinely public broadcasts; it contradicted FR-5, and both statements were citable
// from documents claiming authority. Worse, it reintroduced the hole that makes hosted
// public channels unusable: a namespace where knowing a name is the same as being allowed
// to read it, one config key away from any channel. A public broadcast is now expressed
// by the application putting "status" — or whatever it is called — in every connection's
// grant list (docs/13-review-findings.md S4).
//
// docs/08-config.md §3's table still lists auth_required. That is a leftover; S4 removed
// the key, and strict decoding means a config that still sets it fails startup naming it
// rather than pretending to honour it.
type Namespace struct {
	// Name is the namespace, matched against the substring before a channel's first
	// separator. Required, and unique across the list.
	//
	// It may not be "default". A channel with no separator resolves to the reserved name
	// "", which the built-in default block owns; naming a namespace "default" would make
	// one config block govern two disjoint populations — separator-less channels, and
	// channels literally beginning "default-" (docs/13-review-findings.md m5).
	Name string `yaml:"name"`

	// ClientEvents permits the publish command from clients on these channels. Default
	// false. M4.
	//
	// A client event requires both this and a grant matching the channel. The flag alone
	// would let any connected client inject fabricated events into a channel it cannot
	// even read (docs/13-review-findings.md M19).
	ClientEvents bool `yaml:"client_events"`

	// RateLimit is the client-event rate per connection, as "<int>/<s|m>". Default
	// "10/s". Only meaningful with ClientEvents.
	RateLimit string `yaml:"rate_limit"`

	// MaxMessageSize overrides Limits.MaxMessageSize for this namespace, in bytes. Range
	// 1–1 MiB. nil inherits.
	//
	// It is a pointer because 0 is not a usable sentinel here: an explicit 0 must be a
	// validation error naming the key, not a silent fall-through to the global limit.
	MaxMessageSize *int `yaml:"max_message_size"`

	// Presence tracks and broadcasts membership. Default false. M4, not built: setting it
	// is a startup error.
	//
	// An error rather than a warning, because a config that claims presence is on while
	// presence does nothing is a lie an operator will act on.
	Presence bool `yaml:"presence"`

	// HistorySize is a bounded replay buffer. Default 0. M4, not built: a non-zero value
	// is a startup error, for the same reason as Presence.
	HistorySize int `yaml:"history_size"`
}

// Limits are the resource ceilings.
type Limits struct {
	// MaxConnections caps concurrent connections on this replica; 0 is unlimited. Default
	// 25000. Over the limit, the upgrade answers HTTP 503.
	//
	// Sized for 20,000 concurrent across two replicas, either able to carry the whole
	// fleet alone during a rolling deploy (NFR-1).
	MaxConnections int `yaml:"max_connections"`

	// MaxSubscriptionsPerConn caps one connection's subscriptions. Default 500, range
	// 1–10000. Exceeding it is proto.ErrSubscriptionLimit.
	MaxSubscriptionsPerConn int `yaml:"max_subscriptions_per_conn"`

	// MaxConnectionsPerUser caps concurrent connections for one user id; 0 is unlimited.
	// Default 20. One looping client must not consume the global cap
	// (docs/13-review-findings.md m8).
	MaxConnectionsPerUser int `yaml:"max_connections_per_user"`

	// ReadBuffer is the socket read buffer in bytes. Default 2048.
	//
	// Library defaults of 4 KiB each are the difference between fitting the memory budget
	// and not: NFR-1 is 15,000 connections in 1 GiB at ~35 KB each, and leaving these at
	// a library default roughly doubles the per-connection cost.
	ReadBuffer int `yaml:"read_buffer"`

	// WriteBuffer is the socket write buffer in bytes. Default 2048. See ReadBuffer.
	WriteBuffer int `yaml:"write_buffer"`

	// Compression enables permessage-deflate. Default false.
	//
	// Each compression context is ~256 KiB; at 20,000 connections that is 5 GiB against a
	// 1 GiB budget. Leave it off unless measured (docs/13-review-findings.md m9).
	Compression bool `yaml:"compression"`

	// OutboundQueue is the per-connection outbound queue depth in messages. Default 256,
	// range 16–65536. On overflow the connection is closed with proto.CloseSlowConsumer
	// rather than the publisher being blocked (FR-15).
	//
	// Depth is cheaper than it looks: the queue holds pointers to one shared immutable
	// buffer, so 256 costs ~4 KiB per connection, not 256 × message size
	// (docs/09-internals.md §5).
	OutboundQueue int `yaml:"outbound_queue"`

	// MaxMessageSize caps a published envelope in bytes. Default 32768, range 1–1 MiB.
	// An oversize envelope is dropped and logged once with the channel name, never with
	// the payload (FR-14).
	MaxMessageSize int `yaml:"max_message_size"`

	// MaxFrameSize caps an inbound client frame in bytes. Default 16384, range 1–1 MiB.
	// An oversize frame closes the connection with proto.CloseProtocolError.
	MaxFrameSize int `yaml:"max_frame_size"`

	// MaxChannelLength caps a channel name in bytes. Default 255, range 16–1024. A longer
	// name is refused with proto.ErrBadRequest (docs/07-delivery.md §7).
	MaxChannelLength int `yaml:"max_channel_length"`
}

// Control configures the signed control channel, {bus.prefix}_control.
type Control struct {
	// Secret signs control envelopes. Required, minimum 32 bytes. FR-23.
	//
	// The read-only connect webhook was signed while the operation that can disconnect
	// every user on every replica — and, via refresh, stampede the application — was not.
	// That asymmetry was indefensible (docs/13-review-findings.md C8).
	//
	// Never log this value.
	Secret string `yaml:"secret"`

	// RefreshSpread is the window over which a mass control refresh is spread. Default
	// 60s.
	//
	// docs/13-review-findings.md C8 says 30s; docs/08-config.md §3 says 60s and is
	// normative, so 60s.
	RefreshSpread Duration `yaml:"refresh_spread"`
}

// Log configures logging output.
type Log struct {
	// Level is "debug", "info", "warn" or "error". Default "info".
	//
	// No level ever emits a cookie, an Authorization header, a webhook body, or a message
	// payload. debug adds frame types and channel names, never contents (NFR-7).
	Level string `yaml:"level"`

	// Format is "json" or "text". Default "json".
	Format string `yaml:"format"`
}

// Load reads the configuration from path, applies defaults and environment overrides, and
// validates the result.
//
// Precedence, later winning: built-in defaults, then the YAML file, then the environment.
// path may be empty, in which case defaults and environment alone are used — that is the
// documented minimal deployment (docs/08-config.md §4).
//
// Environment keys use the ST_ prefix with __ for nesting, e.g. ST_SERVER__PATH=/socket.
// Scalar lists are comma-separated. Any key may be supplied from a file by appending
// _FILE, e.g. ST_APP__WEBHOOK_SECRETS_FILE=/run/secrets/st, which is the convention for
// Docker and Swarm secrets.
//
// Namespaces is a list of objects and therefore cannot be expressed as environment
// scalars. It comes from the YAML file, or from ST_NAMESPACES_JSON carrying the whole
// list as JSON. When it ends up empty, a built-in default block applies to every channel.
//
// Decoding is strict: an unrecognised key is an error naming it. A typo, or a key some
// design decision removed, must fail loudly rather than sit in the file doing nothing.
//
// Load calls Validate before returning, so a non-nil *Config is always a usable one.
// Errors are wrapped with the path and, where a rule is broken, the offending key
// (NFR-5). An error from Load must never quote the value of a secret key.
func Load(path string) (*Config, error) {
	c := defaults()
	if path != "" {
		if err := loadFile(c, path); err != nil {
			return nil, wrapConfigErr(path, err)
		}
	}
	if err := applyEnv(c); err != nil {
		return nil, wrapConfigErr(path, err)
	}
	normalizeNamespaces(c)
	if err := c.Validate(); err != nil {
		return nil, wrapConfigErr(path, err)
	}
	return c, nil
}

// wrapConfigErr names the file the configuration came from, where there was one. An
// operator with three config files and a crash loop cannot act on a message that names
// only the key.
func wrapConfigErr(path string, err error) error {
	if path == "" {
		return fmt.Errorf("config: %w", err)
	}
	return fmt.Errorf("config %s: %w", path, err)
}

// Validate checks every rule in docs/08-config.md §3 and returns the first failure, with
// a message naming the offending key.
//
// The process must not start in a partially-configured state, so callers treat any error
// as fatal (NFR-5). Each rule gets its own test asserting that the message names the key;
// server.allowed_origins gets its own test in particular, because it is the rule most
// likely to be relaxed for convenience by someone in a hurry.
//
// Validate is a pure function of the receiver: it performs no I/O, resolves no DNS, and
// dials nothing. A validator that reaches the network turns a config typo into a timeout
// and makes startup depend on a service being up.
func (c *Config) Validate() error {
	if err := validateServer(&c.Server); err != nil {
		return err
	}
	if err := validateApp(&c.App); err != nil {
		return err
	}
	if err := validateBus(&c.Bus); err != nil {
		return err
	}
	if err := validateChannels(&c.Channels); err != nil {
		return err
	}
	if err := validateNamespaces(c.Namespaces, c.Channels.Separator); err != nil {
		return err
	}
	if err := validateLimits(&c.Limits); err != nil {
		return err
	}
	if err := validateControl(&c.Control); err != nil {
		return err
	}
	return validateLog(&c.Log)
}
