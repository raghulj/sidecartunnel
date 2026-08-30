package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Limits shared by several rules. docs/08-config.md §3 states the 1 MiB message ceiling
// and the 32-byte secret floor in more than one place; they are constants so the places
// cannot disagree.
const (
	maxMessageBytes = 1 << 20
	minSecretBytes  = 32
	maxPort         = 65535

	// wildcardHost is the normalized form of a listen host that binds every interface.
	// admin.listen "127.0.0.1:9001" and server.listen ":9001" are the same socket, and a
	// plain string comparison of the two would miss it.
	wildcardHost = "*"
)

// validateServer checks docs/08-config.md §3's server table. server.listen is checked in
// validateListeners, which needs admin.listen in scope as well.
func validateServer(s *Server) error {
	if s.Path == "" || !strings.HasPrefix(s.Path, "/") {
		return fmt.Errorf("server.path is %q, which must begin with %q (FR-1)", s.Path, "/")
	}
	if len(s.AllowedOrigins) == 0 {
		return errors.New("server.allowed_origins is empty — refusing to start; " +
			"every accepted Origin must be listed explicitly (docs/05-authorization.md §5)")
	}
	for i, origin := range s.AllowedOrigins {
		if err := validateOrigin(origin); err != nil {
			return fmt.Errorf("server.allowed_origins[%d] %q %w", i, origin, err)
		}
	}
	if err := durRange("server.handshake_timeout", s.HandshakeTimeout, time.Second, 60*time.Second); err != nil {
		return err
	}
	if err := durRange("server.ping_interval", s.PingInterval, 5*time.Second, 300*time.Second); err != nil {
		return err
	}
	if err := durRange("server.pong_timeout", s.PongTimeout, time.Second, 60*time.Second); err != nil {
		return err
	}
	if s.PongTimeout >= s.PingInterval {
		// A pong deadline at or beyond the next ping never fires before the ping that
		// would have reset it, so a dead peer is never detected (FR-7).
		return fmt.Errorf("server.pong_timeout is %s, which must be less than server.ping_interval %s (FR-7)",
			s.PongTimeout, s.PingInterval)
	}
	if err := durRange("server.drain_timeout", s.DrainTimeout, time.Second, 300*time.Second); err != nil {
		return err
	}
	if err := durNonNegative("server.drain_spread", s.DrainSpread); err != nil {
		return err
	}
	if err := durPositive("server.read_header_timeout", s.ReadHeaderTimeout); err != nil {
		return err
	}
	for i, cidr := range s.TrustedProxies {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("server.trusted_proxies[%d] %q is not a CIDR: %w", i, cidr, err)
		}
	}
	return nil
}

// validateOrigin enforces exact origins. No wildcards and no suffix matching: browsers do
// not apply CORS to websocket handshakes but do attach cookies, so this list is the only
// thing between a logged-in user and cross-site websocket hijacking, and a subdomain
// wildcard is how an attacker who owns one forgotten subdomain gets everything
// (FR-2, docs/05-authorization.md §5).
//
// The returned error is a fragment: the caller prefixes the key and the value.
func validateOrigin(origin string) error {
	if strings.Contains(origin, "*") {
		return errors.New("contains a wildcard; origins are matched exactly (docs/05-authorization.md §5)")
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("is not a URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("must include an http or https scheme, as a browser Origin header does")
	}
	if parsed.Host == "" {
		return errors.New("has no host")
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return errors.New("must be scheme://host[:port] with no path, query, fragment or credentials")
	}
	return nil
}

// validateApp checks docs/08-config.md §3's app table. No message here quotes a secret
// (NFR-7): a length is reported, never a value.
func validateApp(a *App) error {
	if a.Name == "" {
		return errors.New("app.name is empty; it labels every metric and log line (docs/08-config.md §3)")
	}
	if a.ConnectURL == "" {
		return errors.New("app.connect_url is required; there is no authorization without it (FR-3)")
	}
	parsed, err := url.Parse(a.ConnectURL)
	if err != nil {
		return fmt.Errorf("app.connect_url %q is not a URL: %w", a.ConnectURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("app.connect_url %q must be an absolute http or https URL (FR-3)", a.ConnectURL)
	}
	if len(a.WebhookSecrets) == 0 {
		return errors.New("app.webhook_secrets is empty; the connect webhook must be signed (FR-3)")
	}
	for i, secret := range a.WebhookSecrets {
		if len(secret) < minSecretBytes {
			return fmt.Errorf("app.webhook_secrets[%d] is %d bytes, want at least %d (docs/08-config.md §3)",
				i, len(secret), minSecretBytes)
		}
	}
	if err := durPositive("app.connect_timeout", a.ConnectTimeout); err != nil {
		return err
	}
	if err := intAtLeast("app.connect_queue", a.ConnectQueue, 1); err != nil {
		return err
	}
	for i, name := range a.CookieNames {
		if name == "" {
			return fmt.Errorf("app.cookie_names[%d] is empty; it forms the webhook cache key (docs/08-config.md §3)", i)
		}
	}
	if err := durRange("app.webhook_timeout", a.WebhookTimeout, 100*time.Millisecond, 30*time.Second); err != nil {
		return err
	}
	if err := intRange("app.webhook_concurrency", a.WebhookConcurrency, 1, 4096); err != nil {
		return err
	}
	if err := intRange("app.webhook_retries", a.WebhookRetries, 0, 5); err != nil {
		return err
	}
	if err := durNonNegative("app.cache_ttl", a.CacheTTL); err != nil {
		return err
	}
	if err := durPositive("app.min_expiry", a.MinExpiry); err != nil {
		return err
	}
	if err := durPositive("app.max_expiry", a.MaxExpiry); err != nil {
		return err
	}
	if a.MaxExpiry < a.MinExpiry {
		// The two clamp the webhook's expires_in from opposite sides; inverted, every
		// grant is clamped to a value outside its own window (docs/08-config.md §3).
		return fmt.Errorf("app.max_expiry is %s, which is below app.min_expiry %s", a.MaxExpiry, a.MinExpiry)
	}
	return nil
}

// validateBus checks docs/08-config.md §3's bus table.
func validateBus(b *Bus) error {
	if err := oneOf("bus.kind", b.Kind, "redis", "memory"); err != nil {
		return err
	}
	if b.Kind == "redis" {
		if b.URL == "" {
			return errors.New("bus.url is required when bus.kind is redis (docs/08-config.md §3)")
		}
		parsed, err := url.Parse(b.URL)
		if err != nil {
			return fmt.Errorf("bus.url %q is not a URL: %w", b.URL, err)
		}
		if parsed.Scheme != "redis" && parsed.Scheme != "rediss" && parsed.Scheme != "unix" {
			return fmt.Errorf("bus.url %q must be a redis://, rediss:// or unix:// URL", b.URL)
		}
	}
	if err := durPositive("bus.dial_timeout", b.DialTimeout); err != nil {
		return err
	}
	if err := durPositive("bus.reconnect_min", b.ReconnectMin); err != nil {
		return err
	}
	if err := durPositive("bus.reconnect_max", b.ReconnectMax); err != nil {
		return err
	}
	if b.ReconnectMax < b.ReconnectMin {
		return fmt.Errorf("bus.reconnect_max is %s, which is below bus.reconnect_min %s (NFR-8)",
			b.ReconnectMax, b.ReconnectMin)
	}
	if err := intAtLeast("bus.intake_queue", b.IntakeQueue, 1); err != nil {
		return err
	}
	if err := intRange("bus.dispatch_workers", b.DispatchWorkers, 1, 64); err != nil {
		return err
	}
	if err := durNonNegative("bus.ready_grace", b.ReadyGrace); err != nil {
		return err
	}
	if b.Prefix == "" || !isPrintableASCII(b.Prefix) {
		// It is both the Redis channel prefix and the hub's internal key prefix, so a
		// space in it is a subscription nobody can match (FR-21).
		return fmt.Errorf("bus.prefix is %q, want a non-empty printable ASCII string with no spaces", b.Prefix)
	}
	return nil
}

// validateChannels checks docs/08-config.md §3's channels table.
func validateChannels(c *Channels) error {
	if len(c.Separator) != 1 || !isPrintableASCII(c.Separator) {
		return fmt.Errorf("channels.separator is %q, want exactly one printable ASCII character (docs/08-config.md §3)",
			c.Separator)
	}
	return nil
}

// validateNamespaces checks docs/08-config.md §3's namespaces table and the naming rules
// in docs/06-channels.md §1.
//
// The empty name is legal and means the reserved namespace that separator-less channels
// resolve to — the block the built-in default owns. The literal name "default" is not:
// one block would then govern two disjoint populations, separator-less channels and
// channels beginning "default-" (docs/13-review-findings.md m5).
func validateNamespaces(namespaces []Namespace, separator string) error {
	if len(namespaces) == 0 {
		return errors.New("namespaces is empty; a channel with no matching block is refused, " +
			"so an empty list refuses every subscribe (docs/06-channels.md §3)")
	}
	seen := make(map[string]struct{}, len(namespaces))
	for i, ns := range namespaces {
		key := fmt.Sprintf("namespaces[%d]", i)
		if ns.Name == "default" {
			return fmt.Errorf("%s.name is %q, which is reserved: separator-less channels resolve to the "+
				"namespace %q, and a literal default- namespace would share its block (docs/06-channels.md §1)",
				key, "default", "")
		}
		if strings.Contains(ns.Name, separator) {
			return fmt.Errorf("%s.name %q contains the channel separator %q, so no channel can ever resolve to it",
				key, ns.Name, separator)
		}
		if strings.HasPrefix(ns.Name, "_") {
			return fmt.Errorf("%s.name %q begins with %q, which is reserved for control channels (docs/06-channels.md §4)",
				key, ns.Name, "_")
		}
		if !isPrintableASCII(ns.Name) {
			return fmt.Errorf("%s.name %q must be printable ASCII with no whitespace (docs/06-channels.md §1)", key, ns.Name)
		}
		if _, duplicate := seen[ns.Name]; duplicate {
			return fmt.Errorf("%s.name %q is a duplicate; a channel would resolve to two blocks", key, ns.Name)
		}
		seen[ns.Name] = struct{}{}

		if err := validateRateLimit(key+".rate_limit", ns.RateLimit); err != nil {
			return err
		}
		if ns.MaxMessageSize != nil {
			if err := intRange(key+".max_message_size", *ns.MaxMessageSize, 1, maxMessageBytes); err != nil {
				return err
			}
		}
		// M4 keys are a startup error rather than a warning. A config that claims
		// presence is on, while presence does nothing, is a lie an operator will act on
		// (docs/08-config.md §3).
		if ns.Presence {
			return fmt.Errorf("%s.presence is set but presence is not implemented (M4)", key)
		}
		if ns.HistorySize != 0 {
			return fmt.Errorf("%s.history_size is %d but history is not implemented (M4)", key, ns.HistorySize)
		}
	}
	return nil
}

// validateRateLimit checks the "<int>/<s|m>" form from docs/08-config.md §3.
func validateRateLimit(key, value string) error {
	count, unit, found := strings.Cut(value, "/")
	if !found || (unit != "s" && unit != "m") {
		return fmt.Errorf("%s is %q, want the form <int>/<s|m> such as %q (docs/08-config.md §3)", key, value, defaultRateLimit)
	}
	parsed, err := strconv.Atoi(count)
	if err != nil {
		return fmt.Errorf("%s is %q, whose rate is not an integer: %w", key, value, err)
	}
	if parsed < 1 {
		return fmt.Errorf("%s is %q, want a rate of at least 1", key, value)
	}
	return nil
}

// validateLimits checks docs/08-config.md §3's limits table.
func validateLimits(l *Limits) error {
	if err := intAtLeast("limits.max_connections", l.MaxConnections, 0); err != nil {
		return err
	}
	if err := intRange("limits.max_subscriptions_per_conn", l.MaxSubscriptionsPerConn, 1, 10000); err != nil {
		return err
	}
	if err := intAtLeast("limits.max_connections_per_user", l.MaxConnectionsPerUser, 0); err != nil {
		return err
	}
	if err := intRange("limits.read_buffer", l.ReadBuffer, 1, maxMessageBytes); err != nil {
		return err
	}
	if err := intRange("limits.write_buffer", l.WriteBuffer, 1, maxMessageBytes); err != nil {
		return err
	}
	if err := intRange("limits.outbound_queue", l.OutboundQueue, 16, 65536); err != nil {
		return err
	}
	if err := intRange("limits.max_message_size", l.MaxMessageSize, 1, maxMessageBytes); err != nil {
		return err
	}
	if err := intRange("limits.max_frame_size", l.MaxFrameSize, 1, maxMessageBytes); err != nil {
		return err
	}
	return intRange("limits.max_channel_length", l.MaxChannelLength, 16, 1024)
}

// validateControl checks docs/08-config.md §3's control table. The message reports a
// length, never the secret (NFR-7).
func validateControl(c *Control) error {
	if c.Secret == "" {
		return errors.New("control.secret is required; control messages that disconnect every " +
			"user on every replica must be signed (FR-23)")
	}
	if len(c.Secret) < minSecretBytes {
		return fmt.Errorf("control.secret is %d bytes, want at least %d (docs/08-config.md §3)",
			len(c.Secret), minSecretBytes)
	}
	return durNonNegative("control.refresh_spread", c.RefreshSpread)
}

// validateListeners checks both listen addresses and the rule that they differ.
//
// They are checked together because "differ" is not a string comparison: server.listen
// ":9001" and admin.listen "127.0.0.1:9001" are the same socket, and the admin listener
// silently failing to bind is how an operator loses /metrics without noticing
// (docs/08-config.md §3).
func validateListeners(serverListen, adminListen string) error {
	serverHost, serverPort, err := parseListen("server.listen", serverListen)
	if err != nil {
		return err
	}
	adminHost, adminPort, err := parseListen("admin.listen", adminListen)
	if err != nil {
		return err
	}
	if serverPort == adminPort && serverPort != "0" &&
		(serverHost == adminHost || serverHost == wildcardHost || adminHost == wildcardHost) {
		return fmt.Errorf("admin.listen %q is the same socket as server.listen %q; the operator "+
			"listener must be separate and is never exposed publicly (docs/08-config.md §3)",
			adminListen, serverListen)
	}
	return nil
}

// parseListen splits a listen address and normalizes a wildcard host, naming key on
// failure.
func parseListen(key, addr string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		return "", "", fmt.Errorf("%s %q is not a valid listen address: %w", key, addr, err)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > maxPort {
		return "", "", fmt.Errorf("%s %q has no valid port; want host:port with a port in 0–%d", key, addr, maxPort)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = wildcardHost
	}
	return host, port, nil
}

// validateLog checks docs/08-config.md §3's log table.
func validateLog(l *Log) error {
	if err := oneOf("log.level", l.Level, "debug", "info", "warn", "error"); err != nil {
		return err
	}
	return oneOf("log.format", l.Format, "json", "text")
}

// oneOf reports that value is not one of allowed, naming key.
func oneOf(key, value string, allowed ...string) error {
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("%s is %q, want one of %s (docs/08-config.md §3)", key, value, strings.Join(allowed, ", "))
}

// intRange reports that v is outside the documented range for key.
func intRange(key string, v, low, high int) error {
	if v < low || v > high {
		return fmt.Errorf("%s is %d, outside the documented range %d–%d (docs/08-config.md §3)", key, v, low, high)
	}
	return nil
}

// intAtLeast reports that v is below the documented floor for key.
func intAtLeast(key string, v, low int) error {
	if v < low {
		return fmt.Errorf("%s is %d, want at least %d (docs/08-config.md §3)", key, v, low)
	}
	return nil
}

// durRange reports that d is outside the documented range for key.
func durRange(key string, d Duration, low, high time.Duration) error {
	if d.Duration() < low || d.Duration() > high {
		return fmt.Errorf("%s is %s, outside the documented range %s–%s (docs/08-config.md §3)", key, d, low, high)
	}
	return nil
}

// durPositive reports that d is not a positive duration.
func durPositive(key string, d Duration) error {
	if d <= 0 {
		return fmt.Errorf("%s is %s, want a positive duration (docs/08-config.md §3)", key, d)
	}
	return nil
}

// durNonNegative reports that d is negative.
func durNonNegative(key string, d Duration) error {
	if d < 0 {
		return fmt.Errorf("%s is %s, want a duration of zero or more (docs/08-config.md §3)", key, d)
	}
	return nil
}

// isPrintableASCII reports whether every byte of s is printable ASCII, excluding space.
// Channel names, and therefore namespace names, are printable ASCII with no whitespace
// and no control characters (docs/06-channels.md §1).
func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < '!' || s[i] > '~' {
			return false
		}
	}
	return true
}
