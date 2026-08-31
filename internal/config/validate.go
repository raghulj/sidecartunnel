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
)

// validateServer checks docs/08-config.md §3's server table, including server.listen.
//
// server.listen used to be checked elsewhere, paired against a second listen address and a
// rule that the two had to be different sockets. There is one listener now
// (docs/12-roadmap.md §2), so the check belongs with the rest of the server table.
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
	// Ranged, not merely non-negative. Every other duration in docs/08-config.md §3 has
	// a documented range and this one had none, so `drain_spread: 500us` validated,
	// started cleanly, and handed the connection layer a window it rounds to zero
	// milliseconds — the whole spread gone, silently, at exactly the moment it matters.
	// The floor is 1s because a spread finer than that is not a spread; the ceiling is
	// 300s, matching drain_timeout, because it is also the largest retry_after a
	// conforming client will honour (docs/03-client-protocol.md §8.2).
	if err := durRange("server.drain_spread", s.DrainSpread, time.Second, 300*time.Second); err != nil {
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
	return validateListen("server.listen", s.Listen)
}

// RedactedURL returns raw with any userinfo replaced by "redacted", for an error or a log
// line that has to name a configured URL.
//
// A configured URL is a credential carrier: `redis://:password@host` is the documented way
// to give the bus a password, and `https://gw:hunter2@webapp.internal/_st/connect` is an
// ordinary shape for an internal endpoint. Naming the URL is also the most useful thing to
// say when one is wrong, so the answer is to redact rather than to go silent.
//
// net/http redacts userinfo from its own *url.Error for exactly this reason, which is the
// trap: hand-rolled wrapping that does not redact produces a leak that looks like Go's
// output and is not (NFR-7, docs/13-review-findings.md).
//
// A value that does not parse is reported as a placeholder rather than echoed, because the
// reason it does not parse may be the credential inside it.
func RedactedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[unparseable URL]"
	}
	if parsed.User != nil {
		parsed.User = url.User("redacted")
	}
	return parsed.String()
}

// urlErrorCause returns a *url.Error's underlying reason, discarding the URL it quotes.
//
// url.Parse reports `parse "https://gw:hunter2@host/x": …`, so wrapping its error is how
// the value of a key we are refusing ends up in a startup log verbatim. The cause alone —
// "missing protocol scheme", "invalid control character in URL" — is the part an operator
// acts on, and it names nothing.
func urlErrorCause(err error) string {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return uerr.Err.Error()
	}
	// coverage: url.Parse returns nothing but *url.Error, and this is the only caller.
	// The fallback exists so that a future caller passing some other error gets its
	// message rather than an empty string.
	return err.Error()
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
		return errors.New("app.name is empty; it labels every log line (docs/08-config.md §3)")
	}
	if a.ConnectURL == "" {
		return errors.New("app.connect_url is required; there is no authorization without it (FR-3)")
	}
	parsed, err := url.Parse(a.ConnectURL)
	if err != nil {
		// urlErrorCause, not err: url.Parse wraps its reason in a *url.Error whose text
		// quotes the whole input, so printing it echoes any credential in the value of
		// the key we are refusing (NFR-7).
		return fmt.Errorf("app.connect_url is not a URL: %s", urlErrorCause(err))
	}
	if parsed.User != nil {
		// Checked before anything quotes the value, and the message quotes nothing.
		//
		// https://gw:hunter2@webapp.internal/_st/connect is an ordinary shape for an
		// internal endpoint, and every timed-out connect formats this URL into a warn
		// line. The webhook is already authenticated to the application by the HMAC over
		// app.webhook_secrets (FR-3), so userinfo here buys nothing and costs a password
		// in the logs of a process that also sees every user's session cookie (NFR-7).
		return errors.New("app.connect_url carries credentials in its userinfo; " +
			"remove them — the connect webhook is authenticated by app.webhook_secrets, " +
			"and a URL in an error or a log line must not be a password (FR-3, NFR-7)")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("app.connect_url %q must be an absolute http or https URL (FR-3)", RedactedURL(a.ConnectURL))
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
		// redis://:password@host is the documented way to give the bus a password, so
		// the userinfo is accepted here — but never echoed. Same rule as
		// app.connect_url, opposite disposition, and for the same reason (NFR-7).
		parsed, err := url.Parse(b.URL)
		if err != nil {
			return fmt.Errorf("bus.url is not a URL: %s", urlErrorCause(err))
		}
		if parsed.Scheme != "redis" && parsed.Scheme != "rediss" && parsed.Scheme != "unix" {
			return fmt.Errorf("bus.url %q must be a redis://, rediss:// or unix:// URL", RedactedURL(b.URL))
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
	return nil
}

// validateListen checks that addr is a usable host:port, naming key on failure.
//
// It is one address now. There were two, and a rule that they had to be different sockets,
// because ":9001" and "127.0.0.1:9001" are the same socket and a plain string comparison
// would miss it. The second listener is gone (docs/12-roadmap.md §2), so all that is left
// to check is that the one remaining address can be bound at all.
func validateListen(key, addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q is not a valid listen address: %w", key, addr, err)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > maxPort {
		return fmt.Errorf("%s %q has no valid port; want host:port with a port in 0–%d", key, addr, maxPort)
	}
	return nil
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
