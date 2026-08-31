package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/glob"
)

// maxResponseBytes caps what one connect answer may cost the gateway.
//
// 64 KiB is far beyond any legitimate answer — a user id, a grant list and an integer —
// and the cap exists so that an application bug cannot make each of 25,000 connecting
// clients buffer an unbounded response. A body over the cap is truncated, so it does not
// parse, and the connection is refused rather than the gateway growing to hold it.
const maxResponseBytes = 64 << 10

// defaultRejectLogInterval is how often a 403 is logged at ERROR when Options leaves it
// unset. One line a minute is enough to raise an alarm and few enough that a fleet-wide
// rejection — every connection on every replica, which is what a bad secret looks like —
// cannot bury the signal under the incident it describes.
const defaultRejectLogInterval = time.Minute

// Clock is the time source. It exists so tests move time instead of spending it: a sleep
// in a test is either a flake on a loaded CI box or a wasted second, and usually both
// (docs/14-coding-standards.md §2).
//
// It governs the timestamp in the signature, the authorization budget, the cache's TTL
// and the logged duration. It deliberately does not govern the HTTP call's own timeout,
// which is a context deadline because that is what net/http honours.
type Clock interface {
	// Now returns the current instant.
	Now() time.Time

	// After returns a channel that receives once, after d.
	After(d time.Duration) <-chan time.Time
}

// realClock is the production Clock: the wall clock and real timers.
type realClock struct{}

// Now returns time.Now.
func (realClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Options are the dependencies of a Client. Everything but App has a working default, and
// every default is stated on the field.
//
// There is no package-level configuration and no singleton: two tests in this package
// configure a Client differently and run at the same time, which a global would make
// impossible (docs/14-coding-standards.md §7).
type Options struct {
	// App is the app block from docs/08-config.md §3. Required.
	App config.App

	// TrustedProxies is server.trusted_proxies: CIDRs whose X-Forwarded-For is believed.
	// Empty — the default — trusts nothing, so X-St-Forwarded-For is the socket peer
	// address (FR-24).
	TrustedProxies []string

	// HTTPClient is the transport. Defaults to a client that keeps idle connections for
	// the webhook host and never follows redirects.
	//
	// Following one would replay the Cookie header and the signature at whatever host the
	// application named, which is a credential-forwarding primitive handed to anyone who
	// can set a Location header. A 3xx is therefore a transient failure, like any other
	// unexpected status.
	HTTPClient *http.Client

	// Clock defaults to the wall clock.
	Clock Clock

	// Nonce returns the X-St-Nonce value. Defaults to 128 bits from crypto/rand.
	//
	// The nonce is emitted so an application that wants exactly-once can cache seen
	// nonces for 300s; nothing here requires it. It must therefore not repeat, including
	// across the retries of one connect, or such an application would reject the retry as
	// a replay (docs/04-integration.md §1.1).
	Nonce func() string

	// Logger defaults to slog.Default(). Nothing logged here carries a cookie, a
	// signature, a secret or a body (NFR-7).
	Logger *slog.Logger

	// RejectLogInterval is how often a webhook 403 is logged at ERROR. Defaults to
	// defaultRejectLogInterval.
	//
	// A 403 means the gateway cannot authenticate to the application, so it is loud —
	// but a bad secret or a skewed clock rejects every connection on the replica, and one
	// ERROR line per connection is an outage in the log pipeline on top of the outage
	// being reported. Suppressed occurrences are still logged at debug and always
	// counted (FR-6).
	RejectLogInterval time.Duration
}

// Request is one connection's input to the connect webhook. It is passed by value and
// nothing in it is retained after Call returns (FR-22).
type Request struct {
	// Client is the connection's client id, 16 hex characters. It is the only per-user
	// value this package logs.
	Client string

	// Cookie is the client's Cookie header, verbatim. It is forwarded byte for byte and
	// never parsed, validated, decrypted or shortened: session formats belong to the
	// application, and the gateway cannot know them (FR-3).
	Cookie string

	// Origin is the handshake's Origin header, already checked against the allowlist by
	// the caller. It is forwarded as X-St-Origin.
	Origin string

	// UserAgent is the handshake's User-Agent, forwarded as X-St-User-Agent.
	UserAgent string

	// RemoteAddr is the socket peer address, with or without a port.
	RemoteAddr string

	// ForwardedFor is the inbound X-Forwarded-For header, if any. It is used only when
	// RemoteAddr is inside TrustedProxies and is otherwise discarded, never forwarded
	// (FR-24).
	ForwardedFor string

	// ChannelsRequested are the channels the connect frame asked to subscribe to. They
	// are informational: the application answers with grants, and the gateway matches
	// against those.
	ChannelsRequested []string
}

// requestBody is the JSON body of the connect request. docs/04-integration.md §1.1.
type requestBody struct {
	Client            string   `json:"client"`
	ChannelsRequested []string `json:"channels_requested,omitempty"`
}

// responseBody is the 200 answer. docs/04-integration.md §1.2.
//
// Channels and ExpiresIn are a nil-able slice and a pointer so that "absent" is
// distinguishable from "empty" and from "zero": an empty grant list is legal and means a
// connection that cannot subscribe to anything, while a missing one is an application bug
// that must refuse the connection rather than silently produce the same result.
//
// info is deliberately absent: it is opaque, reserved for presence in a later milestone,
// and ignored today. Unknown fields are ignored too, so an application may send more than
// this without being refused for it.
type responseBody struct {
	User      string   `json:"user"`
	Channels  []string `json:"channels"`
	ExpiresIn *int64   `json:"expires_in"`
}

// Stats are a Client's cumulative counters, for tests and for whoever is reading them
// during an incident. They are read with
// Client.Stats.
//
// Rejected is kept apart from Failed on purpose. A 5xx means the application is unwell; a
// 403 means the gateway cannot authenticate to it — a bad secret, a skewed clock, a key
// removed mid-rotation. Those need different people and different fixes, and a single
// "webhook errors" counter makes them indistinguishable at exactly the moment that
// matters (docs/04-integration.md §1.3).
type Stats struct {
	// Authorized counts calls that produced an Authorized, including cache hits.
	Authorized uint64

	// Refused counts permanent refusals: a 401, and a 2xx whose body was unusable.
	Refused uint64

	// Rejected counts 403s — the gateway's own requests being rejected.
	Rejected uint64

	// Failed counts every other transient failure: 5xx, unlisted statuses, timeouts,
	// transport errors, queue overflow, a spent budget.
	Failed uint64
}

// Client calls the application's connect webhook. It is the gateway's single integration
// point for turning a browser's cookie into an identity and a grant set
// (docs/04-integration.md §1).
//
// One Client is shared by every connection on the replica: it holds the concurrency cap,
// the bounded queue and the optional cache, all of which are per-process rather than
// per-connection. Every method is safe for concurrent use by any number of goroutines.
//
// It retains no cookie. FR-22 and docs/13-review-findings.md C3: an earlier design cached
// the cookie for revalidation, which made a memory dump of the process a set of live
// sessions and broke every application that rotates its session on login or on every
// request. Grants expire by re-handshake instead.
type Client struct {
	appName        string
	url            string
	secret         []byte
	connectTimeout time.Duration
	webhookTimeout time.Duration
	retries        int
	minExpiry      time.Duration
	maxExpiry      time.Duration
	trusted        []netip.Prefix

	// inflight is the concurrency cap: its capacity is app.webhook_concurrency and a
	// token is held for a whole call including its retries, because that is one
	// authorization, not several (NFR-4).
	inflight chan struct{}

	// waiting is the bounded queue: its capacity is app.connect_queue. Excess
	// connections wait here, where waiting is cheap, rather than being issued at an
	// application with a fixed worker pool. Overflow is transient, never a refusal — an
	// unbounded queue is 25,000 half-open sockets each holding a captured cookie, and a
	// permanent close would lock out exactly the users a reconnect storm caught
	// (docs/13-review-findings.md C2).
	waiting chan struct{}

	httpClient *http.Client
	clock      Clock
	nonce      func() string
	log        *slog.Logger

	// rejectLogInterval and lastRejectLog rate-limit the ERROR line for a 403. The mutex
	// is held for a clock read and a comparison, never across I/O.
	rejectLogInterval time.Duration
	rejectLogMu       sync.Mutex
	lastRejectLog     time.Time

	// Counters, incremented in logCall — the one place every path passes through.
	authorized atomic.Uint64
	refused    atomic.Uint64
	rejected   atomic.Uint64
	failed     atomic.Uint64

	// cache is nil unless app.cache_ttl is positive. Off is the default and the right
	// one: a cached entry survives a revocation (docs/13-review-findings.md C4).
	cache *cache
}

// New builds a Client from Options. It returns an error rather than a Client that fails
// at the first connect.
//
// config.Validate already enforces every rule checked here, and these checks stay anyway:
// a zero app.webhook_concurrency would produce a semaphore no connection can ever enter,
// which is a deadlock discovered in production, and an empty app.webhook_secrets would
// panic on the first call. Neither error quotes a secret (NFR-7).
func New(opts Options) (*Client, error) {
	app := opts.App

	if len(app.WebhookSecrets) == 0 {
		return nil, errors.New("app.webhook_secrets is empty; the connect webhook must be signed (FR-3)")
	}
	if app.WebhookConcurrency < 1 {
		return nil, fmt.Errorf("app.webhook_concurrency is %d, want at least 1: a cap of zero admits no connection at all (NFR-4)", app.WebhookConcurrency)
	}
	if app.ConnectQueue < 0 {
		return nil, fmt.Errorf("app.connect_queue is %d, want at least 0", app.ConnectQueue)
	}
	if app.WebhookRetries < 0 {
		// Not pedantry: a negative count would make the attempt loop run zero times, and
		// Call would return the nil Result its contract forbids — a nil type switch
		// falling through to the caller's default, on the connect path, for every user.
		return nil, fmt.Errorf("app.webhook_retries is %d, want at least 0", app.WebhookRetries)
	}

	trusted := make([]netip.Prefix, 0, len(opts.TrustedProxies))
	for i, cidr := range opts.TrustedProxies {
		p, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("server.trusted_proxies[%d] %q is not a CIDR: %w", i, cidr, err)
		}
		trusted = append(trusted, p)
	}

	c := &Client{
		appName:        app.Name,
		url:            app.ConnectURL,
		secret:         []byte(app.WebhookSecrets[0]),
		connectTimeout: app.ConnectTimeout.Duration(),
		webhookTimeout: app.WebhookTimeout.Duration(),
		retries:        app.WebhookRetries,
		minExpiry:      app.MinExpiry.Duration(),
		maxExpiry:      app.MaxExpiry.Duration(),
		trusted:        trusted,
		inflight:       make(chan struct{}, app.WebhookConcurrency),
		waiting:        make(chan struct{}, app.ConnectQueue),
		httpClient:     opts.HTTPClient,
		clock:          opts.Clock,
		nonce:          opts.Nonce,
		log:            opts.Logger,

		rejectLogInterval: opts.RejectLogInterval,
	}
	if c.rejectLogInterval <= 0 {
		c.rejectLogInterval = defaultRejectLogInterval
	}

	if c.httpClient == nil {
		c.httpClient = defaultHTTPClient(app.WebhookConcurrency)
	}
	if c.clock == nil {
		c.clock = realClock{}
	}
	if c.nonce == nil {
		c.nonce = rand.Text
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	if app.CacheTTL > 0 {
		c.cache = newCache(app.CacheTTL.Duration(), app.CookieNames)
	}
	return c, nil
}

// defaultHTTPClient is the transport used when Options.HTTPClient is nil.
//
// It never follows a redirect: doing so would replay the Cookie header and the signature
// at whatever host the application named. Idle connections are pooled at the concurrency
// cap, because at that cap a fresh TCP and TLS handshake per connect would dominate the
// authorization latency of a mass reconnect. There is no Client.Timeout: each attempt
// carries its own context deadline, derived from app.webhook_timeout and what remains of
// app.connect_timeout.
func defaultHTTPClient(concurrency int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          concurrency,
			MaxIdleConnsPerHost:   concurrency,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Call turns one connection's cookie into a Result. It never returns nil.
//
// It blocks for at most app.connect_timeout — the whole authorization budget, queue wait
// plus call — or until ctx is done, whichever comes first. Safe for concurrent use.
//
// There is no error return. Every failure is a Result, because FR-6's distinction between
// a refusal and a failure is the decision this whole package exists to make, and a caller
// that has to check an error *and* a result can get it wrong in two places. The three
// outcomes are Authorized, Refused (close 3003, reconnect false, never retried) and
// Unavailable (close 3008, reconnect true, with a retry_after).
//
// Nothing about req is retained after it returns (FR-22).
func (c *Client) Call(ctx context.Context, req Request) Result {
	start := c.clock.Now()
	res, cached := c.call(ctx, req, start)
	c.logCall(req.Client, res, cached, c.clock.Now().Sub(start))
	return res
}

// call is Call without the logging, so that every path logs exactly once and the
// "log once" of docs/04-integration.md §1.3 cannot be lost in a new early return.
//
// It reports whether the answer came from the cache.
func (c *Client) call(ctx context.Context, req Request, start time.Time) (Result, bool) {
	var key string
	if c.cache != nil {
		key = c.cache.key(req.Cookie)
		if cached, ok := c.cache.get(key, start); ok {
			return cached, true
		}
	}

	if res, ok := c.acquire(ctx); !ok {
		return res, false
	}
	defer func() { <-c.inflight }()

	res := c.attempts(ctx, req, start.Add(c.connectTimeout))
	if authorized, ok := res.(Authorized); ok && c.cache != nil {
		// Only an authorization is cached. Caching a refusal would keep a user out for a
		// TTL after the application let them back in; caching a failure would keep
		// answering with a failure after the application recovered.
		c.cache.put(key, authorized, c.clock.Now())
	}
	return res, false
}

// acquire takes an in-flight slot, waiting in the bounded queue if it must. It reports
// whether the slot was taken; when it was not, the Result is the transient failure to
// return (NFR-4, docs/13-review-findings.md C2).
//
// The caller returns the slot with <-c.inflight.
func (c *Client) acquire(ctx context.Context) (Result, bool) {
	select {
	case c.inflight <- struct{}{}:
		return nil, true
	default:
	}

	// The cap is full, so this connection must wait — and the wait itself is bounded, on
	// both axes. app.connect_queue caps how many may wait.
	select {
	case c.waiting <- struct{}{}:
	default:
		return Unavailable{Err: fmt.Errorf("%w at %d waiting connections", ErrQueueOverflow, cap(c.waiting))}, false
	}
	defer func() { <-c.waiting }()

	// app.connect_timeout caps how long any one may wait. Exceeding it is transient:
	// closing these connections permanently would turn the mechanism that protects the
	// application against a reconnect storm into a permanent lockout of every user
	// caught in one (docs/13-review-findings.md C2).
	select {
	case c.inflight <- struct{}{}:
		return nil, true
	case <-ctx.Done():
		return Unavailable{Err: fmt.Errorf("waiting for a webhook slot: %w", ctx.Err())}, false
	case <-c.clock.After(c.connectTimeout):
		return Unavailable{Err: fmt.Errorf("waited app.connect_timeout=%s for a webhook slot: %w", c.connectTimeout, context.DeadlineExceeded)}, false
	}
}

// attempts makes the call and retries a transient failure up to app.webhook_retries.
//
// Retries apply to 5xx, timeouts and transport errors only, never to a 401 or 403:
// retrying a refusal turns a revocation into a denial-of-service against the application
// (FR-6). deadline is the end of the whole authorization budget, so a retry that would
// start after the gateway has already given up is not issued at all.
func (c *Client) attempts(ctx context.Context, req Request, deadline time.Time) Result {
	// Derived once: the peer address does not change between attempts, and deriving it
	// per attempt would invite a future edit that derives it differently on the retry.
	forwarded := forwardedFor(req.RemoteAddr, req.ForwardedFor, c.trusted)

	var last Result
	for range c.retries + 1 {
		// The browser may have gone away mid-authorization. Retrying for a connection
		// that no longer exists is load on an application that is usually already
		// struggling, which is the whole reason the retry exists.
		if err := ctx.Err(); err != nil {
			return Unavailable{Err: fmt.Errorf("connect webhook %q: %w", c.url, err)}
		}
		remaining := deadline.Sub(c.clock.Now())
		if remaining <= 0 {
			return Unavailable{Err: fmt.Errorf("authorization budget app.connect_timeout=%s is spent: %w", c.connectTimeout, context.DeadlineExceeded)}
		}
		res := c.attempt(ctx, req, forwarded, min(remaining, c.webhookTimeout))
		failure, transient := res.(Unavailable)
		if !transient {
			return res
		}
		if errors.Is(failure, ErrRequestRejected) {
			// Transient for the client, but not worth an immediate second attempt: a
			// retry carries the same signature computed from the same secret and the
			// same clock, so it is load on the application with no prospect of a
			// different answer. app.webhook_retries is documented as applying to 5xx and
			// timeouts only (docs/08-config.md §3).
			return res
		}
		last = res
	}
	return last
}

// attempt makes one signed request and turns the answer into a Result.
//
// Each attempt is signed afresh — its own timestamp, nonce and signature. Reusing them
// would look like a replay to an application caching nonces for exactly-once
// (docs/04-integration.md §1.1), which would turn every retry into a refusal.
func (c *Client) attempt(ctx context.Context, req Request, forwarded string, timeout time.Duration) Result {
	body, err := json.Marshal(requestBody{Client: req.Client, ChannelsRequested: req.ChannelsRequested})
	if err != nil {
		// coverage: json.Marshal of a struct holding a string and a []string has no
		// failing case; there is no fault to inject and no harness worth building for
		// it. The branch exists because nilerr and errcheck are right that an ignored
		// error is how a failure goes quiet.
		return Unavailable{Err: fmt.Errorf("encoding the connect request: %w", err)}
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Unavailable{Err: fmt.Errorf("building the connect request for app.connect_url %q: %w", c.url, err)}
	}

	timestamp := strconv.FormatInt(c.clock.Now().Unix(), 10)
	nonce := c.nonce()
	httpReq.Header.Set("Content-Type", "application/json")
	// Verbatim. Never parsed, validated, decrypted or shortened (FR-3).
	setIfPresent(httpReq.Header, "Cookie", req.Cookie)
	setIfPresent(httpReq.Header, "X-St-Origin", req.Origin)
	setIfPresent(httpReq.Header, "X-St-User-Agent", req.UserAgent)
	setIfPresent(httpReq.Header, "X-St-Forwarded-For", forwarded)
	httpReq.Header.Set("X-St-Timestamp", timestamp)
	httpReq.Header.Set("X-St-Nonce", nonce)
	httpReq.Header.Set("X-St-Signature", sign(c.secret, timestamp, nonce, req.Cookie, body))

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// A timeout, a refused connection, a DNS failure: "I could not tell you right
		// now", which is transient and retryable (FR-6).
		return Unavailable{Err: fmt.Errorf("connect webhook %q: %w", c.url, err)}
	}
	defer func() { _ = resp.Body.Close() }()

	return c.classify(resp)
}

// setIfPresent sets a header only when there is something to send. A browser that sent no
// Origin, or a non-browser client with no cookie at all, produces an absent header rather
// than an empty one — the signature covers the digest of "" either way, which is what the
// application computes for a header it did not receive.
func setIfPresent(h http.Header, key, value string) {
	if value == "" {
		return
	}
	h.Set(key, value)
}

// classify applies the status table of docs/04-integration.md §1.3. This is the most
// consequential logic in the package.
func (c *Client) classify(resp *http.Response) Result {
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		// A decision about the user: they may not connect, and the client must stop
		// asking. The body is not read and never will be — it is the application's error
		// page, and applications put session identifiers in those (NFR-7).
		c.drain(resp)
		return Refused{Status: resp.StatusCode}

	case resp.StatusCode == http.StatusForbidden:
		// A statement about the request, not the user: a bad signature, a timestamp
		// outside the ±300s window, an unknown key during a rotation. That is a
		// gateway-side fault, and a gateway fault must never be expressed to users as a
		// permanent refusal — a replica whose clock has drifted would otherwise lock out
		// every user it serves until a human noticed (FR-6,
		// docs/04-integration.md §1.3).
		c.drain(resp)
		return Unavailable{Status: resp.StatusCode, Err: ErrRequestRejected}

	case resp.StatusCode >= 200 && resp.StatusCode <= 299:
		return c.parse(resp)

	default:
		// 5xx, and every status the table does not name. A 404 or a 3xx means the
		// connect_url is wrong or the application is misrouting, which is an operator
		// error with two possible answers: every user locked out until someone notices,
		// or every user retrying until someone notices. The second is recoverable, and
		// FR-6 is explicit that a failure the gateway cannot interpret is a failure and
		// not a decision.
		c.drain(resp)
		return Unavailable{
			Status: resp.StatusCode,
			Err:    fmt.Errorf("connect webhook %q answered %d", c.url, resp.StatusCode),
		}
	}
}

// drain reads and discards a body the gateway will not parse, so the connection returns to
// the idle pool instead of being closed and re-handshaked on the next connect. It is
// bounded, because an unbounded drain is the same denial of service as an unbounded read.
func (c *Client) drain(resp *http.Response) {
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes)); err != nil {
		// The answer has already been classified from the status line, so a body that
		// stops mid-flight changes nothing: it costs one pooled connection. Logged at
		// debug because a rise in these is a symptom of an application killing
		// connections, and never with the body itself (NFR-7).
		c.log.Debug("draining the connect webhook response", "app", c.appName, "err", err)
	}
}

// parse turns a 2xx into an Authorized, or into the permanent refusal that
// docs/04-integration.md §1.3 requires for a 2xx with an unusable body.
//
// Refusing rather than retrying is deliberate: the application answered, so a second
// identical question gets the same unusable answer, and retrying it is load with no
// prospect of a different result.
func (c *Client) parse(resp *http.Response) Result {
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		// The body stopped mid-flight. That is a transport failure, not an application
		// decision, so it is retryable.
		return Unavailable{Status: resp.StatusCode, Err: fmt.Errorf("reading the connect webhook response: %w", err)}
	}

	// The error from encoding/json names an offending character and an offset, never the
	// document, which is what makes it safe to wrap and log under NFR-7.
	var body responseBody
	if jsonErr := json.Unmarshal(raw, &body); jsonErr != nil {
		return Refused{Status: resp.StatusCode, Err: fmt.Errorf("%w: %w", ErrMalformedResponse, jsonErr)}
	}

	switch {
	case body.User == "":
		return Refused{Status: resp.StatusCode, Err: fmt.Errorf("%w: \"user\" is required and must not be empty", ErrMalformedResponse)}
	case body.Channels == nil:
		// Distinct from an empty list, which is legal: a connection with no grants
		// simply cannot subscribe to anything (docs/04-integration.md §1.2). A missing
		// list is an application bug, and defaulting it to "no grants" would present
		// that bug to the user as a connection where nothing ever arrives.
		return Refused{Status: resp.StatusCode, Err: fmt.Errorf("%w: \"channels\" is required; send [] for a connection with no grants", ErrMalformedResponse)}
	case body.ExpiresIn == nil:
		return Refused{Status: resp.StatusCode, Err: fmt.Errorf("%w: \"expires_in\" is required", ErrMalformedResponse)}
	}

	// Compiled here, at authorization time, so a malformed grant refuses the connection
	// instead of surfacing minutes later as one subscribe that mysteriously fails
	// (docs/05-authorization.md §3).
	grants, err := glob.NewSet(body.Channels)
	if err != nil {
		return Refused{Status: resp.StatusCode, Err: fmt.Errorf("%w: %w", ErrMalformedResponse, err)}
	}

	return Authorized{
		User:      body.User,
		Grants:    grants,
		ExpiresIn: clampExpiry(*body.ExpiresIn, c.minExpiry, c.maxExpiry),
	}
}

// clampExpiry bounds the application's expires_in to [app.min_expiry, app.max_expiry].
//
// The clamped value is what the caller reports to the client, because a client told 24h
// by an application whose answer the gateway clamped to 6h would schedule its own refresh
// long after the gateway had already closed it (docs/04-integration.md §1.2).
//
// The comparison is made in seconds before the multiplication, so an application sending
// a nonsense expires_in cannot overflow a time.Duration into a negative lifetime — which
// would clamp *up* to min_expiry and give a connection an hour it should not have.
func clampExpiry(seconds int64, minExpiry, maxExpiry time.Duration) time.Duration {
	if seconds > int64(maxExpiry/time.Second) {
		return maxExpiry
	}
	if d := time.Duration(seconds) * time.Second; d > minExpiry {
		return d
	}
	return minExpiry
}

// logCall emits the one log line per call.
//
// What it logs is fixed by NFR-7 and docs/14-coding-standards.md §9: the client id, the
// status, the outcome and the duration. Never the cookie, the signature, the secret, the
// request body or the response body — not at any level, including debug. This process
// sees every connected user's session cookie, and its logs must not become a credential
// store.
//
// A refusal caused by an unusable 2xx is logged at warn: it is an application bug an
// operator has to see, and it is logged exactly once because this is the only log
// statement on the path (docs/04-integration.md §1.3).
func (c *Client) logCall(clientID string, res Result, cached bool, elapsed time.Duration) {
	attrs := []any{
		"app", c.appName,
		"client", clientID,
		"duration_ms", elapsed.Milliseconds(),
		"cached", cached,
	}

	switch v := res.(type) {
	case Authorized:
		c.authorized.Add(1)
		c.log.Debug("connect webhook authorized",
			append(attrs, "user", v.User, "expires_in_s", int64(v.ExpiresIn.Seconds()))...)
	case Refused:
		c.refused.Add(1)
		if errors.Is(v, ErrMalformedResponse) {
			c.log.Warn("connect webhook returned an unusable body; refusing the connection",
				append(attrs, "status", v.Status, "err", v.Err)...)
			return
		}
		c.log.Info("connect webhook refused the connection",
			append(attrs, "status", v.Status, "reconnect", false)...)
	case Unavailable:
		if errors.Is(v, ErrRequestRejected) {
			c.rejected.Add(1)
			c.logRejection(attrs, v)
			return
		}
		c.failed.Add(1)
		c.log.Warn("connect webhook unavailable",
			append(attrs, "status", v.Status, "reconnect", true, "err", v.Err)...)
	}
}

// logRejection emits the 403 line: ERROR at most once per rejectLogInterval, debug for
// every occurrence in between.
//
// Loud, because retrying cannot fix a bad secret or a skewed clock and somebody has to go
// and look. Rate-limited, because that same bad secret rejects every connection on the
// replica, and one ERROR line per connection buries the signal under the incident it is
// describing. Every occurrence is still counted (FR-6, docs/04-integration.md §1.3).
func (c *Client) logRejection(attrs []any, res Unavailable) {
	now := c.clock.Now()

	c.rejectLogMu.Lock()
	loud := c.lastRejectLog.IsZero() || now.Sub(c.lastRejectLog) >= c.rejectLogInterval
	if loud {
		c.lastRejectLog = now
	}
	c.rejectLogMu.Unlock()

	if !loud {
		c.log.Debug("connect webhook rejected the request",
			append(attrs, "status", res.Status, "reconnect", true)...)
		return
	}
	c.log.Error("connect webhook rejected the gateway's request; check app.webhook_secrets and this replica's clock",
		append(attrs, "status", res.Status, "reconnect", true, "err", res.Err)...)
}

// Stats returns the Client's cumulative counters. It never blocks and is safe to call
// concurrently, including while connects are in flight.
func (c *Client) Stats() Stats {
	return Stats{
		Authorized: c.authorized.Load(),
		Refused:    c.refused.Load(),
		Rejected:   c.rejected.Load(),
		Failed:     c.failed.Load(),
	}
}

// Flush discards every cached answer. The control channel calls it on a disconnect,
// because a cached entry otherwise survives a revocation and a suspended user
// reconnecting within app.cache_ttl gets their pre-revocation grants back
// (docs/13-review-findings.md C4).
//
// It is coarse — the whole cache, not the revoked user's entry — and that is the right
// trade: the cache is small, revocations are rare, and finding one user's entry would
// mean keeping something that maps a user back to a cookie digest. Safe to call
// concurrently, and a no-op when the cache is off.
func (c *Client) Flush() {
	if c.cache == nil {
		return
	}
	c.cache.flush()
}

// InFlight is the number of webhook calls currently issued at the application. It never
// exceeds app.webhook_concurrency (NFR-4).
func (c *Client) InFlight() int { return len(c.inflight) }

// Waiting is the number of connections queued for an in-flight slot. It never exceeds
// app.connect_queue. Sustained non-zero means the application is answering more slowly
// than connections arrive.
func (c *Client) Waiting() int { return len(c.waiting) }
