package integration_test

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/consumer"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/server"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// Budgets. Every one of them is a failure detector, never a pacing device: the happy path
// takes microseconds to milliseconds and the deadline only fires when the test was going
// to fail anyway. Nothing in this package sleeps (docs/14-coding-standards.md §2).
const (
	// waitBudget bounds a wait on observable state — a bus subscription confirmed, a
	// connection unwound, a container answering. Generous, because it also covers a
	// loaded CI box and a Redis that is still starting.
	waitBudget = 30 * time.Second

	// readBudget bounds one socket read. A frame that has not arrived in this long is
	// not late; it is absent.
	readBudget = 15 * time.Second

	// revocationBudget is FR-18's own number: a control disconnect must close the target
	// "within one second". It is asserted as a read deadline, so a slow revocation fails
	// rather than passing late.
	revocationBudget = time.Second
)

// testOrigin is the one origin every cluster allows. FR-2 is an exact string match, so
// the test that matters is the one that varies this by a character.
const testOrigin = "https://app.example.com"

// foreignOrigin is a page that is not on the allowlist. It differs from testOrigin in
// more than a character on purpose: the near-miss cases belong to the unit layer, and
// this one is the cross-site websocket hijack FR-2 exists to stop.
const foreignOrigin = "https://evil.example"

// testCookie is what every client sends. It is a fixed opaque string: the gateway never
// parses a cookie, and the stub application below only ever sees its digest inside the
// signature (FR-3, NFR-7).
const testCookie = "session=integration-cookie-value"

// ---------------------------------------------------------------------------
// stub application
// ---------------------------------------------------------------------------

// appCall is one recorded connect-webhook request, as the application saw it.
type appCall struct {
	Client            string   `json:"client"`
	ChannelsRequested []string `json:"channels_requested"`

	// Cookie, Origin and Forwarded are the headers the gateway forwarded. They are
	// asserted on, and never logged (NFR-7).
	Cookie    string `json:"-"`
	Origin    string `json:"-"`
	Forwarded string `json:"-"`
}

// appReply is what the stub answers with. Status 200 with an empty Body sends the
// default authorization.
type appReply struct {
	Status int
	Body   string
}

// stubApp is the consuming application: one HTTP endpoint that turns a cookie into a user
// and a grant list (docs/04-integration.md §1).
//
// It counts calls, and that is not bookkeeping. FR-2's acceptance criterion is that a
// handshake with a foreign Origin makes **no application call at all**, and the only way
// to assert the absence of a call is to hold something that would have recorded one.
//
// It verifies the gateway's signature exactly as docs/04-integration.md §1.1 specifies,
// so a signing regression fails here rather than showing up as an inexplicable 3008.
type stubApp struct {
	server *httptest.Server
	secret string

	mu     sync.Mutex
	calls  []appCall
	user   string
	grants []string

	// respond overrides the default answer, so a test can refuse, fail, or answer
	// differently per call.
	respond func(call appCall, n int) appReply

	// sigFailures counts requests whose signature or timestamp did not verify. It is
	// asserted zero at the end of every test rather than failed inline, because this
	// handler runs on the server's goroutine and a t.Errorf from there can outlive the
	// test.
	sigFailures atomic.Int64
}

// newStubApp starts the application. grants is what it hands every connection.
func newStubApp(t *testing.T, secret string, grants ...string) *stubApp {
	t.Helper()
	a := &stubApp{secret: secret, user: "u-1", grants: grants}
	a.server = httptest.NewServer(http.HandlerFunc(a.handle))
	t.Cleanup(func() {
		a.server.Close()
		if n := a.sigFailures.Load(); n != 0 {
			t.Errorf("the stub application rejected %d gateway signature(s); the gateway is signing the connect webhook wrongly (docs/04-integration.md §1.1)", n)
		}
	})
	return a
}

// handle is the connect endpoint.
func (a *stubApp) handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		a.sigFailures.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if !a.verify(r, body) {
		a.sigFailures.Add(1)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	var call appCall
	if err := json.Unmarshal(body, &call); err != nil {
		a.sigFailures.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	call.Cookie = r.Header.Get("Cookie")
	call.Origin = r.Header.Get("X-St-Origin")
	call.Forwarded = r.Header.Get("X-St-Forwarded-For")

	a.mu.Lock()
	a.calls = append(a.calls, call)
	n := len(a.calls)
	respond, user, grants := a.respond, a.user, a.grants
	a.mu.Unlock()

	reply := appReply{Status: http.StatusOK}
	if respond != nil {
		reply = respond(call, n)
	}
	if reply.Body == "" && reply.Status == http.StatusOK {
		reply.Body = authorizedBody(user, 3600, grants...)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(reply.Status)
	_, _ = io.WriteString(w, reply.Body)
}

// verify checks the request signature and timestamp the way docs/04-integration.md §1.4's
// reference verifier does: over the literal string, with the cookie digest bound in.
func (a *stubApp) verify(r *http.Request, body []byte) bool {
	ts := r.Header.Get("X-St-Timestamp")
	seconds, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if delta := time.Since(time.Unix(seconds, 0)); delta > 300*time.Second || delta < -300*time.Second {
		return false
	}
	nonce := r.Header.Get("X-St-Nonce")
	if nonce == "" {
		return false
	}
	cookie := sha256.Sum256([]byte(r.Header.Get("Cookie")))
	digest := sha256.Sum256(body)
	mac := hmac.New(sha256.New, []byte(a.secret))
	_, _ = io.WriteString(mac, ts+"."+nonce+"."+hex.EncodeToString(cookie[:])+"."+hex.EncodeToString(digest[:]))
	want := hex.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(want), []byte(r.Header.Get("X-St-Signature"))) == 1
}

// authorizedBody is a 200 answer (docs/04-integration.md §1.2).
func authorizedBody(user string, expiresIn int, grants ...string) string {
	if grants == nil {
		grants = []string{}
	}
	body, err := json.Marshal(map[string]any{
		"user":       user,
		"channels":   grants,
		"expires_in": expiresIn,
	})
	if err != nil {
		panic("stub application body must marshal: " + err.Error())
	}
	return string(body)
}

// answerWith installs a per-call answer.
func (a *stubApp) answerWith(fn func(call appCall, n int) appReply) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.respond = fn
}

// setUser replaces the user id handed to connections from now on.
func (a *stubApp) setUser(user string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.user = user
}

// count is how many times the application has been called.
func (a *stubApp) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

// call returns the i-th recorded request.
func (a *stubApp) call(t *testing.T, i int) appCall {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if i >= len(a.calls) {
		t.Fatalf("connect webhook call %d was never made; %d recorded", i, len(a.calls))
	}
	return a.calls[i]
}

// ---------------------------------------------------------------------------
// cluster
// ---------------------------------------------------------------------------

// clusterOptions configure a cluster. The zero value is two replicas on the shared Redis
// with the default grants, which is what most tests want.
type clusterOptions struct {
	// Replicas is how many independent gateways to build. Default 2 — the topology
	// docs/11-testing.md §4 requires, and the one the unit layer cannot express.
	Replicas int

	// RedisURL overrides the shared endpoint. The bus-outage tests set it to a container
	// of their own, because they stop it.
	RedisURL string

	// Grants is what the stub application hands every connection.
	Grants []string

	// Config mutates each replica's configuration before it is built. It is called once
	// per replica, so a test may not retain the pointer.
	Config func(*config.Config)
}

// cluster is the system under test: one Redis, one stub application, and N independent
// gateway replicas, each with its own hub, its own bus connection and its own listener.
//
// The replicas are built in-process rather than as child processes. That is a genuine
// two-replica topology — two hubs, two Redis subscriber connections, two listeners,
// nothing shared but Redis — and it keeps a failing test debuggable: a stack trace names
// the goroutine that lost the message rather than a PID that has already exited.
type cluster struct {
	t *testing.T

	// prefix is bus.prefix, unique to this test. It is the isolation that actually
	// works: Redis pub/sub ignores the database index, so two tests sharing a prefix
	// would see each other's traffic (FR-21).
	prefix string

	url           string
	app           *stubApp
	controlSecret string
	replicas      []*replica

	// pub is a Redis client that is not a gateway. It publishes the way an application
	// does — straight to Redis, never through the gateway — which is the contract
	// docs/04-integration.md §2 defines, and the only way to publish a control message
	// that neither replica sent (FR-18).
	pub *redis.Client
}

// newCluster builds the topology and tears it all down through t.Cleanup.
func newCluster(t *testing.T, opts clusterOptions) *cluster {
	t.Helper()

	// A cluster on a Redis of its own — one the test started and may stop — does not
	// consult the shared endpoint at all, so it must not be skipped for its absence.
	base := ""
	if opts.RedisURL == "" {
		base = requireRedis(t)
	}

	if opts.Replicas == 0 {
		opts.Replicas = 2
	}
	if opts.Grants == nil {
		opts.Grants = []string{"room-*", "desk-*", "user-*"}
	}
	url := opts.RedisURL
	if url == "" {
		url = redisURLForDB(t, base, nextDB())
	}

	secret := "integration-webhook-secret-0123456789abcdef"
	c := &cluster{
		t:             t,
		prefix:        testPrefix(t),
		url:           url,
		app:           newStubApp(t, secret, opts.Grants...),
		controlSecret: "integration-control-secret-0123456789abcd",
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", url, err)
	}
	opt.DialTimeout = 3 * time.Second
	c.pub = redis.NewClient(opt)
	t.Cleanup(func() { _ = c.pub.Close() })

	for i := range opts.Replicas {
		c.replicas = append(c.replicas, c.newReplica(i, secret, opts.Config))
	}
	return c
}

// testPrefix is this test's bus.prefix: its name, plus randomness so a `-count=3` run
// cannot inherit a stray message from its own previous pass.
func testPrefix(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' || r == '#' {
			return '_'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("it:%s:%s:", name, randomToken(4))
}

// randomToken returns n random bytes as lowercase hex.
func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand must not fail: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// r returns replica i.
func (c *cluster) r(i int) *replica { return c.replicas[i] }

// publish sends one envelope on a bare channel name, prefixed into its bus key, exactly
// as an application does (docs/04-integration.md §2.2).
func (c *cluster) publish(channel string, env map[string]any) {
	c.t.Helper()
	payload, err := json.Marshal(env)
	if err != nil {
		c.t.Fatalf("marshal envelope: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitBudget)
	defer cancel()
	if err := c.pub.Publish(ctx, c.prefix+channel, payload).Err(); err != nil {
		c.t.Fatalf("publish to %q: %v", c.prefix+channel, err)
	}
}

// event is the ordinary envelope: an event name and a payload.
func event(name string, data any) map[string]any {
	return map[string]any{"event": name, "data": data}
}

// publishControl sends one signed control envelope on {bus.prefix}_control, from a
// publisher that is neither replica (docs/04-integration.md §3, FR-18, FR-23).
func (c *cluster) publishControl(body map[string]any) {
	c.t.Helper()
	inner, err := json.Marshal(body)
	if err != nil {
		c.t.Fatalf("marshal control body: %v", err)
	}
	envelope := signControl(c.controlSecret, string(inner), time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), waitBudget)
	defer cancel()
	if err := c.pub.Publish(ctx, c.prefix+"_control", []byte(envelope)).Err(); err != nil {
		c.t.Fatalf("publish control: %v", err)
	}
}

// signControl builds the signed envelope of docs/04-integration.md §3.
//
// The action travels as a JSON **string** and the signature covers those exact bytes. The
// receiver verifies over the literal body and only then parses it: JSON object
// serialization is not canonical, so signing "the object" is not implementable — two
// libraries ordering keys differently produce different signatures for one message.
func signControl(secret, body string, ts time.Time) string {
	seconds := ts.Unix()
	nonce := randomToken(8)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = io.WriteString(mac, strconv.FormatInt(seconds, 10)+"."+nonce+"."+body)
	envelope, err := json.Marshal(map[string]any{
		"ts":    seconds,
		"nonce": nonce,
		"body":  body,
		"sig":   hex.EncodeToString(mac.Sum(nil)),
	})
	if err != nil {
		panic("control envelope must marshal: " + err.Error())
	}
	return string(envelope)
}

// ---------------------------------------------------------------------------
// replica
// ---------------------------------------------------------------------------

// replica is one gateway: a bus connection, a hub, a bus consumer, and one listener
// carrying the websocket endpoint, GET /health and GET /ready.
type replica struct {
	t   *testing.T
	cfg *config.Config

	bus   *bus.RedisBus
	hub   *hub.Hub
	srv   *server.Server
	web   *webhook.Client
	http  *httptest.Server
	cons  *busConsumer
	index int

	drainOnce sync.Once
}

// newReplica builds one gateway against the cluster's Redis.
func (c *cluster) newReplica(i int, secret string, mutate func(*config.Config)) *replica {
	c.t.Helper()
	t := c.t

	cfg := testConfig(c.prefix, c.app.server.URL, secret, c.controlSecret)
	if mutate != nil {
		mutate(cfg)
	}

	b, err := bus.NewRedis(bus.RedisOptions{
		URL:         c.url,
		DialTimeout: cfg.Bus.DialTimeout.Duration(),
		// Tight, because a test that waits out a ten-second reconnect ceiling is a test
		// nobody runs. The schedule itself is asserted in the unit layer.
		ReconnectMin: 20 * time.Millisecond,
		ReconnectMax: 200 * time.Millisecond,
		IntakeQueue:  cfg.Bus.IntakeQueue,
	})
	if err != nil {
		t.Fatalf("replica %d: bus: %v", i, err)
	}

	h := hub.New(context.Background(), b, hub.Options{
		Prefix:                  cfg.Bus.Prefix,
		Separator:               cfg.Channels.Separator,
		Namespaces:              cfg.Namespaces,
		MaxSubscriptionsPerConn: cfg.Limits.MaxSubscriptionsPerConn,
		RetryMin:                20 * time.Millisecond,
		RetryMax:                200 * time.Millisecond,
	})

	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))

	web, err := webhook.New(webhook.Options{App: cfg.App, Logger: log})
	if err != nil {
		t.Fatalf("replica %d: webhook: %v", i, err)
	}

	srv, err := server.New(server.Options{Config: cfg, Hub: h, Bus: b, Webhook: web, Log: log})
	if err != nil {
		t.Fatalf("replica %d: server: %v", i, err)
	}

	r := &replica{
		t:     t,
		cfg:   cfg,
		bus:   b,
		hub:   h,
		srv:   srv,
		web:   web,
		index: i,
	}
	r.cons = startConsumer(h, b, cfg, c.controlSecret, log)
	r.http = httptest.NewServer(srv.Handler())

	// One cleanup, in one order, because the order is a contract: drain the connections,
	// stop the listeners, close the bus so the consumer's workers end, wait for it, and
	// only then close the hub — hub.Close may not run concurrently with Dispatch.
	t.Cleanup(func() {
		r.drain()
		r.http.Close()
		if err := b.Close(); err != nil {
			t.Errorf("replica %d: bus close: %v", i, err)
		}
		r.cons.wait()
		h.Close()
	})
	return r
}

// testConfig is the configuration every replica starts from: the documented defaults for
// the keys these tests read, with the required ones filled in.
func testConfig(prefix, connectURL, webhookSecret, controlSecret string) *config.Config {
	return &config.Config{
		Server: config.Server{
			Listen:            "127.0.0.1:0",
			Path:              "/ws",
			AllowedOrigins:    []string{testOrigin},
			HandshakeTimeout:  config.Duration(5 * time.Second),
			PingInterval:      config.Duration(25 * time.Second),
			PongTimeout:       config.Duration(10 * time.Second),
			DrainTimeout:      config.Duration(20 * time.Second),
			DrainSpread:       config.Duration(60 * time.Second),
			ReadHeaderTimeout: config.Duration(5 * time.Second),
		},
		App: config.App{
			Name:               "app",
			ConnectURL:         connectURL,
			WebhookSecrets:     []string{webhookSecret},
			ConnectTimeout:     config.Duration(10 * time.Second),
			ConnectQueue:       4096,
			WebhookTimeout:     config.Duration(3 * time.Second),
			WebhookConcurrency: 32,
			WebhookRetries:     1,
			MinExpiry:          config.Duration(60 * time.Second),
			MaxExpiry:          config.Duration(6 * time.Hour),
		},
		Bus: config.Bus{
			Kind:            "redis",
			DialTimeout:     config.Duration(3 * time.Second),
			IntakeQueue:     bus.DefaultIntakeQueue,
			DispatchWorkers: 2,
			ReadyGrace:      config.Duration(30 * time.Second),
			Prefix:          prefix,
		},
		Channels: config.Channels{Separator: "-"},
		Namespaces: []config.Namespace{
			{Name: "room", RateLimit: "10/s"},
			{Name: "desk", ClientEvents: true, RateLimit: "10/s"},
			{Name: "user", RateLimit: "10/s"},
		},
		Limits: config.Limits{
			MaxConnections:          25000,
			MaxSubscriptionsPerConn: 500,
			MaxConnectionsPerUser:   20000,
			ReadBuffer:              2048,
			WriteBuffer:             2048,
			OutboundQueue:           256,
			MaxMessageSize:          32768,
			MaxFrameSize:            16384,
			MaxChannelLength:        255,
		},
		Control: config.Control{Secret: controlSecret},
		Log:     config.Log{Level: "warn", Format: "text"},
	}
}

// wsURL is this replica's websocket endpoint.
func (r *replica) wsURL() string {
	return "ws" + strings.TrimPrefix(r.http.URL, "http") + r.cfg.Server.Path
}

// upstream is the number of channels this replica has subscribed on the bus, confirmed by
// Redis. It is the count FR-10's acceptance criterion names, and it always includes the
// control channel.
func (r *replica) upstream() int { return r.bus.Health().Subscriptions }

// reconnects is the number of bus transports established after the first. Climbing
// against a healthy Redis is the M8 signature: eviction for a slow subscriber, not an
// unstable server.
func (r *replica) reconnects() uint64 { return r.bus.Health().Reconnects }

// ready is the status code of GET /ready.
//
// Readiness, and only readiness: 503 once the bus has been down longer than
// bus.ready_grace. Connections stay open and silent while the bus is down; this reports
// it, and nothing closes (NFR-8).
func (r *replica) ready() int {
	r.t.Helper()
	return r.probeStatus("/ready")
}

// drain shuts this replica down gracefully (FR-19). It is idempotent, so a test may drain
// a replica explicitly and let the cleanup drain it again.
func (r *replica) drain() {
	r.drainOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitBudget)
		defer cancel()
		if err := r.srv.Drain(ctx); err != nil {
			r.t.Errorf("replica %d: drain: %v", r.index, err)
		}
	})
}

// ---------------------------------------------------------------------------
// bus consumer
// ---------------------------------------------------------------------------

// busConsumer is the shipped bus consumer — internal/consumer, the same package
// cmd/sidecartunnel constructs — with the lifecycle this suite needs wrapped around it.
//
// It used to be a reimplementation living in this file: the routing rule, the control
// queue and the FR-23 signature check, written a second time because the originals were
// in package main and nothing outside that directory could import them. Two
// implementations of one rule drift, and the copy under test was not the copy that
// shipped (docs/12-roadmap.md §4). The package moved; this is now a thin wrapper, and
// what these tests exercise is the delivery path the binary runs.
type busConsumer struct {
	*consumer.Consumer

	stop context.CancelFunc
	done chan struct{}
}

// startConsumer starts the dispatch workers and the control goroutine. They end when the
// bus closes its intake channel, or when wait cancels them.
func startConsumer(h *hub.Hub, b bus.Bus, cfg *config.Config, controlSecret string, log *slog.Logger) *busConsumer {
	c := consumer.New(consumer.Options{
		Bus:            b,
		Hub:            h,
		Log:            log,
		Secret:         []byte(controlSecret),
		Workers:        cfg.Bus.DispatchWorkers,
		MaxMessageSize: cfg.Limits.MaxMessageSize,
	})
	ctx, cancel := context.WithCancel(context.Background())
	bc := &busConsumer{Consumer: c, stop: cancel, done: make(chan struct{})}
	go func() {
		defer close(bc.done)
		c.Run(ctx)
	}()
	return bc
}

// wait blocks until every consumer goroutine has returned. The caller closes the bus
// first: that is what ends the drain of Receive, and the cancellation here is the backstop
// for a bus that did not.
func (c *busConsumer) wait() {
	c.stop()
	<-c.done
}

// ---------------------------------------------------------------------------
// waiting
// ---------------------------------------------------------------------------

// waitFor blocks until cond holds, and fails the test rather than hanging.
//
// It is the failure detector docs/14-coding-standards.md §2 allows in place of a sleep:
// it yields rather than sleeping, the happy path leaves it in microseconds, and the
// budget only expires when the test was going to fail anyway. what is phrased as the
// thing being waited for, so the failure reads as a sentence.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitBudget)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", waitBudget, what)
		}
		runtime.Gosched()
	}
}

// waitUpstream blocks until a replica's bus is connected and its confirmed subscription
// count is exactly n.
//
// Waiting on Redis's own confirmation, rather than on a duration, is what makes the
// publish that follows deterministic: Bus.Sync returns only once Redis has processed the
// SUBSCRIBE, so a message published afterwards is delivered.
//
// The Connected half is not belt-and-braces. bus.Health().Subscriptions keeps its last
// value while the transport is down — release does not reset it, adopt does — so after a
// Redis restart the count still reads as whatever it was before the outage until the new
// connection is adopted. A wait on the count alone is satisfied by that stale value, and
// the test then publishes into a gateway that has not resubscribed yet. Asserting
// connectivity first is what makes this wait mean "resubscribed" rather than "not yet
// noticed".
func waitUpstream(t *testing.T, r *replica, n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("replica %d to be connected and hold %d upstream subscription(s)", r.index, n), func() bool {
		health := r.bus.Health()
		return health.Connected && health.Subscriptions == n
	})
}

// health is the status code of GET /health.
//
// It is liveness and must answer 200 while the process runs, whatever the bus is doing. A
// /health that consulted the bus, wired to a liveness probe, would kill every replica
// simultaneously during a Redis restart (FR-20, docs/13-review-findings.md M20).
func (r *replica) health() int {
	r.t.Helper()
	return r.probeStatus("/health")
}

// probeStatus performs one GET against this replica's listener and returns its status.
//
// /health and /ready share the listener the websockets are on. The loopback listener they
// used to have went with the operator API it was carrying (docs/12-roadmap.md §2).
func (r *replica) probeStatus(path string) int {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), readBudget)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.http.URL+path, nil)
	if err != nil {
		r.t.Fatalf("build %s request: %v", path, err)
	}
	resp, err := r.http.Client().Do(req)
	if err != nil {
		r.t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// timeNow is time.Now, named so a test that signs a control envelope reads as signing it
// "now" rather than reaching for the clock inline.
func timeNow() time.Time { return time.Now() }

// errTimeout is what a background reader reports when the frame it is waiting for never
// arrived. It is a value rather than a t.Fatalf because the reader runs on a goroutine
// that may outlive the test, and failing a test from one of those panics the run.
var errTimeout = fmt.Errorf("no sentinel frame arrived within %s", waitBudget)

// errDisconnected describes a connection the gateway closed when it should not have.
func errDisconnected(f frame) error {
	return fmt.Errorf("the gateway closed the connection: %s", f)
}
