package integration_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// envRedisURL names the Redis the suite runs against. CI sets it; a laptop that has run
// `make redis` needs nothing, because the default is that target's endpoint.
const envRedisURL = "ST_TEST_REDIS_URL"

// defaultRedisURL is what `make redis` starts: redis:8-alpine on 6380 with
// client-output-buffer-limit "pubsub 256mb 64mb 60".
//
// Port 6380 rather than 6379 on purpose. A developer's laptop usually already has a Redis
// on the default port holding something they care about, and a test suite that publishes
// into it — or that a stray FLUSHDB takes down — is a test suite people stop running
// (docs/10-operations.md §3).
const defaultRedisURL = "redis://127.0.0.1:6380/0"

// redisDatabases is how many logical databases a stock Redis exposes. Each test takes a
// distinct one.
//
// It buys less isolation than it looks like it does, and the comment is here so nobody
// relies on it for more: Redis pub/sub is **not** scoped to a database. A SUBSCRIBE on
// db 3 receives a PUBLISH made on db 9. The isolation that actually works is the
// per-test bus.prefix below; the database index is a second, cheaper fence that keeps any
// future key-space use from colliding.
const redisDatabases = 16

// probe results, resolved once per process. The suite must skip cleanly rather than fail
// when there is no Redis, so that `go test ./...` still passes on a laptop with no Docker.
var (
	probeOnce sync.Once
	probeURL  string
	probeErr  error

	// dbSeq hands out the per-test database index.
	dbSeq atomic.Uint64
)

// baseRedisURL is the configured endpoint, or the `make redis` default.
func baseRedisURL() string {
	if u := os.Getenv(envRedisURL); u != "" {
		return u
	}
	return defaultRedisURL
}

// probeRedis dials the configured Redis once and remembers the answer. Every test asks;
// only the first one pays, and a suite run with no Redis skips in milliseconds rather
// than spending a dial timeout per test.
func probeRedis() (string, error) {
	probeOnce.Do(func() {
		probeURL = baseRedisURL()
		opt, err := redis.ParseURL(probeURL)
		if err != nil {
			probeErr = fmt.Errorf("%s is not a valid redis url: %w", envRedisURL, err)
			return
		}
		opt.DialTimeout = 2 * time.Second
		client := redis.NewClient(opt)
		defer func() { _ = client.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		probeErr = client.Ping(ctx).Err()
	})
	return probeURL, probeErr
}

// requireRedis skips the calling test, with an actionable message, when there is no Redis
// to run it against.
//
// Skipping rather than failing is deliberate. `go test ./...` on a laptop without Docker
// must still pass, or the unit layer stops being run at all — and a suite that cannot be
// run on a laptop is a suite that only fails in CI, where it is most expensive to debug
// (docs/11-testing.md §1).
func requireRedis(t *testing.T) string {
	t.Helper()
	url, err := probeRedis()
	if err != nil {
		t.Skipf("no Redis at %s: %v\n"+
			"    start one with: make redis\n"+
			"    or point the suite elsewhere: %s=redis://host:port/0 go test ./test/integration/...",
			url, err, envRedisURL)
	}
	return url
}

// redisURLForDB returns base with its database index replaced, so every test gets one of
// its own. See redisDatabases for what that does and does not isolate.
func redisURLForDB(t *testing.T, base string, db int) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %s %q: %v", envRedisURL, base, err)
	}
	u.Path = "/" + strconv.Itoa(db)
	return u.String()
}

// nextDB hands the calling test its own database index.
func nextDB() int {
	return int(dbSeq.Add(1) % redisDatabases)
}

// ---------------------------------------------------------------------------
// throwaway containers, for the tests that kill Redis
// ---------------------------------------------------------------------------

// dockerBudget bounds one docker command. Generous: the first run of the suite may be
// pulling an image.
const dockerBudget = 3 * time.Minute

// requireDocker skips the calling test when there is no usable Docker daemon.
//
// Only the bus-outage tests need one — everything else runs against whatever Redis
// ST_TEST_REDIS_URL names, which may be a managed instance nobody may stop.
func requireDocker(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("no docker on PATH: %v\n"+
			"    this test stops and restarts a Redis of its own; it cannot use a shared one", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		t.Skipf("docker is present but not usable: %v: %s", err, strings.TrimSpace(string(out)))
	}
}

// redisContainer is a Redis this test owns outright, so it may be stopped and started.
//
// The shared Redis cannot be used for that: every other test in the run is on it, and
// killing it would fail them all with a symptom that has nothing to do with what they
// assert. NFR-8 is the highest-value test in the suite and it must not be the reason
// somebody starts re-running the job (docs/11-testing.md §4).
type redisContainer struct {
	name string
	url  string
}

// startRedis runs a throwaway Redis on a free loopback port and returns its URL.
//
// The port is chosen here rather than left to Docker: a container published with an
// automatic host port may be given a different one when it is restarted, and this
// container exists precisely to be restarted.
//
// It is deliberately not --rm. `docker stop` on an --rm container deletes it, and there
// would be nothing left to start again.
func startRedis(t *testing.T) *redisContainer {
	t.Helper()
	requireDocker(t)

	name := fmt.Sprintf("st-it-%s-%d", strings.ToLower(randomToken(4)), os.Getpid())
	port := freePort(t)
	c := &redisContainer{
		name: name,
		url:  fmt.Sprintf("redis://127.0.0.1:%d/0", port),
	}
	t.Cleanup(func() { c.remove(t) })

	run := dockerRun(t, "run", "-d", "--name", name,
		"-p", fmt.Sprintf("127.0.0.1:%d:6379", port),
		"redis:8-alpine", "redis-server",
		// The same setting docs/10-operations.md §3 requires of a production Redis. The
		// stock default evicts a subscriber that falls behind, which is the M8
		// oscillation this suite has a test for.
		"--client-output-buffer-limit", "pubsub 256mb 64mb 60",
		"--save", "")
	if run != "" {
		t.Logf("started redis container %s on :%d", name, port)
	}
	c.waitUp(t)
	return c
}

// stop kills the container. Connections to it are severed; this is the bus loss of NFR-8.
func (c *redisContainer) stop(t *testing.T) {
	t.Helper()
	dockerRun(t, "stop", "-t", "1", c.name)
}

// start brings it back and waits until it answers PING.
func (c *redisContainer) start(t *testing.T) {
	t.Helper()
	dockerRun(t, "start", c.name)
	c.waitUp(t)
}

// remove deletes the container, whatever state it is in.
func (c *redisContainer) remove(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dockerBudget)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "rm", "-f", c.name).CombinedOutput(); err != nil {
		t.Logf("docker rm -f %s: %v: %s", c.name, err, strings.TrimSpace(string(out)))
	}
}

// waitUp blocks until the container answers PING. The wait is on the server's own answer
// rather than on a duration, so a slow machine takes longer and a fast one does not wait.
func (c *redisContainer) waitUp(t *testing.T) {
	t.Helper()
	opt, err := redis.ParseURL(c.url)
	if err != nil {
		t.Fatalf("parse container url %q: %v", c.url, err)
	}
	opt.DialTimeout = time.Second
	client := redis.NewClient(opt)
	defer func() { _ = client.Close() }()

	waitFor(t, "the throwaway Redis to answer PING", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return client.Ping(ctx).Err() == nil
	})
}

// dockerRun runs one docker command and fails the test on a non-zero exit.
func dockerRun(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), dockerBudget)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out))
}

// freePort returns a loopback port nothing is listening on.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return port
}
