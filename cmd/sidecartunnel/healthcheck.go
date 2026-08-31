package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// healthcheckTimeout bounds the loopback request. It is well inside the 10s interval
// docs/10-operations.md §3's compose healthcheck uses, so a hung probe fails rather than
// overlapping the next one.
const healthcheckTimeout = 2 * time.Second

// healthcheck performs a loopback GET /health against server.listen and returns 0 or 1.
//
// It is the container healthcheck, and it is a subcommand rather than a curl line for one
// reason: the release image is distroless static, with no shell and no curl in it, so the
// only executable available to probe the process is the process's own binary. That is the
// whole justification for its existence, and it is why the Dockerfile's HEALTHCHECK is
// CMD ["/sidecartunnel", "healthcheck"] in exec form — there is no /bin/sh to parse a
// string form (docs/10-operations.md §1, §3).
//
// It is liveness and nothing else. It must never consult the bus, and it never will,
// because /health never consults the bus: it probes /health, and only /health. /health is
// served from server.listen, alongside the websocket endpoint — there is no second
// listener any more (docs/12-roadmap.md §2).
//
// That rule is not a preference. A Redis restart makes every replica unready at once. A
// bus-dependent check wired to a liveness probe therefore kills every replica
// simultaneously, drops every connection, and turns an eight-second blip into a full
// application outage as the entire fleet re-authorizes together against one connect
// webhook (docs/04-integration.md §4, docs/13-review-findings.md M20). Pointing this at
// /ready would rebuild that outage exactly, which is why /ready is not an option here and
// why README.md's Health Checks section says never to wire /ready to a liveness probe.
//
// The exit status is the entire interface: 0 the process is alive, 1 it is not. Anything
// it has to say goes to stderr, where `docker inspect` keeps the last few outputs.
func healthcheck(ctx context.Context, cfg *config.Config, stderr io.Writer) int {
	url, err := healthURL(cfg.Server.Listen)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return exitFailure
	}

	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		// coverage: NewRequestWithContext fails only on an unparseable URL or a bad
		// method, and healthURL has already built this one from a validated host and
		// port. It is reported rather than assumed away.
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return exitFailure
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return exitFailure
	}
	// The body is not drained before closing. This process exits within milliseconds of
	// reading the status, so there is no connection pool to keep warm and nothing to gain
	// from reading a body no caller looks at.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: %s answered %d\n", url, resp.StatusCode)
		return exitFailure
	}
	return exitOK
}

// healthURL turns server.listen into the loopback URL to probe.
//
// A wildcard host becomes 127.0.0.1: server.listen is normally ":8000" or "0.0.0.0:8000",
// and neither is an address to connect to. The probe is always loopback because it runs
// inside the container it is checking — it is asking whether this process is alive, not
// whether the network can reach it (docs/10-operations.md §1).
func healthURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("server.listen %q is not a host:port address: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/health", nil
}
