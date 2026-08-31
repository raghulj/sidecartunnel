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

// healthcheck performs a loopback GET /health against admin.listen and returns 0 or 1.
//
// It exists so a distroless image with no shell and no curl can still declare a container
// healthcheck: the healthcheck runs the binary in a subcommand rather than shelling out
// (docs/10-operations.md §1, §3).
//
// It checks liveness only and never the bus, because /health never consults the bus. A
// bus-dependent healthcheck wired to a liveness probe kills every replica at once during a
// Redis restart, drops every connection, and turns an eight-second blip into a full
// application outage as the whole fleet re-authorizes together
// (docs/04-integration.md §4, docs/13-review-findings.md M20). Pointing this at /ready
// would rebuild that outage exactly.
func healthcheck(ctx context.Context, cfg *config.Config, stderr io.Writer) int {
	url, err := healthURL(cfg.Admin.Listen)
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

// healthURL turns admin.listen into the loopback URL to probe.
//
// A wildcard host becomes 127.0.0.1: admin.listen may legitimately be ":9001" or
// "0.0.0.0:9001", and neither is an address to connect to. The probe is always loopback,
// because it runs inside the container it is checking and the admin listener defaults to
// loopback precisely so nothing outside can reach it (docs/10-operations.md §1).
func healthURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("admin.listen %q is not a host:port address: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/health", nil
}
