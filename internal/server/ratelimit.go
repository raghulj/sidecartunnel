package server

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/conn"
)

// Client-event abuse thresholds, fixed by docs/03-client-protocol.md §4.4.
const (
	// maxViolations is how many rate-limit errors within violationWindow end the
	// connection with proto.CloseRateLimited.
	maxViolations = 10

	// violationWindow is the span those violations are counted over.
	violationWindow = 60 * time.Second
)

// defaultRate is namespaces[].rate_limit's documented default, applied to a namespace
// block that names none (docs/08-config.md §3).
var defaultRate = rate{count: 10, per: time.Second}

// rate is one namespace's client-event allowance, parsed from the "<int>/<s|m>" form.
type rate struct {
	count int
	per   time.Duration
}

// parseRates parses namespaces[].rate_limit for every configured namespace.
//
// It happens at construction so that an unparseable rate is a startup error naming the
// key, rather than a client event that mysteriously fails an hour later (NFR-5). The map
// is keyed by namespace name, and internal/hub resolves channels against the same list,
// so every namespace a publish can resolve to is present.
func parseRates(namespaces []config.Namespace) (map[string]rate, error) {
	rates := make(map[string]rate, len(namespaces)+1)
	// The reserved empty name is the block internal/hub installs when the list is empty,
	// and the one a channel with no separator resolves to (FR-11).
	rates[""] = defaultRate
	for i, ns := range namespaces {
		if ns.RateLimit == "" {
			rates[ns.Name] = defaultRate
			continue
		}
		parsed, err := parseRate(ns.RateLimit)
		if err != nil {
			return nil, fmt.Errorf("server: namespaces[%d].rate_limit: %w", i, err)
		}
		rates[ns.Name] = parsed
	}
	return rates, nil
}

// parseRate reads the "<int>/<s|m>" form from docs/08-config.md §3.
func parseRate(value string) (rate, error) {
	count, unit, found := strings.Cut(value, "/")
	if !found {
		return rate{}, fmt.Errorf("%q is not of the form <int>/<s|m>, such as %q", value, "10/s")
	}
	var per time.Duration
	switch unit {
	case "s":
		per = time.Second
	case "m":
		per = time.Minute
	default:
		return rate{}, fmt.Errorf("%q has unit %q, want \"s\" or \"m\"", value, unit)
	}
	n, err := strconv.Atoi(count)
	if err != nil {
		return rate{}, fmt.Errorf("%q has a rate that is not an integer: %w", value, err)
	}
	if n < 1 {
		return rate{}, fmt.Errorf("%q has a rate below 1, which would refuse every client event", value)
	}
	return rate{count: n, per: per}, nil
}

// limiter is one connection's client-event allowance, per namespace, plus the count of
// violations that ends the connection.
//
// It is a fixed window rather than a token bucket on purpose: the rate is expressed in
// docs/08-config.md §3 as "<int> per <unit>", a fixed window is exactly that sentence,
// and the burst behaviour at a window boundary is not worth a second concept in a control
// whose only job is to stop one client shouting.
//
// Every method is safe for concurrent use, though in practice only the connection's
// reader goroutine calls them.
type limiter struct {
	clock conn.Clock

	mu         sync.Mutex
	windows    map[string]*window
	violations int
	since      time.Time
}

// window is one namespace's current fixed window.
type window struct {
	start time.Time
	count int
}

func newLimiter(clock conn.Clock) *limiter {
	return &limiter{clock: clock, windows: make(map[string]*window)}
}

// allow reports whether one client event on namespace is within limit, and — when it is
// not — whether this connection has now produced maxViolations refusals inside
// violationWindow and must be closed.
//
// Both answers come from one locked section, because they are two readings of the same
// event and computing them apart lets a concurrent publish be counted by one and not the
// other.
func (l *limiter) allow(namespace string, limit rate) (allowed, abusive bool) {
	now := l.clock.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	w, ok := l.windows[namespace]
	if !ok {
		w = &window{start: now}
		l.windows[namespace] = w
	}
	if now.Sub(w.start) >= limit.per {
		w.start = now
		w.count = 0
	}
	w.count++
	if w.count <= limit.count {
		return true, false
	}

	if l.violations == 0 || now.Sub(l.since) >= violationWindow {
		l.since = now
		l.violations = 0
	}
	l.violations++
	return false, l.violations >= maxViolations
}
