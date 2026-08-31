package main

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/metrics"
	"github.com/raghulj/sidecartunnel/internal/server"
)

// fakeHealth is a bus health source a test can set.
type fakeHealth struct{ health bus.Health }

func (f *fakeHealth) Health() bus.Health { return f.health }

// fakeInflight is a webhook client's in-flight count.
type fakeInflight struct{ n int }

func (f *fakeInflight) InFlight() int { return f.n }

// fakeStats is a server's cumulative counters.
type fakeStats struct{ stats server.Stats }

func (f *fakeStats) Stats() server.Stats { return f.stats }

func newSampler(t *testing.T) (*sampler, *prometheus.Registry, *fakeHealth, *fakeInflight, *fakeStats) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg, metrics.Options{App: "app"})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	health := &fakeHealth{}
	inflight := &fakeInflight{}
	stats := &fakeStats{}
	return &sampler{metrics: m, bus: health, webhook: inflight, server: stats}, reg, health, inflight, stats
}

// TestSampler_FoldsBusHealth is what admin.Options.Refresh exists for: the gauges whose
// value is owned by the bus are sampled at scrape time, which makes them exact and saves
// the process a ticker whose period would become their resolution.
func TestSampler_FoldsBusHealth(t *testing.T) {
	s, reg, health, inflight, _ := newSampler(t)
	health.health = bus.Health{
		Connected: true, Subscriptions: 12, IntakeDepth: 3, Reconnects: 2, Dropped: 5,
	}
	inflight.n = 7

	s.refresh()

	for _, tt := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"st_bus_subscriptions_current", nil, 12},
		{"st_bus_intake_depth", nil, 3},
		{"st_bus_reconnects_total", nil, 2},
		{"st_messages_dropped_total", map[string]string{"reason": "intake"}, 5},
		{"st_webhook_inflight", nil, 7},
	} {
		if got := metricValue(t, reg, tt.name, tt.labels); got != tt.want {
			t.Fatalf("%s = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestSampler_FoldsConnectionCountersAsDeltas covers the rule every counter fold here
// obeys: the sources own cumulative totals and a Prometheus counter only goes up, so a
// second scrape must advance by the increase and not by the total.
func TestSampler_FoldsConnectionCountersAsDeltas(t *testing.T) {
	s, reg, _, _, stats := newSampler(t)

	stats.stats = server.Stats{
		Accepted: 3, Refused: 1, Unavailable: 2, OverCapacity: 1, UserLimited: 1, OriginRejected: 4,
	}
	s.refresh()
	s.refresh() // A second scrape with no traffic must add nothing.

	for _, tt := range []struct {
		result metrics.Result
		want   float64
	}{
		{metrics.ResultAccepted, 3},
		{metrics.ResultUnauthorized, 1},
		{metrics.ResultUnavailable, 2},
		{metrics.ResultOverLimit, 2},
		{metrics.ResultOriginRejected, 4},
	} {
		got := metricValue(t, reg, "st_connections_total", map[string]string{"result": string(tt.result)})
		if got != tt.want {
			t.Fatalf("st_connections_total{result=%s} = %v, want %v", tt.result, got, tt.want)
		}
	}
	if got := metricValue(t, reg, "st_origin_rejected_total", nil); got != 4 {
		t.Fatalf("st_origin_rejected_total = %v, want 4", got)
	}

	stats.stats.Accepted = 5
	s.refresh()
	if got := metricValue(t, reg, "st_connections_total", map[string]string{"result": "accepted"}); got != 5 {
		t.Fatalf("st_connections_total{result=accepted} = %v after two more, want 5", got)
	}
}

// TestSampler_TreatsAWindingBackSourceAsAReset mirrors Metrics.ObserveBus's rule: a
// snapshot that went backwards is a new counter rather than negative progress, and
// Counter.Add panics on a negative value.
func TestSampler_TreatsAWindingBackSourceAsAReset(t *testing.T) {
	s, reg, _, _, stats := newSampler(t)

	stats.stats = server.Stats{Accepted: 10}
	s.refresh()
	stats.stats = server.Stats{Accepted: 2}
	s.refresh()

	if got := metricValue(t, reg, "st_connections_total", map[string]string{"result": "accepted"}); got != 12 {
		t.Fatalf("st_connections_total{result=accepted} = %v, want 12 after a reset", got)
	}
}

// TestDelta covers the arithmetic on its own, including the reset row that keeps
// Counter.Add from ever seeing a negative value.
func TestDelta(t *testing.T) {
	tests := []struct {
		name      string
		now, last uint64
		want      uint64
	}{
		{"no change", 5, 5, 0},
		{"increase", 9, 5, 4},
		{"reset", 2, 5, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := delta(tt.now, tt.last); got != tt.want {
				t.Fatalf("delta(%d, %d) = %d, want %d", tt.now, tt.last, got, tt.want)
			}
		})
	}
}

// TestSampler_IsSafeUnderConcurrentScrapes is the property the mutex exists for: two
// Prometheus servers can scrape at once, and folding a delta twice reports double the
// traffic that happened.
func TestSampler_IsSafeUnderConcurrentScrapes(t *testing.T) {
	s, reg, health, _, stats := newSampler(t)
	health.health = bus.Health{Connected: true}
	stats.stats = server.Stats{Accepted: 1}

	done := make(chan struct{})
	for range 4 {
		go func() {
			s.refresh()
			done <- struct{}{}
		}()
	}
	for range 4 {
		select {
		case <-done:
		case <-time.After(waitFor):
			t.Fatal("a refresh did not return")
		}
	}
	if got := metricValue(t, reg, "st_connections_total", map[string]string{"result": "accepted"}); got != 1 {
		t.Fatalf("st_connections_total{result=accepted} = %v after four concurrent scrapes, want 1", got)
	}
}
