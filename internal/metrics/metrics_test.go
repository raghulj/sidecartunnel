package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// newTestMetrics builds a Metrics over its own registry with the namespaces the tests
// use, and fails the test if construction does.
func newTestMetrics(t *testing.T) (*prometheus.Registry, *Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := New(reg, Options{App: "main", Separator: "-", Namespaces: []string{"room", "user", ""}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return reg, m
}

// assertExposition compares the named metric families against expected exposition text.
func assertExposition(t *testing.T, g prometheus.Gatherer, expected string, names ...string) {
	t.Helper()
	if err := testutil.GatherAndCompare(g, strings.NewReader(expected), names...); err != nil {
		t.Errorf("exposition mismatch: %v", err)
	}
}

// TestNew_Defaults applies the documented defaults for an otherwise zero Options:
// app.name "app" and channels.separator "-" (docs/08-config.md §3).
func TestNew_Defaults(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := New(reg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.OriginRejected()
	assertExposition(t, reg, `
# HELP st_connections_current Connections currently open on this replica.
# TYPE st_connections_current gauge
st_connections_current{app="app"} 0
`, "st_connections_current")

	// The default separator still splits a namespace off a channel.
	if got := m.Namespace("room-4410"); got.String() != otherNamespace {
		t.Errorf("Namespace(room-4410) with no configured namespaces = %q, want %q", got, otherNamespace)
	}
}

// TestNew_DuplicateRegistration returns the registry's error rather than panicking: two
// Metrics on one registry is a wiring mistake and must be reported, not hidden
// (docs/14-coding-standards.md §6).
func TestNew_DuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := New(reg, Options{}); err != nil {
		t.Fatalf("first New: %v", err)
	}
	_, err := New(reg, Options{})
	if err == nil {
		t.Fatal("second New on the same registry: want an error, got nil")
	}
	if !strings.Contains(err.Error(), "st_") {
		t.Errorf("error %q does not name the metric that collided", err)
	}
}

// TestNew_TwoIndependentInstances is the whole reason there is no package-level singleton
// and no promauto: two tests in one process must be able to hold two registries and not
// collide (docs/14-coding-standards.md §7).
func TestNew_TwoIndependentInstances(t *testing.T) {
	regA, a := newTestMetrics(t)
	regB, b := newTestMetrics(t)

	a.ConnectionOpened()
	a.ConnectionOpened()
	b.ConnectionOpened()

	assertExposition(t, regA, `
# HELP st_connections_current Connections currently open on this replica.
# TYPE st_connections_current gauge
st_connections_current{app="main"} 2
`, "st_connections_current")
	assertExposition(t, regB, `
# HELP st_connections_current Connections currently open on this replica.
# TYPE st_connections_current gauge
st_connections_current{app="main"} 1
`, "st_connections_current")
}

// TestMetrics_NamesAndLabels asserts that every metric in docs/10-operations.md §5 is
// registered under exactly the name and label set that table gives it. An operator's
// alerts are written against that table, so a rename is a broken alert.
func TestMetrics_NamesAndLabels(t *testing.T) {
	reg, m := newTestMetrics(t)

	// Touch every metric once so each family appears in the exposition.
	ns := m.Namespace("room-4410")
	m.ConnectionOpened()
	m.ConnectionResult(ResultAccepted)
	m.ConnectionClosed(time.Second)
	m.SubscriptionsAdd(ns, 1)
	m.MessagePublished(ns)
	m.MessagesDelivered(ns, 3)
	m.MessageDropped(DropOversize)
	m.WebhookRequest(StatusOf(200), 40*time.Millisecond)
	m.SetWebhookInflight(2)
	m.ObserveBus(bus.Health{Subscriptions: 4, IntakeDepth: 1, Reconnects: 1, Dropped: 1})
	m.BusSyncFailed()
	m.SlowConsumerDisconnect()
	m.SubscribeDenied(ns)
	m.OriginRejected()
	m.ControlRejected(ControlStale)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string][]string, len(families))
	for _, f := range families {
		labels := make([]string, 0, 2)
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		got[f.GetName()] = labels
	}

	// The table, transcribed. Const label "app" is part of the label set.
	want := map[string][]string{
		"st_connections_current":             {"app"},
		"st_connections_total":               {"app", "result"},
		"st_connection_duration_seconds":     {"app"},
		"st_subscriptions_current":           {"app", "namespace"},
		"st_messages_published_total":        {"app", "namespace"},
		"st_messages_delivered_total":        {"app", "namespace"},
		"st_messages_dropped_total":          {"reason"},
		"st_webhook_duration_seconds":        {"app", "status"},
		"st_webhook_inflight":                {"app"},
		"st_webhook_requests_total":          {"app", "status"},
		"st_bus_subscriptions_current":       {},
		"st_bus_reconnects_total":            {},
		"st_bus_intake_depth":                {},
		"st_bus_sync_failures_total":         {},
		"st_slow_consumer_disconnects_total": {"app"},
		"st_subscribe_denied_total":          {"app", "namespace"},
		"st_origin_rejected_total":           {},
		"st_control_rejected_total":          {"reason"},
	}
	for name, wantLabels := range want {
		gotLabels, ok := got[name]
		if !ok {
			t.Errorf("%s is not registered", name)
			continue
		}
		if strings.Join(gotLabels, ",") != strings.Join(wantLabels, ",") {
			t.Errorf("%s labels = %v, want %v", name, gotLabels, wantLabels)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s is registered but is not in docs/10-operations.md §5", name)
		}
	}
}

// TestConnections covers the three connection metrics, including the result label
// docs/10-operations.md §5 calls out by name.
func TestConnections(t *testing.T) {
	reg, m := newTestMetrics(t)

	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionResult(ResultAccepted)
	m.ConnectionResult(ResultAccepted)
	m.ConnectionResult(ResultOriginRejected)
	m.ConnectionClosed(90 * time.Second)

	assertExposition(t, reg, `
# HELP st_connections_current Connections currently open on this replica.
# TYPE st_connections_current gauge
st_connections_current{app="main"} 1
# HELP st_connections_total Connection attempts by outcome.
# TYPE st_connections_total counter
st_connections_total{app="main",result="accepted"} 2
st_connections_total{app="main",result="origin_rejected"} 1
`, "st_connections_current", "st_connections_total")

	if got := histogram(t, reg, "st_connection_duration_seconds"); got.count != 1 || got.sum != 90 {
		t.Errorf("st_connection_duration_seconds = %+v, want count 1 sum 90", got)
	}
}

// TestMessagesAndSubscriptions covers the namespace-labelled families.
func TestMessagesAndSubscriptions(t *testing.T) {
	reg, m := newTestMetrics(t)

	room := m.Namespace("room-4410")
	user := m.Namespace("user-7")

	m.SubscriptionsAdd(room, 3)
	m.SubscriptionsAdd(room, -1)
	m.SubscriptionsAdd(user, 1)
	m.MessagePublished(room)
	m.MessagesDelivered(room, 5)

	assertExposition(t, reg, `
# HELP st_subscriptions_current Subscriptions currently held on this replica, by namespace.
# TYPE st_subscriptions_current gauge
st_subscriptions_current{app="main",namespace="room"} 2
st_subscriptions_current{app="main",namespace="user"} 1
# HELP st_messages_published_total Messages published through this replica, by namespace.
# TYPE st_messages_published_total counter
st_messages_published_total{app="main",namespace="room"} 1
# HELP st_messages_delivered_total Messages written to a subscriber, by namespace. The ratio to published is the average fan-out.
# TYPE st_messages_delivered_total counter
st_messages_delivered_total{app="main",namespace="room"} 5
`, "st_subscriptions_current", "st_messages_published_total", "st_messages_delivered_total")
}

// TestMessageDropped covers every reason docs/10-operations.md §5 names.
func TestMessageDropped(t *testing.T) {
	reg, m := newTestMetrics(t)
	for _, r := range []DropReason{DropOversize, DropMalformed, DropNoSubscriber} {
		m.MessageDropped(r)
	}
	assertExposition(t, reg, `
# HELP st_messages_dropped_total Messages discarded before delivery, by reason.
# TYPE st_messages_dropped_total counter
st_messages_dropped_total{reason="malformed"} 1
st_messages_dropped_total{reason="no_subscriber"} 1
st_messages_dropped_total{reason="oversize"} 1
`, "st_messages_dropped_total")
}

// TestWebhook covers the two webhook families and the status label, which is what
// docs/10-operations.md §5 says a 401 rate is read from.
func TestWebhook(t *testing.T) {
	reg, m := newTestMetrics(t)

	m.WebhookRequest(StatusOf(200), 40*time.Millisecond)
	m.WebhookRequest(StatusOf(200), 60*time.Millisecond)
	m.WebhookRequest(StatusOf(401), 10*time.Millisecond)
	m.WebhookRequest(StatusTimeout, 3*time.Second)
	m.SetWebhookInflight(32)

	assertExposition(t, reg, `
# HELP st_webhook_requests_total Connect-webhook calls by response status.
# TYPE st_webhook_requests_total counter
st_webhook_requests_total{app="main",status="200"} 2
st_webhook_requests_total{app="main",status="401"} 1
st_webhook_requests_total{app="main",status="timeout"} 1
# HELP st_webhook_inflight Connect-webhook calls in flight. Sitting at app.webhook_concurrency means a reconnect storm is in progress.
# TYPE st_webhook_inflight gauge
st_webhook_inflight{app="main"} 32
`, "st_webhook_requests_total", "st_webhook_inflight")

	if got := histogram(t, reg, "st_webhook_duration_seconds"); got.count != 4 {
		t.Errorf("st_webhook_duration_seconds count = %d, want 4", got.count)
	}
}

// TestStatusOf maps HTTP statuses to the label value, and names the two non-HTTP
// outcomes so a timeout does not become a bare "0".
func TestStatusOf(t *testing.T) {
	tests := []struct {
		name string
		code int
		want Status
	}{
		{"ok", 200, "200"},
		{"unauthorized", 401, "401"},
		{"server error", 503, "503"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StatusOf(tt.code); got != tt.want {
				t.Errorf("StatusOf(%d) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
	if StatusTimeout == StatusError {
		t.Error("timeout and transport error must be distinguishable")
	}
}

// TestObserveBus advances the cumulative bus counters by their delta and sets the
// gauges, so a snapshot the bus owns can drive a Prometheus counter without the bus
// knowing anything about Prometheus.
func TestObserveBus(t *testing.T) {
	reg, m := newTestMetrics(t)

	m.ObserveBus(bus.Health{Subscriptions: 10, IntakeDepth: 3, Reconnects: 2, Dropped: 5})
	m.ObserveBus(bus.Health{Subscriptions: 12, IntakeDepth: 0, Reconnects: 4, Dropped: 9})

	assertExposition(t, reg, `
# HELP st_bus_subscriptions_current Channels currently subscribed upstream. Should track distinct active channels.
# TYPE st_bus_subscriptions_current gauge
st_bus_subscriptions_current 12
# HELP st_bus_intake_depth Messages queued between the bus reader and the dispatch workers. Sustained non-zero means the workers are behind.
# TYPE st_bus_intake_depth gauge
st_bus_intake_depth 0
# HELP st_bus_reconnects_total Bus transports established after the first. Climbing against a healthy Redis is pub/sub output-buffer eviction.
# TYPE st_bus_reconnects_total counter
st_bus_reconnects_total 4
# HELP st_messages_dropped_total Messages discarded before delivery, by reason.
# TYPE st_messages_dropped_total counter
st_messages_dropped_total{reason="intake"} 9
`, "st_bus_subscriptions_current", "st_bus_intake_depth", "st_bus_reconnects_total", "st_messages_dropped_total")
}

// TestObserveBus_CounterReset treats a snapshot that went backwards as a new bus rather
// than subtracting: a counter that goes down is a Prometheus reset, and adding a negative
// delta panics.
func TestObserveBus_CounterReset(t *testing.T) {
	reg, m := newTestMetrics(t)

	m.ObserveBus(bus.Health{Reconnects: 7, Dropped: 7})
	m.ObserveBus(bus.Health{Reconnects: 1, Dropped: 2})

	assertExposition(t, reg, `
# HELP st_bus_reconnects_total Bus transports established after the first. Climbing against a healthy Redis is pub/sub output-buffer eviction.
# TYPE st_bus_reconnects_total counter
st_bus_reconnects_total 8
`, "st_bus_reconnects_total")
	if got := testutil.ToFloat64(m.messagesDropped.WithLabelValues(string(DropIntake))); got != 9 {
		t.Errorf("intake drops after reset = %v, want 9", got)
	}
}

// TestRemainingCounters covers the four unlabelled or singly-labelled counters that no
// other test touches.
func TestRemainingCounters(t *testing.T) {
	reg, m := newTestMetrics(t)

	m.BusSyncFailed()
	m.BusSyncFailed()
	m.SlowConsumerDisconnect()
	m.SubscribeDenied(m.Namespace("room-4410"))
	m.OriginRejected()
	m.ControlRejected(ControlUnsigned)
	m.ControlRejected(ControlStale)
	m.ControlRejected(ControlMalformed)

	assertExposition(t, reg, `
# HELP st_bus_sync_failures_total Failed bus subscription reconciliations. Channels may be locally held but not subscribed.
# TYPE st_bus_sync_failures_total counter
st_bus_sync_failures_total 2
# HELP st_slow_consumer_disconnects_total Connections closed for failing to keep up with their outbound queue.
# TYPE st_slow_consumer_disconnects_total counter
st_slow_consumer_disconnects_total{app="main"} 1
# HELP st_subscribe_denied_total Subscribes refused, by namespace. A spike is a client bug or someone probing.
# TYPE st_subscribe_denied_total counter
st_subscribe_denied_total{app="main",namespace="room"} 1
# HELP st_origin_rejected_total Handshakes refused because the Origin was not in server.allowed_origins.
# TYPE st_origin_rejected_total counter
st_origin_rejected_total 1
# HELP st_control_rejected_total Control messages refused, by reason. Unsigned or stale.
# TYPE st_control_rejected_total counter
st_control_rejected_total{reason="malformed"} 1
st_control_rejected_total{reason="stale"} 1
st_control_rejected_total{reason="unsigned"} 1
`, "st_bus_sync_failures_total", "st_slow_consumer_disconnects_total", "st_subscribe_denied_total",
		"st_origin_rejected_total", "st_control_rejected_total")
}

// histogramSnapshot is the count and sum of a histogram family's single series.
type histogramSnapshot struct {
	count uint64
	sum   float64
}

// histogram reads one histogram family out of a gatherer.
func histogram(t *testing.T, g prometheus.Gatherer, name string) histogramSnapshot {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		var got histogramSnapshot
		for _, series := range f.GetMetric() {
			got.count += series.GetHistogram().GetSampleCount()
			got.sum += series.GetHistogram().GetSampleSum()
		}
		return got
	}
	t.Fatalf("%s not gathered", name)
	return histogramSnapshot{}
}

// newTestMetricsWith builds a Metrics over its own registry with explicit options.
func newTestMetricsWith(t *testing.T, opts Options) (*prometheus.Registry, *Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := New(reg, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return reg, m
}
