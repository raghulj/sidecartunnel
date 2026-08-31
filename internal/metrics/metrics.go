package metrics

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// Metric names, exactly as docs/10-operations.md §5 spells them. They are constants so
// that a rename is a compile-time edit in one place, and so the registration error names
// the metric that collided. An operator's alerts are written against this table; renaming
// one of these silently breaks an alert that will not fire again until someone notices.
const (
	nameConnectionsCurrent      = "st_connections_current"
	nameConnectionsTotal        = "st_connections_total"
	nameConnectionDuration      = "st_connection_duration_seconds"
	nameSubscriptionsCurrent    = "st_subscriptions_current"
	nameMessagesPublished       = "st_messages_published_total"
	nameMessagesDelivered       = "st_messages_delivered_total"
	nameMessagesDropped         = "st_messages_dropped_total"
	nameWebhookDuration         = "st_webhook_duration_seconds"
	nameWebhookInflight         = "st_webhook_inflight"
	nameWebhookRequests         = "st_webhook_requests_total"
	nameBusSubscriptions        = "st_bus_subscriptions_current"
	nameBusReconnects           = "st_bus_reconnects_total"
	nameBusIntakeDepth          = "st_bus_intake_depth"
	nameBusSyncFailures         = "st_bus_sync_failures_total"
	nameSlowConsumerDisconnects = "st_slow_consumer_disconnects_total"
	nameSubscribeDenied         = "st_subscribe_denied_total"
	nameOriginRejected          = "st_origin_rejected_total"
	nameControlRejected         = "st_control_rejected_total"
)

// Label names used by more than one family.
const (
	labelApp       = "app"
	labelNamespace = "namespace"
	labelResult    = "result"
	labelReason    = "reason"
	labelStatus    = "status"
)

// Defaults applied to a zero-valued Options, mirroring docs/08-config.md §3.
const (
	defaultApp       = "app"
	defaultSeparator = "-"
)

// Options configure a Metrics. The zero value is usable and applies the documented
// configuration defaults.
type Options struct {
	// App is app.name. docs/08-config.md §3 says it is used in metric labels, and
	// docs/10-operations.md §5 puts it on eleven of the eighteen families. It is a
	// constant label rather than a per-call argument: there is one application per
	// process (S1), so making every call site pass it would only create a way to get it
	// wrong. Default "app".
	App string

	// Separator is channels.separator, the character whose first occurrence splits a
	// channel's namespace from the rest. Default "-".
	Separator string

	// Namespaces are the configured namespace names — the Name of each block in
	// docs/08-config.md §3's namespaces list, including the reserved empty name if it is
	// configured.
	//
	// It is the allowlist for the "namespace" label. A channel whose namespace is not in
	// it folds into otherNamespace instead of minting a series, because a subscribe to an
	// unconfigured namespace is refused (FR-11) and the refusal is counted with a
	// client-chosen string (docs/06-channels.md §2).
	Namespaces []string
}

// Metrics is the set of Prometheus collectors in docs/10-operations.md §5.
//
// Every collector is unexported and reached through a method. That is deliberate: an
// exported *prometheus.CounterVec would let any caller write
// WithLabelValues("room-4410") and put a channel name in a label, which is the
// cardinality failure docs/06-channels.md §2 describes. The methods take defined label
// types, so the compiler refuses the mistake.
//
// A Metrics is constructed against a registry the caller owns. There is no package-level
// instance and no promauto anywhere in this package: promauto's default registerer is a
// global, and a global makes two tests in one process collide
// (docs/14-coding-standards.md §7).
//
// Every method is safe to call concurrently.
type Metrics struct {
	separator  string
	namespaces map[string]struct{}

	connectionsCurrent prometheus.Gauge
	connectionsTotal   *prometheus.CounterVec
	connectionDuration prometheus.Histogram

	subscriptionsCurrent *prometheus.GaugeVec
	messagesPublished    *prometheus.CounterVec
	messagesDelivered    *prometheus.CounterVec
	messagesDropped      *prometheus.CounterVec

	webhookDuration *prometheus.HistogramVec
	webhookInflight prometheus.Gauge
	webhookRequests *prometheus.CounterVec

	busSubscriptions prometheus.Gauge
	busReconnects    prometheus.Counter
	busIntakeDepth   prometheus.Gauge
	busSyncFailures  prometheus.Counter

	slowConsumerDisconnects prometheus.Counter
	subscribeDenied         *prometheus.CounterVec
	originRejected          prometheus.Counter
	controlRejected         *prometheus.CounterVec

	// mu guards the last-seen bus counters. ObserveBus is called from a single observer
	// in practice, but the lock costs nothing on a path that runs once per scrape and
	// removes the need to reason about who calls it.
	mu             sync.Mutex
	lastReconnects uint64
	lastDropped    uint64
}

// New builds every collector in docs/10-operations.md §5 and registers it with reg.
//
// reg is the caller's registry, never prometheus.DefaultRegisterer. It returns an error
// rather than panicking if registration fails — a duplicate registration is a wiring
// mistake, and a panic in a constructor takes down a process that could have reported the
// problem (docs/14-coding-standards.md §6).
//
// The returned Metrics is safe for concurrent use.
func New(reg prometheus.Registerer, opts Options) (*Metrics, error) {
	app := opts.App
	if app == "" {
		app = defaultApp
	}
	separator := opts.Separator
	if separator == "" {
		separator = defaultSeparator
	}
	known := make(map[string]struct{}, len(opts.Namespaces))
	for _, n := range opts.Namespaces {
		known[n] = struct{}{}
	}

	appLabel := prometheus.Labels{labelApp: app}
	m := &Metrics{separator: separator, namespaces: known}
	var err error

	m.connectionsCurrent = register(reg, &err, nameConnectionsCurrent, prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        nameConnectionsCurrent,
		Help:        "Connections currently open on this replica.",
		ConstLabels: appLabel,
	}))
	m.connectionsTotal = register(reg, &err, nameConnectionsTotal, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        nameConnectionsTotal,
		Help:        "Connection attempts by outcome.",
		ConstLabels: appLabel,
	}, []string{labelResult}))
	m.connectionDuration = register(reg, &err, nameConnectionDuration, prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        nameConnectionDuration,
		Help:        "Connection lifetime in seconds.",
		ConstLabels: appLabel,
		// 1s to 8192s (2h16m). The default buckets top out at 10s, which would put every
		// healthy connection in +Inf and hide the signal docs/10-operations.md §7 reads
		// from this histogram: a median that collapses to about 60s is a proxy idle
		// timeout below server.ping_interval.
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	}))

	m.subscriptionsCurrent = register(reg, &err, nameSubscriptionsCurrent, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        nameSubscriptionsCurrent,
		Help:        "Subscriptions currently held on this replica, by namespace.",
		ConstLabels: appLabel,
	}, []string{labelNamespace}))
	m.messagesPublished = register(reg, &err, nameMessagesPublished, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        nameMessagesPublished,
		Help:        "Messages published through this replica, by namespace.",
		ConstLabels: appLabel,
	}, []string{labelNamespace}))
	m.messagesDelivered = register(reg, &err, nameMessagesDelivered, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        nameMessagesDelivered,
		Help:        "Messages written to a subscriber, by namespace. The ratio to published is the average fan-out.",
		ConstLabels: appLabel,
	}, []string{labelNamespace}))
	// No app label: docs/10-operations.md §5 gives this family "reason" alone, because
	// an intake drop is counted from a bus snapshot that knows nothing about the app.
	m.messagesDropped = register(reg, &err, nameMessagesDropped, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: nameMessagesDropped,
		Help: "Messages discarded before delivery, by reason.",
	}, []string{labelReason}))

	m.webhookDuration = register(reg, &err, nameWebhookDuration, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        nameWebhookDuration,
		Help:        "Connect-webhook latency in seconds, by response status.",
		ConstLabels: appLabel,
		// 5ms to 10.24s. This is the L in docs/10-operations.md §4's reconnect model —
		// N/J x L/1000 concurrent requests at the application — so the interesting
		// resolution is tens of milliseconds, and the ceiling is above app.webhook_timeout.
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{labelStatus}))
	m.webhookInflight = register(reg, &err, nameWebhookInflight, prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        nameWebhookInflight,
		Help:        "Connect-webhook calls in flight. Sitting at app.webhook_concurrency means a reconnect storm is in progress.",
		ConstLabels: appLabel,
	}))
	m.webhookRequests = register(reg, &err, nameWebhookRequests, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        nameWebhookRequests,
		Help:        "Connect-webhook calls by response status.",
		ConstLabels: appLabel,
	}, []string{labelStatus}))

	m.busSubscriptions = register(reg, &err, nameBusSubscriptions, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: nameBusSubscriptions,
		Help: "Channels currently subscribed upstream. Should track distinct active channels.",
	}))
	m.busReconnects = register(reg, &err, nameBusReconnects, prometheus.NewCounter(prometheus.CounterOpts{
		Name: nameBusReconnects,
		Help: "Bus transports established after the first. Climbing against a healthy Redis is pub/sub output-buffer eviction.",
	}))
	m.busIntakeDepth = register(reg, &err, nameBusIntakeDepth, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: nameBusIntakeDepth,
		Help: "Messages queued between the bus reader and the dispatch workers. Sustained non-zero means the workers are behind.",
	}))
	m.busSyncFailures = register(reg, &err, nameBusSyncFailures, prometheus.NewCounter(prometheus.CounterOpts{
		Name: nameBusSyncFailures,
		Help: "Failed bus subscription reconciliations. Channels may be locally held but not subscribed.",
	}))

	m.slowConsumerDisconnects = register(reg, &err, nameSlowConsumerDisconnects, prometheus.NewCounter(prometheus.CounterOpts{
		Name:        nameSlowConsumerDisconnects,
		Help:        "Connections closed for failing to keep up with their outbound queue.",
		ConstLabels: appLabel,
	}))
	m.subscribeDenied = register(reg, &err, nameSubscribeDenied, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        nameSubscribeDenied,
		Help:        "Subscribes refused, by namespace. A spike is a client bug or someone probing.",
		ConstLabels: appLabel,
	}, []string{labelNamespace}))
	m.originRejected = register(reg, &err, nameOriginRejected, prometheus.NewCounter(prometheus.CounterOpts{
		Name: nameOriginRejected,
		Help: "Handshakes refused because the Origin was not in server.allowed_origins.",
	}))
	m.controlRejected = register(reg, &err, nameControlRejected, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: nameControlRejected,
		Help: "Control messages refused, by reason. Unsigned or stale.",
	}, []string{labelReason}))

	if err != nil {
		return nil, err
	}
	return m, nil
}

// register registers c with reg and records the first failure in errp, naming the metric.
//
// The name is threaded through because prometheus.AlreadyRegisteredError says only
// "duplicate metrics collector registration attempted" — an error that tells an operator
// nothing about which of eighteen families collided.
func register[C prometheus.Collector](reg prometheus.Registerer, errp *error, name string, c C) C {
	if *errp == nil {
		if err := reg.Register(c); err != nil {
			*errp = fmt.Errorf("metrics: registering %s: %w", name, err)
		}
	}
	return c
}

// Namespace resolves a channel name to its namespace label value.
//
// It is the only way to obtain a Namespace, and it never returns the channel it was
// given: the separator splits the namespace off at its first occurrence
// (docs/06-channels.md §1), an unmatched namespace falls back to the reserved empty block
// exactly as the hub's own resolution does, and a namespace with no block and no reserved
// block folds into a single bucket. That last case is what keeps a client-chosen channel
// name out of the label space, on the one family a client can drive: a subscribe to an
// unconfigured namespace is refused (FR-11) and counted in st_subscribe_denied_total
// (docs/06-channels.md §2).
//
// It is safe to call concurrently: the configured set is written once, in New.
func (m *Metrics) Namespace(channel string) Namespace {
	name := channel
	if i := strings.Index(channel, m.separator); i >= 0 {
		name = channel[:i]
	}
	if _, ok := m.namespaces[name]; ok {
		return Namespace{label: name}
	}
	// The same fallback the hub applies when it resolves a channel to its configuration
	// block: an unmatched namespace is governed by the reserved empty block when one is
	// configured. The label has to say which block governs the channel, or a dashboard
	// and the config disagree about the same traffic (docs/06-channels.md §3).
	if _, ok := m.namespaces[""]; ok {
		return Namespace{label: ""}
	}
	return Namespace{label: otherNamespace}
}

// ConnectionOpened records a connection entering the connected state:
// st_connections_current.
func (m *Metrics) ConnectionOpened() { m.connectionsCurrent.Inc() }

// ConnectionClosed records a connection ending, with the lifetime it had:
// st_connections_current and st_connection_duration_seconds.
func (m *Metrics) ConnectionClosed(lifetime time.Duration) {
	m.connectionsCurrent.Dec()
	m.connectionDuration.Observe(lifetime.Seconds())
}

// ConnectionResult records the outcome of one handshake: st_connections_total.
//
// It is separate from ConnectionOpened because most of the outcomes never open a
// connection at all — an Origin rejection is refused before the upgrade completes (FR-2).
func (m *Metrics) ConnectionResult(r Result) {
	m.connectionsTotal.WithLabelValues(string(r)).Inc()
}

// SubscriptionsAdd moves st_subscriptions_current by delta, which may be negative. A
// disconnect drops every subscription a connection held, so the caller passes the count
// rather than looping.
func (m *Metrics) SubscriptionsAdd(ns Namespace, delta int) {
	m.subscriptionsCurrent.WithLabelValues(ns.label).Add(float64(delta))
}

// MessagePublished records one message accepted for fan-out: st_messages_published_total.
func (m *Metrics) MessagePublished(ns Namespace) {
	m.messagesPublished.WithLabelValues(ns.label).Inc()
}

// MessagesDelivered records n writes to subscribers for one message:
// st_messages_delivered_total. The ratio to published is the average fan-out
// (docs/10-operations.md §5).
func (m *Metrics) MessagesDelivered(ns Namespace, n int) {
	m.messagesDelivered.WithLabelValues(ns.label).Add(float64(n))
}

// MessageDropped records one message discarded before delivery:
// st_messages_dropped_total.
func (m *Metrics) MessageDropped(reason DropReason) {
	m.messagesDropped.WithLabelValues(string(reason)).Inc()
}

// WebhookRequest records one completed connect-webhook call, in both
// st_webhook_requests_total and st_webhook_duration_seconds.
//
// They are recorded together because they share the status label and are read together:
// the 401 rate against the latency distribution is how docs/10-operations.md §7's "401s
// after a deploy" entry is diagnosed.
func (m *Metrics) WebhookRequest(status Status, d time.Duration) {
	m.webhookRequests.WithLabelValues(string(status)).Inc()
	m.webhookDuration.WithLabelValues(string(status)).Observe(d.Seconds())
}

// SetWebhookInflight sets st_webhook_inflight to the number of calls in flight. It is a
// set rather than an increment because the webhook client already owns the count, and two
// sources of truth for one gauge drift.
func (m *Metrics) SetWebhookInflight(n int) { m.webhookInflight.Set(float64(n)) }

// ObserveBus folds a bus health snapshot into the four bus families:
// st_bus_subscriptions_current, st_bus_intake_depth, st_bus_reconnects_total and
// st_messages_dropped_total{reason="intake"}.
//
// The bus owns cumulative counters and reports them as totals, so the counters here are
// advanced by the delta since the last snapshot. A snapshot that went backwards is a new
// bus rather than negative progress: Counter.Add panics on a negative value, and a panic
// in a metrics observer would take the process down over a number nobody is paging on.
//
// The caller decides when to call it. The admin listener's Refresh hook is the intended
// place, which makes the gauges exact at scrape time and needs no ticker.
//
// It is safe to call concurrently.
func (m *Metrics) ObserveBus(h bus.Health) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.busSubscriptions.Set(float64(h.Subscriptions))
	m.busIntakeDepth.Set(float64(h.IntakeDepth))
	m.busReconnects.Add(advance(&m.lastReconnects, h.Reconnects))
	m.messagesDropped.WithLabelValues(string(DropIntake)).Add(advance(&m.lastDropped, h.Dropped))
}

// advance returns the increase from *last to now and stores now. A value below *last is
// treated as a counter reset and contributes the whole new value.
func advance(last *uint64, now uint64) float64 {
	if now < *last {
		*last = 0
	}
	delta := now - *last
	*last = now
	return float64(delta)
}

// BusSyncFailed records one failed Bus.Sync: st_bus_sync_failures_total.
//
// It is incremented by the reconciler rather than read from a snapshot, because a failed
// Sync is the one bus event that leaves a channel locally held and upstream dead, and
// docs/13-review-findings.md M5 is what happens when it is not counted anywhere.
func (m *Metrics) BusSyncFailed() { m.busSyncFailures.Inc() }

// SlowConsumerDisconnect records one connection closed for overrunning its outbound
// queue: st_slow_consumer_disconnects_total.
func (m *Metrics) SlowConsumerDisconnect() { m.slowConsumerDisconnects.Inc() }

// SubscribeDenied records one refused subscribe: st_subscribe_denied_total.
func (m *Metrics) SubscribeDenied(ns Namespace) {
	m.subscribeDenied.WithLabelValues(ns.label).Inc()
}

// OriginRejected records one handshake refused by the Origin allowlist:
// st_origin_rejected_total.
func (m *Metrics) OriginRejected() { m.originRejected.Inc() }

// ControlRejected records one control message refused: st_control_rejected_total.
func (m *Metrics) ControlRejected(reason ControlReason) {
	m.controlRejected.WithLabelValues(string(reason)).Inc()
}
