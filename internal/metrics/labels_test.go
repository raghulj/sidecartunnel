package metrics

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestNamespace_Resolution covers docs/06-channels.md §1: the separator splits the
// namespace from the rest at its first occurrence, a channel with no separator resolves
// to the reserved empty namespace, and a namespace with no configured block is folded
// into one bucket rather than becoming a label value of its own.
func TestNamespace_Resolution(t *testing.T) {
	_, configured := newTestMetrics(t)
	_, noDefault := newTestMetricsWith(t, Options{App: "main", Separator: "-", Namespaces: []string{"room", "user"}})

	tests := []struct {
		name    string
		m       *Metrics
		channel string
		want    string
	}{
		{"first separator splits", configured, "room-4410", "room"},
		{"only the first separator", configured, "user-7-private", "user"},
		{"no separator is the reserved empty namespace", configured, "standalone", ""},
		{"trailing separator", configured, "room-", "room"},
		{"empty channel", configured, "", ""},
		{"leading separator", configured, "-4410", ""},
		// An unconfigured namespace is governed by the reserved empty block when one
		// exists, and the label says so — the same resolution the hub performs
		// (docs/06-channels.md §3).
		{"unmatched falls back to the reserved block", configured, "probe-1", ""},
		// With no reserved block the subscribe is refused (FR-11) and counted in
		// st_subscribe_denied_total, where the channel name is client-chosen. It folds
		// rather than minting a series (docs/06-channels.md §2).
		{"unmatched with no reserved block folds", noDefault, "probe-1", otherNamespace},
		{"separator-less with no reserved block folds", noDefault, "standalone", otherNamespace},
		{"channel that is all separator", noDefault, "-", otherNamespace},
		{"configured namespace still resolves", noDefault, "room-4410", "room"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.Namespace(tt.channel).String(); got != tt.want {
				t.Errorf("Namespace(%q) = %q, want %q", tt.channel, got, tt.want)
			}
		})
	}
}

// TestNamespace_MultiByteSeparator uses a separator other than "-", because
// channels.separator is configurable (docs/08-config.md §3).
func TestNamespace_MultiByteSeparator(t *testing.T) {
	reg, m := newTestMetricsWith(t, Options{App: "main", Separator: ":", Namespaces: []string{"room"}})
	m.SubscribeDenied(m.Namespace("room:4410"))
	assertExposition(t, reg, `
# HELP st_subscribe_denied_total Subscribes refused, by namespace. A spike is a client bug or someone probing.
# TYPE st_subscribe_denied_total counter
st_subscribe_denied_total{app="main",namespace="room"} 1
`, "st_subscribe_denied_total")
}

// TestNamespace_CardinalityIsBounded is the requirement docs/06-channels.md §2 states in
// prose: a namespace with one channel per user and 200,000 users would destroy
// Prometheus, so channel names never become label values. Whatever the channel names
// look like, the series count is bounded by the configured namespaces plus the fold
// bucket.
func TestNamespace_CardinalityIsBounded(t *testing.T) {
	_, m := newTestMetrics(t)

	for i := range 5000 {
		m.SubscribeDenied(m.Namespace(fmt.Sprintf("room-%d", i)))
		m.SubscribeDenied(m.Namespace(fmt.Sprintf("user-%d", i)))
		m.SubscribeDenied(m.Namespace(fmt.Sprintf("probe%d-%d", i, i)))
		m.SubscriptionsAdd(m.Namespace(fmt.Sprintf("room-%d", i)), 1)
	}

	// room, user and the reserved empty namespace — and nothing else, out of 15,000
	// distinct channel names.
	if got := testutil.CollectAndCount(m.subscribeDenied, "st_subscribe_denied_total"); got != 3 {
		t.Errorf("st_subscribe_denied_total series = %d, want 3", got)
	}
	if got := testutil.CollectAndCount(m.subscriptionsCurrent, "st_subscriptions_current"); got != 1 {
		t.Errorf("st_subscriptions_current series = %d, want 1", got)
	}

	// And with no reserved block, where every unmatched namespace folds instead.
	_, folding := newTestMetricsWith(t, Options{App: "main", Separator: "-", Namespaces: []string{"room"}})
	for i := range 5000 {
		folding.SubscribeDenied(folding.Namespace(fmt.Sprintf("probe%d-%d", i, i)))
	}
	if got := testutil.CollectAndCount(folding.subscribeDenied, "st_subscribe_denied_total"); got != 1 {
		t.Errorf("folded series = %d, want 1", got)
	}
}

// TestNamespace_IsOpaque asserts the API shape that makes the cardinality rule
// unbreakable rather than merely documented: no package outside this one can build a
// Namespace, and no exported recording method takes a bare string a channel name could
// be passed as. The only door in is Metrics.Namespace, which resolves rather than
// accepts.
func TestNamespace_IsOpaque(t *testing.T) {
	nsType := reflect.TypeOf(Namespace{})
	if nsType.Kind() != reflect.Struct {
		t.Fatalf("Namespace kind = %v, want a struct; a defined string type is convertible from any channel name", nsType.Kind())
	}
	for i := range nsType.NumField() {
		if f := nsType.Field(i); f.IsExported() {
			t.Errorf("Namespace.%s is exported: a composite literal outside this package could then carry a channel name", f.Name)
		}
	}

	// Every exported recording method takes defined label types, never a bare string.
	// Metrics.Namespace is the single exception: taking a channel name is its job.
	mt := reflect.TypeOf(&Metrics{})
	for i := range mt.NumMethod() {
		meth := mt.Method(i)
		if meth.Name == "Namespace" {
			continue
		}
		for j := 1; j < meth.Type.NumIn(); j++ {
			if in := meth.Type.In(j); in == reflect.TypeOf("") {
				t.Errorf("(*Metrics).%s takes a bare string at argument %d; a raw channel name could be passed where a label is expected", meth.Name, j)
			}
		}
	}
}

// TestNamespace_ZeroValue is what a caller gets if it forgets to resolve: the reserved
// empty namespace, which is a legal label value and not a channel name.
func TestNamespace_ZeroValue(t *testing.T) {
	var ns Namespace
	if ns.String() != "" {
		t.Errorf("zero Namespace = %q, want the reserved empty namespace", ns)
	}
}

// TestLabelConstants keeps the documented label values in one place and asserts they are
// the strings docs/10-operations.md §5 prints.
func TestLabelConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"accepted", string(ResultAccepted), "accepted"},
		{"origin rejected", string(ResultOriginRejected), "origin_rejected"},
		{"unauthorized", string(ResultUnauthorized), "unauthorized"},
		{"unavailable", string(ResultUnavailable), "unavailable"},
		{"over limit", string(ResultOverLimit), "over_limit"},
		{"oversize", string(DropOversize), "oversize"},
		{"malformed", string(DropMalformed), "malformed"},
		{"no subscriber", string(DropNoSubscriber), "no_subscriber"},
		{"intake", string(DropIntake), "intake"},
		{"unsigned", string(ControlUnsigned), "unsigned"},
		{"stale", string(ControlStale), "stale"},
		{"control malformed", string(ControlMalformed), "malformed"},
		{"timeout", string(StatusTimeout), "timeout"},
		{"transport error", string(StatusError), "error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("label value = %q, want %q", tt.got, tt.want)
			}
		})
	}
	// The fold bucket can never collide with a configured namespace, because a channel
	// beginning "_" is reserved and refused before it reaches a metric
	// (docs/06-channels.md §4).
	if !strings.HasPrefix(otherNamespace, "_") {
		t.Errorf("otherNamespace = %q; it must begin with the reserved prefix so it cannot collide with a configured namespace", otherNamespace)
	}
}
