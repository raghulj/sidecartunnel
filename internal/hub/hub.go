package hub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
)

// Sentinel errors returned by the registry. They exist as sentinels so internal/conn can
// map each one to the protocol error code docs/03-client-protocol.md §6 assigns it,
// with errors.Is rather than by comparing strings.
var (
	// ErrReservedChannel is a subscribe to a channel beginning "_". Reserved for control
	// channels and refused before grants are consulted, so a grant of "*" still cannot
	// reach one (docs/06-channels.md §4). Maps to proto.ErrPermissionDenied.
	ErrReservedChannel = errors.New("hub: channel is reserved")

	// ErrUnknownNamespace is a channel whose namespace has no configured block and for
	// which no reserved empty-name block exists. Failing closed is deliberate: a typo'd
	// namespace must be an error, not a silently permissive channel (FR-11). Maps to
	// proto.ErrUnknownNamespace.
	ErrUnknownNamespace = errors.New("hub: channel namespace is not configured")

	// ErrAlreadySubscribed is a subscribe to a channel this connection already holds.
	// Deliberately not idempotent: a duplicate subscribe means the client's own registry
	// has drifted, and hiding that makes reconnect bugs very hard to find. Maps to
	// proto.ErrAlreadySubscribed.
	ErrAlreadySubscribed = errors.New("hub: already subscribed")

	// ErrNotSubscribed is an unsubscribe from a channel this connection does not hold.
	// Maps to proto.ErrNotSubscribed.
	ErrNotSubscribed = errors.New("hub: not subscribed")

	// ErrSubscriptionLimit is a subscribe past Options.MaxSubscriptionsPerConn. The cap
	// is enforced under the hub lock, in the same critical section as the insert: a
	// check outside it is a time-of-check race that two concurrent subscribes both pass.
	// Maps to proto.ErrSubscriptionLimit.
	ErrSubscriptionLimit = errors.New("hub: subscription limit reached")

	// ErrNotRegistered is any registry call for a connection that was never added or has
	// already been removed.
	//
	// M4: it is what stops a reader goroutine's in-flight subscribe from resurrecting a
	// connection that close has just deregistered. A resurrected connection is resident
	// in the channel map forever, so fan-out writes to a dead connection and the
	// refcount never reaches zero.
	ErrNotRegistered = errors.New("hub: connection is not registered")

	// ErrDuplicateID is an Add for a client id already held by a different connection.
	// Ids are 16 hex characters from crypto/rand, so this cannot happen by accident;
	// overwriting the index entry silently would leave a live connection unreachable by
	// a control disconnect (FR-18), so it is refused instead.
	ErrDuplicateID = errors.New("hub: client id already registered")
)

// Defaults applied by New to a zero-valued Options. Each mirrors the documented
// configuration default in docs/08-config.md §3.
const (
	defaultPrefix    = "st:"
	defaultSeparator = "-"

	// defaultCloserQueue is the depth of the queue between fan-out and the closer
	// goroutine. It is generous because the send into it is non-blocking and the
	// overflow path spawns a goroutine per connection (docs/09-internals.md §4.3).
	defaultCloserQueue = 1024

	// defaultRetryMin and defaultRetryMax bound the reconciler's backoff after a failed
	// Sync, mirroring bus.reconnect_min and bus.reconnect_max.
	defaultRetryMin = 200 * time.Millisecond
	defaultRetryMax = 10 * time.Second

	// controlChannel is the reserved channel every replica consumes
	// (docs/04-integration.md §3). Clients can never subscribe to it: it begins with the
	// reserved "_".
	controlChannel = "_control"

	// reservedPrefix marks a channel clients may never subscribe to.
	reservedPrefix = "_"
)

// Options configure a Hub. The zero value is usable: New applies the documented default
// for every field, so a test or a single-node development binary needs none of them.
type Options struct {
	// Prefix is bus.prefix. It is the difference between the channel a client names and
	// the key the hub and the bus use (FR-21). Default "st:".
	Prefix string

	// Separator is channels.separator, the character whose first occurrence splits a
	// channel's namespace from the rest. Default "-".
	Separator string

	// Namespaces are the configured namespace blocks. An empty list installs the
	// built-in reserved empty-name block, which is what makes the environment-only
	// deployment usable rather than one that refuses every subscribe
	// (docs/13-review-findings.md M11).
	Namespaces []config.Namespace

	// MaxSubscriptionsPerConn caps one connection's subscriptions; 0 is unlimited.
	MaxSubscriptionsPerConn int

	// CloserQueue is the depth of the fan-out-to-closer queue. Default 1024.
	CloserQueue int

	// RetryMin and RetryMax bound the reconciler's backoff after a failed Sync.
	// Defaults 200ms and 10s.
	RetryMin, RetryMax time.Duration

	// After replaces time.After in the reconciler's backoff. It exists so a test can
	// assert a retry schedule exactly instead of sleeping through it
	// (docs/14-coding-standards.md §2). Default time.After.
	After func(time.Duration) <-chan time.Time
}

// Hub is the channel registry and the local fan-out path.
//
// It is safe for concurrent use by any number of goroutines and is built to be shared by
// every connection on the replica. Every exported method is non-blocking with respect to
// the bus: nothing on a request path or on the fan-out path ever waits for Redis.
//
// One sync.RWMutex guards the whole registry, not 32 shards. Fanning out to 10,000
// connections is ~10,000 map iterations — roughly 0.2 ms under the read lock — against
// subscribes that are rare by comparison, so sharding buys nothing measurable and costs a
// shard-index concept threaded through every call site. NFR-9 forbids building it until a
// profile shows contention above 5% of fan-out time.
//
// Sink implementations must be comparable, because Sink values are map keys here. In
// practice that means a pointer; an interface holding a struct with a slice field panics
// the moment it is inserted.
type Hub struct {
	bus bus.Bus

	prefix     string
	separator  string
	namespaces map[string]config.Namespace
	maxSubs    int
	controlKey string
	retryMin   time.Duration
	retryMax   time.Duration
	after      func(time.Duration) <-chan time.Time

	// mu guards every map below, and it is always taken before any connection's own
	// lock — never the reverse (M3, docs/09-internals.md §4.4). Two paths acquiring them
	// in opposite orders is a deadlock -race does not detect and that only appears under
	// contention.
	mu sync.RWMutex

	// channels maps a bus key to the connections holding it. Keyed by {prefix}{channel},
	// never the bare name, which is what makes cross-delivery structurally impossible
	// rather than a filter someone has to remember to apply (FR-21).
	channels map[string]map[Sink]struct{}

	// subs is the per-connection subscription mirror, keyed by bus key. It is the
	// connection's own set, held here because the hub cannot reach into internal/conn
	// and because M4 requires the two views to move in one critical section: any path
	// that touches one without the other leaves a connection resident in the map after
	// close, so fan-out writes to a dead connection forever and the refcount never
	// reaches zero.
	subs map[Sink]map[string]struct{}

	// users indexes connections by the opaque user id, for control targeting (FR-18). It
	// is a second map under the same lock, never a second lock: a separate one is a
	// contention point on every connect and disconnect (docs/09-internals.md §9).
	users map[string]map[Sink]struct{}

	// clients indexes connections by client id, for the other control target.
	clients map[string]Sink

	// desired is the reconciler's target state: the set of bus keys this replica wants
	// subscribed. It moves under mu, in the same critical section as channels.
	//
	// docs/09-internals.md §4.1 sketches it as a sync.Map written after the lock is
	// released. That reorders against a concurrent unsubscribe of the same channel — the
	// two writes land in the opposite order to the two map mutations — and leaves the
	// desired set disagreeing with the map in whichever direction lost. Only markDirty,
	// which cannot block, happens outside the lock.
	desired map[string]struct{}

	dirty atomic.Bool

	// wake carries one token to the reconciler. Capacity 1 with a non-blocking send is
	// the whole of C7: no producer — least of all the fan-out goroutine — can ever stall
	// scheduling bus work.
	wake chan struct{}

	// closeq hands slow connections to the closer goroutine. Closing never happens on
	// the fan-out goroutine: Close needs the write lock fan-out is holding for read, so
	// closing inline deadlocks (docs/09-internals.md §4.3, §4.5).
	closeq chan Sink

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New builds a Hub and starts its two background goroutines: the reconciler that drives
// bus.Sync, and the closer that ends slow connections off the fan-out path.
//
// ctx bounds their lifetime; Close cancels a derived context and waits for both, so a
// hub that has returned from Close has leaked nothing (NFR-3).
//
// The control key is seeded into the desired set and never leaves it. Sync makes the bus
// subscription set exactly the desired set, so a desired set that omitted the control key
// would unsubscribe the replica from control on the first reconciliation and silently
// disable revocation for the life of the process (docs/04-integration.md §3, C8).
func New(ctx context.Context, b bus.Bus, opts Options) *Hub {
	if opts.Prefix == "" {
		opts.Prefix = defaultPrefix
	}
	if opts.Separator == "" {
		opts.Separator = defaultSeparator
	}
	if opts.CloserQueue <= 0 {
		opts.CloserQueue = defaultCloserQueue
	}
	if opts.RetryMin <= 0 {
		opts.RetryMin = defaultRetryMin
	}
	if opts.RetryMax < opts.RetryMin {
		opts.RetryMax = defaultRetryMax
	}
	if opts.After == nil {
		opts.After = time.After
	}
	if len(opts.Namespaces) == 0 {
		// M11: without the built-in block a gateway configured only from the environment
		// starts cleanly, reports healthy, and refuses every single subscribe.
		opts.Namespaces = []config.Namespace{{Name: ""}}
	}

	namespaces := make(map[string]config.Namespace, len(opts.Namespaces))
	for _, ns := range opts.Namespaces {
		namespaces[ns.Name] = ns
	}

	hubCtx, cancel := context.WithCancel(ctx)
	h := &Hub{
		bus:        b,
		prefix:     opts.Prefix,
		separator:  opts.Separator,
		namespaces: namespaces,
		maxSubs:    opts.MaxSubscriptionsPerConn,
		controlKey: opts.Prefix + controlChannel,
		retryMin:   opts.RetryMin,
		retryMax:   opts.RetryMax,
		after:      opts.After,
		channels:   make(map[string]map[Sink]struct{}),
		subs:       make(map[Sink]map[string]struct{}),
		users:      make(map[string]map[Sink]struct{}),
		clients:    make(map[string]Sink),
		desired:    map[string]struct{}{opts.Prefix + controlChannel: {}},
		wake:       make(chan struct{}, 1),
		closeq:     make(chan Sink, opts.CloserQueue),
		ctx:        hubCtx,
		cancel:     cancel,
	}

	h.wg.Add(2)
	go h.reconcile()
	go h.closeLoop()
	h.markDirty() // Subscribe to the control channel before anything else happens.
	return h
}

// Close stops the reconciler and the closer and waits for them to exit. It is idempotent
// and returns nothing, because nothing it does can fail: the bus is not the hub's to
// close, and the registry needs no teardown — connections are ended by the drain path
// (docs/09-internals.md §8) before the hub goes away.
//
// It must not be called concurrently with Dispatch: the caller stops its dispatch workers
// first, exactly as it stops accepting upgrades before draining.
func (h *Hub) Close() {
	h.cancel()
	h.wg.Wait()
}

// ControlKey returns the bus key of the reserved control channel, {bus.prefix}_control.
//
// The bus consumer needs it to route control messages to Hub.Control on their own
// goroutine, so that a revocation cannot queue behind the firehose it may exist to stop
// (docs/09-internals.md §5).
func (h *Hub) ControlKey() string { return h.controlKey }

// Key returns the bus key for a bare channel name: {bus.prefix}{channel}.
func (h *Hub) key(channel string) string { return h.prefix + channel }

// channelName returns the bare channel name for a bus key. Pushes carry the bare name;
// the prefix is the gateway's business, not the client's.
func (h *Hub) channelName(key string) string { return strings.TrimPrefix(key, h.prefix) }

// Namespace resolves a channel to its configuration block.
//
// The namespace is the substring before the channel's first separator; a channel with no
// separator resolves to the reserved empty name "" (FR-11, docs/06-channels.md §3). A
// channel whose namespace has no block falls back to that same reserved block when one is
// configured, and is otherwise unresolved — the caller answers proto.ErrUnknownNamespace.
//
// It parses per call rather than caching: a global namespace cache grows without bound
// with attacker-chosen keys, and this is a single index (docs/09-internals.md §9).
func (h *Hub) Namespace(channel string) (config.Namespace, bool) {
	name := channel
	if i := strings.Index(channel, h.separator); i >= 0 {
		name = channel[:i]
	}
	if ns, ok := h.namespaces[name]; ok {
		return ns, true
	}
	if ns, ok := h.namespaces[""]; ok {
		return ns, true
	}
	return config.Namespace{}, false
}

// Add registers a connection, with no subscriptions, so that control targeting can reach
// it before it has subscribed to anything (FR-18).
//
// It is idempotent for the same Sink and returns ErrDuplicateID for a different Sink
// holding a client id already registered. It never blocks.
func (h *Hub) Add(s Sink) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if existing, ok := h.clients[s.ID()]; ok {
		if existing == s {
			return nil
		}
		return fmt.Errorf("hub: add %s: %w", s.ID(), ErrDuplicateID)
	}
	h.subs[s] = make(map[string]struct{})
	h.clients[s.ID()] = s
	set, ok := h.users[s.User()]
	if !ok {
		set = make(map[Sink]struct{})
		h.users[s.User()] = set
	}
	set[s] = struct{}{}
	return nil
}

// Remove deregisters a connection and drops every subscription it held.
//
// It is idempotent, because expiry, revocation, drain and a slow-consumer overflow can
// all decide to remove the same connection at once. It never blocks: the only work
// outside the lock is markDirty.
//
// M4: the channel map, the subscription mirror and both indexes move in one critical
// section. A path that dropped the mirror without the map would leave the connection
// resident in the map forever, and fan-out would write to a dead connection.
func (h *Hub) Remove(s Sink) {
	h.mu.Lock()
	mirror, ok := h.subs[s]
	if !ok {
		h.mu.Unlock()
		return
	}
	changed := false
	for key := range mirror {
		if h.detach(s, key) {
			changed = true
		}
	}
	delete(h.subs, s)
	delete(h.clients, s.ID())
	if set, found := h.users[s.User()]; found {
		delete(set, s)
		if len(set) == 0 {
			delete(h.users, s.User())
		}
	}
	h.mu.Unlock()

	if changed {
		h.markDirty()
	}
}

// Subscribe adds one channel to a connection.
//
// It refuses a reserved channel, an unresolvable namespace, a duplicate, a connection
// past its subscription cap, and a connection that is not registered. It does no
// authorization: grants are the connection's, and a subscription that exists is delivered
// to without being re-checked (docs/05-authorization.md §4).
//
// It never blocks and never waits for the bus. A 0→1 refcount transition adds the bus key
// to the desired set inside the same critical section as the map mutation and then marks
// the reconciler dirty with a non-blocking store (FR-10, C7).
func (h *Hub) Subscribe(s Sink, channel string) error {
	if strings.HasPrefix(channel, reservedPrefix) {
		return fmt.Errorf("hub: subscribe %q: %w", channel, ErrReservedChannel)
	}
	if _, ok := h.Namespace(channel); !ok {
		return fmt.Errorf("hub: subscribe %q: %w", channel, ErrUnknownNamespace)
	}
	first, err := h.insert(s, h.key(channel))
	if err != nil {
		return fmt.Errorf("hub: subscribe %q: %w", channel, err)
	}
	if first {
		h.markDirty()
	}
	return nil
}

// Unsubscribe drops one channel from a connection.
//
// It never blocks. A 1→0 refcount transition removes the bus key from the desired set
// under the lock and marks the reconciler dirty afterwards (FR-10).
func (h *Hub) Unsubscribe(s Sink, channel string) error {
	last, err := h.drop(s, h.key(channel))
	if err != nil {
		return fmt.Errorf("hub: unsubscribe %q: %w", channel, err)
	}
	if last {
		h.markDirty()
	}
	return nil
}

// Subscriptions returns the connection's authoritative subscription set as bare channel
// names, sorted. It answers the client's sync command, which is the only way a client can
// discover that the gateway dropped a subscription it did not ask to drop (M16).
func (h *Hub) Subscriptions(s Sink) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	mirror := h.subs[s]
	out := make([]string, 0, len(mirror))
	for key := range mirror {
		out = append(out, h.channelName(key))
	}
	sort.Strings(out)
	return out
}

// Publish sends an envelope on a channel, prefixing it into a bus key.
//
// It exists so that bus-key construction lives in exactly one place: a caller that builds
// its own key is one refactor away from publishing to the unprefixed name, which reaches
// nobody and fails silently, because Redis PUBLISH reports gateway replicas rather than
// clients (FR-21, docs/04-integration.md §2.2).
func (h *Hub) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := h.bus.Publish(ctx, h.key(channel), payload); err != nil {
		return fmt.Errorf("hub: publish %q: %w", channel, err)
	}
	return nil
}

// insert adds one connection to one bus key and reports whether the channel went 0→1.
//
// FR-10: the transition is decided here, under the lock, as len(set) == 1 immediately
// after the insert. Recomputing it after the lock is released lets a concurrent
// unsubscribe interleave, which either subscribes upstream twice or drops a subscription
// that still has a holder.
func (h *Hub) insert(s Sink, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	mirror, ok := h.subs[s]
	if !ok {
		return false, ErrNotRegistered
	}
	if _, dup := mirror[key]; dup {
		return false, ErrAlreadySubscribed
	}
	if h.maxSubs > 0 && len(mirror) >= h.maxSubs {
		return false, ErrSubscriptionLimit
	}

	set, ok := h.channels[key]
	if !ok {
		set = make(map[Sink]struct{})
		h.channels[key] = set
	}
	set[s] = struct{}{}
	mirror[key] = struct{}{}

	first := len(set) == 1
	if first {
		h.desired[key] = struct{}{}
	}
	return first, nil
}

// drop removes one connection from one bus key and reports whether the channel went 1→0.
func (h *Hub) drop(s Sink, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	mirror, ok := h.subs[s]
	if !ok {
		return false, ErrNotRegistered
	}
	if _, held := mirror[key]; !held {
		return false, ErrNotSubscribed
	}
	return h.detach(s, key), nil
}

// detach removes one connection from one bus key, updating the map, the mirror and the
// desired set together, and reports whether that was the channel's last holder.
//
// The caller holds the write lock. M4: the three are views of one fact and moving them
// apart is what leaves a connection resident in the map after close.
func (h *Hub) detach(s Sink, key string) bool {
	delete(h.subs[s], key)
	// A missing key cannot happen — the mirror and the map are only ever written
	// together under this lock — and needs no guard if it ever did: deleting from a nil
	// map is a no-op, so the invariant breaking degrades to a redundant reconciliation
	// rather than a panic on a connection goroutine.
	set := h.channels[key]
	delete(set, s)
	if len(set) > 0 {
		return false
	}
	delete(h.channels, key)
	delete(h.desired, key)
	return true
}
