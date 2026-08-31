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
	"github.com/raghulj/sidecartunnel/internal/proto"
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

	// seams are the in-package test hooks. They are unexported, so no caller outside this
	// package can reach them and no production path can set one.
	seams seams
}

// seams are the injection points this package's own tests use to order a shutdown
// deterministically. docs/14-coding-standards.md §2: code that cannot be tested without a
// sleep needs a seam, not the test a delay.
type seams struct {
	// closerExiting runs on the closer goroutine once the hub's context has ended and it
	// has committed to returning, while Close is still waiting for it. It is the window
	// in which a close enqueued by any other goroutine used to be abandoned.
	closerExiting func()
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
	seams      seams

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

	// userOf records which users bucket each connection is filed under.
	//
	// It exists because a connection is registered before the application has answered —
	// internal/server calls Add at the upgrade so a control disconnect can reach it from
	// the moment it exists (FR-18) — and Sink.User is empty until Attach. Filing it once
	// at Add and never again leaves it under "" for its whole life, which breaks
	// revocation in the quietest possible way: a control disconnect naming the real user
	// reaches nothing and reports success. Remove has the same problem in reverse, and
	// deletes under a key the connection was never filed under, leaking one map entry and
	// one dead Sink per connection.
	userOf map[Sink]string

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

	// closeMu guards closeDone and every registration on closers. It is the whole of the
	// fix for Close racing enqueueClose: Add on a WaitGroup whose counter is zero, while
	// another goroutine is inside Wait on it, is "sync: WaitGroup misuse" — a panic,
	// during shutdown, taking every connection on the replica with it. Attach, Subscribe,
	// Unsubscribe and controlUnsubscribe all reach enqueueClose, so "do not call it
	// concurrently with Close" was never a rule a caller could keep.
	//
	// It is taken only on the slow-consumer path, never on delivery, and it is held
	// across nothing but a flag read and a non-blocking send (C7).
	closeMu sync.Mutex

	// closeDone reports that Close has stopped waiting. After it is set, a close is
	// performed inline by whoever enqueued it: there is no goroutine left to hand it to,
	// and dropping it would leave a connection open with nothing to end it.
	closeDone bool

	// closers counts the goroutines the overflow path spawns. It is separate from wg
	// because wg is what Close waits on first, and a group cannot be safely added to
	// while it is being waited on.
	closers sync.WaitGroup

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
	seams := opts.seams
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
		seams:      seams,
		channels:   make(map[string]map[Sink]struct{}),
		subs:       make(map[Sink]map[string]struct{}),
		users:      make(map[string]map[Sink]struct{}),
		userOf:     make(map[Sink]string),
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

// Close stops the reconciler and the closer, performs every close still outstanding, and
// waits for all of it. It is idempotent and returns nothing, because nothing it does can
// fail: the bus is not the hub's to close, and the registry needs no teardown —
// connections are ended by the drain path (docs/09-internals.md §8) before the hub goes
// away.
//
// It must not be called concurrently with Dispatch: the caller stops its dispatch workers
// first, exactly as it stops accepting upgrades before draining. It may be called
// concurrently with everything else. That is not a courtesy: Attach, Subscribe,
// Unsubscribe and controlUnsubscribe all reach enqueueClose, so a rule saying otherwise
// would have to be kept by every caller of four methods, on a path that only runs when a
// connection is already misbehaving.
//
// The order is the whole of it:
//
//  1. Cancel, and wait for the reconciler and the closer.
//  2. Announce, under closeMu, that nothing more may be handed to either. From here
//     enqueueClose does the work inline, so no close is dropped and nothing registers on
//     a WaitGroup that is already being waited on — which is the panic this ordering
//     exists to make impossible.
//  3. Wait for the overflow closers spawned before that announcement.
//  4. Perform the closes still sitting in the queue. The closer goroutine returns on
//     ctx.Done and used to abandon up to CloserQueue of them, which is a connection left
//     open with nothing left to close it.
func (h *Hub) Close() {
	h.cancel()
	h.wg.Wait()

	h.closeMu.Lock()
	h.closeDone = true
	h.closeMu.Unlock()

	h.closers.Wait()

	for {
		select {
		case s := <-h.closeq:
			h.closeSlow(s)
		default:
			return
		}
	}
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
	return h.registerLocked(s)
}

// registerLocked is Add's body. The caller holds the write lock, so that Attach can
// register and subscribe in one critical section (M15).
func (h *Hub) registerLocked(s Sink) error {
	if existing, ok := h.clients[s.ID()]; ok {
		if existing == s {
			// Re-registration is the connect path: Add ran at the upgrade, when the user
			// id was still empty, and Attach runs once the application has answered. The
			// index has to follow, or revocation by user reaches nothing (FR-18).
			h.indexUserLocked(s)
			return nil
		}
		return fmt.Errorf("hub: add %s: %w", s.ID(), ErrDuplicateID)
	}
	h.subs[s] = make(map[string]struct{})
	h.clients[s.ID()] = s
	h.indexUserLocked(s)
	return nil
}

// reindexLocked refiles a registered connection under its current user id, and reports
// whether it is registered at all. The caller holds the write lock.
//
// The refiling is necessary because Add runs at the upgrade, when Sink.User is still
// empty, and the application names the user only later: a connection left filed under ""
// for its whole life breaks revocation in the quietest possible way, because a control
// disconnect naming the real user reaches nothing and reports success (FR-18).
//
// It creates nothing, and that is the point. registerLocked's maps are the difference
// between a connection that exists and one that does not; recreating them for a Sink that
// Remove has already dropped is the resurrection M4 forbids.
func (h *Hub) reindexLocked(s Sink) bool {
	if _, ok := h.subs[s]; !ok {
		return false
	}
	h.indexUserLocked(s)
	return true
}

// indexUserLocked files s under its current user id, moving it out of whatever bucket it
// was in. The caller holds the write lock.
func (h *Hub) indexUserLocked(s Sink) {
	user := s.User()
	if filed, ok := h.userOf[s]; ok {
		if filed == user {
			return
		}
		h.dropFromUserLocked(s, filed)
	}
	set, ok := h.users[user]
	if !ok {
		set = make(map[Sink]struct{})
		h.users[user] = set
	}
	set[s] = struct{}{}
	h.userOf[s] = user
}

// dropFromUserLocked removes s from the named bucket, which is the one it is actually
// filed under and not necessarily the one s.User names today. The caller holds the write
// lock and has already established that s is filed.
func (h *Hub) dropFromUserLocked(s Sink, user string) {
	set := h.users[user]
	delete(set, s)
	if len(set) == 0 {
		delete(h.users, user)
	}
	delete(h.userOf, s)
}

// Attach takes a registered connection's connect-frame subscriptions in one critical
// section, then queues the reply the ack callback builds — still under the same lock.
//
// It is the connect path of docs/03-client-protocol.md §4.1 and the widest case of M15's
// ordering rule: a connect frame may name hundreds of channels, every one of them live
// from the instant it is inserted. One call rather than a loop of Subscribe is therefore
// not an optimization. It is one lock acquisition, and it leaves no window in which a
// push for a just-granted channel could overtake the connect reply that announces it.
//
// ack is called with the channels actually taken, in the order they were requested, and
// its frame is queued before the lock is released. A channel is silently omitted when it
// is reserved, when its namespace does not resolve, when the connection already holds it,
// or when it would exceed the subscription cap: §4.1 requires an omitted channel to be
// left out of the reply rather than to fail the whole connect. ack is called even when
// nothing was taken — an empty subs map is the important case there — and may return nil,
// which queues nothing.
//
// ack runs under the write lock and MUST NOT call back into the hub, which would
// deadlock. Encoding a frame is what it is for, and that is what internal/conn does.
//
// It registers nothing. Add does that, at the upgrade, which is also what makes a
// connection targetable by a control disconnect before its connect frame arrives (FR-18);
// a connection that is not registered when Attach runs is granted nothing, and ack is
// called with an empty list.
//
// That is the whole of the M4 invariant ErrNotRegistered exists for. A hub that
// registered here could not tell a connection that was never added from one that close
// has just deregistered — and the second is a resurrection: a reader goroutine blocked in
// the connect webhook while SIGTERM closes its connection, answered 200, and back into
// the channel map goes a dead connection that nothing will remove again, so fan-out
// writes to it forever and the refcount never reaches zero.
//
// It never blocks and never waits for the bus.
func (h *Hub) Attach(s Sink, channels []string, ack func(granted []string) *proto.Frame) {
	granted := make([]string, 0, len(channels))
	changed := false

	h.mu.Lock()
	// An unregistered connection is granted nothing, and that is not reported: Attach
	// answers a connect frame, and there is no close code for "the gateway assigned a
	// duplicate client id" or for "this connection was closed while the application was
	// deciding". The connection is simply granted nothing and holds nothing.
	if h.reindexLocked(s) {
		for _, channel := range channels {
			if h.admit(channel) != nil {
				continue
			}
			first, err := h.insertLocked(s, h.key(channel))
			if err != nil {
				continue
			}
			granted = append(granted, channel)
			changed = changed || first
		}
	}
	refused := queueAck(s, ack(granted))
	h.mu.Unlock()

	if changed {
		h.markDirty()
	}
	if refused {
		h.enqueueClose(s)
	}
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
	if filed, found := h.userOf[s]; found {
		h.dropFromUserLocked(s, filed)
	}
	h.mu.Unlock()

	if changed {
		h.markDirty()
	}
}

// Subscribe adds one channel to a connection and queues ack, the caller's pre-encoded
// reply, in the same critical section as the insert.
//
// The ack parameter is how the normative rule in docs/03-client-protocol.md §5.1 is
// satisfied: no push for a channel may reach the client before that channel's subscribe
// reply. Queueing the reply under the lock that inserted the subscription makes that free,
// because a fan-out cannot take the read lock until the insert and the reply have both
// happened, and one writer goroutine per socket turns queue order into wire order (M15).
// Queue it after releasing the lock and a dispatch worker slips into the gap, finds the
// connection already in the channel's set, and puts a push in front of the reply that
// announces the channel. The window is small, silent, and makes two conforming clients
// disagree — one drops the message, the other closes.
//
// ack may be nil, which queues nothing. A connection that refuses it has a full outbound
// queue and is handed to the closer goroutine after the lock is released (FR-15).
//
// It refuses a reserved channel, an unresolvable namespace, a duplicate, a connection
// past its subscription cap, and a connection that is not registered; nothing is queued
// on any of those paths, because the caller answers them with an error reply instead. It
// does no authorization: grants are the connection's, and a subscription that exists is
// delivered to without being re-checked (docs/05-authorization.md §4).
//
// It never blocks and never waits for the bus. A 0→1 refcount transition adds the bus key
// to the desired set inside the same critical section as the map mutation and then marks
// the reconciler dirty with a non-blocking store (FR-10, C7).
func (h *Hub) Subscribe(s Sink, channel string, ack *proto.Frame) error {
	if err := h.admit(channel); err != nil {
		return fmt.Errorf("hub: subscribe %q: %w", channel, err)
	}

	h.mu.Lock()
	first, err := h.insertLocked(s, h.key(channel))
	refused := err == nil && queueAck(s, ack)
	h.mu.Unlock()

	if err != nil {
		return fmt.Errorf("hub: subscribe %q: %w", channel, err)
	}
	if first {
		h.markDirty()
	}
	if refused {
		h.enqueueClose(s)
	}
	return nil
}

// Unsubscribe drops one channel from a connection and queues ack in the same critical
// section as the drop.
//
// It is the other half of M15: no push for a channel may reach the client after that
// channel's unsubscribe reply. Dropping the subscription and queueing the reply together
// guarantees it, because a fan-out either ran entirely before the write lock was granted —
// and therefore queued its push before the reply — or entirely after, when the connection
// is no longer in the channel's set.
//
// ack may be nil. A connection that refuses it is handed to the closer goroutine after
// the lock is released.
//
// It never blocks. A 1→0 refcount transition removes the bus key from the desired set
// under the lock and marks the reconciler dirty afterwards (FR-10).
func (h *Hub) Unsubscribe(s Sink, channel string, ack *proto.Frame) error {
	h.mu.Lock()
	last, err := h.dropLocked(s, h.key(channel))
	refused := err == nil && queueAck(s, ack)
	h.mu.Unlock()

	if err != nil {
		return fmt.Errorf("hub: unsubscribe %q: %w", channel, err)
	}
	if last {
		h.markDirty()
	}
	if refused {
		h.enqueueClose(s)
	}
	return nil
}

// admit reports why a channel may not be subscribed to at all, or nil.
//
// It consults no mutable state — the namespace table is written once, by New — so it runs
// outside the lock and keeps the critical section to the map mutation.
func (h *Hub) admit(channel string) error {
	if strings.HasPrefix(channel, reservedPrefix) {
		return ErrReservedChannel
	}
	if _, ok := h.Namespace(channel); !ok {
		return ErrUnknownNamespace
	}
	return nil
}

// queueAck hands one pre-encoded reply to a connection while the caller still holds the
// write lock, and reports whether the connection refused it.
//
// A nil ack is nothing to queue and is not a refusal: the caller had no reply to send, or
// failed to encode one, and neither is a reason to close a connection whose outbound
// queue is perfectly healthy.
func queueAck(s Sink, ack *proto.Frame) bool {
	if ack == nil {
		return false
	}
	return !s.Send(ack)
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

// insertLocked adds one connection to one bus key and reports whether the channel went
// 0→1. The caller holds the write lock.
//
// FR-10: the transition is decided here, under the lock, as len(set) == 1 immediately
// after the insert. Recomputing it after the lock is released lets a concurrent
// unsubscribe interleave, which either subscribes upstream twice or drops a subscription
// that still has a holder.
func (h *Hub) insertLocked(s Sink, key string) (bool, error) {
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

// dropLocked removes one connection from one bus key and reports whether the channel went
// 1→0. The caller holds the write lock.
func (h *Hub) dropLocked(s Sink, key string) (bool, error) {
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
