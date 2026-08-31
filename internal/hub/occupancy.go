package hub

import (
	"sort"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Occupancy is one channel's local membership: the bare channel name, how many
// connections on this replica hold it, and — where the caller asked for them — the opaque
// user ids behind those connections.
//
// It is this replica's view only. Cluster-wide counts would need a scatter-gather across
// replicas, which is not built (docs/04-integration.md §4, docs/12-roadmap.md).
//
// The channel name is bare, never a bus key: the prefix is the gateway's business and an
// operator asks about the channel the application publishes to (FR-21).
type Occupancy struct {
	// Channel is the bare channel name, without bus.prefix.
	Channel string

	// Subscribers is the number of connections on this replica holding the channel.
	Subscribers int

	// Users are the opaque user ids of those connections, sorted, one entry per
	// connection — two tabs of one user appear twice, because the count and the list
	// describe the same set. It is nil unless the caller asked for a single channel.
	Users []string
}

// Channels returns every channel this replica currently holds, sorted by name, with the
// local subscriber count on each. Users is left nil: on a replica holding 10,000 channels
// the full membership is a document nobody asked for, and Channel is where an operator
// asks for one deliberately.
//
// The reserved control channel is omitted. It is a permanent member of the reconciler's
// desired set (C8) and never has subscribers, so listing it would offer an operator a
// channel they cannot act on.
//
// It takes the read lock for the length of one map walk and never blocks on the bus: a
// /channels call that queued behind a 30,000-channel resubscribe is an incident tool that
// stops working during an incident (docs/04-integration.md §4). Safe to call concurrently
// with live traffic.
func (h *Hub) Channels() []Occupancy {
	h.mu.RLock()
	out := make([]Occupancy, 0, len(h.channels))
	for key, set := range h.channels {
		out = append(out, Occupancy{Channel: h.channelName(key), Subscribers: len(set)})
	}
	h.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Channel < out[j].Channel })
	return out
}

// Channel returns one channel's local occupancy, including the user ids holding it, and
// reports whether this replica holds it at all.
//
// name is a bare channel name; it is prefixed into a bus key here, through the same
// helper every other path uses, because a lookup against the unprefixed map key would
// report every channel as unheld (FR-21).
//
// A channel this replica does not hold is not an error — the answer is false, and the
// caller turns it into a 404. Safe to call concurrently with live traffic.
func (h *Hub) Channel(name string) (Occupancy, bool) {
	h.mu.RLock()
	set, ok := h.channels[h.key(name)]
	users := make([]string, 0, len(set))
	for s := range set {
		users = append(users, s.User())
	}
	h.mu.RUnlock()

	if !ok {
		return Occupancy{}, false
	}
	sort.Strings(users)
	return Occupancy{Channel: name, Subscribers: len(users), Users: users}, true
}

// Disconnect closes every connection matching exactly one of user and client, and returns
// how many it closed. It is the admin listener's POST /disconnect, and it has the same
// effect as the control channel's disconnect action: proto.CloseRevoked, reconnect false
// (docs/04-integration.md §4, FR-18).
//
// Both targets are matched exactly, never as globs: a target of "u-*" reaches the
// connection literally named that and nothing else (C8). Naming neither is
// ErrNoTarget and naming both is ErrAmbiguousTarget — an omitted target is a validation
// error and not "everyone", because treating it as everyone means one request forces
// every connection on the replica to re-authorize at once, which is the outage
// docs/10-operations.md §4 models.
//
// A target held by no connection on this replica is not an error; it closes nothing and
// returns zero. It must not be called while holding the hub lock, because closing
// deregisters (docs/09-internals.md §4.5), and it is safe to call concurrently.
func (h *Hub) Disconnect(user, client string) (int, error) {
	c := Control{Action: ActionDisconnect, User: user, Client: client}
	if _, err := c.validate(); err != nil {
		return 0, err
	}
	targets := h.targets(c)
	h.closeAll(targets, proto.CloseRevoked, revokedReason)
	return len(targets), nil
}
