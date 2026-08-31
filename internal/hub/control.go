package hub

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Control actions, exactly as docs/04-integration.md §3 spells them.
const (
	// ActionDisconnect closes matching connections with proto.CloseRevoked and
	// reconnect: false — a decision was made and retrying cannot change it (FR-18).
	ActionDisconnect = "disconnect"

	// ActionRefresh closes matching connections with proto.CloseExpired and
	// reconnect: true. They reconnect and re-authorize with a current cookie (S3).
	ActionRefresh = "refresh"

	// ActionUnsubscribe drops matching subscriptions and pushes each client an
	// unsubscribed frame (FR-17).
	ActionUnsubscribe = "unsubscribe"
)

// Default close reasons. They name the decision and nothing else: a reason must never
// carry a cookie, a header value or a payload (NFR-7).
const (
	revokedReason = "revoked"
	refreshReason = "re-authorization required"
)

// Errors returned when a control message does not validate. They are sentinels so the
// control consumer can count them by kind rather than by string.
var (
	// ErrUnknownAction is an action outside the three docs/04-integration.md §3 defines.
	ErrUnknownAction = errors.New("hub: unknown control action")

	// ErrNoTarget is a control message naming neither user nor client.
	//
	// C8: an omitted target is a validation error, not "everyone". Treating it as
	// everyone means a single publish forces every connection on every replica to
	// re-authorize at once, which is the application outage docs/10-operations.md §4
	// models — available on demand, to anyone who can publish.
	ErrNoTarget = errors.New("hub: control message names no target")

	// ErrAmbiguousTarget is a control message naming both user and client. Every action
	// MUST name exactly one.
	ErrAmbiguousTarget = errors.New("hub: control message names both user and client")

	// ErrNoChannel is an unsubscribe with no channel.
	ErrNoChannel = errors.New("hub: control unsubscribe names no channel")
)

// Control is one decoded control-channel message (docs/04-integration.md §3).
//
// Signature and timestamp verification (FR-23) happens before this, in the control
// consumer that owns control.secret. The hub holds no secrets: it is handed a message
// that has already been proven authentic, exactly as it is handed a grant decision the
// application has already made.
type Control struct {
	// Action is one of ActionDisconnect, ActionRefresh or ActionUnsubscribe.
	Action string `json:"action"`

	// User targets every connection for this opaque user id. Matched exactly, never as a
	// glob (C8).
	User string `json:"user"`

	// Client targets one connection by client id. Matched exactly, never as a glob.
	Client string `json:"client"`

	// Channel is the subscription pattern for an unsubscribe. It may be a glob, and it
	// is the only field here that is one.
	Channel string `json:"channel"`

	// Reason is a short human-readable string carried into the disconnect frame or the
	// unsubscribed push. Informational.
	Reason string `json:"reason"`
}

// ParseControl decodes and validates one control-channel payload.
//
// It enforces the rules C8 turns on: the action is one of three, exactly one of user and
// client is named, and an unsubscribe carries a channel that compiles as a grant pattern.
// A message that fails any of them is dropped by the caller and counted; it is never
// applied partially.
//
// It holds no state and is safe to call concurrently.
func ParseControl(payload []byte) (Control, error) {
	var c Control
	if err := json.Unmarshal(payload, &c); err != nil {
		return Control{}, fmt.Errorf("hub: control message is not a JSON object: %w", err)
	}
	if _, err := c.validate(); err != nil {
		return Control{}, err
	}
	return c, nil
}

// validate enforces the targeting rules on a Control however it was built, and returns
// the compiled channel pattern for an unsubscribe.
//
// The pattern is compiled here, once, and handed to the caller rather than recompiled
// where it is used: a second compile is a second error to handle on a path that has
// already proved the grant is well-formed. For any other action the zero Pattern comes
// back, which matches nothing.
func (c Control) validate() (glob.Pattern, error) {
	switch c.Action {
	case ActionDisconnect, ActionRefresh, ActionUnsubscribe:
	default:
		return glob.Pattern{}, fmt.Errorf("hub: control action %q: %w", c.Action, ErrUnknownAction)
	}
	switch {
	case c.User == "" && c.Client == "":
		return glob.Pattern{}, fmt.Errorf("hub: control %s: %w", c.Action, ErrNoTarget)
	case c.User != "" && c.Client != "":
		return glob.Pattern{}, fmt.Errorf("hub: control %s: %w", c.Action, ErrAmbiguousTarget)
	}
	if c.Action != ActionUnsubscribe {
		return glob.Pattern{}, nil
	}
	if c.Channel == "" {
		return glob.Pattern{}, fmt.Errorf("hub: control %s: %w", c.Action, ErrNoChannel)
	}
	pattern, err := glob.Compile(c.Channel)
	if err != nil {
		return glob.Pattern{}, fmt.Errorf("hub: control %s: %w", c.Action, err)
	}
	return pattern, nil
}

// Control applies one control message to this replica.
//
// It validates first, so a caller that built the message by hand gets the same refusal a
// malformed publish would. A target held by another replica is not an error: every
// replica consumes every control message and acts on whatever it holds.
//
// It is called on the control channel's own goroutine, never on a dispatch worker, so
// that a revocation cannot queue behind the firehose it may exist to stop
// (docs/09-internals.md §5). That is also why it may close connections inline: the ban in
// §4.5 is on closing while the fan-out read lock is held, and nothing here holds a lock
// when it closes.
func (h *Hub) Control(c Control) error {
	pattern, err := c.validate()
	if err != nil {
		return err
	}
	targets := h.targets(c)
	switch c.Action {
	case ActionDisconnect:
		h.closeAll(targets, proto.CloseRevoked, orDefault(c.Reason, revokedReason))
	case ActionRefresh:
		// Spreading a mass refresh over control.refresh_spread is the connection
		// layer's: it is expressed as retry_after in the disconnect frame, which Sink
		// cannot carry (S5, docs/13-review-findings.md C8).
		h.closeAll(targets, proto.CloseExpired, orDefault(c.Reason, refreshReason))
	default:
		h.controlUnsubscribe(targets, c, pattern)
	}
	return nil
}

// targets resolves a control message to the connections it names.
//
// Both lookups are exact map hits, which is what "matched exactly, never as a glob"
// means in code: there is no path here that could scan and match loosely, so a target of
// "u-*" reaches the connection literally named that and nothing else (C8).
func (h *Hub) targets(c Control) []Sink {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if c.Client != "" {
		if s, ok := h.clients[c.Client]; ok {
			return []Sink{s}
		}
		return nil
	}
	set := h.users[c.User]
	out := make([]Sink, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}

// closeAll deregisters and then ends each connection.
//
// Deregistering first stops further fan-out selecting a connection that is going away,
// and both halves are idempotent, because expiry, revocation, drain and a slow-consumer
// overflow can all decide to end the same connection at once.
func (h *Hub) closeAll(targets []Sink, code proto.CloseCode, reason string) {
	for _, s := range targets {
		h.Remove(s)
		s.Close(code, reason)
	}
}

// controlUnsubscribe drops every subscription of every target matching the pattern and
// pushes each client an unsubscribed frame (FR-17).
//
// Everything happens in one write-locked critical section, and that is not an oversight
// about holding a lock across an encode:
//
//   - The subscription change and its push are queued together, which is normative. A
//     client must never receive a push for a channel after being told it no longer holds
//     it, and queue order is what guarantees that given one writer goroutine per socket
//     (proto.Push, M15).
//   - Selecting under a read lock and mutating under a write lock afterwards opens a gap.
//     A channel subscribed inside it survives a revocation that named it, and every pair
//     selected before it has to be re-checked after — two branches that exist only
//     because of the gap.
//
// The cost is one small JSON encode per distinct channel — shared by every recipient of
// it, as in fan-out (M10) — on a path taken by a revocation, not by delivery. Sends are
// non-blocking, so the section stays bounded; connections that refuse one are closed
// after the lock is released (docs/09-internals.md §4.5).
func (h *Hub) controlUnsubscribe(targets []Sink, c Control, pattern glob.Pattern) {
	frames := make(map[string]*proto.Frame)
	seen := make(map[Sink]struct{})
	var changed bool
	var slow []Sink
	var matched []string

	h.mu.Lock()
	for _, s := range targets {
		// Ranging the mirror map directly: a target removed since it was selected has no
		// mirror, and ranging a nil map is zero iterations rather than a special case.
		matched = matched[:0]
		for key := range h.subs[s] {
			if pattern.Match(h.channelName(key)) {
				matched = append(matched, key)
			}
		}
		for _, key := range matched {
			frame, ok := frames[key]
			if !ok {
				encoded, err := encodeFrame(&proto.PushFrame{Push: &proto.Push{
					Channel:      h.channelName(key),
					Unsubscribed: &proto.Unsubscribed{Reason: c.Reason},
				}})
				if err != nil {
					// coverage: unreachable — the push carries no json.RawMessage, so
					// proto.Encode cannot fail on it. The subscription is left in place
					// rather than removed silently: a client that keeps a channel the
					// gateway also keeps is consistent, which is what FR-17 protects.
					continue
				}
				frame = encoded
				frames[key] = frame
			}
			if h.detach(s, key) {
				changed = true
			}
			if !s.Send(frame) {
				if _, dup := seen[s]; !dup {
					seen[s] = struct{}{}
					slow = append(slow, s)
				}
			}
		}
	}
	h.mu.Unlock()

	if changed {
		h.markDirty()
	}
	for _, s := range slow {
		h.enqueueClose(s)
	}
}

// orDefault returns s, or fallback when s is empty.
func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
