package metrics

import "strconv"

// otherNamespace is the label value every channel whose namespace has no configured
// block folds into.
//
// It exists because docs/10-operations.md §5 labels st_subscribe_denied_total by
// namespace, and a denied subscribe is the one place a *client-chosen* string reaches a
// label. A client that subscribes to "probe1-x", "probe2-x", … would otherwise mint a new
// time series per attempt, which is the 200,000-series failure docs/06-channels.md §2
// describes, available on demand to anyone who can open a socket.
//
// It begins with the reserved prefix, so it can never collide with a real namespace: a
// channel beginning "_" is refused before any grant is consulted (docs/06-channels.md §4)
// and therefore no configured namespace can usefully be named this.
const otherNamespace = "_other"

// Namespace is a resolved value of the "namespace" label.
//
// It is an opaque struct with an unexported field, and that is the point: a defined
// string type would be convertible from any channel name, and `metrics.Namespace(channel)`
// would compile. Nothing outside this package can build one, so the only way to obtain a
// namespace label is Metrics.Namespace, which resolves a channel to its namespace instead
// of trusting the caller to have done it.
//
// Label cardinality is a correctness concern here, not a style one. Channel names appear
// in logs and in the admin API by design; they must never appear in a metric label
// (docs/06-channels.md §2).
//
// The zero value is the reserved empty namespace that separator-less channels resolve to,
// which is a legal label value. Namespace is immutable and safe to share across
// goroutines.
type Namespace struct {
	label string
}

// String returns the label value. It is never a channel name.
func (n Namespace) String() string { return n.label }

// Result is a value of the "result" label on st_connections_total. The gateway chooses
// it, so the set is closed and its cardinality is fixed.
type Result string

// The connection outcomes st_connections_total counts. docs/10-operations.md §5 names
// origin_rejected explicitly: climbing means probing or a misconfigured origin.
const (
	// ResultAccepted is a completed handshake that reached the connected state.
	ResultAccepted Result = "accepted"

	// ResultOriginRejected is a handshake refused by the Origin allowlist, before any
	// webhook call was made (FR-2).
	ResultOriginRejected Result = "origin_rejected"

	// ResultUnauthorized is a handshake the application refused: a webhook 401 or 403,
	// closed 3003 with reconnect false (FR-6).
	ResultUnauthorized Result = "unauthorized"

	// ResultUnavailable is a handshake the application could not answer — a 5xx, a
	// timeout, or a full connect queue — closed 3008 and retryable (FR-6).
	ResultUnavailable Result = "unavailable"

	// ResultOverLimit is a handshake refused by limits.max_connections or
	// limits.max_connections_per_user.
	ResultOverLimit Result = "over_limit"
)

// DropReason is a value of the "reason" label on st_messages_dropped_total.
type DropReason string

// The drop reasons docs/10-operations.md §5 lists, plus intake, which
// internal/bus.Health.Dropped reports.
const (
	// DropOversize is a published envelope above limits.max_message_size or the
	// namespace override.
	DropOversize DropReason = "oversize"

	// DropMalformed is a payload that did not decode as an envelope.
	DropMalformed DropReason = "malformed"

	// DropNoSubscriber is a message that arrived for a channel this replica holds no
	// subscriber for. Expected in small numbers around an unsubscribe.
	DropNoSubscriber DropReason = "no_subscriber"

	// DropIntake is a message discarded because the queue between the bus reader and the
	// dispatch workers was full. It is counted from a bus health snapshot rather than at
	// the drop site, because the drop happens inside the bus (docs/09-internals.md §5).
	DropIntake DropReason = "intake"
)

// ControlReason is a value of the "reason" label on st_control_rejected_total.
type ControlReason string

// The control-message rejection reasons. docs/10-operations.md §5 summarises them as
// "unsigned or stale control messages"; malformed covers a payload that failed
// hub.ParseControl.
const (
	// ControlUnsigned is a control message whose signature was absent or did not verify
	// (FR-23).
	ControlUnsigned ControlReason = "unsigned"

	// ControlStale is a control message whose timestamp fell outside the accepted window.
	ControlStale ControlReason = "stale"

	// ControlMalformed is a control message that did not decode, names no target, or
	// names an action that does not exist.
	ControlMalformed ControlReason = "malformed"
)

// Status is a value of the "status" label on the webhook families. It is the HTTP status
// as a string, or one of the two non-HTTP outcomes below.
type Status string

// The non-HTTP webhook outcomes. Without them a timeout would be labelled "0" and would
// be indistinguishable from a transport failure, on the one histogram
// docs/10-operations.md §4's reconnect-storm model is read from.
const (
	// StatusTimeout is a call that exceeded app.webhook_timeout.
	StatusTimeout Status = "timeout"

	// StatusError is a call that failed before a response: DNS, connect, or TLS.
	StatusError Status = "error"
)

// StatusOf returns the label value for an HTTP response status.
func StatusOf(code int) Status { return Status(strconv.Itoa(code)) }
