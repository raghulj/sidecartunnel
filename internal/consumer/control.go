package consumer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/raghulj/sidecartunnel/internal/hub"
)

// Reason classifies a dropped message. It reaches two places: the "reason" field on the
// line the drop is logged with, and the counter it is accounted to. The values are kept
// apart because an operator acts on them differently — unsigned is somebody publishing to
// the control channel who should not be, stale is almost always this replica's clock,
// malformed is a bug in whatever built the message, and oversize is an application
// publishing past limits.max_message_size (docs/10-operations.md §7).
type Reason string

const (
	// ReasonUnsigned is a control message whose signature was absent or did not verify
	// (FR-23).
	ReasonUnsigned Reason = "unsigned"

	// ReasonStale is a control message whose timestamp fell outside the accepted window.
	ReasonStale Reason = "stale"

	// ReasonMalformed is a message that did not decode: a control envelope that names no
	// target or an action that does not exist, or a published envelope missing event or
	// data (docs/04-integration.md §2.2).
	ReasonMalformed Reason = "malformed"

	// ReasonOversize is a published envelope larger than limits.max_message_size. FR-14
	// makes this log line the acceptance criterion itself, so it carries the channel name
	// and never the payload.
	ReasonOversize Reason = "oversize"
)

// Skew is the window a control message's timestamp must fall inside, matching the ±300s
// the connect webhook's own signature uses (docs/04-integration.md §1.1, §3).
//
// It is a replay window and it is documented as one: a signed message can be replayed
// verbatim within it. Nonce is carried so a receiver that wants exactly-once can cache
// seen nonces; this gateway does not, because every control action is idempotent —
// closing a closed connection and unsubscribing an absent subscription are both no-ops.
const Skew = 300 * time.Second

// Envelope is the signed wrapper every control message travels in
// (docs/04-integration.md §3).
//
// Body is the action as an opaque JSON *string*, not as sibling fields, and the signature
// covers those exact bytes. That is the whole design: JSON object serialization is not
// canonical, so a receiver cannot recover the bytes a sender signed, and two libraries
// ordering keys differently produce different signatures for the same message. Carrying
// the body as a string removes the question — nothing is re-serialized, so no
// canonicalization rule is needed.
type Envelope struct {
	// TS is the unix timestamp the signature covers. Outside ±Skew the message is stale
	// and dropped.
	TS int64 `json:"ts"`

	// Nonce is echoed for receivers that want to reject replays inside the window.
	Nonce string `json:"nonce"`

	// Body is the control action as a JSON string, verified byte for byte and only then
	// parsed.
	Body string `json:"body"`

	// Sig is the lowercase-hex HMAC-SHA256 of ts + "." + nonce + "." + body under
	// control.secret.
	Sig string `json:"sig"`
}

// ErrRejected is the sentinel every control rejection wraps, so a caller can tell a
// refused message from a programming error without matching on strings.
var ErrRejected = errors.New("control message rejected")

// Verify authenticates one control-channel payload and returns the action it carries
// (FR-23).
//
// It returns the reason the rejection is logged and counted under, so that a grep for
// reason=unsigned, reason=stale or reason=malformed separates the three cases an operator
// acts on differently: unsigned is somebody publishing to the control channel who should
// not be, stale is this replica's clock or a genuinely old message, malformed is an
// application bug in whatever built it.
//
// The signature is checked before the timestamp, deliberately. The signature covers the
// timestamp, so a forgery cannot be logged as "stale" — a stale line therefore always
// means an authentic message outside the window, which is the one worth chasing. The
// comparison is crypto/hmac.Equal, because == on a MAC is a timing oracle and it is free
// to avoid (docs/14-coding-standards.md §9).
//
// The body is parsed only after it verifies, and it is parsed as a document that may be
// anything: a body that is valid JSON but not an object is a rejection and never a panic.
//
// It holds no state, performs no I/O and is safe to call concurrently. It never returns,
// logs or embeds the secret or the body (NFR-7).
func Verify(secret []byte, now time.Time, payload []byte) (hub.Control, Reason, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return hub.Control{}, ReasonMalformed,
			fmt.Errorf("%w: envelope is not a JSON object: %w", ErrRejected, err)
	}

	sig, err := hex.DecodeString(env.Sig)
	if err != nil {
		return hub.Control{}, ReasonUnsigned,
			fmt.Errorf("%w: sig is not hexadecimal: %w", ErrRejected, err)
	}
	mac := hmac.New(sha256.New, secret)
	// hash.Hash.Write is documented never to return an error, so there is nothing to
	// check; the blank assignment satisfies errcheck without a branch no test can reach.
	_, _ = mac.Write([]byte(strconv.FormatInt(env.TS, 10) + "." + env.Nonce + "." + env.Body))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return hub.Control{}, ReasonUnsigned,
			fmt.Errorf("%w: signature does not verify under control.secret", ErrRejected)
	}

	if skew := now.Sub(time.Unix(env.TS, 0)); skew > Skew || skew < -Skew {
		return hub.Control{}, ReasonStale,
			fmt.Errorf("%w: ts is %s outside the ±%s window; check this replica's clock",
				ErrRejected, skew.Abs()-Skew, Skew)
	}

	c, err := hub.ParseControl([]byte(env.Body))
	if err != nil {
		return hub.Control{}, ReasonMalformed, fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return c, "", nil
}
