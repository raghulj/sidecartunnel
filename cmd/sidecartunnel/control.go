package main

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

// controlReason classifies a rejected control message. It reaches exactly one place: the
// "reason" field on the warn line the rejection is logged with. The three values are kept
// apart because an operator acts on them differently — unsigned is somebody publishing to
// the control channel who should not be, stale is almost always this replica's clock, and
// malformed is a bug in whatever built the message (docs/10-operations.md §7).
type controlReason string

const (
	// controlUnsigned is a control message whose signature was absent or did not verify
	// (FR-23).
	controlUnsigned controlReason = "unsigned"

	// controlStale is a control message whose timestamp fell outside the accepted window.
	controlStale controlReason = "stale"

	// controlMalformed is a control message that did not decode, names no target, or
	// names an action that does not exist.
	controlMalformed controlReason = "malformed"
)

// controlSkew is the window a control message's timestamp must fall inside, matching the
// ±300s the connect webhook's own signature uses (docs/04-integration.md §1.1, §3).
//
// It is a replay window and it is documented as one: a signed message can be replayed
// verbatim within it. nonce is carried so a receiver that wants exactly-once can cache
// seen nonces; this gateway does not, because every control action is idempotent —
// closing a closed connection and unsubscribing an absent subscription are both no-ops.
const controlSkew = 300 * time.Second

// controlEnvelope is the signed wrapper every control message travels in
// (docs/04-integration.md §3).
//
// Body is the action as an opaque JSON *string*, not as sibling fields, and the signature
// covers those exact bytes. That is the whole design: JSON object serialization is not
// canonical, so a receiver cannot recover the bytes a sender signed, and two libraries
// ordering keys differently produce different signatures for the same message. Carrying
// the body as a string removes the question — nothing is re-serialized, so no
// canonicalization rule is needed.
type controlEnvelope struct {
	// TS is the unix timestamp the signature covers. Outside ±controlSkew the message is
	// stale and dropped.
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

// errControl is the sentinel every control rejection wraps, so a caller can tell a
// refused message from a programming error without matching on strings.
var errControl = errors.New("control message rejected")

// verifyControl authenticates one control-channel payload and returns the action it
// carries (FR-23).
//
// It returns the reason the rejection is logged under, so that a grep for
// reason=unsigned, reason=stale or reason=malformed separates the three cases an operator
// acts on differently: unsigned is somebody publishing to the control channel who should
// not be, stale is this replica's clock or a genuinely old message, malformed is an
// application bug in whatever built it.
//
// The signature is checked before the timestamp, deliberately. The signature covers the
// timestamp, so a forgery cannot be logged as "stale" — a stale line therefore always
// means an authentic message outside the window, which is the one worth chasing.
// The comparison is crypto/hmac.Equal, because == on a MAC is a timing oracle and it is
// free to avoid (docs/14-coding-standards.md §9).
//
// It holds no state, performs no I/O and is safe to call concurrently. It never returns,
// logs or embeds the secret or the body (NFR-7).
func verifyControl(secret []byte, now time.Time, payload []byte) (hub.Control, controlReason, error) {
	var env controlEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return hub.Control{}, controlMalformed,
			fmt.Errorf("%w: envelope is not a JSON object: %w", errControl, err)
	}

	sig, err := hex.DecodeString(env.Sig)
	if err != nil {
		return hub.Control{}, controlUnsigned,
			fmt.Errorf("%w: sig is not hexadecimal: %w", errControl, err)
	}
	mac := hmac.New(sha256.New, secret)
	// hash.Hash.Write is documented never to return an error, so there is nothing to
	// check; the blank assignment satisfies errcheck without a branch no test can reach.
	_, _ = mac.Write([]byte(strconv.FormatInt(env.TS, 10) + "." + env.Nonce + "." + env.Body))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return hub.Control{}, controlUnsigned,
			fmt.Errorf("%w: signature does not verify under control.secret", errControl)
	}

	if skew := now.Sub(time.Unix(env.TS, 0)); skew > controlSkew || skew < -controlSkew {
		return hub.Control{}, controlStale,
			fmt.Errorf("%w: ts is %s outside the ±%s window; check this replica's clock",
				errControl, skew.Abs()-controlSkew, controlSkew)
	}

	c, err := hub.ParseControl([]byte(env.Body))
	if err != nil {
		return hub.Control{}, controlMalformed, fmt.Errorf("%w: %w", errControl, err)
	}
	return c, "", nil
}
