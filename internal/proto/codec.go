package proto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// fieldID is the envelope's correlation key. It is the only top-level key that is not a
// command name (docs/03-client-protocol.md §3).
const fieldID = "id"

// Command key names, exactly as docs/03-client-protocol.md §3 and §4 spell them. They
// are case-sensitive: a client sending "Subscribe" gets ErrBadRequest rather than a
// subscription, because a wire format with two spellings is a wire format with two
// implementations.
const (
	keyConnect     = "connect"
	keySubscribe   = "subscribe"
	keyUnsubscribe = "unsubscribe"
	keyPublish     = "publish"
	keySync        = "sync"
	keyPing        = "ping"
)

// Decode parses one client text frame into a Command.
//
// It enforces the framing rules that do not need any connection state, and nothing else:
//
//   - the frame is a single JSON object;
//   - exactly one command key is present (zero or several is an error);
//   - an unknown command key is an error rather than being ignored, because silently
//     ignoring one makes a client's typo look like a server that never answers;
//   - id, where present, is a positive integer.
//
// Unknown fields *inside* a command body are ignored, which is the opposite of the rule
// for the top-level key, deliberately. The top-level key is the command's name and a
// wrong one leaves the client waiting forever for a reply, so it must fail loudly. A body
// field is a parameter, and rejecting unrecognised ones would make every future additive
// field a breaking change against every already-deployed gateway — a browser fleet is
// version-skewed by construction and cannot be upgraded in step with the server. The cost
// of the choice, stated so it is not rediscovered in an incident: a client that misspells
// an optional field gets the default value rather than an error.
//
// Every failure maps to ErrBadRequest at the call site. Decode returns a plain error and
// leaves the code choice to the caller: it has no opinion about whether a failure closes
// the connection, and docs/03-client-protocol.md gives different answers for a malformed
// frame (ErrBadRequest, stay open) and an oversize or binary frame (CloseProtocolError).
// Neither of those two is Decode's to detect — the read loop enforces
// limits.max_frame_size and the frame type before any bytes reach here.
//
// Decode does not validate a channel name, a namespace, or a grant. Those need
// configuration and connection state, which this package deliberately does not have.
// Nor does it enforce that a publish carries an event: §3's own example envelope omits
// it while §4.4 requires it, and rejecting the specification's literal here would be
// worse than checking it in internal/conn, where an ErrBadRequest can actually be sent.
//
// On any error the returned Command is the zero value, so a caller logging both cannot
// report a half-built frame. Decode holds no state and is safe to call concurrently.
// It never panics: it runs on every frame from every browser before any authorization,
// and a panic on a connection goroutine takes the whole replica with it.
func Decode(data []byte) (Command, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return Command{}, fmt.Errorf("proto: frame is not a JSON object: %w", err)
	}
	// A literal "null" unmarshals into a nil map without error, so presence of the map
	// is what separates {} from null. Both are errors, but only for the same reason by
	// accident, and the messages differ.
	if raw == nil {
		return Command{}, fmt.Errorf("proto: frame is null, want a JSON object")
	}

	var cmd Command
	if idRaw, ok := raw[fieldID]; ok {
		id, err := decodeID(idRaw)
		if err != nil {
			return Command{}, err
		}
		cmd.ID = id
	}

	// docs/03-client-protocol.md §3: exactly one command key per frame. A frame with
	// zero or several MUST be answered with error 101 and the connection left open.
	// Sorting makes the message deterministic — Go randomises map iteration, and an
	// error string that changes between identical frames is unbearable to debug against.
	keys := make([]string, 0, len(raw))
	for key := range raw {
		if key == fieldID {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) != 1 {
		return Command{}, fmt.Errorf("proto: frame has %d command keys %q, want exactly 1", len(keys), keys)
	}

	var err error
	switch key := keys[0]; key {
	case keyConnect:
		cmd.Connect = &ConnectRequest{}
		err = decodeBody(key, raw[key], cmd.Connect)
	case keySubscribe:
		cmd.Subscribe = &SubscribeRequest{}
		err = decodeBody(key, raw[key], cmd.Subscribe)
	case keyUnsubscribe:
		cmd.Unsubscribe = &UnsubscribeRequest{}
		err = decodeBody(key, raw[key], cmd.Unsubscribe)
	case keyPublish:
		cmd.Publish = &PublishRequest{}
		err = decodeBody(key, raw[key], cmd.Publish)
	case keySync:
		cmd.Sync = &SyncRequest{}
		err = decodeBody(key, raw[key], cmd.Sync)
	case keyPing:
		cmd.Ping = &PingRequest{}
		err = decodeBody(key, raw[key], cmd.Ping)
	default:
		err = fmt.Errorf("proto: unknown command %q", key)
	}
	if err != nil {
		return Command{}, err
	}
	return cmd, nil
}

// decodeID parses the envelope's id.
//
// docs/03-client-protocol.md §3: id is a positive integer. Absent, zero and negative are
// three different things and only the first is legal — absent means "no reply wanted",
// which §4.6's ping uses, while a zero id is a client that has an off-by-one in its
// counter and will silently fail to correlate every reply it ever receives. A JSON null
// lands here as zero and is refused for the same reason.
func decodeID(raw json.RawMessage) (int64, error) {
	var id int64
	if err := json.Unmarshal(raw, &id); err != nil {
		return 0, fmt.Errorf("proto: id is not an integer: %w", err)
	}
	if id <= 0 {
		return 0, fmt.Errorf("proto: id %d is not a positive integer", id)
	}
	return id, nil
}

// decodeBody unmarshals one command body into v.
//
// The explicit object check is not redundant with the unmarshal: encoding/json accepts a
// JSON null into any struct as a silent no-op, so {"subscribe":null} would otherwise
// decode to a subscribe for the empty channel and reach the grant matcher as a real
// command.
func decodeBody(key string, raw json.RawMessage, v any) error {
	if !isJSONObject(raw) {
		return fmt.Errorf("proto: command %q body is not a JSON object", key)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("proto: command %q body: %w", key, err)
	}
	return nil
}

// isJSONObject reports whether raw is a JSON object.
//
// raw comes from a successful decode into map[string]json.RawMessage, so it is valid
// JSON, non-empty, and stripped of leading whitespace by the decoder. The first byte
// therefore settles the question, and no scanner of our own is needed.
func isJSONObject(raw json.RawMessage) bool {
	return len(raw) > 0 && raw[0] == '{'
}

// Encode serializes one gateway frame to its wire bytes.
//
// v must be one of *Reply, *PushFrame or *DisconnectFrame, and must not be nil. It is
// typed any rather than a sealed interface because the three share no method and
// inventing one to satisfy the type system would put a marker method on every frame
// struct for no benefit; the type switch below is what that costs. A nil pointer is
// refused rather than marshalled, because encoding/json turns it into the four bytes
// "null", which is a text frame no client can parse and no client expects.
//
// Encode is called once per fan-out message, not once per recipient — the result becomes
// a Frame shared by every subscriber (docs/09-internals.md §5). Callers on the fan-out
// path must encode before taking the hub read lock.
//
// It returns nil bytes on any error, holds no state, and is safe to call concurrently.
func Encode(v any) ([]byte, error) {
	var isNil bool
	switch frame := v.(type) {
	case *Reply:
		isNil = frame == nil
	case *PushFrame:
		isNil = frame == nil
	case *DisconnectFrame:
		isNil = frame == nil
	default:
		return nil, fmt.Errorf("proto: cannot encode %T: want *Reply, *PushFrame or *DisconnectFrame", v)
	}
	if isNil {
		return nil, fmt.Errorf("proto: cannot encode a nil %T", v)
	}

	// SetEscapeHTML(false) rather than json.Marshal, which escapes <, > and & into
	// \u003c, \u003e and \u0026. That default exists to make output safe to embed in a
	// <script> block; nothing here is ever embedded in HTML, it inflates a payload full
	// of markup or query strings by six bytes per character on a buffer shared by every
	// subscriber (docs/09-internals.md §5), and docs/03-client-protocol.md §4.4 says a
	// payload is passed through to subscribers untouched — rewriting its bytes is not
	// untouched, even where the JSON is equivalent.
	//
	// The reachable failure is a json.RawMessage holding bytes that are not valid JSON:
	// Pub.Data is publisher-supplied and arrives from outside this process.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("proto: encode %T: %w", v, err)
	}
	// Encoder.Encode terminates the value with a newline. A websocket text frame carries
	// its own length, and Frame.Data is documented as holding no trailing newline.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
