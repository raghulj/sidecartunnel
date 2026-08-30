package glob

import (
	"errors"
	"fmt"
	"strings"
)

// ErrReservedPrefix is returned by Compile, wrapped, for a grant beginning "_". That
// prefix is reserved for control channels and must never be grantable
// (docs/05-authorization.md §3, docs/06-channels.md §4).
//
// It is a sentinel so the connect path can tell a malformed grant list from a transport
// failure and answer the application with the right code rather than a guess; test with
// errors.Is, never by comparing error strings.
var ErrReservedPrefix = errors.New("grant begins with the reserved \"_\" prefix")

// kind names the compiled form of a grant. Each form answers Match with the cheapest
// operation that is exact for it, which is what keeps the match path free of both
// allocation and branching on the pattern text (FR-9, docs/09-internals.md §6).
type kind uint8

const (
	// kindNone is the zero value, and therefore the kind of the zero Pattern and of the
	// Pattern returned alongside a compile error. It matches nothing, which is the safe
	// direction to fail in: a grant that did not compile must not match everything.
	kindNone     kind = iota
	kindLiteral       // no `*`: string equality
	kindAny           // nothing but `*`: matches every channel
	kindPrefix        // "abc*": one trailing star
	kindSuffix        // "*abc": one leading star
	kindContains      // "*abc*": one floating segment, both ends open
	kindSegments      // everything else: anchored head and tail plus floating middles
)

// Pattern is one compiled grant.
//
// Compile turns a grant into the cheapest structure that can answer Match: a literal
// comparison when the grant has no `*`, a prefix or suffix check when it has one at an
// end, and a segment scan otherwise. docs/09-internals.md §6 describes the intended
// representation; it is not part of this contract, and callers must treat a Pattern as
// opaque.
//
// A Pattern is immutable once compiled and safe for concurrent use by any number of
// goroutines. It is a value type so that a Set can hold patterns inline rather than as
// pointers, which is what keeps matching allocation-free.
//
// The zero Pattern matches nothing. That is the useful default: a connection whose grant
// list failed to compile must not accidentally match everything.
type Pattern struct {
	// head is the literal for kindLiteral, the prefix for kindPrefix, the needle for
	// kindContains, and the anchored head for kindSegments. tail is the suffix for
	// kindSuffix and the anchored tail for kindSegments. parts holds the floating middle
	// segments of a kindSegments pattern, in order, with empty ones already dropped —
	// `a**b` has the same meaning as `a*b`, so the redundant scan is removed at compile
	// time rather than paid for on every match. (docs/09-internals.md §6 sketches these
	// three fields as a, b and parts; the names are the only difference.)
	//
	// Nothing writes to these after Compile returns. That is the entire concurrency
	// story: no mutex on the match path, and a Set published through an atomic pointer
	// is safe to read from any number of goroutines (FR-9, docs/09-internals.md §2).
	kind  kind
	head  string
	tail  string
	parts []string
}

// Compile turns one grant string into a Pattern.
//
// It rejects a grant beginning "_": that prefix is reserved for control channels and must
// never be grantable, so the refusal happens here rather than being left to the subscribe
// path where a future caller might forget it (docs/05-authorization.md §3,
// docs/06-channels.md §4).
//
// Compile is called once per grant at connect. It may allocate; Match may not.
func Compile(pattern string) (Pattern, error) {
	if strings.HasPrefix(pattern, "_") {
		// The grant is named in the error because channel names are not secrets — the
		// control they carry is the grant list itself (docs/05-authorization.md §8) —
		// and an operator reading a rejected connect needs to know which grant was bad.
		return Pattern{kind: kindNone}, fmt.Errorf("compile grant %q: %w", pattern, ErrReservedPrefix)
	}

	// `*` is the only metacharacter, so a grant without one is an exact string and
	// nothing else needs deciding (docs/05-authorization.md §3). `?`, `[...]`, `{...}`
	// and `\` are ordinary characters here and MUST stay that way: each one a matcher
	// understands is another way for a grant to cover more than its author meant.
	if !strings.Contains(pattern, "*") {
		return Pattern{kind: kindLiteral, head: pattern}, nil
	}
	// A grant of nothing but stars matches every channel, including the empty one.
	if strings.Trim(pattern, "*") == "" {
		return Pattern{kind: kindAny}, nil
	}

	// Split on `*`. The first and last segments are anchored to the ends of the channel;
	// everything between them floats.
	segs := strings.Split(pattern, "*")
	head, tail := segs[0], segs[len(segs)-1]
	middles := make([]string, 0, len(segs)-2)
	for _, seg := range segs[1 : len(segs)-1] {
		if seg != "" {
			middles = append(middles, seg)
		}
	}

	switch {
	case head == "" && tail == "" && len(middles) == 1:
		return Pattern{kind: kindContains, head: middles[0]}, nil
	case head == "" && len(middles) == 0:
		return Pattern{kind: kindSuffix, tail: tail}, nil
	case tail == "" && len(middles) == 0:
		return Pattern{kind: kindPrefix, head: head}, nil
	default:
		return Pattern{kind: kindSegments, head: head, tail: tail, parts: middles}, nil
	}
}

// Match reports whether s is covered by this pattern.
//
// It performs no I/O, takes no lock, and allocates nothing (FR-9). It is safe to call
// concurrently from any number of goroutines.
//
// The trap worth restating, because someone will eventually try to "fix" it: `user-*`
// matches `user-7-private` and `user-8`, not only `user-7`. Grants are exactly as tight
// as the application writes them and the gateway does not second-guess a loose one. An
// application meaning "only this user's own channel" emits `user-7`. The table in
// docs/05-authorization.md §3 is the required test, including that row.
func (p Pattern) Match(s string) bool {
	switch p.kind {
	case kindLiteral:
		return s == p.head
	case kindAny:
		return true
	case kindPrefix:
		return strings.HasPrefix(s, p.head)
	case kindSuffix:
		return strings.HasSuffix(s, p.tail)
	case kindContains:
		return strings.Contains(s, p.head)
	case kindSegments:
		return p.matchSegments(s)
	}
	// kindNone. The zero Pattern, and the one handed back with a compile error, match
	// nothing; failing closed is the only direction an authorization primitive may fail.
	return false
}

// matchSegments answers a pattern with more than one star, or with a star at neither end.
//
// The head and tail are anchored, then each floating segment is taken at its leftmost
// occurrence in what remains. Leftmost is exact — not a heuristic needing backtracking —
// because `*` is the only metacharacter: segments carry no length constraint of their
// own, so the earliest placement of one leaves the longest remainder for the next, and if
// that fails no later placement can succeed.
//
// Worst case is therefore O(len(s) × len(pattern)): one bounded scan per segment, and the
// segment lengths sum to at most len(pattern). There is no exponential case. That is why
// regexp is banned here (doc.go) and not merely discouraged: a backtracking engine takes
// exponential time on `a*a*a*a*a*a*a*a*a*b` against a run of `a`s, and grants are written
// by an application whose own input may not be trusted.
func (p Pattern) matchSegments(s string) bool {
	if !strings.HasPrefix(s, p.head) {
		return false
	}
	s = s[len(p.head):]
	// Checked against what is left after the head, so a head and tail that would have to
	// overlap — `a*a` against `a` — is rejected here rather than double-counting a
	// character.
	if !strings.HasSuffix(s, p.tail) {
		return false
	}
	s = s[:len(s)-len(p.tail)]

	for _, seg := range p.parts {
		i := strings.Index(s, seg)
		if i < 0 {
			return false
		}
		s = s[i+len(seg):]
	}
	return true
}

// Set is an immutable collection of compiled grants. A channel matches the Set if any
// pattern in it matches.
//
// Set is what a connection actually holds, through an atomic pointer. It is never
// mutated: grant narrowing and revocation build a new Set and swap the pointer, so a
// matcher running concurrently sees either the old set or the new one and never a torn
// mixture of both (FR-9, docs/09-internals.md §2).
//
// The zero Set matches nothing, which is also what an empty grant list means. A
// connection with no grants is legal and simply cannot subscribe to anything
// (docs/04-integration.md §1.2).
type Set struct {
	// patterns is written once, by NewSet, and never afterwards. Callers get a Set by
	// value and must not retain the slice, which they cannot: it is unexported.
	patterns []Pattern
}

// NewSet compiles a whole grant list into a Set.
//
// It returns an error if any grant fails to compile, and does not silently skip the bad
// one: a connection authorized against a partially-compiled grant list is a connection
// holding permissions nobody can account for. The caller refuses the connection.
//
// Duplicate grants are permitted — the application may well emit them — and matching is
// unaffected.
func NewSet(patterns []string) (Set, error) {
	if len(patterns) == 0 {
		// An empty grant list and the zero Set are the same thing: no grants, no
		// subscriptions (docs/05-authorization.md §3, last table row).
		return Set{}, nil
	}

	compiled := make([]Pattern, 0, len(patterns))
	for i, raw := range patterns {
		p, err := Compile(raw)
		if err != nil {
			// The zero Set, not a partial one. A caller that logs the error and carries
			// on must end up with a connection that can subscribe to nothing rather
			// than one holding whichever prefix of the list happened to compile.
			return Set{}, fmt.Errorf("grant %d: %w", i, err)
		}
		compiled = append(compiled, p)
	}
	return Set{patterns: compiled}, nil
}

// Match reports whether channel is covered by any grant in the Set.
//
// Like Pattern.Match it performs no I/O, takes no lock, and allocates nothing, and is
// safe for concurrent use.
func (s Set) Match(channel string) bool {
	// Indexed rather than ranged by value: a grant list is usually short and usually
	// misses on most of its entries, and this keeps each iteration to a bounds check and
	// a call with no copy of the Pattern header.
	for i := range s.patterns {
		if s.patterns[i].Match(channel) {
			return true
		}
	}
	return false
}
