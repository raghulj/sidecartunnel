package glob

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestPatternMatch_SpecTable_FR8 is the required test named by FR-8's acceptance
// criterion: every row of the table in docs/05-authorization.md §3, verbatim.
func TestPatternMatch_SpecTable_FR8(t *testing.T) {
	tests := []struct {
		name    string
		grant   string
		channel string
		want    bool
	}{
		{"literal match", "room-4410", "room-4410", true},
		{"literal is not a prefix", "room-4410", "room-44100", false},
		{"case sensitive", "room-4410", "Room-4410", false},
		{"prefix star matches", "org-42-*", "org-42-alerts", true},
		{"star matches empty", "org-42-*", "org-42-", true},
		{"star does not match a missing separator", "org-42-*", "org-42", false},
		{"prefix is compared exactly", "org-42-*", "org-421-alerts", false},
		{"different prefix", "org-42-*", "org-99-secret", false},
		{"bare star matches anything", "*", "anything", true},
		{"user star matches the exact channel", "user-*", "user-7", true},
		// docs/05-authorization.md §3: THIS ROW IS A DOCUMENTED TRAP, NOT A BUG.
		// `user-*` grants everything beneath `user-`, separators included, so it also
		// covers `user-7-private` and `user-8`. An application meaning "only this
		// user's own channel" must emit `user-7`. Do not "fix" this: narrowing `*` so
		// it stops at some separator would silently change the meaning of every grant
		// an application has already written. The row is locked in by this test.
		{"star crosses separators (documented trap)", "user-*", "user-7-private", true},
		{"star crosses separators, sibling user", "user-*", "user-8", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Compile(tt.grant)
			if err != nil {
				t.Fatalf("Compile(%q) = error %v, want no error", tt.grant, err)
			}
			if got := p.Match(tt.channel); got != tt.want {
				t.Errorf("Compile(%q).Match(%q) = %v, want %v", tt.grant, tt.channel, got, tt.want)
			}
		})
	}
}

// TestSetMatch_EmptyGrantList_FR8 is the table's last row: an empty grant list matches
// nothing. A connection with no grants is legal and simply cannot subscribe
// (docs/04-integration.md §1.2).
func TestSetMatch_EmptyGrantList_FR8(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{"nil list", nil},
		{"empty list", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSet(tt.patterns)
			if err != nil {
				t.Fatalf("NewSet(%v) = error %v, want no error", tt.patterns, err)
			}
			for _, channel := range []string{"anything", "", "*", "room-4410"} {
				if s.Match(channel) {
					t.Errorf("empty Set matched %q, want no match", channel)
				}
			}
		})
	}
}

// TestPatternMatch_StarSemantics_FR8 covers `*` beyond the spec table: it matches any
// run of characters including none, it is the only metacharacter, and it may appear any
// number of times anywhere (docs/05-authorization.md §3).
func TestPatternMatch_StarSemantics_FR8(t *testing.T) {
	tests := []struct {
		name    string
		grant   string
		channel string
		want    bool
	}{
		// Single trailing star.
		{"prefix star, empty tail", "org-42-*", "org-42-", true},
		{"prefix star, long tail", "org-42-*", "org-42-a-b-c", true},
		{"prefix star, one char short", "org-42-*", "org-42", false},
		{"prefix star, unrelated", "org-42-*", "x", false},

		// Single leading star.
		{"suffix star, empty head", "*-alerts", "-alerts", true},
		{"suffix star, long head", "*-alerts", "org-42-alerts", true},
		{"suffix star, no match", "*-alerts", "alerts", false},
		{"suffix star, shorter than suffix", "*-alerts", "erts", false},

		// Star at both ends.
		{"contains, middle", "*-42-*", "org-42-alerts", true},
		{"contains, at the start", "*org*", "org-42", true},
		{"contains, at the end", "*42*", "org-42", true},
		{"contains, whole string", "*org-42*", "org-42", true},
		{"contains, absent", "*-99-*", "org-42-alerts", false},

		// Star only.
		{"bare star matches the empty channel", "*", "", true},
		{"bare star matches a long channel", "*", strings.Repeat("x", 4096), true},
		{"double star matches anything", "**", "org-42", true},
		{"double star matches the empty channel", "**", "", true},
		{"triple star matches anything", "***", "org-42", true},

		// Multi-star.
		{"a*b*c matches", "a*b*c", "aXXbYYc", true},
		{"a*b*c matches with empty runs", "a*b*c", "abc", true},
		{"a*b*c requires order", "a*b*c", "acb", false},
		{"a*b*c requires the suffix", "a*b*c", "aXXbYY", false},
		{"a*b*c requires the prefix", "a*b*c", "XaXbXc", false},
		{"a*b*c segments may not overlap", "a*b*c", "abc0", false},
		{"segments may not be reused", "a*a", "a", false},
		{"segments may not be reused, two chars", "a*a", "aa", true},
		{"greedy suffix anchoring", "a*ab", "aab", true},
		{"adjacent stars in the middle", "a**b", "ab", true},
		{"adjacent stars in the middle, long", "a**b", "aXXXb", true},
		{"leading and trailing star with middles", "*a*b*", "0a1b2", true},
		{"leading and trailing star, missing middle", "*a*b*", "0b1a2", false},

		// No other metacharacter exists. `?`, `[...]`, `{...}` and `\` are literals;
		// docs/05-authorization.md §3 says adding them is forbidden, so a grant
		// containing one must match only a channel containing it too.
		{"question mark is a literal", "room-?", "room-4", false},
		{"question mark matches itself", "room-?", "room-?", true},
		{"bracket class is a literal", "room-[0-9]", "room-4", false},
		{"bracket class matches itself", "room-[0-9]", "room-[0-9]", true},
		{"brace alternation is a literal", "room-{a,b}", "room-a", false},
		{"brace alternation matches itself", "room-{a,b}", "room-{a,b}", true},
		{"backslash does not escape a star", `room-\*`, "room-x", false},
		{"backslash is a literal", `room-\*`, `room-\x`, true},
		{"backslash before star still stars", `room-\*`, `room-\`, true},
		{"dot is a literal, not regexp", "room.", "roomX", false},
		{"dot matches itself", "room.", "room.", true},

		// Case sensitivity, restated for the star forms.
		{"prefix star is case sensitive", "Org-*", "org-42", false},
		{"suffix star is case sensitive", "*-Alerts", "org-42-alerts", false},

		// The empty grant. It is not rejected — only a `_` prefix is — and it compiles
		// to a literal that matches the empty channel and nothing else. Channel names
		// are validated by the caller (docs/06-channels.md §2), never here.
		{"empty grant matches the empty channel", "", "", true},
		{"empty grant matches nothing else", "", "x", false},

		// Unicode. Matching is over bytes of valid UTF-8, which is equivalent to
		// matching over runes for a needle that is itself valid UTF-8.
		{"unicode literal", "héllo", "héllo", true},
		{"unicode literal differs", "héllo", "hello", false},
		{"unicode prefix star", "héllo-*", "héllo-wörld", true},
		{"unicode suffix star", "*-wörld", "héllo-wörld", true},
		{"unicode middle", "h*d", "héllo-wörld", true},
		{"unicode case sensitivity", "É*", "é-x", false},
		{"star spans a multibyte rune", "a*b", "a→b", true},
		{"emoji literal", "room-🚪", "room-🚪", true},
		{"emoji under a star", "room-*", "room-🚪", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Compile(tt.grant)
			if err != nil {
				t.Fatalf("Compile(%q) = error %v, want no error", tt.grant, err)
			}
			if got := p.Match(tt.channel); got != tt.want {
				t.Errorf("Compile(%q).Match(%q) = %v, want %v", tt.grant, tt.channel, got, tt.want)
			}
		})
	}
}

// TestCompile_RejectsReservedPrefix_FR8 pins the refusal of `_` grants. The underscore
// prefix is reserved for control channels and must never be grantable, so the refusal
// happens at compile time rather than being left to the subscribe path where a future
// caller might forget it (docs/05-authorization.md §3, docs/06-channels.md §4).
func TestCompile_RejectsReservedPrefix_FR8(t *testing.T) {
	tests := []struct {
		name    string
		grant   string
		wantErr bool
	}{
		{"the control channel itself", "_control", true},
		{"a bare underscore", "_", true},
		{"underscore star", "_*", true},
		{"underscore prefix with a star", "_internal-*", true},
		{"underscore not at the start is fine", "room_4410", false},
		{"underscore after a star is fine", "*_control", false},
		{"a star grant is not itself reserved", "*", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Compile(tt.grant)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Compile(%q) = error %v, want no error", tt.grant, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Compile(%q) = no error, want ErrReservedPrefix", tt.grant)
			}
			if !errors.Is(err, ErrReservedPrefix) {
				t.Errorf("Compile(%q) error = %v, want errors.Is ErrReservedPrefix", tt.grant, err)
			}
			if !strings.Contains(err.Error(), tt.grant) {
				t.Errorf("Compile(%q) error = %q, want it to name the offending grant", tt.grant, err)
			}
			// The returned Pattern is the zero value, which matches nothing: a grant
			// that failed to compile must never accidentally match everything.
			if p.Match(tt.grant) || p.Match("") || p.Match("anything") {
				t.Errorf("Compile(%q) returned a Pattern that matches something", tt.grant)
			}
		})
	}
}

// TestPatternZeroValue_MatchesNothing pins the documented default: the zero Pattern
// matches nothing, so a Pattern that was never compiled cannot authorize a subscribe.
func TestPatternZeroValue_MatchesNothing(t *testing.T) {
	var p Pattern
	for _, channel := range []string{"", "*", "anything", "_control"} {
		if p.Match(channel) {
			t.Errorf("zero Pattern matched %q, want no match", channel)
		}
	}
}

// TestSetZeroValue_MatchesNothing pins the same default for Set, which is what a
// connection holds before its grants are installed.
func TestSetZeroValue_MatchesNothing(t *testing.T) {
	var s Set
	for _, channel := range []string{"", "*", "anything", "_control"} {
		if s.Match(channel) {
			t.Errorf("zero Set matched %q, want no match", channel)
		}
	}
}

// TestSetMatch_AnyPattern_FR8 covers the Set rule: a channel matches if ANY grant
// matches it (docs/05-authorization.md §3).
func TestSetMatch_AnyPattern_FR8(t *testing.T) {
	// The grant list from docs/05-authorization.md §1, verbatim.
	grants := []string{"room-4410", "user-7", "org-42-*"}

	tests := []struct {
		name    string
		channel string
		want    bool
	}{
		{"first grant", "room-4410", true},
		{"second grant", "user-7", true},
		{"third grant, star", "org-42-alerts", true},
		{"third grant, empty star", "org-42-", true},
		{"no grant covers it", "org-99-alerts", false},
		{"no grant covers a near miss", "room-44100", false},
		{"no grant covers another user", "user-8", false},
		{"empty channel", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSet(grants)
			if err != nil {
				t.Fatalf("NewSet(%v) = error %v, want no error", grants, err)
			}
			if got := s.Match(tt.channel); got != tt.want {
				t.Errorf("NewSet(%v).Match(%q) = %v, want %v", grants, tt.channel, got, tt.want)
			}
		})
	}
}

// TestNewSet_DuplicateGrants covers the documented allowance: an application may emit
// the same grant twice and matching is unaffected.
func TestNewSet_DuplicateGrants(t *testing.T) {
	s, err := NewSet([]string{"org-42-*", "org-42-*", "room-4410", "room-4410"})
	if err != nil {
		t.Fatalf("NewSet with duplicates = error %v, want no error", err)
	}
	if !s.Match("org-42-alerts") || !s.Match("room-4410") {
		t.Error("duplicate grants changed matching, want them to be inert")
	}
	if s.Match("org-99-alerts") {
		t.Error("duplicate grants widened matching")
	}
}

// TestNewSet_RejectsAnyBadGrant_FR8 pins that a bad grant fails the whole list rather
// than being skipped: a connection authorized against a partially-compiled grant list is
// a connection holding permissions nobody can account for.
func TestNewSet_RejectsAnyBadGrant_FR8(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{"reserved grant alone", []string{"_control"}},
		{"reserved grant first", []string{"_control", "room-4410"}},
		{"reserved grant last", []string{"room-4410", "_control"}},
		{"reserved grant in the middle", []string{"room-4410", "_internal-*", "org-42-*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSet(tt.patterns)
			if err == nil {
				t.Fatalf("NewSet(%v) = no error, want ErrReservedPrefix", tt.patterns)
			}
			if !errors.Is(err, ErrReservedPrefix) {
				t.Errorf("NewSet(%v) error = %v, want errors.Is ErrReservedPrefix", tt.patterns, err)
			}
			// The returned Set is the zero Set. A caller that ignores the error must
			// still not end up with a Set that authorizes the good grants.
			if s.Match("room-4410") || s.Match("org-42-alerts") || s.Match("anything") {
				t.Errorf("NewSet(%v) returned a Set that matches something", tt.patterns)
			}
		})
	}
}

// TestSetMatch_UnderscoreGuardIsTheCallers documents WHERE the `_` guard lives, because
// it lives in two different places for two different things and confusing them is how a
// control channel becomes reachable.
//
//   - A `_` GRANT is refused here, by Compile (docs/05-authorization.md §3).
//   - A `_` CHANNEL is refused by the subscribe path, before grants are consulted, so
//     that a grant of `*` still cannot reach `_control` (docs/06-channels.md §4).
//
// This package matches strings and knows nothing about what a channel means, so
// `*` genuinely does match `_control` here. That is correct and must stay correct: if
// this test ever starts failing because someone added a channel-side guard to glob, the
// guard is in the wrong package and the subscribe path may have stopped carrying it.
func TestSetMatch_UnderscoreGuardIsTheCallers(t *testing.T) {
	s, err := NewSet([]string{"*"})
	if err != nil {
		t.Fatalf(`NewSet(["*"]) = error %v, want no error`, err)
	}
	for _, channel := range []string{"_control", "_anything", "_"} {
		if !s.Match(channel) {
			t.Errorf("Set{\"*\"}.Match(%q) = false, want true: the `_` channel guard "+
				"belongs to the subscribe path (docs/06-channels.md §4), not to glob", channel)
		}
	}
}

// TestPatternMatch_Pathological asserts that adversarial grants and channels terminate
// with the right answer and without blowing up.
//
// The matcher is leftmost-greedy over the star-separated segments, which is exact for a
// language whose only metacharacter is `*`: taking the earliest occurrence of each
// segment leaves the longest possible remainder, so no backtracking is ever required.
// Worst case is O(len(channel) × len(pattern)) — one bounded scan per segment, and the
// segment lengths sum to at most len(pattern). There is no exponential case, which is
// the whole reason `regexp` is banned here: a backtracking matcher answers
// `a*a*a*a*a*a*a*a*a*b` against a run of `a`s in exponential time, and a grant list is
// attacker-adjacent input.
func TestPatternMatch_Pathological(t *testing.T) {
	tests := []struct {
		name    string
		grant   string
		channel string
		want    bool
	}{
		// The classic backtracking bomb. Anchoring the `b` suffix answers it in one
		// comparison; a naive backtracker would not.
		{"backtracking bomb", "a*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 20), false},
		{"backtracking bomb, satisfied", "a*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 20) + "b", true},
		{"many stars, no match", strings.Repeat("a*", 500) + "b", strings.Repeat("a", 5000), false},
		{"many stars, match", strings.Repeat("a*", 500) + "b", strings.Repeat("a", 5000) + "b", true},
		{"all stars", strings.Repeat("*", 1000), strings.Repeat("x", 10000), true},
		{"long literal, match", strings.Repeat("x", 10000), strings.Repeat("x", 10000), true},
		{"long literal, one char off", strings.Repeat("x", 10000), strings.Repeat("x", 9999) + "y", false},
		{"long prefix star", strings.Repeat("x", 10000) + "*", strings.Repeat("x", 20000), true},
		{"long suffix star", "*" + strings.Repeat("x", 10000), strings.Repeat("y", 10000) + strings.Repeat("x", 10000), true},
		// 100 `a` segments cannot be satisfied by a one-character channel: `*` matches
		// an empty run, but each literal segment still has to be there.
		{"pattern longer than the channel", strings.Repeat("a*", 100), "a", false},
		{"pattern longer than the channel, anchored", strings.Repeat("ab*", 100), "ab", false},
		{"many stars around one literal", strings.Repeat("*", 100) + "a" + strings.Repeat("*", 100), "xax", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Compile(tt.grant)
			if err != nil {
				t.Fatalf("Compile(len %d grant) = error %v, want no error", len(tt.grant), err)
			}
			if got := p.Match(tt.channel); got != tt.want {
				t.Errorf("Compile(len %d).Match(len %d) = %v, want %v",
					len(tt.grant), len(tt.channel), got, tt.want)
			}
		})
	}
}

// TestPatternMatch_AgreesWithReference cross-checks the compiled matcher against an
// obviously-correct exponential reference implementation over a small alphabet. A
// hand-written table locks in the cases I thought of; this locks in the ones I did not.
func TestPatternMatch_AgreesWithReference(t *testing.T) {
	alphabet := []string{"", "a", "b", "*", "ab", "a*", "*a", "**", "a*b", "*a*", "aab", "a*a", "ba*"}

	for _, grant := range alphabet {
		for _, channel := range alphabet {
			p, err := Compile(grant)
			if err != nil {
				t.Fatalf("Compile(%q) = error %v, want no error", grant, err)
			}
			want := referenceMatch(grant, channel)
			if got := p.Match(channel); got != want {
				t.Errorf("Compile(%q).Match(%q) = %v, want %v (reference)", grant, channel, got, want)
			}
		}
	}
}

// referenceMatch is a deliberately naive, obviously-correct glob matcher used only to
// cross-check the compiled one. It recurses and it is exponential; that is fine for the
// tiny inputs above and is exactly why the real matcher does not work this way.
func referenceMatch(pattern, s string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern[0] == '*' {
		// `*` matches any run of characters including none.
		for i := 0; i <= len(s); i++ {
			if referenceMatch(pattern[1:], s[i:]) {
				return true
			}
		}
		return false
	}
	return s != "" && s[0] == pattern[0] && referenceMatch(pattern[1:], s[1:])
}

// TestSetMatch_Concurrent_FR9 is FR-9's second acceptance criterion: many goroutines
// match one Set while another goroutine swaps the whole Set through an atomic pointer,
// with no race reported. This is the shape a connection actually uses — grant narrowing
// and revocation build a new Set and swap, never writing through the old one — and it is
// the defect docs/13-review-findings.md M2 describes, which code review did not catch.
func TestSetMatch_Concurrent_FR9(t *testing.T) {
	wide, err := NewSet([]string{"org-42-*", "room-4410"})
	if err != nil {
		t.Fatalf("NewSet = error %v, want no error", err)
	}
	narrow, err := NewSet([]string{"room-4410"})
	if err != nil {
		t.Fatalf("NewSet = error %v, want no error", err)
	}

	var current atomic.Pointer[Set]
	current.Store(&wide)

	const (
		matchers   = 16
		iterations = 5000
	)

	var wg sync.WaitGroup
	// Readers. `room-4410` is in both sets, so its answer is invariant under the swap
	// and any false here is a torn read rather than a legitimate narrowing.
	for range matchers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				s := current.Load()
				if !s.Match("room-4410") {
					t.Error("Match(room-4410) = false during a swap, want true from either set")
					return
				}
				// Answers that legitimately change across the swap are exercised for
				// the race detector's benefit, not asserted on.
				_ = s.Match("org-42-alerts")
			}
		}()
	}
	// The writer, swapping the grant set under the readers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			if i%2 == 0 {
				current.Store(&narrow)
			} else {
				current.Store(&wide)
			}
		}
	}()
	wg.Wait()
}

// TestPatternMatch_ConcurrentSharedPattern covers the other half of the immutability
// contract: one compiled Pattern, shared by value and by reference across goroutines, is
// safe to match on with no locking because nothing ever writes to it after Compile.
func TestPatternMatch_ConcurrentSharedPattern(t *testing.T) {
	p, err := Compile("a*b*c")
	if err != nil {
		t.Fatalf("Compile = error %v, want no error", err)
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := p // a Pattern is a value type; copying it must not copy a lock
			for range 2000 {
				if !local.Match("aXbYc") || p.Match("cba") {
					t.Error("shared Pattern gave the wrong answer under concurrency")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestMatch_ZeroAllocations_FR9 is FR-9's first acceptance criterion, asserted as a test
// rather than only as a benchmark so that `go test` fails on a regression instead of a
// benchmark nobody runs. Matching must perform no I/O, take no mutex and allocate
// nothing; an allocation here means a matcher that builds something per call, which is
// the `regexp`/`path.Match` failure mode this package exists to avoid.
//
// It holds under -race as well as without, and is meant to: `go test -race` is the only
// invocation this repository uses (docs/14-coding-standards.md §2), so an assertion that
// had to be skipped under the detector would never run in CI at all.
func TestMatch_ZeroAllocations_FR9(t *testing.T) {
	set, err := NewSet([]string{"room-4410", "user-7", "org-42-*", "*-alerts", "a*b*c"})
	if err != nil {
		t.Fatalf("NewSet = error %v, want no error", err)
	}

	patterns := []struct {
		name  string
		grant string
	}{
		{"literal", "room-4410"},
		{"prefix", "org-42-*"},
		{"suffix", "*-alerts"},
		{"any", "*"},
		{"segments", "a*b*c"},
	}

	channels := []string{"room-4410", "org-42-alerts", "aXbYc", "", "nothing-matches-this"}

	for _, pt := range patterns {
		t.Run("Pattern/"+pt.name, func(t *testing.T) {
			p, err := Compile(pt.grant)
			if err != nil {
				t.Fatalf("Compile(%q) = error %v, want no error", pt.grant, err)
			}
			var sink bool
			got := testing.AllocsPerRun(100, func() {
				for _, c := range channels {
					sink = p.Match(c) || sink
				}
			})
			if got != 0 {
				t.Errorf("Compile(%q).Match allocated %v times per run, want 0 (FR-9)", pt.grant, got)
			}
		})
	}

	t.Run("Set", func(t *testing.T) {
		var sink bool
		got := testing.AllocsPerRun(100, func() {
			for _, c := range channels {
				sink = set.Match(c) || sink
			}
		})
		if got != 0 {
			t.Errorf("Set.Match allocated %v times per run, want 0 (FR-9)", got)
		}
	})
}

// BenchmarkPatternMatch reports the per-match cost and allocation count for each
// compiled form (FR-9).
func BenchmarkPatternMatch(b *testing.B) {
	benchmarks := []struct {
		name    string
		grant   string
		channel string
	}{
		{"literal", "room-4410", "room-4410"},
		{"literal_miss", "room-4410", "room-44100"},
		{"prefix", "org-42-*", "org-42-alerts"},
		{"suffix", "*-alerts", "org-42-alerts"},
		{"contains", "*-42-*", "org-42-alerts"},
		{"any", "*", "org-42-alerts"},
		{"segments", "a*b*c", "aXXXXbYYYYc"},
		{"segments_pathological", "a*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 20)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			p, err := Compile(bm.grant)
			if err != nil {
				b.Fatalf("Compile(%q) = error %v, want no error", bm.grant, err)
			}
			var sink bool
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sink = p.Match(bm.channel)
			}
			_ = sink
		})
	}
}

// BenchmarkSetMatch reports the cost of the operation a subscribe actually performs: one
// channel against a whole grant list (FR-9).
func BenchmarkSetMatch(b *testing.B) {
	sizes := []int{1, 4, 16, 64}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("grants_%d", n), func(b *testing.B) {
			grants := make([]string, 0, n)
			for i := range n {
				grants = append(grants, fmt.Sprintf("org-%d-*", i))
			}
			s, err := NewSet(grants)
			if err != nil {
				b.Fatalf("NewSet = error %v, want no error", err)
			}
			var sink bool
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				// The worst case for a Set: no grant matches, so every one is tried.
				sink = s.Match("room-4410")
			}
			_ = sink
		})
	}
}
