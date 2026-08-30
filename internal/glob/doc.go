// Package glob compiles and matches channel grants.
//
// A grant is a string from the application's connect-webhook response. `*` matches any
// run of characters, including none, and it is the only metacharacter: `?`, `[...]`,
// `{...}` and escaping are not supported and MUST NOT be added. Every addition is a new
// way for a grant to match more than its author intended, and the author is an
// application developer writing a list, not a regex. Matching is case-sensitive.
// docs/05-authorization.md §3.
//
// The package exists as its own thing for one reason: FR-9 requires matching to perform
// no I/O, allocate nothing, and take no mutex, so patterns are compiled once at connect
// and a compiled Set is immutable and safe to publish through an atomic pointer. Keeping
// that property is the package's whole job.
//
// What this package must never do:
//
//   - Use regexp or path/filepath. The former allocates on every match; the latter
//     supports metacharacters this design deliberately excludes.
//   - Allocate on the match path. FR-9's acceptance criterion is a benchmark showing zero
//     allocations per match, and a -race test that swaps the set from one goroutine while
//     another matches continuously.
//   - Mutate a compiled Pattern or Set. Revocation and grant narrowing swap the pointer;
//     they never write through it. The earlier design guarded grants with a mutex, which
//     meant matching either took a lock (violating FR-9) or read a slice header while it
//     was being written — a torn read yielding a new pointer with an old length, i.e. a
//     subscribe authorized against grants revoked seconds earlier
//     (docs/13-review-findings.md M2).
//   - Know what a channel means. It matches strings. Namespace resolution and channel
//     length limits are enforced by the caller, before or after, never here.
//
// The "_" prefix rule is split, and the split is deliberate. A _grant_ beginning "_" is
// rejected by Compile, because a reserved-channel grant can never be legitimate and the
// cheapest place to refuse it is where grants are built (docs/05-authorization.md §3). A
// _channel_ beginning "_" is refused by the subscribe path before grants are consulted
// (docs/06-channels.md §4), so a misconfigured grant of "*" still cannot reach a control
// channel. Set.Match("_control") therefore returns true for a "*" grant, and
// TestSetMatch_UnderscoreGuardIsTheCallers asserts exactly that: if it ever fails, the
// guard has been moved into the wrong package.
package glob
