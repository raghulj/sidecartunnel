// Package config owns the configuration surface: the struct tree, the built-in defaults,
// the YAML and environment loaders, and the validation rules from docs/08-config.md.
//
// docs/08-config.md is normative. A key that exists here and not there is a bug, in that
// document or in this package, and it is resolved by editing one of them in the same
// commit — never by leaving both standing.
//
// What this package must never do:
//
//   - Default anything silently. NFR-5: invalid configuration fails startup with a
//     message naming the offending key, and there is no partially-configured start. The
//     rule exists because the alternative is a gateway that starts cleanly, reports
//     healthy, and refuses every subscribe — which is exactly what an earlier
//     environment-only example produced (docs/13-review-findings.md M11).
//   - Default server.allowed_origins to anything. ["*"] is a security hole shipped as a
//     convenience and [] that silently accepts everything is worse. Refusing to start is
//     the only honest option (docs/05-authorization.md §5).
//   - Accept a key it does not know. Decoding is strict: an unrecognised key is a startup
//     error naming it. That is what turns a typo, or a key removed by a design decision
//     such as the cut auth_required, into a loud failure instead of a setting that
//     quietly does nothing.
//   - Log a value. This package handles app.webhook_secrets and control.secret; neither
//     may reach a log line, an error message, or a String method at any level (NFR-7).
//   - Become a live-reload mechanism. Configuration is read once at startup. A key that
//     can change under a running connection is a key whose two readers can disagree.
package config
