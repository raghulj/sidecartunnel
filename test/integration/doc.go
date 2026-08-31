// Package integration holds the integration and failure layers of docs/11-testing.md
// §4 and §5: two gateway replicas, one real Redis, and real websocket clients.
//
// It has no non-test source of its own. This file exists so the directory is a package
// `go list ./...` can see — a package that vanishes from a coverage report is the exact
// failure the report exists to prevent (scripts/cover.sh) — and so the suite's own
// documentation has somewhere to live next to it.
//
// The tests are in package integration_test and are described in README.md.
package integration
