// Command sidecartunnel is an application-agnostic websocket gateway.
//
// It parses flags, loads and validates configuration, wires the packages under internal/
// together, and handles signals. It holds no logic of its own — main is where the object
// graph is built and nowhere else, so that every package below it can be constructed with
// explicit dependencies in a test.
//
// Subcommands:
//
//	sidecartunnel                    run the gateway
//	sidecartunnel healthcheck        loopback GET /health against admin.listen, exit 0 or 1
//
// healthcheck exists so a distroless image with no shell and no curl can still declare a
// container healthcheck. It checks liveness only, never the bus — a bus-dependent
// healthcheck kills the whole fleet during a Redis restart
// (docs/04-integration.md §4, docs/08-config.md §1).
package main

// Build information, injected at link time with -X (see .goreleaser.yaml and Dockerfile).
//
// These must exist as package-level vars even though nothing sets them in a plain `go
// build`: the Go linker silently ignores -X for a symbol it cannot find, so a release
// built without them succeeds and produces a binary that reports no version at all. The
// failure is invisible until someone asks a running container what it is.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// coverage: referenced so the linker keeps the symbols and -X has something to write
	// to. Replaced by real flag handling when the wiring lands.
	_, _, _ = version, commit, date

	// coverage: contract stub. Wiring lands with the first milestone; there is nothing
	// here to test yet, and a test that asserted a panic would have to be deleted the day
	// it is implemented.
	panic("not implemented")
}
