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

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

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

// signalBuffer is the depth of the signal channel. It is two because a second SIGTERM
// abandons the drain, and an unbuffered channel drops a signal that arrives while the
// drain is being set up — which is the one signal the operator sending it most means.
const signalBuffer = 2

func main() {
	// coverage: main is the process boundary and nothing else. It installs the real
	// signal handler and turns run's exit code into a process status; every decision
	// lives in run, which is called directly by the tests
	// (docs/14-coding-standards.md §3).
	signals := make(chan os.Signal, signalBuffer)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, signals))
}
