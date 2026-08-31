package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// Process exit codes. There are two, and the second one means the same thing everywhere:
// the gateway did not do what it was asked. A configuration that does not validate, a
// port that will not bind and a drain that ran out of budget are all one status, because
// nothing downstream — a container runtime, a healthcheck, a shell — distinguishes them.
const (
	exitOK      = 0
	exitFailure = 1
)

// usage is printed for -h and for an unknown subcommand. It is the doc comment on this
// package in short form; the two must not disagree.
const usage = `sidecartunnel — an application-agnostic websocket gateway.

Usage:
  sidecartunnel [flags]                run the gateway
  sidecartunnel healthcheck [flags]    loopback GET /health against admin.listen, exit 0 or 1

Configuration is a YAML file, overridden by ST_-prefixed environment variables with __
for nesting (ST_SERVER__PATH=/socket). See docs/08-config.md.

Flags:
`

// run is the whole binary: flag parsing, subcommand dispatch, and the gateway's
// lifecycle. It returns the process exit status.
//
// It takes its arguments, its output streams and its signal channel rather than reaching
// for os.Args, os.Stdout and signal.Notify, so that every path through it is exercised by
// a test rather than by starting a process (docs/14-coding-standards.md §3). main does
// nothing but supply the real ones.
//
// The environment is deliberately not a parameter: internal/config reads it with
// os.LookupEnv, because ST_..._FILE has to resolve a path at load time, and a second
// environment threaded through here would be one the loader ignores.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) int {
	fs := flag.NewFlagSet("sidecartunnel", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	configPath := fs.String("config", "", "path to the configuration file; omit to configure from the environment alone")
	showVersion := fs.Bool("version", false, "print the version, commit and build date, then exit")

	command, err := parseArgs(fs, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitFailure
	}

	if *showVersion {
		// The three symbols are injected with -X at link time. The linker silently
		// ignores an -X for a symbol it cannot find, so a release built without them
		// succeeds and reports nothing; this is the check that catches it
		// (docs/15-releasing.md §5).
		fmt.Fprintf(stdout, "sidecartunnel %s\ncommit: %s\nbuilt:  %s\n", version, commit, date)
		return exitOK
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		// NFR-5: the message names the offending key, and the process does not start
		// partially configured. It goes to stderr rather than through the logger,
		// because log.level and log.format are part of what failed to validate.
		fmt.Fprintf(stderr, "FATAL %v\n", err)
		return exitFailure
	}

	log, err := newLogger(cfg.Log, stderr)
	if err != nil {
		// coverage: config.Validate has already rejected every level and format this
		// rejects. The check stays because newLogger is the only thing that knows the
		// mapping, and a silent fallback to info is a gateway whose logs an operator
		// reads as evidence of quiet.
		fmt.Fprintf(stderr, "FATAL %v\n", err)
		return exitFailure
	}

	if command == commandHealthcheck {
		return healthcheck(ctx, cfg, stderr)
	}

	g, err := build(ctx, cfg, prometheus.NewRegistry(), log)
	if err != nil {
		log.Error("startup failed", "err", err)
		return exitFailure
	}
	defer g.close()
	return g.serve(ctx, signals)
}

// The subcommands docs/08-config.md §1 defines. The empty name is the gateway itself.
const (
	commandServe       = ""
	commandHealthcheck = "healthcheck"
)

// parseArgs parses the flags and returns the subcommand, accepting it on either side of
// them.
//
// Both orders have to work. `sidecartunnel --config /etc/st.yaml healthcheck` is what a
// person types, and `sidecartunnel healthcheck --config /etc/st.yaml` is what a compose
// file's healthcheck ends up as once somebody adds a config file to a deployment that
// started env-only. Go's flag package stops at the first non-flag argument, so the second
// form needs the remainder parsed again.
func parseArgs(fs *flag.FlagSet, args []string) (string, error) {
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return commandServe, nil
	}
	if rest[0] != commandHealthcheck {
		fmt.Fprintf(fs.Output(), "sidecartunnel: unknown subcommand %q\n", rest[0])
		fs.Usage()
		return "", fmt.Errorf("unknown subcommand %q", rest[0])
	}
	if err := fs.Parse(rest[1:]); err != nil {
		return "", err
	}
	if extra := fs.Args(); len(extra) > 0 {
		fmt.Fprintf(fs.Output(), "sidecartunnel: healthcheck takes no arguments, got %q\n", extra[0])
		fs.Usage()
		return "", fmt.Errorf("healthcheck takes no arguments, got %q", extra[0])
	}
	return commandHealthcheck, nil
}
