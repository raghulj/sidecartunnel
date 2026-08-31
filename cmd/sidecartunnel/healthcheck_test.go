package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestHealthURL covers the address forms admin.listen legitimately takes. A wildcard host
// is not an address to connect to, and the probe is always loopback: it runs inside the
// container it is checking, and the admin listener defaults to loopback precisely so
// nothing outside can reach it.
func TestHealthURL(t *testing.T) {
	tests := []struct {
		name   string
		listen string
		want   string
		errs   bool
	}{
		{"loopback", "127.0.0.1:9001", "http://127.0.0.1:9001/health", false},
		{"every interface, short form", ":9001", "http://127.0.0.1:9001/health", false},
		{"every interface, long form", "0.0.0.0:9001", "http://127.0.0.1:9001/health", false},
		{"every interface, v6", "[::]:9001", "http://127.0.0.1:9001/health", false},
		{"a named host", "sidecartunnel:9001", "http://sidecartunnel:9001/health", false},
		{"no port at all", "9001", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := healthURL(tt.listen)
			if tt.errs {
				if err == nil {
					t.Fatalf("healthURL(%q) = %q, nil; want an error naming admin.listen", tt.listen, got)
				}
				if !strings.Contains(err.Error(), "admin.listen") {
					t.Fatalf("healthURL error = %q, want it to name admin.listen (NFR-5)", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("healthURL(%q) = %v", tt.listen, err)
			}
			if got != tt.want {
				t.Fatalf("healthURL(%q) = %q, want %q", tt.listen, got, tt.want)
			}
		})
	}
}

// TestHealthcheck_Answers walks the outcomes a container runtime acts on.
func TestHealthcheck_Answers(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			// FR-20 and docs/04-integration.md §4: a healthcheck that consulted /ready
			// would kill every replica at once during a Redis restart.
			t.Errorf("healthcheck probed %q, want /health only", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	unwell := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unwell.Close()

	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	dead.Close()

	tests := []struct {
		name   string
		listen string
		want   int
		says   string
	}{
		{"a running instance", strings.TrimPrefix(ok.URL, "http://"), exitOK, ""},
		{"an instance answering 503", strings.TrimPrefix(unwell.URL, "http://"), exitFailure, "503"},
		{"nothing listening", deadAddr, exitFailure, "connect"},
		{"an unusable admin.listen", "9001", exitFailure, "admin.listen"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			cfg := &config.Config{Admin: config.Admin{Listen: tt.listen}}
			if got := healthcheck(context.Background(), cfg, &stderr); got != tt.want {
				t.Fatalf("healthcheck = %d, want %d (stderr %q)", got, tt.want, stderr.String())
			}
			if tt.says != "" && !strings.Contains(stderr.String(), tt.says) {
				t.Fatalf("stderr = %q, want it to mention %q", stderr.String(), tt.says)
			}
		})
	}
}

// TestHealthcheck_AgainstARunningGateway is the subcommand doing its actual job: a
// loopback probe of a live admin listener, which is what the distroless image's
// HEALTHCHECK runs (docs/10-operations.md §3).
func TestHealthcheck_AgainstARunningGateway(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	f.start(t)

	f.cfg.Admin.Listen = f.adminLn.Addr().String()
	var stderr bytes.Buffer
	if got := healthcheck(context.Background(), f.cfg, &stderr); got != exitOK {
		t.Fatalf("healthcheck against a running gateway = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
}

// TestHealthcheck_NeverConsultsTheBus_FR20 is the rule the whole subcommand exists to
// keep. The bus is torn out from under a live gateway and /health still answers 200, so
// a Redis restart cannot restart the fleet.
func TestHealthcheck_NeverConsultsTheBus_FR20(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	f.start(t)
	f.cfg.Admin.Listen = f.adminLn.Addr().String()

	// Draining is the strongest not-ready this process can report; /health is indifferent.
	f.readiness.drain()

	var stderr bytes.Buffer
	if got := healthcheck(context.Background(), f.cfg, &stderr); got != exitOK {
		t.Fatalf("healthcheck = %d while the replica is not ready, want %d — liveness is not readiness (FR-20)",
			got, exitOK)
	}
}
