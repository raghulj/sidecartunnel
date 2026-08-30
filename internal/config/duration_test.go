package config

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestDuration_UnmarshalYAML covers every duration spelling docs/08-config.md §3 uses —
// "500ms" on bus.reconnect_min, "30s" on bus.ready_grace, "6h" on app.max_expiry — plus
// the failures, which must name the offending value rather than yielding a silent zero
// (NFR-5).
func TestDuration_UnmarshalYAML(t *testing.T) {
	type holder struct {
		D Duration `yaml:"d"`
	}

	tests := []struct {
		name    string
		yaml    string
		want    time.Duration
		wantErr string
	}{
		{name: "seconds", yaml: "d: 30s", want: 30 * time.Second},
		{name: "hours", yaml: "d: 6h", want: 6 * time.Hour},
		{name: "milliseconds", yaml: "d: 500ms", want: 500 * time.Millisecond},
		{name: "quoted", yaml: `d: "25s"`, want: 25 * time.Second},
		{name: "compound", yaml: "d: 1h30m", want: 90 * time.Minute},
		{name: "zero", yaml: "d: 0s", want: 0},
		{name: "negative", yaml: "d: -1s", want: -time.Second},
		{name: "no unit", yaml: "d: 30", wantErr: "30"},
		{name: "nonsense", yaml: "d: banana", wantErr: "banana"},
		{name: "empty", yaml: `d: ""`, wantErr: "invalid duration"},
		{name: "not a scalar", yaml: "d: [1, 2]", wantErr: "duration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h holder
			err := yaml.Unmarshal([]byte(tt.yaml), &h)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Unmarshal(%q) = nil error, want error containing %q", tt.yaml, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Unmarshal(%q) error = %q, want it to contain %q", tt.yaml, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%q) = %v, want no error", tt.yaml, err)
			}
			if h.D.Duration() != tt.want {
				t.Fatalf("Unmarshal(%q) = %v, want %v", tt.yaml, h.D.Duration(), tt.want)
			}
		})
	}
}

// TestDuration_RoundTrip asserts a dumped config comes back as "25s" rather than a
// nanosecond count, which is the whole reason MarshalYAML exists.
func TestDuration_RoundTrip(t *testing.T) {
	type holder struct {
		D Duration `yaml:"d"`
	}

	for _, want := range []string{"30s", "6h0m0s", "500ms", "1h30m0s"} {
		t.Run(want, func(t *testing.T) {
			parsed, err := time.ParseDuration(want)
			if err != nil {
				t.Fatalf("ParseDuration(%q) = %v", want, err)
			}
			out, err := yaml.Marshal(holder{D: Duration(parsed)})
			if err != nil {
				t.Fatalf("Marshal = %v", err)
			}
			if !strings.Contains(string(out), want) {
				t.Fatalf("Marshal = %q, want it to contain %q", out, want)
			}
			var back holder
			if err := yaml.Unmarshal(out, &back); err != nil {
				t.Fatalf("Unmarshal(%q) = %v", out, err)
			}
			if back.D != Duration(parsed) {
				t.Fatalf("round trip = %v, want %v", back.D, parsed)
			}
		})
	}
}

// TestDuration_String covers the String method independently of YAML, since it is the
// form that reaches an operator in a dumped config.
func TestDuration_String(t *testing.T) {
	if got := Duration(25 * time.Second).String(); got != "25s" {
		t.Fatalf("String() = %q, want %q", got, "25s")
	}
}
