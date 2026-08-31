package main

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/metrics"
)

// now is the fixed clock the control tests measure the ±300s window against. A fake clock
// is what makes the boundary assertable exactly, instead of by sleeping through it
// (docs/14-coding-standards.md §2).
var controlNow = time.Unix(1756612800, 0)

// TestVerifyControl_Accepts is the happy path of docs/04-integration.md §3: the action
// travels as a JSON string and the signature covers those exact bytes.
func TestVerifyControl_Accepts(t *testing.T) {
	body := `{"action":"disconnect","user":"u-7","reason":"suspended"}`
	got, reason, err := verifyControl([]byte(testControlSecret), controlNow,
		signControl(testControlSecret, controlNow, body))
	if err != nil {
		t.Fatalf("verifyControl = %v", err)
	}
	if reason != "" {
		t.Fatalf("verifyControl reason = %q on an accepted message, want empty", reason)
	}
	want := hub.Control{Action: "disconnect", User: "u-7", Reason: "suspended"}
	if got != want {
		t.Fatalf("verifyControl = %+v, want %+v", got, want)
	}
}

// TestVerifyControl_Rejections is the table FR-23 turns on. Each row names the reason
// st_control_rejected_total counts it under, because "unsigned", "stale" and "malformed"
// send an operator to three different places.
func TestVerifyControl_Rejections(t *testing.T) {
	body := `{"action":"disconnect","user":"u-7"}`
	valid := signControl(testControlSecret, controlNow, body)

	tampered := func() []byte {
		var env controlEnvelope
		if err := json.Unmarshal(valid, &env); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		env.Body = `{"action":"disconnect","user":"u-8"}`
		out, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return out
	}()

	tests := []struct {
		name    string
		secret  string
		now     time.Time
		payload []byte
		want    metrics.ControlReason
		names   string
	}{
		{"not an object", testControlSecret, controlNow, []byte("["), metrics.ControlMalformed, "JSON object"},
		{"signature is not hex", testControlSecret, controlNow,
			signedWith(t, "zz"), metrics.ControlUnsigned, "hexadecimal"},
		{"wrong secret", "another-secret-long-enough-to-be-legal-at-32-bytes", controlNow, valid,
			metrics.ControlUnsigned, "signature"},
		{"body swapped under the signature", testControlSecret, controlNow, tampered,
			metrics.ControlUnsigned, "signature"},
		{"too old", testControlSecret, controlNow.Add(controlSkew + time.Second), valid,
			metrics.ControlStale, "window"},
		{"too far in the future", testControlSecret, controlNow.Add(-controlSkew - time.Second), valid,
			metrics.ControlStale, "window"},
		{"signed but no target", testControlSecret, controlNow,
			signControl(testControlSecret, controlNow, `{"action":"disconnect"}`),
			metrics.ControlMalformed, "target"},
		{"signed but not JSON", testControlSecret, controlNow,
			signControl(testControlSecret, controlNow, `nope`),
			metrics.ControlMalformed, "JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, reason, err := verifyControl([]byte(tt.secret), tt.now, tt.payload)
			if err == nil {
				t.Fatalf("verifyControl accepted %s", tt.name)
			}
			if reason != tt.want {
				t.Fatalf("reason = %q, want %q", reason, tt.want)
			}
			if cmd != (hub.Control{}) {
				t.Fatalf("verifyControl returned %+v on a rejection, want the zero Control", cmd)
			}
			if !strings.Contains(err.Error(), tt.names) {
				t.Fatalf("error = %q, want it to mention %q", err, tt.names)
			}
			if strings.Contains(err.Error(), tt.secret) {
				t.Fatal("a control rejection quoted control.secret (NFR-7)")
			}
		})
	}
}

// TestVerifyControl_WindowBoundary pins both edges of the ±300s replay window exactly, so
// that a change to the constant is a failing test rather than a silent widening.
func TestVerifyControl_WindowBoundary(t *testing.T) {
	body := `{"action":"refresh","user":"u-7"}`
	payload := signControl(testControlSecret, controlNow, body)

	for _, at := range []time.Time{controlNow.Add(controlSkew), controlNow.Add(-controlSkew)} {
		if _, _, err := verifyControl([]byte(testControlSecret), at, payload); err != nil {
			t.Fatalf("verifyControl at the window edge %s = %v, want accepted", at, err)
		}
	}
}

// signedWith builds an envelope carrying an arbitrary sig, for the rows where the
// signature itself is malformed rather than merely wrong.
func signedWith(t *testing.T, sig string) []byte {
	t.Helper()
	out, err := json.Marshal(controlEnvelope{
		TS:    controlNow.Unix(),
		Nonce: "nonce-1",
		Body:  `{"action":"disconnect","user":"u-7"}`,
		Sig:   sig,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := hex.DecodeString(sig); err == nil {
		t.Fatalf("signedWith(%q) is valid hex; the row needs one that is not", sig)
	}
	return out
}
