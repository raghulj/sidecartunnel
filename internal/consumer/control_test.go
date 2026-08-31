package consumer

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/hub"
)

// TestVerify_Accepts is the happy path of docs/04-integration.md §3: the action travels
// as a JSON string and the signature covers those exact bytes.
func TestVerify_Accepts(t *testing.T) {
	body := `{"action":"disconnect","user":"u-7","reason":"suspended"}`
	got, reason, err := Verify([]byte(testSecret), testNow, sign(testSecret, testNow, body))
	if err != nil {
		t.Fatalf("Verify = %v", err)
	}
	if reason != "" {
		t.Fatalf("Verify reason = %q on an accepted message, want empty", reason)
	}
	want := hub.Control{Action: "disconnect", User: "u-7", Reason: "suspended"}
	if got != want {
		t.Fatalf("Verify = %+v, want %+v", got, want)
	}
}

// TestVerify_Rejections is the table FR-23 turns on. Each row names the reason the
// rejection is logged under, because "unsigned", "stale" and "malformed" send an operator
// to three different places (docs/10-operations.md §7).
//
// The rows where the body is valid JSON but not an object are the ones that must not
// panic: json.Unmarshal into a struct reports them, and nothing downstream may assume the
// document was an object because the signature verified.
func TestVerify_Rejections(t *testing.T) {
	body := `{"action":"disconnect","user":"u-7"}`
	valid := sign(testSecret, testNow, body)

	tampered := func() []byte {
		var env Envelope
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
		want    Reason
		names   string
	}{
		{"envelope is not an object", testSecret, testNow, []byte("["), ReasonMalformed, "JSON object"},
		{"signature is not hex", testSecret, testNow, signedWith(t, "zz"), ReasonUnsigned, "hexadecimal"},
		{"wrong secret", "another-secret-long-enough-to-be-legal-at-32-bytes", testNow, valid,
			ReasonUnsigned, "signature"},
		{"body swapped under the signature", testSecret, testNow, tampered, ReasonUnsigned, "signature"},
		{"too old", testSecret, testNow.Add(Skew + time.Second), valid, ReasonStale, "window"},
		{"too far in the future", testSecret, testNow.Add(-Skew - time.Second), valid, ReasonStale, "window"},
		{"signed but no target", testSecret, testNow,
			sign(testSecret, testNow, `{"action":"disconnect"}`), ReasonMalformed, "target"},
		{"signed but not JSON", testSecret, testNow,
			sign(testSecret, testNow, `nope`), ReasonMalformed, "JSON object"},
		{"body is a JSON array", testSecret, testNow,
			sign(testSecret, testNow, `[{"action":"disconnect","user":"u-7"}]`), ReasonMalformed, "JSON object"},
		{"body is a JSON string", testSecret, testNow,
			sign(testSecret, testNow, `"disconnect"`), ReasonMalformed, "JSON object"},
		{"body is JSON null", testSecret, testNow,
			sign(testSecret, testNow, `null`), ReasonMalformed, "action"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, reason, err := Verify([]byte(tt.secret), tt.now, tt.payload)
			if err == nil {
				t.Fatalf("Verify accepted %s", tt.name)
			}
			if !errors.Is(err, ErrRejected) {
				t.Fatalf("error = %v, want it to wrap ErrRejected", err)
			}
			if reason != tt.want {
				t.Fatalf("reason = %q, want %q", reason, tt.want)
			}
			if cmd != (hub.Control{}) {
				t.Fatalf("Verify returned %+v on a rejection, want the zero Control", cmd)
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

// TestVerify_WindowBoundary pins both edges of the ±300s replay window exactly, so that a
// change to the constant is a failing test rather than a silent widening.
func TestVerify_WindowBoundary(t *testing.T) {
	payload := sign(testSecret, testNow, `{"action":"refresh","user":"u-7"}`)

	for _, at := range []time.Time{testNow.Add(Skew), testNow.Add(-Skew)} {
		if _, _, err := Verify([]byte(testSecret), at, payload); err != nil {
			t.Fatalf("Verify at the window edge %s = %v, want accepted", at, err)
		}
	}
}

// TestSkew_IsTheDocumentedWindow keeps the constant and docs/04-integration.md §3
// together: ±300s is the connect webhook's own window, and the two are one number.
func TestSkew_IsTheDocumentedWindow(t *testing.T) {
	if Skew != 300*time.Second {
		t.Fatalf("Skew = %s, want 300s (docs/04-integration.md §3)", Skew)
	}
}

// signedWith builds an envelope carrying an arbitrary sig, for the rows where the
// signature itself is malformed rather than merely wrong.
func signedWith(t *testing.T, sig string) []byte {
	t.Helper()
	out, err := json.Marshal(Envelope{
		TS:    testNow.Unix(),
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
