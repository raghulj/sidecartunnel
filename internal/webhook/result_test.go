package webhook

import (
	"errors"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestResult_DispositionsDiffer_FR6 is the requirement stated as a type.
//
// A 401 means this user may not connect and the client must stop asking; a 500 means I
// could not tell you right now and the client must come back. Collapsing them either
// locks every user out during an application deploy or turns a revocation into an
// infinite retry loop against the endpoint. The two MUST NOT share a close code
// (FR-6, docs/04-integration.md §1.3).
func TestResult_DispositionsDiffer_FR6(t *testing.T) {
	refused := Refused{Status: 401}
	unavailable := Unavailable{Status: 500}

	if refused.CloseCode() != proto.CloseUnauthorized {
		t.Errorf("Refused.CloseCode() = %d, want %d", refused.CloseCode(), proto.CloseUnauthorized)
	}
	if unavailable.CloseCode() != proto.CloseAuthUnavailable {
		t.Errorf("Unavailable.CloseCode() = %d, want %d", unavailable.CloseCode(), proto.CloseAuthUnavailable)
	}
	if refused.CloseCode() == unavailable.CloseCode() {
		t.Fatal("a refusal and a failure share a close code (FR-6)")
	}
	if refused.Reconnect() {
		t.Error("Refused.Reconnect() = true; a decision was made and retrying cannot change it")
	}
	if !unavailable.Reconnect() {
		t.Error("Unavailable.Reconnect() = false; a transient failure must be retryable")
	}
}

// TestResult_TypeSwitchIsExhaustive documents the caller's contract: a Result is one of
// exactly three concrete types, and the type switch a caller writes has no other case.
func TestResult_TypeSwitchIsExhaustive(t *testing.T) {
	results := []Result{
		Authorized{User: "u-7", Grants: glob.Set{}, ExpiresIn: time.Hour},
		Refused{Status: 403},
		Unavailable{Status: 503},
	}
	for _, r := range results {
		switch v := r.(type) {
		case Authorized, Refused, Unavailable:
			r.result() // the marker; unexported, so no other package can add a fourth.
		default:
			t.Fatalf("unexpected Result type %T", v)
		}
	}
}

// TestResult_ErrorsUnwrap keeps the wrap chain intact across the package boundary. FR-6's
// distinction is made several frames above where the error is created, and a chain broken
// by a %v turns that into a guess (docs/14-coding-standards.md §6).
func TestResult_ErrorsUnwrap(t *testing.T) {
	sentinel := errors.New("underlying")

	if got := (Refused{Err: sentinel}); !errors.Is(got, sentinel) {
		t.Error("errors.Is(Refused{Err: sentinel}, sentinel) = false")
	}
	if got := (Unavailable{Err: sentinel}); !errors.Is(got, sentinel) {
		t.Error("errors.Is(Unavailable{Err: sentinel}, sentinel) = false")
	}
	if (Refused{Status: 401}).Error() == "" {
		t.Error("Refused.Error() is empty; an operator needs to know which status refused")
	}
	if (Unavailable{Status: 503}).Error() == "" {
		t.Error("Unavailable.Error() is empty")
	}
	if got := (Refused{Status: 401, Err: sentinel}).Error(); got == "" {
		t.Error("Refused.Error() with a cause is empty")
	}
	if got := (Unavailable{Err: sentinel}).Error(); got == "" {
		t.Error("Unavailable.Error() with a cause is empty")
	}
	if errors.Is(Refused{Status: 401}, sentinel) {
		t.Error("a Refused with no cause unwrapped to something")
	}
}
