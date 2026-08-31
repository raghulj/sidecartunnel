package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestServe_Lifecycle drives the listener the way main does: Serve on a bound listener,
// requests over a real socket, then Shutdown. Serve must return http.ErrServerClosed so
// the caller can tell a clean stop from a crash.
func TestServe_Lifecycle(t *testing.T) {
	h := newHarness(t, nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- h.Serve(l) }()

	resp, err := http.Get("http://" + l.Addr().String() + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /health = %d, want 200", resp.StatusCode)
	}
	var decoded struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil || decoded.Status != "ok" {
		t.Errorf("body = %q, err = %v", body, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of Shutdown")
	}
}

// TestServe_ClosedListener returns the listener's error rather than pretending it served.
func TestServe_ClosedListener(t *testing.T) {
	h := newHarness(t, nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if err := h.Serve(l); err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve on a closed listener = %v, want the accept error", err)
	}
}

// TestWriteJSON_MarshalFailure falls back to a plain 500 rather than emitting a truncated
// body with a 200 status already written.
func TestWriteJSON_MarshalFailure(t *testing.T) {
	h := newHarness(t, nil)
	w := &errWriter{}

	h.writeJSON(w, http.StatusOK, func() {})

	if w.status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.status)
	}
}

// TestWriteJSON_WriteFailure is the client hanging up mid-response. There is nothing to
// do about it and nothing to panic about; it is logged at debug and dropped.
func TestWriteJSON_WriteFailure(t *testing.T) {
	h := newHarness(t, nil)
	w := &errWriter{}

	h.writeJSON(w, http.StatusOK, healthBody{Status: "ok"})

	if w.status != http.StatusOK {
		t.Errorf("status = %d, want 200", w.status)
	}
	if h.logs.Len() == 0 {
		t.Error("a failed write was not logged")
	}
}
