package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/admin"
	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// newRegistryFixture builds a hub on a memory bus and the admin adapter over it.
func newRegistryFixture(t *testing.T) (*hub.Hub, adminRegistry, chan struct{}) {
	t.Helper()
	b := bus.NewMemory(4)
	h := hub.New(context.Background(), b, hub.Options{Prefix: "st:", Separator: "-"})
	t.Cleanup(func() {
		h.Close()
		_ = b.Close()
	})
	flushed := make(chan struct{}, 4)
	return h, adminRegistry{hub: h, flush: func() { flushed <- struct{}{} }}, flushed
}

// TestAdminRegistry_ChannelsAndChannel_FR20 is the operator's view of one replica: bare
// channel names and local counts on the list, the holding user ids on the single-channel
// route, and 404 — reported as false — for a channel this replica does not hold.
func TestAdminRegistry_ChannelsAndChannel_FR20(t *testing.T) {
	h, reg, _ := newRegistryFixture(t)

	one := newTestSink("c1", "u-7")
	two := newTestSink("c2", "u-8")
	for _, s := range []*testSink{one, two} {
		if err := h.Add(s); err != nil {
			t.Fatalf("hub.Add: %v", err)
		}
		if err := h.Subscribe(s, "room-4410", nil); err != nil {
			t.Fatalf("hub.Subscribe: %v", err)
		}
	}

	want := []admin.Channel{{Name: "room-4410", Subscribers: 2}}
	if got := reg.Channels(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Channels() = %+v, want %+v", got, want)
	}

	got, ok := reg.Channel("room-4410")
	if !ok {
		t.Fatal("Channel(room-4410) reported the channel as unheld")
	}
	if !reflect.DeepEqual(got, admin.Channel{Name: "room-4410", Subscribers: 2, Users: []string{"u-7", "u-8"}}) {
		t.Fatalf("Channel(room-4410) = %+v", got)
	}

	if got, ok := reg.Channel("room-9"); ok {
		t.Fatalf("Channel(room-9) = %+v, true; want false so the endpoint answers 404", got)
	}
}

// TestAdminRegistry_DisconnectFlushesTheCache_C4 is POST /disconnect having the same
// effect as the control action: the connection is closed with 3501, and the webhook cache
// is flushed, because a cached entry otherwise survives the revocation.
func TestAdminRegistry_DisconnectFlushesTheCache_C4(t *testing.T) {
	h, reg, flushed := newRegistryFixture(t)
	s := newTestSink("c1", "u-7")
	if err := h.Add(s); err != nil {
		t.Fatalf("hub.Add: %v", err)
	}

	closed, err := reg.Disconnect(admin.Target{User: "u-7"})
	if err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if closed != 1 {
		t.Fatalf("Disconnect closed %d, want 1", closed)
	}
	if got := s.waitClose(t); got != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d", got, proto.CloseRevoked)
	}
	select {
	case <-flushed:
	case <-time.After(waitFor):
		t.Fatal("POST /disconnect did not flush the webhook cache (C4)")
	}
}

// TestAdminRegistry_DisconnectRefusesAnEmptyTarget keeps C8's rule at the adapter as well
// as at the handler: an omitted target is a validation error, not "everyone".
func TestAdminRegistry_DisconnectRefusesAnEmptyTarget(t *testing.T) {
	_, reg, flushed := newRegistryFixture(t)

	closed, err := reg.Disconnect(admin.Target{})
	if !errors.Is(err, hub.ErrNoTarget) {
		t.Fatalf("Disconnect(empty) = %v, want ErrNoTarget", err)
	}
	if closed != 0 {
		t.Fatalf("Disconnect(empty) closed %d connections", closed)
	}
	select {
	case <-flushed:
		t.Fatal("a refused disconnect flushed the webhook cache")
	default:
	}
}

// TestReadiness_ReportsTheBusUntilTheDrainBegins covers the seam docs/09-internals.md §8
// step 1 needs: /ready flips to not-ready the moment the drain starts, and reports the
// bus's own state before that.
func TestReadiness_ReportsTheBusUntilTheDrainBegins(t *testing.T) {
	b := bus.NewMemory(4)
	t.Cleanup(func() { _ = b.Close() })

	r := &readiness{bus: b}
	if got := r.Health(); !got.Connected {
		t.Fatalf("Health() = %+v before the drain, want the memory bus's own connected state", got)
	}

	r.drain()
	got := r.Health()
	if got.Connected {
		t.Fatal("Health() still reports connected while draining; /ready would stay 200")
	}
	if got.DisconnectedFor <= 300*time.Second {
		t.Fatalf("DisconnectedFor = %s, which a large bus.ready_grace could still absorb", got.DisconnectedFor)
	}
	r.drain() // idempotent
}
