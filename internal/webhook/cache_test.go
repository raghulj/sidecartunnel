package webhook

import (
	"testing"
	"time"
)

// baseTime is a fixed instant every clock-driven test starts from. A fixed instant, not
// time.Now(), so a test that depends on the clock fails for a reason rather than at 23:59.
var baseTime = time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)

// TestCacheKey_OnlyTheNamedCookies_C4 is the finding restated as a test.
//
// Hashing the whole Cookie header is why the cache did not deduplicate tabs: _ga, _fbp
// and CSRF tokens differ per tab, so two tabs of one user missed each other and the
// claimed benefit largely did not exist (docs/13-review-findings.md C4).
func TestCacheKey_OnlyTheNamedCookies_C4(t *testing.T) {
	c := newCache(time.Minute, []string{"sessionid"})

	tabOne := c.key("_ga=GA1.1.111; sessionid=abc123; csrftoken=aaa")
	tabTwo := c.key("_ga=GA1.1.222; sessionid=abc123; csrftoken=bbb")

	if tabOne == "" {
		t.Fatal("key is empty for a header containing the named cookie")
	}
	if tabOne != tabTwo {
		t.Error("two tabs of one user produced different cache keys: the key is not restricted to cookie_names (C4)")
	}
	if same := c.key("_ga=GA1.1.111; sessionid=different; csrftoken=aaa"); same == tabOne {
		t.Error("two different sessions produced the same cache key")
	}
}

// TestCacheKey_Variants covers the remaining key-construction rules.
func TestCacheKey_Variants(t *testing.T) {
	t.Run("no names configured hashes the whole header", func(t *testing.T) {
		c := newCache(time.Minute, nil)
		a := c.key("_ga=1; sessionid=abc")
		b := c.key("_ga=2; sessionid=abc")
		if a == "" || b == "" {
			t.Fatal("key is empty for a non-empty header")
		}
		if a == b {
			t.Error("with no cookie_names the whole header is the key, so these must differ (docs/08-config.md §3)")
		}
	})

	t.Run("no named cookie present is uncacheable", func(t *testing.T) {
		c := newCache(time.Minute, []string{"sessionid"})
		if got := c.key("_ga=1; csrftoken=aaa"); got != "" {
			t.Errorf("key = %q, want \"\": with no named cookie present every such request would share one entry", got)
		}
	})

	t.Run("an unparseable header is uncacheable", func(t *testing.T) {
		c := newCache(time.Minute, []string{"sessionid"})
		if got := c.key("this is not a cookie header"); got != "" {
			t.Errorf("key = %q, want \"\"", got)
		}
	})

	t.Run("an empty header is uncacheable", func(t *testing.T) {
		c := newCache(time.Minute, nil)
		if got := c.key(""); got != "" {
			t.Errorf("key = %q, want \"\": there is no session to key on", got)
		}
	})

	t.Run("names are taken in configured order, not header order", func(t *testing.T) {
		c := newCache(time.Minute, []string{"a", "b"})
		if c.key("a=1; b=2") != c.key("b=2; a=1") {
			t.Error("header order changed the key")
		}
		if c.key("a=1; b=2") == c.key("a=2; b=1") {
			t.Error("swapping two cookie values left the key unchanged")
		}
	})
}

// TestCache_HitMissExpiryFlush drives the whole lifecycle on a fake clock. No sleeps:
// TTL expiry is a clock reading, so it is tested by moving the clock
// (docs/14-coding-standards.md §2).
func TestCache_HitMissExpiryFlush(t *testing.T) {
	c := newCache(30*time.Second, []string{"sessionid"})
	key := c.key("sessionid=abc")
	want := Authorized{User: "u-7", ExpiresIn: time.Hour}

	if _, ok := c.get(key, baseTime); ok {
		t.Fatal("empty cache reported a hit")
	}

	c.put(key, want, baseTime)

	if got, ok := c.get(key, baseTime.Add(29*time.Second)); !ok || got.User != want.User {
		t.Errorf("get within the TTL = (%+v, %v), want a hit for %s", got, ok, want.User)
	}
	// The boundary: an entry stored at t is valid until t+ttl and not at t+ttl.
	if _, ok := c.get(key, baseTime.Add(30*time.Second)); ok {
		t.Error("an entry survived exactly its TTL")
	}
	if _, ok := c.get(key, baseTime.Add(time.Hour)); ok {
		t.Error("an expired entry was returned")
	}

	c.put(key, want, baseTime)
	if _, ok := c.get(key, baseTime); !ok {
		t.Fatal("re-stored entry did not come back")
	}
	// FR-18 via docs/13-review-findings.md C4: a control disconnect flushes the whole
	// cache, because a cached entry otherwise survives a revocation and a suspended user
	// reconnecting within the TTL gets their pre-revocation grants back.
	c.flush()
	if _, ok := c.get(key, baseTime); ok {
		t.Error("an entry survived a flush: a revocation would not take effect for a whole TTL")
	}
}

// TestCache_PutIgnoresAnEmptyKey: an uncacheable request must not create an entry, or
// every request without the named cookie would share one.
func TestCache_PutIgnoresAnEmptyKey(t *testing.T) {
	c := newCache(time.Minute, []string{"sessionid"})
	c.put("", Authorized{User: "u-7"}, baseTime)
	if n := c.size(); n != 0 {
		t.Errorf("size = %d, want 0", n)
	}
	if _, ok := c.get("", baseTime); ok {
		t.Error("an empty key produced a hit")
	}
}

// TestCache_SweepsExpiredEntries keeps the map bounded by the connection rate over one
// TTL rather than by the process's whole uptime. Without it the cache is a slow leak: an
// entry that is never looked up again is never removed.
func TestCache_SweepsExpiredEntries(t *testing.T) {
	c := newCache(30*time.Second, nil)

	c.put(c.key("sessionid=a"), Authorized{User: "u-1"}, baseTime)
	c.put(c.key("sessionid=b"), Authorized{User: "u-2"}, baseTime)
	if n := c.size(); n != 2 {
		t.Fatalf("size = %d, want 2", n)
	}

	// A put after the sweep interval clears everything that has expired, including the
	// entries nobody asked for again.
	c.put(c.key("sessionid=c"), Authorized{User: "u-3"}, baseTime.Add(time.Minute))
	if n := c.size(); n != 1 {
		t.Errorf("size = %d, want 1: expired entries were not swept", n)
	}
	if _, ok := c.get(c.key("sessionid=c"), baseTime.Add(time.Minute)); !ok {
		t.Error("the entry that triggered the sweep was swept too")
	}
}
